//! Command Code 网页登录授权（等价于官方 `cmd login` 的 loopback 流程）。
//!
//! Go 套餐无法在 Studio 面板创建 API Key，官方 CLI 的登录流程会在浏览器授权后
//! 把 Studio 签发的 key POST 回本机回环服务。本模块把该流程原样内建到网关：
//!
//! 1. `begin` 在 `127.0.0.1:5959..5968` 上起一次性回调服务；
//! 2. 打开 `{studio}/studio/auth/cli?callback=http://localhost:{port}/callback&state=…`；
//! 3. Studio POST `/callback`，载荷 `{apiKey,state,userId,userName,keyName}`；
//! 4. `state` 校验 + `/alpha/whoami` 校验后，密钥只留在服务端，通过一次性
//!    `login_id` 交给建渠道流程（绝不返回给管理端前端）。
//!
//! 事实依据（均为 MIT，仅作协议依据）：patlux/pi-commandcode-provider
//! `src/auth-server.ts` + `src/oauth.ts`、Mars-Sea/dsh-commandcode-provider
//! `src/login.ts`（含 wire 测试）、rashidrazak/opencode-cmd-provider
//! `src/plugin/auth.ts`。三份实现交叉一致。

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};
use base64::Engine as _;
use serde::Serialize;
use serde_json::{Value, json};

use crate::ports::{UpstreamClient, UpstreamRequest};

/// 与官方 CLI 一致：无回调 120 秒后放弃。
pub const LOGIN_TIMEOUT_MS: u64 = 120_000;
/// 与官方 CLI 一致：回调端口从 5959 起尝试。
pub const LOGIN_START_PORT: u16 = 5959;
/// 与官方 CLI 一致：最多顺延 10 个端口。
pub const LOGIN_MAX_PORT_ATTEMPTS: u16 = 10;
/// 与官方 CLI 一致：回调体上限 10 KB（按 wire bytes 计）。
pub const LOGIN_BODY_LIMIT_BYTES: usize = 10_000;
/// Studio 允许回调的 Origin 白名单（只有白名单内才回显 CORS）。
pub const LOGIN_ALLOWED_ORIGINS: [&str; 3] = [
    "http://localhost:3000",
    "https://staging.commandcode.ai",
    "https://commandcode.ai",
];
/// Studio 授权路由。
pub const STUDIO_AUTH_PATH: &str = "/studio/auth/cli";
/// 成功密钥的一次性交接有效期。
pub const LOGIN_HANDOFF_TTL: Duration = Duration::from_secs(300);
/// 官方 CLI 的凭据文件（作为网页登录不可用时的兜底导入来源）。
pub const CLI_AUTH_PATHS: [&str; 2] = ["~/.commandcode/auth.json", "~/.config/commandcode/auth.json"];

/// 登录失败原因（稳定字符串，供 UI 文案映射）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum LoginFailure {
    /// Studio 页面被用户拒绝。
    Denied,
    /// 回调窗口内没有收到回调。
    Timeout,
    /// 送达的 key 未通过 `/alpha/whoami`（401）。
    InvalidKey,
    /// 校验请求无法到达 API。
    Network,
    /// 校验/存储等内部错误。
    Error,
    /// 用户取消或流程被替换/关闭。
    Cancelled,
}

impl LoginFailure {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Denied => "denied",
            Self::Timeout => "timeout",
            Self::InvalidKey => "invalid-key",
            Self::Network => "network",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
        }
    }
}

/// 登录状态机（序列化给管理端；`login_id` 是一次性交接句柄，不是密钥）。
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum LoginStatus {
    Idle,
    Waiting {
        auth_url: String,
        port: u16,
    },
    Success {
        login_id: String,
        user_name: String,
        key_name: String,
    },
    Failed {
        reason: LoginFailure,
        message: String,
    },
}

impl LoginStatus {
    fn is_waiting(&self) -> bool {
        matches!(self, Self::Waiting { .. })
    }
}

/// 一次登录尝试的参数。
#[derive(Debug, Clone)]
pub struct LoginConfig {
    /// API 基址；同时决定 Studio 基址（staging/localhost 成对映射）。
    pub api_base: String,
    pub timeout: Duration,
    pub start_port: u16,
    pub max_port_attempts: u16,
}

impl Default for LoginConfig {
    fn default() -> Self {
        Self {
            api_base: crate::commandcode::DEFAULT_API_BASE.to_owned(),
            timeout: Duration::from_millis(LOGIN_TIMEOUT_MS),
            start_port: LOGIN_START_PORT,
            max_port_attempts: LOGIN_MAX_PORT_ATTEMPTS,
        }
    }
}

/// 成功送达、等待被渠道创建立即取走的密钥。
struct PendingKey {
    login_id: String,
    api_key: String,
    expires_at: Instant,
}

struct State_ {
    status: LoginStatus,
    attempt: u64,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    pending: Option<PendingKey>,
}

/// 进程内单例登录流程（每个网关 Context 一份；测试可各自独立）。
pub struct CommandCodeLogin {
    http: Arc<dyn UpstreamClient>,
    state: parking_lot::Mutex<State_>,
}

impl CommandCodeLogin {
    pub fn new(http: Arc<dyn UpstreamClient>) -> Arc<Self> {
        Arc::new(Self {
            http,
            state: parking_lot::Mutex::new(State_ {
                status: LoginStatus::Idle,
                attempt: 0,
                shutdown: None,
                pending: None,
            }),
        })
    }

    pub fn status(&self) -> LoginStatus {
        self.state.lock().status.clone()
    }

    /// 取消进行中的等待；终态不受影响。
    pub fn cancel(&self) {
        let mut state = self.state.lock();
        if !state.status.is_waiting() {
            return;
        }
        if let Some(tx) = state.shutdown.take() {
            let _ = tx.send(());
        }
        state.status = LoginStatus::Failed {
            reason: LoginFailure::Cancelled,
            message: "登录已取消".into(),
        };
    }

    /// 读取一次性密钥交接但**不消费**（渠道创建失败时凭据仍然可用）。
    pub fn peek_key(&self, login_id: &str) -> Option<String> {
        let mut state = self.state.lock();
        let now = Instant::now();
        if state
            .pending
            .as_ref()
            .is_some_and(|pending| pending.expires_at <= now)
        {
            state.pending = None;
        }
        state
            .pending
            .as_ref()
            .filter(|pending| pending.login_id == login_id)
            .map(|pending| pending.api_key.clone())
    }

    /// 渠道创建/改写**成功后**消费交接（单次有效）。返回是否确实消费。
    pub fn consume_key(&self, login_id: &str) -> bool {
        let mut state = self.state.lock();
        let matches = state
            .pending
            .as_ref()
            .is_some_and(|pending| pending.login_id == login_id);
        if !matches {
            return false;
        }
        state.pending = None;
        true
    }

    /// 取走一次性密钥交接（peek + consume；测试与兼容入口）。
    pub fn take_key(&self, login_id: &str) -> Option<String> {
        let key = self.peek_key(login_id)?;
        self.consume_key(login_id);
        Some(key)
    }

    /// 测试/导入路径共用的“直接落一个已校验 key”入口。
    pub(crate) fn store_validated_key(
        &self,
        api_key: String,
        user_name: String,
        key_name: String,
    ) -> LoginStatus {
        let login_id = uuid::Uuid::new_v4().simple().to_string();
        let mut state = self.state.lock();
        state.pending = Some(PendingKey {
            login_id: login_id.clone(),
            api_key,
            expires_at: Instant::now() + LOGIN_HANDOFF_TTL,
        });
        state.status = LoginStatus::Success {
            login_id,
            user_name,
            key_name,
        };
        state.status.clone()
    }

    /// 开始（或加入）一次浏览器登录。等待中重复调用返回同一次尝试的 URL。
    pub async fn begin(self: &Arc<Self>, config: LoginConfig) -> Result<LoginStatus> {
        // 快路径：已在等待时直接加入（不再绑定端口）。
        {
            let state = self.state.lock();
            if state.status.is_waiting() && state.shutdown.is_some() {
                return Ok(state.status.clone());
            }
        }
        let listener = bind_first_free(config.start_port, config.max_port_attempts).await?;
        let port = listener.local_addr()?.port();
        let state_token = random_state_token();
        let auth_url =
            build_auth_url(&studio_base_for_api_base(&config.api_base), port, &state_token);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let attempt;
        {
            let mut state = self.state.lock();
            // 并发 begin：后到者让位给已经上线的尝试。
            if state.status.is_waiting() && state.shutdown.is_some() {
                return Ok(state.status.clone());
            }
            if let Some(previous) = state.shutdown.take() {
                let _ = previous.send(());
            }
            state.attempt += 1;
            attempt = state.attempt;
            // 新的尝试作废上一次尚未被取走的交接，避免内存里悬挂多个密钥。
            state.pending = None;
            state.shutdown = Some(shutdown_tx);
            state.status = LoginStatus::Waiting {
                auth_url: auth_url.clone(),
                port,
            };
        }
        self.spawn_callback_server(listener, shutdown_rx, attempt, state_token, config.clone());
        Ok(LoginStatus::Waiting { auth_url, port })
    }

    /// 从官方 CLI 凭据文件（或环境变量）导入 key，并像网页登录一样完成校验。
    /// `auth_path` 为测试/自定义路径覆盖；生产传 `None` 走官方默认路径。
    pub async fn import_cli_key(
        self: &Arc<Self>,
        api_base: &str,
        auth_path: Option<&str>,
    ) -> Result<LoginStatus> {
        let key = match auth_path {
            Some(path) => read_cli_api_key_from(&[path.to_owned()]),
            None => read_cli_api_key(),
        }
        .context("未找到 Command Code 凭据（~/.commandcode/auth.json 或 COMMANDCODE_API_KEY）")?;
        let verdict = validate_api_key(self.http.as_ref(), api_base, &key).await;
        match verdict {
            ApiKeyVerdict::Valid => Ok(self.store_validated_key(
                key,
                String::new(),
                "cli-import".to_owned(),
            )),
            ApiKeyVerdict::Invalid => bail!("CLI 中的 API Key 未通过 /alpha/whoami 校验"),
            ApiKeyVerdict::Network => bail!("无法连接 Command Code API 完成校验"),
            ApiKeyVerdict::Server => bail!("Command Code API 校验失败（服务端错误）"),
        }
    }

    /// 官方 CLI 凭据是否存在（UI 用它决定是否展示“从 CLI 导入”按钮）。
    pub fn cli_key_available(&self) -> bool {
        read_cli_api_key().is_some()
    }

    fn spawn_callback_server(
        self: &Arc<Self>,
        listener: tokio::net::TcpListener,
        shutdown_rx: tokio::sync::oneshot::Receiver<()>,
        attempt: u64,
        state_token: String,
        config: LoginConfig,
    ) {
        let ctx = CallbackCtx {
            flow: Arc::clone(self),
            attempt,
            state_token,
            api_base: config.api_base.clone(),
        };
        let app = Router::new()
            .route(
                "/callback",
                post(callback).options(callback_preflight),
            )
            .fallback(fallback_not_found)
            .layer(DefaultBodyLimit::max(LOGIN_BODY_LIMIT_BYTES))
            .with_state(ctx);
        let flow = Arc::clone(self);
        tokio::spawn(async move {
            let served = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
            if let Err(error) = served {
                flow.settle_failure(
                    attempt,
                    LoginFailure::Error,
                    format!("登录回调服务异常：{error}"),
                );
            }
        });
        // 官方 CLI 的两分钟窗口。
        let flow = Arc::clone(self);
        let timeout = config.timeout;
        tokio::spawn(async move {
            tokio::time::sleep(timeout).await;
            flow.settle_failure(
                attempt,
                LoginFailure::Timeout,
                "等待浏览器回调超时（120 秒）".into(),
            );
        });
    }

    /// 决定性回调到达后立即关闭回环服务（校验仍在后台进行）。
    fn close_attempt_server(&self, attempt: u64) {
        let mut state = self.state.lock();
        if state.attempt != attempt || !state.status.is_waiting() {
            return;
        }
        if let Some(tx) = state.shutdown.take() {
            let _ = tx.send(());
        }
    }

    /// 只在当前尝试仍然等待且 id 匹配时改写状态。
    fn settle_failure(&self, attempt: u64, reason: LoginFailure, message: String) {
        let mut state = self.state.lock();
        if state.attempt != attempt || !state.status.is_waiting() {
            return;
        }
        if let Some(tx) = state.shutdown.take() {
            let _ = tx.send(());
        }
        state.status = LoginStatus::Failed { reason, message };
    }

    fn owns_attempt(&self, attempt: u64) -> bool {
        let state = self.state.lock();
        state.attempt == attempt && state.status.is_waiting()
    }

    /// 回调送达并通过校验后异步完成：whoami 校验 → 一次性交接。
    fn spawn_complete(
        self: &Arc<Self>,
        attempt: u64,
        credentials: CallbackCredentials,
    ) {
        let flow = Arc::clone(self);
        let http = Arc::clone(&self.http);
        let api_base = credentials.api_base.clone();
        tokio::spawn(async move {
            let verdict = validate_api_key(http.as_ref(), &api_base, &credentials.api_key).await;
            if !flow.owns_attempt(attempt) {
                return;
            }
            match verdict {
                ApiKeyVerdict::Valid => {
                    let _ = flow.store_validated_key(
                        credentials.api_key,
                        credentials.user_name,
                        credentials.key_name,
                    );
                }
                ApiKeyVerdict::Invalid => flow.settle_failure(
                    attempt,
                    LoginFailure::InvalidKey,
                    "签发的 API Key 未通过 /alpha/whoami 校验（401）".into(),
                ),
                ApiKeyVerdict::Network => flow.settle_failure(
                    attempt,
                    LoginFailure::Network,
                    "无法连接 Command Code API 完成校验".into(),
                ),
                ApiKeyVerdict::Server => flow.settle_failure(
                    attempt,
                    LoginFailure::Error,
                    "Command Code API 校验失败（服务端错误）".into(),
                ),
            }
        });
    }
}

// ---------------------------------------------------------------------------
// 纯函数（协议契约，便于单测）
// ---------------------------------------------------------------------------

/// 组合 Studio 授权 URL（与官方 CLI/三份实现逐字一致）。
pub fn build_auth_url(studio_base: &str, port: u16, state: &str) -> String {
    let callback = format!("http://localhost:{port}/callback");
    format!(
        "{}{}?callback={}&state={}",
        studio_base.trim_end_matches('/'),
        STUDIO_AUTH_PATH,
        urlencode(&callback),
        urlencode(state)
    )
}

/// API 基址 → 配对的 Studio 基址（staging/localhost 各自成对）。
pub fn studio_base_for_api_base(api_base: &str) -> String {
    let lower = api_base.to_ascii_lowercase();
    if lower.starts_with("https://staging-api.commandcode.ai") {
        return "https://staging.commandcode.ai".to_owned();
    }
    if lower.starts_with("http://localhost") && !lower.contains("//localhost:") {
        return "http://localhost:3000".to_owned();
    }
    if let Some(rest) = lower.strip_prefix("http://localhost:")
        && rest.chars().all(|c| c.is_ascii_digit())
    {
        return "http://localhost:3000".to_owned();
    }
    "https://commandcode.ai".to_owned()
}

/// 去掉终端粘贴包装（CSI 200~/201~）与控制字符。
pub fn sanitize_api_key(input: &str) -> String {
    input
        .replace("\u{1b}[200~", "")
        .replace("\u{1b}[201~", "")
        .replace("[200~", "")
        .replace("[201~", "")
        .chars()
        .filter(|value| {
            let code = *value as u32;
            code > 0x1f && code != 0x7f
        })
        .collect::<String>()
        .trim()
        .to_owned()
}

fn urlencode(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

fn random_state_token() -> String {
    let bytes: [u8; 32] = rand::random();
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

async fn bind_first_free(start_port: u16, attempts: u16) -> Result<tokio::net::TcpListener> {
    let attempts = attempts.max(1);
    for offset in 0..attempts {
        let port = if start_port == 0 {
            0
        } else {
            start_port.saturating_add(offset)
        };
        match tokio::net::TcpListener::bind(("127.0.0.1", port)).await {
            Ok(listener) => return Ok(listener),
            Err(error) if start_port != 0 && offset + 1 < attempts => {
                tracing::debug!(port, %error, "command code login port busy; trying next");
            }
            Err(error) => bail!("没有可用端口（从 {start_port} 尝试 {attempts} 个）：{error}"),
        }
    }
    unreachable!("loop returns on the last attempt")
}

// ---------------------------------------------------------------------------
// /alpha/whoami 校验
// ---------------------------------------------------------------------------

enum ApiKeyVerdict {
    Valid,
    Invalid,
    Server,
    Network,
}

async fn validate_api_key(
    http: &dyn UpstreamClient,
    api_base: &str,
    api_key: &str,
) -> ApiKeyVerdict {
    let url = match url::Url::parse(&format!(
        "{}/alpha/whoami",
        api_base.trim_end_matches('/')
    )) {
        Ok(url) => url,
        Err(_) => return ApiKeyVerdict::Server,
    };
    let mut headers = HeaderMap::new();
    match HeaderValue::from_str(&format!("Bearer {api_key}")) {
        Ok(value) => {
            headers.insert(header::AUTHORIZATION, value);
        }
        Err(_) => return ApiKeyVerdict::Invalid,
    }
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers.insert(header::ACCEPT, HeaderValue::from_static("application/json"));
    let response = http
        .send(UpstreamRequest {
            url,
            headers,
            method: http::Method::GET,
            body: None,
            connect_timeout: Duration::from_secs(10),
            deadline: Duration::from_secs(20),
        })
        .await;
    match response {
        Ok(response) if response.status == StatusCode::UNAUTHORIZED => ApiKeyVerdict::Invalid,
        Ok(response) if response.status.is_success() => ApiKeyVerdict::Valid,
        Ok(_) => ApiKeyVerdict::Server,
        Err(_) => ApiKeyVerdict::Network,
    }
}

// ---------------------------------------------------------------------------
// CLI 凭据文件导入
// ---------------------------------------------------------------------------

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
}

fn expand_home(path: &str) -> Option<PathBuf> {
    if path == "~" {
        return home_dir();
    }
    if let Some(rest) = path.strip_prefix("~/") {
        return home_dir().map(|home| home.join(rest));
    }
    Some(PathBuf::from(path))
}

/// 从 CLI auth JSON 中提取 API Key（镜像 yelixir/commandcode-bridge 与 dsh 的解析规则）。
pub fn extract_cli_api_key(value: &Value) -> Option<String> {
    fn string(value: &Value) -> Option<String> {
        value
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
    }
    fn walk(value: &Value, depth: usize) -> Option<String> {
        if depth > 8 {
            return None;
        }
        match value {
            Value::String(_) => string(value),
            Value::Array(items) => items.iter().find_map(|item| walk(item, depth + 1)),
            Value::Object(map) => {
                for key in [
                    "apiKey",
                    "api_key",
                    "access",
                    "accessToken",
                    "token",
                    "key",
                ] {
                    if let Some(found) = map.get(key).and_then(string) {
                        return Some(found);
                    }
                }
                for key in [
                    "commandcode",
                    "commandCode",
                    "command_code",
                    "auth",
                    "credentials",
                    "oauth",
                    "account",
                ] {
                    if let Some(found) = map.get(key).and_then(|value| walk(value, depth + 1)) {
                        return Some(found);
                    }
                }
                None
            }
            _ => None,
        }
    }
    // 官方 CLI 写入的是对象；顶层裸字符串不是合法 auth 文件。
    value.as_object()?;
    walk(value, 0)
        .map(|key| sanitize_api_key(&key))
        .filter(|key| !key.is_empty())
}

/// 按显式路径列表读取 CLI auth 文件（纯 IO，便于测试）。
pub fn read_cli_api_key_from(paths: &[String]) -> Option<String> {
    for path in paths {
        let Some(path) = expand_home(path) else {
            continue;
        };
        let Ok(bytes) = std::fs::read(&path) else {
            continue;
        };
        let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
            continue;
        };
        if let Some(key) = extract_cli_api_key(&value) {
            return Some(key);
        }
    }
    None
}

/// 读取 CLI 凭据：环境变量优先，其次 `COMMANDCODE_AUTH_PATH`，最后官方默认路径。
pub fn read_cli_api_key() -> Option<String> {
    for name in ["COMMAND_CODE_API_KEY", "COMMANDCODE_API_KEY", "CMD_API_KEY"] {
        if let Ok(value) = std::env::var(name) {
            let value = sanitize_api_key(&value);
            if !value.is_empty() {
                return Some(value);
            }
        }
    }
    let mut paths: Vec<String> = Vec::new();
    if let Ok(path) = std::env::var("COMMANDCODE_AUTH_PATH") {
        paths.push(path);
    }
    paths.extend(CLI_AUTH_PATHS.map(str::to_owned));
    read_cli_api_key_from(&paths)
}

// ---------------------------------------------------------------------------
// 回环回调服务
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct CallbackCtx {
    flow: Arc<CommandCodeLogin>,
    attempt: u64,
    state_token: String,
    api_base: String,
}

struct CallbackCredentials {
    api_key: String,
    user_name: String,
    key_name: String,
    api_base: String,
}

/// 只有白名单 Origin 才回显（其余不回显，浏览器自然拒绝）。
fn cors_origin(origin: Option<&HeaderValue>) -> Option<String> {
    let origin = origin?.to_str().ok()?;
    LOGIN_ALLOWED_ORIGINS
        .contains(&origin)
        .then(|| origin.to_owned())
}

fn json_response(
    status: StatusCode,
    origin: Option<String>,
    payload: Value,
) -> Response {
    let mut builder = Response::builder().status(status).header(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    if let Some(origin) = origin {
        builder = builder.header(
            header::ACCESS_CONTROL_ALLOW_ORIGIN,
            HeaderValue::from_str(&origin).unwrap_or(HeaderValue::from_static("")),
        );
    }
    builder
        .header(
            header::ACCESS_CONTROL_ALLOW_METHODS,
            HeaderValue::from_static("POST, OPTIONS"),
        )
        .header(
            header::ACCESS_CONTROL_ALLOW_HEADERS,
            HeaderValue::from_static("Content-Type"),
        )
        .header(
            "access-control-allow-private-network",
            HeaderValue::from_static("true"),
        )
        .header(header::CONNECTION, HeaderValue::from_static("close"))
        .body(axum::body::Body::from(payload.to_string()))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

async fn callback_preflight(headers: HeaderMap) -> Response {
    let origin = cors_origin(headers.get(header::ORIGIN));
    let mut response = json_response(StatusCode::NO_CONTENT, origin, json!({}));
    // 204 不带正文。
    *response.body_mut() = axum::body::Body::empty();
    response
}

async fn fallback_not_found(State(ctx): State<CallbackCtx>) -> Response {
    let _ = ctx;
    json_response(
        StatusCode::NOT_FOUND,
        None,
        json!({"success": false, "error": "Not found"}),
    )
}

async fn callback(
    State(ctx): State<CallbackCtx>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let origin = cors_origin(headers.get(header::ORIGIN));
    let Ok(payload) = serde_json::from_slice::<Value>(&body) else {
        return json_response(
            StatusCode::BAD_REQUEST,
            origin,
            json!({"success": false, "error": "Invalid JSON"}),
        );
    };

    // Studio 的拒绝载荷同样先校验 state：否则任意网页都能用一条简单请求
    // 结束正在进行的登录（与官方 CLI 的检查顺序一致）。
    if let Some(object) = payload.as_object()
        && let Some(error) = object.get("error")
    {
        let state = payload.get("state").and_then(Value::as_str).unwrap_or("");
        if state != ctx.state_token {
            return json_response(
                StatusCode::FORBIDDEN,
                origin,
                json!({"success": false, "error": "Invalid state token"}),
            );
        }
        let reason = if error.as_str() == Some("access_denied") {
            LoginFailure::Denied
        } else {
            LoginFailure::Error
        };
        let message = payload
            .get("error_description")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| "浏览器授权被拒绝".to_owned());
        let response = json_response(StatusCode::OK, origin, json!({"success": true}));
        // 决定性回调：立刻关停回环服务，避免端口在窗口内继续暴露。
        ctx.flow.close_attempt_server(ctx.attempt);
        ctx.flow.settle_failure(ctx.attempt, reason, message);
        return response;
    }

    let string_field = |key: &str| payload.get(key).and_then(Value::as_str);
    let (Some(api_key), Some(state), Some(_user_id), Some(user_name), Some(key_name)) = (
        string_field("apiKey"),
        string_field("state"),
        string_field("userId"),
        string_field("userName"),
        string_field("keyName"),
    ) else {
        return json_response(
            StatusCode::BAD_REQUEST,
            origin,
            json!({"success": false, "error": "Missing required fields"}),
        );
    };
    if api_key.is_empty() || state.is_empty() || user_name.is_empty() || key_name.is_empty() {
        return json_response(
            StatusCode::BAD_REQUEST,
            origin,
            json!({"success": false, "error": "Missing required fields"}),
        );
    }
    if state != ctx.state_token {
        // 过期标签页重放旧 state 不允许杀掉当前尝试。
        return json_response(
            StatusCode::FORBIDDEN,
            origin,
            json!({"success": false, "error": "Invalid state token"}),
        );
    }
    let api_key = sanitize_api_key(api_key);
    if api_key.is_empty() {
        return json_response(
            StatusCode::BAD_REQUEST,
            origin,
            json!({"success": false, "error": "Missing required fields"}),
        );
    }
    let response = json_response(StatusCode::OK, origin, json!({"success": true}));
    // 决定性回调：立刻关停回环服务（校验在后台继续），新的 begin 会作废
    // 这次仍在校验中的尝试，与 CLI/dsh 的单飞语义一致。
    ctx.flow.close_attempt_server(ctx.attempt);
    // 先确认当前尝试仍属本次回调，再进入异步校验。
    if !ctx.flow.owns_attempt(ctx.attempt) {
        return response;
    }
    ctx.flow.spawn_complete(
        ctx.attempt,
        CallbackCredentials {
            api_key,
            user_name: user_name.to_owned(),
            key_name: key_name.to_owned(),
            api_base: ctx.api_base.clone(),
        },
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_url_matches_the_cli_contract() {
        let url = build_auth_url("https://commandcode.ai", 5959, "abc");
        assert!(url.starts_with("https://commandcode.ai/studio/auth/cli?callback="));
        let parsed = url::Url::parse(&url).unwrap();
        assert_eq!(
            parsed
                .query_pairs()
                .find(|(key, _)| key == "callback")
                .map(|(_, value)| value.into_owned())
                .as_deref(),
            Some("http://localhost:5959/callback")
        );
        assert_eq!(
            parsed
                .query_pairs()
                .find(|(key, _)| key == "state")
                .map(|(_, value)| value.into_owned())
                .as_deref(),
            Some("abc")
        );
        assert!(build_auth_url("https://commandcode.ai/", 5959, "x").contains("/studio/auth/cli?"));
    }

    #[test]
    fn studio_base_pairs_staging_and_localhost() {
        assert_eq!(
            studio_base_for_api_base("https://api.commandcode.ai"),
            "https://commandcode.ai"
        );
        assert_eq!(
            studio_base_for_api_base("https://staging-api.commandcode.ai"),
            "https://staging.commandcode.ai"
        );
        assert_eq!(
            studio_base_for_api_base("http://localhost:8080"),
            "http://localhost:3000"
        );
        assert_eq!(
            studio_base_for_api_base("https://elsewhere.example"),
            "https://commandcode.ai"
        );
    }

    #[test]
    fn sanitize_strips_bracketed_paste_and_control_characters() {
        assert_eq!(sanitize_api_key("\u{1b}[200~user_abc\u{1b}[201~"), "user_abc");
        assert_eq!(sanitize_api_key("[200~user_abc[201~"), "user_abc");
        assert_eq!(sanitize_api_key("  user_abc\n"), "user_abc");
        assert_eq!(sanitize_api_key("\u{7}user_abc"), "user_abc");
    }

    #[test]
    fn cli_auth_json_shapes_are_supported() {
        assert_eq!(
            extract_cli_api_key(&json!({"apiKey": "user_1"})).as_deref(),
            Some("user_1")
        );
        assert_eq!(
            extract_cli_api_key(&json!({"access": "user_2"})).as_deref(),
            Some("user_2")
        );
        assert_eq!(
            extract_cli_api_key(&json!({"commandcode": {"api_key": "user_3"}})).as_deref(),
            Some("user_3")
        );
        assert_eq!(
            extract_cli_api_key(&json!({"credentials": [{"token": "user_4"}]})).as_deref(),
            Some("user_4")
        );
        assert_eq!(
            extract_cli_api_key(&json!({"oauth": {"account": {"key": "user_5"}}})).as_deref(),
            Some("user_5")
        );
        assert!(extract_cli_api_key(&json!({"userName": "x"})).is_none());
        assert!(extract_cli_api_key(&json!("not-an-object")).is_none());
    }

    #[test]
    fn state_tokens_are_32_random_bytes_base64url() {
        let first = random_state_token();
        let second = random_state_token();
        assert!(first.len() >= 42, "base64url of 32 bytes: {first}");
        assert_ne!(first, second);
        assert!(first.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
    }
    // ---------------------------------------------------------------- flow

    use crate::ports::{UpstreamBody, UpstreamError, UpstreamResponse};
    use futures_util::future::BoxFuture;
    use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};

    /// whoami 校验桩：记录 Authorization，可脚本化为 200/401/网络失败。
    #[derive(Default)]
    struct FakeWhoami {
        keys: parking_lot::Mutex<Vec<String>>,
        status: AtomicU16,
        unreachable: AtomicBool,
    }

    impl FakeWhoami {
        fn with_status(status: u16) -> Arc<Self> {
            let fake = Arc::new(Self::default());
            fake.status.store(status, Ordering::SeqCst);
            fake
        }

        fn keys(&self) -> Vec<String> {
            self.keys.lock().clone()
        }
    }

    impl UpstreamClient for FakeWhoami {
        fn send(
            &self,
            request: UpstreamRequest,
        ) -> BoxFuture<'static, Result<UpstreamResponse, UpstreamError>> {
            let key = request
                .headers
                .get(http::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .map(|value| value.trim_start_matches("Bearer ").to_owned())
                .unwrap_or_default();
            self.keys.lock().push(key);
            if self.unreachable.load(Ordering::SeqCst) {
                return Box::pin(async move { Err(UpstreamError::Transport("down".into())) });
            }
            let status = match self.status.load(Ordering::SeqCst) {
                0 => 200,
                other => other,
            };
            Box::pin(async move {
                Ok(UpstreamResponse {
                    status: StatusCode::from_u16(status).unwrap(),
                    headers: HeaderMap::new(),
                    body: UpstreamBody::new(Box::pin(futures_util::stream::iter(Vec::new()))),
                })
            })
        }
    }

    fn test_client() -> reqwest::Client {
        // 本机回环测试绝不走系统代理。
        reqwest::Client::builder().no_proxy().build().unwrap()
    }

    fn parse_auth_url(auth_url: &str) -> (String, String) {
        let parsed = url::Url::parse(auth_url).unwrap();
        let callback = parsed
            .query_pairs()
            .find(|(key, _)| key == "callback")
            .map(|(_, value)| value.into_owned())
            .expect("callback parameter");
        let state = parsed
            .query_pairs()
            .find(|(key, _)| key == "state")
            .map(|(_, value)| value.into_owned())
            .expect("state parameter");
        (callback, state)
    }

    async fn wait_status(
        flow: &Arc<CommandCodeLogin>,
        predicate: impl Fn(&LoginStatus) -> bool,
    ) -> LoginStatus {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let status = flow.status();
            if predicate(&status) {
                return status;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for login status; last: {status:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn fast_config() -> LoginConfig {
        LoginConfig {
            api_base: crate::commandcode::DEFAULT_API_BASE.to_owned(),
            timeout: Duration::from_secs(5),
            start_port: 0,
            max_port_attempts: 1,
        }
    }


    #[test]
    fn handoff_peek_does_not_consume_until_explicit_consume() {
        let flow = CommandCodeLogin::new(Arc::new(FakeWhoami::default()));
        let status = flow.store_validated_key(
            "user_k".to_owned(),
            "alice".to_owned(),
            "cli".to_owned(),
        );
        let login_id = match status {
            LoginStatus::Success { login_id, .. } => login_id,
            other => panic!("expected success, got {other:?}"),
        };
        assert_eq!(flow.peek_key(&login_id).as_deref(), Some("user_k"));
        assert_eq!(
            flow.peek_key(&login_id).as_deref(),
            Some("user_k"),
            "peek must not consume"
        );
        assert!(flow.consume_key(&login_id));
        assert!(flow.peek_key(&login_id).is_none());
        assert!(!flow.consume_key(&login_id));
        assert!(flow.take_key(&login_id).is_none());
    }

    #[tokio::test]
    async fn browser_login_happy_path_validates_and_hands_off_the_key() {
        let whoami = Arc::new(FakeWhoami::default());
        let flow = CommandCodeLogin::new(whoami.clone());
        let waiting = flow.begin(fast_config()).await.unwrap();
        assert!(waiting.is_waiting());
        let (callback, state) = parse_auth_url(match &waiting {
            LoginStatus::Waiting { auth_url, .. } => auth_url,
            other => panic!("expected waiting, got {other:?}"),
        });
        assert!(callback.starts_with("http://localhost:"));
        assert!(callback.ends_with("/callback"));

        let client = test_client();
        let response = client
            .post(&callback)
            .json(&json!({
                "apiKey": "user_login_key",
                "state": state,
                "userId": "u1",
                "userName": "mars-sea",
                "keyName": "cli"
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.json::<Value>().await.unwrap(),
            json!({"success": true})
        );

        let status = wait_status(&flow, |status| !status.is_waiting()).await;
        match status {
            LoginStatus::Success {
                login_id,
                user_name,
                key_name,
            } => {
                assert_eq!(user_name, "mars-sea");
                assert_eq!(key_name, "cli");
                assert_eq!(
                    flow.take_key(&login_id).as_deref(),
                    Some("user_login_key"),
                    "the handed-off key is the Studio key"
                );
                assert!(
                    flow.take_key(&login_id).is_none(),
                    "handoff must be single-use"
                );
            }
            other => panic!("expected success, got {other:?}"),
        }
        assert_eq!(whoami.keys(), vec!["user_login_key".to_owned()]);
        // The loopback server is gone once the attempt settles.
        assert!(
            client
                .post(&callback)
                .json(&json!({}))
                .send()
                .await
                .is_err(),
            "callback port must close after the attempt"
        );
    }

    #[tokio::test]
    async fn stale_state_is_rejected_without_killing_the_attempt() {
        let flow = CommandCodeLogin::new(Arc::new(FakeWhoami::default()));
        let waiting = flow.begin(fast_config()).await.unwrap();
        let (callback, state) = parse_auth_url(match &waiting {
            LoginStatus::Waiting { auth_url, .. } => auth_url,
            other => panic!("expected waiting, got {other:?}"),
        });
        let client = test_client();
        let response = client
            .post(&callback)
            .json(&json!({
                "apiKey": "user_x", "state": "forged", "userId": "u1",
                "userName": "u", "keyName": "k"
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(flow.status().is_waiting(), "stale state must not kill the attempt");

        // The real state still completes afterwards.
        let response = client
            .post(&callback)
            .json(&json!({
                "apiKey": "user_x", "state": state, "userId": "u1",
                "userName": "u", "keyName": "k"
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(matches!(
            wait_status(&flow, |status| !status.is_waiting()).await,
            LoginStatus::Success { .. }
        ));
    }

    #[tokio::test]
    async fn denied_authorization_requires_the_state_and_fails_denied() {
        let flow = CommandCodeLogin::new(Arc::new(FakeWhoami::default()));
        let waiting = flow.begin(fast_config()).await.unwrap();
        let (callback, state) = parse_auth_url(match &waiting {
            LoginStatus::Waiting { auth_url, .. } => auth_url,
            other => panic!("expected waiting, got {other:?}"),
        });
        let client = test_client();
        // State-less denial (a CORS simple request any page could send) is
        // refused and the attempt survives.
        let response = client
            .post(&callback)
            .header("content-type", "text/plain")
            .body("{\"error\":\"access_denied\"}")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(flow.status().is_waiting());

        let response = client
            .post(&callback)
            .json(&json!({"error": "access_denied", "state": state, "error_description": "denied"}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        match wait_status(&flow, |status| !status.is_waiting()).await {
            LoginStatus::Failed { reason, .. } => assert_eq!(reason, LoginFailure::Denied),
            other => panic!("expected denied, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn invalid_delivered_key_fails_before_handoff() {
        let whoami = FakeWhoami::with_status(401);
        let flow = CommandCodeLogin::new(whoami);
        let waiting = flow.begin(fast_config()).await.unwrap();
        let (callback, state) = parse_auth_url(match &waiting {
            LoginStatus::Waiting { auth_url, .. } => auth_url,
            other => panic!("expected waiting, got {other:?}"),
        });
        let client = test_client();
        let response = client
            .post(&callback)
            .json(&json!({
                "apiKey": "user_bad", "state": state, "userId": "u1",
                "userName": "u", "keyName": "k"
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        match wait_status(&flow, |status| !status.is_waiting()).await {
            LoginStatus::Failed { reason, .. } => assert_eq!(reason, LoginFailure::InvalidKey),
            other => panic!("expected invalid-key, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unreachable_validation_api_fails_with_network() {
        let whoami = Arc::new(FakeWhoami::default());
        whoami.unreachable.store(true, Ordering::SeqCst);
        let flow = CommandCodeLogin::new(whoami);
        let waiting = flow.begin(fast_config()).await.unwrap();
        let (callback, state) = parse_auth_url(match &waiting {
            LoginStatus::Waiting { auth_url, .. } => auth_url,
            other => panic!("expected waiting, got {other:?}"),
        });
        let client = test_client();
        client
            .post(&callback)
            .json(&json!({
                "apiKey": "user_x", "state": state, "userId": "u1",
                "userName": "u", "keyName": "k"
            }))
            .send()
            .await
            .unwrap();
        match wait_status(&flow, |status| !status.is_waiting()).await {
            LoginStatus::Failed { reason, .. } => assert_eq!(reason, LoginFailure::Network),
            other => panic!("expected network, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn attempt_times_out_without_a_callback() {
        let flow = CommandCodeLogin::new(Arc::new(FakeWhoami::default()));
        let mut config = fast_config();
        config.timeout = Duration::from_millis(120);
        let waiting = flow.begin(config).await.unwrap();
        let (callback, _) = parse_auth_url(match &waiting {
            LoginStatus::Waiting { auth_url, .. } => auth_url,
            other => panic!("expected waiting, got {other:?}"),
        });
        match wait_status(&flow, |status| !status.is_waiting()).await {
            LoginStatus::Failed { reason, .. } => assert_eq!(reason, LoginFailure::Timeout),
            other => panic!("expected timeout, got {other:?}"),
        }
        // The callback server must be gone too.
        assert!(
            test_client()
                .post(&callback)
                .json(&json!({}))
                .send()
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn cancel_closes_the_callback_server() {
        let flow = CommandCodeLogin::new(Arc::new(FakeWhoami::default()));
        let waiting = flow.begin(fast_config()).await.unwrap();
        let (callback, _) = parse_auth_url(match &waiting {
            LoginStatus::Waiting { auth_url, .. } => auth_url,
            other => panic!("expected waiting, got {other:?}"),
        });
        flow.cancel();
        match flow.status() {
            LoginStatus::Failed { reason, .. } => assert_eq!(reason, LoginFailure::Cancelled),
            other => panic!("expected cancelled, got {other:?}"),
        }
        assert!(
            test_client()
                .post(&callback)
                .json(&json!({}))
                .send()
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn callback_endpoint_mirrors_the_cli_rules() {
        let flow = CommandCodeLogin::new(Arc::new(FakeWhoami::default()));
        let waiting = flow.begin(fast_config()).await.unwrap();
        let (callback, _) = parse_auth_url(match &waiting {
            LoginStatus::Waiting { auth_url, .. } => auth_url,
            other => panic!("expected waiting, got {other:?}"),
        });
        let client = test_client();
        let callback_url = url::Url::parse(&callback).unwrap();

        // Preflight from an allowlisted Studio origin echoes CORS + PNA.
        let preflight = client
            .request(reqwest::Method::OPTIONS, &callback)
            .header("origin", "https://commandcode.ai")
            .send()
            .await
            .unwrap();
        assert_eq!(preflight.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            preflight
                .headers()
                .get("access-control-allow-origin")
                .and_then(|value| value.to_str().ok()),
            Some("https://commandcode.ai")
        );
        assert_eq!(
            preflight
                .headers()
                .get("access-control-allow-private-network")
                .and_then(|value| value.to_str().ok()),
            Some("true")
        );
        // A foreign origin is not echoed.
        let foreign = client
            .request(reqwest::Method::OPTIONS, &callback)
            .header("origin", "https://evil.example")
            .send()
            .await
            .unwrap();
        assert!(
            foreign
                .headers()
                .get("access-control-allow-origin")
                .is_none_or(|value| value.is_empty())
        );
        // Wrong path / method / JSON / fields.
        let other = url::Url::parse(&format!(
            "http://{}:{}/other",
            callback_url.host_str().unwrap(),
            callback_url.port().unwrap()
        ))
        .unwrap();
        assert_eq!(
            client
                .post(other)
                .json(&json!({}))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            client.get(&callback).send().await.unwrap().status(),
            StatusCode::METHOD_NOT_ALLOWED
        );
        assert_eq!(
            client
                .post(&callback)
                .header("content-type", "application/json")
                .body("{nope")
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            client
                .post(&callback)
                .json(&json!({"apiKey": "k"}))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
        assert!(flow.status().is_waiting(), "malformed callbacks never settle the attempt");
    }

    #[tokio::test]
    async fn begin_rejoins_waiting_and_terminal_state_starts_fresh() {
        let flow = CommandCodeLogin::new(Arc::new(FakeWhoami::default()));
        let first = flow.begin(fast_config()).await.unwrap();
        let second = flow.begin(fast_config()).await.unwrap();
        match (&first, &second) {
            (
                LoginStatus::Waiting { auth_url: a, .. },
                LoginStatus::Waiting { auth_url: b, .. },
            ) => assert_eq!(a, b, "waiting attempts are single-flight"),
            _ => panic!("expected two waiting statuses"),
        }
        flow.cancel();
        let third = flow.begin(fast_config()).await.unwrap();
        match (&first, &third) {
            (
                LoginStatus::Waiting { auth_url: a, .. },
                LoginStatus::Waiting { auth_url: b, .. },
            ) => assert_ne!(a, b, "a terminal state starts a fresh attempt"),
            _ => panic!("expected waiting statuses"),
        }
    }

    #[tokio::test]
    async fn no_free_port_reports_a_clear_error() {
        let occupant = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let busy_port = occupant.local_addr().unwrap().port();
        let flow = CommandCodeLogin::new(Arc::new(FakeWhoami::default()));
        let result = flow
            .begin(LoginConfig {
                start_port: busy_port,
                max_port_attempts: 1,
                ..fast_config()
            })
            .await;
        assert!(result.is_err(), "bound port must fail the attempt start");
        drop(occupant);
    }

    #[tokio::test]
    async fn cli_import_reads_auth_file_and_validates() {
        let dir = std::env::temp_dir().join(format!("lagw-cc-login-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("auth.json");
        std::fs::write(&path, r#"{"commandcode":{"apiKey":"user_cli_key"}}"#).unwrap();
        let whoami = Arc::new(FakeWhoami::default());
        let flow = CommandCodeLogin::new(whoami.clone());
        let status = flow
            .import_cli_key(
                crate::commandcode::DEFAULT_API_BASE,
                Some(path.to_str().unwrap()),
            )
            .await
            .unwrap();
        match status {
            LoginStatus::Success { login_id, .. } => {
                assert_eq!(
                    flow.take_key(&login_id).as_deref(),
                    Some("user_cli_key")
                );
            }
            other => panic!("expected success, got {other:?}"),
        }
        assert_eq!(whoami.keys(), vec!["user_cli_key".to_owned()]);
        let _ = std::fs::remove_dir_all(&dir);
    }

}
