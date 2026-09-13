//! Command Code Go 集成（路线 B）：身份、会话、transport router 与额度辅助。
//!
//! 协议转换在 [`crate::convert::commandcode`]，本模块只负责「以 CLI 身份与
//! 上游对话」的周边：每 API Key（=每渠道）的指纹、初始化节流、会话粘滞、
//! 官方 Provider API → `/alpha/generate` 的 transport 记忆，以及额度重置解析。
//!
//! 事实基准见 `docs/command-code-protocol.md`（阶段 0）。

use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::db::Database;
use crate::ports::{UpstreamClient, UpstreamRequest};
use crate::protocol;

/// 默认 API 基址；渠道 `base_url` 可覆盖（自建桥场景）。
pub const DEFAULT_API_BASE: &str = "https://api.commandcode.ai";
/// 社区实现交叉验证过的基线 CLI 版本，运行时以探测值为准。
pub const DEFAULT_CLI_VERSION: &str = "1.53.1";
/// 官方 Provider API（合法路径，Go 套餐 403 `upgrade_required`）。
pub const PROVIDER_CHAT_PATH: &str = "/provider/v1/chat/completions";
/// 反代路径（主推理端点，NDJSON 流）。
pub const GENERATE_PATH: &str = "/alpha/generate";
/// 指纹上报端点。
pub const FINGERPRINT_PATH: &str = "/alpha/fingerprint/record";
/// 生命周期事件端点。
pub const LIFECYCLE_PATH: &str = "/alpha/lifecycle-events";
/// npm 包名（版本漂移对照）。
pub const NPM_PACKAGE: &str = "command-code";

/// 初始化节流：首次 + 每 8h（+ 0–2h 抖动）。
pub const INIT_INTERVAL_HOURS: i64 = 8;
pub const INIT_JITTER_HOURS: i64 = 2;
/// 会话 TTL：12h + 1h 抖动，按 API Key（渠道）。
pub const SESSION_TTL_HOURS: i64 = 12;
pub const SESSION_JITTER_HOURS: i64 = 1;

/// 官方 CLI 包内 `dist/bundled/command-code-knowledge/reference/models.md`
/// （`command-code@1.53.1`）中标记 **Go and above** 的模型目录快照：
/// `(id, 显示名, 上下文窗口)`。仅当 `/provider/v1/models` 对当前凭据不可达
/// （Go 套餐稳定返回 403 `upgrade_required`）时作为模型探测兜底；不随上游自动
/// 更新，漂移由版本探测与 UI 告警提示，用户仍可手工增删模型。
pub const BUNDLED_GO_CATALOG: &[(&str, &str, &str)] = &[
    ("deepseek/deepseek-v4-pro", "DeepSeek V4 Pro (latest)", "1M"),
    ("deepseek/deepseek-v4-flash", "DeepSeek V4 Flash (latest)", "1M"),
    ("deepseek/deepseek-v4-flash-vision-exp", "DeepSeek V4 Flash Vision (exp)", "1M"),
    ("deepseek/deepseek-v4-flash-fast", "DeepSeek V4 Flash Fast", "1M"),
    ("deepseek/deepseek-v4.1-flash", "DeepSeek V4.1 Flash", "1M"),
    ("moonshotai/Kimi-K3", "Kimi K3", "1M"),
    ("moonshotai/Kimi-K2.7-Code", "Kimi K2.7 Code", "256K"),
    ("moonshotai/Kimi-K2.7-Code-Highspeed", "Kimi K2.7 Code HighSpeed", "262K"),
    ("moonshotai/Kimi-K2.6", "Kimi K2.6", "256K"),
    ("moonshotai/Kimi-K2.5", "Kimi K2.5", "256K"),
    ("z-ai/glm-5.3-flash", "GLM-5.3 Flash", "1.05M"),
    ("zai-org/GLM-5.3", "GLM-5.3", "1M"),
    ("zai-org/GLM-5.2", "GLM-5.2", "1M"),
    ("zai-org/GLM-5.2-Fast", "GLM-5.2 Fast", "1M"),
    ("zai-org/GLM-5.1", "GLM-5.1", "-"),
    ("zai-org/GLM-5", "GLM-5", "200K"),
    ("MiniMaxAI/MiniMax-M3", "MiniMax M3", "1M"),
    ("MiniMaxAI/MiniMax-M2.7", "MiniMax M2.7", "-"),
    ("MiniMaxAI/MiniMax-M2.5", "MiniMax M2.5", "200K"),
    ("xiaomi/mimo-v2.5-pro", "MiMo V2.5 Pro", "1M"),
    ("xiaomi/mimo-v2.5", "MiMo V2.5", "1M"),
    ("Qwen/Qwen3.8-Max-0902", "Qwen 3.8 Max 0902", "1M"),
    ("Qwen/Qwen3.8-Max", "Qwen 3.8 Max", "1M"),
    ("Qwen/Qwen3.8-27B", "Qwen 3.8 27B", "262K"),
    ("Qwen/Qwen3.8-Flash", "Qwen 3.8 Flash", "1M"),
    ("Qwen/Qwen3.7-Max", "Qwen 3.7 Max", "1M"),
    ("Qwen/Qwen3.7-Plus", "Qwen 3.7 Plus", "1M"),
    ("Qwen/Qwen3.7-Flash", "Qwen 3.7 Flash", "1M"),
    ("Qwen/Qwen3.6-Max-Preview", "Qwen 3.6 Max Preview", "-"),
    ("Qwen/Qwen3.6-Plus", "Qwen 3.6 Plus", "-"),
    ("meituan/LongCat-2.0:free", "LongCat 2.0", "1.05M"),
    ("stepfun/Step-3.7-Flash", "Step 3.7 Flash", "256K"),
    ("stepfun/Step-3.5-Flash", "Step 3.5 Flash", "1M"),
    ("tencent/hy3-paid", "Tencent Hy3", "262K"),
    ("tencent/hy4-preview", "Tencent Hy4 Preview", "1.05M"),
    ("nvidia/nemotron-3-ultra-550b-a55b", "Nemotron 3 Ultra", "1M"),
    ("thinkingmachines/inkling", "Inkling", "256K"),
    ("thinkingmachines/inkling-small", "Inkling Small", "1M"),
    ("poolside/laguna-s-2.1-free", "Laguna S 2.1", "256K"),
    ("inclusionai/ling-3.0-flash-sante:free", "Ling 3.0 Flash Sante", "262K"),
    ("gpt-5.6-luna", "GPT-5.6 Luna", "1.05M"),
    ("meta/muse-spark-1.2-contributor", "Muse Spark 1.2 Contributor", "1.05M"),
    ("meta/muse-spark-1.3-contributor", "Muse Spark 1.3 Contributor", "1.05M"),
    ("xai/grok-4.5", "Grok 4.5", "500K"),
];

/// 兜底目录 → 模型目录条目（与 `/provider/v1/models` 的 item 形状兼容；
/// `command_code_bundled` 标记会进入 `channel_models.metadata_json`）。
pub fn bundled_catalog_items() -> Vec<Value> {
    BUNDLED_GO_CATALOG
        .iter()
        .map(|(id, name, context)| {
            json!({
                "id": id,
                "display_name": name,
                "context_window": context,
                "command_code_bundled": true,
            })
        })
        .collect()
}

/// transport router 状态（按渠道记忆，先官方 Provider API、403 后降级反代）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    Unknown,
    Provider,
    Generate,
}

impl Transport {
    pub fn parse(value: &str) -> Self {
        match value {
            "provider" => Self::Provider,
            "generate" => Self::Generate,
            _ => Self::Unknown,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Provider => "provider",
            Self::Generate => "generate",
        }
    }
}

// ---------------------------------------------------------------------------
// settings KV helpers (settings 是 KV 表，新增键无需迁移)
// ---------------------------------------------------------------------------

async fn read_json(db: &Database, key: &str) -> Option<Value> {
    sqlx::query_scalar::<_, String>("SELECT CAST(value_json AS TEXT) FROM settings WHERE key=?")
        .bind(key)
        .fetch_optional(db.pool())
        .await
        .ok()
        .flatten()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
}

async fn write_json(db: &Database, key: &str, value: &Value) -> Result<()> {
    sqlx::query(
        "INSERT INTO settings(key, value_json, updated_at) VALUES(?, ?, ?) \
         ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at",
    )
    .bind(key)
    .bind(serde_json::to_string(value)?)
    .bind(Utc::now().to_rfc3339())
    .execute(db.pool())
    .await?;
    Ok(())
}

pub fn channel_key(name: &str, channel_id: &str) -> String {
    format!("command_code_{name}_{channel_id}")
}

// ---------------------------------------------------------------------------
// session / version / identity
// ---------------------------------------------------------------------------

fn now_epoch() -> i64 {
    Utc::now().timestamp()
}

/// 会话粘滞（12h + 1h 抖动，按渠道持久化）。粘滞保证上游 prompt cache 命中；
/// 过期后重建。
pub async fn session_id(db: &Database, channel_id: &str) -> Result<String> {
    let key = channel_key("session", channel_id);
    if let Some(value) = read_json(db, &key).await {
        let id = value
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let expires_at = value
            .get("expires_at")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        if !id.is_empty() && expires_at > now_epoch() {
            return Ok(id);
        }
    }
    let id = format!("sess_{}", uuid::Uuid::new_v4().simple());
    let ttl = SESSION_TTL_HOURS * 3600 + rand::random_range(0..=SESSION_JITTER_HOURS * 3600);
    write_json(
        db,
        &key,
        &json!({"id": id, "expires_at": now_epoch() + ttl}),
    )
    .await?;
    Ok(id)
}

/// 当前 CLI 版本：优先运行时探测值，缺失时用基线。
pub async fn cli_version(db: &Database) -> String {
    read_json(db, "command_code_cli_version")
        .await
        .and_then(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .or_else(|| value.get("version").and_then(Value::as_str).map(str::to_owned))
        })
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_CLI_VERSION.to_owned())
}

/// ZDR 开关（全局设置，默认关闭；计划 §7.4 未决项的保守实现）。
pub async fn zdr_enabled(db: &Database) -> bool {
    read_json(db, "command_code_zdr")
        .await
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
}

/// 组装当前请求的身份头值。会话在首次调用时创建并持久化。
pub async fn identity(db: &Database, channel_id: &str) -> Result<protocol::CommandCodeIdentity> {
    let session_id = session_id(db, channel_id).await?;
    Ok(protocol::CommandCodeIdentity {
        cli_version: cli_version(db).await,
        project_slug: crate::protocol::derive_project_slug(&session_id),
        session_id,
        zdr: zdr_enabled(db).await,
    })
}

/// 探测路径的容错身份：读取失败也返回可用值（版本基线、空会话），
/// 供 health/discovery 使用，绝不因本地存储问题阻断探测。
pub async fn identity_for_probe(db: &Database, channel_id: &str) -> protocol::CommandCodeIdentity {
    match identity(db, channel_id).await {
        Ok(value) => value,
        Err(error) => {
            tracing::warn!(%error, channel_id, "command code identity fallback");
            protocol::CommandCodeIdentity {
                cli_version: DEFAULT_CLI_VERSION.to_owned(),
                session_id: String::new(),
                project_slug: String::new(),
                zdr: false,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// fingerprint
// ---------------------------------------------------------------------------

fn sha256_hex(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Windows x64 指纹对照表的一个子集（与社区实现同构：随机选取、按 key 持久化）。
const FINGERPRINT_CPUS: [(&str, i64); 8] = [
    ("Intel(R) Core(TM) i7-10700 CPU @ 2.90GHz", 8),
    ("Intel(R) Core(TM) i5-10400 CPU @ 2.90GHz", 6),
    ("AMD Ryzen 5 5600X 6-Core Processor", 6),
    ("AMD Ryzen 7 5800X 8-Core Processor", 8),
    ("Intel(R) Core(TM) i9-10900K CPU @ 3.70GHz", 10),
    ("AMD Ryzen 9 5900X 12-Core Processor", 12),
    ("Intel(R) Xeon(R) W-2295 CPU @ 3.00GHz", 18),
    ("AMD Ryzen 5 3600 6-Core Processor", 6),
];
const FINGERPRINT_MEMS: [i64; 5] = [8, 16, 24, 32, 64];
const FINGERPRINT_TZS: [&str; 6] = [
    "Asia/Shanghai",
    "Asia/Tokyo",
    "Europe/London",
    "America/New_York",
    "America/Los_Angeles",
    "Europe/Berlin",
];

/// 生成一个随机设备指纹（结构见协议文档 §3）。纯函数 + 随机源，
/// 单测只断言结构与 thumbmark 一致性。
pub fn generate_fingerprint() -> Value {
    let cpu = FINGERPRINT_CPUS[rand::random_range(0..FINGERPRINT_CPUS.len())];
    let mem_gib = FINGERPRINT_MEMS[rand::random_range(0..FINGERPRINT_MEMS.len())];
    let timezone = FINGERPRINT_TZS[rand::random_range(0..FINGERPRINT_TZS.len())];
    let mac_count = rand::random_range(2..=5usize);
    let random_hex = |bytes: usize| {
        let mut out = String::with_capacity(bytes * 2);
        while out.len() < bytes * 2 {
            out.push_str(&uuid::Uuid::new_v4().simple().to_string());
        }
        out.truncate(bytes * 2);
        out
    };
    let mac_hashes: Vec<String> = (0..mac_count)
        .map(|_| sha256_hex(&random_hex(32)))
        .collect();
    let machine_id_hash = sha256_hex(&random_hex(32));
    let os_user_hash = sha256_hex(&random_hex(16));
    let hostname_hash = sha256_hex(&random_hex(16));
    let git_email_hash = sha256_hex(&random_hex(16));
    // 平台/架构随宿主（社区对照表是 win32/x64，其他宿主按真实值上报）。
    let platform = match std::env::consts::OS {
        "windows" => "win32".to_owned(),
        other => other.to_owned(),
    };
    let arch = match std::env::consts::ARCH {
        "x86_64" => "x64".to_owned(),
        other => other.to_owned(),
    };
    let os_release = if platform == "win32" {
        "10.0.22631".to_owned()
    } else {
        std::env::consts::OS.to_owned()
    };
    let thumb_data = format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        machine_id_hash,
        mac_hashes.join("|"),
        os_user_hash,
        hostname_hash,
        git_email_hash,
        platform,
        os_release,
        cpu.0,
        cpu.1,
        mem_gib
    );
    let thumbmark = sha256_hex(&thumb_data);
    json!({
        "thumbmark": thumbmark,
        "components": {
            "machineIdHash": machine_id_hash,
            "macHashes": mac_hashes,
            "osUserHash": os_user_hash,
            "hostnameHash": hostname_hash,
            "gitEmailHash": git_email_hash,
            "platform": platform,
            "arch": arch,
            "osRelease": os_release,
            "cpuModel": cpu.0,
            "cpuCount": cpu.1,
            "memGiB": mem_gib,
            "isContainer": false,
            "timezone": timezone,
            "runtime": "cli",
            "collectorVersion": 1
        }
    })
}

/// 每渠道（每 API Key）稳定指纹：首次生成后持久化。
pub async fn ensure_fingerprint(db: &Database, channel_id: &str) -> Result<Value> {
    let key = channel_key("fingerprint", channel_id);
    if let Some(value) = read_json(db, &key).await
        && value.get("thumbmark").and_then(Value::as_str).is_some()
    {
        return Ok(value);
    }
    let fingerprint = generate_fingerprint();
    write_json(db, &key, &fingerprint).await?;
    Ok(fingerprint)
}

// ---------------------------------------------------------------------------
// 初始化节流：fingerprint/record + lifecycle-events
// ---------------------------------------------------------------------------

/// 首次 + 每 8h（+0–2h 抖动）上报指纹与生命周期事件。按渠道节流，
/// 失败不阻断业务请求（下次请求重试）；headers 与 `/alpha/generate` 同构。
pub async fn ensure_initialized(
    db: &Database,
    http: &dyn UpstreamClient,
    channel_id: &str,
    api_key: &str,
    base_url: &str,
    init_interval_hours: i64,
) -> Result<()> {
    let key = channel_key("init_at", channel_id);
    let now = now_epoch();
    if let Some(value) = read_json(db, &key).await
        && value.get("next_init_at").and_then(Value::as_i64).unwrap_or(0) > now
    {
        return Ok(());
    }
    let fingerprint = ensure_fingerprint(db, channel_id).await?;
    let version = cli_version(db).await;
    let zdr = zdr_enabled(db).await;
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/json"),
    );
    headers.insert(
        axum::http::header::AUTHORIZATION,
        axum::http::HeaderValue::from_str(&format!("Bearer {api_key}"))?,
    );
    headers.insert(
        axum::http::HeaderName::from_static("x-cli-environment"),
        axum::http::HeaderValue::from_static("production"),
    );
    headers.insert(
        axum::http::HeaderName::from_static("x-command-code-version"),
        axum::http::HeaderValue::from_str(&version)?,
    );
    if zdr {
        headers.insert(
            axum::http::HeaderName::from_static("x-cmd-zdr"),
            axum::http::HeaderValue::from_static("1"),
        );
    }
    let lifecycle = json!({
        "eventType": "cli_session_exists",
        "metadata": {
            "sessionId": format!("sess_{}", &uuid::Uuid::new_v4().simple().to_string()[..16]),
            "cliVersion": version,
            "mode": "interactive",
            "os": format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
        }
    });
    let mut ok = true;
    for (path, body) in [
        (FINGERPRINT_PATH, &fingerprint),
        (LIFECYCLE_PATH, &lifecycle),
    ] {
        let url = match crate::protocol::upstream_url(base_url, path, None, "command_code") {
            Ok(url) => url,
            Err(error) => {
                tracing::warn!(%error, channel_id, "command code init url invalid");
                ok = false;
                continue;
            }
        };
        let payload = match serde_json::to_vec(body) {
            Ok(payload) => payload,
            Err(error) => {
                tracing::warn!(%error, channel_id, "command code init body invalid");
                ok = false;
                continue;
            }
        };
        let result = http
            .send(UpstreamRequest {
                url,
                headers: headers.clone(),
                method: http::Method::POST,
                body: Some(bytes::Bytes::from(payload)),
                connect_timeout: Duration::from_secs(10),
                deadline: Duration::from_secs(20),
            })
            .await;
        match result {
            Ok(response) if response.status.is_success() => {}
            Ok(response) => {
                ok = false;
                tracing::warn!(
                    channel_id,
                    status = response.status.as_u16(),
                    path,
                    "command code init request rejected"
                );
            }
            Err(error) => {
                ok = false;
                tracing::warn!(channel_id, ?error, path, "command code init request failed");
            }
        }
    }
    if ok {
        let jitter = rand::random_range(0..=INIT_JITTER_HOURS * 3600);
        write_json(
            db,
            &key,
            &json!({"next_init_at": now + init_interval_hours.max(1) * 3600 + jitter}),
        )
        .await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// transport router
// ---------------------------------------------------------------------------

pub async fn transport(db: &Database, channel_id: &str) -> Transport {
    read_json(db, &channel_key("transport", channel_id))
        .await
        .and_then(|value| value.get("transport").and_then(Value::as_str).map(Transport::parse))
        .unwrap_or(Transport::Unknown)
}

pub async fn set_transport(db: &Database, channel_id: &str, value: Transport) -> Result<()> {
    write_json(
        db,
        &channel_key("transport", channel_id),
        &json!({"transport": value.as_str()}),
    )
    .await
}

/// 403 + `error.code == "upgrade_required"`：Go 套餐无官方 API 访问的文档化信号。
pub fn is_upgrade_required(status: u16, body: &[u8]) -> bool {
    if status != 403 {
        return false;
    }
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return false;
    };
    let error = value.get("error").unwrap_or(&value);
    error.get("code").and_then(Value::as_str) == Some("upgrade_required")
}

/// 额度类 HTTP 状态：402（需付费）与 429（限流/额度窗口）。
pub fn is_quota_status(status: u16) -> bool {
    matches!(status, 402 | 429)
}

/// 从额度错误体中解析窗口重置时间（`resetAt` / `resetsAt` / `reset_at`，
/// ISO 字符串或秒级时间戳）。解析不到返回 `None`。
pub fn quota_reset_at(body: &[u8]) -> Option<chrono::DateTime<Utc>> {
    let value: Value = serde_json::from_slice(body).ok()?;
    let mut candidates: Vec<&Value> = Vec::new();
    fn walk<'a>(value: &'a Value, out: &mut Vec<&'a Value>, depth: usize) {
        if depth > 6 {
            return;
        }
        match value {
            Value::Object(map) => {
                for (key, item) in map {
                    if matches!(
                        key.as_str(),
                        "resetAt" | "resetsAt" | "reset_at" | "resetTime" | "windowResetsAt"
                    ) {
                        out.push(item);
                    }
                    walk(item, out, depth + 1);
                }
            }
            Value::Array(items) => {
                for item in items {
                    walk(item, out, depth + 1);
                }
            }
            _ => {}
        }
    }
    walk(&value, &mut candidates, 0);
    for candidate in candidates {
        if let Some(text) = candidate.as_str() {
            if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(text.trim()) {
                return Some(parsed.with_timezone(&Utc));
            }
            if let Ok(epoch) = text.trim().parse::<i64>() {
                return chrono::DateTime::from_timestamp(epoch, 0);
            }
        }
        if let Some(epoch) = candidate.as_i64() {
            return chrono::DateTime::from_timestamp(epoch, 0);
        }
    }
    None
}

/// 额度耗尽的账号轮换（计划决策 3）：直接写 `channel_health`，
/// 窗口重置后由既有健康探测自动放回；无需账号池调度器、无需新表。
pub async fn mark_quota_exhausted(
    db: &Database,
    channel_id: &str,
    reset_at: Option<chrono::DateTime<Utc>>,
    fallback_seconds: i64,
    status: u16,
) -> Result<()> {
    let now = Utc::now();
    let until = reset_at
        .filter(|value| *value > now)
        .unwrap_or_else(|| now + chrono::Duration::seconds(fallback_seconds.max(1)));
    sqlx::query(
        "UPDATE channel_health SET state='open', last_failure_at=?, last_error_kind='quota_exhausted', \
         last_status_code=?, disabled_until=?, updated_at=? WHERE channel_id=?",
    )
    .bind(now.to_rfc3339())
    .bind(status as i64)
    .bind(until.to_rfc3339())
    .bind(now.to_rfc3339())
    .bind(channel_id)
    .execute(db.pool())
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// 版本漂移探测（npm latest 对照）
// ---------------------------------------------------------------------------

/// 从 npm registry 取 `latest` 版本并持久化；失败保留当前值。
/// 维护循环按 `command_code_version_check_interval` 调用（功能开启时才探测）。
pub async fn refresh_cli_version(
    db: &Database,
    http: &dyn UpstreamClient,
) -> Result<Option<String>> {
    let url = url::Url::parse(&format!(
        "https://registry.npmjs.org/{NPM_PACKAGE}/latest"
    ))?;
    let response = http
        .send(UpstreamRequest {
            url,
            headers: axum::http::HeaderMap::new(),
            method: http::Method::GET,
            body: None,
            connect_timeout: Duration::from_secs(10),
            deadline: Duration::from_secs(20),
        })
        .await
        .map_err(|error| anyhow::anyhow!("npm registry transport: {error:?}"))?;
    if !response.status.is_success() {
        anyhow::bail!("npm registry returned {}", response.status.as_u16());
    }
    let (body, _truncated) = response.body.read_capped(1024 * 1024).await;
    let value: Value = serde_json::from_slice(&body).context("npm registry response")?;
    let version = value
        .get("version")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("npm registry response has no version"))?;
    write_json(db, "command_code_cli_version", &json!(version)).await?;
    write_json(
        db,
        "command_code_cli_version_checked_at",
        &json!(Utc::now().to_rfc3339()),
    )
    .await?;
    Ok(Some(version.to_owned()))
}

/// 版本漂移：探测到的 CLI 版本已偏离协议夹具基准（`DEFAULT_CLI_VERSION`）
/// 即视为线协议可能已静默变更，UI 应提示。纯函数便于测试。
pub fn version_drift(version: &str) -> bool {
    !version.trim().is_empty() && version.trim() != DEFAULT_CLI_VERSION
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ports::{UpstreamBody, UpstreamClient, UpstreamError, UpstreamResponse};
    use futures_util::future::BoxFuture;

    #[derive(Default)]
    struct FakeHttp {
        calls: parking_lot::Mutex<Vec<(String, Option<String>)>>,
        npm_body: parking_lot::Mutex<Option<String>>,
    }

    impl FakeHttp {
        fn calls(&self) -> Vec<(String, Option<String>)> {
            self.calls.lock().clone()
        }
    }

    impl UpstreamClient for FakeHttp {
        fn send(
            &self,
            request: UpstreamRequest,
        ) -> BoxFuture<'static, Result<UpstreamResponse, UpstreamError>> {
            let url = request.url.to_string();
            let body = request
                .body
                .as_ref()
                .map(|body| String::from_utf8_lossy(body).into_owned());
            self.calls.lock().push((url.clone(), body));
            let payload = if url.contains("registry.npmjs.org") {
                self.npm_body
                    .lock()
                    .clone()
                    .unwrap_or_else(|| "{}".into())
            } else {
                "{}".into()
            };
            Box::pin(async move {
                Ok(UpstreamResponse {
                    status: http::StatusCode::OK,
                    headers: http::HeaderMap::new(),
                    body: UpstreamBody::new(Box::pin(futures_util::stream::iter(vec![Ok(
                        bytes::Bytes::from(payload),
                    )]))),
                })
            })
        }
    }

    async fn test_db() -> (Database, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("lagw-cc-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = Database::open(&dir.join("test.db")).await.unwrap();
        (db, dir)
    }

    async fn seed_channel(db: &Database, id: &str) {
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("INSERT INTO providers(id,name,base_url,created_at,updated_at) VALUES('prov-1','cc','https://api.commandcode.ai',?,?)")
            .bind(time)
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channels(id,provider_id,name,protocol,api_key_encrypted,api_key_hint,manual_enabled,created_at,updated_at) VALUES(?,'prov-1','chan','command_code',x'00','',1,?,?)")
            .bind(id)
            .bind(time)
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_health(channel_id,state,consecutive_failures,updated_at) VALUES(?,'active',0,?)")
            .bind(id)
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
    }

    #[test]
    fn fingerprint_structure_and_thumbmark_match() {
        let value = generate_fingerprint();
        let components = value.get("components").unwrap();
        let thumb = value.get("thumbmark").unwrap().as_str().unwrap();
        assert_eq!(thumb.len(), 64);
        let platform = components.get("platform").unwrap().as_str().unwrap();
        let os_release = components.get("osRelease").unwrap().as_str().unwrap();
        let cpu = components.get("cpuModel").unwrap().as_str().unwrap();
        let cores = components.get("cpuCount").unwrap().as_i64().unwrap();
        let mem = components.get("memGiB").unwrap().as_i64().unwrap();
        let macs: Vec<&str> = components
            .get("macHashes")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item.as_str().unwrap())
            .collect();
        let expected = format!(
            "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
            components.get("machineIdHash").unwrap().as_str().unwrap(),
            macs.join("|"),
            components.get("osUserHash").unwrap().as_str().unwrap(),
            components.get("hostnameHash").unwrap().as_str().unwrap(),
            components.get("gitEmailHash").unwrap().as_str().unwrap(),
            platform,
            os_release,
            cpu,
            cores,
            mem
        );
        assert_eq!(sha256_hex(&expected), thumb);
    }

    #[test]
    fn upgrade_required_only_matches_documented_shape() {
        assert!(is_upgrade_required(
            403,
            br#"{"error":{"code":"upgrade_required","message":"upgrade"}}"#
        ));
        assert!(!is_upgrade_required(
            403,
            br#"{"error":{"code":"forbidden"}}"#
        ));
        assert!(!is_upgrade_required(401, br#"{"error":{"code":"upgrade_required"}}"#));
        assert!(!is_upgrade_required(403, b"not json"));
    }

    #[test]
    fn quota_reset_accepts_iso_and_epoch() {
        let iso = quota_reset_at(br#"{"error":{"details":{"resetAt":"2026-09-12T10:00:00Z"}}}"#)
            .unwrap();
        assert_eq!(iso.timestamp(), 1789207200);
        let epoch = quota_reset_at(br#"{"fiveHour":{"resetAt":1789207200}}"#).unwrap();
        assert_eq!(epoch.timestamp(), 1789207200);
        assert!(quota_reset_at(br#"{"error":{"message":"nope"}}"#).is_none());
    }

    #[test]
    fn project_slug_is_stable_and_not_the_session_id() {
        let slug = protocol::derive_project_slug("sess_0123456789abcdef");
        assert_eq!(slug, protocol::derive_project_slug("sess_0123456789abcdef"));
        assert!(!slug.contains("0123456789abcdef"));
        let other = protocol::derive_project_slug("not-hex-session-value");
        assert!(other.starts_with("ws-"));
        assert_eq!(other, protocol::derive_project_slug("not-hex-session-value"));
    }

    #[test]
    fn transport_round_trips() {
        for value in [Transport::Unknown, Transport::Provider, Transport::Generate] {
            assert_eq!(Transport::parse(value.as_str()), value);
        }
        assert_eq!(Transport::parse("bogus"), Transport::Unknown);
    }


    #[test]
    fn bundled_go_catalog_is_well_formed() {
        let items = bundled_catalog_items();
        assert!(
            items.len() >= 40,
            "expected the bundled Go catalog, got {} entries",
            items.len()
        );
        let ids: std::collections::HashSet<&str> = items
            .iter()
            .filter_map(|item| item.get("id").and_then(Value::as_str))
            .collect();
        assert_eq!(ids.len(), items.len(), "model ids must be unique");
        assert!(ids.contains("deepseek/deepseek-v4-flash"));
        assert!(ids.contains("z-ai/glm-5.3-flash"));
        // Pro/Max/GOAT-only models must never leak into the Go fallback.
        assert!(!ids.contains("claude-opus-5"));
        assert!(!ids.contains("gpt-6-astra"));
        assert!(!ids.contains("gpt-5.6-sol"));
        for item in &items {
            assert!(
                item.get("display_name")
                    .and_then(Value::as_str)
                    .is_some_and(|value| !value.is_empty())
            );
            assert_eq!(
                item.get("command_code_bundled").and_then(Value::as_bool),
                Some(true)
            );
        }
    }

    #[test]
    fn version_drift_only_off_baseline() {
        assert!(!version_drift(DEFAULT_CLI_VERSION));
        assert!(!version_drift("  1.53.1  "));
        assert!(version_drift("1.54.0"));
        assert!(!version_drift(""));
    }
    #[tokio::test]
    async fn fingerprint_session_and_transport_are_stable_per_channel() {
        let (db, _dir) = test_db().await;
        let first = ensure_fingerprint(&db, "ch-a").await.unwrap();
        let again = ensure_fingerprint(&db, "ch-a").await.unwrap();
        assert_eq!(
            first["thumbmark"], again["thumbmark"],
            "the same channel keeps one fingerprint"
        );
        let other = ensure_fingerprint(&db, "ch-b").await.unwrap();
        assert_ne!(
            first["thumbmark"], other["thumbmark"],
            "each channel (API key) has its own fingerprint"
        );

        let session = session_id(&db, "ch-a").await.unwrap();
        assert!(session.starts_with("sess_"));
        assert_eq!(session, session_id(&db, "ch-a").await.unwrap());
        assert_ne!(session, session_id(&db, "ch-b").await.unwrap());

        let identity = identity(&db, "ch-a").await.unwrap();
        assert_eq!(identity.session_id, session);
        assert_eq!(
            identity.project_slug,
            crate::protocol::derive_project_slug(&session)
        );
        assert_eq!(identity.cli_version, DEFAULT_CLI_VERSION);
        assert!(!identity.zdr);

        assert_eq!(transport(&db, "ch-a").await, Transport::Unknown);
        set_transport(&db, "ch-a", Transport::Generate).await.unwrap();
        assert_eq!(transport(&db, "ch-a").await, Transport::Generate);
        assert_eq!(
            transport(&db, "ch-b").await,
            Transport::Unknown,
            "transport memory is per channel"
        );
    }

    #[tokio::test]
    async fn ensure_initialized_records_fingerprint_lifecycle_and_throttles() {
        let (db, _dir) = test_db().await;
        let http = FakeHttp::default();
        ensure_initialized(&db, &http, "ch-a", "user_test", DEFAULT_API_BASE, 8)
            .await
            .unwrap();
        let calls = http.calls();
        assert_eq!(calls.len(), 2, "fingerprint + lifecycle");
        let fingerprint = calls
            .iter()
            .find(|(url, _)| url.ends_with("/alpha/fingerprint/record"))
            .expect("fingerprint recorded");
        let fingerprint_body = fingerprint.1.as_deref().unwrap_or_default();
        assert!(fingerprint_body.contains("\"thumbmark\""));
        assert!(fingerprint_body.contains("\"components\""));
        let lifecycle = calls
            .iter()
            .find(|(url, _)| url.ends_with("/alpha/lifecycle-events"))
            .expect("lifecycle sent");
        let lifecycle_body = lifecycle.1.as_deref().unwrap_or_default();
        assert!(lifecycle_body.contains("cli_session_exists"));
        assert!(lifecycle_body.contains(DEFAULT_CLI_VERSION));

        // Throttled per channel: the next call performs zero requests.
        ensure_initialized(&db, &http, "ch-a", "user_test", DEFAULT_API_BASE, 8)
            .await
            .unwrap();
        assert_eq!(http.calls().len(), 2, "init is throttled per channel");
        // A different channel initializes independently.
        ensure_initialized(&db, &http, "ch-b", "user_other", DEFAULT_API_BASE, 8)
            .await
            .unwrap();
        assert_eq!(http.calls().len(), 4);
    }

    #[tokio::test]
    async fn refresh_cli_version_stores_npm_latest_and_reports_drift() {
        let (db, _dir) = test_db().await;
        let http = FakeHttp::default();
        assert_eq!(cli_version(&db).await, DEFAULT_CLI_VERSION);
        *http.npm_body.lock() = Some(r#"{"name":"command-code","version":"1.99.0"}"#.into());
        let version = refresh_cli_version(&db, &http).await.unwrap();
        assert_eq!(version.as_deref(), Some("1.99.0"));
        assert_eq!(cli_version(&db).await, "1.99.0");
        assert!(version_drift(&cli_version(&db).await));
        assert!(
            read_json(&db, "command_code_cli_version_checked_at")
                .await
                .is_some()
        );
        // A non-JSON registry answer fails soft: the stored version stays.
        *http.npm_body.lock() = Some("not json".into());
        assert!(refresh_cli_version(&db, &http).await.is_err());
        assert_eq!(cli_version(&db).await, "1.99.0");
    }

    #[tokio::test]
    async fn mark_quota_exhausted_opens_channel_until_reset() {
        let (db, _dir) = test_db().await;
        seed_channel(&db, "ch-1").await;
        let reset = chrono::DateTime::parse_from_rfc3339("2099-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        mark_quota_exhausted(&db, "ch-1", Some(reset), 900, 429)
            .await
            .unwrap();
        let (state, until, kind, status): (String, Option<String>, Option<String>, Option<i64>) =
            sqlx::query_as(
                "SELECT state, disabled_until, last_error_kind, last_status_code \
                 FROM channel_health WHERE channel_id='ch-1'",
            )
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(state, "open");
        assert!(until.unwrap_or_default().starts_with("2099-01-01"));
        assert_eq!(kind.as_deref(), Some("quota_exhausted"));
        assert_eq!(status, Some(429));

        // Without a parsable reset the circuit window is the fallback.
        mark_quota_exhausted(&db, "ch-1", None, 60, 402)
            .await
            .unwrap();
        let (until, kind): (Option<String>, Option<String>) = sqlx::query_as(
            "SELECT disabled_until, last_error_kind FROM channel_health WHERE channel_id='ch-1'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        let until = until.unwrap_or_default();
        assert!(!until.starts_with("2099-01-01"), "fallback must replace the reset");
        assert_eq!(kind.as_deref(), Some("quota_exhausted"));
    }

}
