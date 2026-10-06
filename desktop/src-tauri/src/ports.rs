//! 应用端口层：业务逻辑与基础设施之间的接缝。
//!
//! 端口是业务逻辑与基础设施的接缝；`Context` 同时暴露 `db` / `secrets` /
//! `telemetry` 等具体值类型（历史约定），端口化是方向而非既成事实。
//! SQLx / reqwest 等实现位于 `infrastructure`，经
//! [`crate::application::Context`] 注入。
//! 业务模块不得直接 `use axum|sqlx|reqwest`，也不得伸手进具体 repository：
//! `scripts/check-layers.sh` 只对 `src/application*`、`src/domain`、`src/ports*`
//! 这三类路径检查该 import 规则，另有 14 个 admin 文件（13 个子模块 +
//! `admin/mod.rs`）的正文检查（handler 体内不得出现 `sqlx::` 或
//! `state.db.pool()`）。

use std::pin::Pin;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::{Stream, StreamExt};
use http::{HeaderMap, StatusCode};
use url::Url;

use crate::domain::{Candidate, CompactionMode, Event, RoutableModel};

/// 一次上游 HTTP 交换的抽象，业务逻辑因此永远看不到 reqwest。
pub struct UpstreamRequest {
    pub url: Url,
    pub headers: HeaderMap,
    /// 显式 HTTP 方法。既有调用方无请求体时用 GET、有请求体时用 POST；
    /// balance sidecar 是首个需要 PUT 的调用方（自定义适配器），
    /// 因此端口直接携带方法，而不是从 `body` 推断。
    pub method: http::Method,
    /// `Some` 表示发送请求体，`None` 表示不发送。
    pub body: Option<Bytes>,
    /// 本次交换的客户端连接超时。
    pub connect_timeout: Duration,
    /// 发送阶段（到响应头为止）的绝对截止时间。
    pub deadline: Duration,
}

/// 上游交换在拿到可用状态码之前就失败的原因。
#[derive(Debug, Clone)]
pub enum UpstreamError {
    /// 发送阶段超过了 [`UpstreamRequest::deadline`]。
    Deadline,
    /// 底层客户端把该失败归类为连接超时。
    ConnectTimeout,
    /// 其它传输失败（连接被拒、DNS、连接重置）。
    Transport(String),
}

/// 响应头已到达时的上游响应。响应体是字节流：有界读取由调用方负责。
pub struct UpstreamResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: UpstreamBody,
}

/// 上游响应体的流。每一项是一个网络分块，或响应体中途的传输失败。
pub struct UpstreamBody {
    inner: Pin<Box<dyn Stream<Item = Result<Bytes, UpstreamError>> + Send>>,
}

impl UpstreamBody {
    pub fn new(inner: Pin<Box<dyn Stream<Item = Result<Bytes, UpstreamError>> + Send>>) -> Self {
        Self { inner }
    }

    pub fn into_stream(self) -> Pin<Box<dyn Stream<Item = Result<Bytes, UpstreamError>> + Send>> {
        self.inner
    }

    /// 读取整个响应体，到 `cap` 字节为止（尾部随流一起丢弃）。
    /// 返回 `(bytes, truncated)`。
    pub async fn read_capped(mut self, cap: usize) -> (Vec<u8>, bool) {
        let mut buf = Vec::with_capacity(cap.min(64 * 1024));
        let mut truncated = false;
        while let Some(item) = self.inner.next().await {
            match item {
                Ok(chunk) => {
                    let remaining = cap - buf.len();
                    if chunk.len() > remaining {
                        buf.extend_from_slice(&chunk[..remaining]);
                        truncated = true;
                        break;
                    }
                    buf.extend_from_slice(&chunk);
                }
                Err(_) => break,
            }
        }
        (buf, truncated)
    }
}

/// 上游 HTTP 端口：proxy、健康探测与 discovery 一律经此访问上游。
pub trait UpstreamClient: Send + Sync {
    fn send(
        &self,
        request: UpstreamRequest,
    ) -> BoxFuture<'static, Result<UpstreamResponse, UpstreamError>>;
}

/// 遥测/事件端口：fire-and-forget 的领域事件。入队与落库实现是 `telemetry::Telemetry`。
pub trait EventSink: Send + Sync {
    fn emit(&self, event: Event);
}

/// 运行时设置与网关鉴权读取端口：代理路径因此不再直连 `Database`。
/// 实现是 `infrastructure::SettingsStore`（settings 表 + 密钥库）。
pub trait SettingsReader: Send + Sync {
    /// 运行时设置；损坏的行 fail-closed（返回错误而不是默认值）。
    fn runtime_settings(&self) -> BoxFuture<'static, Result<crate::settings::RuntimeSettings>>;

    /// OpenCode Zen/Go 的会话头值；不可用时退化为临时 id（不返回错误）。
    fn opencode_session_id(&self) -> BoxFuture<'static, String>;

    /// 网关入口鉴权：访问策略由 settings 与密钥库共同决定。
    fn authorize_gateway<'a>(
        &'a self,
        headers: &'a http::HeaderMap,
        query: Option<&'a str>,
        protocol: &'a str,
    ) -> BoxFuture<'a, Result<bool>>;
}

/// Command Code 状态端口：传输模式、身份头、初始化节流与配额冷却。
/// 实现是 `infrastructure::CommandCodeStore`（薄封装 `commandcode` 的状态函数）。
pub trait CommandCodeState: Send + Sync {
    /// 该渠道记住的传输模式；读取失败退化为 `Provider`（与既有实现一致）。
    fn transport(&self, channel_id: &str) -> BoxFuture<'static, crate::commandcode::Transport>;

    /// 记住传输模式（升级探测/回退时写库）。
    fn set_transport(
        &self,
        channel_id: &str,
        transport: crate::commandcode::Transport,
    ) -> BoxFuture<'static, Result<()>>;

    /// 该渠道的 CLI 身份头（session/fingerprint/version）。
    fn identity(
        &self,
        channel_id: &str,
    ) -> BoxFuture<'static, Result<crate::protocol::CommandCodeIdentity>>;

    /// 按渠道节流的指纹/生命周期上报；失败不阻断业务请求。
    fn ensure_initialized(
        &self,
        channel_id: &str,
        api_key: &str,
        base_url: &str,
        interval_hours: i64,
    ) -> BoxFuture<'static, Result<()>>;

    /// 配额耗尽：开断渠道并记录重置时间（`HealthEvent::QuotaExhausted`）。
    fn mark_quota_exhausted(
        &self,
        channel_id: &str,
        reset_at: Option<chrono::DateTime<chrono::Utc>>,
        open_seconds: i64,
        status: u16,
    ) -> BoxFuture<'static, Result<()>>;
}

/// 时间端口：让服务不必硬依赖 chrono 就能取时间戳；测试可替换为固定时钟。
pub trait Clock: Send + Sync {
    fn now_utc(&self) -> chrono::DateTime<chrono::Utc>;
}

/// 模型路由持久化端口：候选解析与可路由端点列表。SQLite 实现是 `infrastructure`。
pub trait RouteRepository: Send + Sync {
    /// (protocol, model) 下有序的合格候选，最多 `max_attempts` 个
    /// （proxy 的故障转移预算）。
    fn resolve_candidates(
        &self,
        protocol: &str,
        model: &str,
        max_attempts: i64,
    ) -> BoxFuture<'static, Result<Vec<Candidate>>>;

    /// Codex 远程压缩请求下有序的合格候选。
    /// `mode` 已知不支持的候选会在应用尝试上限之前被排除。
    fn resolve_compaction_candidates(
        &self,
        protocol: &str,
        model: &str,
        mode: CompactionMode,
        max_attempts: i64,
    ) -> BoxFuture<'static, Result<Vec<Candidate>>>;

    /// 当前真正可调用的模型（已路由、启用、健康），供模型目录端点使用。
    fn list_routable_models(
        &self,
        protocol: Option<&str>,
    ) -> BoxFuture<'static, Result<Vec<RoutableModel>>>;

    /// 记录一次运行时确认的远程压缩能力探测结果（`channel_protocols`）。
    ///
    /// 写入失败只记录告警：这是一次“顺带”的能力标注，失败不该影响本次请求。
    fn set_compaction_support(
        &self,
        channel_id: &str,
        mode: CompactionMode,
        supported: bool,
    ) -> BoxFuture<'static, Result<()>>;

    /// 模型当前可被路由经过的主要入口协议。
    fn routable_endpoints_for_model(
        &self,
        model_id: &str,
    ) -> BoxFuture<'static, Result<Vec<String>>>;
}

/// 后台服务（健康探测、discovery）所需的渠道行快照：
/// 凭据保持加密，解密发生在服务层。
#[derive(Debug, Clone)]
pub struct ChannelRow {
    pub id: String,
    pub name: String,
    pub protocol: String,
    /// `providers.kind`：身份标记（如 `command_code`）。
    pub kind: Option<String>,
    pub api_key_encrypted: Vec<u8>,
    pub base_url: String,
    pub manual_enabled: bool,
    pub health_check_model_id: Option<String>,
}

/// 渠道持久化端口：探测、discovery 与 admin 守卫背后的行加载。SQLite 实现是
/// `infrastructure`。
pub trait ChannelRepository: Send + Sync {
    fn load_channel(&self, channel_id: &str) -> BoxFuture<'static, Result<Option<ChannelRow>>>;

    /// 渠道是否存在（存在性守卫；不加载整行）。
    fn exists(&self, channel_id: &str) -> BoxFuture<'static, Result<bool>>;

    /// 渠道所属提供商的 `providers.kind`（身份标记，如 `command_code`）；
    /// 渠道不存在时为 `None`。
    fn provider_kind(&self, channel_id: &str) -> BoxFuture<'static, Result<Option<String>>>;

    /// 渠道绑定的协议清单（`channel_protocols`），按插入顺序（`rowid`）返回。
    fn protocols(&self, channel_id: &str) -> BoxFuture<'static, Result<Vec<String>>>;
}

/// 一次故障转移事件：`failed_channel_name` 上的一次请求尝试被转交给
/// `next_channel_name`。`error_kind` 是简短的稳定标签
/// （如 "connect_timeout"、"transport_error"、"HTTP 500" 等），
/// 失败无分类时为 None。
#[derive(Debug, Clone)]
pub struct FailoverNotice {
    pub model_id: String,
    pub failed_channel_name: String,
    pub next_channel_name: String,
    pub error_kind: Option<String>,
}

/// 桌面通知端口：fire-and-forget 的用户提醒。
/// 实现绝不能阻塞调用方：投递是异步的（排入后台 worker），
/// 没有通知守护进程的环境静默降级。
pub trait Notifier: Send + Sync {
    fn notify_failover(&self, notice: FailoverNotice);
}
