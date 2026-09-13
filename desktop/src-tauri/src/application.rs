//! Application layer: the shared request/runtime context (P2-2).
//!
//! `Context` used to be `server::AppState` — a service locator defined by
//! the composition root and depended on by every business module, creating
//! a crate-level cycle (`server -> admin/proxy -> server::AppState`). The
//! context now lives here: business modules depend on `application`, and
//! `server` only assembles it. The next step (per the architecture report)
//! replaces this wide context with narrow per-handler services.
//!
//! `HttpClients` and `RuntimeSupervisor` are still defined in `server` and
//! imported here for now; they move to `infrastructure` when the split
//! continues.

use std::sync::Arc;

use crate::{
    auth::RecoverySession,
    config::AppConfig,
    crypto::SecretStore,
    db::Database,
    infrastructure::RuntimeSupervisor,
    ports::{ChannelRepository, Clock, RouteRepository, UpstreamClient},
    runtime::RuntimeLimits,
    telemetry::Telemetry,
};

/// Shared state handed to every handler. Clonable: each task clones the
/// context and drops it when it finishes, which is also how the telemetry
/// sender gets released during shutdown.
#[derive(Clone)]
pub struct Context {
    pub config: Arc<AppConfig>,
    pub db: Database,
    pub secrets: SecretStore,
    /// Upstream HTTP port (P2-1); the reqwest pool lives in infrastructure.
    pub http: Arc<dyn UpstreamClient>,
    pub telemetry: Telemetry,
    pub background: Arc<RuntimeSupervisor>,
    /// Model-route port (P2-1).
    pub routes: Arc<dyn RouteRepository>,
    /// Channel port (P2-1).
    pub channels: Arc<dyn ChannelRepository>,
    /// Time port (P2-1).
    pub clock: Arc<dyn Clock>,
    /// Desktop-notification port (P2-1): coalesced failover alerts.
    pub notifier: Arc<dyn crate::ports::Notifier>,
    /// Discovery service (P2-1).
    pub discovery: Arc<crate::discovery::DiscoveryService>,
    /// Proxy service (P2-1): request orchestration entry for the proxy
    /// handlers. Constructed after the context, as it holds a clone.
    pub proxy: Arc<crate::proxy::ProxyService>,
    /// Admin service (P2-1): provider/channel domain operations; other
    /// subdomains migrate progressively. Also constructed after the
    /// context.
    pub admin: Arc<crate::admin::AdminService>,
    /// Channel balance/usage sidecar: default-off per-channel adapter
    /// queries plus the hourly background refresh.
    pub balance: Arc<crate::balance::BalanceService>,
    /// Immutable operational limits shared by every supervisor (P2-10).
    pub limits: Arc<RuntimeLimits>,
    /// One-time challenge store for corrupt-key recovery (P1-4).
    pub recovery: Arc<RecoverySession>,
    /// Command Code 网页登录授权流程（等价 `cmd login` 的 loopback 回调；
    /// 每 Context 一份，密钥只在服务端内存与加密库之间流转）。
    pub command_code_login: Arc<crate::commandcode_login::CommandCodeLogin>,
}
