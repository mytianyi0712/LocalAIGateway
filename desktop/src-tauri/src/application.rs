//! 应用层：承载共享的请求/运行时上下文（`Context`）。
//!
//! `Context` 只承载端口与不可变值：配置、数据库、密钥库、上游端口、遥测、
//! 后台监管、路由/渠道/时钟/通知端口与运行限额。具体服务实例
//! （`proxy` / `admin` / `balance` / `discovery` / `command_code_login` /
//! `recovery`）在 `state::AppState`。两者分开是为了让业务服务只依赖 `Context`
//! 这一组端口，而不反向依赖组合根——否则 application ↔ 服务会成环。
//!
//! `RuntimeSupervisor` 与 HTTP 客户端池位于 `infrastructure`，此处经
//! `crate::infrastructure` 导入使用。

use std::sync::Arc;

use crate::{
    config::AppConfig,
    crypto::SecretStore,
    db::Database,
    infrastructure::RuntimeSupervisor,
    ports::{ChannelRepository, Clock, RouteRepository, UpstreamClient},
    runtime::RuntimeLimits,
    telemetry::Telemetry,
};

/// 交给每个 handler 的共享状态。可克隆：每个任务克隆一份上下文并在结束时丢弃，
/// 这也是关闭期间 telemetry 发送端被释放的方式。
#[derive(Clone)]
pub struct Context {
    pub config: Arc<AppConfig>,
    pub db: Database,
    pub secrets: SecretStore,
    /// 上游 HTTP 端口；reqwest 连接池位于 infrastructure。
    pub http: Arc<dyn UpstreamClient>,
    pub telemetry: Telemetry,
    pub background: Arc<RuntimeSupervisor>,
    /// 模型路由端口。
    pub routes: Arc<dyn RouteRepository>,
    /// 渠道端口。
    pub channels: Arc<dyn ChannelRepository>,
    /// 时间端口。
    pub clock: Arc<dyn Clock>,
    /// 桌面通知端口：合并后的故障转移告警。
    pub notifier: Arc<dyn crate::ports::Notifier>,
    /// 各 supervisor 共享的不可变运行限额。
    pub limits: Arc<RuntimeLimits>,
}
