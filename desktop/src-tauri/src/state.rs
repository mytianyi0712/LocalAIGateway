//! 组合根状态：`Context`（端口与值类型）+ 具体服务实例。
//!
//! 具体服务放在这里而不是 `Context`，是为了让业务服务只依赖 `Context`、
//! 不反向依赖组合根（否则 application ↔ 服务会成环）。
//! `Deref<Target = Context>` 让 handler 里 `state.db` / `state.admin` 的写法不变，
//! 也让接收 `&Context` 的服务方法继续接收 `&AppState`（自动降级）。

use std::sync::Arc;

use crate::{
    admin::AdminService, application::Context, auth::RecoverySession, balance::BalanceService,
    commandcode_login::CommandCodeLogin, discovery::DiscoveryService, proxy::ProxyService,
};

#[derive(Clone)]
pub struct AppState {
    pub ctx: Context,
    pub proxy: Arc<ProxyService>,
    pub admin: Arc<AdminService>,
    pub balance: Arc<BalanceService>,
    pub discovery: Arc<DiscoveryService>,
    pub command_code_login: Arc<CommandCodeLogin>,
    pub recovery: Arc<RecoverySession>,
}

impl std::ops::Deref for AppState {
    type Target = Context;

    fn deref(&self) -> &Context {
        &self.ctx
    }
}
