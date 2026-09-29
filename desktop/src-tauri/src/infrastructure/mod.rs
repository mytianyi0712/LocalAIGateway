//! 基础设施层：端口的生产实现（HTTP 客户端池、SQLite repository、系统时钟、
//! 设置与 Command Code 状态存储）以及后台任务监管者。
//!
//! 业务模块不直接依赖这里的类型，而是经 `crate::application::Context` 拿到
//! `ports` 中的 trait 对象。边界：本模块只负责 I/O、持久化与任务生命周期，
//! 不做业务判定；`channel_health` 的唯一写入口在 `crate::health`。
//!
//! 文件划分：`http`（上游客户端）、`repositories`（持久化与状态存储）、
//! `supervisor`（后台任务监管）。

mod http;
mod repositories;
mod supervisor;

pub use http::HttpClientPool;
pub use repositories::{
    CommandCodeStore, SettingsStore, SqliteChannelRepository, SqliteRouteRepository, SystemClock,
};
pub use supervisor::{RuntimeSupervisor, ShuttingDown, TaskOutcome};
