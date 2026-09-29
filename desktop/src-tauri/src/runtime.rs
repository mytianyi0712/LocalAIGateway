//! 内部安全边界与调度间隔：决定资源占用、关闭流程与外部 I/O 行为的常量。
//!
//! 历史上这些常量散落在各模块且互有冲突；现在启动时构建唯一一份不可变
//! [`RuntimeLimits`] 快照，由所有 supervisor 共享，因此调整某个上限只需改一处。
//!
//! 边界：用户可配置的上游超时在 [`crate::settings::RuntimeSettings`]，
//! 本模块只负责固定的运维层；测试注入短值而非依赖生产常量。

use std::time::Duration;

/// 不可变的运维上限，每个运行时只构建一次。
#[derive(Debug, Clone, Copy)]
pub struct RuntimeLimits {
    /// 一次性任务回收轮的轮询间隔。
    pub reaper_interval: Duration,
    /// 单次健康探测的硬超时。
    pub probe_timeout: Duration,
    /// 健康 supervisor 的轮询间隔。
    pub probe_interval: Duration,
    /// 单次发现运行的硬超时。
    pub discovery_timeout: Duration,
    /// 发现分页上限（页数）。
    pub discovery_max_pages: usize,
    /// 维护 supervisor 的节拍。
    pub maintenance_interval: Duration,
    /// 维护循环内日志清理的节拍。
    pub cleanup_interval: Duration,
    /// 单次渠道余额查询的绝对超时（上游往返加 body 读取）；余额查询是旁路，
    /// 绝不能长时间占用连接。
    pub balance_timeout: Duration,
    /// 后台刷新启用余额查询的渠道的节拍。测试注入短值。
    pub balance_interval: Duration,
    /// 非 2xx 上游错误体回放的上限。
    pub error_body_max: usize,
    /// 绝对关闭期限：取消后，用这么久排空所有任务；仍在运行的会被中止并 join。
    pub shutdown_deadline: Duration,
}

impl Default for RuntimeLimits {
    fn default() -> Self {
        Self {
            reaper_interval: Duration::from_millis(500),
            probe_timeout: Duration::from_secs(20),
            probe_interval: Duration::from_secs(5),
            discovery_timeout: Duration::from_secs(120),
            discovery_max_pages: 50,
            maintenance_interval: Duration::from_secs(60),
            cleanup_interval: Duration::from_secs(3600),
            balance_timeout: Duration::from_secs(15),
            balance_interval: Duration::from_secs(3600),
            error_body_max: 1024 * 1024,
            shutdown_deadline: Duration::from_secs(30),
        }
    }
}
