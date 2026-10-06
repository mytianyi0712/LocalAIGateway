//! 网关的代理流水线：入口 handler 经 `ProxyService::proxy` 进入，
//! 完成设置读取、鉴权、候选路由与上游转发。
//!
//! 边界：上游、路由、设置与 Command Code 状态都经 `crate::ports` 端口访问；
//! 正文转换委托 `convert`，压缩解码委托 `compression`，协议差异委托 `protocol`
//! ——本模块只做编排。
//! 关键不变量：每次候选尝试恰好终结一次遥测（[`attempt::AttemptFinalizer`]）；
//! 客户端中途断开不得把已完成的流降级为零值 `cancelled`。
//!
//! 文件划分：`service`（服务与入口 handler）、`prepare`（请求准备）、
//! `attempt`（尝试记账）、`stream` / `nonstream`（两条响应路径）、
//! `compaction`（远程压缩）、`commandcode`（Command Code 专用）、
//! `catalog`（目录/信息端点）、`error`（网关错误形状）。

mod attempt;
mod catalog;
mod commandcode;
mod compaction;
mod error;
mod nonstream;
mod prepare;
mod service;
mod stream;

#[cfg(test)]
mod tests;

pub use catalog::{
    claude_models, gemini_models, openai_models, responses_models,
};
pub use service::{
    ProxyService, ProxyServiceDeps, claude, gemini, openai, responses, responses_compact,
};
