//! 无 GUI 的网关入口（headless）：绑定监听、组装运行时、注册后台任务，
//! 并在 Ctrl-C 或 serve 结束时做一次有界排空。
//!
//! 边界：本文件只做进程级编排（信号 → 取消令牌 → 排空），
//! 不含任何业务逻辑；配置与数据目录由 `AppConfig` 决定。
//! 关键不变量：serve 返回后必须先 `shutdown` 再退出——不留任何游离任务
//! （后台任务全部登记在 `RuntimeSupervisor` 的 JoinSet 里）。

use anyhow::Result;
use local_ai_gateway::{config::AppConfig, server};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let config = AppConfig::load(std::path::PathBuf::from("data")).await?;
    let listener = server::bind(&config).await?;
    let server::GatewayRuntime {
        state,
        router,
        telemetry_rx,
        supervisor,
    } = server::build(config.clone()).await?;
    tracing::info!(url = %config.local_url(), "gateway ready");
    // Ctrl-C 取消运行时令牌：serve 与所有后台任务都观察它，从而实现优雅退出。
    let signal = supervisor.cancel.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        signal.cancel();
    });
    let limits = *state.limits;
    let serve_cancel = supervisor.cancel.clone();
    server::spawn_background(&supervisor, state, telemetry_rx, &limits).await;
    // serve 一直运行到令牌被取消（Ctrl-C）或自身失败。返回后做一次有界排空，
    // 停掉全部后台任务：不留下任何能活过本进程的游离句柄。
    let result = server::serve(listener, router, serve_cancel).await;
    supervisor.shutdown(limits.shutdown_deadline).await;
    result
}
