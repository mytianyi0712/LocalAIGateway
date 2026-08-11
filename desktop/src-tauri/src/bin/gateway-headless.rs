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
    // Ctrl-C cancels the runtime token; serve observes it for graceful
    // shutdown.
    let signal = supervisor.cancel.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        signal.cancel();
    });
    let limits = *state.limits;
    let serve_cancel = supervisor.cancel.clone();
    server::spawn_background(&supervisor, state, telemetry_rx, &limits).await;
    // Serve runs until the token is cancelled (Ctrl-C) or fails. After it
    // returns, one bounded drain stops every background task — no detached
    // handles can outlive the process (P1-3).
    let result = server::serve(listener, router, serve_cancel).await;
    supervisor.shutdown(limits.shutdown_deadline).await;
    result
}
