use anyhow::Result;
use local_ai_gateway::{config::AppConfig, server};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let config = AppConfig::load(std::path::PathBuf::from("data")).await?;
    let listener = server::bind(&config).await?;
    let router = server::build(config.clone()).await?;
    tracing::info!(url = %config.local_url(), "gateway ready");
    let cancel = CancellationToken::new();
    let signal = cancel.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        signal.cancel();
    });
    server::serve(listener, router, cancel).await
}
