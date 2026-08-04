use std::sync::Arc;

use anyhow::{Context, Result};
use axum::{Json, Router, routing::get};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;

use crate::{assets, config::AppConfig, crypto::SecretStore, db::Database, telemetry::Telemetry};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<AppConfig>,
    pub db: Database,
    pub secrets: SecretStore,
    pub http: reqwest::Client,
    pub telemetry: Telemetry,
}

pub async fn build(config: AppConfig) -> Result<Router> {
    let db = Database::open(&config.database_path()).await?;
    let secrets = SecretStore::load(&config.key_path()).await?;
    let http = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .pool_idle_timeout(std::time::Duration::from_secs(90))
        .pool_max_idle_per_host(20)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("无法初始化 HTTP 客户端")?;
    let telemetry = Telemetry::start(db.clone(), 1000);
    let state = AppState {
        config: Arc::new(config),
        db,
        secrets,
        http,
        telemetry,
    };
    // Background supervisors (circuit auto-recovery + maintenance) live for
    // the process lifetime, like the telemetry writer.
    crate::health::spawn_supervisor(state.clone());
    crate::maintenance::spawn_supervisor(state.clone());
    Ok(Router::new()
        .merge(crate::admin::router())
        .route("/api/health", get(health))
        .route("/v1/models", get(crate::proxy::openai_models))
        .route("/v1/responses/models", get(crate::proxy::responses_models))
        .route("/v1/messages/models", get(crate::proxy::claude_models))
        .route("/v1beta/models", get(crate::proxy::gemini_models))
        .route(
            "/v1/chat/completions",
            axum::routing::post(crate::proxy::openai),
        )
        .route("/v1/completions", axum::routing::post(crate::proxy::openai))
        .route("/v1/embeddings", axum::routing::post(crate::proxy::openai))
        .route(
            "/v1/responses",
            axum::routing::post(crate::proxy::responses),
        )
        .route("/v1/messages", axum::routing::post(crate::proxy::claude))
        .route(
            "/v1beta/models/{*action}",
            axum::routing::post(crate::proxy::gemini),
        )
        .route("/claudecode", get(crate::proxy::claudecode_info))
        .route(
            "/claudecode/v1/models",
            get(crate::proxy::claudecode_models),
        )
        .route(
            "/claudecode/v1/messages/models",
            get(crate::proxy::claudecode_models),
        )
        .route(
            "/claudecode/v1/messages",
            axum::routing::post(crate::proxy::claudecode),
        )
        .route("/codex", get(crate::proxy::codex_info))
        .route("/codex/v1/models", get(crate::proxy::codex_models))
        .route(
            "/codex/v1/responses/models",
            get(crate::proxy::codex_models),
        )
        .route(
            "/codex/v1/responses",
            axum::routing::post(crate::proxy::codex),
        )
        .fallback(assets::serve)
        .layer(TraceLayer::new_for_http())
        .with_state(state))
}

pub async fn bind(config: &AppConfig) -> Result<TcpListener> {
    TcpListener::bind((config.host.as_str(), config.port))
        .await
        .with_context(|| format!("无法监听 {}:{}，端口可能已被占用", config.host, config.port))
}

pub async fn serve(listener: TcpListener, router: Router, cancel: CancellationToken) -> Result<()> {
    axum::serve(listener, router)
        .with_graceful_shutdown(cancel.cancelled_owned())
        .await
        .context("网关服务异常退出")
}

async fn health() -> Json<Value> {
    Json(json!({"status": "ok", "runtime": "rust"}))
}
