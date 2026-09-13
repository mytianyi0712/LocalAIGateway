use std::sync::Arc;

use anyhow::{Context as _, Result};
use axum::{Json, Router, routing::get};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;

use crate::{
    application::Context,
    assets,
    config::AppConfig,
    crypto::SecretStore,
    db::Database,
    infrastructure::{HttpClientPool, RuntimeSupervisor},
    telemetry::{Event, Telemetry},
};

/// Assembled gateway: state + router + supervisor, plus the telemetry event
/// channel so the caller can spawn (and later join) all background tasks.
pub struct GatewayRuntime {
    pub state: Context,
    pub router: Router,
    pub telemetry_rx: mpsc::Receiver<Event>,
    /// Owns serve + every background task; call [`RuntimeSupervisor::shutdown`]
    /// to stop the runtime with one bounded, joinable drain.
    pub supervisor: Arc<RuntimeSupervisor>,
}

pub async fn build(config: AppConfig) -> Result<GatewayRuntime> {
    let db = Database::open(&config.database_path()).await?;
    let secrets = SecretStore::load(&config.key_path()).await?;
    let (telemetry, telemetry_rx) = Telemetry::new(1000);
    let cancel = CancellationToken::new();
    let supervisor = RuntimeSupervisor::new(cancel.clone());
    let limits = Arc::new(crate::runtime::RuntimeLimits::default());
    let routes = crate::infrastructure::SqliteRouteRepository::new(db.clone());
    let channels = crate::infrastructure::SqliteChannelRepository::new(db.clone());
    let http: Arc<dyn crate::ports::UpstreamClient> = Arc::new(HttpClientPool::default());
    let clock: Arc<dyn crate::ports::Clock> = Arc::new(crate::infrastructure::SystemClock);
    let channels_dyn: Arc<dyn crate::ports::ChannelRepository> = channels.clone();
    let discovery = crate::discovery::DiscoveryService::new(
        db.clone(),
        secrets.clone(),
        Arc::clone(&http),
        Arc::clone(&channels_dyn),
        Arc::clone(&clock),
        supervisor.clone(),
        Arc::clone(&limits),
    );
    let proxy = crate::proxy::ProxyService::new(
        db.clone(),
        secrets.clone(),
        Arc::clone(&http),
        routes.clone(),
        telemetry.clone(),
        Arc::clone(&clock),
        Arc::clone(&limits),
        crate::notification::DesktopNotifier::new(
            crate::notification::DEFAULT_FLUSH_WINDOW,
        ),
    );
    let admin = crate::admin::AdminService::new(db.clone(), secrets.clone());
    let notifier = crate::notification::DesktopNotifier::new(
        crate::notification::DEFAULT_FLUSH_WINDOW,
    );
    let balance = crate::balance::BalanceService::new(
        db.clone(),
        secrets.clone(),
        Arc::clone(&http),
        Arc::clone(&channels_dyn),
        Arc::clone(&clock),
        Arc::clone(&limits),
    );
    let command_code_login = crate::commandcode_login::CommandCodeLogin::new(std::sync::Arc::clone(&http));
    let state = Context {
        config: Arc::new(config),
        db,
        secrets,
        http,
        routes,
        channels,
        clock,
        notifier,
        discovery,
        proxy,
        admin,
        balance,
        telemetry,
        background: supervisor.clone(),
        limits,
        command_code_login,
        recovery: crate::auth::RecoverySession::new(),
    };
    let router = Router::new()
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
        .route(
            "/v1/responses/compact",
            axum::routing::post(crate::proxy::responses_compact),
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
        .route(
            "/codex/v1/responses/compact",
            axum::routing::post(crate::proxy::codex_compact),
        )
        .fallback(assets::serve)
        .layer(TraceLayer::new_for_http())
        .with_state(state.clone());
    Ok(GatewayRuntime {
        state,
        router,
        telemetry_rx,
        supervisor,
    })
}

/// Registers every long-lived background task (one-shot task reaper, health
/// supervisor, maintenance supervisor, telemetry writer) on the supervisor.
/// Each entry is the task body itself — no inner `tokio::spawn` layer, so
/// the supervisor's `JoinSet` owns the real tasks and a deadline abort can
/// never detach them (P1-1). The caller owns the supervisor and stops
/// everything with one [`RuntimeSupervisor::shutdown`] call.
pub async fn spawn_background(
    supervisor: &Arc<RuntimeSupervisor>,
    state: Context,
    telemetry_rx: mpsc::Receiver<Event>,
    limits: &crate::runtime::RuntimeLimits,
) {
    // Composition time: the supervisor cannot be shutting down, so a
    // rejection here is a programming error, not a runtime condition.
    let supervisor = Arc::clone(supervisor);
    supervisor
        .spawn(supervisor.clone().reap_loop(limits.reaper_interval))
        .await
        .expect("background registration at startup must be accepted");
    let health_state = state.clone();
    let health_cancel = supervisor.cancel.clone();
    supervisor
        .spawn(crate::health::run_supervisor(health_state, health_cancel))
        .await
        .expect("background registration at startup must be accepted");
    let maintenance_state = state.clone();
    let maintenance_cancel = supervisor.cancel.clone();
    supervisor
        .spawn(crate::maintenance::run_supervisor(maintenance_state, maintenance_cancel))
        .await
        .expect("background registration at startup must be accepted");
    // Only db + the dropped counter are cloned in: the writer task must
    // NEVER hold a telemetry sender — it would keep its own receive channel
    // open and the shutdown drain would wait on itself (P1-3).
    let writer_db = state.db.clone();
    let writer_dropped = state.telemetry.dropped_handle();
    let writer_cancel = supervisor.cancel.clone();
    supervisor
        .spawn(Telemetry::run_writer(
            writer_db,
            telemetry_rx,
            writer_cancel,
            writer_dropped,
        ))
        .await
        .expect("background registration at startup must be accepted");
}

pub async fn bind(config: &AppConfig) -> Result<TcpListener> {
    TcpListener::bind((config.host.as_str(), config.port))
        .await
        .with_context(|| format!("无法监听 {}:{}，端口可能已被占用", config.host, config.port))
}

pub async fn serve(listener: TcpListener, router: Router, cancel: CancellationToken) -> Result<()> {
    // ConnectInfo is required by the RecoveryAuth extractor (P1-7): the
    // loopback check for the key-recovery endpoints needs the peer address.
    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(cancel.cancelled_owned())
    .await
    .context("网关服务异常退出")
}

async fn health() -> Json<Value> {
    Json(json!({"status": "ok", "runtime": "rust"}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    /// P1-3: once shutdown starts no new task may register, and a task that
    /// only exits on cancellation is still joined within the deadline.
    #[tokio::test]
    async fn shutdown_rejects_registrations_and_joins_everything() {
        let supervisor = RuntimeSupervisor::new(CancellationToken::new());
        let cancel = supervisor.cancel.clone();
        supervisor
            .spawn(async move {
                cancel.cancelled().await;
            })
            .await
            .unwrap();
        supervisor.spawn(async {}).await.unwrap();
        // Registration racing shutdown: every outcome is safe — registered
        // (then joined/aborted) or rejected — never an orphaned task.
        let racer = supervisor.clone();
        let spawner = tokio::spawn(async move {
            for _ in 0..50 {
                let _ = racer.spawn(async {}).await;
            }
        });
        supervisor.shutdown(std::time::Duration::from_secs(5)).await;
        spawner.await.unwrap();
        assert!(
            supervisor.spawn(async {}).await.is_err(),
            "registration after shutdown must be refused"
        );
        assert_eq!(
            supervisor.active_task_count(),
            0,
            "no task may survive a completed shutdown"
        );
    }

    /// P1-1: a task that ignores cancellation and never ends is aborted
    /// when the deadline expires, and its drop guards still run — the drain
    /// returns with zero live tasks and released resources, not detached
    /// survivors.
    #[tokio::test]
    async fn deadline_abort_runs_drop_guards_and_leaves_zero_tasks() {
        /// Drop guard for a never-ending task; proving it runs proves the
        /// task future was dropped (not detached).
        struct Guard(Arc<std::sync::atomic::AtomicU64>);
        impl Drop for Guard {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let supervisor = RuntimeSupervisor::new(CancellationToken::new());
        let dropped = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let dropped_probe = Arc::clone(&dropped);
        supervisor
            .spawn(async move {
                let _guard = Guard(dropped_probe);
                // Never observes cancellation; only an abort can stop it.
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                }
            })
            .await
            .unwrap();
        // A second never-ending task, so the abort path must drain more
        // than one entry.
        supervisor
            .spawn(async {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                }
            })
            .await
            .unwrap();
        supervisor.shutdown(std::time::Duration::from_millis(10)).await;
        assert_eq!(
            supervisor.active_task_count(),
            0,
            "abort must leave zero live tasks"
        );
        assert_eq!(
            dropped.load(Ordering::Relaxed),
            1,
            "the aborted task's drop guard must run"
        );
    }

    /// P1-3: repeated start/stop cycles leave nothing running — each
    /// shutdown returns only after every registered task has finished.
    #[tokio::test]
    async fn ten_restart_cycles_drain_completely() {
        for cycle in 0..10 {
            let supervisor = RuntimeSupervisor::new(CancellationToken::new());
            let cancel = supervisor.cancel.clone();
            let task_cancel = cancel.clone();
            supervisor
                .spawn(async move {
                    // Simulates a health/telemetry-style task that runs
                    // until cancelled, with some async churn in between.
                    while !task_cancel.is_cancelled() {
                        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            supervisor
                .spawn(async {
                    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                })
                .await
                .unwrap();
            supervisor.shutdown(std::time::Duration::from_secs(5)).await;
            assert!(
                cancel.is_cancelled(),
                "cycle {cycle}: shutdown must cancel the token"
            );
            assert_eq!(
                supervisor.active_task_count(),
                0,
                "cycle {cycle}: no task may survive a shutdown"
            );
        }
    }
}
