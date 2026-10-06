//! 组合根：装配 Database/SecretStore/Telemetry/supervisor 与各业务服务，
//! 并把 axum router 组装好、绑定监听地址并 serve。
//!
//! 边界：只负责装配与启动，不实现任何业务逻辑。
//! 不变量：一个 supervisor 同时拥有 serve 与全部后台任务；关停只经一次有界排空，
//! 绝不把任务 detach 出去（重启换端口时旧 supervisor 必须被回收）。
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
    state::AppState,
    assets,
    config::AppConfig,
    crypto::SecretStore,
    db::Database,
    domain::Event,
    infrastructure::{HttpClientPool, RuntimeSupervisor},
    telemetry::Telemetry,
};

/// 装配完成的网关：state + router + supervisor，外加遥测事件通道，
/// 供调用方 spawn（并在之后 join）全部后台任务。
pub struct GatewayRuntime {
    pub state: AppState,
    pub router: Router,
    pub telemetry_rx: mpsc::Receiver<Event>,
    /// 拥有 serve 与全部后台任务；调用 [`RuntimeSupervisor::shutdown`]
    /// 即可用一次有界、可 join 的排空停止整个运行时。
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
    // 只构造一个 notifier：worker 注册进 supervisor，proxy 与 Context
    // 看到的是同一实例，不再多出一个没人等待的后台任务。
    let notifier = crate::notification::DesktopNotifier::registered(
        crate::notification::DEFAULT_FLUSH_WINDOW,
        &supervisor,
    )
    .await?;
    let notifier_dyn: Arc<dyn crate::ports::Notifier> = notifier.clone();
    let settings_reader: Arc<dyn crate::ports::SettingsReader> =
        crate::infrastructure::SettingsStore::new(db.clone(), secrets.clone());
    let command_code_state: Arc<dyn crate::ports::CommandCodeState> =
        crate::infrastructure::CommandCodeStore::new(db.clone(), Arc::clone(&http));
    let proxy = crate::proxy::ProxyService::new(crate::proxy::ProxyServiceDeps {
        settings: Arc::clone(&settings_reader),
        command_code: Arc::clone(&command_code_state),
        secrets: secrets.clone(),
        http: Arc::clone(&http),
        routes: routes.clone(),
        telemetry: telemetry.clone(),
        clock: Arc::clone(&clock),
        limits: Arc::clone(&limits),
        notifier: notifier_dyn,
    });
    let admin = crate::admin::AdminService::new(db.clone(), secrets.clone(), Arc::clone(&channels_dyn));
    let balance = crate::balance::BalanceService::new(
        db.clone(),
        secrets.clone(),
        Arc::clone(&http),
        Arc::clone(&channels_dyn),
        Arc::clone(&clock),
        Arc::clone(&limits),
    );
    let command_code_login = crate::commandcode_login::CommandCodeLogin::new(std::sync::Arc::clone(&http));
    // 先构造只含端口与值的 `Context`，再组装 `AppState`（服务在其上）。
    let ctx = Context {
        config: Arc::new(config),
        db,
        secrets,
        http,
        routes,
        channels,
        clock,
        notifier,
        telemetry,
        background: supervisor.clone(),
        limits,
    };
    let state = crate::state::AppState {
        ctx,
        proxy,
        admin,
        balance,
        discovery,
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

/// 把所有长驻后台任务（一次性任务回收器、健康 supervisor、维护 supervisor、
/// 遥测写入器）注册到 supervisor。每个条目本身就是任务体——不再套一层
/// `tokio::spawn`，因此 supervisor 的 `JoinSet` 直接持有真实任务，到期 abort
/// 绝不会把它们 detach 掉。调用方持有 supervisor，用一次
/// [`RuntimeSupervisor::shutdown`] 即可停止全部任务。
pub async fn spawn_background(
    supervisor: &Arc<RuntimeSupervisor>,
    state: AppState,
    telemetry_rx: mpsc::Receiver<Event>,
    limits: &crate::runtime::RuntimeLimits,
) {
    // 组装期（启动）：supervisor 不可能已进入关停流程，因此注册被拒属于编程
    // 错误；启动期失败就快速退出是预期行为，故此处保留 `expect`。
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
    // 这里只 clone 进 db 与被丢弃计数器：写入任务绝不能持有遥测 sender——
    // 否则它会把自己的接收端一直保持打开，关停排空就会等待自己。
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
    // RecoveryAuth 提取器要求 `ConnectInfo`：密钥恢复端点的回环地址检查
    // 需要拿到对端地址。
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

    /// 关停一开始就不得再注册新任务，且只靠取消退出的任务仍须在期限内被 join。
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
        // 注册与关停竞争：任何结果都安全——要么注册成功（随后被 join/abort），
        // 要么被拒——绝不会残留孤立任务。
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

    /// 忽略取消、永不结束的任务会在期限到达时被 abort，且其 Drop 守卫仍会执行——
    /// 排空返回时存活任务为零、资源已释放，而不是留下 detach 的幸存者。
    #[tokio::test]
    async fn deadline_abort_runs_drop_guards_and_leaves_zero_tasks() {
        /// 永不结束任务的 Drop 守卫；它能执行即证明该任务 future 被 drop（而非 detach）。
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
                // 永不观察取消信号；只有 abort 能停止它。
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                }
            })
            .await
            .unwrap();
        // 第二个永不结束的任务，好让 abort 路径必须排空不止一个条目。
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

    /// 反复 start/stop 不留任何运行中的东西——每次 shutdown 只有在
    /// 所有已注册任务都结束后才返回。
    #[tokio::test]
    async fn ten_restart_cycles_drain_completely() {
        for cycle in 0..10 {
            let supervisor = RuntimeSupervisor::new(CancellationToken::new());
            let cancel = supervisor.cancel.clone();
            let task_cancel = cancel.clone();
            supervisor
                .spawn(async move {
                    // 模拟健康/遥测类任务：一直运行到被取消，
                    // 中间夹杂一些异步切换。
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
