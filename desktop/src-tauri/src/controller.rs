//! 桌面启动器 / 网关生命周期控制器：负责 start/stop/set_port 与托盘状态，
//! 维护运行时 generation 轮次，并监控 serve 退出。
//!
//! 边界：只管生命周期，不碰任何业务逻辑。
//! 不变量：slot 锁串行化 start/stop（并发调用不会产生两个存活的 generation）；
//! serve 异常退出由控制器自有的 monitor 回收槽位并排空运行时；stop 为有界排空。
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use serde::Serialize;
use tokio::sync::{Mutex, RwLock, mpsc};

use crate::{
    config::{AppConfig, validate_port},
    infrastructure::RuntimeSupervisor,
    server,
};

#[derive(Clone, Debug, Default)]
struct RuntimeStatus {
    running: bool,
    error: Option<String>,
}

struct RunningServer {
    /// 拥有 serve 与全部后台任务：停止服务器就是一次有界
    /// [`RuntimeSupervisor::shutdown`]——不留下任何 detach 的句柄。
    supervisor: Arc<RuntimeSupervisor>,
    /// 该运行时的绝对关停期限。
    limits: crate::runtime::RuntimeLimits,
    /// 该运行时的 generation 轮次：更早轮次的 serve 退出 monitor
    /// 永远不会回收更新的槽位。
    generation: u64,
    /// serve 任务返回（正常或异常）后置位；让 `start()` 能在其 monitor
    /// 回收之前就识别出已死的运行时。
    serve_ended: Arc<AtomicBool>,
    /// serve 完成通道，被 clone 进 serve 任务。保留在槽位上，
    /// 以便测试注入一次异常 serve 退出。
    #[allow(dead_code)] // 测试读取它来注入 serve 失败
    serve_done: mpsc::Sender<anyhow::Result<()>>,
    /// 控制器自有的 monitor：监听 serve 完成通道，在 serve 异常退出时
    /// 回收槽位并排空运行时。它被显式持有在此处（不在 supervisor 的
    /// JoinSet 内），因此能调用 [`RuntimeSupervisor::shutdown`] 而无需等待自己。
    monitor: tokio::task::JoinHandle<()>,
}

pub struct ServerController {
    platform_data_dir: PathBuf,
    config: RwLock<AppConfig>,
    runtime: RwLock<RuntimeStatus>,
    server: Mutex<Option<RunningServer>>,
    /// 单调递增的运行时 generation 计数器；每次 `start()` 都会自增。
    generation: AtomicU64,
}

#[derive(Serialize)]
pub struct LauncherState {
    port: u16,
    url: String,
    running: bool,
    error: Option<String>,
    version: &'static str,
}

impl ServerController {
    pub async fn load(platform_data_dir: PathBuf) -> Result<Arc<Self>> {
        let config = AppConfig::load(&platform_data_dir).await?;
        Ok(Arc::new(Self {
            platform_data_dir,
            config: RwLock::new(config),
            runtime: RwLock::new(RuntimeStatus::default()),
            server: Mutex::new(None),
            generation: AtomicU64::new(0),
        }))
    }

    pub async fn state(&self) -> LauncherState {
        let config = self.config.read().await;
        let runtime = self.runtime.read().await;
        LauncherState {
            port: config.port,
            url: config.local_url(),
            running: runtime.running,
            error: runtime.error.clone(),
            version: env!("CARGO_PKG_VERSION"),
        }
    }
    pub async fn record_error(&self, error: impl Into<String>) {
        let mut runtime = self.runtime.write().await;
        runtime.running = false;
        runtime.error = Some(error.into());
    }

    /// 启动网关。整个 start 期间都持有 slot 锁，因此并发的
    /// `start`/`stop`/`set_port` 绝不会产生两个存活的 generation。
    /// serve 任务已返回的槽位会先被回收：排空其后台任务后，
    /// 再由新运行时接管该槽位。
    pub async fn start(self: &Arc<Self>) -> Result<()> {
        let mut slot = self.server.lock().await;
        if let Some(running) = slot.as_ref()
            && !running.serve_ended.load(Ordering::SeqCst)
        {
            // 已有存活的运行时（serve 仍在接受连接），无需再启动。
            return Ok(());
        }
        // 陈旧槽位：serve 任务已经返回（monitor 尚未回收的异常退出，
        // 或 request_stop 之后的槽位）。先排空它的后台任务再重新启动。
        let stale = slot.take();
        if let Some(stale) = stale {
            stale.supervisor.shutdown(stale.limits.shutdown_deadline).await;
            // 陈旧的 monitor（控制器自有、位于 JoinSet 之外）会醒来，
            // 看到新 generation 或空槽位后自行退出——这里不 join 它，
            // 因此不可能出现自等待。
        }
        let config = self.config.read().await.clone();
        let server::GatewayRuntime {
            state,
            router,
            telemetry_rx,
            supervisor,
        } = server::build(config.clone()).await?;
        let listener = server::bind(&config).await?;
        server::spawn_background(&supervisor, state.clone(), telemetry_rx, &state.limits).await;
        let limits = *state.limits;
        let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
        let serve_ended = Arc::new(AtomicBool::new(false));
        let (serve_done_tx, mut serve_done_rx) = mpsc::channel(1);
        let serve_cancel = supervisor.cancel.clone();
        let controller = Arc::clone(self);
        let task_serve_ended = Arc::clone(&serve_ended);
        let task_serve_done = serve_done_tx.clone();
        // 监听端口已绑定：运行时从此刻起算作运行中。
        // 同步置位该标志，使 `start()` 返回即代表运行时存活，
        // serve 任务只负责记录退出侧的状态。
        {
            let mut runtime = self.runtime.write().await;
            runtime.running = true;
            runtime.error = None;
        }
        // serve 任务同样加入 supervisor 的 JoinSet：关停与所有后台任务一样
        // 覆盖 HTTP 服务器。它只做状态记账并发出完成信号；槽位回收与异常退出时的
        // 统一关停由下方控制器自有的 monitor 负责。
        supervisor
            .spawn(async move {
                let result = server::serve(listener, router, serve_cancel).await;
                {
                    let mut runtime = controller.runtime.write().await;
                    runtime.running = false;
                    if let Err(error) = &result {
                        runtime.error = Some(error.to_string());
                    }
                }
                task_serve_ended.store(true, Ordering::SeqCst);
                let _ = task_serve_done.send(result).await;
            })
            .await
            // 启动期专用任务：此处 `expect` 保留——控制器在同一协程内顺序调用，
            // 不可能与关停竞争；真失败时快速退出是预期行为。
            .expect("start must not race shutdown");
        // 控制器自有的 monitor：监听 serve 完成通道，在异常退出时回收槽位并
        // 排空运行时，避免健康/维护/遥测在已死的 HTTP 服务器下继续运行。
        // 它位于 supervisor 的 JoinSet 之外，并由 `RunningServer` 显式持有，
        // 因此可以调用 `shutdown()` 而无需等待自己。
        let monitor_controller = Arc::clone(self);
        let monitor_generation = generation;
        let monitor = tokio::spawn(async move {
            match serve_done_rx.recv().await {
                Some(Ok(())) => {
                    // 优雅停止：所有者（stop()/request_stop）已回收槽位并
                    // 排空了 supervisor，此处无事可做。
                }
                Some(Err(error)) => {
                    // serve 异常退出。仅当槽位中仍是本 monitor 的 generation 时才回收——
                    // 更新的运行时绝不能被陈旧 monitor 拆掉。
                    let stale = {
                        let mut slot = monitor_controller.server.lock().await;
                        match slot.as_ref() {
                            Some(running) if running.generation == monitor_generation => {
                                slot.take()
                            }
                            _ => None,
                        }
                    };
                    if let Some(stale) = stale {
                        // serve 任务已异常返回；在（有界的）排空之前先把运行时
                        // 标记为已停止，避免观察者把已死的运行时当成运行中。
                        {
                            let mut runtime = monitor_controller.runtime.write().await;
                            runtime.running = false;
                            runtime.error = Some(error.to_string());
                        }
                        stale
                            .supervisor
                            .shutdown(stale.limits.shutdown_deadline)
                            .await;
                    }
                }
                None => {}
            }
        });
        *slot = Some(RunningServer {
            supervisor,
            limits,
            generation,
            serve_ended,
            serve_done: serve_done_tx,
            monitor,
        });
        Ok(())
    }

    pub async fn stop(&self) {
        let running = self.server.lock().await.take();
        if let Some(running) = running {
            // 一次有界排空覆盖 serve + 遥测尾部 + 全部后台任务；期限即该运行时
            // 的绝对关停上界，超过后剩余任务被 abort 并 join——绝不 detach。
            running
                .supervisor
                .shutdown(running.limits.shutdown_deadline)
                .await;
            // serve 任务已返回（优雅关停），因此 monitor 已收到完成信号；
            // 槽位已清空，它会自行退出而不触碰任何东西。这里短暂 join 它，
            // 确保没有控制器自有的任务比 stop() 活得更久。
            let _ = tokio::time::timeout(Duration::from_secs(5), running.monitor).await;
        }
        self.runtime.write().await.running = false;
    }

    pub fn request_stop(&self) {
        if let Ok(slot) = self.server.try_lock()
            && let Some(running) = slot.as_ref()
        {
            running.supervisor.cancel.cancel();
        }
    }

    pub async fn set_port(self: &Arc<Self>, port: u16) -> Result<()> {
        validate_port(port)?;
        self.stop().await;
        {
            let mut config = self.config.write().await;
            config.port = port;
            config.save().await.context("无法保存端口配置")?;
        }
        if let Err(error) = self.start().await {
            self.runtime.write().await.error = Some(error.to_string());
            return Err(error);
        }
        Ok(())
    }

    pub async fn open_dashboard(&self) -> Result<()> {
        let state = self.state().await;
        if !state.running {
            anyhow::bail!("网关尚未运行");
        }
        open::that(&state.url).context("无法打开系统浏览器")
    }

    pub async fn start_to_tray(&self) -> bool {
        self.config.read().await.start_to_tray
    }

    pub async fn set_start_to_tray(&self, enabled: bool) -> Result<()> {
        let mut config = self.config.write().await;
        config.start_to_tray = enabled;
        config.save().await.context("无法保存启动选项")
    }

    pub fn platform_data_dir(&self) -> &PathBuf {
        &self.platform_data_dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;
    use crate::test_support::TempDir;
    use std::time::Duration;

    /// 接受循环式上游：提供真实的 `/v1/models` 目录（让维护 supervisor 的
    /// 启动期 discovery 保持已播种模型可用）以及一个健康的 chat-completions 响应。
    async fn spawn_upstream() -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buf = [0u8; 4096];
                    let mut total = 0usize;
                    loop {
                        match stream.read(&mut buf[total..]).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                total += n;
                                if buf[..total].windows(4).any(|w| w == b"\r\n\r\n") {
                                    break;
                                }
                            }
                        }
                    }
                    let request = String::from_utf8_lossy(&buf[..total]);
                    let body = if request.contains("/v1/models") {
                        r#"{"object":"list","data":[{"id":"probe-model","object":"model","owned_by":"test"}]}"#
                    } else {
                        r#"{"choices":[{"finish_reason":"stop","message":{"content":"OK"}}]}"#
                    };
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        });
        port
    }

    fn free_port() -> u16 {
        std::net::TcpListener::bind(("127.0.0.1", 0))
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    async fn wait_for_probe_count(db: &Database, expected: i64, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM health_probe_logs")
                .fetch_one(db.pool())
                .await
                .unwrap();
            if count == expected {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {expected} probe rows, have {count}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// 换端口重启不得泄漏旧的健康 supervisor——每 5s 周期恰好产生一行探测。
    #[tokio::test(flavor = "multi_thread")]
    async fn port_restart_keeps_single_supervisor() {
        let upstream_port = spawn_upstream().await;
        let dir = TempDir::new("ctrl");

        // 按运行时将要看到的样子播种渠道数据库，然后关闭播种用的连接池，
        // 只留控制器自己的连接池活跃——与生产形态一致。加密前密钥文件必须已存在：
        // SecretStore::load 会创建它。
        let secrets = crate::crypto::SecretStore::load(&dir.path().join("master.key"))
            .await
            .unwrap();
        let seed_db = Database::open(&dir.path().join("gateway.db")).await.unwrap();
        let time = chrono::Utc::now().to_rfc3339();
        let past = (chrono::Utc::now() - chrono::Duration::minutes(1)).to_rfc3339();
        sqlx::query("INSERT INTO providers(id,name,base_url,created_at,updated_at) VALUES('prov-1','mock',?,?,?)")
            .bind(format!("http://127.0.0.1:{upstream_port}"))
            .bind(&time)
            .bind(&time)
            .execute(seed_db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channels(id,provider_id,name,protocol,api_key_encrypted,api_key_hint,manual_enabled,created_at,updated_at) VALUES('ch-1','prov-1','chan','openai_compatible',?,?,1,?,?)")
            .bind(secrets.encrypt("test-key"))
            .bind("...key")
            .bind(&time)
            .bind(&time)
            .execute(seed_db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_models(id,channel_id,model_id,display_name,source,available,first_seen_at,created_at,updated_at) VALUES('cm-1','ch-1','probe-model','Probe','discovered',1,?,?,?)")
            .bind(&time)
            .bind(&time)
            .bind(&time)
            .execute(seed_db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_health(channel_id,state,consecutive_failures,disabled_until,updated_at) VALUES('ch-1','open',0,?,?)")
            .bind(&past)
            .bind(&time)
            .execute(seed_db.pool())
            .await
            .unwrap();
        seed_db.pool().close().await;

        let controller = ServerController::load(dir.path().to_path_buf()).await.unwrap();
        let first_port = free_port();
        controller.set_port(first_port).await.unwrap();
        // supervisor 的第一次 tick 立即触发，之后每 5s 一次。
        let db = Database::open(&dir.path().join("gateway.db")).await.unwrap();
        wait_for_probe_count(&db, 1, Duration::from_secs(8)).await;

        // 重新武装熔断，然后换端口重启。
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE channel_health SET state='open', disabled_until=? WHERE channel_id='ch-1'",
        )
        .bind(&now)
        .execute(db.pool())
        .await
        .unwrap();
        let second_port = free_port();
        controller.set_port(second_port).await.unwrap();
        wait_for_probe_count(&db, 2, Duration::from_secs(8)).await;

        controller.stop().await;
        // stop 必须 join 掉全部后台句柄；之后不应再有泄漏的 supervisor
        // 产生第三行探测记录。
        tokio::time::sleep(Duration::from_millis(500)).await;
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM health_probe_logs")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(count, 2, "no probe may fire after stop");
    }

    /// 优雅关停会排空遥测尾部。`stop()` 被调用时仍在途的流式请求，其成功
    /// 结束记录仍必须落进 request_logs——写入器在关停完成期间持续排空，
    /// 而不是在第一次取消 tick 时就退出。
    ///
    /// 响应与 `stop()` 是并发驱动的：`serve` 的优雅关停会等待在途连接，
    /// 而该连接只有在客户端消费完流后才结束，因此若先 await `stop()`
    /// 再读取响应就会死锁。
    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_drains_tail_telemetry() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buf = [0u8; 4096];
                    let mut total = 0usize;
                    loop {
                        match stream.read(&mut buf[total..]).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                total += n;
                                if buf[..total].windows(4).any(|w| w == b"\r\n\r\n") {
                                    break;
                                }
                            }
                        }
                    }
                    let request = String::from_utf8_lossy(&buf[..total]);
                    if request.contains("/v1/models") {
                        // 让 discovery 定时拉取的目录保持健康。
                        let body = r#"{"object":"list","data":[{"id":"probe-model","object":"model","owned_by":"test"}]}"#;
                        let response = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                            body.len()
                        );
                        let _ = stream.write_all(response.as_bytes()).await;
                        return;
                    }
                    // chat completions：延迟 1s，然后返回完整的 SSE 响应体。
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                    let body = "data: {\"id\":\"1\",\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\ndata: [DONE]\n\n";
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        });

        let dir = std::env::temp_dir().join(format!("lagw-ctrl-tail-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let secrets = crate::crypto::SecretStore::load(&dir.join("master.key"))
            .await
            .unwrap();
        let seed_db = Database::open(&dir.join("gateway.db")).await.unwrap();
        let time = chrono::Utc::now().to_rfc3339();
        sqlx::query("INSERT INTO providers(id,name,base_url,created_at,updated_at) VALUES('prov-1','mock',?,?,?)")
            .bind(format!("http://127.0.0.1:{upstream_port}"))
            .bind(&time)
            .bind(&time)
            .execute(seed_db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channels(id,provider_id,name,protocol,api_key_encrypted,api_key_hint,manual_enabled,created_at,updated_at) VALUES('ch-1','prov-1','chan','openai_compatible',?,?,1,?,?)")
            .bind(secrets.encrypt("test-key"))
            .bind("...key")
            .bind(&time)
            .bind(&time)
            .execute(seed_db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_models(id,channel_id,model_id,display_name,source,available,first_seen_at,created_at,updated_at) VALUES('cm-1','ch-1','probe-model','Probe','discovered',1,?,?,?)")
            .bind(&time)
            .bind(&time)
            .bind(&time)
            .execute(seed_db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES('cm-1','openai_compatible')")
            .execute(seed_db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO model_routes(id,protocol,requested_model_id,enabled,created_at,updated_at) VALUES('route-1','openai_compatible','probe-model',1,?,?)")
            .bind(&time)
            .bind(&time)
            .execute(seed_db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO route_candidates(id,route_id,channel_model_id,priority,enabled,created_at,updated_at) VALUES('rc-1','route-1','cm-1',1,1,?,?)")
            .bind(&time)
            .bind(&time)
            .execute(seed_db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_health(channel_id,state,consecutive_failures,updated_at) VALUES('ch-1','active',0,?)")
            .bind(&time)
            .execute(seed_db.pool())
            .await
            .unwrap();
        seed_db.pool().close().await;

        let controller = Arc::new(ServerController::load(dir.clone()).await.unwrap());
        let port = free_port();
        controller.set_port(port).await.unwrap();
        // accept 循环在 `bind` 返回后片刻才启动；发请求前先等待监听就绪。
        let mut accepted = false;
        for _ in 0..50 {
            if tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_ok()
            {
                accepted = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(accepted, "gateway must accept connections");
        // 发起流式请求，但先不 await 它。`send()` 是惰性的，因此把 future 放进
        // spawn 的任务里：连接会立即建立，调用 `stop()` 时请求确实在途。
        let client = reqwest::Client::new();
        let response_future = tokio::spawn(
            client
                .post(format!("http://127.0.0.1:{port}/v1/chat/completions"))
                .header("content-type", "application/json")
                .body(
                    r#"{"model":"probe-model","stream":true,"messages":[{"role":"user","content":"hi"}]}"#,
                )
                .send(),
        );
        // 确保请求在停止之前已经发出。
        tokio::time::sleep(Duration::from_millis(200)).await;
        let controller_for_stop = Arc::clone(&controller);
        let stop_task = tokio::spawn(async move { controller_for_stop.stop().await });
        let response = tokio::time::timeout(Duration::from_secs(10), response_future)
            .await
            .expect("in-flight request must complete")
            .expect("request task must not panic")
            .expect("request must succeed");
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let bytes = tokio::time::timeout(Duration::from_secs(10), response.bytes())
            .await
            .expect("stream must complete")
            .expect("body read must succeed");
        assert!(!bytes.is_empty());
        tokio::time::timeout(Duration::from_secs(15), stop_task)
            .await
            .expect("stop must finish within its bounded joins")
            .expect("stop task must not panic");

        // 即便写入器在请求在途时被取消，请求日志的尾部也必须持久化。
        let db = Database::open(&dir.join("gateway.db")).await.unwrap();
        let (outcome, attempts): (String, i64) = sqlx::query_as(
            "SELECT outcome, attempt_count FROM request_logs ORDER BY started_at DESC LIMIT 1",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(outcome, "success", "in-flight finish must be drained");
        assert_eq!(attempts, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// serve 异常退出必须回收槽位、排空全部后台任务、暴露错误，并允许在同一端口
    /// 上重新 `start()`——旧运行时不得继续占用槽位或监听端口。
    #[tokio::test(flavor = "multi_thread")]
    async fn abnormal_serve_exit_reaps_slot_and_allows_restart() {
        let dir = std::env::temp_dir().join(format!("lagw-ctrl-fail-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let controller = Arc::new(ServerController::load(dir.clone()).await.unwrap());
        let port = free_port();
        controller.set_port(port).await.unwrap();
        let mut accepted = false;
        for _ in 0..50 {
            if tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_ok()
            {
                accepted = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(accepted, "gateway must accept connections");

        // 完全按 serve 任务上报异常退出的方式注入：经由槽位上持有的
        // serve 完成通道。
        let serve_done = {
            let slot = controller.server.lock().await;
            let running = slot.as_ref().expect("runtime must be live");
            assert!(
                !running.serve_ended.load(Ordering::SeqCst),
                "serve must not have ended yet"
            );
            running.serve_done.clone()
        };
        serve_done
            .send(Err(anyhow::anyhow!("injected accept failure")))
            .await
            .expect("completion channel must be open");

        // monitor 回收槽位并排空运行时。
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if controller.server.lock().await.is_none() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the monitor must reap the dead slot");
        let state = controller.state().await;
        assert!(!state.running, "a dead runtime must report stopped");
        assert!(
            state
                .error
                .as_deref()
                .is_some_and(|error| error.contains("injected")),
            "the serve error must be visible: {:?}",
            state.error
        );

        // 同一端口上的新 start() 必须真正重新监听。
        controller.start().await.unwrap();
        let mut reaccepted = false;
        for _ in 0..50 {
            if tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_ok()
            {
                reaccepted = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(reaccepted, "restart must rebind the same port");
        let state = controller.state().await;
        assert!(state.running, "restarted runtime must report running");
        controller.stop().await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 并发的 `start()` 调用必须只产生一个运行时 generation——slot 锁串行化它们，
    /// 失败者观察到的是存活槽位，而不是再建一个无人持有的运行时。
    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_starts_yield_one_generation() {
        let dir = std::env::temp_dir().join(format!("lagw-ctrl-race-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let controller = Arc::new(ServerController::load(dir.clone()).await.unwrap());
        let port = free_port();
        controller.config.write().await.port = port;
        let mut starters = Vec::new();
        for _ in 0..5 {
            let controller = Arc::clone(&controller);
            starters.push(tokio::spawn(async move { controller.start().await }));
        }
        for starter in starters {
            starter.await.unwrap().unwrap();
        }
        let slot = controller.server.lock().await;
        let running = slot.as_ref().expect("exactly one runtime must be live");
        assert_eq!(
            running.generation, 1,
            "only the first start() may create a generation"
        );
        drop(slot);
        controller.stop().await;
        assert!(!controller.state().await.running);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
