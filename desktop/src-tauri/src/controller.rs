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
    /// Owns serve + every background task (P1-3): stopping the server is one
    /// bounded [`RuntimeSupervisor::shutdown`] — no detached handles.
    supervisor: Arc<RuntimeSupervisor>,
    /// Absolute shutdown deadline for this runtime (P2-10).
    limits: crate::runtime::RuntimeLimits,
    /// Generation of this runtime (P1-2): a serve-exit monitor from an
    /// earlier generation never reaps a newer slot.
    generation: u64,
    /// Set once the serve task has returned (normal or abnormal). Lets
    /// `start()` recognise a dead runtime before its monitor reaped it.
    serve_ended: Arc<AtomicBool>,
    /// Serve completion channel, cloned into the serve task. Kept on the
    /// slot so tests can inject an abnormal serve exit.
    #[allow(dead_code)] // read by tests to inject serve failures
    serve_done: mpsc::Sender<anyhow::Result<()>>,
    /// Controller-owned monitor: watches the serve completion channel and
    /// reaps the slot + drains the runtime on an abnormal serve exit.
    /// Explicitly held here (NOT inside the supervisor's JoinSet) so it can
    /// call [`RuntimeSupervisor::shutdown`] without waiting on itself.
    monitor: tokio::task::JoinHandle<()>,
}

pub struct ServerController {
    platform_data_dir: PathBuf,
    config: RwLock<AppConfig>,
    runtime: RwLock<RuntimeStatus>,
    server: Mutex<Option<RunningServer>>,
    /// Monotonic runtime generation counter; bumped on every `start()`.
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

    /// Starts the gateway. The slot lock is held across the whole start so
    /// concurrent `start`/`stop`/`set_port` can never produce two live
    /// generations (P1-2). A slot whose serve task already returned is
    /// reaped first: its background tasks are drained, then a fresh runtime
    /// takes the slot.
    pub async fn start(self: &Arc<Self>) -> Result<()> {
        let mut slot = self.server.lock().await;
        if let Some(running) = slot.as_ref()
            && !running.serve_ended.load(Ordering::SeqCst)
        {
            // A live runtime (serve still accepting) is already running.
            return Ok(());
        }
        // Stale slot: the serve task already returned (abnormal exit the
        // monitor has not reaped yet, or a post-request_stop slot). Drain
        // its background tasks before starting fresh.
        let stale = slot.take();
        if let Some(stale) = stale {
            stale.supervisor.shutdown(stale.limits.shutdown_deadline).await;
            // The stale monitor (controller-owned, outside the JoinSet)
            // wakes, sees the new generation or an empty slot, and exits on
            // its own — nothing here joins it, so no self-wait is possible.
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
        // The listener is bound: the runtime is running from this point.
        // Set the flag synchronously so `start()` returning means a live
        // runtime, and the serve task only records the exit side.
        {
            let mut runtime = self.runtime.write().await;
            runtime.running = true;
            runtime.error = None;
        }
        // The serve task joins the supervisor's JoinSet too (P1-3): shutdown
        // covers the HTTP server just like every background task. It only
        // does state bookkeeping and signals completion; the slot reap and
        // unified shutdown on abnormal exit belong to the controller-owned
        // monitor below.
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
            .expect("start must not race shutdown");
        // Controller-owned monitor (P1-2): watches the serve completion
        // channel and, on an abnormal exit, reaps the slot and drains the
        // runtime so health/maintenance/telemetry do not keep running under
        // a dead HTTP server. It lives OUTSIDE the supervisor's JoinSet and
        // is explicitly held by `RunningServer`, so it can call
        // `shutdown()` without waiting on itself.
        let monitor_controller = Arc::clone(self);
        let monitor_generation = generation;
        let monitor = tokio::spawn(async move {
            match serve_done_rx.recv().await {
                Some(Ok(())) => {
                    // Graceful stop: the owner (stop()/request_stop) reaped
                    // the slot and drained the supervisor; nothing to do.
                }
                Some(Err(error)) => {
                    // Abnormal serve exit. Reap only if this monitor's
                    // generation is still in the slot — a newer runtime must
                    // not be torn down by a stale monitor.
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
                        // The serve task already returned abnormally; mark
                        // the runtime stopped BEFORE the (bounded) drain so
                        // observers never see a dead runtime as running.
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
            // One bounded drain covers serve + telemetry tail + every
            // background task; the deadline is the runtime's absolute
            // shutdown bound, after which remaining tasks are aborted and
            // joined (P1-3) — nothing is ever detached.
            running
                .supervisor
                .shutdown(running.limits.shutdown_deadline)
                .await;
            // The serve task has returned (graceful shutdown), so the
            // monitor received its completion signal; the slot is gone, so
            // it exits without touching anything. Join it briefly so no
            // controller-owned task outlives stop().
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
    use std::time::Duration;

    /// Accept-loop upstream: a real `/v1/models` catalog (so the maintenance
    /// supervisor's startup discovery keeps the seeded model available) plus
    /// a healthy chat-completions response.
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

    /// Report §8 scenario 3: restarting on a new port must not leak the old
    /// health supervisor — each 5s cycle produces exactly one probe row.
    #[tokio::test(flavor = "multi_thread")]
    async fn port_restart_keeps_single_supervisor() {
        let upstream_port = spawn_upstream().await;
        let dir = std::env::temp_dir().join(format!("lagw-ctrl-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();

        // Seed the channel DB exactly as the runtime would see it, then close
        // the seeding pool so only the controller's own pool is active — the
        // production shape. The key file must exist before encryption:
        // SecretStore::load creates it.
        let secrets = crate::crypto::SecretStore::load(&dir.join("master.key"))
            .await
            .unwrap();
        let seed_db = Database::open(&dir.join("gateway.db")).await.unwrap();
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

        let controller = ServerController::load(dir.clone()).await.unwrap();
        let first_port = free_port();
        controller.set_port(first_port).await.unwrap();
        // The first supervisor tick fires immediately, then every 5s.
        let db = Database::open(&dir.join("gateway.db")).await.unwrap();
        wait_for_probe_count(&db, 1, Duration::from_secs(8)).await;

        // Re-arm the circuit and restart on a new port.
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
        // Stop must join every background handle; no leaked supervisor can
        // produce a third probe row afterwards.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM health_probe_logs")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(count, 2, "no probe may fire after stop");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// P1-5: graceful shutdown drains the telemetry tail. A streaming request
    /// that is in flight when `stop()` is called must still land its success
    /// finish in request_logs — the writer keeps draining while shutdown
    /// completes, instead of exiting at the first cancel tick.
    ///
    /// The response and `stop()` are driven concurrently: `serve`'s graceful
    /// shutdown waits for the in-flight connection, and the connection only
    /// completes once the client consumes the stream, so awaiting `stop()`
    /// before reading the response would deadlock.
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
                        // Keep the discovery-scheduled catalog healthy.
                        let body = r#"{"object":"list","data":[{"id":"probe-model","object":"model","owned_by":"test"}]}"#;
                        let response = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                            body.len()
                        );
                        let _ = stream.write_all(response.as_bytes()).await;
                        return;
                    }
                    // Chat completions: 1s delay, then a complete SSE body.
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
        // The accept loop starts a moment after `bind` returns; wait for the
        // listener before firing the request.
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
        // Fire a streaming request; do not await it yet. `send()` is lazy, so
        // the future runs in a spawned task: the connect happens immediately
        // and the request is truly in flight when `stop()` is called.
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
        // Ensure the request is in flight before stopping.
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

        // The tail of the request log must be persisted even though the
        // writer was cancelled while the request was in flight.
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

    /// P1-2: an abnormal serve exit must reap the slot, drain every
    /// background task, make the error visible, and allow a fresh `start()`
    /// on the same port — the old runtime must not keep the slot or the
    /// listener.
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

        // Inject the abnormal exit exactly as the serve task reports one:
        // through the serve completion channel held on the slot.
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

        // The monitor reaps the slot and drains the runtime.
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

        // A fresh start() on the same port must actually listen again.
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

    /// P1-2: concurrent `start()` calls must yield exactly one runtime
    /// generation — the slot lock serialises them, and the losers observe a
    /// live slot instead of building a second, unowned runtime.
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
