//! Channel health probing and automatic circuit recovery — port of
//! backend/app/services/health.py.
//!
//! `probe` checks a channel with a minimal per-protocol request and treats a
//! response as healthy only when it is 2xx **and** carries a completion or
//! first-token signal (a 2xx JSON error body must not reset the circuit).
//! Probe failures open the circuit immediately with the configured
//! `circuit_open_seconds` cooldown.
//!
//! `run_supervisor` is the background loop that re-probes every open,
//! enabled channel once its `disabled_until` has passed — the Rust equivalent
//! of the Python `HealthSupervisor` half-open recovery (C3). The
//! [`RuntimeSupervisor`] registers it directly as a task body; this module
//! never spawns tasks itself.

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use tokio::sync::Mutex;
use tokio::time::{Instant, timeout};
use tokio_util::sync::CancellationToken;

use crate::{application::Context, protocol, settings};

/// The per-protocol probe body/endpoint — mirror of `adapter.health_probe`.
pub async fn queue(state: Context, channel_id: String) -> Result<()> {
    let background = state.background.clone();
    let cancel = background.cancel.clone();
    let task_channel_id = channel_id.clone();
    if background
        .spawn_tracked("manual_probe", async move {
            match probe(&state, &task_channel_id, cancel).await {
                Ok(_) => crate::infrastructure::TaskOutcome::Success,
                Err(error) => {
                    // A cancelled probe is a normal shutdown outcome, not a
                    // task failure (P2-8).
                    if !state.background.cancel.is_cancelled() {
                        tracing::warn!(channel_id = %task_channel_id, %error, "manual probe failed");
                        crate::infrastructure::TaskOutcome::Failed
                    } else {
                        crate::infrastructure::TaskOutcome::Cancelled
                    }
                }
            }
        })
        .await
        .is_err()
    {
        // Shutdown already started: the manual probe simply never runs.
        tracing::warn!(channel_id, "probe not started: runtime shutting down");
    }
    Ok(())
}

/// Probe one channel and update its health state. A failed probe opens the
/// circuit immediately (Python uses threshold 1 for probes) with the runtime
/// `circuit_open_seconds` cooldown; a successful probe resets it.
///
/// Health is credential-level: every protocol configured on the channel is
/// probed, one `health_probe_logs` row per (channel, protocol). The channel
/// is only marked active when every protocol with a usable model succeeds;
/// any failure opens the circuit (conservative: a broken protocol entry
/// would fail real requests anyway). A protocol without any usable model is
/// skipped — it cannot serve requests until discovery registers models for
/// it, so it must not drag the whole channel into the open state (providers
/// whose protocols are only partially covered would otherwise never probe
/// healthy).
///
/// `cancel` aborts the in-flight send / body read so shutdown never waits
/// One protocol's probe outcome, collected during the network phase and
/// persisted atomically with the channel verdict (P2-6).
struct ProbeResult {
    protocol: String,
    model: String,
    started_at: String,
    duration_ms: i64,
    success: bool,
    status_code: Option<i64>,
    error_kind: Option<String>,
    next_probe_at: Option<String>,
}

/// on a silent upstream (P1-6).
async fn probe(state: &Context, channel_id: &str, cancel: CancellationToken) -> Result<bool> {
    let row = state
        .channels
        .load_channel(channel_id)
        .await?
        .context("Channel not found")?;
    let default_protocol = row.protocol;
    let configured_model = row.health_check_model_id;
    // The probe model is chosen per protocol: the global
    // `health_check_model_id` is only used for protocols it actually
    // supports; otherwise the first available model of that protocol is
    // used. A protocol without any usable model is skipped below.
    let global_model = configured_model.filter(|value| !value.is_empty());
    let mut protocols: Vec<String> = sqlx::query_scalar(
        "SELECT protocol FROM channel_protocols WHERE channel_id=? ORDER BY rowid",
    )
    .bind(channel_id)
    .fetch_all(state.db.pool())
    .await?;
    if protocols.is_empty() {
        protocols.push(default_protocol);
    }
    let key = state.secrets.decrypt(&row.api_key_encrypted)?;
    let mut results: Vec<ProbeResult> = Vec::with_capacity(protocols.len());
    let base = row.base_url;
    let runtime = settings::runtime_settings(state).await?;
    let open_seconds = runtime.circuit_open_seconds.max(1);
    let mut successes = 0usize;
    let mut probed = 0usize;
    let mut first_failure: Option<(bool, Option<i64>, Option<String>)> = None;
    for protocol_name in &protocols {
        let model = match &global_model {
            Some(model) => {
                let supported: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM channel_models cm \
                     JOIN channel_model_protocols cmp ON cmp.channel_model_id = cm.id \
                     WHERE cm.channel_id=? AND cm.model_id=? AND cmp.protocol=? AND cm.available=1",
                )
                .bind(channel_id)
                .bind(model)
                .bind(protocol_name)
                .fetch_one(state.db.pool())
                .await?;
                if supported > 0 {
                    Some(model.clone())
                } else {
                    None
                }
            }
            None => None,
        };
        let model = match model {
            Some(model) => model,
            None => sqlx::query_scalar(
                "SELECT cm.model_id FROM channel_models cm \
                 JOIN channel_model_protocols cmp ON cmp.channel_model_id = cm.id \
                 WHERE cm.channel_id=? AND cmp.protocol=? AND cm.available=1 \
                 ORDER BY cm.model_id LIMIT 1",
            )
            .bind(channel_id)
            .bind(protocol_name)
            .fetch_optional(state.db.pool())
            .await?
            .unwrap_or_default(),
        };
        if model.is_empty() {
            // No usable model for this protocol: there is nothing to probe
            // with and no request can be routed to this channel for the
            // protocol until discovery registers one. Skip it instead of
            // failing the whole channel (P1-9 fallback semantics).
            continue;
        }
        let Some((path, body)) = protocol::health_probe(protocol_name, &model) else {
            if first_failure.is_none() {
                first_failure = Some((
                    false,
                    None,
                    Some(format!("unsupported protocol {protocol_name}")),
                ));
            }
            continue;
        };
        let url = protocol::upstream_url(&base, &path, None, protocol_name)?;
        let mut headers =
            protocol::outbound_headers(&axum::http::HeaderMap::new(), protocol_name, &key)?;
        if protocol::requires_opencode_session(&base) {
            let session_id = settings::opencode_session_id(&state.db).await;
            protocol::apply_opencode_session(&mut headers, &base, &session_id)?;
        }
        // Probes originate a JSON body without inbound headers to copy;
        // several upstreams (e.g. opencode.ai) reject the request with a 500
        // when Content-Type is missing.
        if !headers.contains_key(axum::http::header::CONTENT_TYPE) {
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                axum::http::HeaderValue::from_static("application/json"),
            );
        }
        let payload = serde_json::to_vec(&body)?;
        probed += 1;
        let started = Instant::now();
        // P2-1: probes go through the upstream port; the port applies the
        // connect timeout and the send deadline.
        let result = tokio::select! {
            _ = cancel.cancelled() => {
                return Err(anyhow::anyhow!("probe cancelled"));
            }
            result = state.http.send(crate::ports::UpstreamRequest {
                url,
                headers,
                method: http::Method::POST,
                body: Some(bytes::Bytes::from(payload)),
                connect_timeout: std::time::Duration::from_secs(
                    runtime.connect_timeout_seconds.max(1) as u64,
                ),
                deadline: state.limits.probe_timeout,
            }) => result,
        };
        let (success, status, error_kind) = match result {
            Ok(response) => {
                let status_code = response.status;
                let status = status_code.as_u16() as i64;
                let body = tokio::select! {
                    _ = cancel.cancelled() => {
                        return Err(anyhow::anyhow!("probe cancelled"));
                    }
                    result = timeout(
                        state.limits.probe_timeout,
                        response.body.read_capped(state.limits.error_body_max),
                    ) => result,
                };
                let body = match body {
                    Ok((body, _truncated)) => body,
                    Err(_) => Vec::new(),
                };
                (
                    status_code.is_success() && protocol::probe_body_ok(protocol_name, &body),
                    Some(status),
                    None,
                )
            }
            Err(error) => (false, None, Some(format!("{error:?}"))),
        };
        results.push(ProbeResult {
            protocol: protocol_name.clone(),
            model,
            started_at: state.clock.now_utc().to_rfc3339(),
            duration_ms: started.elapsed().as_millis() as i64,
            success,
            status_code: status,
            error_kind,
            next_probe_at: if success {
                None
            } else {
                Some((state.clock.now_utc() + chrono::Duration::seconds(open_seconds)).to_rfc3339())
            },
        });
        if success {
            successes += 1;
        } else if first_failure.is_none() {
            first_failure = Some((success, status, results.last().unwrap().error_kind.clone()));
        }
    }
    // P2-6: all probe logs and the channel verdict commit in ONE
    // transaction — a partial failure can never show logs that disagree
    // with the circuit state, and the verdict can never be lost to a
    // mid-write error.
    // A channel with nothing to probe (no model for any protocol) is a
    // failure, not a success: discovery has not run or found anything.
    let all_ok = probed > 0 && successes == probed;
    let time = state.clock.now_utc().to_rfc3339();
    let mut tx = state.db.pool().begin().await?;
    for result in &results {
        sqlx::query("INSERT INTO health_probe_logs(id,channel_id,protocol,model_id,started_at,duration_ms,success,status_code,error_kind,next_probe_at) VALUES(?,?,?,?,?,?,?,?,?,?)")
            .bind(uuid::Uuid::new_v4().to_string()).bind(channel_id).bind(&result.protocol).bind(&result.model).bind(&result.started_at).bind(result.duration_ms).bind(result.success).bind(result.status_code).bind(&result.error_kind).bind(&result.next_probe_at).execute(&mut *tx).await?;
    }
    if all_ok {
        sqlx::query("UPDATE channel_health SET state='active',consecutive_failures=0,disabled_until=NULL,last_success_at=?,last_error_kind=NULL,last_status_code=NULL,updated_at=? WHERE channel_id=?")
            .bind(&time).bind(&time).bind(channel_id).execute(&mut *tx).await?;
    } else {
        let (_success, status, error_kind) = first_failure.unwrap_or((
            false,
            None,
            Some("no probe model for any protocol".into()),
        ));
        sqlx::query("UPDATE channel_health SET consecutive_failures=consecutive_failures+1,last_failure_at=?,last_error_kind=?,last_status_code=?,state='open',disabled_until=?,updated_at=? WHERE channel_id=?")
            .bind(&time).bind(error_kind).bind(status).bind((chrono::Utc::now() + chrono::Duration::seconds(open_seconds)).to_rfc3339()).bind(&time).bind(channel_id).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(all_ok)
}

/// Background supervisor loop: polls every few seconds for open channels
/// whose cooldown has expired and probes them (deduplicated while in
/// flight). Mirrors the Python `HealthSupervisor` auto-recovery loop (C3).
///
/// This is the task body itself: the [`RuntimeSupervisor`] registers it
/// directly, so no `tokio::spawn` boundary can detach it (P1-1). The probe
/// tasks run in a [`ProbeSet`] owned by this task so cancellation can drain
/// them (probes are bounded by `RuntimeLimits::probe_timeout`) before the
/// task returns; if this task is aborted, the `ProbeSet`'s `Drop` aborts
/// every probe synchronously, so no probe outlives its owner.
pub async fn run_supervisor(state: Context, cancel: CancellationToken) {
    let probing: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let mut probes = ProbeSet::default();
    let mut interval = tokio::time::interval(state.limits.probe_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = interval.tick() => {
                let due: Vec<String> = match sqlx::query_scalar(
                    "SELECT ch.channel_id FROM channel_health ch \
                     JOIN channels c ON c.id = ch.channel_id \
                     WHERE ch.state = 'open' AND c.manual_enabled = 1 \
                       AND ch.disabled_until IS NOT NULL AND ch.disabled_until <= ?",
                )
                .bind(chrono::Utc::now().to_rfc3339())
                .fetch_all(state.db.pool())
                .await
                {
                    Ok(due) => due,
                    Err(error) => {
                        tracing::warn!(%error, "health supervisor query failed");
                        continue;
                    }
                };
                for channel_id in due {
                    let mut guard = probing.lock().await;
                    if guard.contains(&channel_id) {
                        continue;
                    }
                    guard.insert(channel_id.clone());
                    drop(guard);
                    let state = state.clone();
                    let probing = probing.clone();
                    let cancel = cancel.clone();
                    probes.spawn(async move {
                        let outcome = match probe(&state, &channel_id, cancel).await {
                            Ok(_) => crate::infrastructure::TaskOutcome::Success,
                            Err(error) => {
                                if !state.background.cancel.is_cancelled() {
                                    tracing::warn!(channel_id, %error, "health probe task failed");
                                    crate::infrastructure::TaskOutcome::Failed
                                } else {
                                    crate::infrastructure::TaskOutcome::Cancelled
                                }
                            }
                        };
                        // P2-8: probe failures are counted even though
                        // this task lives in the supervisor's own set.
                        if outcome != crate::infrastructure::TaskOutcome::Success {
                            state.background.record_failure();
                        }
                        probing.lock().await.remove(&channel_id);
                    });
                }
            }
        }
    }
    // Abort every in-flight probe: each is cancellable now (P1-6), so
    // the supervisor exits promptly instead of waiting out 20s timeouts.
    probes.shutdown().await;
}

/// Owns the in-flight probe `JoinSet`. A graceful cancellation joins every
/// probe via [`Self::shutdown`]; if this guard's owner is aborted (shutdown
/// deadline exceeded), `Drop` aborts every probe synchronously — a probe
/// never outlives the health supervisor task, so no probe holds the
/// database pool after a drain returns.
#[derive(Default)]
struct ProbeSet {
    tasks: tokio::task::JoinSet<()>,
}

impl ProbeSet {
    fn spawn<F>(&mut self, future: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.tasks.spawn(future);
    }

    async fn shutdown(&mut self) {
        self.tasks.shutdown().await;
    }
}

impl Drop for ProbeSet {
    fn drop(&mut self) {
        self.tasks.abort_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{application::Context, config::AppConfig, db::Database};
    use serde_json::Value;
    use std::sync::Arc;
    use std::time::Duration;

    /// Upstream that answers every POST with `200 + body` (or `500` when
    /// `fail_claude` is set and the path is the claude endpoint).
    async fn spawn_upstream(fail_claude: bool) -> u16 {
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
                    let claude = request.contains("/v1/messages");
                    let (status, body) = if claude && fail_claude {
                        (
                            "500 Internal Server Error",
                            r#"{"error":{"message":"boom"}}"#,
                        )
                    } else if claude {
                        (
                            "200 OK",
                            r#"{"stop_reason":"end_turn","content":[{"type":"text","text":"OK"}]}"#,
                        )
                    } else {
                        (
                            "200 OK",
                            r#"{"choices":[{"finish_reason":"stop","message":{"content":"OK"}}]}"#,
                        )
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        });
        port
    }

    async fn test_state(upstream_port: u16) -> (Context, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("lagw-health-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = Database::open(&dir.join("test.db")).await.unwrap();
        let secrets = crate::crypto::SecretStore::load(&dir.join("master.key"))
            .await
            .unwrap();
        let (telemetry, _rx) = crate::telemetry::Telemetry::new(1000);
        let http: Arc<dyn crate::ports::UpstreamClient> =
            Arc::new(crate::infrastructure::HttpClientPool::default());
        let routes: Arc<dyn crate::ports::RouteRepository> =
            crate::infrastructure::SqliteRouteRepository::new(db.clone());
        let channels: Arc<dyn crate::ports::ChannelRepository> =
            crate::infrastructure::SqliteChannelRepository::new(db.clone());
        let clock: Arc<dyn crate::ports::Clock> = Arc::new(crate::infrastructure::SystemClock);
        let background = crate::infrastructure::RuntimeSupervisor::new(
            tokio_util::sync::CancellationToken::new(),
        );
        let limits = Arc::new(crate::runtime::RuntimeLimits::default());
        let discovery = crate::discovery::DiscoveryService::new(
            db.clone(),
            secrets.clone(),
            Arc::clone(&http),
            Arc::clone(&channels),
            Arc::clone(&clock),
            Arc::clone(&background),
            Arc::clone(&limits),
        );
        let notifier: std::sync::Arc<dyn crate::ports::Notifier> =
            crate::notification::DesktopNotifier::new(std::time::Duration::from_millis(50));
        let proxy = crate::proxy::ProxyService::new(
            db.clone(),
            secrets.clone(),
            Arc::clone(&http),
            routes.clone(),
            telemetry.clone(),
            Arc::clone(&clock),
            Arc::clone(&limits),
            crate::notification::DesktopNotifier::new(std::time::Duration::from_millis(50)),
        );
        let balance = crate::balance::BalanceService::new(
            db.clone(),
            secrets.clone(),
            Arc::clone(&http),
            channels.clone(),
            Arc::clone(&clock),
            Arc::clone(&limits),
        );
        let state = crate::application::Context {
            config: Arc::new(AppConfig::default()),
            db: db.clone(),
            secrets: secrets.clone(),
            http,
            routes,
            channels,
            clock,
            notifier,
            discovery,
            proxy,
            telemetry,
            background,
            limits,
            balance,
            admin: crate::admin::AdminService::new(db.clone(), secrets.clone()),
            recovery: crate::auth::RecoverySession::new(),
        };
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("INSERT INTO providers(id,name,base_url,created_at,updated_at) VALUES('prov-1','mock',?,?,?)")
            .bind(format!("http://127.0.0.1:{upstream_port}"))
            .bind(time)
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channels(id,provider_id,name,protocol,api_key_encrypted,api_key_hint,manual_enabled,created_at,updated_at) VALUES('ch-1','prov-1','chan','openai_compatible',?,?,1,?,?)")
            .bind(secrets.encrypt("test-key"))
            .bind("...key")
            .bind(time)
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_protocols(channel_id,protocol) VALUES('ch-1','openai_compatible'),('ch-1','claude')")
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_models(id,channel_id,model_id,display_name,source,available,first_seen_at,created_at,updated_at) VALUES('cm-1','ch-1','probe-model','Probe','discovered',1,?,?,?)")
            .bind(time)
            .bind(time)
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES('cm-1','openai_compatible'),('cm-1','claude')")
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_health(channel_id,state,consecutive_failures,updated_at) VALUES('ch-1','open',0,?)")
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
        (state, dir)
    }

    /// P2-7: health is credential-level — both configured protocols must
    /// succeed for the channel to be active; one failing protocol opens it.
    #[tokio::test]
    async fn multi_protocol_probe_aggregates_all_protocols() {
        let port = spawn_upstream(false).await;
        let (state, _dir) = test_state(port).await;
        let ok = probe(&state, "ch-1", CancellationToken::new())
            .await
            .unwrap();
        assert!(ok, "all protocols healthy -> probe succeeds");
        // A second sequential probe on the same state must also work
        // (regression: pooled connections must not wedge the channel query).
        let ok = probe(&state, "ch-1", CancellationToken::new())
            .await
            .unwrap();
        assert!(ok, "second sequential probe must succeed");
        let (state_health, failures): (String, i64) = sqlx::query_as(
            "SELECT state, consecutive_failures FROM channel_health WHERE channel_id='ch-1'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(state_health, "active");
        assert_eq!(failures, 0);
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM health_probe_logs")
            .fetch_one(state.db.pool())
            .await
            .unwrap();
        assert_eq!(rows, 4, "one probe log row per configured protocol per run");

        // Second scenario: the claude entry is broken.
        let port = spawn_upstream(true).await;
        let (state, _dir) = test_state(port).await;
        let ok = probe(&state, "ch-1", CancellationToken::new())
            .await
            .unwrap();
        assert!(!ok, "one failing protocol -> probe fails");
        let (state_health, failures): (String, i64) = sqlx::query_as(
            "SELECT state, consecutive_failures FROM channel_health WHERE channel_id='ch-1'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(state_health, "open");
        assert_eq!(failures, 1);
    }

    /// P2-6: probe logs and the channel verdict commit atomically — a
    /// mid-write failure (fault-injected via a SQL trigger) rolls back BOTH,
    /// so logs can never disagree with the circuit state.
    #[tokio::test]
    async fn probe_verdict_rolls_back_with_logs() {
        let port = spawn_upstream(false).await;
        let (state, _dir) = test_state(port).await;
        sqlx::query(
            "CREATE TRIGGER fail_probe_log BEFORE INSERT ON health_probe_logs \
             BEGIN SELECT RAISE(ABORT, 'boom'); END",
        )
        .execute(state.db.pool())
        .await
        .unwrap();
        let result = probe(&state, "ch-1", CancellationToken::new()).await;
        assert!(result.is_err(), "the injected write failure must surface");
        let (state_health, failures): (String, i64) = sqlx::query_as(
            "SELECT state, consecutive_failures FROM channel_health WHERE channel_id='ch-1'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(
            state_health, "open",
            "the channel verdict must roll back together with the logs"
        );
        assert_eq!(failures, 0);
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM health_probe_logs")
            .fetch_one(state.db.pool())
            .await
            .unwrap();
        assert_eq!(rows, 0, "no partial probe logs may survive the rollback");
    }

    /// P1-6: a cancelled token aborts an in-flight probe against a silent
    /// upstream instead of waiting out the 20s timeout.
    #[tokio::test]
    async fn probe_cancelled_while_upstream_silent() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                use tokio::io::AsyncReadExt;
                // Never answer; hold the connection open.
                let _ = stream.read(&mut [0u8; 4096]).await;
            }
        });
        let (state, _dir) = test_state(port).await;
        let cancel = CancellationToken::new();
        cancel.cancel();
        let started = Instant::now();
        let result = probe(&state, "ch-1", cancel).await;
        assert!(result.is_err(), "a cancelled probe must abort");
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "a cancelled probe must return promptly, not wait out the timeout"
        );
    }

    /// Model-aware upstream: reads the request body's `model` field. The
    /// claude path fails for model `A` and succeeds for `B`; the openai path
    /// succeeds for any model.
    async fn spawn_upstream_model_aware() -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buf = [0u8; 8192];
                    let mut total = 0usize;
                    let mut needed = usize::MAX;
                    loop {
                        match stream.read(&mut buf[total..]).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => {
                                total += n;
                                if needed == usize::MAX {
                                    let text = String::from_utf8_lossy(&buf[..total]);
                                    if let Some(pos) = text.find("\r\n\r\n") {
                                        let content_length = text[..pos]
                                            .lines()
                                            .find_map(|line| {
                                                line.to_ascii_lowercase()
                                                    .strip_prefix("content-length:")
                                                    .and_then(|v| v.trim().parse().ok())
                                            })
                                            .unwrap_or(0);
                                        needed = pos + 4 + content_length;
                                    }
                                }
                                if total >= needed {
                                    break;
                                }
                            }
                        }
                    }
                    let request = String::from_utf8_lossy(&buf[..total]);
                    let claude = request.contains("/v1/messages");
                    let body_start = request.find("\r\n\r\n").map(|pos| pos + 4).unwrap_or(total);
                    let model = serde_json::from_str::<Value>(&request[body_start..])
                        .ok()
                        .and_then(|value| {
                            value
                                .get("model")
                                .and_then(Value::as_str)
                                .map(str::to_owned)
                        })
                        .unwrap_or_default();
                    let (status, body) = if claude {
                        if model == "A" {
                            (
                                "500 Internal Server Error",
                                r#"{"error":{"message":"boom"}}"#,
                            )
                        } else {
                            (
                                "200 OK",
                                r#"{"stop_reason":"end_turn","content":[{"type":"text","text":"OK"}]}"#,
                            )
                        }
                    } else {
                        (
                            "200 OK",
                            r#"{"choices":[{"finish_reason":"stop","message":{"content":"OK"}}]}"#,
                        )
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        });
        port
    }

    /// P1-9: each protocol probes with its own model. The global
    /// `health_check_model_id` (`A`) only covers openai; the claude path must
    /// fall back to the first claude-available model (`B`) — pre-fix the
    /// claude probe used `A` and failed, opening the circuit.
    #[tokio::test]
    async fn multi_protocol_probe_uses_per_protocol_models() {
        let port = spawn_upstream_model_aware().await;
        let (state, _dir) = test_state(port).await;
        let time = chrono::Utc::now().to_rfc3339();
        sqlx::query("DELETE FROM channel_model_protocols WHERE channel_model_id='cm-1'")
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM channel_models WHERE id='cm-1'")
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_models(id,channel_id,model_id,display_name,source,available,first_seen_at,created_at,updated_at) VALUES('cm-a','ch-1','A','Model A','discovered',1,?,?,?),('cm-b','ch-1','B','Model B','discovered',1,?,?,?)")
            .bind(&time)
            .bind(&time)
            .bind(&time)
            .bind(&time)
            .bind(&time)
            .bind(&time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES('cm-a','openai_compatible'),('cm-b','claude')")
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE channels SET health_check_model_id='A' WHERE id='ch-1'")
            .execute(state.db.pool())
            .await
            .unwrap();
        let ok = probe(&state, "ch-1", CancellationToken::new())
            .await
            .unwrap();
        assert!(ok, "each protocol must probe with a model it supports");
        let (state_health, failures): (String, i64) = sqlx::query_as(
            "SELECT state, consecutive_failures FROM channel_health WHERE channel_id='ch-1'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(state_health, "active");
        assert_eq!(failures, 0);
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT protocol, model_id FROM health_probe_logs WHERE channel_id='ch-1' ORDER BY protocol",
        )
        .fetch_all(state.db.pool())
        .await
        .unwrap();
        assert_eq!(rows.len(), 2, "one probe row per configured protocol");
        assert!(rows.contains(&("openai_compatible".into(), "A".into())));
        assert!(rows.contains(&("claude".into(), "B".into())));
    }

    /// A protocol without any usable model is skipped: the channel verdict
    /// is decided by the protocols that could actually be probed. A broken
    /// provider with partial protocol coverage must not sit in the open
    /// state forever.
    #[tokio::test]
    async fn protocol_without_model_is_skipped_while_others_pass() {
        let port = spawn_upstream(false).await;
        let (state, _dir) = test_state(port).await;
        // Drop the claude binding: no channel model supports claude anymore.
        sqlx::query("DELETE FROM channel_model_protocols WHERE channel_model_id='cm-1' AND protocol='claude'")
            .execute(state.db.pool())
            .await
            .unwrap();
        let ok = probe(&state, "ch-1", CancellationToken::new())
            .await
            .unwrap();
        assert!(ok, "the probed protocol succeeded; claude is simply skipped");
        let (state_health, failures): (String, i64) = sqlx::query_as(
            "SELECT state, consecutive_failures FROM channel_health WHERE channel_id='ch-1'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(state_health, "active");
        assert_eq!(failures, 0);
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM health_probe_logs")
            .fetch_one(state.db.pool())
            .await
            .unwrap();
        assert_eq!(rows, 1, "only the probed protocol logs a row");
    }

    /// When NO protocol has a usable model there is nothing to probe; the
    /// probe fails explicitly instead of silently declaring the channel
    /// healthy.
    #[tokio::test]
    async fn probe_without_any_model_fails_explicitly() {
        let port = spawn_upstream(false).await;
        let (state, _dir) = test_state(port).await;
        sqlx::query("DELETE FROM channel_model_protocols WHERE channel_model_id='cm-1'")
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM channel_models WHERE id='cm-1'")
            .execute(state.db.pool())
            .await
            .unwrap();
        let ok = probe(&state, "ch-1", CancellationToken::new())
            .await
            .unwrap();
        assert!(!ok, "no probe model for any protocol -> probe fails");
        let (state_health, error_kind): (String, Option<String>) = sqlx::query_as(
            "SELECT state, last_error_kind FROM channel_health WHERE channel_id='ch-1'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(state_health, "open");
        assert!(
            error_kind
                .as_deref()
                .is_some_and(|kind| kind.contains("no probe model")),
            "the failure must name the missing model, got {error_kind:?}"
        );
    }
}
