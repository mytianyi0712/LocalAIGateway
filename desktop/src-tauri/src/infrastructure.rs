//! Infrastructure implementations of the application ports (P2-1): reqwest
//! HTTP client pool, SQLite repositories, and the system clock. Business
//! modules never import these directly — they see `ports` traits through
//! [`crate::application::Context`].

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Result;
use futures_util::StreamExt;
use parking_lot::RwLock;
use sqlx::Row;
use tokio_util::sync::CancellationToken;

use crate::{
    db::Database,
    ports::{
        Candidate, ChannelRepository, ChannelRow, Clock, MappingTarget, RoutableModel,
        RouteRepository, UpstreamBody, UpstreamClient, UpstreamError, UpstreamRequest,
        UpstreamResponse,
    },
    remote_compaction::CompactionMode,
};

/// Per-connect-timeout reqwest client pool. reqwest has no per-request
/// connect timeout, so the runtime `connect_timeout_seconds` setting is
/// honored by selecting a client built for that timeout value.
pub struct HttpClientPool {
    clients: RwLock<std::collections::HashMap<i64, reqwest::Client>>,
}

impl Default for HttpClientPool {
    fn default() -> Self {
        Self {
            clients: RwLock::new(std::collections::HashMap::new()),
        }
    }
}

impl HttpClientPool {
    pub fn for_connect_timeout(&self, seconds: i64) -> reqwest::Client {
        let seconds = seconds.max(1);
        if let Some(client) = self.clients.read().get(&seconds) {
            return client.clone();
        }
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(seconds as u64))
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(20)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("reqwest client build");
        self.clients.write().insert(seconds, client.clone());
        client
    }
}

impl UpstreamClient for HttpClientPool {
    fn send(
        &self,
        request: UpstreamRequest,
    ) -> futures_util::future::BoxFuture<'static, Result<UpstreamResponse, UpstreamError>> {
        let client = self.for_connect_timeout(request.connect_timeout.as_secs() as i64);
        Box::pin(async move {
            let method = request.method;
            let mut builder = client.request(method, request.url).headers(request.headers);
            if let Some(body) = request.body {
                builder = builder.body(body);
            }
            let response = match tokio::time::timeout(request.deadline, builder.send()).await {
                Ok(Ok(response)) => response,
                Ok(Err(error)) => {
                    return Err(if error.is_timeout() {
                        UpstreamError::ConnectTimeout
                    } else {
                        UpstreamError::Transport(error.to_string())
                    });
                }
                Err(_) => return Err(UpstreamError::Deadline),
            };
            let status = response.status();
            let headers = response.headers().clone();
            let body =
                UpstreamBody::new(Box::pin(response.bytes_stream().map(|item| {
                    item.map_err(|error| UpstreamError::Transport(error.to_string()))
                })));
            Ok(UpstreamResponse {
                status,
                headers,
                body,
            })
        })
    }
}

/// The system clock: production implementation of the `Clock` port.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_utc(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }
}

/// SQLite implementation of the route port: the candidate / catalog /
/// mapping queries.
pub struct SqliteRouteRepository {
    db: Database,
}

impl SqliteRouteRepository {
    pub fn new(db: Database) -> Arc<Self> {
        Arc::new(Self { db })
    }
}

impl RouteRepository for SqliteRouteRepository {
    fn resolve_candidates(
        &self,
        protocol: &str,
        model_id: &str,
        limit: i64,
    ) -> futures_util::future::BoxFuture<'static, Result<Vec<Candidate>>> {
        let db = self.db.clone();
        let protocol = protocol.to_owned();
        let model_id = model_id.to_owned();
        Box::pin(async move {
            Ok(sqlx::query_as::<_, Candidate>(
            "SELECT rc.id AS candidate_id, c.id AS channel_id, c.name AS channel_name, \
                    rc.priority, p.base_url, p.kind, c.api_key_encrypted, cm.model_id, \
                    COALESCE(cp.remote_compaction_v1_support, 0) AS remote_compaction_v1_support, \
                    COALESCE(cp.remote_compaction_v2_support, 0) AS remote_compaction_v2_support \
             FROM route_candidates rc \
             JOIN model_routes mr ON mr.id = rc.route_id \
             JOIN channel_models cm ON cm.id = rc.channel_model_id \
             JOIN channel_model_protocols cmp ON cmp.channel_model_id = cm.id AND cmp.protocol = mr.protocol \
             JOIN channels c ON c.id = cm.channel_id \
             JOIN providers p ON p.id = c.provider_id \
             JOIN channel_health ch ON ch.channel_id = c.id \
             LEFT JOIN channel_protocols cp ON cp.channel_id = c.id AND cp.protocol = mr.protocol \
             WHERE mr.protocol = ? AND mr.requested_model_id = ? AND mr.enabled = 1 \
               AND rc.enabled = 1 AND cm.available = 1 AND c.manual_enabled = 1 AND ch.state = 'active' \
             ORDER BY rc.priority ASC LIMIT ?"
        )
        .bind(protocol.as_str())
        .bind(model_id.as_str())
        .bind(limit)
        .fetch_all(db.pool())
        .await?)
        })
    }

    fn resolve_compaction_candidates(
        &self,
        protocol: &str,
        model_id: &str,
        mode: CompactionMode,
        limit: i64,
    ) -> futures_util::future::BoxFuture<'static, Result<Vec<Candidate>>> {
        let db = self.db.clone();
        let protocol = protocol.to_owned();
        let model_id = model_id.to_owned();
        let capability_column = match mode {
            CompactionMode::V1 => "cp.remote_compaction_v1_support",
            CompactionMode::V2 => "cp.remote_compaction_v2_support",
        };
        let sql = format!(
            "SELECT rc.id AS candidate_id, c.id AS channel_id, c.name AS channel_name, \
                    rc.priority, p.base_url, p.kind, c.api_key_encrypted, cm.model_id, \
                    COALESCE(cp.remote_compaction_v1_support, 0) AS remote_compaction_v1_support, \
                    COALESCE(cp.remote_compaction_v2_support, 0) AS remote_compaction_v2_support \
             FROM route_candidates rc \
             JOIN model_routes mr ON mr.id = rc.route_id \
             JOIN channel_models cm ON cm.id = rc.channel_model_id \
             JOIN channel_model_protocols cmp ON cmp.channel_model_id = cm.id AND cmp.protocol = mr.protocol \
             JOIN channels c ON c.id = cm.channel_id \
             JOIN providers p ON p.id = c.provider_id \
             JOIN channel_health ch ON ch.channel_id = c.id \
             LEFT JOIN channel_protocols cp ON cp.channel_id = c.id AND cp.protocol = mr.protocol \
             WHERE mr.protocol = ? AND mr.requested_model_id = ? AND mr.enabled = 1 \
               AND rc.enabled = 1 AND cm.available = 1 AND c.manual_enabled = 1 AND ch.state = 'active' \
               AND COALESCE({capability_column}, 0) != 2 \
             ORDER BY rc.priority ASC LIMIT ?"
        );
        Box::pin(async move {
            Ok(sqlx::query_as::<_, Candidate>(&sql)
                .bind(protocol.as_str())
                .bind(model_id.as_str())
                .bind(limit)
                .fetch_all(db.pool())
                .await?)
        })
    }

    fn list_routable_models(
        &self,
        protocol: Option<&str>,
    ) -> futures_util::future::BoxFuture<'static, Result<Vec<RoutableModel>>> {
        let db = self.db.clone();
        let protocol = protocol.map(str::to_owned);
        Box::pin(async move {
            let rows = sqlx::query_as::<_, RoutableModel>(
            "SELECT mr.requested_model_id AS id, \
                    COALESCE((SELECT MAX(cm.display_name) FROM channel_models cm \
                      JOIN route_candidates rc ON rc.channel_model_id = cm.id \
                      JOIN model_routes r2 ON r2.id = rc.route_id \
                      WHERE r2.requested_model_id = mr.requested_model_id AND r2.enabled = 1), \
                     mr.requested_model_id) AS display_name, \
                    MIN(mr.created_at) AS created_at \
             FROM model_routes mr \
             WHERE mr.enabled = 1 AND (? IS NULL OR mr.protocol = ?) \
               AND EXISTS (SELECT 1 FROM route_candidates rc \
                 JOIN channel_models cm ON cm.id = rc.channel_model_id \
                 JOIN channel_model_protocols cmp ON cmp.channel_model_id = cm.id AND cmp.protocol = mr.protocol \
                 JOIN channels c ON c.id = cm.channel_id \
                 JOIN channel_health ch ON ch.channel_id = c.id \
                 WHERE rc.route_id = mr.id AND rc.enabled = 1 AND cm.available = 1 \
                   AND c.manual_enabled = 1 AND ch.state = 'active') \
             GROUP BY mr.requested_model_id ORDER BY mr.requested_model_id"
        )
        .bind(protocol.as_deref())
        .bind(protocol.as_deref())
        .fetch_all(db.pool())
        .await?;
            Ok(rows)
        })
    }

    fn resolve_mapping(
        &self,
        entry: &str,
        model: &str,
    ) -> futures_util::future::BoxFuture<'static, Result<Option<MappingTarget>>> {
        let db = self.db.clone();
        let entry = entry.to_owned();
        let model = model.to_owned();
        Box::pin(async move {
            let (table, id_column) = match entry.as_str() {
                "claude" => ("claude_model_mappings", "claude_model_id"),
                "openai_responses" => ("codex_model_mappings", "codex_model_id"),
                _ => return Ok(None),
            };
            let sql = format!(
                "SELECT upstream_protocol,upstream_model_id FROM {table} WHERE {id_column}=? AND enabled=1"
            );
            let row = sqlx::query(&sql)
                .bind(model)
                .fetch_optional(db.pool())
                .await?;
            Ok(row.map(|row| MappingTarget {
                entry: entry.to_owned(),
                upstream_protocol: row.get("upstream_protocol"),
                upstream_model: row.get("upstream_model_id"),
            }))
        })
    }

    fn routable_endpoints_for_model(
        &self,
        model_id: &str,
    ) -> futures_util::future::BoxFuture<'static, Result<Vec<String>>> {
        let db = self.db.clone();
        let model_id = model_id.to_owned();
        Box::pin(async move {
            let protocols: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT mr.protocol FROM model_routes mr \
             WHERE mr.requested_model_id = ? AND mr.enabled = 1 \
               AND EXISTS (SELECT 1 FROM route_candidates rc \
                 JOIN channel_models cm ON cm.id = rc.channel_model_id \
                 JOIN channel_model_protocols cmp ON cmp.channel_model_id = cm.id AND cmp.protocol = mr.protocol \
                 JOIN channels c ON c.id = cm.channel_id \
                 JOIN channel_health ch ON ch.channel_id = c.id \
                 WHERE rc.route_id = mr.id AND rc.enabled = 1 AND cm.available = 1 \
                   AND c.manual_enabled = 1 AND ch.state = 'active') \
             ORDER BY CASE mr.protocol WHEN 'openai_compatible' THEN 0 \
               WHEN 'openai_responses' THEN 1 WHEN 'claude' THEN 2 WHEN 'command_code' THEN 4 ELSE 3 END",
        )
        .bind(model_id.as_str())
        .fetch_all(db.pool())
        .await?;
            Ok(crate::routing::endpoints_for_protocols(
                &protocols.iter().map(String::as_str).collect::<Vec<_>>(),
                &model_id,
            ))
        })
    }

    fn list_mapping_models(
        &self,
        kind: &str,
    ) -> futures_util::future::BoxFuture<'static, Result<Vec<RoutableModel>>> {
        let db = self.db.clone();
        let kind = kind.to_owned();
        Box::pin(async move {
            let (table, id_column) = match kind.as_str() {
                "claude" => ("claude_model_mappings", "claude_model_id"),
                // codex 映射按入口协议 openai_responses 标识（proxy::mapped_models 传 entry）
                "openai_responses" => ("codex_model_mappings", "codex_model_id"),
                _ => return Ok(Vec::new()),
            };
            let sql = format!(
                "SELECT m.{id_column} AS id, COALESCE(m.display_name, m.{id_column}) AS display_name, m.created_at \
             FROM {table} m WHERE m.enabled = 1 AND EXISTS (SELECT 1 FROM model_routes mr \
               JOIN route_candidates rc ON rc.route_id = mr.id \
               JOIN channel_models cm ON cm.id = rc.channel_model_id \
               JOIN channel_model_protocols cmp ON cmp.channel_model_id = cm.id AND cmp.protocol = mr.protocol \
               JOIN channels c ON c.id = cm.channel_id \
               JOIN channel_health ch ON ch.channel_id = c.id \
               WHERE mr.protocol = m.upstream_protocol AND mr.requested_model_id = m.upstream_model_id \
                 AND mr.enabled = 1 AND rc.enabled = 1 AND cm.available = 1 AND c.manual_enabled = 1 AND ch.state = 'active') \
             ORDER BY m.{id_column}"
            );
            Ok(sqlx::query_as::<_, RoutableModel>(&sql)
                .fetch_all(db.pool())
                .await?)
        })
    }
}

/// SQLite implementation of the channel port: the row loads behind probes
/// and discovery.
pub struct SqliteChannelRepository {
    db: Database,
}

impl SqliteChannelRepository {
    pub fn new(db: Database) -> Arc<Self> {
        Arc::new(Self { db })
    }
}

impl ChannelRepository for SqliteChannelRepository {
    fn load_channel(
        &self,
        channel_id: &str,
    ) -> futures_util::future::BoxFuture<'static, Result<Option<ChannelRow>>> {
        let db = self.db.clone();
        let channel_id = channel_id.to_owned();
        Box::pin(async move {
            let row = sqlx::query(
            "SELECT c.id, c.name, c.protocol, c.health_check_model_id, c.api_key_encrypted, p.base_url, p.kind, c.manual_enabled \
             FROM channels c JOIN providers p ON p.id=c.provider_id WHERE c.id=?",
        )
        .bind(channel_id)
        .fetch_optional(db.pool())
        .await?;
            Ok(row.map(|row| ChannelRow {
                id: row.get("id"),
                name: row.get("name"),
                protocol: row.get("protocol"),
                kind: row.get("kind"),
                health_check_model_id: row.get("health_check_model_id"),
                api_key_encrypted: row.get("api_key_encrypted"),
                base_url: row.get("base_url"),
                manual_enabled: row.get("manual_enabled"),
            }))
        })
    }
}

/// Owns every background task — one-shot probes/discoveries and the
/// long-lived supervisors (health, maintenance, telemetry writer) — so
/// shutdown is one bounded, joinable operation (P1-3).
///
/// Registration is synchronous under the `JoinSet` lock: a task is either
/// registered before `shutdown()` starts draining, or rejected with
/// [`ShuttingDown`] — there is no second, unregistered `tokio::spawn`
/// window that could outlive the runtime.
pub struct RuntimeSupervisor {
    tasks: Arc<tokio::sync::Mutex<tokio::task::JoinSet<()>>>,
    pub cancel: CancellationToken,
    shutting_down: AtomicBool,
    failed_tasks: Arc<AtomicU64>,
    /// Tasks currently running (spawned, not yet finished or aborted). A
    /// drop guard decrements it, so an abort during a deadline drain is
    /// counted too — `active_task_count() == 0` after `shutdown()` proves
    /// nothing survived.
    active_tasks: Arc<AtomicU64>,
}

/// Decrements the supervisor's active-task counter when the wrapped task
/// finishes OR is aborted. Held inside the registered task, so a deadline
/// abort (which drops the task future) still releases the count.
struct ActiveTaskGuard(Arc<AtomicU64>);

impl Drop for ActiveTaskGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Outcome of a one-shot background task, recorded by the supervisor so no
/// background boundary ends silently (P2-8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskOutcome {
    Success,
    Failed,
    Cancelled,
}

/// Returned by [`RuntimeSupervisor::spawn`] once shutdown has started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShuttingDown;

impl std::fmt::Display for ShuttingDown {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "runtime is shutting down")
    }
}
impl std::error::Error for ShuttingDown {}

impl RuntimeSupervisor {
    pub fn new(cancel: CancellationToken) -> Arc<Self> {
        Arc::new(Self {
            tasks: Arc::new(tokio::sync::Mutex::new(tokio::task::JoinSet::new())),
            cancel,
            shutting_down: AtomicBool::new(false),
            failed_tasks: Arc::new(AtomicU64::new(0)),
            active_tasks: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Number of registered tasks that have not finished or been aborted
    /// yet. After [`Self::shutdown`] returns this is always 0 — the drain
    /// proof that no task survived (P1-1).
    pub fn active_task_count(&self) -> u64 {
        self.active_tasks.load(Ordering::Relaxed)
    }

    /// Counts one-shot tasks that finished with [`TaskOutcome::Failed`]
    /// (P2-8): a persisted observability signal for silent-failure audits.
    pub fn failed_task_count(&self) -> u64 {
        self.failed_tasks.load(Ordering::Relaxed)
    }

    /// Records a failure from a task that lives outside this supervisor's
    /// `JoinSet` (e.g. health-supervisor-internal probe tasks).
    pub fn record_failure(&self) {
        self.failed_tasks.fetch_add(1, Ordering::Relaxed);
    }

    /// Register a background task. Refused once [`Self::shutdown`] has
    /// started (checked before AND after taking the set lock, so no task
    /// slips into an already-draining runtime).
    pub async fn spawn(
        self: &Arc<Self>,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), ShuttingDown> {
        if self.shutting_down.load(Ordering::SeqCst) {
            return Err(ShuttingDown);
        }
        let mut tasks = self.tasks.lock().await;
        if self.shutting_down.load(Ordering::SeqCst) {
            return Err(ShuttingDown);
        }
        let active = Arc::clone(&self.active_tasks);
        active.fetch_add(1, Ordering::Relaxed);
        tasks.spawn(async move {
            let _guard = ActiveTaskGuard(active);
            future.await;
        });
        Ok(())
    }

    /// Register a tracked one-shot task (P2-8): the task returns a
    /// [`TaskOutcome`] and the supervisor logs and counts every
    /// non-successful finish, so no background boundary ends silently.
    pub async fn spawn_tracked(
        self: &Arc<Self>,
        name: &'static str,
        future: impl Future<Output = TaskOutcome> + Send + 'static,
    ) -> Result<(), ShuttingDown> {
        if self.shutting_down.load(Ordering::SeqCst) {
            return Err(ShuttingDown);
        }
        let mut tasks = self.tasks.lock().await;
        if self.shutting_down.load(Ordering::SeqCst) {
            return Err(ShuttingDown);
        }
        let failed = Arc::clone(&self.failed_tasks);
        let active = Arc::clone(&self.active_tasks);
        active.fetch_add(1, Ordering::Relaxed);
        tasks.spawn(async move {
            let _guard = ActiveTaskGuard(active);
            let outcome = future.await;
            if outcome != TaskOutcome::Success {
                tracing::warn!(task = name, outcome = ?outcome, "background task finished unsuccessfully");
                failed.fetch_add(1, Ordering::Relaxed);
            }
        });
        Ok(())
    }

    /// Reaps finished one-shot tasks so the `JoinSet` never grows without
    /// bound. On cancel it simply exits: the owner's [`Self::shutdown`] is
    /// the only abort/drain authority, and it already holds the set lock —
    /// this task must never contend for it during a drain (deadlock).
    pub async fn reap_loop(self: Arc<Self>, interval: std::time::Duration) {
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                _ = tokio::time::sleep(interval) => {
                    let mut guard = self.tasks.lock().await;
                    while guard.try_join_next().is_some() {}
                }
            }
        }
    }

    /// Stop the whole runtime (P1-3):
    /// 1. reject any new registration,
    /// 2. cancel the shared token (serve, health, maintenance, writer all
    ///    observe it),
    /// 3. join everything within the absolute `deadline`,
    /// 4. on timeout abort every remaining task and join again.
    ///
    /// Returns only when no task is left running — callers never detach.
    pub async fn shutdown(self: &Arc<Self>, deadline: std::time::Duration) {
        self.shutting_down.store(true, Ordering::SeqCst);
        self.cancel.cancel();
        let drained = tokio::time::timeout(deadline, async {
            let mut guard = self.tasks.lock().await;
            while guard.join_next().await.is_some() {}
        })
        .await;
        if drained.is_err() {
            tracing::warn!("shutdown deadline exceeded; aborting remaining tasks");
            let mut guard = self.tasks.lock().await;
            guard.shutdown().await;
        }
    }
}
