use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use serde_json::Value;
use sqlx::Row;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{db::Database, ports::EventSink};

#[derive(Clone)]
pub struct Telemetry {
    sender: mpsc::Sender<Event>,
    dropped: Arc<AtomicU64>,
}

#[derive(Debug)]
pub enum Event {
    RequestStart {
        id: String,
        protocol: String,
        model_id: Option<String>,
        endpoint: String,
        stream: bool,
        started_at: String,
        request_bytes: i64,
    },
    RequestFinish {
        id: String,
        finished_at: String,
        duration_ms: i64,
        status: Option<i64>,
        outcome: String,
        attempts: i64,
        channel_id: Option<String>,
        response_bytes: i64,
    },
    // Attempt is by far the largest variant (21 fields incl. Usage); boxing it
    // keeps the Event enum small so every queued event costs one allocation
    // of the same size instead of padding each message to the largest variant.
    Attempt(Box<AttemptData>),
    ChannelSuccess {
        channel_id: String,
    },
    ChannelFailure {
        channel_id: String,
        error_kind: String,
        status: Option<i64>,
        threshold: i64,
        open_seconds: i64,
        countable: bool,
    },
}

/// Per-attempt telemetry snapshot, carried by [`Event::Attempt`].
#[derive(Debug)]
pub struct AttemptData {
    pub id: String,
    pub request_id: String,
    pub channel_id: String,
    pub channel_name: String,
    pub attempt_no: i64,
    pub priority: i64,
    pub started_at: String,
    pub finished_at: String,
    pub status: Option<i64>,
    pub outcome: String,
    pub error_kind: Option<String>,
    pub failover: bool,
    pub response_started: bool,
    pub first_byte_ms: Option<i64>,
    pub first_token_ms: Option<i64>,
    pub duration_ms: i64,
    pub usage: Usage,
    pub response_bytes: i64,
    pub upstream_protocol: Option<String>,
    pub upstream_model_id: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct Usage {
    pub input_tokens: Option<i64>,
    pub cache_read_tokens: Option<i64>,
    pub cache_write_tokens: Option<i64>,
    pub cache_miss_input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub raw: Option<Value>,
}

impl Usage {
    /// Merge a newer usage snapshot into this one, per-field
    /// latest-non-None-wins: a streaming upstream reports usage across
    /// several chunks and a later chunk often omits fields the earlier one
    /// carried (e.g. `prompt_tokens_details.cached_tokens`), so a full
    /// overwrite would silently drop input/cache data. `cache_miss` is
    /// recomputed from the merged input/cache values; `raw` keeps the newest
    /// snapshot.
    pub fn merge(&mut self, other: &Usage) {
        if other.input_tokens.is_some() {
            self.input_tokens = other.input_tokens;
        }
        if other.cache_read_tokens.is_some() {
            self.cache_read_tokens = other.cache_read_tokens;
        }
        if other.cache_write_tokens.is_some() {
            self.cache_write_tokens = other.cache_write_tokens;
        }
        if other.output_tokens.is_some() {
            self.output_tokens = other.output_tokens;
        }
        // All four protocol adapters derive the miss as
        // total_input - cache_read - cache_write.
        self.cache_miss_input_tokens = match (self.input_tokens, self.cache_read_tokens) {
            (Some(total), Some(read)) => {
                Some((total - read - self.cache_write_tokens.unwrap_or(0)).max(0))
            }
            _ => None,
        };
        if other.raw.is_some() {
            self.raw = other.raw.clone();
        }
    }
}

impl Telemetry {
    /// Pure assembly: builds the telemetry handle and its event channel
    /// without spawning any task. The writer task is owned by the caller via
    /// [`Self::run_writer`], so background-task lifetime is explicit.
    pub fn new(capacity: usize) -> (Self, mpsc::Receiver<Event>) {
        let (sender, receiver) = mpsc::channel(capacity);
        let dropped = Arc::new(AtomicU64::new(0));
        (Self { sender, dropped }, receiver)
    }

    pub fn dropped_handle(&self) -> Arc<AtomicU64> {
        self.dropped.clone()
    }

    /// Runs the telemetry writer task body in the caller's task — the
    /// [`RuntimeSupervisor`] registers it directly, so no `tokio::spawn`
    /// boundary can detach it mid-shutdown. On cancel it keeps draining
    /// events with a blocking receive — handlers may still emit during
    /// graceful shutdown — and only exits once every sender is released
    /// (channel closed), so the tail of the request log is never lost
    /// (P1-5).
    pub async fn run_writer(
        db: Database,
        mut rx: mpsc::Receiver<Event>,
        cancel: CancellationToken,
        dropped: Arc<AtomicU64>,
    ) {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    while let Some(event) = rx.recv().await {
                        if write_event(&db, event).await.is_err() {
                            dropped.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    break;
                }
                maybe = rx.recv() => match maybe {
                    Some(event) => {
                        if write_event(&db, event).await.is_err() {
                            dropped.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    None => break,
                },
            }
        }
    }

    pub fn emit(&self, event: Event) {
        if self.sender.try_send(event).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
    pub fn queue_size(&self) -> usize {
        self.sender.max_capacity() - self.sender.capacity()
    }
}

impl EventSink for Telemetry {
    fn emit(&self, event: Event) {
        Telemetry::emit(self, event);
    }
}

async fn write_event(db: &Database, event: Event) -> anyhow::Result<()> {
    match event {
        Event::RequestStart {
            id,
            protocol,
            model_id,
            endpoint,
            stream,
            started_at,
            request_bytes,
        } => {
            sqlx::query("INSERT INTO request_logs(id, protocol, model_id, endpoint, stream, started_at, outcome, attempt_count, request_bytes) VALUES(?,?,?,?,?,?,'pending',0,?)")
                .bind(id).bind(protocol).bind(model_id).bind(endpoint).bind(stream).bind(started_at).bind(request_bytes).execute(db.pool()).await?;
        }
        Event::RequestFinish {
            id,
            finished_at,
            duration_ms,
            status,
            outcome,
            attempts,
            channel_id,
            response_bytes,
        } => {
            sqlx::query("UPDATE request_logs SET finished_at=?, total_duration_ms=?, final_status_code=?, outcome=?, attempt_count=?, final_channel_id=?, response_bytes=? WHERE id=?")
                .bind(finished_at).bind(duration_ms).bind(status).bind(outcome).bind(attempts).bind(channel_id).bind(response_bytes).bind(id).execute(db.pool()).await?;
        }
        Event::Attempt(attempt) => {
            let tps = match (attempt.usage.output_tokens, attempt.duration_ms) {
                (Some(tokens), ms) if ms > 0 => Some(tokens as f64 * 1000.0 / ms as f64),
                _ => None,
            };
            // The request log row and the token usage snapshot are written in
            // one transaction: token statistics must never diverge from the
            // logged attempt, and a failed write rolls both back together.
            let mut tx = db.pool().begin().await?;
            sqlx::query("INSERT INTO request_attempts(id, request_id, channel_id, channel_name, attempt_no, priority_snapshot, started_at, finished_at, status_code, outcome, error_kind, failover_eligible, response_started, first_byte_ms, first_token_ms, duration_ms, input_tokens, cache_read_tokens, cache_write_tokens, cache_miss_input_tokens, output_tokens, tps, raw_usage_json, response_bytes, upstream_protocol, upstream_model_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
                .bind(&attempt.id).bind(&attempt.request_id).bind(&attempt.channel_id).bind(&attempt.channel_name).bind(attempt.attempt_no).bind(attempt.priority).bind(&attempt.started_at).bind(&attempt.finished_at).bind(attempt.status).bind(&attempt.outcome).bind(&attempt.error_kind).bind(attempt.failover).bind(attempt.response_started).bind(attempt.first_byte_ms).bind(attempt.first_token_ms).bind(attempt.duration_ms)
                .bind(attempt.usage.input_tokens).bind(attempt.usage.cache_read_tokens).bind(attempt.usage.cache_write_tokens).bind(attempt.usage.cache_miss_input_tokens).bind(attempt.usage.output_tokens).bind(tps).bind(attempt.usage.raw.as_ref().map(|v| serde_json::to_string(v).unwrap_or_default())).bind(attempt.response_bytes).bind(&attempt.upstream_protocol).bind(&attempt.upstream_model_id).execute(&mut *tx).await?;
            if attempt.response_started {
                // Only the attempt whose response reached the client carries
                // user-visible usage. The whole request is attributed to its
                // actual start time (request_logs.started_at, the same source
                // the backfill migration joins), so a failover attempt that
                // crosses midnight is never split onto another day. protocol /
                // model_id are snapshotted from the same request row.
                let request_row = sqlx::query(
                    "SELECT started_at, protocol, model_id FROM request_logs WHERE id=?",
                )
                .bind(&attempt.request_id)
                .fetch_optional(&mut *tx)
                .await?;
                let occurred_at = request_row
                    .as_ref()
                    .and_then(|row| row.try_get::<String, _>("started_at").ok())
                    .and_then(|started| {
                        // Normalize to the canonical UTC representation of
                        // token_usage.occurred_at (fixed milliseconds, Z) so
                        // [from, to) range comparisons on the text index are
                        // exact regardless of the stored log format.
                        chrono::DateTime::parse_from_rfc3339(&started)
                            .ok()
                            .map(|value| {
                                value
                                    .with_timezone(&chrono::Utc)
                                    .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                            })
                    });
                if let (Some(row), Some(occurred_at)) = (request_row, occurred_at) {
                    // INSERT OR IGNORE deduplicates by attempt id: replaying a
                    // previously recorded attempt can never double-count.
                    sqlx::query("INSERT OR IGNORE INTO token_usage(attempt_id, occurred_at, bucket, protocol, model_id, input_tokens, cache_read_tokens, cache_write_tokens, cache_miss_input_tokens, output_tokens, first_token_ms, duration_ms) VALUES(?,?,strftime('%Y-%m-%dT%H:00:00Z', ?),?,?,?,?,?,?,?,?,?)")
                        .bind(&attempt.id)
                        .bind(&occurred_at)
                        .bind(&occurred_at)
                        .bind(
                            row.try_get::<String, _>("protocol")
                                .unwrap_or_default(),
                        )
                        .bind(
                            row.try_get::<Option<String>, _>("model_id")
                                .ok()
                                .flatten()
                                .unwrap_or_default(),
                        )
                        .bind(attempt.usage.input_tokens)
                        .bind(attempt.usage.cache_read_tokens)
                        .bind(attempt.usage.cache_write_tokens)
                        .bind(attempt.usage.cache_miss_input_tokens)
                        .bind(attempt.usage.output_tokens)
                        .bind(attempt.first_token_ms)
                        .bind(attempt.duration_ms)
                        .execute(&mut *tx)
                        .await?;
                }
            }
            tx.commit().await?;
        }
        Event::ChannelSuccess { channel_id } => {
            sqlx::query("UPDATE channel_health SET state='active', consecutive_failures=0, disabled_until=NULL, last_success_at=?, last_error_kind=NULL, last_status_code=NULL, updated_at=? WHERE channel_id=?")
                .bind(chrono::Utc::now().to_rfc3339()).bind(chrono::Utc::now().to_rfc3339()).bind(channel_id).execute(db.pool()).await?;
        }
        Event::ChannelFailure {
            channel_id,
            error_kind,
            status,
            threshold,
            open_seconds,
            countable,
        } => {
            if countable {
                let now = chrono::Utc::now();
                let disabled = now + chrono::Duration::seconds(open_seconds);
                sqlx::query("UPDATE channel_health SET consecutive_failures=consecutive_failures+1, last_failure_at=?, last_error_kind=?, last_status_code=?, state=CASE WHEN consecutive_failures+1 >= ? THEN 'open' ELSE state END, disabled_until=CASE WHEN consecutive_failures+1 >= ? THEN ? ELSE disabled_until END, updated_at=? WHERE channel_id=?")
                    .bind(now.to_rfc3339()).bind(error_kind).bind(status).bind(threshold).bind(threshold).bind(disabled.to_rfc3339()).bind(now.to_rfc3339()).bind(channel_id).execute(db.pool()).await?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;
    use std::time::Duration;

    fn attempt_event(response_started: bool) -> Event {
        Event::Attempt(Box::new(AttemptData {
            id: "attempt-1".into(),
            request_id: "req-1".into(),
            channel_id: "ch-1".into(),
            channel_name: "chan".into(),
            attempt_no: 1,
            priority: 0,
            started_at: "2026-08-04T01:23:45.123456789+00:00".into(),
            finished_at: "2026-08-04T01:23:50+00:00".into(),
            status: Some(200),
            outcome: "success".into(),
            error_kind: None,
            failover: false,
            response_started,
            first_byte_ms: Some(100),
            first_token_ms: Some(200),
            duration_ms: 5000,
            usage: Usage {
                input_tokens: Some(10),
                cache_read_tokens: Some(2),
                cache_write_tokens: None,
                cache_miss_input_tokens: Some(8),
                output_tokens: Some(4),
                raw: None,
            },
            response_bytes: 10,
            upstream_protocol: Some("openai_compatible".into()),
            upstream_model_id: Some("upstream-model".into()),
        }))
    }

    async fn temp_db() -> Database {
        let dir =
            std::env::temp_dir().join(format!("lagw-telemetry-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        Database::open(&dir.join("test.db")).await.unwrap()
    }

    /// Attempt rows reference their request log and channel via FK; seed the
    /// parent rows exactly like the proxy flow does before writing an attempt.
    async fn seed_request(db: &Database) {
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("INSERT INTO providers(id,name,base_url,created_at,updated_at) VALUES('prov-1','mock','http://127.0.0.1:1',?,?)")
            .bind(time)
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channels(id,provider_id,name,protocol,api_key_encrypted,api_key_hint,manual_enabled,created_at,updated_at) VALUES('ch-1','prov-1','chan','openai_compatible',x'00','',1,?,?)")
            .bind(time)
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
        write_event(
            db,
            Event::RequestStart {
                id: "req-1".into(),
                protocol: "openai_compatible".into(),
                model_id: Some("gpt-4.1-mini".into()),
                endpoint: "/v1/chat/completions".into(),
                stream: false,
                started_at: "2026-08-04T01:20:00+00:00".into(),
                request_bytes: 100,
            },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn responded_attempt_writes_token_usage_with_canonical_utc_time() {
        let db = temp_db().await;
        seed_request(&db).await;
        write_event(&db, attempt_event(true)).await.unwrap();
        let row: (String, String, String, String, i64, i64, i64) = sqlx::query_as(
            "SELECT occurred_at, bucket, protocol, model_id, input_tokens, output_tokens, first_token_ms FROM token_usage WHERE attempt_id='attempt-1'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(
            row.0, "2026-08-04T01:20:00.000Z",
            "occurred_at must be the request start, normalized to fixed-millis UTC"
        );
        assert_eq!(row.1, "2026-08-04T01:00:00Z");
        assert_eq!(row.2, "openai_compatible");
        assert_eq!(row.3, "gpt-4.1-mini");
        assert_eq!(row.4, 10);
        assert_eq!(row.5, 4);
        assert_eq!(row.6, 200);
    }

    #[tokio::test]
    async fn non_responded_attempt_never_reaches_token_usage() {
        let db = temp_db().await;
        seed_request(&db).await;
        write_event(&db, attempt_event(false)).await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM token_usage")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(count, 0);
        let logged: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM request_attempts")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(logged, 1);
    }

    #[tokio::test]
    async fn replaying_the_same_attempt_rolls_back_and_never_double_counts() {
        let db = temp_db().await;
        seed_request(&db).await;
        write_event(&db, attempt_event(true)).await.unwrap();
        let error = write_event(&db, attempt_event(true)).await.unwrap_err();
        assert!(
            error.to_string().contains("UNIQUE"),
            "duplicate attempt_id must fail the transaction"
        );
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM token_usage")
            .fetch_one(db.pool())
            .await
            .unwrap();
        let logged: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM request_attempts")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(count, 1, "token snapshot must not be double counted");
        assert_eq!(
            logged, 1,
            "log row must roll back together with the snapshot"
        );
    }

    /// P1-5: cancelling the writer must flush every queued event before exit,
    /// and the task must be joinable promptly.
    #[tokio::test]
    async fn writer_drains_remaining_events_on_cancel() {
        let db = temp_db().await;
        seed_request(&db).await;
        let (telemetry, rx) = Telemetry::new(1000);
        let cancel = CancellationToken::new();
        let writer = tokio::spawn(Telemetry::run_writer(
            db.clone(),
            rx,
            cancel.clone(),
            telemetry.dropped_handle(),
        ));
        telemetry.emit(Event::RequestStart {
            id: "req-2".into(),
            protocol: "openai_compatible".into(),
            model_id: None,
            endpoint: "/v1/chat/completions".into(),
            stream: false,
            started_at: "2026-08-04T02:00:00+00:00".into(),
            request_bytes: 10,
        });
        telemetry.emit(attempt_event(false));
        cancel.cancel();
        // The writer only exits once every sender is released (blocking
        // drain), so the test must drop its own sender first.
        let dropped_before = telemetry.dropped();
        drop(telemetry);
        tokio::time::timeout(Duration::from_secs(5), writer)
            .await
            .expect("writer must exit after cancel and sender release")
            .unwrap();
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM request_logs WHERE id='req-2'")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(rows, 1, "queued RequestStart must be flushed on cancel");
        let attempts: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM request_attempts WHERE id='attempt-1'")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(attempts, 1, "queued Attempt must be flushed on cancel");
        assert_eq!(dropped_before, 0);
    }

    #[tokio::test]
    async fn token_usage_is_not_cascaded_by_log_deletion() {
        let db = temp_db().await;
        seed_request(&db).await;
        write_event(&db, attempt_event(true)).await.unwrap();
        let mut tx = db.pool().begin().await.unwrap();
        sqlx::query("DELETE FROM request_attempts")
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("DELETE FROM request_logs")
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM token_usage")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(count, 1, "token stats must survive log cleanup");
    }
}
