use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use serde_json::Value;
use sqlx::Row;
use tokio::sync::mpsc;

use crate::db::Database;

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
    Attempt {
        id: String,
        request_id: String,
        channel_id: String,
        channel_name: String,
        attempt_no: i64,
        priority: i64,
        started_at: String,
        finished_at: String,
        status: Option<i64>,
        outcome: String,
        error_kind: Option<String>,
        failover: bool,
        response_started: bool,
        first_byte_ms: Option<i64>,
        first_token_ms: Option<i64>,
        duration_ms: i64,
        usage: Usage,
        response_bytes: i64,
        upstream_protocol: Option<String>,
        upstream_model_id: Option<String>,
    },
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

#[derive(Clone, Debug, Default)]
pub struct Usage {
    pub input_tokens: Option<i64>,
    pub cache_read_tokens: Option<i64>,
    pub cache_write_tokens: Option<i64>,
    pub cache_miss_input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub raw: Option<Value>,
}

impl Telemetry {
    pub fn start(db: Database, capacity: usize) -> Self {
        let (sender, mut receiver) = mpsc::channel(capacity);
        let dropped = Arc::new(AtomicU64::new(0));
        let dropped_worker = Arc::clone(&dropped);
        tokio::spawn(async move {
            while let Some(event) = receiver.recv().await {
                if write_event(&db, event).await.is_err() {
                    dropped_worker.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        Self { sender, dropped }
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
        Event::Attempt {
            id,
            request_id,
            channel_id,
            channel_name,
            attempt_no,
            priority,
            started_at,
            finished_at,
            status,
            outcome,
            error_kind,
            failover,
            response_started,
            first_byte_ms,
            first_token_ms,
            duration_ms,
            usage,
            response_bytes,
            upstream_protocol,
            upstream_model_id,
        } => {
            let tps = match (usage.output_tokens, duration_ms) {
                (Some(tokens), ms) if ms > 0 => Some(tokens as f64 * 1000.0 / ms as f64),
                _ => None,
            };
            // The request log row and the token usage snapshot are written in
            // one transaction: token statistics must never diverge from the
            // logged attempt, and a failed write rolls both back together.
            let mut tx = db.pool().begin().await?;
            sqlx::query("INSERT INTO request_attempts(id, request_id, channel_id, channel_name, attempt_no, priority_snapshot, started_at, finished_at, status_code, outcome, error_kind, failover_eligible, response_started, first_byte_ms, first_token_ms, duration_ms, input_tokens, cache_read_tokens, cache_write_tokens, cache_miss_input_tokens, output_tokens, tps, raw_usage_json, response_bytes, upstream_protocol, upstream_model_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
                .bind(&id).bind(&request_id).bind(&channel_id).bind(&channel_name).bind(attempt_no).bind(priority).bind(&started_at).bind(&finished_at).bind(status).bind(&outcome).bind(&error_kind).bind(failover).bind(response_started).bind(first_byte_ms).bind(first_token_ms).bind(duration_ms)
                .bind(usage.input_tokens).bind(usage.cache_read_tokens).bind(usage.cache_write_tokens).bind(usage.cache_miss_input_tokens).bind(usage.output_tokens).bind(tps).bind(usage.raw.as_ref().map(|v| serde_json::to_string(v).unwrap_or_default())).bind(response_bytes).bind(&upstream_protocol).bind(&upstream_model_id).execute(&mut *tx).await?;
            if response_started {
                // Only the attempt whose response reached the client carries
                // user-visible usage. The whole request is attributed to its
                // actual start time (request_logs.started_at, the same source
                // the backfill migration joins), so a failover attempt that
                // crosses midnight is never split onto another day. protocol /
                // model_id are snapshotted from the same request row.
                let request_row = sqlx::query(
                    "SELECT started_at, protocol, model_id FROM request_logs WHERE id=?",
                )
                .bind(&request_id)
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
                        .bind(&id)
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
                        .bind(usage.input_tokens)
                        .bind(usage.cache_read_tokens)
                        .bind(usage.cache_write_tokens)
                        .bind(usage.cache_miss_input_tokens)
                        .bind(usage.output_tokens)
                        .bind(first_token_ms)
                        .bind(duration_ms)
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

    fn attempt_event(response_started: bool) -> Event {
        Event::Attempt {
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
        }
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
