use std::sync::{Arc, atomic::{AtomicU64, Ordering}};

use serde_json::Value;
use tokio::sync::mpsc;

use crate::db::Database;

#[derive(Clone)]
pub struct Telemetry {
    sender: mpsc::Sender<Event>,
    dropped: Arc<AtomicU64>,
}

#[derive(Debug)]
pub enum Event {
    RequestStart { id: String, protocol: String, model_id: Option<String>, endpoint: String, stream: bool, started_at: String, request_bytes: i64 },
    RequestFinish { id: String, finished_at: String, duration_ms: i64, status: Option<i64>, outcome: String, attempts: i64, channel_id: Option<String>, response_bytes: i64 },
    Attempt { id: String, request_id: String, channel_id: String, channel_name: String, attempt_no: i64, priority: i64, started_at: String, finished_at: String, status: Option<i64>, outcome: String, error_kind: Option<String>, failover: bool, response_started: bool, first_byte_ms: Option<i64>, duration_ms: i64, usage: Usage, response_bytes: i64, upstream_protocol: Option<String>, upstream_model_id: Option<String> },
    ChannelSuccess { channel_id: String },
    ChannelFailure { channel_id: String, error_kind: String, status: Option<i64>, threshold: i64, open_seconds: i64, countable: bool },
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
                if write_event(&db, event).await.is_err() { dropped_worker.fetch_add(1, Ordering::Relaxed); }
            }
        });
        Self { sender, dropped }
    }

    pub fn emit(&self, event: Event) {
        if self.sender.try_send(event).is_err() { self.dropped.fetch_add(1, Ordering::Relaxed); }
    }

    pub fn dropped(&self) -> u64 { self.dropped.load(Ordering::Relaxed) }
    pub fn queue_size(&self) -> usize { self.sender.max_capacity() - self.sender.capacity() }
}

async fn write_event(db: &Database, event: Event) -> anyhow::Result<()> {
    match event {
        Event::RequestStart { id, protocol, model_id, endpoint, stream, started_at, request_bytes } => {
            sqlx::query("INSERT INTO request_logs(id, protocol, model_id, endpoint, stream, started_at, outcome, attempt_count, request_bytes) VALUES(?,?,?,?,?,?,'pending',0,?)")
                .bind(id).bind(protocol).bind(model_id).bind(endpoint).bind(stream).bind(started_at).bind(request_bytes).execute(db.pool()).await?;
        }
        Event::RequestFinish { id, finished_at, duration_ms, status, outcome, attempts, channel_id, response_bytes } => {
            sqlx::query("UPDATE request_logs SET finished_at=?, total_duration_ms=?, final_status_code=?, outcome=?, attempt_count=?, final_channel_id=?, response_bytes=? WHERE id=?")
                .bind(finished_at).bind(duration_ms).bind(status).bind(outcome).bind(attempts).bind(channel_id).bind(response_bytes).bind(id).execute(db.pool()).await?;
        }
        Event::Attempt { id, request_id, channel_id, channel_name, attempt_no, priority, started_at, finished_at, status, outcome, error_kind, failover, response_started, first_byte_ms, duration_ms, usage, response_bytes, upstream_protocol, upstream_model_id } => {
            let tps = match (usage.output_tokens, duration_ms) { (Some(tokens), ms) if ms > 0 => Some(tokens as f64 * 1000.0 / ms as f64), _ => None };
            sqlx::query("INSERT INTO request_attempts(id, request_id, channel_id, channel_name, attempt_no, priority_snapshot, started_at, finished_at, status_code, outcome, error_kind, failover_eligible, response_started, first_byte_ms, duration_ms, input_tokens, cache_read_tokens, cache_write_tokens, cache_miss_input_tokens, output_tokens, tps, raw_usage_json, response_bytes, upstream_protocol, upstream_model_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
                .bind(id).bind(request_id).bind(channel_id).bind(channel_name).bind(attempt_no).bind(priority).bind(started_at).bind(finished_at).bind(status).bind(outcome).bind(error_kind).bind(failover).bind(response_started).bind(first_byte_ms).bind(duration_ms)
                .bind(usage.input_tokens).bind(usage.cache_read_tokens).bind(usage.cache_write_tokens).bind(usage.cache_miss_input_tokens).bind(usage.output_tokens).bind(tps).bind(usage.raw.map(|v| serde_json::to_string(&v).unwrap_or_default())).bind(response_bytes).bind(upstream_protocol).bind(upstream_model_id).execute(db.pool()).await?;
        }
        Event::ChannelSuccess { channel_id } => {
            sqlx::query("UPDATE channel_health SET state='active', consecutive_failures=0, disabled_until=NULL, last_success_at=?, last_error_kind=NULL, last_status_code=NULL, updated_at=? WHERE channel_id=?")
                .bind(chrono::Utc::now().to_rfc3339()).bind(chrono::Utc::now().to_rfc3339()).bind(channel_id).execute(db.pool()).await?;
        }
        Event::ChannelFailure { channel_id, error_kind, status, threshold, open_seconds, countable } => {
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
