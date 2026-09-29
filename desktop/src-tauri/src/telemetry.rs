//! 遥测事件通道与写入 worker：请求/尝试日志、token 用量统计的落库路径。
//!
//! 职责：`Telemetry` 是事件 channel 的发送端，`EventSink` 实现把事件非阻塞地
//! 投递进队列；`run_writer` 作为写入 worker 消费事件，落 `request_logs` /
//! `request_attempts` / `token_usage` 三张表。
//! 边界：只负责落库，不做任何业务判定（熔断/路由/重试都在别处）。
//! 不变量：worker 自身不持有发送端，否则关停时会等待自己；`run_writer` 收到
//! cancel 后阻塞式排空队列，直到所有发送端释放（channel 关闭）才退出。

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use sqlx::Row;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    db::Database,
    domain::Event,
    health::{HealthEvent, apply_health_event},
    ports::EventSink,
};

#[derive(Clone)]
pub struct Telemetry {
    sender: mpsc::Sender<Event>,
    dropped: Arc<AtomicU64>,
}

impl Telemetry {
    /// 纯组装：构造遥测句柄及其事件 channel，不启动任何任务。写入任务由调用方
    /// 通过 [`Self::run_writer`] 持有，因此后台任务的生命周期是显式的。
    pub fn new(capacity: usize) -> (Self, mpsc::Receiver<Event>) {
        let (sender, receiver) = mpsc::channel(capacity);
        let dropped = Arc::new(AtomicU64::new(0));
        (Self { sender, dropped }, receiver)
    }

    pub fn dropped_handle(&self) -> Arc<AtomicU64> {
        self.dropped.clone()
    }

    /// 在调用方的任务里运行遥测写入任务体：`RuntimeSupervisor` 直接注册它，
    /// 因此不存在可能让任务在关停中途脱管的 `tokio::spawn` 边界。收到 cancel 后
    /// 仍以阻塞式接收继续排空事件——优雅关停期间 handler 仍可能发出事件——直到
    /// 所有发送端释放（channel 关闭）才退出，因此请求日志的尾部永不丢失。
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
            // 请求日志行与 token 用量快照在同一个事务里写入：token 统计绝不能与
            // 记录的尝试发生偏离，写入失败时两者一起回滚。
            let mut tx = db.pool().begin().await?;
            sqlx::query("INSERT INTO request_attempts(id, request_id, channel_id, channel_name, attempt_no, priority_snapshot, started_at, finished_at, status_code, outcome, error_kind, failover_eligible, response_started, first_byte_ms, first_token_ms, duration_ms, input_tokens, cache_read_tokens, cache_write_tokens, cache_miss_input_tokens, output_tokens, tps, raw_usage_json, response_bytes, upstream_protocol, upstream_model_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
                .bind(&attempt.id).bind(&attempt.request_id).bind(&attempt.channel_id).bind(&attempt.channel_name).bind(attempt.attempt_no).bind(attempt.priority).bind(&attempt.started_at).bind(&attempt.finished_at).bind(attempt.status).bind(&attempt.outcome).bind(&attempt.error_kind).bind(attempt.failover).bind(attempt.response_started).bind(attempt.first_byte_ms).bind(attempt.first_token_ms).bind(attempt.duration_ms)
                .bind(attempt.usage.input_tokens).bind(attempt.usage.cache_read_tokens).bind(attempt.usage.cache_write_tokens).bind(attempt.usage.cache_miss_input_tokens).bind(attempt.usage.output_tokens).bind(tps).bind(attempt.usage.raw.as_ref().map(|v| serde_json::to_string(v).unwrap_or_default())).bind(attempt.response_bytes).bind(&attempt.upstream_protocol).bind(&attempt.upstream_model_id).execute(&mut *tx).await?;
            if attempt.response_started {
                // 只有响应已经到达客户端的尝试才携带用户可见的用量。整条请求按它
                // 实际的开始时间归属（request_logs.started_at，与回填迁移 join 的
                // 是同一来源），因此跨越午夜的故障转移尝试不会被拆到另一天。
                // protocol / model_id 从同一请求行快照而来。
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
                        // 归一化为 token_usage.occurred_at 的规范 UTC 表示（固定
                        // 毫秒、Z），这样无论日志里存的是什么格式，文本索引上的
                        // [from, to) 区间比较都保持精确。
                        chrono::DateTime::parse_from_rfc3339(&started)
                            .ok()
                            .map(|value| {
                                value
                                    .with_timezone(&chrono::Utc)
                                    .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                            })
                    });
                if let (Some(row), Some(occurred_at)) = (request_row, occurred_at) {
                    // protocol 读不出来就不写这一行统计：与其把一个无法归类的空协议
                    // 写进 token_usage，不如少一行并留下告警。
                    match row.try_get::<String, _>("protocol") {
                        Ok(protocol) => {
                            let model_id = match row.try_get::<Option<String>, _>("model_id") {
                                Ok(model_id) => model_id,
                                Err(error) => {
                                    tracing::warn!(
                                        error = ?error,
                                        attempt_id = attempt.id,
                                        "token_usage model_id unreadable; recorded as NULL"
                                    );
                                    None
                                }
                            };
                            // INSERT OR IGNORE 以 attempt id 去重：重放同一次尝试
                            // 永远不会重复计数。
                            sqlx::query("INSERT OR IGNORE INTO token_usage(attempt_id, occurred_at, bucket, protocol, model_id, input_tokens, cache_read_tokens, cache_write_tokens, cache_miss_input_tokens, output_tokens, first_token_ms, duration_ms) VALUES(?,?,strftime('%Y-%m-%dT%H:00:00Z', ?),?,?,?,?,?,?,?,?,?)")
                                .bind(&attempt.id)
                                .bind(&occurred_at)
                                .bind(&occurred_at)
                                .bind(protocol)
                                .bind(model_id)
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
                        Err(error) => {
                            tracing::warn!(
                                error = ?error,
                                attempt_id = attempt.id,
                                "token_usage skipped: protocol column is unreadable"
                            );
                        }
                    }
                }
            }
            tx.commit().await?;
        }
        Event::ChannelSuccess { channel_id } => {
            let at = chrono::Utc::now().to_rfc3339();
            apply_health_event(
                db.pool(),
                HealthEvent::Success {
                    channel_id: &channel_id,
                    at: &at,
                },
            )
            .await?;
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
                let at = now.to_rfc3339();
                let disabled_until =
                    (now + chrono::Duration::seconds(open_seconds)).to_rfc3339();
                apply_health_event(
                    db.pool(),
                    HealthEvent::RequestFailure {
                        channel_id: &channel_id,
                        at: &at,
                        error_kind: Some(error_kind.as_str()),
                        status,
                        disabled_until: &disabled_until,
                        threshold,
                    },
                )
                .await?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;
    use crate::domain::{AttemptData, Usage};
    use crate::test_support::TempDir;
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

    /// 测试数据库与其临时目录：目录随返回值一起交给调用方，测试结束时由
    /// `TempDir` 的 `Drop` 清理（必须等连接池用完，目录才会被删除）。
    async fn temp_db() -> (Database, TempDir) {
        let dir = TempDir::new("telemetry");
        let db = Database::open(&dir.path().join("test.db")).await.unwrap();
        (db, dir)
    }

    /// 尝试行通过外键引用其请求日志与渠道；写尝试之前，像代理流程那样先播种父行。
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
        let (db, _dir) = temp_db().await;
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

    /// `request_logs.protocol` 不可读（类型不符）时跳过 token_usage 写入并留下
    /// 告警——旧实现在这里 `unwrap_or_default()`，会写进一个空协议。
    #[tokio::test]
    async fn unreadable_protocol_column_skips_token_usage_without_panicking() {
        let (db, _dir) = temp_db().await;
        seed_request(&db).await;
        // 用 BLOB 覆盖文本列：`try_get::<String>` 会失败（TEXT 亲和性不转换 BLOB）。
        sqlx::query("UPDATE request_logs SET protocol = x'00ff' WHERE id='req-1'")
            .execute(db.pool())
            .await
            .unwrap();
        write_event(&db, attempt_event(true)).await.unwrap();
        let usage_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM token_usage")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(usage_rows, 0, "no token_usage row may be written");
        let attempt_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM request_attempts")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(attempt_rows, 1, "the attempt itself must still be recorded");
    }

    #[tokio::test]
    async fn non_responded_attempt_never_reaches_token_usage() {
        let (db, _dir) = temp_db().await;
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
        let (db, _dir) = temp_db().await;
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

    /// 取消 writer 必须让它在退出前排空所有排队事件，且任务能被及时 join。
    #[tokio::test]
    async fn writer_drains_remaining_events_on_cancel() {
        let (db, _dir) = temp_db().await;
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
        // writer 只在所有发送端释放后才退出（阻塞式排空），所以测试必须先 drop
        // 自己的发送端。
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
        let (db, _dir) = temp_db().await;
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
