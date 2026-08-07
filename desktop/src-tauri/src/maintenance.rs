//! Background maintenance — port of backend/app/services/maintenance.py (C4).
//!
//! Runs a 60-second loop that:
//! 1. schedules model discovery for channels whose last run is older than
//!    `model_discovery_interval_hours`,
//! 2. finalizes request logs stuck in `pending` (cancelled, with a computed
//!    duration),
//! 3. deletes logs/probes/runs older than `log_retention_days` (hourly).
//!
//! `reconcile_completed_stream_cancellations` repairs legacy rows where a
//! fully completed stream was recorded as cancelled; it runs once at startup.

use std::collections::HashMap;
use std::sync::Arc;

use sqlx::Row;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};

use crate::server::AppState;
use crate::settings;

/// Final attempt bookkeeping read back from `request_attempts` for repair:
/// (outcome, status_code, response_started, has_raw_usage, output_tokens).
type FinalAttempt = (String, Option<i64>, Option<i64>, bool, Option<i64>);

/// Repair legacy cancellations that already captured a completed stream's
/// usage (Python `reconcile_completed_stream_cancellations`).
pub async fn reconcile_completed_stream_cancellations(state: &AppState) -> anyhow::Result<i64> {
    let rows: Vec<(String, String)> = sqlx::query(
        "SELECT rl.id, rl.response_bytes FROM request_logs rl \
         WHERE rl.outcome = 'cancelled' AND rl.final_status_code = 200 AND rl.response_bytes > 0",
    )
    .fetch_all(state.db.pool())
    .await?
    .into_iter()
    .map(|row| {
        (
            row.try_get("id").unwrap_or_default(),
            row.try_get("response_bytes").unwrap_or_default(),
        )
    })
    .collect();
    let mut repaired = 0i64;
    for (request_id, _) in rows {
        let final_attempt: Option<FinalAttempt> = sqlx::query(
            "SELECT outcome, status_code, response_started, raw_usage_json, output_tokens \
                 FROM request_attempts WHERE request_id = ? ORDER BY attempt_no DESC LIMIT 1",
        )
        .bind(&request_id)
        .fetch_optional(state.db.pool())
        .await?
        .map(|row| -> anyhow::Result<FinalAttempt> {
            Ok((
                row.try_get::<String, _>("outcome")?,
                row.try_get::<Option<i64>, _>("status_code")?,
                row.try_get::<Option<bool>, _>("response_started")?
                    .map(|v| v as i64),
                row.try_get::<Option<String>, _>("raw_usage_json")?
                    .is_some(),
                row.try_get::<Option<i64>, _>("output_tokens")?,
            ))
        })
        .transpose()?;
        let Some((outcome, status_code, response_started, raw_usage, output_tokens)) =
            final_attempt
        else {
            continue;
        };
        if outcome == "cancelled"
            && status_code == Some(200)
            && response_started == Some(1)
            && raw_usage
            && output_tokens.is_some()
        {
            sqlx::query("UPDATE request_logs SET outcome='success' WHERE id=?")
                .bind(&request_id)
                .execute(state.db.pool())
                .await?;
            sqlx::query(
                "UPDATE request_attempts SET outcome='success' \
                 WHERE request_id=? AND attempt_no=(SELECT MAX(attempt_no) FROM request_attempts WHERE request_id=?)",
            )
            .bind(&request_id)
            .bind(&request_id)
            .execute(state.db.pool())
            .await?;
            repaired += 1;
        }
    }
    Ok(repaired)
}

/// Background supervisor (Python `MaintenanceSupervisor`, 60s cadence).
pub fn spawn_supervisor(state: AppState) {
    tokio::spawn(async move {
        let _ = reconcile_completed_stream_cancellations(&state).await;
        let discovering: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
        let mut last_cleanup: Option<Instant> = None;
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if let Err(error) = schedule_discovery(&state, &discovering).await {
                tracing::warn!(%error, "scheduled discovery failed");
            }
            if let Err(error) = finalize_stale_pending_requests(&state).await {
                tracing::warn!(%error, "stale request finalization failed");
            }
            let due = last_cleanup.is_none_or(|last| last.elapsed() >= Duration::from_secs(3600));
            if due {
                if let Err(error) = cleanup_logs(&state).await {
                    tracing::warn!(%error, "log cleanup failed");
                }
                last_cleanup = Some(Instant::now());
            }
        }
    });
}

/// Create `scheduled` discovery runs for enabled channels whose last run is
/// older than `model_discovery_interval_hours` and spawn the background work.
async fn schedule_discovery(
    state: &AppState,
    discovering: &Arc<Mutex<HashMap<String, String>>>,
) -> anyhow::Result<()> {
    let runtime = settings::runtime_settings(state).await?;
    let interval_hours = runtime.model_discovery_interval_hours.max(1);
    let cutoff = (chrono::Utc::now() - chrono::Duration::hours(interval_hours)).to_rfc3339();
    let channels: Vec<String> =
        sqlx::query_scalar("SELECT id FROM channels WHERE manual_enabled = 1 ORDER BY id")
            .fetch_all(state.db.pool())
            .await?;
    let mut due: Vec<String> = Vec::new();
    for channel_id in channels {
        let last_started: Option<String> =
            sqlx::query_scalar("SELECT MAX(started_at) FROM discovery_runs WHERE channel_id = ?")
                .bind(&channel_id)
                .fetch_optional(state.db.pool())
                .await?;
        let needs_run = match last_started {
            None => true,
            Some(last) => last <= cutoff,
        };
        if needs_run {
            due.push(channel_id);
        }
    }
    let mut discovering = discovering.lock().await;
    // Drop channels whose queued run already finished.
    let mut finished = Vec::new();
    for (channel_id, run_id) in discovering.iter() {
        let done: Option<Option<String>> =
            sqlx::query_scalar("SELECT finished_at FROM discovery_runs WHERE id = ?")
                .bind(run_id)
                .fetch_optional(state.db.pool())
                .await?;
        if done.flatten().is_some() {
            finished.push(channel_id.clone());
        }
    }
    for channel_id in finished {
        discovering.remove(&channel_id);
    }
    for channel_id in due {
        if discovering.contains_key(&channel_id) {
            continue;
        }
        let run_id =
            match crate::discovery::queue_scheduled(state.clone(), channel_id.clone()).await {
                Ok(run_id) => run_id,
                Err(error) => {
                    tracing::warn!(%error, "failed to queue scheduled discovery");
                    continue;
                }
            };
        discovering.insert(channel_id, run_id);
    }
    Ok(())
}

/// Mark request logs stuck in `pending` as cancelled (Python
/// `_finalize_stale_pending_requests`).
async fn finalize_stale_pending_requests(state: &AppState) -> anyhow::Result<()> {
    let runtime = settings::runtime_settings(state).await?;
    let stale_seconds = runtime
        .stream_idle_timeout_seconds
        .max(runtime.first_byte_timeout_seconds)
        .max(300)
        * 2;
    let cutoff = (chrono::Utc::now() - chrono::Duration::seconds(stale_seconds)).to_rfc3339();
    let rows: Vec<(String, Option<String>)> = sqlx::query(
        "SELECT id, started_at FROM request_logs \
         WHERE outcome = 'pending' AND started_at < ?",
    )
    .bind(&cutoff)
    .fetch_all(state.db.pool())
    .await?
    .into_iter()
    .map(|row| {
        Ok((
            row.try_get::<String, _>("id")?,
            row.try_get::<Option<String>, _>("started_at")?,
        ))
    })
    .collect::<anyhow::Result<Vec<_>>>()?;
    let now = chrono::Utc::now();
    for (request_id, started_at) in rows {
        let duration_ms = started_at
            .and_then(|started| chrono::DateTime::parse_from_rfc3339(&started).ok())
            .map(|started| {
                (now - started.with_timezone(&chrono::Utc))
                    .num_milliseconds()
                    .max(0)
            });
        sqlx::query(
            "UPDATE request_logs SET finished_at=?, total_duration_ms=?, outcome='cancelled', \
             response_bytes=COALESCE(response_bytes,0) WHERE id=?",
        )
        .bind(now.to_rfc3339())
        .bind(duration_ms)
        .bind(&request_id)
        .execute(state.db.pool())
        .await?;
    }
    Ok(())
}

/// Delete requests, attempts, probes and discovery runs older than
/// `log_retention_days` (Python `_cleanup_logs`, at most once per hour).
async fn cleanup_logs(state: &AppState) -> anyhow::Result<()> {
    let runtime = settings::runtime_settings(state).await?;
    let cutoff = (chrono::Utc::now() - chrono::Duration::days(runtime.log_retention_days.max(1)))
        .to_rfc3339();
    let mut tx = state.db.pool().begin().await?;
    sqlx::query(
        "DELETE FROM request_attempts WHERE request_id IN (SELECT id FROM request_logs WHERE started_at < ?)",
    )
    .bind(&cutoff)
    .execute(&mut *tx)
    .await?;
    sqlx::query("DELETE FROM request_logs WHERE started_at < ?")
        .bind(&cutoff)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM health_probe_logs WHERE started_at < ?")
        .bind(&cutoff)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM discovery_runs WHERE started_at < ?")
        .bind(&cutoff)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
