//! 后台维护循环：调度模型发现、收尾卡住的 pending 请求日志、按保留期清理。
//!
//! 每 60 秒运行一次：①为上次运行早于 `model_discovery_interval_hours` 的渠道调度
//! 模型发现；②把卡在 `pending` 的请求日志收尾为 cancelled 并补上计算出的耗时；
//! ③删除超过 `log_retention_days` 的日志/探测/运行记录（每小时至多一次）。
//! 边界：只做后台维护，不处理请求路径。`reconcile_completed_stream_cancellations`
//! 修复历史上把已完整完成的流记成 cancelled 的记录，仅在启动时运行一次。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use sqlx::Row;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::state::AppState;
use crate::settings;

/// 从 `request_attempts` 读回、用于修复的最终尝试记账：
/// (outcome, status_code, response_started, has_raw_usage, output_tokens)。
type FinalAttempt = (String, Option<i64>, Option<i64>, bool, Option<i64>);

/// 修复历史上已捕获完整流用量、却被记成 cancelled 的取消记录。
pub async fn reconcile_completed_stream_cancellations(state: &AppState) -> anyhow::Result<i64> {
    let mut rows: Vec<(String, String)> = Vec::new();
    for row in sqlx::query(
        "SELECT rl.id, rl.response_bytes FROM request_logs rl \
         WHERE rl.outcome = 'cancelled' AND rl.final_status_code = 200 AND rl.response_bytes > 0",
    )
    .fetch_all(state.db.pool())
    .await?
    {
        // 读不出来的行直接跳过并告警：用 `unwrap_or_default()` 会伪造出一个空
        // id，后续按 id 修复会命中错误的记录。
        let (Ok(id), Ok(response_bytes)) = (
            row.try_get::<String, _>("id"),
            row.try_get::<String, _>("response_bytes"),
        ) else {
            tracing::warn!("cancelled-stream repair: skipping unreadable request_logs row");
            continue;
        };
        rows.push((id, response_bytes));
    }
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

/// 后台监督循环主体（60 秒节奏）：`RuntimeSupervisor` 直接注册这个任务体，
/// 因此不存在可能让任务脱管的 `tokio::spawn` 边界。预定发现通过
/// `DiscoveryService::queue_scheduled` 排进同一个监督器，关停时会与其余运行时
/// 任务一起被排空。
pub async fn run_supervisor(state: AppState, cancel: CancellationToken) {
    // 启动期修复失败不能悄无声息地结束。
    if let Err(error) = reconcile_completed_stream_cancellations(&state).await {
        tracing::warn!(%error, "startup stream reconciliation failed");
    }
    let discovering: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
    let mut last_cleanup: Option<Instant> = None;
    let mut last_balance: Option<Instant> = None;
    let mut last_command_code_version: Option<Instant> = None;
    let mut interval = tokio::time::interval(state.limits.maintenance_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = interval.tick() => {
                if let Err(error) = schedule_discovery(&state, &discovering).await {
                    tracing::warn!(%error, "scheduled discovery failed");
                }
                if let Err(error) = finalize_stale_pending_requests(&state).await {
                    tracing::warn!(%error, "stale request finalization failed");
                }
                // 余额刷新默认按渠道关闭：没有启用的配置行时，这里只解析成一次本地
                // SELECT，不产生任何上游请求。
                // 此处为已知的 fail-open 行为：读取运行时设置失败会退化为默认值继续运行
                // （不改代码）。
                let runtime = settings::runtime_settings(&state).await.unwrap_or_default();
                if runtime.command_code_enabled {
                    let version_due = last_command_code_version.is_none_or(|last| {
                        last.elapsed()
                            >= Duration::from_secs(
                                runtime.command_code_version_check_interval_hours.max(1) as u64
                                    * 3600,
                            )
                    });
                    if version_due {
                        match crate::commandcode::refresh_cli_version(
                            &state.db,
                            state.http.as_ref(),
                        )
                        .await
                        {
                            Ok(Some(version)) => {
                                if crate::commandcode::version_drift(&version) {
                                    tracing::warn!(
                                        version,
                                        baseline = crate::commandcode::DEFAULT_CLI_VERSION,
                                        "command code CLI version drifted from the verified protocol baseline"
                                    );
                                }
                            }
                            Ok(None) => {}
                            Err(error) => {
                                tracing::debug!(%error, "command code version check skipped");
                            }
                        }
                        last_command_code_version = Some(Instant::now());
                    }
                }
                // Command Code 的配额窗口（5h）重置得比每小时的余额节奏更快，
                // 因此在集成启用时用配置的配额间隔缩短它。
                let balance_interval = if runtime.command_code_enabled {
                    state.limits.balance_interval.min(Duration::from_secs(
                        runtime.command_code_quota_interval_minutes.max(1) as u64 * 60,
                    ))
                } else {
                    state.limits.balance_interval
                };
                let balance_due =
                    last_balance.is_none_or(|last| last.elapsed() >= balance_interval);
                if balance_due {
                    if let Err(error) = state.balance.refresh_enabled(&state).await {
                        tracing::warn!(%error, "balance refresh failed");
                    }
                    last_balance = Some(Instant::now());
                }
                let due = last_cleanup.is_none_or(|last| last.elapsed() >= state.limits.cleanup_interval);
                if due {
                    if let Err(error) = cleanup_logs(&state).await {
                        tracing::warn!(%error, "log cleanup failed");
                    }
                    last_cleanup = Some(Instant::now());
                }
            }
        }
    }
}

/// 为上次运行早于 `model_discovery_interval_hours` 的启用渠道创建 `scheduled`
/// 发现运行，并启动后台工作。
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
    // 丢弃排队的运行已经结束的渠道。
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
        let run_id = match state
            .discovery
            .queue_scheduled(state, channel_id.clone())
            .await
        {
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

/// 把卡在 `pending` 的请求日志标记为 cancelled。
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

/// 删除超过 `log_retention_days` 的请求、尝试、探测与发现运行记录
/// （每小时至多一次）。
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
