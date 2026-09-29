//! 渠道健康探测与熔断自动恢复。
//!
//! `probe` 用每协议的最小请求探测渠道，只有响应为 2xx **且**带有完成/首 token
//! 信号才算健康（2xx 的 JSON 错误体不得复位熔断）；探测失败立即按配置的
//! `circuit_open_seconds` 冷却时间打开熔断，成功则清零。
//!
//! `run_supervisor` 是后台循环：已开断且已启用的渠道在 `disabled_until` 到期后
//! 会被重新探测（半开恢复）；[`RuntimeSupervisor`] 直接把它注册为任务体，本模块自身不 spawn 任务。
//!
//! 关键不变量：状态迁移与建渠道初始化都经 [`apply_health_event`]；
//! 本模块是 `channel_health` 的唯一写入口。

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use tokio::time::{Instant, timeout};
use tokio_util::sync::CancellationToken;

use crate::{application::Context, protocol, settings, state::AppState};

/// 手动触发一次渠道探测的入口（把探测任务入队后台任务系统后立即返回）。
/// 每协议实际使用的探测端点/请求体由 `protocol::health_probe` 提供。
pub async fn queue(state: Context, channel_id: String) -> Result<()> {
    let background = state.background.clone();
    let cancel = background.cancel.clone();
    let task_channel_id = channel_id.clone();
    if background
        .spawn_tracked("manual_probe", async move {
            match probe(&state, &task_channel_id, cancel).await {
                Ok(_) => crate::infrastructure::TaskOutcome::Success,
                Err(error) => {
                    // 被取消的探测属于正常关停结果，而非任务失败。
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
        // 关停已开始：手动探测直接不执行。
        tracing::warn!(channel_id, "probe not started: runtime shutting down");
    }
    Ok(())
}

/// 单个协议的探测结果：在网络阶段收集，随后与渠道判定一并原子落库。
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

/// 探测单个渠道并更新其健康状态。探测失败立即打开熔断（探测阈值即为 1），
/// 冷却时长为运行时的 `circuit_open_seconds`；探测成功则复位熔断。
///
/// 健康判定是凭据级的：渠道上配置的每个协议都会被探测，每 (channel, protocol)
/// 生成一行 `health_probe_logs`。只有当每个有可用模型的协议都成功时，渠道才被
/// 标记为 active；任一失败即打开熔断（保守策略：坏掉的协议条目本来也会让真实请求
/// 失败）。没有可用模型的协议会被跳过——在发现流程为它注册模型之前，它无法承载
/// 请求，因此不能把整个渠道拖入开断状态（否则协议只被部分覆盖的供应商会永远探测
/// 不到健康）。
///
/// `cancel` 会中止在途的发送/响应体读取，因此关停时无需一直等到静默上游超时。
async fn probe(state: &Context, channel_id: &str, cancel: CancellationToken) -> Result<bool> {
    let row = state
        .channels
        .load_channel(channel_id)
        .await?
        .context("Channel not found")?;
    let default_protocol = row.protocol;
    let configured_model = row.health_check_model_id;
    // 探测模型按协议逐个选择：全局 `health_check_model_id` 只用于它真正支持的
    // 协议；否则使用该协议下第一个可用模型。没有任何可用模型的协议会在下方跳过。
    let global_model = configured_model.filter(|value| !value.is_empty());
    let mut protocols = state.channels.protocols(channel_id).await?;
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
    // 渠道只配置了 Command Code 且集成被关闭：这不是模型缺失，而是功能未启用，
    // 直接给出稳定原因，避免 UI 显示“没有可用探测模型”。
    if !runtime.command_code_enabled
        && !protocols.is_empty()
        && protocols.iter().all(|value| value == "command_code")
    {
        first_failure = Some((false, None, Some("command_code_disabled".into())));
    }
    for protocol_name in &protocols {
        // 集成被关闭时，网关不向 Command Code 上游发起任何请求，探测也不例外。
        if protocol_name == "command_code" && !runtime.command_code_enabled {
            continue;
        }
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
            // 该协议没有可用模型：没有可用来探测的模型，且在发现流程为它注册模型
            // 之前，任何请求都不会被路由到该渠道的这个协议上。跳过它，而不是把
            // 整个渠道判定为失败。
            continue;
        }
        let Some(probe) = protocol::health_probe(protocol_name, &model) else {
            if first_failure.is_none() {
                first_failure = Some((
                    false,
                    None,
                    Some(format!("unsupported protocol {protocol_name}")),
                ));
            }
            continue;
        };
        let url = protocol::upstream_url(&base, &probe.path, None, protocol_name)?;
        let mut headers =
            protocol::outbound_headers(&axum::http::HeaderMap::new(), protocol_name, &key)?;
        if protocol::requires_opencode_session(&base) {
            let session_id = settings::opencode_session_id(&state.db).await;
            protocol::apply_opencode_session(&mut headers, &base, &session_id)?;
        }
        if protocol_name == "command_code"
            && protocol::requires_command_code_identity(row.kind.as_deref())
        {
            // Command Code 探测 /alpha/whoami，携带与 CLI 相同的身份头；
            // session/version 按渠道读取。
            let identity = crate::commandcode::identity_for_probe(&state.db, channel_id).await;
            protocol::apply_command_code_identity(&mut headers, &identity)?;
        }
        // 探测请求自行构造 JSON 请求体，没有可复制的入站头；若缺少 Content-Type，
        // 部分上游（如 opencode.ai）会以 500 拒绝请求。
        if probe.body.is_some() && !headers.contains_key(axum::http::header::CONTENT_TYPE) {
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                axum::http::HeaderValue::from_static("application/json"),
            );
        }
        let payload = match &probe.body {
            Some(body) => Some(bytes::Bytes::from(serde_json::to_vec(body)?)),
            None => None,
        };
        probed += 1;
        let started = Instant::now();
        // 探测经由上游端口发送；该端口负责施加连接超时与发送截止时间。
        let result = tokio::select! {
            _ = cancel.cancelled() => {
                return Err(anyhow::anyhow!("probe cancelled"));
            }
            result = state.http.send(crate::ports::UpstreamRequest {
                url,
                headers,
                method: probe.method,
                body: payload,
                connect_timeout: std::time::Duration::from_secs(
                    runtime.connect_timeout_seconds.max(1) as u64,
                ),
                deadline: state.limits.probe_timeout,
            }) => result,
        };
        let (success, status, error_kind) = match result {
            Ok(response) => {
                let body = tokio::select! {
                    _ = cancel.cancelled() => {
                        return Err(anyhow::anyhow!("probe cancelled"));
                    }
                    result = timeout(
                        state.limits.probe_timeout,
                        response.body.read_capped(state.limits.error_body_max),
                    ) => match result {
                        Ok(read) => Ok(read),
                        Err(_) => Err(()),
                    },
                };
                probe_body_verdict(body, response.status, protocol_name)
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
            // 刚刚 push 过，`last()` 必然存在；这里刻意不用 `unwrap()`，
            // 免得探测路径留下 panic 点。
            let error_kind = results
                .last()
                .and_then(|result| result.error_kind.clone());
            first_failure = Some((success, status, error_kind));
        }
    }
    // 所有探测日志与渠道判定在同一个事务中提交——部分失败时绝不会出现日志与熔断
    // 状态互相矛盾，判定也不会因写入中途出错而丢失。
    // 没有任何可探测内容的渠道（所有协议都没有模型）判定为失败而非成功：说明发现
    // 流程尚未运行或未找到任何模型。
    let all_ok = probed > 0 && successes == probed;
    let time = state.clock.now_utc().to_rfc3339();
    let mut tx = state.db.pool().begin().await?;
    for result in &results {
        sqlx::query("INSERT INTO health_probe_logs(id,channel_id,protocol,model_id,started_at,duration_ms,success,status_code,error_kind,next_probe_at) VALUES(?,?,?,?,?,?,?,?,?,?)")
            .bind(uuid::Uuid::new_v4().to_string()).bind(channel_id).bind(&result.protocol).bind(&result.model).bind(&result.started_at).bind(result.duration_ms).bind(result.success).bind(result.status_code).bind(&result.error_kind).bind(&result.next_probe_at).execute(&mut *tx).await?;
    }
    if all_ok {
        apply_health_event(
            &mut *tx,
            HealthEvent::Success {
                channel_id,
                at: &time,
            },
        )
        .await?;
    } else {
        let (_success, status, error_kind) = first_failure.unwrap_or((
            false,
            None,
            Some("no probe model for any protocol".into()),
        ));
        let disabled_until =
            (chrono::Utc::now() + chrono::Duration::seconds(open_seconds)).to_rfc3339();
        apply_health_event(
            &mut *tx,
            HealthEvent::ProbeFailure {
                channel_id,
                at: &time,
                error_kind: error_kind.as_deref(),
                status,
                disabled_until: &disabled_until,
            },
        )
        .await?;
    }
    tx.commit().await?;
    Ok(all_ok)
}

/// 探测体读取结果 → 探测判定。
///
/// 读取超时/失败**不能**当作健康：旧实现把错误吞成空体，于是“2xx 但读取
/// 超时”会被判成成功并复位熔断。现在读取失败按失败判定，`error_kind` 记为
/// `body_read_timeout` 错误码。
fn probe_body_verdict(
    body_result: Result<(Vec<u8>, bool), ()>,
    status_code: axum::http::StatusCode,
    protocol: &str,
) -> (bool, Option<i64>, Option<String>) {
    let status = Some(status_code.as_u16() as i64);
    match body_result {
        Ok((body, _truncated)) => (
            status_code.is_success() && protocol::probe_body_ok(protocol, &body),
            status,
            None,
        ),
        Err(()) => (false, status, Some("body_read_timeout".to_string())),
    }
}

/// 后台监管循环：每隔数秒轮询一次冷却已过期的开断渠道并探测它们（在途任务会
/// 去重），实现熔断的自动恢复。
///
/// 它本身就是任务体：[`RuntimeSupervisor`] 直接注册它，因此没有 `tokio::spawn`
/// 边界能让它脱离监管。探测任务运行在本任务持有的 [`ProbeSet`] 中，使取消时能在
/// 任务返回前将它们排空（探测受 `RuntimeLimits::probe_timeout` 约束）；若本任务
/// 被 abort，`ProbeSet` 的 `Drop` 会同步 abort 每个探测，因此没有探测会存活到其
/// 宿主之外。
pub async fn run_supervisor(state: AppState, cancel: CancellationToken) {
    // 在飞去重集合用 parking_lot：它能在 `Drop` 里同步加锁，
    // 而 tokio 的异步锁在 `Drop` 中无法使用。
    let probing: Arc<parking_lot::Mutex<HashSet<String>>> =
        Arc::new(parking_lot::Mutex::new(HashSet::new()));
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
                    if !probing.lock().insert(channel_id.clone()) {
                        continue;
                    }
                    let state = state.clone();
                    let cancel = cancel.clone();
                    // 守卫随任务一起被 drop：任务正常结束、出错、甚至被 abort，
                    // 都会把自己从在飞集合里摘掉。
                    let guard = ProbeGuard {
                        set: Arc::clone(&probing),
                        id: channel_id.clone(),
                    };
                    probes.spawn(async move {
                        let _guard = guard;
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
                        if outcome != crate::infrastructure::TaskOutcome::Success {
                            state.background.record_failure();
                        }
                    });
                }
            }
        }
    }
    // abort 所有在途探测：它们现在都可取消，因此监管循环能立即退出，
    // 而不必等到 20s 超时耗尽。
    probes.shutdown().await;
}

/// 在飞去重集合的守卫：任务无论正常结束还是被 abort，`Drop` 都会把自己的
/// `channel_id` 摘掉。旧实现只在任务体末尾手动 remove——任务被 abort
/// 时那一行永远不会执行，该渠道此后会被永久跳过（集合泄漏）。
struct ProbeGuard {
    set: Arc<parking_lot::Mutex<HashSet<String>>>,
    id: String,
}

impl Drop for ProbeGuard {
    fn drop(&mut self) {
        self.set.lock().remove(&self.id);
    }
}

/// 持有在途探测的 `JoinSet`。优雅取消时通过 [`Self::shutdown`] join 每个探测；
/// 若其宿主被 abort（关停超过截止时间），`Drop` 会同步 abort 每个探测——探测绝
/// 不会存活到健康监管任务之外，因此排空返回后不再有探测占用数据库连接池。
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

/// 一次 `channel_health` 状态迁移的语义描述。
///
/// 调用方只表达“发生了什么”（成功、请求失败、探测失败、手动重置、建渠道初始化、
/// 额度耗尽），
/// 具体写哪些列、怎么计数、何时开断由 [`apply_health_event`] 统一决定。
pub enum HealthEvent<'a> {
    /// 请求成功：清零失败计数、恢复 `active` 并写 `last_success_at`。
    Success { channel_id: &'a str, at: &'a str },
    /// 请求失败：失败计数 +1，只有达到阈值才开断（`disabled_until` 由调用方按窗口算出）。
    RequestFailure {
        channel_id: &'a str,
        at: &'a str,
        error_kind: Option<&'a str>,
        status: Option<i64>,
        disabled_until: &'a str,
        threshold: i64,
    },
    /// 探测失败：探测是权威判定，无条件开断并写 `disabled_until`。
    ProbeFailure {
        channel_id: &'a str,
        at: &'a str,
        error_kind: Option<&'a str>,
        status: Option<i64>,
        disabled_until: &'a str,
    },
    /// 管理端手动重置：恢复 `active`，但不写 `last_success_at`（没有真实成功过）。
    ManualReset { channel_id: &'a str, at: &'a str },
    /// 新建渠道时的初始行：插入 `active` 行；已存在则不动（幂等）。
    Initialize { channel_id: &'a str, at: &'a str },
    /// Command Code 配额冷却：开断并写入 `last_error_kind='quota_exhausted'`，
    /// 窗口重置后由健康探测自动放回。
    QuotaExhausted {
        channel_id: &'a str,
        at: &'a str,
        status: Option<i64>,
        disabled_until: &'a str,
    },
}

/// `channel_health` 的唯一写入口：熔断/恢复状态机集中在此，
/// 其它模块只表达“发生了什么”，不再各自拼 SQL（改阈值或列时只需改这里）。
///
/// `executor` 可以是连接池，也可以是调用方事务里的 `&mut *tx`，
/// 因此“探测日志 + 判定”仍能保持单个事务。返回值即那条 UPDATE 的
/// [`sqlx::SqliteQueryResult`]，调用方可据 `rows_affected()` 判断渠道是否存在。
pub async fn apply_health_event<'e, E>(
    executor: E,
    event: HealthEvent<'_>,
) -> Result<sqlx::sqlite::SqliteQueryResult, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    match event {
        HealthEvent::Success { channel_id, at } => {
            sqlx::query("UPDATE channel_health SET state='active',consecutive_failures=0,disabled_until=NULL,last_success_at=?,last_error_kind=NULL,last_status_code=NULL,updated_at=? WHERE channel_id=?")
                .bind(at)
                .bind(at)
                .bind(channel_id)
                .execute(executor)
                .await
        }
        HealthEvent::RequestFailure {
            channel_id,
            at,
            error_kind,
            status,
            disabled_until,
            threshold,
        } => {
            sqlx::query("UPDATE channel_health SET consecutive_failures=consecutive_failures+1, last_failure_at=?, last_error_kind=?, last_status_code=?, state=CASE WHEN consecutive_failures+1 >= ? THEN 'open' ELSE state END, disabled_until=CASE WHEN consecutive_failures+1 >= ? THEN ? ELSE disabled_until END, updated_at=? WHERE channel_id=?")
                .bind(at)
                .bind(error_kind)
                .bind(status)
                .bind(threshold)
                .bind(threshold)
                .bind(disabled_until)
                .bind(at)
                .bind(channel_id)
                .execute(executor)
                .await
        }
        HealthEvent::ProbeFailure {
            channel_id,
            at,
            error_kind,
            status,
            disabled_until,
        } => {
            sqlx::query("UPDATE channel_health SET consecutive_failures=consecutive_failures+1,last_failure_at=?,last_error_kind=?,last_status_code=?,state='open',disabled_until=?,updated_at=? WHERE channel_id=?")
                .bind(at)
                .bind(error_kind)
                .bind(status)
                .bind(disabled_until)
                .bind(at)
                .bind(channel_id)
                .execute(executor)
                .await
        }
        HealthEvent::ManualReset { channel_id, at } => {
            sqlx::query("UPDATE channel_health SET state='active',consecutive_failures=0,disabled_until=NULL,last_error_kind=NULL,last_status_code=NULL,updated_at=? WHERE channel_id=?")
                .bind(at)
                .bind(channel_id)
                .execute(executor)
                .await
        }
        HealthEvent::Initialize { channel_id, at } => {
            sqlx::query(
                "INSERT INTO channel_health(channel_id,state,consecutive_failures,updated_at) \
                 VALUES(?,'active',0,?) ON CONFLICT(channel_id) DO NOTHING",
            )
            .bind(channel_id)
            .bind(at)
            .execute(executor)
            .await
        }
        HealthEvent::QuotaExhausted {
            channel_id,
            at,
            status,
            disabled_until,
        } => {
            sqlx::query(
                "UPDATE channel_health SET state='open', last_failure_at=?, last_error_kind='quota_exhausted', \
                 last_status_code=?, disabled_until=?, updated_at=? WHERE channel_id=?",
            )
            .bind(at)
            .bind(status)
            .bind(disabled_until)
            .bind(at)
            .bind(channel_id)
            .execute(executor)
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 守卫 drop 后，去重集合不再包含该渠道。
    #[test]
    fn probe_guard_removes_the_channel_on_drop() {
        let set: Arc<parking_lot::Mutex<HashSet<String>>> =
            Arc::new(parking_lot::Mutex::new(HashSet::new()));
        set.lock().insert("ch-1".to_string());
        let guard = ProbeGuard {
            set: Arc::clone(&set),
            id: "ch-1".to_string(),
        };
        drop(guard);
        assert!(
            !set.lock().contains("ch-1"),
            "the in-flight set must not leak the channel id"
        );
    }

    /// 任务被 abort 时守卫同样释放（旧实现只在任务体末尾 remove，abort 会漏掉）。
    #[tokio::test]
    async fn probe_guard_releases_when_the_task_is_aborted() {
        let set: Arc<parking_lot::Mutex<HashSet<String>>> =
            Arc::new(parking_lot::Mutex::new(HashSet::new()));
        set.lock().insert("ch-2".to_string());
        let guard = ProbeGuard {
            set: Arc::clone(&set),
            id: "ch-2".to_string(),
        };
        let task = tokio::spawn(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        task.abort();
        let _ = task.await;
        assert!(
            !set.lock().contains("ch-2"),
            "an aborted probe must release its slot"
        );
    }

    /// 探测体读取失败按失败判定（`body_read_timeout`），不能吞成空体当健康。
    #[test]
    fn probe_body_verdict_fails_on_read_error() {
        let (success, status, error_kind) =
            probe_body_verdict(Err(()), axum::http::StatusCode::OK, "claude");
        assert!(
            !success,
            "a 2xx probe whose body could not be read must not reset the circuit"
        );
        assert_eq!(status, Some(200));
        assert_eq!(error_kind.as_deref(), Some("body_read_timeout"));
    }

    /// 正常读到探测体时仍按协议判定（与旧行为一致）。
    #[test]
    fn probe_body_verdict_uses_the_body_when_read_succeeds() {
        let ok_body =
            br#"{"stop_reason":"end_turn","content":[{"type":"text","text":"OK"}]}"#.to_vec();
        let (success, status, error_kind) =
            probe_body_verdict(Ok((ok_body, false)), axum::http::StatusCode::OK, "claude");
        assert!(success);
        assert_eq!(status, Some(200));
        assert!(error_kind.is_none());

        let (success, _, error_kind) = probe_body_verdict(
            Ok((b"not a completion".to_vec(), false)),
            axum::http::StatusCode::OK,
            "claude",
        );
        assert!(
            !success,
            "a 2xx body without a completion signal must not reset the circuit"
        );
        assert!(error_kind.is_none());
    }
    use crate::state::AppState;
    use crate::test_support::TempDir;
    use serde_json::Value;
    use std::sync::Arc;
    use std::time::Duration;

    /// 上游 mock：对每个 POST 都以 `200 + body` 应答（当设置了 `fail_claude` 且
    /// 路径为 claude 端点时，改为 `500`）。
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

    async fn test_state(upstream_port: u16) -> (AppState, TempDir) {
        // 统一夹具：临时目录 + 整套 Context + 标准 provider/channel 播种。
        let crate::test_support::TestEnv {
            dir,
            context: state,
        } = crate::test_support::context("health").await;
        let time = crate::test_support::SEED_TIME;
        crate::test_support::seed_provider(
            &state.db,
            "prov-1",
            "mock",
            &format!("http://127.0.0.1:{upstream_port}"),
        )
        .await;
        crate::test_support::seed_channel(
            &state.db,
            &state.secrets,
            "ch-1",
            "prov-1",
            "openai_compatible",
            "test-key",
        )
        .await;
        // 该渠道额外绑定 claude 协议（探测与健康恢复按协议逐个执行）。
        sqlx::query("INSERT INTO channel_protocols(channel_id,protocol) VALUES('ch-1','claude')")
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_models(id,channel_id,model_id,display_name,source,available,first_seen_at,created_at,updated_at) VALUES('cm-1','ch-1','probe-model','Probe','discovered',1,?,?,?)")
            .bind(time)
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES('cm-1','openai_compatible'),('cm-1','claude')")
            .execute(state.db.pool())
            .await
            .unwrap();
        // 初始熔断：健康监管只挑选 state='open' 且到期的渠道。
        sqlx::query("INSERT INTO channel_health(channel_id,state,consecutive_failures,updated_at) VALUES('ch-1','open',0,?)")
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        (state, dir)
    }


    /// Command Code 探测 mock：对每个请求都以 `whoami` 账户体应答，并记录请求头。
    async fn spawn_command_code_probe_upstream() -> (
        u16,
        tokio::sync::mpsc::UnboundedReceiver<String>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let tx = tx.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];
                    loop {
                        let n = match stream.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => n,
                        };
                        buf.extend_from_slice(&chunk[..n]);
                        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                    }
                    let _ = tx.send(String::from_utf8_lossy(&buf).into_owned());
                    let body = r#"{"org":{"id":"org_1","login":"me"},"user":{"userName":"u"}}"#;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        });
        (port, rx)
    }

    /// Command Code 健康探测是只读的 `GET /alpha/whoami`，携带 CLI 身份头；
    /// 判定为健康会激活该渠道。
    #[tokio::test]
    async fn command_code_probe_uses_whoami_with_identity_and_activates() {
        let (port, mut heads) = spawn_command_code_probe_upstream().await;
        let (state, _dir) = test_state(port).await;
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("UPDATE providers SET kind='command_code' WHERE id='prov-1'")
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE channels SET protocol='command_code' WHERE id='ch-1'")
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM channel_protocols WHERE channel_id='ch-1'")
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_protocols(channel_id,protocol) VALUES('ch-1','command_code')")
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM channel_model_protocols WHERE channel_model_id='cm-1'")
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES('cm-1','command_code')")
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO settings(key,value_json,updated_at) VALUES('command_code_enabled','true',?)")
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();

        let ok = probe(&state, "ch-1", CancellationToken::new())
            .await
            .unwrap();
        assert!(ok, "whoami 200 must mark the channel healthy");

        let head = tokio::time::timeout(Duration::from_secs(5), heads.recv())
            .await
            .expect("probe request timed out")
            .expect("probe request missing");
        assert!(head.starts_with("GET /alpha/whoami"), "{head}");
        let lower = head.to_ascii_lowercase();
        assert!(lower.contains("x-cli-environment: production"), "{head}");
        assert!(lower.contains("x-command-code-version:"), "{head}");
        assert!(lower.contains("x-session-id:"), "{head}");
        assert!(lower.contains("authorization: bearer test-key"), "{head}");
        assert!(!lower.contains("content-length: "), "whoami carries no body");

        let (health,): (String,) =
            sqlx::query_as("SELECT state FROM channel_health WHERE channel_id='ch-1'")
                .fetch_one(state.db.pool())
                .await
                .unwrap();
        assert_eq!(health, "active");
    }

    /// 健康判定是凭据级的——两个已配置协议都必须成功渠道才为 active；
    /// 任一协议失败即开断。
    #[tokio::test]
    async fn multi_protocol_probe_aggregates_all_protocols() {
        let port = spawn_upstream(false).await;
        let (state, _dir) = test_state(port).await;
        let ok = probe(&state, "ch-1", CancellationToken::new())
            .await
            .unwrap();
        assert!(ok, "all protocols healthy -> probe succeeds");
        // 对同一 state 的第二次连续探测也必须成功（回归点：连接池中的连接
        // 不得卡住渠道查询）。
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

        // 第二种场景：claude 条目已损坏。
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

    /// 探测日志与渠道判定原子提交——写入中途失败（通过 SQL 触发器注入故障）会把
    /// 两者一起回滚，因此日志绝不会与熔断状态互相矛盾。
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

    /// 取消令牌会中止针对静默上游的在途探测，而不是一直等到 20s 超时。
    #[tokio::test]
    async fn probe_cancelled_while_upstream_silent() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                use tokio::io::AsyncReadExt;
                // 始终不应答；保持连接打开。
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

    /// 感知模型的上游：读取请求体的 `model` 字段。claude 路径对模型 `A` 失败、对
    /// `B` 成功；openai 路径对任何模型都成功。
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

    /// 每个协议用自己支持的模型探测。全局 `health_check_model_id`（`A`）只覆盖
    /// openai；claude 路径必须回退到第一个可用于 claude 的模型（`B`）——修复前
    /// claude 探测使用 `A` 因而失败并开断了渠道。
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

    /// 没有任何可用模型的协议会被跳过：渠道判定由真正被探测的协议决定。协议覆盖
    /// 不完整的故障供应商不应永远停留在开断状态。
    #[tokio::test]
    async fn protocol_without_model_is_skipped_while_others_pass() {
        let port = spawn_upstream(false).await;
        let (state, _dir) = test_state(port).await;
        // 删除 claude 绑定：已无渠道模型支持 claude。
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

    /// 当没有任何协议有可用模型时无可探测内容；探测会显式失败，而不是悄悄把渠道
    /// 判定为健康。
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
