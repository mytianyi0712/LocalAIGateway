//! 单次候选尝试的记账：终态枚举（`AttemptOutcome`）、遥测终结器
//! （`AttemptFinalizer`）与尝试上下文（`AttemptEnv`）。

use std::{
    pin::Pin,
    sync::Arc,
    sync::atomic::{AtomicBool, AtomicI64, Ordering},
    task::{Context, Poll},
};

use axum::http::StatusCode;

use crate::{
    domain::{AttemptData, Candidate, Event, TransportFailure, Usage},
    settings,
    telemetry::Telemetry,
};

// 同目录兄弟模块的内部项（`pub(super)` / `pub(crate)`）。
use super::service::*;

/// 包装上游字节流：客户端中途断开（丢弃响应体、不再轮询到结束）时仍会
/// 记录一次 `cancelled` 尝试，而不会让请求永远停留在 pending。
///
/// `completed` 由生成器在到达任一终态（成功、上游错误、空闲或首 token
/// 超时）时置位，表示真实结果已记录；`finalized` 则是测试注入的
/// “已终结”标记，生产路径不会设置它。
/// `Drop` 仅在 `completed` 与 `finalized` 均为 false 时追加一次
/// `cancelled`，因此不会重复终结。
pub(super) struct CancelAware<S> {
    pub(super) inner: S,
    pub(super) completed: Arc<AtomicBool>,
    pub(super) finalized: Arc<AtomicBool>,
    pub(super) responded: Arc<AtomicBool>,
    pub(super) on_cancel: Option<Box<dyn FnOnce() + Send>>,
}

impl<S: futures_util::Stream + Unpin> futures_util::Stream for CancelAware<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(item)) => {
                self.responded.store(true, Ordering::SeqCst);
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => {
                self.completed.store(true, Ordering::SeqCst);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S> Drop for CancelAware<S> {
    fn drop(&mut self) {
        if !self.completed.load(Ordering::SeqCst)
            && !self.finalized.load(Ordering::SeqCst)
            && let Some(callback) = self.on_cancel.take()
        {
            callback();
        }
    }
}

/// 流生成器与其 `CancelAware` 的 Drop 路径共享的实时流统计。客户端中途
/// 断开时生成器的局部变量已消失，取消终结只能读取这里实际观测到的值，
/// 因此已经流转的字节与已经解析的 usage 必须留在这里，
/// 不能被替换成一条零值记录——客户端实际收到的内容
/// 与遥测落库的内容必须一致。
#[derive(Default)]
pub(super) struct SharedStreamStats {
    pub(super) bytes: AtomicI64,
    pub(super) usage: parking_lot::Mutex<Usage>,
    pub(super) first_byte_ms: parking_lot::Mutex<Option<i64>>,
    pub(super) first_token_ms: parking_lot::Mutex<Option<i64>>,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn attempt_event(
    request_id: &str,
    candidate: &Candidate,
    attempt_no: i64,
    started_at: chrono::DateTime<chrono::Utc>,
    finished_at: chrono::DateTime<chrono::Utc>,
    status: Option<i64>,
    outcome: &str,
    error_kind: Option<String>,
    failover: bool,
    response_started: bool,
    first_byte_ms: Option<i64>,
    first_token_ms: Option<i64>,
    usage: Usage,
    response_bytes: i64,
    upstream_protocol: Option<String>,
    upstream_model_id: Option<String>,
) -> Event {
    Event::Attempt(Box::new(AttemptData {
        id: uuid::Uuid::new_v4().to_string(),
        request_id: request_id.to_owned(),
        channel_id: candidate.channel_id.clone(),
        channel_name: candidate.channel_name.clone(),
        attempt_no,
        priority: candidate.priority,
        started_at: started_at.to_rfc3339(),
        finished_at: finished_at.to_rfc3339(),
        status,
        outcome: outcome.to_owned(),
        error_kind,
        failover,
        response_started,
        first_byte_ms,
        first_token_ms,
        duration_ms: finished_at
            .signed_duration_since(started_at)
            .num_milliseconds(),
        usage,
        response_bytes,
        upstream_protocol,
        upstream_model_id,
    }))
}

/// 一次候选尝试的终态。它驱动全部三类遥测事件
/// （channel / attempt / request），三者因此不可能互相矛盾。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AttemptOutcome {
    Success,
    /// 网关自身产生了协议错误（转换失败、上游正文超过缓冲上限、
    /// 正文无法解码）。渠道未必有错——是否 `countable` 由各调用点决定。
    GatewayError,
    /// 上游以错误状态码或错误正文作答。
    UpstreamError,
    /// 在拿到可用状态码之前传输即失败（连接/重置/超时）。
    TransportError,
    Cancelled,
    /// 流在正文中途断开（空闲超时/上游重置）。
    StreamInterrupted,
}

impl AttemptOutcome {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::GatewayError => "gateway_error",
            Self::UpstreamError => "upstream_error",
            Self::TransportError => "transport_error",
            Self::Cancelled => "cancelled",
            Self::StreamInterrupted => "stream_interrupted",
        }
    }
}

/// 为一次终态尝试发出 channel/attempt/request 三类遥测。
/// 每个终结点都必须经过这里，三条事件流才会一致：
/// 同一 outcome、同一 error_kind、同一 status。
pub(super) struct AttemptFinalizer {
    pub(super) telemetry: std::sync::Arc<dyn crate::ports::EventSink>,
    pub(super) request_id: String,
    pub(super) request_started: chrono::DateTime<chrono::Utc>,
    pub(super) failure_threshold: i64,
    pub(super) circuit_open_seconds: i64,
}

impl AttemptFinalizer {
    pub(super) fn new(
        telemetry: std::sync::Arc<dyn crate::ports::EventSink>,
        request_id: &str,
        request_started: chrono::DateTime<chrono::Utc>,
        failure_threshold: i64,
        circuit_open_seconds: i64,
    ) -> Self {
        Self {
            telemetry,
            request_id: request_id.to_owned(),
            request_started,
            failure_threshold,
            circuit_open_seconds,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn finalize(
        &self,
        candidate: &Candidate,
        attempt_no: i64,
        started_at: chrono::DateTime<chrono::Utc>,
        finished_at: chrono::DateTime<chrono::Utc>,
        outcome: AttemptOutcome,
        status: Option<i64>,
        error_kind: Option<String>,
        failover: bool,
        response_started: bool,
        first_byte_ms: Option<i64>,
        first_token_ms: Option<i64>,
        usage: Usage,
        response_bytes: i64,
        upstream_protocol: Option<String>,
        upstream_model_id: Option<String>,
        countable: bool,
    ) {
        match outcome {
            AttemptOutcome::Success => {
                self.telemetry.emit(Event::ChannelSuccess {
                    channel_id: candidate.channel_id.clone(),
                });
            }
            AttemptOutcome::Cancelled => {
                // 客户端主动取消不构成对渠道的判定。
            }
            _ => {
                self.telemetry.emit(Event::ChannelFailure {
                    channel_id: candidate.channel_id.clone(),
                    error_kind: error_kind
                        .clone()
                        .unwrap_or_else(|| outcome.as_str().to_owned()),
                    status,
                    threshold: self.failure_threshold,
                    open_seconds: self.circuit_open_seconds,
                    countable,
                });
            }
        }
        self.telemetry.emit(attempt_event(
            &self.request_id,
            candidate,
            attempt_no,
            started_at,
            finished_at,
            status,
            outcome.as_str(),
            error_kind,
            failover,
            response_started,
            first_byte_ms,
            first_token_ms,
            usage,
            response_bytes,
            upstream_protocol,
            upstream_model_id,
        ));
        self.telemetry.emit(Event::RequestFinish {
            id: self.request_id.clone(),
            finished_at: finished_at.to_rfc3339(),
            duration_ms: finished_at
                .signed_duration_since(self.request_started)
                .num_milliseconds(),
            status,
            outcome: outcome.as_str().into(),
            attempts: attempt_no,
            channel_id: Some(candidate.channel_id.clone()),
            response_bytes,
        });
    }
}
/// 从尝试上下文构造终结器：所有 `AttemptEnv` 调用点的实参完全一致，
/// 集中在这里后，阈值/熔断窗口的取值只有一处。
pub(super) fn attempt_finalizer(env: &AttemptEnv<'_>) -> AttemptFinalizer {
    AttemptFinalizer::new(
        Arc::new(env.telemetry.clone()),
        env.request_id,
        env.started,
        env.runtime.failure_threshold,
        env.runtime.circuit_open_seconds,
    )
}

/// 转换响应正文无法解码时的终态遥测：一个 outcome 驱动
/// channel/attempt/request 三类事件，调用方发送稳定的 502。
/// 转换所需的明文并不存在。
pub(super) fn decode_failure(env: &AttemptEnv<'_>, failover: bool, response_bytes: i64) {
    let finished = env.clock.now_utc();
    attempt_finalizer(env)
    .finalize(
        env.candidate,
        env.attempts,
        env.attempt_started,
        finished,
        AttemptOutcome::GatewayError,
        Some(502),
        Some("upstream_decode_error".into()),
        failover,
        false,
        None,
        None,
        Usage::default(),
        response_bytes,
        Some(env.upstream_protocol.into()),
        Some(env.upstream_model.into()),
        true,
    );
}

/// 一次候选尝试除上游响应之外所需的全部上下文：
/// 让响应策略构造器（透明/转换流式、有界非流式）保持为
/// 独立函数，而不是内联闭包。
#[derive(Clone)]
pub(super) struct AttemptEnv<'a> {
    pub(super) telemetry: &'a Telemetry,
    pub(super) clock: Arc<dyn crate::ports::Clock>,
    pub(super) request_id: &'a str,
    pub(super) started: chrono::DateTime<chrono::Utc>,
    pub(super) candidate: &'a Candidate,
    pub(super) attempts: i64,
    pub(super) attempt_started: chrono::DateTime<chrono::Utc>,
    pub(super) attempt_started_instant: tokio::time::Instant,
    pub(super) runtime: &'a settings::RuntimeSettings,
    pub(super) entry_protocol: &'a str,
    pub(super) entry_model: &'a str,
    pub(super) upstream_protocol: &'a str,
    /// 本次候选实际发往上游的模型 id（记录进尝试遥测）。
    /// 普通路由下等于 `response_model`；自定义模型下
    /// 它是候选真实的 `channel_models.model_id`。
    pub(super) upstream_model: &'a str,
    /// 路由层（面向网关）的模型名，用于给转换后的响应打标——
    /// 仅 Command Code 解码与协议转换流使用。
    pub(super) response_model: &'a str,
    /// 本次尝试实际使用的 Command Code 传输方式：
    /// `Generate` 响应是 NDJSON，需要 CC 解码器；
    /// `Provider` 响应是普通 OpenAI SSE/JSON。
    pub(super) command_code_transport: Option<crate::commandcode::Transport>,
    /// 本次之后剩余的候选数（failover 资格标志）。
    pub(super) candidates_len: i64,
}
/// 组装一次候选尝试的上下文：四个调用点（压缩 / 透明流 / 转换流 /
/// 有界非流式）的实参完全一致，集中在这里后新增字段只需改一处。
///
/// 全部字段都是借用或廉价拷贝，刻意不引入 `Clone`/`Arc` 包装。
#[allow(clippy::too_many_arguments)]
pub(super) fn attempt_env<'a>(
    svc: &'a ProxyService,
    request_id: &'a str,
    started: chrono::DateTime<chrono::Utc>,
    candidate: &'a Candidate,
    attempts: i64,
    attempt_started: chrono::DateTime<chrono::Utc>,
    attempt_started_instant: tokio::time::Instant,
    runtime: &'a settings::RuntimeSettings,
    entry_protocol: &'a str,
    entry_model: &'a str,
    upstream_protocol: &'a str,
    upstream_model: &'a str,
    response_model: &'a str,
    candidates_len: i64,
    command_code_transport: Option<crate::commandcode::Transport>,
) -> AttemptEnv<'a> {
    AttemptEnv {
        telemetry: &svc.telemetry,
        clock: Arc::clone(&svc.clock),
        request_id,
        started,
        candidate,
        attempts,
        attempt_started,
        attempt_started_instant,
        runtime,
        entry_protocol,
        entry_model,
        upstream_protocol,
        upstream_model,
        response_model,
        candidates_len,
        command_code_transport,
    }
}

/// 到达已知终点的流的终态遥测：每个终结点都必须经过 [`AttemptFinalizer`]，
/// 且在终态标记处结束的流必须在客户端可能挂断之前记录结果——
/// 已完成的响应绝不能退化成带零值的 `cancelled`。
///
/// 透明转发与转换两条流式路径都通过它终结尝试，
/// 因此两者的 outcome/error_kind/status 口径保持一致。
#[allow(clippy::too_many_arguments)]
pub(super) fn finalize_stream_attempt(
    telemetry: &Telemetry,
    request_id: &str,
    started: chrono::DateTime<chrono::Utc>,
    failure_threshold: i64,
    circuit_open_seconds: i64,
    candidate: &Candidate,
    attempt_no: i64,
    attempt_started: chrono::DateTime<chrono::Utc>,
    finished: chrono::DateTime<chrono::Utc>,
    outcome: AttemptOutcome,
    final_status: i64,
    error_kind: Option<String>,
    countable: bool,
    first_byte_ms: Option<i64>,
    first_token_ms: Option<i64>,
    usage: Usage,
    response_bytes: i64,
    upstream_protocol: String,
    upstream_model: String,
) {
    AttemptFinalizer::new(
        Arc::new(telemetry.clone()),
        request_id,
        started,
        failure_threshold,
        circuit_open_seconds,
    )
    .finalize(
        candidate,
        attempt_no,
        attempt_started,
        finished,
        outcome,
        Some(final_status),
        error_kind,
        false,
        true,
        first_byte_ms,
        first_token_ms,
        usage,
        response_bytes,
        Some(upstream_protocol),
        Some(upstream_model),
        countable,
    );
}

/// 构造客户端中途断开时的“取消终态”回调：透明流转发与转换流转发
/// 各有一个字节级相同的闭包体，集中在这里后两者不可能再漂移。
///
/// 回调读取 **流关闭那一刻** 的真实统计（已转发字节、已解析的 usage、
/// first-token 时间），因此取消记录不会退化成一条零值记录。
#[allow(clippy::too_many_arguments)]
pub(super) fn cancel_on_cancel(
    telemetry: Telemetry,
    clock: Arc<dyn crate::ports::Clock>,
    request_id: String,
    candidate: Candidate,
    attempt_no: i64,
    attempt_started: chrono::DateTime<chrono::Utc>,
    started: chrono::DateTime<chrono::Utc>,
    status: u16,
    failure_threshold: i64,
    circuit_open_seconds: i64,
    upstream_protocol: String,
    upstream_model: String,
    stats: Arc<SharedStreamStats>,
    responded: Arc<AtomicBool>,
) -> Box<dyn FnOnce() + Send> {
    Box::new(move || {
        let finished = clock.now_utc();
        AttemptFinalizer::new(
            Arc::new(telemetry.clone()),
            &request_id,
            started,
            failure_threshold,
            circuit_open_seconds,
        )
        .finalize(
            &candidate,
            attempt_no,
            attempt_started,
            finished,
            AttemptOutcome::Cancelled,
            Some(status as i64),
            None,
            false,
            responded.load(Ordering::SeqCst),
            *stats.first_byte_ms.lock(),
            *stats.first_token_ms.lock(),
            stats.usage.lock().clone(),
            stats.bytes.load(Ordering::SeqCst),
            Some(upstream_protocol),
            Some(upstream_model),
            false,
        );
    })
}

/// 单次请求内跨候选共享的失败记录（与候选循环的局部变量一一对应）。
#[derive(Default)]
pub(super) struct FailoverState {
    /// 最近一次可回放的上游错误：状态、响应头、正文与渠道 id。
    pub(super) last_error: Option<(StatusCode, axum::http::HeaderMap, Vec<u8>, String)>,
    /// 最近一次纯传输失败的种类（从未收到上游状态码），
    /// 让最终网关错误能区分 504 超时与 502 不可达。
    pub(super) last_transport_kind: Option<TransportFailure>,
    /// 最近一次「网关侧」压缩失败（解码/校验/超限）：没有可回放的上游错误体，
    /// 但真实原因必须活到最终响应，而不是退化成一个笼统的 502。
    pub(super) last_gateway_error: Option<(StatusCode, &'static str, &'static str, String)>,
    /// OpenCode Zen/Go 会话头的惰性解析缓存；
    /// 只有路由到这类上游的请求才会读取它。
    pub(super) opencode_session_id: Option<String>,
}

impl FailoverState {
    /// URL / 出站头构造失败：只记录失败种类，不写 attempt 遥测
    /// （与既有行为一致，见报告中的已知不一致项）。
    pub(super) fn note_construction_failure(&mut self) {
        self.last_error = None;
        self.last_transport_kind = Some(TransportFailure::ConnectionReset);
    }
}

/// 一次候选尝试的推进结果。
pub(super) enum AttemptStep {
    /// 已有最终响应，直接返回给客户端。
    Respond(axum::http::Response<axum::body::Body>),
    /// 该候选失败，尝试下一个（若无则走最终网关错误）。
    Next,
}
