//! 代理服务与入口 handler：`ProxyService` 的构造、请求编排（候选循环）
//! 与全部 HTTP 入口。

use std::{
    collections::HashMap,
    sync::Arc,
    time::Duration,
};

use axum::{
    body::Body,
    extract::{Path, Request, State},
    http::{Response, StatusCode},
};
use bytes::Bytes;

use crate::{
    crypto::SecretStore,
    domain::{Candidate, CompactionMode, TransportFailure, Usage},
    protocol, settings,
    runtime::RuntimeLimits,
    state::AppState,
    telemetry::Telemetry,
};

// 同目录兄弟模块的内部项（`pub(super)` / `pub(crate)`）。
use super::attempt::*;
use super::commandcode::*;
use super::compaction::*;
use super::error::*;
use super::nonstream::*;
use super::prepare::*;
use super::stream::*;

/// 代理服务：网关的请求编排，由入口 handler 经 `AppState::proxy` 到达。
/// 上游、路由、设置与 Command Code 状态都经 `crate::ports` 端口访问，
/// 因此本服务不持有 `Database`；流水线各阶段位于策略构造器中
/// （`prepare_request`、`transparent_stream`、`mapped_stream`、
/// `bounded_non_stream`、`final_gateway_response`）。
pub struct ProxyService {
    pub(super) settings: Arc<dyn crate::ports::SettingsReader>,
    pub(super) command_code: Arc<dyn crate::ports::CommandCodeState>,
    pub(super) secrets: SecretStore,
    pub(super) http: Arc<dyn crate::ports::UpstreamClient>,
    pub(super) routes: Arc<dyn crate::ports::RouteRepository>,
    pub(super) telemetry: Telemetry,
    pub(super) clock: Arc<dyn crate::ports::Clock>,
    pub(super) limits: Arc<RuntimeLimits>,
    pub(super) notifier: Arc<dyn crate::ports::Notifier>,
    /// 每渠道的 Command Code `/alpha/generate` 在途上限
    /// （一个账号 = 一个 API key）。许可一直持有到上游
    /// 正文被完整消费，因此突发流量不会在同一账号上
    /// 扇出成并行的生成请求。容量保存在 map 中，
    /// 因此运行期设置变化会重建信号量。
    pub(super) command_code_slots: Arc<parking_lot::Mutex<HashMap<String, (usize, Arc<tokio::sync::Semaphore>)>>>,
}

/// [`ProxyService`] 的依赖集合：构造参数超过 7 个，收成一个结构体，
/// 既避免位置参数错位，也让新增依赖只改一处。
pub struct ProxyServiceDeps {
    pub settings: Arc<dyn crate::ports::SettingsReader>,
    pub command_code: Arc<dyn crate::ports::CommandCodeState>,
    pub secrets: SecretStore,
    pub http: Arc<dyn crate::ports::UpstreamClient>,
    pub routes: Arc<dyn crate::ports::RouteRepository>,
    pub telemetry: Telemetry,
    pub clock: Arc<dyn crate::ports::Clock>,
    pub limits: Arc<RuntimeLimits>,
    pub notifier: Arc<dyn crate::ports::Notifier>,
}

/// 候选循环的请求级不变量（每次请求构造一次，循环内只读）。
pub(super) struct RequestCtx<'a> {
    pub(super) service: &'a ProxyService,
    pub(super) request_id: &'a str,
    pub(super) started: chrono::DateTime<chrono::Utc>,
    pub(super) entry_protocol: &'a str,
    pub(super) entry_model: &'a str,
    /// 路由层（面向网关）的模型名。
    pub(super) upstream_model: &'a str,
    pub(super) runtime: &'a settings::RuntimeSettings,
    pub(super) candidates: &'a [Candidate],
    pub(super) compaction_mode: Option<CompactionMode>,
    pub(super) stream_requested: bool,
}

/// 本轮候选的尝试上下文：请求级不变量 + 本轮字段。
pub(super) struct CandidateCtx<'a> {
    pub(super) request: &'a RequestCtx<'a>,
    pub(super) candidate: &'a Candidate,
    /// 本轮的线格式协议：Command Code 提供商会把路由协议覆盖为 `command_code`。
    pub(super) upstream_protocol: &'a str,
    pub(super) attempts: i64,
    pub(super) attempt_started: chrono::DateTime<chrono::Utc>,
    pub(super) attempt_started_instant: tokio::time::Instant,
    pub(super) api_key: &'a str,
    pub(super) converted_body: &'a Bytes,
    pub(super) provider_body: Option<&'a Bytes>,
    /// 候选真实的上游模型 id（与路由名不同时才需要改写正文）。
    pub(super) candidate_model: Option<&'a str>,
}

impl CandidateCtx<'_> {
    /// 组装尝试上下文：四个调用点（压缩 / 透明流 / 转换流 / 有界非流式）
    /// 的实参完全一致，集中在这里后新增字段只需改一处。
    fn attempt_env(&self, cc_transport_used: Option<crate::commandcode::Transport>) -> AttemptEnv<'_> {
        attempt_env(
            self.request.service,
            self.request.request_id,
            self.request.started,
            self.candidate,
            self.attempts,
            self.attempt_started,
            self.attempt_started_instant,
            self.request.runtime,
            self.request.entry_protocol,
            self.request.entry_model,
            self.upstream_protocol,
            self.candidate.model_id.as_str(),
            self.request.upstream_model,
            self.request.candidates.len() as i64,
            cc_transport_used,
        )
    }
}

impl ProxyService {
    pub fn new(deps: ProxyServiceDeps) -> Arc<Self> {
        Arc::new(Self {
            settings: deps.settings,
            command_code: deps.command_code,
            secrets: deps.secrets,
            http: deps.http,
            routes: deps.routes,
            telemetry: deps.telemetry,
            clock: deps.clock,
            limits: deps.limits,
            notifier: deps.notifier,
            command_code_slots: command_code_slots(),
        })
    }

    /// 一个入口协议的完整代理流水线。
    /// 一次请求的编排：准备 → 逐候选尝试 → 全部失败后的最终网关响应。
    ///
    /// 候选循环的每个出口都经 [`AttemptFinalizer`] 终结一次遥测；
    /// 请求级不变量放在 [`RequestCtx`]，跨候选的失败记录放在 [`FailoverState`]。
    pub async fn proxy(
        &self,
        request: Request,
        entry_protocol: &str,
        fixed_path: Option<String>,
    ) -> Response<Body> {
        let request_id = uuid::Uuid::new_v4().to_string();
        let started = self.clock.now_utc();
        // 准备阶段（设置、鉴权、协议与候选）自成一相；
        // 失败在此短路返回网关错误。
        let prepared = match prepare_request(
            self,
            request,
            entry_protocol,
            fixed_path,
            &request_id,
            started,
        )
        .await
        {
            Ok(prepared) => prepared,
            Err(response) => return response,
        };
        let PreparedRequest {
            runtime,
            headers,
            path,
            query,
            entry_model,
            stream_requested,
            upstream_protocol,
            upstream_model,
            converted_body,
            provider_body,
            command_code_bodies,
            candidates,
            compaction_mode,
        } = prepared;
        let upstream_protocol = upstream_protocol.as_str();
        let upstream_model = upstream_model.as_str();
        let request_ctx = RequestCtx {
            service: self,
            request_id: &request_id,
            started,
            entry_protocol,
            entry_model: &entry_model,
            upstream_model,
            runtime: &runtime,
            candidates: &candidates,
            compaction_mode,
            stream_requested,
        };
        let mut state = FailoverState::default();
        let mut attempts = 0i64;
        'candidates: for (index, candidate) in candidates.iter().enumerate() {
            attempts = index as i64 + 1;
            // 自定义模型：面向网关的路由名可能不同于该候选真实的
            // 上游 id。仅在两者不同时才改写，
            // 这样普通的逐字节直通正文永远不会被重新序列化。
            let candidate_model: Option<&str> =
                (candidate.model_id != upstream_model).then_some(candidate.model_id.as_str());
            // 进入备用候选意味着上一个候选失败了：提醒用户。
            // 远程压缩的 failover 有意保持静默：Codex 客户端自己会经网关重试/回退，
            // 因此不发桌面通知。notifier 会排队并合并通知（1s 窗口）——此路径从不阻塞。
            if index > 0 && compaction_mode.is_none() {
                self.notify_failover(&request_ctx, &state, index);
            }
            // Command Code provider 始终说 CC 协议（官方 Provider API 或
            // `/alpha/generate`）；路由协议只描述客户端入口，因此候选自身的协议
            // （及线格式正文）在本轮迭代余下部分覆盖路由级的值。
            // 非 CC 候选原样沿用路由协议。
            let upstream_protocol: &str = if upstream_protocol != "command_code"
                && protocol::requires_command_code_identity(candidate.kind.as_deref())
            {
                "command_code"
            } else {
                upstream_protocol
            };
            // `Bytes::clone` 只增加引用计数——不拷贝正文。
            let converted_body = match command_code_bodies.as_ref() {
                Some(bodies) if upstream_protocol == "command_code" => bodies.generate.clone(),
                _ => converted_body.clone(),
            };
            let provider_body = match command_code_bodies.as_ref() {
                Some(bodies) if upstream_protocol == "command_code" => Some(bodies.provider.clone()),
                _ => provider_body.clone(),
            };
            let attempt_started = self.clock.now_utc();
            // 首 token 记账的绝对锚点：截止时间覆盖整次尝试，
            // 因此缓慢的响应头无法延长窗口。
            let attempt_started_instant = tokio::time::Instant::now();
            let api_key = match self.secrets.decrypt(&candidate.api_key_encrypted) {
                Ok(value) => value,
                Err(error) => {
                    let _ = error;
                    self.fail_transport(
                        &request_ctx,
                        &mut state,
                        candidate,
                        attempts,
                        attempt_started,
                        upstream_protocol,
                        TransportFailure::ConnectionReset,
                        Some("key_decrypt_error".into()),
                    );
                    continue;
                }
            };
            let ctx = CandidateCtx {
                request: &request_ctx,
                candidate,
                upstream_protocol,
                attempts,
                attempt_started,
                attempt_started_instant,
                api_key: &api_key,
                converted_body: &converted_body,
                provider_body: provider_body.as_ref(),
                candidate_model,
            };
            match self
                .attempt_candidate(&ctx, &mut state, &headers, &path, query.as_deref())
                .await
            {
                AttemptStep::Respond(response) => return response,
                AttemptStep::Next => continue 'candidates,
            }
        }
        final_gateway_response(
            &self.telemetry,
            self.clock.as_ref(),
            entry_protocol,
            &request_id,
            started,
            attempts,
            state.last_error,
            state.last_gateway_error,
            state.last_transport_kind,
        )
    }

    /// 进入备用候选时的桌面提醒（`state` 描述上一个候选的失败）。
    fn notify_failover(&self, ctx: &RequestCtx<'_>, state: &FailoverState, index: usize) {
        let error_kind = state
            .last_transport_kind
            .map(|kind| kind.as_str().to_owned())
            .or_else(|| {
                state
                    .last_error
                    .as_ref()
                    .map(|(status, _, _, _)| format!("HTTP {}", status.as_u16()))
            });
        self.notifier.notify_failover(crate::ports::FailoverNotice {
            model_id: ctx.entry_model.to_owned(),
            failed_channel_name: ctx.candidates[index - 1].channel_name.clone(),
            next_channel_name: ctx.candidates[index].channel_name.clone(),
            error_kind,
        });
    }

    /// 传输失败（从未拿到可用状态码）的终态：记录失败种类并终结该次尝试的遥测。
    #[allow(clippy::too_many_arguments)]
    pub(super) fn fail_transport(
        &self,
        ctx: &RequestCtx<'_>,
        state: &mut FailoverState,
        candidate: &Candidate,
        attempts: i64,
        attempt_started: chrono::DateTime<chrono::Utc>,
        upstream_protocol: &str,
        kind: TransportFailure,
        error_kind: Option<String>,
    ) {
        let finished = self.clock.now_utc();
        state.last_error = None;
        state.last_transport_kind = Some(kind);
        AttemptFinalizer::new(
            Arc::new(self.telemetry.clone()),
            ctx.request_id,
            ctx.started,
            ctx.runtime.failure_threshold,
            ctx.runtime.circuit_open_seconds,
        )
        .finalize(
            candidate,
            attempts,
            attempt_started,
            finished,
            AttemptOutcome::TransportError,
            None,
            error_kind,
            attempts < ctx.candidates.len() as i64,
            false,
            None,
            None,
            Usage::default(),
            0,
            Some(upstream_protocol.into()),
            Some(candidate.model_id.clone()),
            ctx.compaction_mode.is_none(),
        );
    }

    /// 一次候选尝试：Command Code 传输轮 → 响应分派。
    /// 任何出口都不会再回到本候选。
    async fn attempt_candidate(
        &self,
        ctx: &CandidateCtx<'_>,
        state: &mut FailoverState,
        headers: &axum::http::HeaderMap,
        path: &str,
        query: Option<&str>,
    ) -> AttemptStep {
        // Command Code 传输路由器（见 [`ProxyService::cc_transport_rounds`]）：
        // 非 CC 协议一轮即返回；CC 渠道先试官方 Provider API，
        // 只有 403 upgrade_required 才会降级到 `/alpha/generate`。
        let Some((response, cc_transport_used)) = self
            .cc_transport_rounds(ctx, state, headers, path, query)
            .await
        else {
            return AttemptStep::Next;
        };
        let status = response.status;
        let response_headers = protocol::response_headers(&response.headers);
        self.dispatch_response(
            ctx,
            state,
            response,
            status,
            response_headers,
            cc_transport_used,
        )
        .await
    }

    /// 上游响应分派：远程压缩 → 透明流 → 转换流 → 有界非流式；
    /// 非 2xx 交给 [`Self::handle_upstream_error`]。
    async fn dispatch_response(
        &self,
        ctx: &CandidateCtx<'_>,
        state: &mut FailoverState,
        response: crate::ports::UpstreamResponse,
        status: StatusCode,
        response_headers: axum::http::HeaderMap,
        cc_transport_used: Option<crate::commandcode::Transport>,
    ) -> AttemptStep {
        if let Some(mode) = ctx.request.compaction_mode {
            // 远程压缩有自己的构造器：V1 是一次性（unary）的，
            // V2 则在向客户端发出任何字节之前完成缓冲/校验，
            // 因此损坏或不受支持的上游响应仍能 failover。
            match compaction_attempt(
                self.routes.as_ref(),
                ctx.attempt_env(cc_transport_used),
                mode,
                self.limits.error_body_max,
                response,
                status,
                response_headers,
            )
            .await
            {
                CompactionResult::Respond(result) => return AttemptStep::Respond(result),
                CompactionResult::FailOver(transport) => {
                    state.last_transport_kind = transport;
                    state.last_error = None;
                    return AttemptStep::Next;
                }
                CompactionResult::FailOverError(
                    error_status,
                    error_headers,
                    raw,
                    channel_id,
                ) => {
                    state.last_transport_kind = None;
                    state.last_gateway_error = None;
                    state.last_error = Some((error_status, error_headers, raw, channel_id));
                    return AttemptStep::Next;
                }
                CompactionResult::FailOverGateway {
                    status,
                    code,
                    message,
                    channel_id,
                } => {
                    state.last_transport_kind = None;
                    state.last_error = None;
                    state.last_gateway_error = Some((status, code, message, channel_id));
                    return AttemptStep::Next;
                }
            }
        }
        if status.is_success() {
            if ctx.request.stream_requested && ctx.upstream_protocol != "command_code" {
                // 透明转发自成一个构造器。
                return AttemptStep::Respond(transparent_stream(
                    ctx.attempt_env(cc_transport_used),
                    response,
                    status,
                    response_headers,
                ));
            }
            // 需要协议转换的响应经增量转换器流式输出（同协议直通原样转发字节）。
            // 先缓冲一小段 prelude，这样 200 响应若其实是上游错误仍能 failover，
            // 并且像历史实现一样遵守 `first_token_timeout_seconds`。
            // 其余正文按事件逐个转换，客户端因此看到的是实时流。
            if ctx.request.stream_requested {
                return match mapped_stream(
                    ctx.attempt_env(cc_transport_used),
                    response,
                    status,
                    response_headers,
                )
                .await
                {
                    MappedStreamResult::Respond(result) => AttemptStep::Respond(result),
                    MappedStreamResult::FailOver(transport) => {
                        state.last_transport_kind = transport;
                        state.last_error = None;
                        AttemptStep::Next
                    }
                };
            }
            return match bounded_non_stream(
                ctx.attempt_env(cc_transport_used),
                response,
                status,
                response_headers,
            )
            .await
            {
                NonStreamResult::Respond(result) => AttemptStep::Respond(result),
                NonStreamResult::FailOver(transport) => {
                    state.last_transport_kind = transport;
                    state.last_error = None;
                    AttemptStep::Next
                }
            };
        }
        self.handle_upstream_error(ctx, state, response, status, response_headers)
            .await
    }

    /// 非 2xx 的上游响应：缓冲错误正文（上限 `error_body_max`）、
    /// Command Code 配额冷却，并终结该次尝试。
    async fn handle_upstream_error(
        &self,
        ctx: &CandidateCtx<'_>,
        state: &mut FailoverState,
        response: crate::ports::UpstreamResponse,
        status: StatusCode,
        response_headers: axum::http::HeaderMap,
    ) -> AttemptStep {
        // 错误正文只为回放/转换而缓冲，且上限为 1 MiB；超过则截断并记录。
        let (raw_result, truncated) = read_bounded_body(
            response.body.into_stream(),
            Duration::from_secs(ctx.request.runtime.first_byte_timeout_seconds.max(1) as u64),
            self.limits.error_body_max,
        )
        .await;
        // 读取失败时按空正文回放：下面的 `last_error` 才是这次失败的描述，
        // 传输种类不再单独记录（紧接着就会被清空）。
        let raw = raw_result.unwrap_or_default();
        if truncated {
            tracing::warn!(
                request_id = %ctx.request.request_id,
                channel_id = %ctx.candidate.channel_id,
                status = %status,
                bytes_captured = self.limits.error_body_max,
                body_truncated = true,
                "upstream error body truncated at the error-body cap"
            );
        }
        // 上游以错误状态码作答：这个失败由 `last_error` 描述，
        // 而不是之前的任何传输种类。
        state.last_transport_kind = None;
        // Command Code 配额窗口（402 payment required / 429）：将该渠道冷却到窗口重置，
        // 让既有的候选排序移到下一个账号；健康监督器在窗口过后恢复它。
        if ctx.upstream_protocol == "command_code"
            && crate::commandcode::is_quota_status(status.as_u16())
        {
            let reset_at = crate::commandcode::quota_reset_at(&raw);
            if let Err(error) = self
                .command_code
                .mark_quota_exhausted(
                    &ctx.candidate.channel_id,
                    reset_at,
                    ctx.request.runtime.circuit_open_seconds,
                    status.as_u16(),
                )
                .await
            {
                tracing::warn!(%error, "command code quota cooldown persist failed");
            }
        }
        let (kind, countable) = status_kind(status);
        let finished = self.clock.now_utc();
        AttemptFinalizer::new(
            Arc::new(self.telemetry.clone()),
            ctx.request.request_id,
            ctx.request.started,
            ctx.request.runtime.failure_threshold,
            ctx.request.runtime.circuit_open_seconds,
        )
        .finalize(
            ctx.candidate,
            ctx.attempts,
            ctx.attempt_started,
            finished,
            AttemptOutcome::UpstreamError,
            Some(status.as_u16() as i64),
            Some(kind.into()),
            ctx.attempts < ctx.request.candidates.len() as i64,
            false,
            None,
            None,
            Usage::default(),
            raw.len() as i64,
            Some(ctx.upstream_protocol.into()),
            Some(ctx.candidate.model_id.clone()),
            countable,
        );
        state.last_error = Some((
            status,
            response_headers,
            raw,
            ctx.candidate.channel_id.clone(),
        ));
        // 有了可回放的上游错误体，之前记录的网关侧原因就不再是最终原因。
        state.last_gateway_error = None;
        AttemptStep::Next
    }
}

pub(super) async fn normal(
    State(state): State<AppState>,
    request: Request,
    protocol: &'static str,
) -> Response<Body> {
    state.proxy.proxy(request, protocol, None).await
}

pub async fn openai(State(state): State<AppState>, request: Request) -> Response<Body> {
    normal(State(state), request, "openai_compatible").await
}
pub async fn responses(State(state): State<AppState>, request: Request) -> Response<Body> {
    normal(State(state), request, "openai_responses").await
}
pub async fn responses_compact(
    State(state): State<AppState>,
    request: Request,
) -> Response<Body> {
    normal(State(state), request, "openai_responses").await
}
pub async fn claude(State(state): State<AppState>, request: Request) -> Response<Body> {
    normal(State(state), request, "claude").await
}
pub async fn gemini(
    State(state): State<AppState>,
    Path(_action): Path<String>,
    request: Request,
) -> Response<Body> {
    normal(State(state), request, "gemini").await
}
