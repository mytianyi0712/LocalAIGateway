//! Command Code 专用：每渠道的生成并发许可（信号量）与传输模式
//! 探测/降级。

use std::{
    collections::HashMap,
    sync::Arc,
};

use std::time::Duration;

use axum::http::StatusCode;
use bytes::Bytes;
use futures_util::StreamExt;

use crate::domain::TransportFailure;
use crate::protocol;

use super::attempt::*;
use super::service::*;

/// 一轮 Command Code 传输的推进结果。
pub(super) enum CcRound {
    /// 拿到上游响应，以及本轮实际使用的传输模式。
    Send(
        crate::ports::UpstreamResponse,
        Option<crate::commandcode::Transport>,
    ),
    /// 官方 API 返回 403 upgrade_required：已把渠道记为 `generate`，需要重新发包。
    Downgrade,
    /// 本轮失败（URL/头构造、并发许可等待超时、传输错误）：已终结遥测，换下一个候选。
    FailCandidate,
}

/// 每渠道的 Command Code 突发保护：返回的信号量限制单个账号
/// （API key）在途的 `/alpha/generate` 请求数。map 的键是 `channel_id`；
/// 运行期容量变化会就地重建信号量（新的请求用新容量，
/// 已持有的许可仍在旧信号量上结束）。
pub(super) fn command_code_slots()
-> Arc<parking_lot::Mutex<HashMap<String, (usize, Arc<tokio::sync::Semaphore>)>>> {
    Arc::new(parking_lot::Mutex::new(HashMap::new()))
}

/// 把每渠道许可附到上游正文上，使其一直持有到
/// 正文（流式或缓冲）被完整消费并丢弃。
pub(super) fn hold_command_code_slot(
    response: crate::ports::UpstreamResponse,
    slot: Option<tokio::sync::OwnedSemaphorePermit>,
) -> crate::ports::UpstreamResponse {
    let Some(permit) = slot else {
        return response;
    };
    let inner = response.body.into_stream();
    let stream = futures_util::stream::unfold(
        (inner, permit),
        |(mut inner, permit)| async move {
            inner
                .next()
                .await
                .map(|item| (item, (inner, permit)))
        },
    );
    crate::ports::UpstreamResponse {
        status: response.status,
        headers: response.headers,
        body: crate::ports::UpstreamBody::new(Box::pin(stream)),
    }
}

impl ProxyService {
    /// Command Code 传输路由器：非 CC 协议一轮即返回；CC 渠道先试官方
    /// Provider API，只有文档化的 403 upgrade_required 才会把该渠道降级到
    /// CLI 兼容的 `/alpha/generate` 路径。GOAT/Pro/Max 从不进入反向路径；
    /// 影响面仅限服务器自身拒绝的 Go 账号，因此不会影响其余套餐的既有行为。
    /// （未知状态先按 Provider 探测。）
    ///
    /// 返回 `None` 表示本轮尝试已失败并终结遥测，应换下一个候选。
    pub(super) async fn cc_transport_rounds(
        &self,
        ctx: &CandidateCtx<'_>,
        state: &mut FailoverState,
        headers: &axum::http::HeaderMap,
        path: &str,
        query: Option<&str>,
    ) -> Option<(crate::ports::UpstreamResponse, Option<crate::commandcode::Transport>)> {
        let mut cc_transport = if ctx.upstream_protocol == "command_code" {
            Some(self.command_code.transport(&ctx.candidate.channel_id).await)
        } else {
            None
        };
        loop {
            match self
                .cc_transport_round(ctx, state, &mut cc_transport, headers, path, query)
                .await
            {
                CcRound::Send(response, transport) => return Some((response, transport)),
                // 已把渠道记为 `generate`：重新发包。
                CcRound::Downgrade => continue,
                CcRound::FailCandidate => return None,
            }
        }
    }

    /// 一轮 Command Code 传输：构造目标路径与正文 → 出站头与身份头 → 取并发许可
    /// → 发包。非 CC 协议同样走这里（一轮即返回）。
    async fn cc_transport_round(
        &self,
        ctx: &CandidateCtx<'_>,
        state: &mut FailoverState,
        cc_transport: &mut Option<crate::commandcode::Transport>,
        headers: &axum::http::HeaderMap,
        path: &str,
        query: Option<&str>,
    ) -> CcRound {
        let transport = cc_transport.unwrap_or(crate::commandcode::Transport::Provider);
        let (target_path, request_body) = if ctx.upstream_protocol == "command_code" {
            match transport {
                crate::commandcode::Transport::Generate => (
                    crate::commandcode::GENERATE_PATH.to_owned(),
                    ctx.converted_body.clone(),
                ),
                // 未知状态先用官方 API 探测；
                // 已记住/已是 provider 的渠道继续沿用。
                _ => (
                    crate::commandcode::PROVIDER_CHAT_PATH.to_owned(),
                    ctx.provider_body
                        .cloned()
                        .unwrap_or_else(|| ctx.converted_body.clone()),
                ),
            }
        } else {
            (path.to_owned(), ctx.converted_body.clone())
        };
        // 候选真实的上游模型 id 与路由名不同时改写正文与路径
        // （gemini 的路径里内嵌模型名）。
        let (target_path, request_body) = match ctx.candidate_model {
            Some(model) => {
                let (path, body) =
                    protocol::retarget_model(ctx.upstream_protocol, &target_path, &request_body, model);
                (path, body.map(Bytes::from).unwrap_or(request_body))
            }
            None => (target_path, request_body),
        };
        let target_url = match protocol::upstream_url(
            &ctx.candidate.base_url,
            &target_path,
            query,
            ctx.upstream_protocol,
        ) {
            Ok(value) => value,
            Err(error) => {
                let _ = error;
                state.note_construction_failure();
                return CcRound::FailCandidate;
            }
        };
        let mut outbound =
            match protocol::outbound_headers(headers, ctx.upstream_protocol, ctx.api_key) {
                Ok(value) => value,
                Err(error) => {
                    let _ = error;
                    state.note_construction_failure();
                    return CcRound::FailCandidate;
                }
            };
        if protocol::requires_opencode_session(&ctx.candidate.base_url) {
            if state.opencode_session_id.is_none() {
                state.opencode_session_id = Some(self.settings.opencode_session_id().await);
            }
            let session_id = state.opencode_session_id.as_deref().unwrap_or_default();
            if let Err(error) = protocol::apply_opencode_session(
                &mut outbound,
                &ctx.candidate.base_url,
                session_id,
            ) {
                tracing::warn!(%error, "opencode session header skipped");
            }
        }
        if ctx.upstream_protocol == "command_code"
            && transport == crate::commandcode::Transport::Generate
        {
            // 指纹/生命周期初始化按渠道（API key）节流；
            // 失败从不阻塞实际请求。
            if let Err(error) = self
                .command_code
                .ensure_initialized(
                    &ctx.candidate.channel_id,
                    ctx.api_key,
                    &ctx.candidate.base_url,
                    ctx.request.runtime.command_code_init_interval_hours,
                )
                .await
            {
                tracing::warn!(
                    %error,
                    channel_id = %ctx.candidate.channel_id,
                    "command code init skipped"
                );
            }
            // 身份头按 provider `kind` 生成，
            // 绝不从 base_url 推断。
            if protocol::requires_command_code_identity(ctx.candidate.kind.as_deref()) {
                match self.command_code.identity(&ctx.candidate.channel_id).await {
                    Ok(identity) => {
                        if let Err(error) =
                            protocol::apply_command_code_identity(&mut outbound, &identity)
                        {
                            tracing::warn!(%error, "command code identity header skipped");
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, "command code identity unavailable");
                    }
                }
            } else {
                tracing::warn!(
                    channel_id = %ctx.candidate.channel_id,
                    "command_code channel without kind marker: CLI identity headers not injected"
                );
            }
        }
        // 每账号突发保护：限制一个渠道在途的 `/alpha/generate` 请求数。
        // 许可附在下面的响应正文上，正文被丢弃时释放。
        let cc_slot = match self.acquire_cc_slot(ctx, transport).await {
            Ok(slot) => slot,
            Err(kind) => {
                // 突发保护触发：failover 到下一个候选，
                // 而不是继续堆积到这个账号。
                self.fail_transport(
                    ctx.request,
                    state,
                    ctx.candidate,
                    ctx.attempts,
                    ctx.attempt_started,
                    ctx.upstream_protocol,
                    kind,
                    Some("concurrency_limit".into()),
                );
                return CcRound::FailCandidate;
            }
        };
        // 上游端口施加连接超时与发送截止时间；分类后的错误决定传输种类。
        let response = match self.send_upstream(ctx, target_url, outbound, request_body).await {
            Ok(response) => response,
            Err((kind, error_kind)) => {
                self.fail_transport(
                    ctx.request,
                    state,
                    ctx.candidate,
                    ctx.attempts,
                    ctx.attempt_started,
                    ctx.upstream_protocol,
                    kind,
                    error_kind,
                );
                return CcRound::FailCandidate;
            }
        };
        // Go 套餐（或后续的套餐降级）：官方 API 返回 403 upgrade_required。
        // 渠道一旦已知处于 `generate`，就不再探测 provider。
        if ctx.upstream_protocol == "command_code"
            && response.status == StatusCode::FORBIDDEN
            && *cc_transport != Some(crate::commandcode::Transport::Generate)
        {
            let (body, _truncated) = response
                .body
                .read_capped(self.limits.error_body_max)
                .await;
            if crate::commandcode::is_upgrade_required(403, &body) {
                if let Err(error) = self
                    .command_code
                    .set_transport(
                        &ctx.candidate.channel_id,
                        crate::commandcode::Transport::Generate,
                    )
                    .await
                {
                    tracing::warn!(%error, "command code transport persist failed");
                }
                tracing::info!(
                    channel_id = %ctx.candidate.channel_id,
                    "command code provider API requires upgrade; using /alpha/generate"
                );
                *cc_transport = Some(crate::commandcode::Transport::Generate);
                return CcRound::Downgrade;
            }
            // 不是升级信号：回放已缓冲的正文，
            // 让下面的通用错误路径原样看到它。
            let replayed = crate::ports::UpstreamResponse {
                status: response.status,
                headers: response.headers.clone(),
                body: crate::ports::UpstreamBody::new(Box::pin(futures_util::stream::once(
                    async move { Ok::<Bytes, crate::ports::UpstreamError>(Bytes::from(body)) },
                ))),
            };
            return CcRound::Send(hold_command_code_slot(replayed, cc_slot), *cc_transport);
        }
        // 成功的 provider 探测会被记住，后续请求跳过检测步骤。
        if ctx.upstream_protocol == "command_code"
            && response.status.is_success()
            && *cc_transport == Some(crate::commandcode::Transport::Unknown)
        {
            if let Err(error) = self
                .command_code
                .set_transport(
                    &ctx.candidate.channel_id,
                    crate::commandcode::Transport::Provider,
                )
                .await
            {
                tracing::warn!(%error, "command code transport persist failed");
            }
        }
        CcRound::Send(hold_command_code_slot(response, cc_slot), *cc_transport)
    }

    /// 取该渠道的 Command Code 生成许可。非 CC / 非 `Generate` 路径直接返回
    /// `None`；等待超时（突发保护）返回 `Err(FirstByteTimeout)`。
    async fn acquire_cc_slot(
        &self,
        ctx: &CandidateCtx<'_>,
        transport: crate::commandcode::Transport,
    ) -> Result<Option<tokio::sync::OwnedSemaphorePermit>, TransportFailure> {
        if ctx.upstream_protocol != "command_code"
            || transport != crate::commandcode::Transport::Generate
        {
            return Ok(None);
        }
        let desired = ctx.request.runtime.command_code_max_concurrency.clamp(1, 32) as usize;
        let semaphore = {
            let mut map = self.command_code_slots.lock();
            let entry = map
                .entry(ctx.candidate.channel_id.clone())
                .or_insert_with(|| (desired, Arc::new(tokio::sync::Semaphore::new(desired))));
            if entry.0 != desired {
                // 设置变了：新请求用新容量；
                // 已持有的许可仍在旧容量上结束。
                *entry = (desired, Arc::new(tokio::sync::Semaphore::new(desired)));
            }
            Arc::clone(&entry.1)
        };
        let wait = Duration::from_secs(ctx.request.runtime.first_byte_timeout_seconds.max(1) as u64);
        match tokio::time::timeout(wait, semaphore.acquire_owned()).await {
            Ok(Ok(permit)) => Ok(Some(permit)),
            Ok(Err(_closed)) => Ok(None),
            Err(_) => Err(TransportFailure::FirstByteTimeout),
        }
    }

    /// 发包并分类传输错误（连接超时 / 连接重置 / 首字节截止时间）。
    async fn send_upstream(
        &self,
        ctx: &CandidateCtx<'_>,
        url: url::Url,
        headers: axum::http::HeaderMap,
        body: Bytes,
    ) -> Result<crate::ports::UpstreamResponse, (TransportFailure, Option<String>)> {
        let runtime = ctx.request.runtime;
        match self
            .http
            .send(crate::ports::UpstreamRequest {
                url,
                headers,
                method: http::Method::POST,
                body: Some(body),
                connect_timeout: Duration::from_secs(runtime.connect_timeout_seconds.max(1) as u64),
                deadline: Duration::from_secs(runtime.first_byte_timeout_seconds.max(1) as u64),
            })
            .await
        {
            Ok(response) => Ok(response),
            Err(crate::ports::UpstreamError::ConnectTimeout) => {
                Err((TransportFailure::ConnectTimeout, Some("connect_timeout".into())))
            }
            Err(crate::ports::UpstreamError::Transport(error)) => {
                Err((TransportFailure::ConnectionReset, Some(error)))
            }
            Err(crate::ports::UpstreamError::Deadline) => {
                Err((TransportFailure::FirstByteTimeout, Some("timeout".into())))
            }
        }
    }
}
