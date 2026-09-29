//! 非流式响应路径：有界缓冲、无损解码与映射正文的一次性响应。

use std::time::Duration;

use anyhow::Result;
use axum::{
    body::Body,
    http::{HeaderValue, Response, StatusCode},
};
use bytes::Bytes;
use futures_util::StreamExt;
use serde_json::{Value, json};

use crate::{
    compression, convert,
    domain::{TransportFailure, Usage},
    protocol,
};

// 同目录兄弟模块的内部项（`pub(super)` / `pub(crate)`）。
use super::attempt::*;
use super::compaction::*;
use super::error::*;

pub(super) fn response_with_headers(
    status: StatusCode,
    headers: axum::http::HeaderMap,
    body: Vec<u8>,
) -> Response<Body> {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    for (name, value) in headers {
        if let Some(name) = name {
            response.headers_mut().append(name, value);
        }
    }
    response
}

/// 转换类响应的统一头收尾：响应体已被解码/转换，上游的 `content-length`
/// 与 `content-encoding` 不再描述它，`content-type` 由调用方指定
/// （SSE 用 `text/event-stream`，一次性 JSON 用 `application/json`）。
pub(super) fn transformed_body_response(
    status: StatusCode,
    headers: axum::http::HeaderMap,
    content_type: &'static str,
    body: Body,
) -> Response<Body> {
    let mut response = Response::new(body);
    *response.status_mut() = status;
    let mut headers = headers;
    headers.remove("content-length");
    headers.remove("content-encoding");
    headers.insert("content-type", HeaderValue::from_static(content_type));
    *response.headers_mut() = headers;
    response
}

/// 解码一个已完整缓冲的上游响应体（映射非流式与映射压缩路径共用）。
///
/// 必须走 [`compression::RequiredDecoder::feed_all`]：单次 feed 的输出被
/// [`compression::FEED_LIMIT`] 截断，直接判 `finished()` 会把超过 2 MiB 的
/// 合法响应误判成“截断”。失败时返回原因，由调用方终结该次尝试。
pub(super) fn decode_required_buffered(
    content_encoding: Option<&HeaderValue>,
    raw: &[u8],
    cap: usize,
) -> Result<Vec<u8>, compression::DecodeError> {
    let mut decoder = compression::RequiredDecoder::new(
        compression::ContentDecoder::from_encoding(content_encoding),
        cap,
    );
    decoder.feed_all(raw)
}

/// 以硬字节上限读取响应体。返回缓冲到的字节（至多 `cap`）以及是否撞到上限；
/// 撞到上限后不再继续抽干流。整个读取受 `timeout` 约束：正文中途的传输错误
/// 归为 `ConnectionReset`，超时归为 `FirstByteTimeout`——
/// 与既有的非流式分类保持一致。
pub(super) async fn read_bounded_body<S>(
    mut stream: S,
    timeout: Duration,
    cap: usize,
) -> (Result<Vec<u8>, TransportFailure>, bool)
where
    S: futures_util::Stream<Item = Result<Bytes, crate::ports::UpstreamError>> + Unpin,
{
    let mut buf = Vec::with_capacity(cap.min(64 * 1024));
    let mut truncated = false;
    let read = async {
        while let Some(item) = stream.next().await {
            let chunk = item.map_err(|_| TransportFailure::ConnectionReset)?;
            let remaining = cap - buf.len();
            if chunk.len() > remaining {
                buf.extend_from_slice(&chunk[..remaining]);
                truncated = true;
                // 撞到硬上限：停止缓冲。未交付的尾部随流丢弃
                // （连接关闭，不复用）。
                break;
            }
            buf.extend_from_slice(&chunk);
        }
        Ok::<(), TransportFailure>(())
    };
    match tokio::time::timeout(timeout, read).await {
        Ok(Ok(())) => (Ok(buf), truncated),
        Ok(Err(kind)) => (Err(kind), truncated),
        Err(_) => (Err(TransportFailure::FirstByteTimeout), truncated),
    }
}

/// 把非流式上游响应体解析成 Usage。
pub(super) fn usage_from_body(protocol: &str, body: &[u8]) -> Usage {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return Usage::default();
    };
    match protocol::usage_value(protocol, &value) {
        Some(usage) => protocol::normalize_usage(protocol, &usage),
        None => Usage::default(),
    }
}

/// 有界非流式构造器的结果：要么返回一个响应，
/// 要么是一个传输失败——它已经发出终态遥测，
/// 应 failover 到下一个候选。
pub(super) enum NonStreamResult {
    Respond(Response<Body>),
    FailOver(Option<TransportFailure>),
}

/// 非流式成功响应（映射与非映射）：有界缓冲、映射转换所需的无损解码、
/// 转换，以及统一的终态遥测。
pub(super) async fn bounded_non_stream(
    env: AttemptEnv<'_>,
    response: crate::ports::UpstreamResponse,
    status: axum::http::StatusCode,
    response_headers: axum::http::HeaderMap,
) -> NonStreamResult {
    // 映射的非流式响应必须完整缓冲才能转换，
    // 但缓冲是有界的——先预检 Content-Length，
    // 再由分块累积执行硬上限。
    let buffered_cap = (env.runtime.max_buffered_upstream_body_mb.max(1) as usize) * 1024 * 1024;
    let (raw, decoded) =
        match read_decoded_body(&env, response, &response_headers, buffered_cap).await {
            Ok(value) => value,
            Err(result) => return result,
        };
    // Command Code 的两种传输都交给转换层一个 OpenAI 形状的正文。
    let body_protocol = if env.upstream_protocol == "command_code" {
        "openai_compatible"
    } else {
        env.upstream_protocol
    };
    let usage = usage_from_body(body_protocol, &decoded);
    let assembled =
        respond_non_stream(&env, status, response_headers, &decoded, &raw, body_protocol, usage);
    // 一个 outcome 驱动全部三类事件。转换失败是网关错误——绝不是成功——
    // 不带 usage、不发 ChannelSuccess；渠道判定保持不动。
    let (outcome, error_kind, usage, countable): (AttemptOutcome, Option<String>, Usage, bool) =
        if assembled.conversion_failed {
            (
                AttemptOutcome::GatewayError,
                Some("conversion_error".into()),
                Usage::default(),
                false,
            )
        } else {
            (AttemptOutcome::Success, None, assembled.usage, false)
        };
    finalize_non_stream_attempt(
        &env,
        status,
        outcome,
        error_kind,
        usage,
        assembled.response_bytes,
        countable,
    );
    NonStreamResult::Respond(assembled.response)
}

#[allow(clippy::result_large_err)]
/// 读取并解码非流式正文：Content-Length 预检 → 有界读取 → 无损解码
/// （映射路径）或尽力而为解码（非映射路径）→ Command Code NDJSON 聚合。
///
/// 返回 `(原始字节, 解码后字节)`；失败时返回已终结遥测的终态结果。
async fn read_decoded_body(
    env: &AttemptEnv<'_>,
    response: crate::ports::UpstreamResponse,
    response_headers: &axum::http::HeaderMap,
    buffered_cap: usize,
) -> Result<(Vec<u8>, Vec<u8>), NonStreamResult> {
    // 从原始 reqwest 头读取：`response_headers` 会把 content-length 当作
    // hop-by-hop 剥掉，但预检需要的正是声明的长度。
    if let Some(content_length) = response
        .headers
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        && content_length > buffered_cap
    {
        // 声明长度超过上限：不读取直接拒绝。
        finalize_non_stream_attempt(
            env,
            StatusCode::BAD_GATEWAY,
            AttemptOutcome::GatewayError,
            Some("upstream_response_too_large".into()),
            Usage::default(),
            0,
            true,
        );
        return Err(NonStreamResult::Respond(gateway_error(
            env.entry_protocol,
            StatusCode::BAD_GATEWAY,
            "upstream_response_too_large",
            "Upstream response exceeds the configured buffered-body limit.",
            env.request_id,
        )));
    }
    let (raw_result, truncated) = read_bounded_body(
        response.body.into_stream(),
        Duration::from_secs(env.runtime.non_stream_total_timeout_seconds.max(1) as u64),
        buffered_cap,
    )
    .await;
    let raw = match raw_result {
        Ok(raw) if !truncated => raw,
        Ok(_) => {
            // 正文中途撞到上限：停止读取，绝不转换半截 JSON 正文。
            finalize_non_stream_attempt(
                env,
                StatusCode::BAD_GATEWAY,
                AttemptOutcome::GatewayError,
                Some("upstream_response_too_large".into()),
                Usage::default(),
                buffered_cap as i64,
                true,
            );
            return Err(NonStreamResult::Respond(gateway_error(
                env.entry_protocol,
                StatusCode::BAD_GATEWAY,
                "upstream_response_too_large",
                "Upstream response exceeds the configured buffered-body limit.",
                env.request_id,
            )));
        }
        Err(kind) => {
            // 正文中途传输失败：重置会自带一次尝试事件（尾部映射为 502）；
            // 总超时归类为 BodyTimeout（504）。
            let (failure, error_kind) = match kind {
                TransportFailure::ConnectionReset => {
                    (TransportFailure::ConnectionReset, "transport_error")
                }
                _ => (TransportFailure::BodyTimeout, "timeout"),
            };
            finalize_non_stream_attempt(
                env,
                StatusCode::BAD_GATEWAY,
                AttemptOutcome::TransportError,
                Some(error_kind.into()),
                Usage::default(),
                0,
                true,
            );
            return Err(NonStreamResult::FailOver(Some(failure)));
        }
    };
    // 映射转换必须有明文：解码失败是终态——给出稳定的网关错误，
    // 绝不把截断的正文交给转换器。
    // 非映射响应只需要尽力而为的可观测性，因此其解码是软失败。
    let decoded = if env.mapping.is_some() {
        match decode_required_buffered(response_headers.get("content-encoding"), &raw, buffered_cap)
        {
            Ok(decoded) => decoded,
            Err(error) => {
                // 截断或解码失败：明文已不可信，绝不把半截正文交给转换器。
                tracing::warn!(
                    request_id = %env.request_id,
                    channel_id = %env.candidate.channel_id,
                    error = ?error,
                    "mapped response decode failed"
                );
                decode_failure(env, env.attempts < env.candidates_len, raw.len() as i64);
                return Err(NonStreamResult::Respond(gateway_error(
                    env.entry_protocol,
                    StatusCode::BAD_GATEWAY,
                    "upstream_decode_error",
                    "Upstream response could not be decoded.",
                    env.request_id,
                )));
            }
        }
    } else {
        let mut decoder = compression::ObservableDecoder::new(
            compression::ContentDecoder::from_encoding(response_headers.get("content-encoding")),
        );
        decoder.feed_observable(&raw)
    };
    // Command Code（generate 传输）即使对非流式客户端也返回 NDJSON；
    // 先把它聚合成一条规范 OpenAI completion。
    // 携带错误事件的 200 正文是上游失败，绝不是伪造的成功。
    let decoded = if env.upstream_protocol == "command_code"
        && env.command_code_transport == Some(crate::commandcode::Transport::Generate)
    {
        match convert::ndjson_to_chat_completion(env.entry_model, &decoded) {
            Ok(value) => serde_json::to_vec(&value).unwrap_or_else(|_| decoded.clone()),
            Err(message) => {
                tracing::warn!(
                    request_id = %env.request_id,
                    channel_id = %env.candidate.channel_id,
                    message,
                    "command code non-stream body carried an upstream error"
                );
                finalize_non_stream_attempt(
                    env,
                    StatusCode::BAD_GATEWAY,
                    AttemptOutcome::UpstreamError,
                    Some("upstream_error".into()),
                    Usage::default(),
                    raw.len() as i64,
                    true,
                );
                return Err(NonStreamResult::FailOver(None));
            }
        }
    } else {
        decoded
    };
    Ok((raw, decoded))
}

/// 非流式响应组装的结果。
struct NonStreamResponse {
    response: Response<Body>,
    usage: Usage,
    conversion_failed: bool,
    /// 遥测口径与既有实现一致：转发给客户端的正文长度。
    response_bytes: i64,
}

/// 入口协议转换与响应组装。
///
/// 客户端期望的入口协议：映射入口总是转换；非映射的 Command Code 尝试会把
/// 解码出的 OpenAI 正文转回入口协议——与流式路径的
/// `MappedStreamConverter::new(entry, "openai_compatible", …)` 同一对协议。
/// `entry == openai_compatible` 的情形本身就是解码后的正文。
fn respond_non_stream(
    env: &AttemptEnv<'_>,
    status: axum::http::StatusCode,
    response_headers: axum::http::HeaderMap,
    decoded: &[u8],
    raw: &[u8],
    body_protocol: &str,
    usage: Usage,
) -> NonStreamResponse {
    let target_entry = match env.mapping {
        Some(value) => Some(value.entry.as_str()),
        None if env.upstream_protocol == "command_code"
            && env.entry_protocol != "openai_compatible" =>
        {
            Some(env.entry_protocol)
        }
        None => None,
    };
    let (result_body, conversion_failed, mapped) = if let Some(entry) = target_entry {
        match convert::convert_response(entry, body_protocol, env.entry_model, decoded) {
            Ok(converted) => (converted, false, true),
            Err(error) => {
                // 转换失败会在入口格式下产生一个网关错误正文（历史上由转换层
                // 直接给出错误正文）；HTTP 状态保持 200。
                // 消息是固定字符串——内部转换文本不得泄漏给客户端。
                tracing::error!(request_id = %env.request_id, error = %error, "response conversion failed");
                let payload = if env.entry_protocol == "claude" {
                    json!({"type": "error", "error": {"type": "gateway_error", "message": "Response conversion failed"}})
                } else {
                    json!({"error": {"message": "Response conversion failed", "type": "gateway_error", "code": "conversion_error"}})
                };
                (
                    serde_json::to_vec(&payload).unwrap_or_else(|_| raw.to_vec()),
                    true,
                    true,
                )
            }
        }
    } else if env.upstream_protocol == "command_code" {
        // 非映射的 Command Code：上游字节是 NDJSON（generate）或 Provider-API JSON，
        // 而 `decoded` 是客户端协议期望的规范 OpenAI 正文。
        // 标记为已转换会丢掉原来的 content-length/content-encoding 头。
        (decoded.to_vec(), false, true)
    } else {
        // 非映射：用原始头转发原始字节（含 content-length——正文是完整的）。
        (raw.to_vec(), false, false)
    };
    let result = if mapped {
        // 转换后的正文是明文，上游的长度与编码头不再描述它。
        transformed_body_response(
            status,
            response_headers,
            "application/json",
            Body::from(result_body.clone()),
        )
    } else {
        response_with_headers(status, response_headers, result_body.clone())
    };
    NonStreamResponse {
        response_bytes: result_body.len() as i64,
        response: result,
        usage,
        conversion_failed,
    }
}

/// 非流式尝试的终态遥测：成功、转换失败与全部提前失败出口共用。
fn finalize_non_stream_attempt(
    env: &AttemptEnv<'_>,
    status: axum::http::StatusCode,
    outcome: AttemptOutcome,
    error_kind: Option<String>,
    usage: Usage,
    response_bytes: i64,
    countable: bool,
) {
    let finished_at = env.clock.now_utc();
    attempt_finalizer(env).finalize(
        env.candidate,
        env.attempts,
        env.attempt_started,
        finished_at,
        outcome,
        Some(status.as_u16() as i64),
        error_kind,
        false,
        true,
        None,
        None,
        usage,
        response_bytes,
        Some(env.upstream_protocol.into()),
        Some(env.upstream_model.into()),
        countable,
    );
}

/// 映射压缩响应的共享无损解码。失败时终结该次尝试，并返回携带真实失败原因的
/// [`CompactionResult`]——没有可回放的上游错误体时，最终响应仍要说明是解码
/// 失败，而不是笼统的 502。
pub(super) fn decode_mapped_response(
    env: &AttemptEnv<'_>,
    content_encoding: Option<&axum::http::HeaderValue>,
    raw: &[u8],
    buffered_cap: usize,
) -> Result<Vec<u8>, CompactionResult> {
    decode_required_buffered(content_encoding, raw, buffered_cap).map_err(|error| {
        tracing::warn!(
            request_id = %env.request_id,
            channel_id = %env.candidate.channel_id,
            error = ?error,
            "mapped compaction response decode failed"
        );
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
            env.attempts < env.candidates_len,
            false,
            None,
            None,
            Usage::default(),
            raw.len() as i64,
            Some(env.upstream_protocol.into()),
            Some(env.upstream_model.into()),
            false,
        );
        CompactionResult::FailOverGateway {
            status: StatusCode::BAD_GATEWAY,
            code: "upstream_decode_error",
            message: "Upstream response could not be decoded.",
            channel_id: env.candidate.channel_id.clone(),
        }
    })
}
