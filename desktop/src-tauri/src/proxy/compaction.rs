//! 远程压缩（Codex remote compaction）的尝试处理：V1 / V2 成功路径、
//! 网关侧失败分类，以及 V2 流的读取骨架。

use std::time::Duration;

use axum::{
    body::Body,
    http::{Response, StatusCode},
};

use crate::{
    compression,
    domain::{
        CompactionMode,
        TransportFailure, Usage,
    },
    protocol,
    remote_compaction::{
        self, CompactionV2Validator, ValidatedCompactionStream,
        read_validated_compaction_stream,
    },
};

// 同目录兄弟模块的内部项（`pub(super)` / `pub(crate)`）。
use super::attempt::*;
use super::error::*;
use super::nonstream::*;


/// 压缩路径的网关侧失败：终结该次尝试，并带着真实原因回到主循环。
pub(super) fn compaction_gateway_failure(
    env: &AttemptEnv<'_>,
    code: &'static str,
    message: &'static str,
    response_bytes: i64,
) -> CompactionResult {
    let finished = env.clock.now_utc();
    attempt_finalizer(env)
    .finalize(
        env.candidate,
        env.attempts,
        env.attempt_started,
        finished,
        AttemptOutcome::GatewayError,
        Some(502),
        Some(code.into()),
        env.attempts < env.candidates_len,
        false,
        None,
        None,
        Usage::default(),
        response_bytes,
        Some(env.upstream_protocol.into()),
        Some(env.upstream_model.into()),
        false,
    );
    CompactionResult::FailOverGateway {
        status: StatusCode::BAD_GATEWAY,
        code,
        message,
        channel_id: env.candidate.channel_id.clone(),
    }
}

/// 压缩路径的传输失败：终结该次尝试并保留 502/504 的分类。
pub(super) fn compaction_transport_failure(env: &AttemptEnv<'_>, kind: TransportFailure) -> CompactionResult {
    let finished = env.clock.now_utc();
    attempt_finalizer(env)
    .finalize(
        env.candidate,
        env.attempts,
        env.attempt_started,
        finished,
        AttemptOutcome::TransportError,
        None,
        Some("transport_error".into()),
        env.attempts < env.candidates_len,
        false,
        None,
        None,
        Usage::default(),
        0,
        Some(env.upstream_protocol.into()),
        Some(env.upstream_model.into()),
        false,
    );
    CompactionResult::FailOver(Some(kind))
}


/// 一次远程压缩尝试的结果。
pub(super) enum CompactionResult {
    Respond(Response<Body>),
    FailOver(Option<TransportFailure>),
    /// 上游返回了 HTTP 错误体：若后续候选全部失败，就把这个错误体回放给客户端。
    FailOverError(StatusCode, axum::http::HeaderMap, Vec<u8>, String),
    /// 网关侧失败（解码/校验/超限）：没有可回放的上游错误体，但必须把真实原因
    /// 带到最终响应，而不是退化成笼统的 `upstream_unreachable`。
    FailOverGateway {
        status: StatusCode,
        code: &'static str,
        message: &'static str,
        channel_id: String,
    },
}

/// 处理一次远程压缩的上游响应。V1 与 V2 都会在把字节返回给客户端之前
/// 校验完整正文，
/// 这样损坏的上游仍能 failover 到下一个候选。
pub(super) async fn compaction_attempt(
    routes: &dyn crate::ports::RouteRepository,
    env: AttemptEnv<'_>,
    mode: CompactionMode,
    // 错误体（非 2xx）的读取上限：走 `error_body_max` 而不是缓冲上限。
    error_cap: usize,
    response: crate::ports::UpstreamResponse,
    status: axum::http::StatusCode,
    response_headers: axum::http::HeaderMap,
) -> CompactionResult {
    if !status.is_success() {
        return compaction_error_attempt(
            routes,
            env,
            mode,
            error_cap,
            response,
            status,
            response_headers,
        )
        .await;
    }
    match mode {
        CompactionMode::V1 => {
            compaction_v1_success(routes, env, response, status, response_headers).await
        }
        CompactionMode::V2 => {
            compaction_v2_success(routes, env, response, status, response_headers).await
        }
    }
}

pub(super) async fn compaction_error_attempt(
    routes: &dyn crate::ports::RouteRepository,
    env: AttemptEnv<'_>,
    mode: CompactionMode,
    error_cap: usize,
    response: crate::ports::UpstreamResponse,
    status: axum::http::StatusCode,
    response_headers: axum::http::HeaderMap,
) -> CompactionResult {
    let (raw_result, truncated) = read_bounded_body(
        response.body.into_stream(),
        Duration::from_secs(env.runtime.first_byte_timeout_seconds.max(1) as u64),
        error_cap,
    )
    .await;
    let raw = match raw_result {
        Ok(raw) => raw,
        Err(TransportFailure::ConnectionReset) => Vec::new(),
        Err(_) => Vec::new(),
    };
    if truncated {
        tracing::warn!(
            request_id = %env.request_id,
            channel_id = %env.candidate.channel_id,
            "remote compaction error body truncated"
        );
    }

    let capability_rejected = matches!(status.as_u16(), 404 | 405 | 501);
    if capability_rejected {
        let _ = routes
            .set_compaction_support(&env.candidate.channel_id, mode, false)
            .await;
    }

    // 远程压缩尝试从不参与渠道熔断：
    // 网关可能有意依次尝试不受支持的压缩模式，
    // 而不因此惩罚该 provider 的普通流量。
    let kind = if capability_rejected {
        "remote_compaction_unsupported"
    } else {
        status_kind(status).0
    };
    let finished = env.clock.now_utc();
    attempt_finalizer(&env)
    .finalize(
        env.candidate,
        env.attempts,
        env.attempt_started,
        finished,
        AttemptOutcome::UpstreamError,
        Some(status.as_u16() as i64),
        Some(kind.into()),
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
    CompactionResult::FailOverError(
        status,
        response_headers,
        raw,
        env.candidate.channel_id.clone(),
    )
}

pub(super) async fn compaction_v1_success(
    routes: &dyn crate::ports::RouteRepository,
    env: AttemptEnv<'_>,
    response: crate::ports::UpstreamResponse,
    status: axum::http::StatusCode,
    response_headers: axum::http::HeaderMap,
) -> CompactionResult {
    let buffered_cap = (env.runtime.max_buffered_upstream_body_mb.max(1) as usize) * 1024 * 1024;
    let (raw_result, truncated) = read_bounded_body(
        response.body.into_stream(),
        Duration::from_secs(env.runtime.non_stream_total_timeout_seconds.max(1) as u64),
        buffered_cap,
    )
    .await;
    let raw = match raw_result {
        Ok(raw) if !truncated => raw,
        Ok(_) => {
            let finished = env.clock.now_utc();
            attempt_finalizer(&env)
            .finalize(
                env.candidate,
                env.attempts,
                env.attempt_started,
                finished,
                AttemptOutcome::GatewayError,
                Some(502),
                Some("upstream_response_too_large".into()),
                env.attempts < env.candidates_len,
                false,
                None,
                None,
                Usage::default(),
                buffered_cap as i64,
                Some(env.upstream_protocol.into()),
                Some(env.upstream_model.into()),
                false,
            );
            return CompactionResult::FailOverGateway {
                status: StatusCode::BAD_GATEWAY,
                code: "upstream_response_too_large",
                message: "Upstream response exceeds the configured buffered-body limit.",
                channel_id: env.candidate.channel_id.clone(),
            };
        }
        Err(kind) => {
            let finished = env.clock.now_utc();
            attempt_finalizer(&env)
            .finalize(
                env.candidate,
                env.attempts,
                env.attempt_started,
                finished,
                AttemptOutcome::TransportError,
                None,
                Some("transport_error".into()),
                env.attempts < env.candidates_len,
                false,
                None,
                None,
                Usage::default(),
                0,
                Some(env.upstream_protocol.into()),
                Some(env.upstream_model.into()),
                false,
            );
            return CompactionResult::FailOver(Some(kind));
        }
    };

    let decoded = match decode_mapped_response(&env, response_headers.get("content-encoding"), &raw, buffered_cap) {
        Ok(decoded) => decoded,
        Err(result) => return result,
    };
    if !remote_compaction::validate_v1_response(&decoded) {
        let finished = env.clock.now_utc();
        attempt_finalizer(&env)
        .finalize(
            env.candidate,
            env.attempts,
            env.attempt_started,
            finished,
            AttemptOutcome::GatewayError,
            Some(502),
            Some("upstream_compaction_format_error".into()),
            env.attempts < env.candidates_len,
            false,
            None,
            None,
            Usage::default(),
            decoded.len() as i64,
            Some(env.upstream_protocol.into()),
            Some(env.upstream_model.into()),
            false,
        );
        return CompactionResult::FailOverGateway {
            status: StatusCode::BAD_GATEWAY,
            code: "upstream_compaction_format_error",
            message: "Upstream did not return a valid remote-compaction response.",
            channel_id: env.candidate.channel_id.clone(),
        };
    }

    let _ = routes
        .set_compaction_support(&env.candidate.channel_id, CompactionMode::V1, true)
        .await;

    let finished = env.clock.now_utc();
    attempt_finalizer(&env)
    .finalize(
        env.candidate,
        env.attempts,
        env.attempt_started,
        finished,
        AttemptOutcome::Success,
        Some(status.as_u16() as i64),
        None,
        false,
        true,
        None,
        None,
        Usage::default(),
        decoded.len() as i64,
        Some(env.upstream_protocol.into()),
        Some(env.upstream_model.into()),
        false,
    );

    CompactionResult::Respond(transformed_body_response(
        status,
        response_headers,
        "application/json",
        Body::from(decoded),
    ))
}

pub(super) async fn compaction_v2_success(
    routes: &dyn crate::ports::RouteRepository,
    env: AttemptEnv<'_>,
    response: crate::ports::UpstreamResponse,
    status: axum::http::StatusCode,
    response_headers: axum::http::HeaderMap,
) -> CompactionResult {
    let buffered_cap = (env.runtime.max_buffered_upstream_body_mb.max(1) as usize) * 1024 * 1024;
    // 边读边判读，校验器一确认完整就停止读取——上游发完 `response.completed`
    // 后保持连接时，等连接 EOF 只会等到超时。
    let mut validator = CompactionV2Validator::new();
    let mut decoder = compression::RequiredDecoder::new(
        compression::ContentDecoder::from_encoding(response_headers.get("content-encoding")),
        buffered_cap,
    );
    let decoded = match read_validated_compaction_stream(
        response.body.into_stream(),
        Duration::from_secs(env.runtime.non_stream_total_timeout_seconds.max(1) as u64),
        buffered_cap,
        &mut validator,
        Some(&mut decoder),
    )
    .await
    {
        ValidatedCompactionStream::Read { payload, truncated } if !truncated => payload,
        ValidatedCompactionStream::Read { .. }
        | ValidatedCompactionStream::Decode {
            error: compression::DecodeError::CumulativeLimit,
            ..
        } => {
            // 明文超过缓冲上限：与旧实现的“原始字节超限”对外表现一致。
            return compaction_gateway_failure(
                &env,
                "upstream_response_too_large",
                "Upstream response exceeds the configured buffered-body limit.",
                buffered_cap as i64,
            );
        }
        ValidatedCompactionStream::Decode { error, read } => {
            tracing::warn!(
                request_id = %env.request_id,
                channel_id = %env.candidate.channel_id,
                error = ?error,
                "mapped compaction response decode failed"
            );
            return compaction_gateway_failure(
                &env,
                "upstream_decode_error",
                "Upstream response could not be decoded.",
                read as i64,
            );
        }
        ValidatedCompactionStream::Transport { kind } => {
            return compaction_transport_failure(&env, kind);
        }
    };

    let validation = validator.finish();

    // 第六项是失败时的对外错误码与文案：网关侧失败也要把真实原因说出来。
    let (outcome, error_kind, status_code, countable, usage, gateway_failure) = match validation {
        Ok(()) => {
            let _ = routes
                .set_compaction_support(&env.candidate.channel_id, CompactionMode::V2, true)
                .await;
            let usage = validator
                .usage()
                .map(|value| protocol::normalize_usage(env.upstream_protocol, value))
                .unwrap_or_default();
            (
                AttemptOutcome::Success,
                None,
                status.as_u16() as i64,
                false,
                usage,
                None,
            )
        }
        Err(remote_compaction::CompactionV2Error::Upstream(message)) => {
            tracing::warn!(
                request_id = %env.request_id,
                channel_id = %env.candidate.channel_id,
                message,
                "remote compaction v2 upstream stream failed"
            );
            (
                AttemptOutcome::UpstreamError,
                Some("upstream_error".into()),
                502,
                false,
                Usage::default(),
                Some(("upstream_error", "Upstream remote compaction stream failed.")),
            )
        }
        Err(remote_compaction::CompactionV2Error::NotCompaction) => {
            let _ = routes
                .set_compaction_support(&env.candidate.channel_id, CompactionMode::V2, false)
                .await;
            (
                AttemptOutcome::UpstreamError,
                Some("remote_compaction_unsupported".into()),
                502,
                false,
                Usage::default(),
                Some((
                    "remote_compaction_unsupported",
                    "Remote compaction is only supported for openai_responses upstream channels.",
                )),
            )
        }
        Err(error) => {
            tracing::warn!(
                request_id = %env.request_id,
                channel_id = %env.candidate.channel_id,
                %error,
                "remote compaction v2 response invalid"
            );
            (
                AttemptOutcome::GatewayError,
                Some("upstream_compaction_format_error".into()),
                502,
                false,
                Usage::default(),
                Some((
                    "upstream_compaction_format_error",
                    "Upstream did not return a valid remote-compaction response.",
                )),
            )
        }
    };

    let finished = env.clock.now_utc();
    attempt_finalizer(&env)
    .finalize(
        env.candidate,
        env.attempts,
        env.attempt_started,
        finished,
        outcome,
        Some(status_code),
        error_kind,
        env.attempts < env.candidates_len,
        outcome == AttemptOutcome::Success,
        None,
        None,
        usage,
        decoded.len() as i64,
        Some(env.upstream_protocol.into()),
        Some(env.upstream_model.into()),
        countable,
    );

    if outcome != AttemptOutcome::Success {
        let (code, message) = gateway_failure.unwrap_or((
            "upstream_unreachable",
            "All eligible upstream channels failed before a response was available.",
        ));
        return CompactionResult::FailOverGateway {
            status: StatusCode::BAD_GATEWAY,
            code,
            message,
            channel_id: env.candidate.channel_id.clone(),
        };
    }

    CompactionResult::Respond(transformed_body_response(
        status,
        response_headers,
        "text/event-stream",
        Body::from(decoded),
    ))
}
