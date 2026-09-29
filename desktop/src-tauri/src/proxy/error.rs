//! 网关错误形状与最终失败响应：JSON 响应组装、状态分类，以及全部候选
//! 失败后的回放。

use axum::{
    body::Body,
    http::{HeaderValue, Response, StatusCode},
    response::IntoResponse,
};
use serde_json::{Value, json};

use crate::{
    convert,
    domain::{
        Event, MappingTarget,
        TransportFailure,
    },
    protocol,
    telemetry::Telemetry,
};

// 同目录兄弟模块的内部项（`pub(super)` / `pub(crate)`）。
use super::nonstream::*;

pub(super) fn json_response(status: StatusCode, value: Value) -> Response<Body> {
    let mut response = (status, axum::Json(value)).into_response();
    response
        .headers_mut()
        .insert("cache-control", HeaderValue::from_static("no-store"));
    response
}

pub(super) fn gateway_error(
    entry: &str,
    status: StatusCode,
    code: &str,
    message: &str,
    request_id: &str,
) -> Response<Body> {
    // 对外错误结构由协议适配器给出。
    let body = protocol::ProtocolId::parse(entry)
        .map(|id| id.adapter().error_shape(status, code, message, request_id))
        .unwrap_or_else(|| {
            json!({"error": {"message": message, "type": code, "code": code, "request_id": request_id}})
        });
    json_response(status, body)
}

pub(super) fn status_kind(status: StatusCode) -> (&'static str, bool) {
    if status == StatusCode::TOO_MANY_REQUESTS {
        ("rate_limit", true)
    } else if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        // 鉴权失败计入熔断：轮换/过期的 key 是渠道的持续性问题，
        // 而不是客户端错误（对应 401/403）。
        ("auth_error", true)
    } else if status == StatusCode::REQUEST_TIMEOUT || status == StatusCode::GATEWAY_TIMEOUT {
        ("timeout", true)
    } else if status.is_server_error() {
        ("upstream_5xx", true)
    } else if status.is_client_error() {
        ("upstream_4xx", false)
    } else {
        ("upstream_error", false)
    }
}

/// 所有候选都失败后的最终网关响应：回放最后一个上游错误，
/// 把纯传输失败分类为 502/504，
/// 并写入请求终态遥测。
#[allow(clippy::too_many_arguments)]
pub(super) fn final_gateway_response(
    telemetry: &Telemetry,
    clock: &dyn crate::ports::Clock,
    entry_protocol: &str,
    request_id: &str,
    started: chrono::DateTime<chrono::Utc>,
    attempts: i64,
    mapping: Option<&MappingTarget>,
    last_error: Option<(StatusCode, axum::http::HeaderMap, Vec<u8>, String)>,
    last_gateway_error: Option<(StatusCode, &'static str, &'static str, String)>,
    last_transport_kind: Option<TransportFailure>,
) -> Response<Body> {
    if let Some((status, headers, raw, channel_id)) = last_error {
        let result_body = if let Some(value) = mapping.as_ref() {
            convert::convert_error(&value.entry, &value.upstream_protocol, &raw)
        } else {
            raw
        };
        telemetry.emit(Event::RequestFinish {
            id: request_id.to_owned(),
            finished_at: clock.now_utc().to_rfc3339(),
            duration_ms: clock
                .now_utc()
                .signed_duration_since(started)
                .num_milliseconds(),
            status: Some(status.as_u16() as i64),
            outcome: "upstream_error".into(),
            attempts,
            channel_id: Some(channel_id),
            response_bytes: result_body.len() as i64,
        });
        return response_with_headers(status, headers, result_body);
    }
    // 优先级：上游原文回放 > 网关侧失败原因 > 504 超时 > 通用 502。
    if let Some((status, code, message, channel_id)) = last_gateway_error {
        telemetry.emit(Event::RequestFinish {
            id: request_id.to_owned(),
            finished_at: clock.now_utc().to_rfc3339(),
            duration_ms: clock
                .now_utc()
                .signed_duration_since(started)
                .num_milliseconds(),
            status: Some(status.as_u16() as i64),
            outcome: "gateway_error".into(),
            attempts,
            channel_id: Some(channel_id),
            response_bytes: 0,
        });
        return gateway_error(entry_protocol, status, code, message, request_id);
    }
    if last_transport_kind.is_some_and(|kind| kind.is_timeout()) {
        telemetry.emit(Event::RequestFinish {
            id: request_id.to_owned(),
            finished_at: clock.now_utc().to_rfc3339(),
            duration_ms: clock
                .now_utc()
                .signed_duration_since(started)
                .num_milliseconds(),
            status: Some(504),
            outcome: "gateway_error".into(),
            attempts,
            channel_id: None,
            response_bytes: 0,
        });
        return gateway_error(
            entry_protocol,
            StatusCode::GATEWAY_TIMEOUT,
            "upstream_timeout",
            "Upstream did not respond within the configured timeout.",
            request_id,
        );
    }
    telemetry.emit(Event::RequestFinish {
        id: request_id.to_owned(),
        finished_at: clock.now_utc().to_rfc3339(),
        duration_ms: clock
            .now_utc()
            .signed_duration_since(started)
            .num_milliseconds(),
        status: Some(502),
        outcome: "gateway_error".into(),
        attempts,
        channel_id: None,
        response_bytes: 0,
    });
    gateway_error(
        entry_protocol,
        StatusCode::BAD_GATEWAY,
        "upstream_unreachable",
        "All eligible upstream channels failed before a response was available.",
        request_id,
    )
}
