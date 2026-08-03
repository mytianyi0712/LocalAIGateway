use std::{convert::Infallible, time::Duration};

use anyhow::Result;
use async_stream::stream;
use axum::{
    body::{Body, to_bytes},
    extract::{Path, Request, State},
    http::{HeaderValue, Response, StatusCode},
    response::IntoResponse,
};
use bytes::Bytes;
use futures_util::StreamExt;
use serde_json::{Value, json};
use sqlx::Row;

use crate::{
    convert, protocol,
    routing::{self, Candidate},
    server::AppState,
    settings,
    telemetry::{Event, Usage},
};

const MAX_REQUEST_BODY: usize = 8 * 1024 * 1024;

#[derive(Clone)]
struct MappingTarget {
    entry: String,
    upstream_protocol: String,
    upstream_model: String,
}

fn json_response(status: StatusCode, value: Value) -> Response<Body> {
    let mut response = (status, axum::Json(value)).into_response();
    response
        .headers_mut()
        .insert("cache-control", HeaderValue::from_static("no-store"));
    response
}

fn gateway_error(
    entry: &str,
    status: StatusCode,
    code: &str,
    message: &str,
    request_id: &str,
) -> Response<Body> {
    if entry == "claude" {
        json_response(
            status,
            json!({"type":"error","error":{"type":code,"message":message},"request_id":request_id}),
        )
    } else if entry == "gemini" {
        json_response(
            status,
            json!({"error":{"code":status.as_u16(),"message":message,"status":code},"request_id":request_id}),
        )
    } else {
        json_response(
            status,
            json!({"error":{"message":message,"type":code,"code":code,"request_id":request_id}}),
        )
    }
}

fn status_kind(status: StatusCode) -> (&'static str, bool) {
    if status == StatusCode::TOO_MANY_REQUESTS {
        ("rate_limit", true)
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

fn response_with_headers(
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

fn mapped_path(
    mapping: &MappingTarget,
    request_path: &str,
    stream_requested: bool,
    model: &str,
) -> String {
    if mapping.entry == "claude" && request_path.starts_with("/claudecode/") {
        return match mapping.upstream_protocol.as_str() {
            "openai_compatible" => "/v1/chat/completions".into(),
            "openai_responses" => "/v1/responses".into(),
            "claude" => "/v1/messages".into(),
            "gemini" => format!(
                "/v1beta/models/{model}:{}",
                if stream_requested {
                    "streamGenerateContent"
                } else {
                    "generateContent"
                }
            ),
            _ => request_path.into(),
        };
    }
    if mapping.entry == "openai_responses" && request_path.starts_with("/codex/") {
        return match mapping.upstream_protocol.as_str() {
            "openai_compatible" => "/v1/chat/completions".into(),
            "openai_responses" => "/v1/responses".into(),
            "claude" => "/v1/messages".into(),
            "gemini" => format!(
                "/v1beta/models/{model}:{}",
                if stream_requested {
                    "streamGenerateContent"
                } else {
                    "generateContent"
                }
            ),
            _ => request_path.into(),
        };
    }
    request_path.into()
}

async fn resolve_mapping(
    state: &AppState,
    entry: &str,
    model: &str,
) -> Result<Option<MappingTarget>> {
    let (table, id_column) = match entry {
        "claude" => ("claude_model_mappings", "claude_model_id"),
        "openai_responses" => ("codex_model_mappings", "codex_model_id"),
        _ => return Ok(None),
    };
    let sql = format!(
        "SELECT upstream_protocol,upstream_model_id FROM {table} WHERE {id_column}=? AND enabled=1"
    );
    let row = sqlx::query(&sql)
        .bind(model)
        .fetch_optional(state.db.pool())
        .await?;
    Ok(row.map(|row| MappingTarget {
        entry: entry.to_owned(),
        upstream_protocol: row.get("upstream_protocol"),
        upstream_model: row.get("upstream_model_id"),
    }))
}

fn attempt_event(
    request_id: &str,
    candidate: &Candidate,
    attempt_no: i64,
    started_at: String,
    finished_at: String,
    status: Option<i64>,
    outcome: &str,
    error_kind: Option<String>,
    failover: bool,
    response_started: bool,
    response_bytes: i64,
    upstream_protocol: Option<String>,
    upstream_model_id: Option<String>,
) -> Event {
    Event::Attempt {
        id: uuid::Uuid::new_v4().to_string(),
        request_id: request_id.to_owned(),
        channel_id: candidate.channel_id.clone(),
        channel_name: candidate.channel_name.clone(),
        attempt_no,
        priority: candidate.priority,
        started_at,
        finished_at,
        status,
        outcome: outcome.to_owned(),
        error_kind,
        failover,
        response_started,
        first_byte_ms: None,
        duration_ms: 0,
        usage: Usage::default(),
        response_bytes,
        upstream_protocol,
        upstream_model_id,
    }
}

async fn read_body(
    request: Request,
) -> Result<(axum::http::HeaderMap, String, Option<String>, Vec<u8>)> {
    let headers = request.headers().clone();
    let path = request.uri().path().to_owned();
    let query = request.uri().query().map(str::to_owned);
    let body = to_bytes(request.into_body(), MAX_REQUEST_BODY).await?;
    Ok((headers, path, query, body.to_vec()))
}

async fn proxy(
    state: AppState,
    request: Request,
    entry_protocol: &str,
    fixed_path: Option<String>,
) -> Response<Body> {
    let request_id = uuid::Uuid::new_v4().to_string();
    let started = chrono::Utc::now();
    let started_at = started.to_rfc3339();
    let (headers, request_path, query_string, body) = match read_body(request).await {
        Ok(value) => value,
        Err(error) => {
            return gateway_error(
                entry_protocol,
                StatusCode::PAYLOAD_TOO_LARGE,
                "request_too_large",
                &error.to_string(),
                &request_id,
            );
        }
    };
    let path = fixed_path.unwrap_or(request_path);
    let query = query_string.as_deref();
    if !settings::authorize_gateway(&state, &headers, query, entry_protocol)
        .await
        .unwrap_or(false)
    {
        return gateway_error(
            entry_protocol,
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Gateway access denied.",
            &request_id,
        );
    }
    let (entry_model, stream_requested) =
        protocol::inspect_request(entry_protocol, &path, query, &body);
    let Some(entry_model) = entry_model else {
        return gateway_error(
            entry_protocol,
            StatusCode::BAD_REQUEST,
            "model_required",
            "Unable to determine model from request.",
            &request_id,
        );
    };
    let mapping = match resolve_mapping(&state, entry_protocol, &entry_model).await {
        Ok(value) => value,
        Err(error) => {
            return gateway_error(
                entry_protocol,
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                &error.to_string(),
                &request_id,
            );
        }
    };
    let upstream_protocol = mapping
        .as_ref()
        .map(|value| value.upstream_protocol.as_str())
        .unwrap_or(entry_protocol);
    let upstream_model = mapping
        .as_ref()
        .map(|value| value.upstream_model.as_str())
        .unwrap_or(entry_model.as_str());
    let converted_body = if let Some(value) = mapping.as_ref() {
        match convert::convert_request(
            &value.entry,
            &value.upstream_protocol,
            &value.upstream_model,
            &body,
        ) {
            Ok(body) => body,
            Err(error) => {
                return gateway_error(
                    entry_protocol,
                    StatusCode::BAD_REQUEST,
                    "invalid_request",
                    &error.to_string(),
                    &request_id,
                );
            }
        }
    } else {
        body.clone()
    };
    let (model_id, stream) =
        protocol::inspect_request(upstream_protocol, &path, query, &converted_body);
    let stream_requested = stream_requested || stream;
    let route_model = model_id.as_deref().unwrap_or(upstream_model);
    state.telemetry.emit(Event::RequestStart {
        id: request_id.clone(),
        protocol: entry_protocol.to_owned(),
        model_id: Some(entry_model.clone()),
        endpoint: path.clone(),
        stream: stream_requested,
        started_at: started_at.clone(),
        request_bytes: body.len() as i64,
    });
    let runtime = match settings::runtime_settings(&state).await {
        Ok(value) => value,
        Err(error) => {
            return gateway_error(
                entry_protocol,
                StatusCode::INTERNAL_SERVER_ERROR,
                "settings_error",
                &error.to_string(),
                &request_id,
            );
        }
    };
    let candidates = match routing::resolve_candidates(
        &state,
        upstream_protocol,
        route_model,
        runtime.max_failover_attempts,
    )
    .await
    {
        Ok(value) => value,
        Err(error) => {
            return gateway_error(
                entry_protocol,
                StatusCode::INTERNAL_SERVER_ERROR,
                "routing_error",
                &error.to_string(),
                &request_id,
            );
        }
    };
    if candidates.is_empty() {
        state.telemetry.emit(Event::RequestFinish {
            id: request_id.clone(),
            finished_at: chrono::Utc::now().to_rfc3339(),
            duration_ms: chrono::Utc::now()
                .signed_duration_since(started)
                .num_milliseconds(),
            status: Some(503),
            outcome: "gateway_error".into(),
            attempts: 0,
            channel_id: None,
            response_bytes: 0,
        });
        return gateway_error(
            entry_protocol,
            StatusCode::SERVICE_UNAVAILABLE,
            "no_active_channel",
            "No active channel is available for this model.",
            &request_id,
        );
    }
    let mut last_error: Option<(StatusCode, axum::http::HeaderMap, Vec<u8>, String)> = None;
    let mut attempts = 0i64;
    for (index, candidate) in candidates.iter().enumerate() {
        attempts = index as i64 + 1;
        let attempt_started = chrono::Utc::now();
        let api_key = match state.secrets.decrypt(&candidate.api_key_encrypted) {
            Ok(value) => value,
            Err(error) => {
                last_error = None;
                state.telemetry.emit(attempt_event(
                    &request_id,
                    candidate,
                    attempts,
                    attempt_started.to_rfc3339(),
                    chrono::Utc::now().to_rfc3339(),
                    None,
                    "transport_error",
                    Some("key_decrypt_error".into()),
                    attempts < candidates.len() as i64,
                    false,
                    0,
                    Some(upstream_protocol.into()),
                    Some(upstream_model.into()),
                ));
                let _ = error;
                continue;
            }
        };
        let target_path = mapping
            .as_ref()
            .map(|value| mapped_path(value, &path, stream_requested, &value.upstream_model))
            .unwrap_or_else(|| path.clone());
        let target_url = match protocol::upstream_url(
            &candidate.base_url,
            &target_path,
            query,
            upstream_protocol,
        ) {
            Ok(value) => value,
            Err(error) => {
                last_error = None;
                let _ = error;
                continue;
            }
        };
        let outbound = match protocol::outbound_headers(&headers, upstream_protocol, &api_key) {
            Ok(value) => value,
            Err(error) => {
                let _ = error;
                continue;
            }
        };
        let send = state
            .http
            .request(reqwest::Method::from_bytes(b"POST").unwrap(), target_url)
            .headers(outbound)
            .body(converted_body.clone())
            .send();
        let response = match tokio::time::timeout(
            Duration::from_secs(runtime.first_byte_timeout_seconds.max(1) as u64),
            send,
        )
        .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                let finished = chrono::Utc::now();
                state.telemetry.emit(Event::ChannelFailure {
                    channel_id: candidate.channel_id.clone(),
                    error_kind: "transport_error".into(),
                    status: None,
                    threshold: runtime.failure_threshold,
                    open_seconds: runtime.circuit_open_seconds,
                    countable: true,
                });
                state.telemetry.emit(attempt_event(
                    &request_id,
                    candidate,
                    attempts,
                    attempt_started.to_rfc3339(),
                    finished.to_rfc3339(),
                    None,
                    "transport_error",
                    Some(error.to_string()),
                    attempts < candidates.len() as i64,
                    false,
                    0,
                    Some(upstream_protocol.into()),
                    Some(upstream_model.into()),
                ));
                continue;
            }
            Err(_) => {
                let finished = chrono::Utc::now();
                state.telemetry.emit(Event::ChannelFailure {
                    channel_id: candidate.channel_id.clone(),
                    error_kind: "timeout".into(),
                    status: None,
                    threshold: runtime.failure_threshold,
                    open_seconds: runtime.circuit_open_seconds,
                    countable: true,
                });
                state.telemetry.emit(attempt_event(
                    &request_id,
                    candidate,
                    attempts,
                    attempt_started.to_rfc3339(),
                    finished.to_rfc3339(),
                    None,
                    "transport_error",
                    Some("timeout".into()),
                    attempts < candidates.len() as i64,
                    false,
                    0,
                    Some(upstream_protocol.into()),
                    Some(upstream_model.into()),
                ));
                continue;
            }
        };
        let status = response.status();
        let response_headers = protocol::response_headers(response.headers());
        if status.is_success() {
            if stream_requested && mapping.is_none() {
                let upstream_stream = response.bytes_stream();
                let response_headers_for_client = response_headers.clone();
                let telemetry = state.telemetry.clone();
                let request_id_stream = request_id.clone();
                let candidate_stream = candidate.clone();
                let upstream_protocol_stream = upstream_protocol.to_owned();
                let upstream_model_stream = upstream_model.to_owned();
                let stream_body = stream! {
                    let mut upstream_stream = upstream_stream;
                    let mut response_bytes = 0i64;
                    let mut ok = true;
                    while let Some(chunk) = upstream_stream.next().await {
                        match chunk { Ok(chunk) => { response_bytes += chunk.len() as i64; yield Ok::<Bytes, Infallible>(chunk); }, Err(_) => { ok = false; break; } }
                    }
                    let finished = chrono::Utc::now().to_rfc3339();
                    let outcome = if ok { "success" } else { "stream_interrupted" };
                    if ok { telemetry.emit(Event::ChannelSuccess { channel_id:candidate_stream.channel_id.clone() }); }
                    else { telemetry.emit(Event::ChannelFailure { channel_id:candidate_stream.channel_id.clone(), error_kind:"stream_interrupted".into(), status:Some(status.as_u16() as i64), threshold:3, open_seconds:900, countable:true }); }
                    telemetry.emit(attempt_event(&request_id_stream,&candidate_stream,attempts,attempt_started.to_rfc3339(),finished.clone(),Some(status.as_u16() as i64),outcome, if ok {None}else{Some("stream_interrupted".into())},false,true,response_bytes,Some(upstream_protocol_stream),Some(upstream_model_stream)));
                    telemetry.emit(Event::RequestFinish { id:request_id_stream, finished_at:finished, duration_ms:chrono::Utc::now().signed_duration_since(started).num_milliseconds(), status:Some(status.as_u16() as i64), outcome:outcome.into(), attempts, channel_id:Some(candidate_stream.channel_id), response_bytes });
                };
                let mut result = Response::new(Body::from_stream(stream_body));
                *result.status_mut() = status;
                *result.headers_mut() = response_headers_for_client;
                return result;
            }
            let read = tokio::time::timeout(
                Duration::from_secs(runtime.non_stream_total_timeout_seconds.max(1) as u64),
                response.bytes(),
            )
            .await;
            let raw = match read {
                Ok(Ok(value)) => value.to_vec(),
                _ => {
                    state.telemetry.emit(Event::ChannelFailure {
                        channel_id: candidate.channel_id.clone(),
                        error_kind: "timeout".into(),
                        status: None,
                        threshold: runtime.failure_threshold,
                        open_seconds: runtime.circuit_open_seconds,
                        countable: true,
                    });
                    continue;
                }
            };
            let result_body = if let Some(value) = mapping.as_ref() {
                if stream_requested {
                    convert::convert_stream(
                        &value.entry,
                        &value.upstream_protocol,
                        &entry_model,
                        &raw,
                    )
                    .unwrap_or(raw.clone())
                } else {
                    convert::convert_response(
                        &value.entry,
                        &value.upstream_protocol,
                        &entry_model,
                        &raw,
                    )
                    .unwrap_or(raw.clone())
                }
            } else {
                raw
            };
            let mut result = response_with_headers(status, response_headers, result_body.clone());
            if mapping.is_some() {
                result.headers_mut().remove("content-length");
                result.headers_mut().insert(
                    "content-type",
                    HeaderValue::from_static(if stream_requested {
                        "text/event-stream"
                    } else {
                        "application/json"
                    }),
                );
            }
            state.telemetry.emit(Event::ChannelSuccess {
                channel_id: candidate.channel_id.clone(),
            });
            state.telemetry.emit(attempt_event(
                &request_id,
                candidate,
                attempts,
                attempt_started.to_rfc3339(),
                chrono::Utc::now().to_rfc3339(),
                Some(status.as_u16() as i64),
                "success",
                None,
                false,
                false,
                result_body.len() as i64,
                Some(upstream_protocol.into()),
                Some(upstream_model.into()),
            ));
            state.telemetry.emit(Event::RequestFinish {
                id: request_id,
                finished_at: chrono::Utc::now().to_rfc3339(),
                duration_ms: chrono::Utc::now()
                    .signed_duration_since(started)
                    .num_milliseconds(),
                status: Some(status.as_u16() as i64),
                outcome: "success".into(),
                attempts,
                channel_id: Some(candidate.channel_id.clone()),
                response_bytes: result_body.len() as i64,
            });
            return result;
        }
        let raw = match tokio::time::timeout(
            Duration::from_secs(runtime.first_byte_timeout_seconds.max(1) as u64),
            response.bytes(),
        )
        .await
        {
            Ok(Ok(value)) => value.to_vec(),
            _ => Vec::new(),
        };
        let (kind, countable) = status_kind(status);
        state.telemetry.emit(Event::ChannelFailure {
            channel_id: candidate.channel_id.clone(),
            error_kind: kind.into(),
            status: Some(status.as_u16() as i64),
            threshold: runtime.failure_threshold,
            open_seconds: runtime.circuit_open_seconds,
            countable,
        });
        state.telemetry.emit(attempt_event(
            &request_id,
            candidate,
            attempts,
            attempt_started.to_rfc3339(),
            chrono::Utc::now().to_rfc3339(),
            Some(status.as_u16() as i64),
            "http_error",
            Some(kind.into()),
            attempts < candidates.len() as i64,
            false,
            raw.len() as i64,
            Some(upstream_protocol.into()),
            Some(upstream_model.into()),
        ));
        last_error = Some((status, response_headers, raw, candidate.channel_id.clone()));
    }
    if let Some((status, headers, raw, channel_id)) = last_error {
        let result_body = if let Some(value) = mapping.as_ref() {
            convert::convert_error(&value.entry, &raw)
        } else {
            raw
        };
        state.telemetry.emit(Event::RequestFinish {
            id: request_id,
            finished_at: chrono::Utc::now().to_rfc3339(),
            duration_ms: chrono::Utc::now()
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
    state.telemetry.emit(Event::RequestFinish {
        id: request_id.clone(),
        finished_at: chrono::Utc::now().to_rfc3339(),
        duration_ms: chrono::Utc::now()
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
        &request_id,
    )
}

async fn normal(
    State(state): State<AppState>,
    request: Request,
    protocol: &'static str,
) -> Response<Body> {
    proxy(state, request, protocol, None).await
}

pub async fn openai(State(state): State<AppState>, request: Request) -> Response<Body> {
    normal(State(state), request, "openai_compatible").await
}
pub async fn responses(State(state): State<AppState>, request: Request) -> Response<Body> {
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
pub async fn claudecode(State(state): State<AppState>, request: Request) -> Response<Body> {
    normal(State(state), request, "claude").await
}
pub async fn codex(State(state): State<AppState>, request: Request) -> Response<Body> {
    normal(State(state), request, "openai_responses").await
}

pub async fn models(
    State(state): State<AppState>,
    request: Request,
    protocol: &'static str,
) -> Response<Body> {
    if !settings::authorize_gateway(&state, request.headers(), request.uri().query(), "catalog")
        .await
        .unwrap_or(false)
    {
        return gateway_error(
            protocol,
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Gateway access denied.",
            "catalog",
        );
    }
    let items = match routing::list_routable_models(&state, Some(protocol)).await {
        Ok(value) => value,
        Err(error) => {
            return gateway_error(
                protocol,
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                &error.to_string(),
                "catalog",
            );
        }
    };
    if protocol == "gemini" {
        json_response(
            StatusCode::OK,
            json!({"models":items.iter().map(|item|json!({"name":format!("models/{}",item.id),"displayName":item.display_name,"supportedGenerationMethods":["generateContent"]})).collect::<Vec<_>>() }),
        )
    } else {
        json_response(
            StatusCode::OK,
            json!({"object":"list","data":items.iter().map(|item|json!({"id":item.id,"object":"model","owned_by":"local-gateway","created":item.created_at})).collect::<Vec<_>>() }),
        )
    }
}

pub async fn mapped_models(
    State(state): State<AppState>,
    request: Request,
    entry: &'static str,
) -> Response<Body> {
    if !settings::authorize_gateway(&state, request.headers(), request.uri().query(), entry)
        .await
        .unwrap_or(false)
    {
        return gateway_error(
            entry,
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Gateway access denied.",
            "catalog",
        );
    }
    let items = match routing::list_mapping_models(&state, entry).await {
        Ok(value) => value,
        Err(error) => {
            return gateway_error(
                entry,
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                &error.to_string(),
                "catalog",
            );
        }
    };
    json_response(
        StatusCode::OK,
        json!({"object":"list","data":items.iter().map(|item|json!({"id":item.id,"object":"model","owned_by":"local-gateway","created":item.created_at})).collect::<Vec<_>>() }),
    )
}

pub async fn claudecode_info(State(state): State<AppState>, request: Request) -> Response<Body> {
    if !settings::authorize_gateway(&state, request.headers(), request.uri().query(), "claude")
        .await
        .unwrap_or(false)
    {
        gateway_error(
            "claude",
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Gateway access denied.",
            "info",
        )
    } else {
        json_response(
            StatusCode::OK,
            json!({"protocol":"claude","endpoint":"/claudecode/v1/messages","models":"/claudecode/v1/models"}),
        )
    }
}
pub async fn codex_info(State(state): State<AppState>, request: Request) -> Response<Body> {
    if !settings::authorize_gateway(
        &state,
        request.headers(),
        request.uri().query(),
        "openai_responses",
    )
    .await
    .unwrap_or(false)
    {
        gateway_error(
            "openai_responses",
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Gateway access denied.",
            "info",
        )
    } else {
        json_response(
            StatusCode::OK,
            json!({"protocol":"openai_responses","endpoint":"/codex/v1/responses","models":"/codex/v1/models"}),
        )
    }
}

pub async fn openai_models(State(state): State<AppState>, request: Request) -> Response<Body> {
    models(State(state), request, "openai_compatible").await
}
pub async fn responses_models(State(state): State<AppState>, request: Request) -> Response<Body> {
    models(State(state), request, "openai_responses").await
}
pub async fn claude_models(State(state): State<AppState>, request: Request) -> Response<Body> {
    models(State(state), request, "claude").await
}
pub async fn gemini_models(State(state): State<AppState>, request: Request) -> Response<Body> {
    models(State(state), request, "gemini").await
}
pub async fn claudecode_models(State(state): State<AppState>, request: Request) -> Response<Body> {
    mapped_models(State(state), request, "claude").await
}
pub async fn codex_models(State(state): State<AppState>, request: Request) -> Response<Body> {
    mapped_models(State(state), request, "openai_responses").await
}
