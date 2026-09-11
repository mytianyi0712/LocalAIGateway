use std::{
    convert::Infallible,
    pin::Pin,
    sync::Arc,
    sync::atomic::{AtomicBool, AtomicI64, Ordering},
    task::{Context, Poll},
    time::Duration,
};

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

use crate::{
    application::Context as AppContext,
    capabilities, compression, convert,
    crypto::SecretStore,
    db::Database,
    protocol,
    remote_compaction::{self, CompactionMode, CompactionSupport, CompactionV2Validator},
    routing::{self, Candidate, RoutableModel},
    runtime::RuntimeLimits,
    settings,
    telemetry::{AttemptData, Event, Telemetry, Usage},
};

/// Wraps an upstream byte stream so a client-side disconnect (which drops the
/// response body without polling it to completion) still records a cancelled
/// attempt instead of leaving the request permanently pending.
///
/// `finalized` marks a stream whose generator ran to its terminal telemetry
/// (success, upstream error, or idle/first-token timeout) without being
/// dropped early: such a stream already recorded its real outcome, so the
/// `Drop` path must not append a duplicate `cancelled` finish.
struct CancelAware<S> {
    inner: S,
    completed: Arc<AtomicBool>,
    finalized: Arc<AtomicBool>,
    responded: Arc<AtomicBool>,
    on_cancel: Option<Box<dyn FnOnce() + Send>>,
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

/// Live stream statistics shared between a stream generator and its
/// `CancelAware` Drop path. When a client disconnects mid-stream the
/// generator's locals are gone, so the cancelled finish must read what was
/// actually observed — bytes that flowed and usage that was parsed must not
/// be replaced by a zeroed record.
#[derive(Default)]
struct SharedStreamStats {
    bytes: AtomicI64,
    usage: parking_lot::Mutex<Usage>,
    first_byte_ms: parking_lot::Mutex<Option<i64>>,
    first_token_ms: parking_lot::Mutex<Option<i64>>,
}

/// Usage snapshot from raw parts with the protocol's cache-miss derivation
/// (mirror of the protocol adapters and of `converter_usage`).
fn usage_from_parts(
    stream_protocol: &str,
    input: Option<i64>,
    output: Option<i64>,
    cache_read: Option<i64>,
    cache_write: Option<i64>,
) -> Usage {
    let cache_miss = match stream_protocol {
        "claude" => input,
        "gemini" => input.map(|total| (total - cache_read.unwrap_or(0)).max(0)),
        _ => match (input, cache_read) {
            (Some(total), Some(read)) => {
                Some((total - read - cache_write.unwrap_or(0)).max(0))
            }
            _ => None,
        },
    };
    Usage {
        input_tokens: input,
        cache_read_tokens: cache_read,
        cache_write_tokens: cache_write,
        cache_miss_input_tokens: cache_miss,
        output_tokens: output,
        raw: None,
    }
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
    // P2-3: the public error shape comes from the protocol adapter.
    let body = protocol::ProtocolId::parse(entry)
        .map(|id| id.adapter().error_shape(status, code, message, request_id))
        .unwrap_or_else(|| {
            json!({"error": {"message": message, "type": code, "code": code, "request_id": request_id}})
        });
    json_response(status, body)
}

fn status_kind(status: StatusCode) -> (&'static str, bool) {
    if status == StatusCode::TOO_MANY_REQUESTS {
        ("rate_limit", true)
    } else if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        // Auth failures count toward the circuit: a rotated/expired key is a
        // persistent channel problem, not a client mistake (requirements 401/403).
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

/// Kind of the last pure-transport failure (no upstream status was ever
/// received), so the final gateway error can distinguish 504 timeouts from
/// 502 unreachability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransportFailure {
    /// reqwest `send` failed with `error.is_timeout()` (connect timeout).
    ConnectTimeout,
    /// The send future itself hit the `first_byte_timeout` tokio timeout,
    /// or an error-response body read hit the same outer timeout.
    FirstByteTimeout,
    /// Mapped-stream prelude produced no first token within the attempt's
    /// absolute `first_token_timeout` window.
    FirstTokenTimeout,
    /// A non-streaming successful response body read hit its total timeout.
    BodyTimeout,
    /// Any other transport failure (reset, local key/URL/header construction
    /// failure) — the tail maps this to 502.
    ConnectionReset,
}

impl TransportFailure {
    fn is_timeout(self) -> bool {
        matches!(
            self,
            Self::ConnectTimeout
                | Self::FirstByteTimeout
                | Self::FirstTokenTimeout
                | Self::BodyTimeout
        )
    }

    /// Short stable label for telemetry and user-facing failover alerts.
    fn as_str(self) -> &'static str {
        match self {
            Self::ConnectTimeout => "connect_timeout",
            Self::FirstByteTimeout => "first_byte_timeout",
            Self::FirstTokenTimeout => "first_token_timeout",
            Self::BodyTimeout => "body_timeout",
            Self::ConnectionReset => "connection_reset",
        }
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
    mapping: &routing::MappingTarget,
    request_path: &str,
    stream_requested: bool,
    model: &str,
    compaction: Option<CompactionMode>,
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
        if compaction == Some(CompactionMode::V1) && mapping.upstream_protocol == "openai_responses"
        {
            return "/v1/responses/compact".into();
        }
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

// Telemetry boundary assembler: fields come from disjoint call-site contexts
// (candidate, attempt bookkeeping, timings, usage), so grouping them would
// merely move the verbosity to eight call sites. Allow the argument count.
#[allow(clippy::too_many_arguments)]
fn attempt_event(
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

/// Terminal outcome of one candidate attempt. Drives all three telemetry
/// events (channel, attempt, request) so they can never disagree (P1-5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttemptOutcome {
    Success,
    /// The gateway produced a protocol error itself (conversion failure,
    /// upstream body over the buffer cap, undecodable body). The channel
    /// is not necessarily at fault — `countable` is decided per site.
    GatewayError,
    /// Upstream answered with an error status or an error body.
    UpstreamError,
    /// Transport failed before any status was usable (connect/reset/timeout).
    TransportError,
    Cancelled,
    /// The stream broke mid-body (idle timeout / upstream reset).
    StreamInterrupted,
}

impl AttemptOutcome {
    fn as_str(self) -> &'static str {
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

/// Emits the channel/attempt/request telemetry for one terminal attempt.
/// Every finalization site must go through here so the three event streams
/// stay consistent: one outcome, one error_kind, one status (P1-5).
struct AttemptFinalizer {
    telemetry: std::sync::Arc<dyn crate::ports::EventSink>,
    request_id: String,
    request_started: chrono::DateTime<chrono::Utc>,
    failure_threshold: i64,
    circuit_open_seconds: i64,
}

impl AttemptFinalizer {
    fn new(
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
    fn finalize(
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
                // A client-side cancellation is not a channel verdict.
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

/// Reads a response body with a hard byte cap (P1-1). Returns the buffered
/// bytes (at most `cap`) plus whether the cap was hit; the stream is not
/// drained past the cap. The whole read is bounded by `timeout`: a transport
/// error mid-body maps to `ConnectionReset`, an expired deadline to
/// `FirstByteTimeout` — matching the pre-existing non-stream classification.
async fn read_bounded_body<S>(
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
                // Hard cap reached: stop buffering. The undelivered tail is
                // dropped with the stream (connection closes, no reuse).
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

/// Terminal telemetry for a mapped response whose body could not be decoded
/// (P1-2): one outcome drives channel/attempt/request, and the caller sends
/// a stable 502. The plaintext the conversion needs does not exist.
fn decode_failure(env: &AttemptEnv<'_>, failover: bool, response_bytes: i64) {
    let finished = env.clock.now_utc();
    AttemptFinalizer::new(
        Arc::new(env.telemetry.clone()),
        env.request_id,
        env.started,
        env.runtime.failure_threshold,
        env.runtime.circuit_open_seconds,
    )
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

/// Parses a non-streamed upstream response body into Usage.
fn usage_from_body(protocol: &str, body: &[u8]) -> Usage {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return Usage::default();
    };
    match protocol::usage_value(protocol, &value) {
        Some(usage) => protocol::normalize_usage(protocol, &usage),
        None => Usage::default(),
    }
}

async fn read_body(request: Request, max_bytes: usize) -> Result<Bytes> {
    to_bytes(request.into_body(), max_bytes)
        .await
        .map_err(Into::into)
}

/// Everything one candidate attempt needs besides the upstream response
/// (P2-4): keeps the response-policy builders (transparent/mapped streaming,
/// bounded non-stream) standalone functions instead of inline closures.
#[derive(Clone)]
struct AttemptEnv<'a> {
    telemetry: &'a Telemetry,
    clock: Arc<dyn crate::ports::Clock>,
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
    /// Mapped entry target, when this attempt runs on a mapping entry.
    mapping: Option<&'a routing::MappingTarget>,
    /// Remaining candidates after this one (failover eligibility flag).
    candidates_len: i64,
}

/// Streamed non-mapped 2xx: transparent forwarding with bounded plaintext
/// Terminal telemetry for a stream that ran to a known end (P1-5): every
/// finalize site must go through [`AttemptFinalizer`], and streams that end
/// on a terminal marker record their outcome BEFORE the client can hang up
/// after the last event — a completed response must never degrade into a
/// spurious `cancelled` with zeroed data.
#[allow(clippy::too_many_arguments)]
fn finalize_stream_attempt(
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

/// Whether an SSE event carries its protocol's terminal marker — the
/// response is complete and nothing of value follows:
/// - openai chat: `data: [DONE]` (checked separately, it is not JSON)
/// - openai responses: `response.completed` (carries the final usage)
/// - claude: `message_stop`
/// - gemini: the final chunk carries `finishReason`
fn terminal_marker(protocol: &str, value: &Value) -> bool {
    match protocol {
        "openai_responses" => {
            value.get("type").and_then(Value::as_str) == Some("response.completed")
        }
        "claude" => value.get("type").and_then(Value::as_str) == Some("message_stop"),
        "gemini" => value.pointer("/candidates/0/finishReason").is_some(),
        _ => false,
    }
}

/// Parse SSE `data:` lines out of the pending buffer for observability
/// (first-token detection, usage merge) and terminal-marker detection.
/// Complete `\n`-terminated lines are consumed; with `tail` the final
/// unterminated line is processed too (some upstreams end without a newline,
/// and the last usage event may live there). Returns true when a terminal
/// marker was seen. Observed first-token/usage values are mirrored into
/// `stats` so a mid-stream client disconnect still records them.
#[allow(clippy::too_many_arguments)]
fn scan_observable_lines(
    pending: &mut Vec<u8>,
    tail: bool,
    stream_protocol: &str,
    usage: &mut Usage,
    first_token_ms: &mut Option<i64>,
    first_token_seen: &mut bool,
    stats: &SharedStreamStats,
    now: chrono::DateTime<chrono::Utc>,
    attempt_started: chrono::DateTime<chrono::Utc>,
) -> bool {
    let mut terminal = false;
    let mut consumed = 0usize;
    while consumed < pending.len() {
        let line_end = match pending[consumed..].iter().position(|byte| *byte == b'\n') {
            Some(offset) => consumed + offset,
            None => {
                if !tail {
                    break;
                }
                pending.len()
            }
        };
        let line = &pending[consumed..line_end];
        consumed = if line_end < pending.len() {
            line_end + 1
        } else {
            line_end
        };
        let line = String::from_utf8_lossy(line);
        let line = line.trim_end_matches('\r');
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let data = data.trim();
        if data.is_empty() {
            continue;
        }
        if stream_protocol == "openai_compatible" && data == "[DONE]" {
            terminal = true;
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(data) else {
            continue;
        };
        if first_token_ms.is_none() && convert::chunk_has_content(stream_protocol, &value) {
            *first_token_ms =
                Some(now.signed_duration_since(attempt_started).num_milliseconds());
            *stats.first_token_ms.lock() = *first_token_ms;
            *first_token_seen = true;
        }
        if terminal_marker(stream_protocol, &value) {
            terminal = true;
        }
        if let Some(raw_usage) = protocol::usage_value(stream_protocol, &value) {
            let normalized = protocol::normalize_usage(stream_protocol, &raw_usage);
            usage.merge(&normalized);
            *stats.usage.lock() = usage.clone();
        }
    }
    pending.drain(..consumed);
    terminal
}

/// Usage snapshot from the mapped-stream converter (P1-5): meaningful only
/// when the conversion was clean; the cache-miss derivation mirrors the
/// protocol adapters.
fn converter_usage(
    converter: &convert::MappedStreamConverter,
    stream_protocol: &str,
    decode_failed: bool,
) -> Usage {
    if decode_failed {
        return Usage::default();
    }
    let (input, output, cache_read, cache_write) = converter.usage();
    usage_from_parts(stream_protocol, input, output, cache_read, cache_write)
}

/// Mirror the converter's current usage into the shared stats so a mid-stream
/// client disconnect records what was already observed.
fn sync_converter_usage(
    converter: &convert::MappedStreamConverter,
    stats: &SharedStreamStats,
    stream_protocol: &str,
) {
    let (input, output, cache_read, cache_write) = converter.usage();
    *stats.usage.lock() = usage_from_parts(stream_protocol, input, output, cache_read, cache_write);
}

/// Cumulative decode cap for the transparent streaming forward path. The
/// stream is never buffered — this only keeps the decoder's arithmetic sane;
/// per-feed output is bounded by `compression::FEED_LIMIT`, so memory stays
/// bounded regardless of this value.
const STREAM_FORWARD_DECODE_MAX_TOTAL: usize = 8 * 1024 * 1024 * 1024;

/// Decodes the upstream stream and forwards the plaintext to the client
/// (P1-9 revised): a compressed upstream response is decoded incrementally
/// so a truncated upstream stream surfaces downstream as a cleanly
/// interrupted plaintext stream — never as a corrupt compressed body that
/// fails client-side inflate (e.g. omp's `ZlibError`). Never buffers the
/// body; mid-stream failures surface as `stream_interrupted` terminal
/// events.
fn transparent_stream(
    env: AttemptEnv<'_>,
    response: crate::ports::UpstreamResponse,
    status: axum::http::StatusCode,
    response_headers: axum::http::HeaderMap,
) -> Response<Body> {
    let AttemptEnv {
        telemetry,
        clock,
        request_id,
        started,
        candidate,
        attempts,
        attempt_started,
        runtime,
        upstream_protocol,
        upstream_model,
        ..
    } = env;
    let upstream_stream = response.body.into_stream();
    let mut response_headers_for_client = response_headers.clone();
    response_headers_for_client.remove("content-length");
    // The relayed body is plaintext; the upstream transfer encoding must not
    // leak through (the client would try to inflate it and fail on any
    // truncation).
    response_headers_for_client.remove("content-encoding");
    // Lossless incremental decoder: the decoded plaintext is BOTH forwarded
    // and scanned. The cumulative cap is effectively unbounded — the
    // streaming path never buffers the body; only per-feed output is capped.
    let decoder = compression::RequiredDecoder::new(
        compression::ContentDecoder::from_encoding(response_headers.get("content-encoding")),
        STREAM_FORWARD_DECODE_MAX_TOTAL,
    );
    let telemetry = telemetry.clone();
    let request_id_stream = request_id.to_owned();
    let candidate_stream = candidate.clone();
    let upstream_protocol_stream = upstream_protocol.to_owned();
    let upstream_model_stream = upstream_model.to_owned();
    let stream_protocol = upstream_protocol_stream.clone();
    let failure_threshold = runtime.failure_threshold;
    let circuit_open_seconds = runtime.circuit_open_seconds;
    // First-token accounting spans the whole attempt (P1-1): the deadline is
    // anchored at attempt start, so a slow response head cannot extend the
    // window. The send phase itself is bounded by first_byte_timeout.
    let first_token_deadline = env.attempt_started_instant
        + Duration::from_secs(runtime.first_token_timeout_seconds.max(1) as u64);
    let stream_idle_timeout =
        Duration::from_secs(runtime.stream_idle_timeout_seconds.max(1) as u64);
    let stats = Arc::new(SharedStreamStats::default());
    let cancel_completed = Arc::new(AtomicBool::new(false));
    let cancel_finalized = Arc::new(AtomicBool::new(false));
    let cancel_completed_flag = cancel_completed.clone();
    let cancel_responded = Arc::new(AtomicBool::new(false));
    let cancel_telemetry = telemetry.clone();
    let cancel_request_id = request_id.to_owned();
    let cancel_candidate = candidate.clone();
    let cancel_attempt_no = attempts;
    let cancel_attempt_started = attempt_started;
    let cancel_started = started;
    let cancel_status = status.as_u16() as i64;
    let cancel_threshold = failure_threshold;
    let cancel_open_seconds = circuit_open_seconds;
    let cancel_upstream_protocol = upstream_protocol_stream.clone();
    let cancel_upstream_model = upstream_model_stream.clone();
    let cancel_responded_flag = cancel_responded.clone();
    let cancel_clock = Arc::clone(&clock);
    let cancel_stats = stats.clone();
    let on_cancel: Box<dyn FnOnce() + Send> = Box::new(move || {
        let finished = cancel_clock.now_utc();
        AttemptFinalizer::new(
            Arc::new(cancel_telemetry.clone()),
            &cancel_request_id,
            cancel_started,
            cancel_threshold,
            cancel_open_seconds,
        )
        .finalize(
            &cancel_candidate,
            cancel_attempt_no,
            cancel_attempt_started,
            finished,
            AttemptOutcome::Cancelled,
            Some(cancel_status),
            None,
            false,
            cancel_responded_flag.load(Ordering::SeqCst),
            *cancel_stats.first_byte_ms.lock(),
            *cancel_stats.first_token_ms.lock(),
            cancel_stats.usage.lock().clone(),
            cancel_stats.bytes.load(Ordering::SeqCst),
            Some(cancel_upstream_protocol),
            Some(cancel_upstream_model),
            false,
        );
    });
    let cancel_aware = CancelAware {
        inner: upstream_stream,
        completed: cancel_completed,
        finalized: cancel_finalized,
        responded: cancel_responded,
        on_cancel: Some(on_cancel),
    };
    let stream_body = stream! {
        let mut upstream_stream = cancel_aware;
        let mut decoder = decoder;
        let mut response_bytes = 0i64;
        let mut ok = true;
        let mut idle_timeout = false;
        let mut pending: Vec<u8> = Vec::new();
        let mut first_byte_ms: Option<i64> = None;
        let mut first_token_ms: Option<i64> = None;
        let mut first_token_seen = false;
        let mut usage: Usage = Usage::default();
        loop {
            // Before the first token the absolute deadline governs. The
            // deadline is polled first (biased) so a first token that
            // arrived past the window times out even when the bytes are
            // already buffered (P1-1).
            let next = if first_token_seen {
                tokio::time::timeout(stream_idle_timeout, upstream_stream.next())
                    .await
                    .map_err(|_| ())
            } else {
                tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until(first_token_deadline) => Err(()),
                    next = upstream_stream.next() => Ok(next),
                }
            };
            match next {
                Ok(Some(Ok(chunk))) => {
                    let now = clock.now_utc();
                    if first_byte_ms.is_none() {
                        first_byte_ms = Some(
                            now.signed_duration_since(attempt_started).num_milliseconds(),
                        );
                        *stats.first_byte_ms.lock() = first_byte_ms;
                    }
                    // Lossless decode: the plaintext is BOTH forwarded and
                    // scanned. A corrupt/truncated frame is terminal for the
                    // relay — the client must never receive compressed bytes
                    // it cannot finish inflating (a truncated body would
                    // crash client-side decompression, e.g. omp's ZlibError).
                    // Each feed is capped by compression::FEED_LIMIT, so the
                    // drain below streams retained input out piece by piece
                    // without ever buffering more than one piece.
                    let mut piece = match decoder.feed_required(&chunk) {
                        Ok(decoded) => decoded,
                        Err(error) => {
                            tracing::warn!(
                                request_id = %request_id_stream,
                                error = ?error,
                                "transparent stream decode failed; interrupting downstream"
                            );
                            ok = false;
                            break;
                        }
                    };
                    loop {
                        if !piece.is_empty() {
                            response_bytes += piece.len() as i64;
                            stats.bytes.store(response_bytes, Ordering::SeqCst);
                            pending.extend_from_slice(&piece);
                            let terminal = scan_observable_lines(
                                &mut pending,
                                false,
                                &stream_protocol,
                                &mut usage,
                                &mut first_token_ms,
                                &mut first_token_seen,
                                &stats,
                                now,
                                attempt_started,
                            );
                            if terminal {
                                // The terminal marker (e.g. `data: [DONE]`)
                                // is in this piece: the response is complete.
                                // Record the terminal telemetry BEFORE
                                // handing the final bytes to the client — a
                                // client that hangs up right after the last
                                // event must not turn a completed stream
                                // into a spurious `cancelled` with zeroed
                                // data.
                                cancel_completed_flag.store(true, Ordering::SeqCst);
                                let finished = clock.now_utc();
                                finalize_stream_attempt(
                                    &telemetry,
                                    &request_id_stream,
                                    started,
                                    failure_threshold,
                                    circuit_open_seconds,
                                    &candidate_stream,
                                    attempts,
                                    attempt_started,
                                    finished,
                                    AttemptOutcome::Success,
                                    status.as_u16() as i64,
                                    None,
                                    false,
                                    first_byte_ms,
                                    first_token_ms,
                                    usage,
                                    response_bytes,
                                    upstream_protocol_stream.clone(),
                                    upstream_model_stream.clone(),
                                );
                                yield Ok::<Bytes, Infallible>(piece.into());
                                return;
                            }
                            yield Ok::<Bytes, Infallible>(piece.into());
                        }
                        // The first feed may leave input retained (per-feed
                        // output cap); drain it before the next upstream
                        // chunk. `feed_required` re-processes retained input
                        // even when given an empty slice.
                        match decoder.feed_required(&[]) {
                            Ok(more) => piece = more,
                            Err(error) => {
                                tracing::warn!(
                                    request_id = %request_id_stream,
                                    error = ?error,
                                    "transparent stream decode failed while draining; interrupting downstream"
                                );
                                ok = false;
                                break;
                            }
                        }
                        if piece.is_empty() {
                            break;
                        }
                    }
                    if !ok {
                        break;
                    }
                }
                Ok(Some(Err(_))) => {
                    ok = false;
                    break;
                }
                Ok(None) => {
                    // Drain any input retained by the decoder (per-feed
                    // output caps) so the final scan sees the whole body.
                    let now = clock.now_utc();
                    loop {
                        match decoder.feed_required(&[]) {
                            Ok(more) if !more.is_empty() => {
                                response_bytes += more.len() as i64;
                                stats.bytes.store(response_bytes, Ordering::SeqCst);
                                pending.extend_from_slice(&more);
                            }
                            Ok(_) => break,
                            Err(error) => {
                                tracing::warn!(
                                    request_id = %request_id_stream,
                                    error = ?error,
                                    "transparent stream decode failed at EOF; interrupting downstream"
                                );
                                ok = false;
                                break;
                            }
                        }
                    }
                    if !ok {
                        break;
                    }
                    // Parse any final unterminated line: usage (and
                    // occasionally a terminal marker) may live in the last
                    // line when the upstream ends without a trailing newline.
                    let terminal = scan_observable_lines(
                        &mut pending,
                        true,
                        &stream_protocol,
                        &mut usage,
                        &mut first_token_ms,
                        &mut first_token_seen,
                        &stats,
                        now,
                        attempt_started,
                    );
                    if terminal {
                        cancel_completed_flag.store(true, Ordering::SeqCst);
                        let finished = clock.now_utc();
                        finalize_stream_attempt(
                            &telemetry,
                            &request_id_stream,
                            started,
                            failure_threshold,
                            circuit_open_seconds,
                            &candidate_stream,
                            attempts,
                            attempt_started,
                            finished,
                            AttemptOutcome::Success,
                            status.as_u16() as i64,
                            None,
                            false,
                            first_byte_ms,
                            first_token_ms,
                            usage,
                            response_bytes,
                            upstream_protocol_stream.clone(),
                            upstream_model_stream.clone(),
                        );
                        return;
                    }
                    if !decoder.finished() {
                        // The compressed stream ended mid-frame: the
                        // plaintext is incomplete (e.g. the gzip trailer is
                        // missing). The plaintext prefix already forwarded is
                        // clean; count the attempt as interrupted so the
                        // channel verdict matches what the client observed.
                        tracing::warn!(
                            request_id = %request_id_stream,
                            "transparent stream ended before the compressed stream finished"
                        );
                        ok = false;
                    }
                    break;
                }
                Err(_) => {
                    ok = false;
                    idle_timeout = true;
                    break;
                }
            }
        }
        // The generator ran to its terminal telemetry (success, upstream
        // error, idle/first-token timeout, or a clean end that never
        // produced a first token): the Drop path must not append a duplicate
        // `cancelled` finish.
        cancel_completed_flag.store(true, Ordering::SeqCst);
        let finished = clock.now_utc();
        let (outcome, error_kind, final_status): (AttemptOutcome, Option<String>, i64) = if !ok {
            if idle_timeout {
                (
                    AttemptOutcome::StreamInterrupted,
                    Some("transport_timeout".into()),
                    504,
                )
            } else {
                (
                    AttemptOutcome::StreamInterrupted,
                    Some("stream_interrupted".into()),
                    status.as_u16() as i64,
                )
            }
        } else if first_token_ms.is_none() {
            // The upstream closed the stream without ever producing a first
            // token and without a terminal marker (e.g. an empty 200 body).
            // That violates the first-token protection just as much as a
            // stall does — count it against the circuit so a broken upstream
            // trips instead of silently "succeeding" with an empty response.
            (
                AttemptOutcome::StreamInterrupted,
                Some("no_first_token".into()),
                status.as_u16() as i64,
            )
        } else {
            (AttemptOutcome::Success, None, status.as_u16() as i64)
        };
        AttemptFinalizer::new(
                Arc::new(telemetry.clone()),
            &request_id_stream,
            started,
            failure_threshold,
            circuit_open_seconds,
        )
        .finalize(
            &candidate_stream,
            attempts,
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
            Some(upstream_protocol_stream),
            Some(upstream_model_stream),
            true,
        );
    };
    let mut result = Response::new(Body::from_stream(stream_body));
    *result.status_mut() = status;
    *result.headers_mut() = response_headers_for_client;
    result
}
/// Outcome of the mapped-stream builder: either a response to send, or a
/// prelude-level failure that already emitted its terminal telemetry and
/// should fail over to the next candidate (P2-4).
enum MappedStreamResult {
    Respond(Response<Body>),
    FailOver(Option<TransportFailure>),
}

/// Mapped streaming: a short buffered prelude (so a 200 error body can
/// still fail over) then incremental conversion of the live stream. The
/// prelude decode is lossless (`RequiredDecoder`, P1-2) — a corrupt or
/// oversized body fails the attempt instead of truncating the stream.
async fn mapped_stream(
    env: AttemptEnv<'_>,
    response: crate::ports::UpstreamResponse,
    status: axum::http::StatusCode,
    response_headers: axum::http::HeaderMap,
) -> MappedStreamResult {
    let AttemptEnv {
        telemetry,
        clock,
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
        candidates_len,
        ..
    } = env;
    let failure_threshold = runtime.failure_threshold;
    let circuit_open_seconds = runtime.circuit_open_seconds;
    // Absolute first-token window anchored at attempt start (P1-1): a slow
    // response head cannot extend it.
    let first_token_deadline = attempt_started_instant
        + Duration::from_secs(runtime.first_token_timeout_seconds.max(1) as u64);
    let stream_idle_timeout =
        Duration::from_secs(runtime.stream_idle_timeout_seconds.max(1) as u64);
    let stats = Arc::new(SharedStreamStats::default());
    let mut upstream_stream = response.body.into_stream();
    // P1-2: mapped conversion REQUIRES the plaintext — a decode failure
    // must fail the attempt, never degrade to silence.
    let mut decoder = compression::RequiredDecoder::new(
        compression::ContentDecoder::from_encoding(response_headers.get("content-encoding")),
        (runtime.max_buffered_upstream_body_mb.max(1) as usize) * 1024 * 1024,
    );
    // Raw chunk + its decoded plaintext: the prelude is replayed through
    // the converter exactly once (P1-3) — re-feeding the decoder would
    // double-advance its state and corrupt output.
    let mut prelude: Vec<(Bytes, Vec<u8>)> = Vec::new();
    let mut prelude_bytes = 0usize;
    let mut scan = convert::StreamScan::new(upstream_protocol);
    // Absolute-deadline prelude scan. `timeout(remaining, ...)` polls the
    // inner future first, so an already-buffered body would win against an
    // expired deadline; the deadline is polled first (biased) so a late
    // first token is a timeout even when the bytes are already available
    // (P1-1).
    enum PreludeEnd {
        Ready,
        UpstreamError(String),
        DecodeError,
        Deadline,
    }
    let prelude_outcome = async {
        loop {
            let next = tokio::select! {
                biased;
                _ = tokio::time::sleep_until(first_token_deadline) => {
                    return PreludeEnd::Deadline;
                }
                next = upstream_stream.next() => next,
            };
            match next {
                Some(Ok(chunk)) => {
                    prelude_bytes += chunk.len();
                    let decoded = match decoder.feed_required(&chunk) {
                        Ok(decoded) => decoded,
                        Err(error) => {
                            tracing::warn!(
                                request_id = %request_id,
                                channel_id = %candidate.channel_id,
                                error = ?error,
                                "mapped response decode failed during prelude"
                            );
                            return PreludeEnd::DecodeError;
                        }
                    };
                    prelude.push((chunk.clone(), decoded.clone()));
                    if let Some(message) = scan.feed(upstream_protocol, &decoded) {
                        return PreludeEnd::UpstreamError(message);
                    }
                    if scan.first_token_now || prelude_bytes >= 1024 * 1024 {
                        return PreludeEnd::Ready;
                    }
                }
                Some(Err(_)) => return PreludeEnd::Ready,
                None => return PreludeEnd::Ready,
            }
        }
    }
    .await;
    match prelude_outcome {
        PreludeEnd::UpstreamError(_message) => {
            // 2xx body carries an upstream error: record and fail over to
            // the next candidate (Python prelude path).
            let finished = clock.now_utc();
            AttemptFinalizer::new(
                Arc::new(telemetry.clone()),
                request_id,
                started,
                failure_threshold,
                circuit_open_seconds,
            )
            .finalize(
                candidate,
                attempts,
                attempt_started,
                finished,
                AttemptOutcome::UpstreamError,
                Some(502),
                Some("upstream_error".into()),
                attempts < candidates_len,
                false,
                None,
                None,
                Usage::default(),
                prelude_bytes as i64,
                Some(env.upstream_protocol.into()),
                Some(env.upstream_model.into()),
                true,
            );
            return MappedStreamResult::FailOver(None);
        }
        PreludeEnd::DecodeError => {
            // The 200 body cannot be decoded (corrupt frame or over the
            // buffered-body cap): the plaintext the conversion needs does
            // not exist. Fail over.
            let finished = clock.now_utc();
            AttemptFinalizer::new(
                Arc::new(telemetry.clone()),
                request_id,
                started,
                failure_threshold,
                circuit_open_seconds,
            )
            .finalize(
                candidate,
                attempts,
                attempt_started,
                finished,
                AttemptOutcome::UpstreamError,
                Some(502),
                Some("upstream_decode_error".into()),
                attempts < candidates_len,
                false,
                None,
                None,
                Usage::default(),
                prelude_bytes as i64,
                Some(env.upstream_protocol.into()),
                Some(env.upstream_model.into()),
                true,
            );
            return MappedStreamResult::FailOver(None);
        }
        PreludeEnd::Deadline => {
            // No first token within the absolute attempt window: fail over
            // (P1-1). Classified as a timeout so the tail maps a
            // single-candidate run to 504 (P1-2).
            let finished = clock.now_utc();
            AttemptFinalizer::new(
                Arc::new(telemetry.clone()),
                request_id,
                started,
                failure_threshold,
                circuit_open_seconds,
            )
            .finalize(
                candidate,
                attempts,
                attempt_started,
                finished,
                AttemptOutcome::TransportError,
                None,
                Some("timeout".into()),
                attempts < candidates_len,
                false,
                None,
                None,
                Usage::default(),
                prelude_bytes as i64,
                Some(env.upstream_protocol.into()),
                Some(env.upstream_model.into()),
                true,
            );
            return MappedStreamResult::FailOver(Some(TransportFailure::FirstTokenTimeout));
        }
        PreludeEnd::Ready => {}
    }
    let converter = match convert::MappedStreamConverter::new(
        entry_protocol,
        upstream_protocol,
        entry_model,
    ) {
        Ok(converter) => converter,
        Err(error) => {
            tracing::error!(request_id = %request_id, error = %error, "stream converter init failed");
            return MappedStreamResult::Respond(gateway_error(
                entry_protocol,
                StatusCode::INTERNAL_SERVER_ERROR,
                "conversion_error",
                "Conversion error",
                request_id,
            ));
        }
    };
    let stream_protocol = upstream_protocol.to_owned();
    let response_headers_for_client = response_headers.clone();
    let telemetry = telemetry.clone();
    let request_id_stream = request_id.to_owned();
    let candidate_stream = candidate.clone();
    let upstream_protocol_stream = upstream_protocol.to_owned();
    let upstream_model_stream = upstream_model.to_owned();
    let cancel_completed = Arc::new(AtomicBool::new(false));
    let cancel_finalized = Arc::new(AtomicBool::new(false));
    let cancel_completed_flag = cancel_completed.clone();
    let cancel_responded = Arc::new(AtomicBool::new(false));
    let cancel_telemetry = telemetry.clone();
    let cancel_request_id = request_id.to_owned();
    let cancel_candidate = candidate.clone();
    let cancel_attempt_no = attempts;
    let cancel_attempt_started = attempt_started;
    let cancel_started = started;
    let cancel_status = status.as_u16() as i64;
    let cancel_threshold = failure_threshold;
    let cancel_open_seconds = circuit_open_seconds;
    let cancel_upstream_protocol = upstream_protocol_stream.clone();
    let cancel_upstream_model = upstream_model_stream.clone();
    let cancel_responded_flag = cancel_responded.clone();
    let cancel_clock = Arc::clone(&clock);
    let cancel_stats = stats.clone();
    let on_cancel: Box<dyn FnOnce() + Send> = Box::new(move || {
        let finished = cancel_clock.now_utc();
        AttemptFinalizer::new(
            Arc::new(cancel_telemetry.clone()),
            &cancel_request_id,
            cancel_started,
            cancel_threshold,
            cancel_open_seconds,
        )
        .finalize(
            &cancel_candidate,
            cancel_attempt_no,
            cancel_attempt_started,
            finished,
            AttemptOutcome::Cancelled,
            Some(cancel_status),
            None,
            false,
            cancel_responded_flag.load(Ordering::SeqCst),
            *cancel_stats.first_byte_ms.lock(),
            *cancel_stats.first_token_ms.lock(),
            cancel_stats.usage.lock().clone(),
            cancel_stats.bytes.load(Ordering::SeqCst),
            Some(cancel_upstream_protocol),
            Some(cancel_upstream_model),
            false,
        );
    });
    let cancel_aware = CancelAware {
        inner: upstream_stream,
        completed: cancel_completed,
        finalized: cancel_finalized,
        responded: cancel_responded,
        on_cancel: Some(on_cancel),
    };
    let stream_body = stream! {
        let mut upstream_stream = cancel_aware;
        let mut converter = converter;
        let mut scan = scan;
        let mut decoder = decoder;
        let mut response_bytes = 0i64;
        let mut ok = true;
        let mut idle_timeout = false;
        let mut decode_failed = false;
        let mut received_any = !prelude.is_empty();
        let mut first_byte_ms: Option<i64> = None;
        let mut first_token_ms: Option<i64> = None;
        // Replay the buffered prelude through the converter. The decoder was
        // already advanced over these bytes during the scan; only the
        // stored plaintext is fed here (P1-3).
        for (raw, decoded) in prelude {
            response_bytes += raw.len() as i64;
            stats.bytes.store(response_bytes, Ordering::SeqCst);
            if first_byte_ms.is_none() {
                first_byte_ms = Some(
                    clock.now_utc()
                        .signed_duration_since(attempt_started)
                        .num_milliseconds(),
                );
                *stats.first_byte_ms.lock() = first_byte_ms;
            }
            let converted = converter.feed(&decoded);
            sync_converter_usage(&converter, &stats, &stream_protocol);
            if converter.finished() {
                // The whole stream fit in the prelude: record the terminal
                // telemetry before the client has seen a single byte — a
                // disconnect after the last event must not downgrade a
                // completed stream to `cancelled`.
                if first_token_ms.is_none() && scan.first_token() {
                    first_token_ms = Some(
                        clock.now_utc()
                            .signed_duration_since(attempt_started)
                            .num_milliseconds(),
                    );
                    *stats.first_token_ms.lock() = first_token_ms;
                }
                cancel_completed_flag.store(true, Ordering::SeqCst);
                finalize_stream_attempt(
                    &telemetry,
                    &request_id_stream,
                    started,
                    failure_threshold,
                    circuit_open_seconds,
                    &candidate_stream,
                    attempts,
                    attempt_started,
                    clock.now_utc(),
                    AttemptOutcome::Success,
                    status.as_u16() as i64,
                    None,
                    false,
                    first_byte_ms,
                    first_token_ms,
                    converter_usage(&converter, &stream_protocol, false),
                    response_bytes,
                    upstream_protocol_stream.clone(),
                    upstream_model_stream.clone(),
                );
                if !converted.is_empty() {
                    yield Ok::<Bytes, Infallible>(Bytes::from(converted));
                }
                return;
            }
            if !converted.is_empty() {
                yield Ok::<Bytes, Infallible>(Bytes::from(converted));
            }
        }
        if first_token_ms.is_none() && scan.first_token() {
            first_token_ms = Some(
                clock.now_utc()
                    .signed_duration_since(attempt_started)
                    .num_milliseconds(),
            );
            *stats.first_token_ms.lock() = first_token_ms;
        }
        loop {
            let next = tokio::time::timeout(stream_idle_timeout, upstream_stream.next()).await;
            match next {
                Ok(Some(Ok(chunk))) => {
                    response_bytes += chunk.len() as i64;
                    stats.bytes.store(response_bytes, Ordering::SeqCst);
                    received_any = true;
                    let now = clock.now_utc();
                    if first_byte_ms.is_none() {
                        first_byte_ms = Some(
                            now.signed_duration_since(attempt_started)
                                .num_milliseconds(),
                        );
                        *stats.first_byte_ms.lock() = first_byte_ms;
                    }
                    let decoded = match decoder.feed_required(&chunk) {
                        Ok(decoded) => decoded,
                        Err(error) => {
                            // P1-2: the plaintext the conversion needs no
                            // longer exists — emit a fixed gateway error
                            // event and stop, never a silently truncated
                            // stream.
                            tracing::warn!(
                                request_id = %request_id_stream,
                                channel_id = %candidate_stream.channel_id,
                                error = ?error,
                                "mapped response decode failed mid-stream"
                            );
                            let converted = converter.error_event(
                                "Upstream response could not be decoded",
                            );
                            // The error event is the terminal event: record
                            // the outcome before the client can hang up.
                            cancel_completed_flag.store(true, Ordering::SeqCst);
                            finalize_stream_attempt(
                                &telemetry,
                                &request_id_stream,
                                started,
                                failure_threshold,
                                circuit_open_seconds,
                                &candidate_stream,
                                attempts,
                                attempt_started,
                                clock.now_utc(),
                                AttemptOutcome::GatewayError,
                                status.as_u16() as i64,
                                Some("upstream_decode_error".into()),
                                true,
                                first_byte_ms,
                                first_token_ms,
                                Usage::default(),
                                response_bytes,
                                upstream_protocol_stream.clone(),
                                upstream_model_stream.clone(),
                            );
                            if !converted.is_empty() {
                                yield Ok::<Bytes, Infallible>(Bytes::from(converted));
                            }
                            return;
                        }
                    };
                    if let Some(message) = scan.feed(&stream_protocol, &decoded) {
                        let converted = converter.error_event(&message);
                        // The 2xx stream carries an upstream error; the
                        // converted error event is terminal.
                        cancel_completed_flag.store(true, Ordering::SeqCst);
                        finalize_stream_attempt(
                            &telemetry,
                            &request_id_stream,
                            started,
                            failure_threshold,
                            circuit_open_seconds,
                            &candidate_stream,
                            attempts,
                            attempt_started,
                            clock.now_utc(),
                            AttemptOutcome::UpstreamError,
                            status.as_u16() as i64,
                            Some("upstream_error".into()),
                            true,
                            first_byte_ms,
                            first_token_ms,
                            converter_usage(&converter, &stream_protocol, false),
                            response_bytes,
                            upstream_protocol_stream.clone(),
                            upstream_model_stream.clone(),
                        );
                        if !converted.is_empty() {
                            yield Ok::<Bytes, Infallible>(Bytes::from(converted));
                        }
                        return;
                    }
                    if first_token_ms.is_none() && scan.first_token_now {
                        first_token_ms = Some(
                            now.signed_duration_since(attempt_started)
                                .num_milliseconds(),
                        );
                        *stats.first_token_ms.lock() = first_token_ms;
                    }
                    let converted = converter.feed(&decoded);
                    sync_converter_usage(&converter, &stats, &stream_protocol);
                    if converter.finished() {
                        // The terminal event (e.g. `response.completed`)
                        // was produced: record the outcome BEFORE yielding
                        // it, so a client that hangs up right after the
                        // last event still leaves a success behind.
                        cancel_completed_flag.store(true, Ordering::SeqCst);
                        finalize_stream_attempt(
                            &telemetry,
                            &request_id_stream,
                            started,
                            failure_threshold,
                            circuit_open_seconds,
                            &candidate_stream,
                            attempts,
                            attempt_started,
                            clock.now_utc(),
                            AttemptOutcome::Success,
                            status.as_u16() as i64,
                            None,
                            false,
                            first_byte_ms,
                            first_token_ms,
                            converter_usage(&converter, &stream_protocol, false),
                            response_bytes,
                            upstream_protocol_stream.clone(),
                            upstream_model_stream.clone(),
                        );
                        if !converted.is_empty() {
                            yield Ok::<Bytes, Infallible>(Bytes::from(converted));
                        }
                        return;
                    }
                    if !converted.is_empty() {
                        yield Ok::<Bytes, Infallible>(Bytes::from(converted));
                    }
                }
                Ok(Some(Err(_))) => {
                    ok = false;
                    break;
                }
                Ok(None) => {
                    if !decoder.finished() && received_any {
                        // The body ended mid-frame: the plaintext is
                        // incomplete (P1-2). A body that carried NO bytes at
                        // all is an empty stream (the no-first-token case),
                        // not a decode failure.
                        decode_failed = true;
                    }
                    break;
                }
                Err(_) => {
                    ok = false;
                    idle_timeout = true;
                    break;
                }
            }
        }
        // Close any remaining items on a clean finish. When the converter
        // finishes here (upstream ended WITHOUT a terminal marker — a bare
        // close), record the outcome before the tail events reach the
        // client. A bare close that never produced a first token is a
        // first-token protection violation, not a success.
        if !decode_failed && ok {
            let tail = converter.flush();
            if converter.finished() {
                let (outcome, error_kind, countable) = if first_token_ms.is_none() {
                    (
                        AttemptOutcome::StreamInterrupted,
                        Some("no_first_token".into()),
                        true,
                    )
                } else {
                    (AttemptOutcome::Success, None, false)
                };
                cancel_completed_flag.store(true, Ordering::SeqCst);
                finalize_stream_attempt(
                    &telemetry,
                    &request_id_stream,
                    started,
                    failure_threshold,
                    circuit_open_seconds,
                    &candidate_stream,
                    attempts,
                    attempt_started,
                    clock.now_utc(),
                    outcome,
                    status.as_u16() as i64,
                    error_kind,
                    countable,
                    first_byte_ms,
                    first_token_ms,
                    converter_usage(&converter, &stream_protocol, false),
                    response_bytes,
                    upstream_protocol_stream.clone(),
                    upstream_model_stream.clone(),
                );
                if !tail.is_empty() {
                    yield Ok::<Bytes, Infallible>(Bytes::from(tail));
                }
                return;
            }
            if !tail.is_empty() {
                yield Ok::<Bytes, Infallible>(Bytes::from(tail));
            }
        }
        // The generator ran to its terminal telemetry (mid-frame end,
        // upstream error, or idle/first-token timeout): the Drop path must
        // not append a duplicate `cancelled` finish.
        cancel_completed_flag.store(true, Ordering::SeqCst);
        let finished = clock.now_utc();
        let (outcome, error_kind, final_status, countable): (
            AttemptOutcome,
            Option<String>,
            i64,
            bool,
        ) = if decode_failed {
            (
                AttemptOutcome::GatewayError,
                Some("upstream_decode_error".into()),
                status.as_u16() as i64,
                true,
            )
        } else if !ok {
            if idle_timeout {
                (
                    AttemptOutcome::StreamInterrupted,
                    Some("transport_timeout".into()),
                    504,
                    true,
                )
            } else {
                (
                    AttemptOutcome::StreamInterrupted,
                    Some("stream_interrupted".into()),
                    status.as_u16() as i64,
                    true,
                )
            }
        } else if first_token_ms.is_none() {
            // Passthrough conversion (entry == upstream protocol): no
            // terminal marker exists to detect, and the upstream closed
            // without ever producing a first token. Count it against the
            // circuit like the transparent path does.
            (
                AttemptOutcome::StreamInterrupted,
                Some("no_first_token".into()),
                status.as_u16() as i64,
                true,
            )
        } else {
            (AttemptOutcome::Success, None, status.as_u16() as i64, false)
        };
        finalize_stream_attempt(
            &telemetry,
            &request_id_stream,
            started,
            failure_threshold,
            circuit_open_seconds,
            &candidate_stream,
            attempts,
            attempt_started,
            finished,
            outcome,
            final_status,
            error_kind,
            countable,
            first_byte_ms,
            first_token_ms,
            converter_usage(&converter, &stream_protocol, decode_failed),
            response_bytes,
            upstream_protocol_stream,
            upstream_model_stream,
        );
    };
    let mut result = Response::new(Body::from_stream(stream_body));
    *result.status_mut() = status;
    let mut headers = response_headers_for_client;
    headers.remove("content-length");
    // The converted stream is plaintext; the original encoding header must
    // not leak through.
    headers.remove("content-encoding");
    headers.insert(
        "content-type",
        HeaderValue::from_static("text/event-stream"),
    );
    *result.headers_mut() = headers;
    MappedStreamResult::Respond(result)
}

/// Result of the request-preparation phase (P2-4): everything the attempt
/// loop needs, or an early gateway error response.
struct PreparedRequest {
    runtime: settings::RuntimeSettings,
    headers: axum::http::HeaderMap,
    path: String,
    query: Option<String>,
    entry_model: String,
    stream_requested: bool,
    mapping: Option<routing::MappingTarget>,
    upstream_protocol: String,
    upstream_model: String,
    converted_body: Bytes,
    candidates: Vec<Candidate>,
    compaction_mode: Option<CompactionMode>,
}

/// Request preparation (P2-4): settings, body read, gateway auth, model
/// inspection, mapping resolution and candidate routing. Any failure
/// produces an early gateway-error response instead of entering the attempt
/// loop.
async fn prepare_request(
    svc: &ProxyService,
    request: Request,
    entry_protocol: &str,
    fixed_path: Option<String>,
    request_id: &str,
    started: chrono::DateTime<chrono::Utc>,
) -> Result<PreparedRequest, Response<Body>> {
    let started_at = started.to_rfc3339();
    let (headers, request_path, query_string) = {
        let headers = request.headers().clone();
        let path = request.uri().path().to_owned();
        let query = request.uri().query().map(str::to_owned);
        (headers, path, query)
    };
    let path = fixed_path.unwrap_or(request_path);
    let query = query_string.as_deref();
    let runtime = match settings::runtime_settings_from(&svc.db).await {
        Ok(value) => value,
        Err(error) => {
            tracing::error!(request_id = %request_id, error = %error, "settings load failed");
            return Err(gateway_error(
                entry_protocol,
                StatusCode::INTERNAL_SERVER_ERROR,
                "settings_error",
                "Settings error",
                request_id,
            ));
        }
    };
    let body = match read_body(
        request,
        (runtime.max_request_body_mb.max(1) as usize) * 1024 * 1024,
    )
    .await
    {
        Ok(body) => body,
        Err(_) => {
            tracing::error!(request_id, "request body read failed");
            return Err(gateway_error(
                entry_protocol,
                StatusCode::PAYLOAD_TOO_LARGE,
                "request_too_large",
                "Request body exceeds the configured limit.",
                request_id,
            ));
        }
    };
    if !settings::authorize_gateway_from(&svc.db, &svc.secrets, &headers, query, entry_protocol)
        .await
        .unwrap_or(false)
    {
        return Err(gateway_error(
            entry_protocol,
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Gateway access denied.",
            request_id,
        ));
    }
    let (entry_model, stream_requested) =
        protocol::inspect_request(entry_protocol, &path, query, &body);
    let Some(entry_model) = entry_model else {
        return Err(gateway_error(
            entry_protocol,
            StatusCode::BAD_REQUEST,
            "model_required",
            "Unable to determine model from request.",
            request_id,
        ));
    };
    let compaction_mode =
        match remote_compaction::detect_compaction(&path, &body) {
            Ok(mode) => mode,
            Err(error) => {
                tracing::warn!(request_id = %request_id, %error, "invalid remote compaction request");
                return Err(gateway_error(
                    entry_protocol,
                    StatusCode::BAD_REQUEST,
                    "invalid_compaction_request",
                    &error.to_string(),
                    request_id,
                ));
            }
        };
    // Model mappings only apply to the dedicated mapping entry points
    // (/codex/v1/responses, /claudecode/v1/messages); the regular protocol
    // endpoints (/v1/responses, /v1/messages) are always routed directly,
    // mirroring the Python gateway.
    let is_mapped_entry = (entry_protocol == "openai_responses" && path.starts_with("/codex/"))
        || (entry_protocol == "claude" && path.starts_with("/claudecode/"));
    let mapping = if is_mapped_entry {
        match svc
            .routes
            .resolve_mapping(entry_protocol, &entry_model)
            .await
        {
            Ok(value) => value,
            Err(error) => {
                tracing::error!(request_id = %request_id, error = %error, "mapping lookup failed");
                return Err(gateway_error(
                    entry_protocol,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "database_error",
                    "Database error",
                    request_id,
                ));
            }
        }
    } else {
        None
    };
    // C1: a mapped entry with an unknown or disabled mapping must fail with a
    // clear 404 (Python: `unknown_mapped_model`) instead of silently routing
    // the request as a regular protocol request.
    if is_mapped_entry && mapping.is_none() {
        let finished = svc.clock.now_utc();
        svc.telemetry.emit(Event::RequestStart {
            id: request_id.to_owned(),
            protocol: entry_protocol.to_owned(),
            model_id: Some(entry_model.clone()),
            endpoint: path.clone(),
            stream: stream_requested,
            started_at: started_at.clone(),
            request_bytes: body.len() as i64,
        });
        svc.telemetry.emit(Event::RequestFinish {
            id: request_id.to_owned(),
            finished_at: finished.to_rfc3339(),
            duration_ms: finished.signed_duration_since(started).num_milliseconds(),
            status: Some(404),
            outcome: "gateway_error".into(),
            attempts: 0,
            channel_id: None,
            response_bytes: 0,
        });
        return Err(gateway_error(
            entry_protocol,
            StatusCode::NOT_FOUND,
            "unknown_mapped_model",
            &format!("Model '{entry_model}' is not a configured model mapping."),
            request_id,
        ));
    }
    let upstream_protocol = mapping
        .as_ref()
        .map(|value| value.upstream_protocol.as_str())
        .unwrap_or(entry_protocol);
    let upstream_model = mapping
        .as_ref()
        .map(|value| value.upstream_model.as_str())
        .unwrap_or(entry_model.as_str());
    if compaction_mode.is_some() && upstream_protocol != "openai_responses" {
        let finished = svc.clock.now_utc();
        svc.telemetry.emit(Event::RequestStart {
            id: request_id.to_owned(),
            protocol: entry_protocol.to_owned(),
            model_id: Some(entry_model.clone()),
            endpoint: path.clone(),
            stream: stream_requested,
            started_at: started_at.clone(),
            request_bytes: body.len() as i64,
        });
        svc.telemetry.emit(Event::RequestFinish {
            id: request_id.to_owned(),
            finished_at: finished.to_rfc3339(),
            duration_ms: finished.signed_duration_since(started).num_milliseconds(),
            status: Some(422),
            outcome: "gateway_error".into(),
            attempts: 0,
            channel_id: None,
            response_bytes: 0,
        });
        return Err(gateway_error(
            entry_protocol,
            StatusCode::UNPROCESSABLE_ENTITY,
            "remote_compaction_unsupported_upstream",
            "Remote compaction is only supported for openai_responses upstream channels.",
            request_id,
        ));
    }
    let converted_body = if let Some(value) = mapping.as_ref() {
        match convert::convert_request(
            &value.entry,
            &value.upstream_protocol,
            &value.upstream_model,
            &body,
        ) {
            Ok(body) => Bytes::from(body),
            Err(error) => {
                return Err(gateway_error(
                    entry_protocol,
                    StatusCode::BAD_REQUEST,
                    "invalid_request",
                    &error.to_string(),
                    request_id,
                ));
            }
        }
    } else {
        body.clone()
    };
    let (model_id, stream) =
        protocol::inspect_request(upstream_protocol, &path, query, &converted_body);
    let stream_requested = stream_requested || stream;
    let route_model = model_id.as_deref().unwrap_or(upstream_model);
    svc.telemetry.emit(Event::RequestStart {
        id: request_id.to_owned(),
        protocol: entry_protocol.to_owned(),
        model_id: Some(entry_model.clone()),
        endpoint: path.clone(),
        stream: stream_requested,
        started_at: started_at.clone(),
        request_bytes: body.len() as i64,
    });
    let candidates = match if let Some(mode) = compaction_mode {
        svc.routes
            .resolve_compaction_candidates(
                upstream_protocol,
                route_model,
                mode,
                runtime.max_failover_attempts,
            )
            .await
    } else {
        svc.routes
            .resolve_candidates(
                upstream_protocol,
                route_model,
                runtime.max_failover_attempts,
            )
            .await
    } {
        Ok(value) => value,
        Err(error) => {
            tracing::error!(request_id = %request_id, error = %error, "candidate resolution failed");
            return Err(gateway_error(
                entry_protocol,
                StatusCode::INTERNAL_SERVER_ERROR,
                "routing_error",
                "Routing error",
                request_id,
            ));
        }
    };
    if candidates.is_empty() {
        let (status_code, code, message) = if compaction_mode.is_some() {
            (
                422,
                "remote_compaction_unavailable",
                "No active channel supports remote compaction for this model.",
            )
        } else {
            (
                503,
                "no_active_channel",
                "No active channel is available for this model.",
            )
        };
        svc.telemetry.emit(Event::RequestFinish {
            id: request_id.to_owned(),
            finished_at: svc.clock.now_utc().to_rfc3339(),
            duration_ms: svc
                .clock
                .now_utc()
                .signed_duration_since(started)
                .num_milliseconds(),
            status: Some(status_code),
            outcome: "gateway_error".into(),
            attempts: 0,
            channel_id: None,
            response_bytes: 0,
        });
        return Err(gateway_error(
            entry_protocol,
            StatusCode::from_u16(status_code as u16)
                .unwrap_or(StatusCode::SERVICE_UNAVAILABLE),
            code,
            message,
            request_id,
        ));
    }
    let upstream_protocol = upstream_protocol.to_owned();
    let upstream_model = upstream_model.to_owned();
    Ok(PreparedRequest {
        runtime,
        headers,
        path,
        query: query.map(str::to_owned),
        entry_model,
        stream_requested,
        mapping,
        upstream_protocol,
        upstream_model,
        converted_body,
        candidates,
        compaction_mode,
    })
}

/// Outcome of the bounded non-stream builder (P2-4): a response to send,
/// or a transport failure that already emitted its terminal telemetry and
/// should fail over to the next candidate.
enum NonStreamResult {
    Respond(Response<Body>),
    FailOver(Option<TransportFailure>),
}

/// Non-stream success responses (mapped and non-mapped): bounded buffering
/// (P1-1), lossless decode for mapped conversion (P1-2), conversion, and
/// the unified terminal telemetry (P1-5).
async fn bounded_non_stream(
    env: AttemptEnv<'_>,
    response: crate::ports::UpstreamResponse,
    status: axum::http::StatusCode,
    response_headers: axum::http::HeaderMap,
) -> NonStreamResult {
    // P1-1: mapped non-stream responses must be fully buffered for
    // conversion, but the buffering is bounded — Content-Length is
    // pre-checked and the chunked accumulation enforces a hard cap.
    let buffered_cap = (env.runtime.max_buffered_upstream_body_mb.max(1) as usize) * 1024 * 1024;
    // Read from the raw reqwest headers: `response_headers` strips
    // content-length as hop-by-hop, but the declared size is exactly
    // what the pre-check needs.
    if let Some(content_length) = response
        .headers
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        && content_length > buffered_cap
    {
        // Declared larger than the cap: reject without reading.
        let finished = chrono::Utc::now();
        AttemptFinalizer::new(
            Arc::new(env.telemetry.clone()),
            env.request_id,
            env.started,
            env.runtime.failure_threshold,
            env.runtime.circuit_open_seconds,
        )
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
            0,
            Some(env.upstream_protocol.into()),
            Some(env.upstream_model.into()),
            true,
        );
        return NonStreamResult::Respond(gateway_error(
            env.entry_protocol,
            StatusCode::BAD_GATEWAY,
            "upstream_response_too_large",
            "Upstream response exceeds the configured buffered-body limit.",
            env.request_id,
        ));
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
            // Cap hit mid-body: stop reading, never convert a
            // partial JSON body.
            let finished = chrono::Utc::now();
            AttemptFinalizer::new(
                Arc::new(env.telemetry.clone()),
                env.request_id,
                env.started,
                env.runtime.failure_threshold,
                env.runtime.circuit_open_seconds,
            )
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
                true,
            );
            return NonStreamResult::Respond(gateway_error(
                env.entry_protocol,
                StatusCode::BAD_GATEWAY,
                "upstream_response_too_large",
                "Upstream response exceeds the configured buffered-body limit.",
                env.request_id,
            ));
        }
        Err(kind) => {
            // Transport failure mid-body: reset carries its own
            // attempt event (the tail maps it to 502); the total
            // timeout classifies as BodyTimeout (504).
            let (failure, error_kind) = match kind {
                TransportFailure::ConnectionReset => {
                    (TransportFailure::ConnectionReset, "transport_error")
                }
                _ => (TransportFailure::BodyTimeout, "timeout"),
            };
            let finished = chrono::Utc::now();

            AttemptFinalizer::new(
                Arc::new(env.telemetry.clone()),
                env.request_id,
                env.started,
                env.runtime.failure_threshold,
                env.runtime.circuit_open_seconds,
            )
            .finalize(
                env.candidate,
                env.attempts,
                env.attempt_started,
                finished,
                AttemptOutcome::TransportError,
                None,
                Some(error_kind.into()),
                env.attempts < env.candidates_len,
                false,
                None,
                None,
                Usage::default(),
                0,
                Some(env.upstream_protocol.into()),
                Some(env.upstream_model.into()),
                true,
            );
            return NonStreamResult::FailOver(Some(failure));
        }
    };
    // Mapped conversion REQUIRES the plaintext: a decode failure is
    // terminal (P1-2) — a stable gateway error, never a truncated
    // body handed to the converter. Non-mapped responses only need
    // best-effort observability, so their decode is soft.
    let decoded = if env.mapping.is_some() {
        let mut decoder = compression::RequiredDecoder::new(
            compression::ContentDecoder::from_encoding(response_headers.get("content-encoding")),
            buffered_cap,
        );
        match decoder.feed_required(&raw) {
            Ok(decoded) if decoder.finished() => decoded,
            Ok(_) => {
                // The body ended mid-frame: the plaintext is
                // incomplete and cannot be converted.
                tracing::warn!(
                    request_id = %env.request_id,
                    channel_id = %env.candidate.channel_id,
                    "mapped response truncated mid-frame"
                );
                decode_failure(&env, env.attempts < env.candidates_len, raw.len() as i64);
                return NonStreamResult::Respond(gateway_error(
                    env.entry_protocol,
                    StatusCode::BAD_GATEWAY,
                    "upstream_decode_error",
                    "Upstream response could not be decoded.",
                    env.request_id,
                ));
            }
            Err(error) => {
                tracing::warn!(
                    request_id = %env.request_id,
                    channel_id = %env.candidate.channel_id,
                    error = ?error,
                    "mapped response decode failed"
                );
                decode_failure(&env, env.attempts < env.candidates_len, raw.len() as i64);
                return NonStreamResult::Respond(gateway_error(
                    env.entry_protocol,
                    StatusCode::BAD_GATEWAY,
                    "upstream_decode_error",
                    "Upstream response could not be decoded.",
                    env.request_id,
                ));
            }
        }
    } else {
        let mut decoder = compression::ObservableDecoder::new(
            compression::ContentDecoder::from_encoding(response_headers.get("content-encoding")),
        );
        decoder.feed_observable(&raw)
    };
    let usage = usage_from_body(env.upstream_protocol, &decoded);
    let (result_body, conversion_failed, mapped) = if let Some(value) = env.mapping {
        match convert::convert_response(
            &value.entry,
            &value.upstream_protocol,
            env.entry_model,
            &decoded,
        ) {
            Ok(converted) => (converted, false, true),
            Err(error) => {
                // Conversion failure produces a gateway error body in
                // the entry format (Python `_mapped_error_body`); the
                // HTTP status stays 200 like the Python gateway. The
                // message is a fixed string — internal conversion
                // text must not leak to the client (P1-8).
                tracing::error!(request_id = %env.request_id, error = %error, "response conversion failed");
                let payload = if env.entry_protocol == "claude" {
                    json!({"type": "error", "error": {"type": "gateway_error", "message": "Response conversion failed"}})
                } else {
                    json!({"error": {"message": "Response conversion failed", "type": "gateway_error", "code": "conversion_error"}})
                };
                (
                    serde_json::to_vec(&payload).unwrap_or_else(|_| raw.clone()),
                    true,
                    true,
                )
            }
        }
    } else {
        // Non-mapped: forward the raw bytes with the original
        // headers (content-length included — the body is complete).
        (raw, false, false)
    };
    let mut result = response_with_headers(status, response_headers, result_body.clone());
    if mapped {
        // The converted body is plaintext; the original length and
        // encoding no longer describe it.
        result.headers_mut().remove("content-length");
        result.headers_mut().remove("content-encoding");
        result
            .headers_mut()
            .insert("content-type", HeaderValue::from_static("application/json"));
    }
    let finished_at = chrono::Utc::now();
    // P1-5: one outcome drives all three events. A conversion failure
    // is a gateway error — never success — with no usage and no
    // ChannelSuccess; the channel verdict stays untouched.
    let (outcome, error_kind, usage, countable): (AttemptOutcome, Option<String>, Usage, bool) =
        if conversion_failed {
            (
                AttemptOutcome::GatewayError,
                Some("conversion_error".into()),
                Usage::default(),
                false,
            )
        } else {
            (AttemptOutcome::Success, None, usage, false)
        };
    AttemptFinalizer::new(
        Arc::new(env.telemetry.clone()),
        env.request_id,
        env.started,
        env.runtime.failure_threshold,
        env.runtime.circuit_open_seconds,
    )
    .finalize(
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
        result_body.len() as i64,
        Some(env.upstream_protocol.into()),
        Some(env.upstream_model.into()),
        countable,
    );
    NonStreamResult::Respond(result)
}

/// Outcome of a remote-compaction attempt.
enum CompactionResult {
    Respond(Response<Body>),
    FailOver(Option<TransportFailure>),
    /// The attempt failed with an upstream HTTP error body that should be
    /// replayed if no further candidate succeeds.
    FailOverError(StatusCode, axum::http::HeaderMap, Vec<u8>, String),
}

/// Best-effort persistence of a runtime-confirmed remote-compaction capability.
async fn update_compaction_capability(
    db: &Database,
    channel_id: &str,
    mode: CompactionMode,
    supported: bool,
) {
    let column = match mode {
        CompactionMode::V1 => "remote_compaction_v1_support",
        CompactionMode::V2 => "remote_compaction_v2_support",
    };
    let value = if supported {
        CompactionSupport::Supported.as_db()
    } else {
        CompactionSupport::Unsupported.as_db()
    };
    let now = chrono::Utc::now().to_rfc3339();
    let sql = format!(
        "UPDATE channel_protocols SET {column} = ?, remote_compaction_probed_at = ? \
         WHERE channel_id = ? AND protocol = 'openai_responses'"
    );
    if let Err(error) = sqlx::query(&sql)
        .bind(value)
        .bind(&now)
        .bind(channel_id)
        .execute(db.pool())
        .await
    {
        tracing::warn!(
            channel_id,
            mode = mode.as_str(),
            %error,
            "failed to persist remote compaction capability"
        );
    }
}

/// Handles one remote-compaction upstream response. Both V1 and V2 validate
/// the full body before returning bytes to the client so a bad upstream can
/// still fail over to the next candidate.
async fn compaction_attempt(
    db: &Database,
    env: AttemptEnv<'_>,
    mode: CompactionMode,
    response: crate::ports::UpstreamResponse,
    status: axum::http::StatusCode,
    response_headers: axum::http::HeaderMap,
) -> CompactionResult {
    if !status.is_success() {
        return compaction_error_attempt(db, env, mode, response, status, response_headers).await;
    }
    match mode {
        CompactionMode::V1 => compaction_v1_success(db, env, response, status, response_headers).await,
        CompactionMode::V2 => compaction_v2_success(db, env, response, status, response_headers).await,
    }
}

async fn compaction_error_attempt(
    db: &Database,
    env: AttemptEnv<'_>,
    mode: CompactionMode,
    response: crate::ports::UpstreamResponse,
    status: axum::http::StatusCode,
    response_headers: axum::http::HeaderMap,
) -> CompactionResult {
    let (raw_result, truncated) = read_bounded_body(
        response.body.into_stream(),
        Duration::from_secs(env.runtime.first_byte_timeout_seconds.max(1) as u64),
        env.runtime.max_buffered_upstream_body_mb.max(1) as usize * 1024 * 1024,
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
        update_compaction_capability(db, &env.candidate.channel_id, mode, false).await;
    }

    // Remote-compaction attempts never participate in the channel circuit
    // breaker: the gateway may deliberately fall through unsupported
    // compaction modes without penalizing the provider's ordinary traffic.
    let kind = if capability_rejected {
        "remote_compaction_unsupported"
    } else {
        status_kind(status).0
    };
    let finished = env.clock.now_utc();
    AttemptFinalizer::new(
        Arc::new(env.telemetry.clone()),
        env.request_id,
        env.started,
        env.runtime.failure_threshold,
        env.runtime.circuit_open_seconds,
    )
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

async fn compaction_v1_success(
    db: &Database,
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
            AttemptFinalizer::new(
                Arc::new(env.telemetry.clone()),
                env.request_id,
                env.started,
                env.runtime.failure_threshold,
                env.runtime.circuit_open_seconds,
            )
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
            return CompactionResult::FailOver(None);
        }
        Err(kind) => {
            let finished = env.clock.now_utc();
            AttemptFinalizer::new(
                Arc::new(env.telemetry.clone()),
                env.request_id,
                env.started,
                env.runtime.failure_threshold,
                env.runtime.circuit_open_seconds,
            )
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
        AttemptFinalizer::new(
            Arc::new(env.telemetry.clone()),
            env.request_id,
            env.started,
            env.runtime.failure_threshold,
            env.runtime.circuit_open_seconds,
        )
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
        return CompactionResult::FailOver(None);
    }

    update_compaction_capability(db, &env.candidate.channel_id, CompactionMode::V1, true).await;

    let finished = env.clock.now_utc();
    AttemptFinalizer::new(
        Arc::new(env.telemetry.clone()),
        env.request_id,
        env.started,
        env.runtime.failure_threshold,
        env.runtime.circuit_open_seconds,
    )
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

    let mut result = response_with_headers(status, response_headers, decoded);
    result.headers_mut().remove("content-encoding");
    result.headers_mut().remove("content-length");
    result.headers_mut().insert(
        "content-type",
        HeaderValue::from_static("application/json"),
    );
    CompactionResult::Respond(result)
}

async fn compaction_v2_success(
    db: &Database,
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
            AttemptFinalizer::new(
                Arc::new(env.telemetry.clone()),
                env.request_id,
                env.started,
                env.runtime.failure_threshold,
                env.runtime.circuit_open_seconds,
            )
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
            return CompactionResult::FailOver(None);
        }
        Err(kind) => {
            let finished = env.clock.now_utc();
            AttemptFinalizer::new(
                Arc::new(env.telemetry.clone()),
                env.request_id,
                env.started,
                env.runtime.failure_threshold,
                env.runtime.circuit_open_seconds,
            )
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

    let mut validator = CompactionV2Validator::new();
    validator.feed(&decoded);
    let validation = validator.finish();

    let (outcome, error_kind, status_code, countable, usage) = match validation {
        Ok(()) => {
            update_compaction_capability(db, &env.candidate.channel_id, CompactionMode::V2, true)
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
            )
        }
        Err(remote_compaction::CompactionV2Error::NotCompaction) => {
            update_compaction_capability(db, &env.candidate.channel_id, CompactionMode::V2, false)
                .await;
            (
                AttemptOutcome::UpstreamError,
                Some("remote_compaction_unsupported".into()),
                502,
                false,
                Usage::default(),
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
            )
        }
    };

    let finished = env.clock.now_utc();
    AttemptFinalizer::new(
        Arc::new(env.telemetry.clone()),
        env.request_id,
        env.started,
        env.runtime.failure_threshold,
        env.runtime.circuit_open_seconds,
    )
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
        return CompactionResult::FailOver(None);
    }

    let mut result = response_with_headers(status, response_headers, decoded);
    result.headers_mut().remove("content-encoding");
    result.headers_mut().remove("content-length");
    result.headers_mut().insert(
        "content-type",
        HeaderValue::from_static("text/event-stream"),
    );
    CompactionResult::Respond(result)
}

/// Shared lossless decode for mapped compaction responses. On failure it
/// finalizes the attempt and returns the appropriate `CompactionResult`.
fn decode_mapped_response(
    env: &AttemptEnv<'_>,
    content_encoding: Option<&axum::http::HeaderValue>,
    raw: &[u8],
    buffered_cap: usize,
) -> Result<Vec<u8>, CompactionResult> {
    let mut decoder = compression::RequiredDecoder::new(
        compression::ContentDecoder::from_encoding(content_encoding),
        buffered_cap,
    );
    match decoder.feed_required(raw) {
        Ok(decoded) if decoder.finished() => Ok(decoded),
        Ok(_) => {
            let finished = env.clock.now_utc();
            AttemptFinalizer::new(
                Arc::new(env.telemetry.clone()),
                env.request_id,
                env.started,
                env.runtime.failure_threshold,
                env.runtime.circuit_open_seconds,
            )
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
            Err(CompactionResult::FailOver(None))
        }
        Err(_) => {
            let finished = env.clock.now_utc();
            AttemptFinalizer::new(
                Arc::new(env.telemetry.clone()),
                env.request_id,
                env.started,
                env.runtime.failure_threshold,
                env.runtime.circuit_open_seconds,
            )
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
            Err(CompactionResult::FailOver(None))
        }
    }
}

/// Final gateway response after every candidate failed (P2-4): replays the
/// last upstream error, classifies pure-transport failures into 502/504,
/// and writes the request terminal telemetry.
#[allow(clippy::too_many_arguments)]
fn final_gateway_response(
    telemetry: &Telemetry,
    clock: &dyn crate::ports::Clock,
    entry_protocol: &str,
    request_id: &str,
    started: chrono::DateTime<chrono::Utc>,
    attempts: i64,
    mapping: Option<&routing::MappingTarget>,
    last_error: Option<(StatusCode, axum::http::HeaderMap, Vec<u8>, String)>,
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

/// Proxy service (P2-1): the gateway request orchestration, reached by the
/// entry handlers through `Context::proxy`. All upstream/route access goes
/// through the ports inside its context; the pipeline stages live in the
/// policy builders (`prepare_request`, `transparent_stream`,
/// `mapped_stream`, `bounded_non_stream`, `final_gateway_response`).
pub struct ProxyService {
    db: Database,
    secrets: SecretStore,
    http: Arc<dyn crate::ports::UpstreamClient>,
    routes: Arc<dyn crate::ports::RouteRepository>,
    telemetry: Telemetry,
    clock: Arc<dyn crate::ports::Clock>,
    limits: Arc<RuntimeLimits>,
    notifier: Arc<dyn crate::ports::Notifier>,
}

impl ProxyService {
    pub fn new(
        db: Database,
        secrets: SecretStore,
        http: Arc<dyn crate::ports::UpstreamClient>,
        routes: Arc<dyn crate::ports::RouteRepository>,
        telemetry: Telemetry,
        clock: Arc<dyn crate::ports::Clock>,
        limits: Arc<RuntimeLimits>,
        notifier: Arc<dyn crate::ports::Notifier>,
    ) -> Arc<Self> {
        Arc::new(Self {
            db,
            secrets,
            http,
            routes,
            telemetry,
            clock,
            limits,
            notifier,
        })
    }

    /// Full proxy pipeline for one entry protocol (P2-4).
    pub async fn proxy(
        &self,
        request: Request,
        entry_protocol: &str,
        fixed_path: Option<String>,
    ) -> Response<Body> {
        let request_id = uuid::Uuid::new_v4().to_string();
        let started = self.clock.now_utc();
        // P2-4: preparation (settings, auth, mapping, candidates) is its own
        // phase; failures short-circuit here with a gateway error.
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
            mapping,
            upstream_protocol,
            upstream_model,
            converted_body,
            candidates,
            compaction_mode,
        } = prepared;
        let upstream_protocol = upstream_protocol.as_str();
        let upstream_model = upstream_model.as_str();
        let query = query.as_deref();

        let mut last_error: Option<(StatusCode, axum::http::HeaderMap, Vec<u8>, String)> = None;
        // Tracks the kind of the last pure-transport failure (no upstream status
        // was ever received) so the final gateway error can distinguish 504
        // timeouts from 502 unreachability.
        let mut last_transport_kind: Option<TransportFailure> = None;
        // Lazily resolved fallback for the OpenCode Zen/Go session header;
        // only requests routed to such an upstream ever read it.
        let mut opencode_session_id: Option<String> = None;
        let mut attempts = 0i64;
        for (index, candidate) in candidates.iter().enumerate() {
            attempts = index as i64 + 1;
            // Entering a backup candidate means the previous one failed:
            // alert the user. Remote-compaction failover is silent by design
            // (P2-remote-compaction): the Codex client itself retries/falls
            // back through the gateway, so we do not send a desktop notice.
            // The notifier queues and coalesces the notice (1s window, P2-1) —
            // this path never blocks. `last_transport_kind` and `last_error`
            // are mutually exclusive by construction (each failure path clears
            // the other), so the surviving one describes the previous
            // candidate's failure.
            if index > 0 && compaction_mode.is_none() {
                let error_kind = last_transport_kind
                    .map(|kind| kind.as_str().to_owned())
                    .or_else(|| {
                        last_error
                            .as_ref()
                            .map(|(status, _, _, _)| format!("HTTP {}", status.as_u16()))
                    });
                self.notifier.notify_failover(crate::ports::FailoverNotice {
                    model_id: entry_model.clone(),
                    failed_channel_name: candidates[index - 1].channel_name.clone(),
                    next_channel_name: candidate.channel_name.clone(),
                    error_kind,
                });
            }
            let attempt_started = self.clock.now_utc();
            // Absolute anchor for first-token accounting: the deadline covers the
            // whole attempt, so a slow response head cannot extend the window.
            let attempt_started_instant = tokio::time::Instant::now();
            let api_key = match self.secrets.decrypt(&candidate.api_key_encrypted) {
                Ok(value) => value,
                Err(error) => {
                    let finished = self.clock.now_utc();
                    last_error = None;
                    last_transport_kind = Some(TransportFailure::ConnectionReset);
                    AttemptFinalizer::new(
                        Arc::new(self.telemetry.clone()),
                        &request_id,
                        started,
                        runtime.failure_threshold,
                        runtime.circuit_open_seconds,
                    )
                    .finalize(
                        candidate,
                        attempts,
                        attempt_started,
                        finished,
                        AttemptOutcome::TransportError,
                        None,
                        Some("key_decrypt_error".into()),
                        attempts < candidates.len() as i64,
                        false,
                        None,
                        None,
                        Usage::default(),
                        0,
                        Some(upstream_protocol.into()),
                        Some(upstream_model.into()),
                        compaction_mode.is_none(),
                    );
                    let _ = error;
                    continue;
                }
            };
            let target_path = mapping
                .as_ref()
                .map(|value| {
                    mapped_path(
                        value,
                        &path,
                        stream_requested,
                        &value.upstream_model,
                        compaction_mode,
                    )
                })
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
                    last_transport_kind = Some(TransportFailure::ConnectionReset);
                    let _ = error;
                    continue;
                }
            };
            let mut outbound =
                match protocol::outbound_headers(&headers, upstream_protocol, &api_key) {
                    Ok(value) => value,
                    Err(error) => {
                        last_error = None;
                        last_transport_kind = Some(TransportFailure::ConnectionReset);
                        let _ = error;
                        continue;
                    }
                };
            if protocol::requires_opencode_session(&candidate.base_url) {
                if opencode_session_id.is_none() {
                    opencode_session_id =
                        Some(crate::settings::opencode_session_id(&self.db).await);
                }
                let session_id = opencode_session_id.as_deref().unwrap_or_default();
                if let Err(error) =
                    protocol::apply_opencode_session(&mut outbound, &candidate.base_url, session_id)
                {
                    tracing::warn!(%error, "opencode session header skipped");
                }
            }
            // P2-1: the upstream port applies connect timeout and the send
            // deadline; the classified error decides the transport kind.
            let response = match self
                .http
                .send(crate::ports::UpstreamRequest {
                    url: target_url,
                    headers: outbound,
                    body: Some(converted_body.clone()),
                    connect_timeout: Duration::from_secs(
                        runtime.connect_timeout_seconds.max(1) as u64
                    ),
                    deadline: Duration::from_secs(runtime.first_byte_timeout_seconds.max(1) as u64),
                })
                .await
            {
                Ok(response) => response,
                Err(crate::ports::UpstreamError::ConnectTimeout) => {
                    let finished = self.clock.now_utc();
                    last_error = None;
                    last_transport_kind = Some(TransportFailure::ConnectTimeout);
                    AttemptFinalizer::new(
                        Arc::new(self.telemetry.clone()),
                        &request_id,
                        started,
                        runtime.failure_threshold,
                        runtime.circuit_open_seconds,
                    )
                    .finalize(
                        candidate,
                        attempts,
                        attempt_started,
                        finished,
                        AttemptOutcome::TransportError,
                        None,
                        Some("connect_timeout".into()),
                        attempts < candidates.len() as i64,
                        false,
                        None,
                        None,
                        Usage::default(),
                        0,
                        Some(upstream_protocol.into()),
                        Some(upstream_model.into()),
                        compaction_mode.is_none(),
                    );
                    continue;
                }
                Err(crate::ports::UpstreamError::Transport(error)) => {
                    let finished = self.clock.now_utc();
                    last_error = None;
                    last_transport_kind = Some(TransportFailure::ConnectionReset);
                    AttemptFinalizer::new(
                        Arc::new(self.telemetry.clone()),
                        &request_id,
                        started,
                        runtime.failure_threshold,
                        runtime.circuit_open_seconds,
                    )
                    .finalize(
                        candidate,
                        attempts,
                        attempt_started,
                        finished,
                        AttemptOutcome::TransportError,
                        None,
                        Some(error),
                        attempts < candidates.len() as i64,
                        false,
                        None,
                        None,
                        Usage::default(),
                        0,
                        Some(upstream_protocol.into()),
                        Some(upstream_model.into()),
                        compaction_mode.is_none(),
                    );
                    continue;
                }
                Err(crate::ports::UpstreamError::Deadline) => {
                    let finished = self.clock.now_utc();
                    last_error = None;
                    last_transport_kind = Some(TransportFailure::FirstByteTimeout);
                    AttemptFinalizer::new(
                        Arc::new(self.telemetry.clone()),
                        &request_id,
                        started,
                        runtime.failure_threshold,
                        runtime.circuit_open_seconds,
                    )
                    .finalize(
                        candidate,
                        attempts,
                        attempt_started,
                        finished,
                        AttemptOutcome::TransportError,
                        None,
                        Some("timeout".into()),
                        attempts < candidates.len() as i64,
                        false,
                        None,
                        None,
                        Usage::default(),
                        0,
                        Some(upstream_protocol.into()),
                        Some(upstream_model.into()),
                        compaction_mode.is_none(),
                    );
                    continue;
                }
            };
            let status = response.status;
            let response_headers = protocol::response_headers(&response.headers);
            if let Some(mode) = compaction_mode {
                // Remote compaction has its own builders: V1 is unary and V2 is
                // buffered/validated before any client bytes are emitted so a
                // malformed or unsupported upstream response can still fail over.
                let env = AttemptEnv {
                    telemetry: &self.telemetry,
                    clock: Arc::clone(&self.clock),
                    request_id: &request_id,
                    started,
                    candidate,
                    attempts,
                    attempt_started,
                    attempt_started_instant,
                    runtime: &runtime,
                    entry_protocol,
                    entry_model: &entry_model,
                    upstream_protocol,
                    upstream_model,
                    mapping: mapping.as_ref(),
                    candidates_len: candidates.len() as i64,
                };
                match compaction_attempt(
                    &self.db,
                    env,
                    mode,
                    response,
                    status,
                    response_headers,
                )
                .await
                {
                    CompactionResult::Respond(result) => return result,
                    CompactionResult::FailOver(transport) => {
                        last_transport_kind = transport;
                        last_error = None;
                        continue;
                    }
                    CompactionResult::FailOverError(error_status, error_headers, raw, channel_id) => {
                        last_transport_kind = None;
                        last_error = Some((error_status, error_headers, raw, channel_id));
                        continue;
                    }
                }
            }
            if status.is_success() {
                if stream_requested && mapping.is_none() {
                    // P2-4: transparent forwarding lives in its own builder.
                    let env = AttemptEnv {
                        telemetry: &self.telemetry,
                        clock: Arc::clone(&self.clock),
                        request_id: &request_id,
                        started,
                        candidate,
                        attempts,
                        attempt_started,
                        attempt_started_instant,
                        runtime: &runtime,
                        entry_protocol,
                        entry_model: &entry_model,
                        upstream_protocol,
                        upstream_model,
                        mapping: mapping.as_ref(),
                        candidates_len: candidates.len() as i64,
                    };
                    return transparent_stream(env, response, status, response_headers);
                }
                // C2: mapped entries stream through an incremental converter. A
                // short prelude is buffered first so a 200 response whose body is
                // actually an upstream error can still fail over, and
                // `first_token_timeout_seconds` is honored like the Python
                // gateway. The remaining body is converted event-by-event so the
                // client sees a live stream.
                if stream_requested {
                    // P2-4: the mapped stream pipeline (prelude + incremental
                    // conversion) lives in its own builder; prelude failures
                    // fail over with their transport classification.
                    let env = AttemptEnv {
                        telemetry: &self.telemetry,
                        clock: Arc::clone(&self.clock),
                        request_id: &request_id,
                        started,
                        candidate,
                        attempts,
                        attempt_started,
                        attempt_started_instant,
                        runtime: &runtime,
                        entry_protocol,
                        entry_model: &entry_model,
                        upstream_protocol,
                        upstream_model,
                        mapping: mapping.as_ref(),
                        candidates_len: candidates.len() as i64,
                    };
                    match mapped_stream(env, response, status, response_headers).await {
                        MappedStreamResult::Respond(result) => return result,
                        MappedStreamResult::FailOver(transport) => {
                            last_transport_kind = transport;
                            last_error = None;
                            continue;
                        }
                    }
                }
                let env = AttemptEnv {
                    telemetry: &self.telemetry,
                    clock: Arc::clone(&self.clock),
                    request_id: &request_id,
                    started,
                    candidate,
                    attempts,
                    attempt_started,
                    attempt_started_instant,
                    runtime: &runtime,
                    entry_protocol,
                    entry_model: &entry_model,
                    upstream_protocol,
                    upstream_model,
                    mapping: mapping.as_ref(),
                    candidates_len: candidates.len() as i64,
                };
                match bounded_non_stream(env, response, status, response_headers).await {
                    NonStreamResult::Respond(result) => return result,
                    NonStreamResult::FailOver(transport) => {
                        last_transport_kind = transport;
                        last_error = None;
                        continue;
                    }
                }
            }
            // P1-1: error bodies are buffered only for replay/conversion and are
            // capped at 1 MiB; beyond that the body is truncated and recorded.
            let (raw_result, truncated) = read_bounded_body(
                response.body.into_stream(),
                Duration::from_secs(runtime.first_byte_timeout_seconds.max(1) as u64),
                self.limits.error_body_max,
            )
            .await;
            let raw = match raw_result {
                Ok(raw) => raw,
                Err(TransportFailure::ConnectionReset) => {
                    last_transport_kind = Some(TransportFailure::ConnectionReset);
                    Vec::new()
                }
                Err(_) => {
                    last_transport_kind = Some(TransportFailure::FirstByteTimeout);
                    Vec::new()
                }
            };
            if truncated {
                tracing::warn!(
                    request_id = %request_id,
                    channel_id = %candidate.channel_id,
                    status = %status,
                    bytes_captured = self.limits.error_body_max,
                    body_truncated = true,
                    "upstream error body truncated at the error-body cap"
                );
            }
            // The upstream answered with an error status: this failure is
            // described by `last_error`, not by any earlier transport kind.
            last_transport_kind = None;
            let (kind, countable) = status_kind(status);
            let finished = self.clock.now_utc();
            AttemptFinalizer::new(
                Arc::new(self.telemetry.clone()),
                &request_id,
                started,
                runtime.failure_threshold,
                runtime.circuit_open_seconds,
            )
            .finalize(
                candidate,
                attempts,
                attempt_started,
                finished,
                AttemptOutcome::UpstreamError,
                Some(status.as_u16() as i64),
                Some(kind.into()),
                attempts < candidates.len() as i64,
                false,
                None,
                None,
                Usage::default(),
                raw.len() as i64,
                Some(upstream_protocol.into()),
                Some(upstream_model.into()),
                countable,
            );
            last_error = Some((status, response_headers, raw, candidate.channel_id.clone()));
        }
        final_gateway_response(
            &self.telemetry,
            self.clock.as_ref(),
            entry_protocol,
            &request_id,
            started,
            attempts,
            mapping.as_ref(),
            last_error,
            last_transport_kind,
        )
    }
}

async fn normal(
    State(state): State<AppContext>,
    request: Request,
    protocol: &'static str,
) -> Response<Body> {
    state.proxy.proxy(request, protocol, None).await
}

pub async fn openai(State(state): State<AppContext>, request: Request) -> Response<Body> {
    normal(State(state), request, "openai_compatible").await
}
pub async fn responses(State(state): State<AppContext>, request: Request) -> Response<Body> {
    normal(State(state), request, "openai_responses").await
}
pub async fn responses_compact(
    State(state): State<AppContext>,
    request: Request,
) -> Response<Body> {
    normal(State(state), request, "openai_responses").await
}
pub async fn claude(State(state): State<AppContext>, request: Request) -> Response<Body> {
    normal(State(state), request, "claude").await
}
pub async fn gemini(
    State(state): State<AppContext>,
    Path(_action): Path<String>,
    request: Request,
) -> Response<Body> {
    normal(State(state), request, "gemini").await
}
pub async fn claudecode(State(state): State<AppContext>, request: Request) -> Response<Body> {
    normal(State(state), request, "claude").await
}
pub async fn codex(State(state): State<AppContext>, request: Request) -> Response<Body> {
    normal(State(state), request, "openai_responses").await
}
pub async fn codex_compact(
    State(state): State<AppContext>,
    request: Request,
) -> Response<Body> {
    normal(State(state), request, "openai_responses").await
}

/// Builds the `x_local_gateway` metadata for one catalog item: the union of
/// every endpoint the model is currently routable through (ordered by
/// `PROTOCOL_ORDER`), plus capability data (context window, max tokens,
/// reasoning, image input, costs) and the pi model config so catalog consumers
/// such as the omp extension can use real values instead of their built-in
/// defaults. Capabilities are detected on the fly for `auto` rows (C5); models
/// without any capability data omit the field entirely.
async fn gateway_metadata(
    state: &AppContext,
    item: &RoutableModel,
) -> Result<Option<Value>, Response<Body>> {
    let mut gateway = json!({});
    match state.routes.routable_endpoints_for_model(&item.id).await {
        Ok(endpoints) => {
            if !endpoints.is_empty() {
                gateway["supported_endpoints"] = json!(endpoints);
            }
        }
        Err(error) => {
            tracing::error!(error = %error, "model endpoints query failed");
            return Err(gateway_error(
                "catalog",
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "Database error",
                "catalog",
            ));
        }
    }
    match capabilities::get_model_caps(state, &item.id).await {
        Ok(caps) => {
            if capabilities::has_capability_data(&caps) {
                let pi_config = capabilities::pi_model_config(&caps);
                gateway["capabilities"] = caps;
                gateway["pi_model_config"] = pi_config;
            }
        }
        Err(error) => {
            tracing::error!(error = %error, "model caps query failed");
            return Err(gateway_error(
                "catalog",
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "Database error",
                "catalog",
            ));
        }
    }
    Ok(if gateway
        .as_object()
        .is_some_and(|object| !object.is_empty())
    {
        Some(gateway)
    } else {
        None
    })
}

/// OpenAI list-form catalog entries (id/object/owned_by/created) with the
/// shared `x_local_gateway` metadata; used by the per-protocol catalogs and
/// the `/v1/models` aggregate alike.
async fn openai_catalog_items(
    state: &AppContext,
    items: &[RoutableModel],
) -> Result<Vec<Value>, Response<Body>> {
    let mut data = Vec::new();
    for item in items {
        let mut value = json!({
            "id": item.id,
            "object": "model",
            "owned_by": "local-gateway",
            "created": item.created_at,
        });
        match gateway_metadata(state, item).await {
            Ok(Some(metadata)) => value["x_local_gateway"] = metadata,
            Ok(None) => {}
            Err(response) => return Err(response),
        }
        data.push(value);
    }
    Ok(data)
}

/// Per-protocol model catalog. Each protocol answers with its native list
/// shape: OpenAI list form (`/v1/responses/models`, protocol-selected
/// `/v1/models`), Claude list form (`/v1/messages/models`, `anthropic-version`
/// selection), or Gemini (`/v1beta/models`).
pub async fn models(
    State(state): State<AppContext>,
    request: Request,
    protocol: &str,
) -> Response<Body> {
    if !settings::authorize_gateway(&state, request.headers(), request.uri().query(), protocol)
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
    let items = match state.routes.list_routable_models(Some(protocol)).await {
        Ok(value) => value,
        Err(error) => {
            tracing::error!(error = %error, "model catalog query failed");
            return gateway_error(
                protocol,
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "Database error",
                "catalog",
            );
        }
    };
    match protocol {
        "gemini" => json_response(
            StatusCode::OK,
            json!({"models":items.iter().map(|item|json!({"name":format!("models/{}",item.id),"displayName":item.display_name,"supportedGenerationMethods":["generateContent"]})).collect::<Vec<_>>() }),
        ),
        "claude" => {
            let mut data = Vec::new();
            for item in &items {
                let mut value = json!({
                    "type": "model",
                    "id": item.id,
                    "display_name": item.display_name,
                    "created_at": item.created_at,
                });
                match gateway_metadata(&state, item).await {
                    Ok(Some(metadata)) => value["x_local_gateway"] = metadata,
                    Ok(None) => {}
                    Err(response) => return response,
                }
                data.push(value);
            }
            json_response(
                StatusCode::OK,
                json!({
                    "data": data,
                    "has_more": false,
                    "first_id": data.first().map(|value| value["id"].clone()).unwrap_or(Value::Null),
                    "last_id": data.last().map(|value| value["id"].clone()).unwrap_or(Value::Null),
                }),
            )
        }
        _ => match openai_catalog_items(&state, &items).await {
            Ok(data) => json_response(StatusCode::OK, json!({ "object": "list", "data": data })),
            Err(response) => response,
        },
    }
}

/// Aggregated catalog for `GET /v1/models` without an explicit protocol
/// selector: every enabled, routable model across all protocols, deduplicated
/// by model id (requirements.md:126-131).
pub async fn aggregate_models(
    State(state): State<AppContext>,
    request: Request,
) -> Response<Body> {
    if !settings::authorize_gateway(&state, request.headers(), request.uri().query(), "catalog")
        .await
        .unwrap_or(false)
    {
        return gateway_error(
            "catalog",
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Gateway access denied.",
            "catalog",
        );
    }
    let items = match state.routes.list_routable_models(None).await {
        Ok(value) => value,
        Err(error) => {
            tracing::error!(error = %error, "model catalog query failed");
            return gateway_error(
                "catalog",
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "Database error",
                "catalog",
            );
        }
    };
    match openai_catalog_items(&state, &items).await {
        Ok(data) => json_response(StatusCode::OK, json!({ "object": "list", "data": data })),
        Err(response) => response,
    }
}

pub async fn mapped_models(
    State(state): State<AppContext>,
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
    let items = match state.routes.list_mapping_models(entry).await {
        Ok(value) => value,
        Err(error) => {
            tracing::error!(error = %error, "mapping model catalog query failed");
            return gateway_error(
                entry,
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "Database error",
                "catalog",
            );
        }
    };
    json_response(
        StatusCode::OK,
        json!({"object":"list","data":items.iter().map(|item|json!({"id":item.id,"object":"model","owned_by":"local-gateway","created":item.created_at})).collect::<Vec<_>>() }),
    )
}

pub async fn claudecode_info(State(state): State<AppContext>, request: Request) -> Response<Body> {
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
pub async fn codex_info(State(state): State<AppContext>, request: Request) -> Response<Body> {
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

/// `GET /v1/models`. Protocol selection, in order: the `protocol` query
/// parameter, the `X-Local-Gateway-Protocol` header, or the
/// `anthropic-version` header (Claude SDKs) — each returns that protocol's
/// native catalog. Without any selector the catalog aggregates every enabled
/// routable model across all protocols (api-design.md:447).
pub async fn openai_models(State(state): State<AppContext>, request: Request) -> Response<Body> {
    let selected = request
        .uri()
        .query()
        .and_then(|query| {
            url::form_urlencoded::parse(query.as_bytes())
                .find(|(key, _)| key == "protocol")
                .map(|(_, value)| value.into_owned())
        })
        .or_else(|| {
            request
                .headers()
                .get("x-local-gateway-protocol")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        });
    if let Some(protocol) = selected.as_deref() {
        if matches!(protocol, "openai_compatible" | "openai_responses" | "claude") {
            return models(State(state), request, protocol).await;
        }
        return json_response(
            StatusCode::BAD_REQUEST,
            json!({"error":{"message":"Unsupported model catalog protocol."}}),
        );
    }
    if request.headers().contains_key("anthropic-version") {
        models(State(state), request, "claude").await
    } else {
        aggregate_models(State(state), request).await
    }
}
pub async fn responses_models(State(state): State<AppContext>, request: Request) -> Response<Body> {
    models(State(state), request, "openai_responses").await
}
pub async fn claude_models(State(state): State<AppContext>, request: Request) -> Response<Body> {
    models(State(state), request, "claude").await
}
pub async fn gemini_models(State(state): State<AppContext>, request: Request) -> Response<Body> {
    models(State(state), request, "gemini").await
}
pub async fn claudecode_models(
    State(state): State<AppContext>,
    request: Request,
) -> Response<Body> {
    mapped_models(State(state), request, "claude").await
}
pub async fn codex_models(State(state): State<AppContext>, request: Request) -> Response<Body> {
    mapped_models(State(state), request, "openai_responses").await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ports::FailoverNotice;
    use crate::{config::AppConfig, db::Database, telemetry::Telemetry};
    use futures_util::stream;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_util::sync::CancellationToken;

    /// In-memory Notifier recording every failover notice for assertions.
    #[derive(Clone, Default)]
    struct FakeNotifier(Arc<parking_lot::Mutex<Vec<FailoverNotice>>>);

    impl FakeNotifier {
        fn new() -> Self {
            Self::default()
        }
        fn notices(&self) -> Vec<FailoverNotice> {
            self.0.lock().clone()
        }
    }

    impl crate::ports::Notifier for FakeNotifier {
        fn notify_failover(&self, notice: FailoverNotice) {
            self.0.lock().push(notice);
        }
    }

    /// In-memory-free test scaffold: temp dir, real migrations, a seeded
    /// openai_compatible route pointing at a raw TCP upstream we control.
    struct TestGateway {
        db: Database,
        state: AppContext,
        secrets: SecretStore,
        notifier: FakeNotifier,
        writer_cancel: CancellationToken,
        writer: tokio::task::JoinHandle<()>,
        _dir: std::path::PathBuf,
    }

    impl TestGateway {
        /// Stop the writer task: release the telemetry sender first (the
        /// writer only exits once the channel closes), then join it.
        async fn shutdown(self) {
            drop(self.state);
            self.writer_cancel.cancel();
            let _ = self.writer.await;
        }
    }

    async fn temp_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("lagw-proxy-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    async fn test_gateway(upstream_port: u16, extra_settings: &[(&str, &str)]) -> TestGateway {
        test_gateway_inner(upstream_port, extra_settings, false, false).await
    }

    /// Same scaffold plus a claude model mapping (`mapped-model` ->
    /// openai_compatible `test-model`) so requests through
    /// `/claudecode/v1/messages` take the mapped stream pipeline.
    async fn test_gateway_mapped(
        upstream_port: u16,
        extra_settings: &[(&str, &str)],
    ) -> TestGateway {
        test_gateway_inner(upstream_port, extra_settings, true, false).await
    }

    /// Same scaffold plus a second, claude-protocol route (`claude-model`
    /// through the same channel model) so catalog aggregation covers more
    /// than the openai family.
    async fn test_gateway_claude(
        upstream_port: u16,
        extra_settings: &[(&str, &str)],
    ) -> TestGateway {
        test_gateway_inner(upstream_port, extra_settings, false, true).await
    }

    async fn test_gateway_inner(
        upstream_port: u16,
        extra_settings: &[(&str, &str)],
        mapped: bool,
        claude_route: bool,
    ) -> TestGateway {
        let dir = temp_dir().await;
        let db = Database::open(&dir.join("test.db")).await.unwrap();
        let secrets = crate::crypto::SecretStore::load(&dir.join("master.key"))
            .await
            .unwrap();
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("INSERT INTO providers(id,name,base_url,created_at,updated_at) VALUES('prov-1','mock',?,?,?)")
            .bind(format!("http://127.0.0.1:{upstream_port}"))
            .bind(time)
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channels(id,provider_id,name,protocol,api_key_encrypted,api_key_hint,manual_enabled,created_at,updated_at) VALUES('ch-1','prov-1','chan','openai_compatible',?,?,1,?,?)")
            .bind(secrets.encrypt("test-key"))
            .bind("...key")
            .bind(time)
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_models(id,channel_id,model_id,display_name,source,available,first_seen_at,created_at,updated_at) VALUES('cm-1','ch-1','test-model','Test Model','discovered',1,?,?,?)")
            .bind(time)
            .bind(time)
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES('cm-1','openai_compatible')")
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO model_routes(id,protocol,requested_model_id,enabled,created_at,updated_at) VALUES('route-1','openai_compatible','test-model',1,?,?)")
            .bind(time)
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO route_candidates(id,route_id,channel_model_id,priority,enabled,created_at,updated_at) VALUES('rc-1','route-1','cm-1',1,1,?,?)")
            .bind(time)
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_health(channel_id,state,consecutive_failures,updated_at) VALUES('ch-1','active',0,?)")
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
        if mapped {
            sqlx::query("INSERT INTO claude_model_mappings(id,claude_model_id,display_name,upstream_protocol,upstream_model_id,enabled,created_at,updated_at) VALUES('map-1','mapped-model','Mapped','openai_compatible','test-model',1,?,?)")
                .bind(time)
                .bind(time)
                .execute(db.pool())
                .await
                .unwrap();
        }
        if claude_route {
            sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES('cm-1','claude')")
                .execute(db.pool())
                .await
                .unwrap();
            sqlx::query("INSERT INTO model_routes(id,protocol,requested_model_id,enabled,created_at,updated_at) VALUES('route-2','claude','claude-model',1,?,?)")
                .bind(time)
                .bind(time)
                .execute(db.pool())
                .await
                .unwrap();
            sqlx::query("INSERT INTO route_candidates(id,route_id,channel_model_id,priority,enabled,created_at,updated_at) VALUES('rc-2','route-2','cm-1',1,1,?,?)")
                .bind(time)
                .bind(time)
                .execute(db.pool())
                .await
                .unwrap();
        }
        for (key, raw) in extra_settings {
            sqlx::query("INSERT INTO settings(key,value_json,updated_at) VALUES(?,?,?)")
                .bind(key)
                .bind(raw)
                .bind(time)
                .execute(db.pool())
                .await
                .unwrap();
        }
        let (telemetry, rx) = Telemetry::new(1000);
        let cancel = CancellationToken::new();
        let writer = tokio::spawn(Telemetry::run_writer(
            db.clone(),
            rx,
            cancel.clone(),
            telemetry.dropped_handle(),
        ));
        let http: Arc<dyn crate::ports::UpstreamClient> =
            Arc::new(crate::infrastructure::HttpClientPool::default());
        let routes: Arc<dyn crate::ports::RouteRepository> =
            crate::infrastructure::SqliteRouteRepository::new(db.clone());
        let channels: Arc<dyn crate::ports::ChannelRepository> =
            crate::infrastructure::SqliteChannelRepository::new(db.clone());
        let clock: Arc<dyn crate::ports::Clock> = Arc::new(crate::infrastructure::SystemClock);
        let background = crate::infrastructure::RuntimeSupervisor::new(
            tokio_util::sync::CancellationToken::new(),
        );
        let limits = Arc::new(crate::runtime::RuntimeLimits::default());
        let discovery = crate::discovery::DiscoveryService::new(
            db.clone(),
            secrets.clone(),
            Arc::clone(&http),
            Arc::clone(&channels),
            Arc::clone(&clock),
            Arc::clone(&background),
            Arc::clone(&limits),
        );
        let recorder = FakeNotifier::new();
        let notifier: Arc<dyn crate::ports::Notifier> = Arc::new(recorder.clone());
        let proxy = crate::proxy::ProxyService::new(
            db.clone(),
            secrets.clone(),
            Arc::clone(&http),
            routes.clone(),
            telemetry.clone(),
            Arc::clone(&clock),
            Arc::clone(&limits),
            notifier.clone(),
        );
        let state = crate::application::Context {
            config: Arc::new(AppConfig::default()),
            db: db.clone(),
            secrets: secrets.clone(),
            http,
            routes,
            channels,
            clock,
            notifier,
            discovery,
            proxy,
            telemetry,
            background,
            limits,
            admin: crate::admin::AdminService::new(db.clone(), secrets.clone()),
            recovery: crate::auth::RecoverySession::new(),
        };
        TestGateway {
            db,
            state,
            secrets,
            notifier: recorder,
            writer_cancel: cancel,
            writer,
            _dir: dir,
        }
    }

    /// Request through the claude mapping entry (/claudecode/v1/messages).
    fn chat_request_mapped(stream: bool) -> axum::extract::Request {
        let body = format!(
            r#"{{"model":"mapped-model","max_tokens":10,"stream":{stream},"messages":[{{"role":"user","content":"hi"}}]}}"#
        );
        axum::extract::Request::builder()
            .method("POST")
            .uri("/claudecode/v1/messages")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(body))
            .unwrap()
    }

    /// Upstream that serves `response` to every accepted connection, so a
    /// backup channel stays reachable across consecutive requests.
    async fn spawn_upstream_reusable(response: Vec<u8>) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let response = Arc::new(response);
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let response = Arc::clone(&response);
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let mut total = 0usize;
                    loop {
                        match stream.read(&mut buf[total..]).await {
                            Ok(0) => break,
                            Ok(n) => {
                                total += n;
                                if buf[..total].windows(4).any(|w| w == b"\r\n\r\n") {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    let _ = stream.write_all(&response).await;
                });
            }
        });
        port
    }

    /// Raw HTTP/1.1 upstream: reads the request headers, writes `response`,
    /// then either closes (default) or keeps the connection open.
    async fn spawn_upstream(response: Vec<u8>, hang: bool) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 4096];
                let mut total = 0usize;
                loop {
                    match stream.read(&mut buf[total..]).await {
                        Ok(0) => break,
                        Ok(n) => {
                            total += n;
                            if buf[..total].windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                let _ = stream.write_all(&response).await;
                if hang {
                    let mut sink = [0u8; 1024];
                    let _ = stream.read(&mut sink).await;
                }
            }
        });
        port
    }

    /// Raw HTTP/1.1 upstream that writes `parts` sequentially (with `gap`
    /// between writes) then closes, so reqwest observes several body chunks.
    async fn spawn_upstream_parts(parts: Vec<Vec<u8>>, gap: Duration) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 4096];
                let mut total = 0usize;
                loop {
                    match stream.read(&mut buf[total..]).await {
                        Ok(0) => break,
                        Ok(n) => {
                            total += n;
                            if buf[..total].windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                for part in parts {
                    let _ = stream.write_all(&part).await;
                    if !gap.is_zero() {
                        tokio::time::sleep(gap).await;
                    }
                }
            }
        });
        port
    }

    /// Raw HTTP/1.1 upstream that answers only after `delay` has elapsed
    /// (after reading the request head), then writes `response` and closes.
    async fn spawn_upstream_delayed(response: Vec<u8>, delay: Duration) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 4096];
                let mut total = 0usize;
                loop {
                    match stream.read(&mut buf[total..]).await {
                        Ok(0) => break,
                        Ok(n) => {
                            total += n;
                            if buf[..total].windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                tokio::time::sleep(delay).await;
                let _ = stream.write_all(&response).await;
            }
        });
        port
    }

    fn sse_event(payload: &str) -> String {
        format!("data: {payload}\n\n")
    }

    fn stream_response(body: &str, content_length: Option<usize>) -> String {
        let mut head = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n".to_owned();
        match content_length {
            Some(len) => head.push_str(&format!("content-length: {len}\r\n")),
            None => head.push_str("transfer-encoding: chunked\r\n"),
        }
        head.push_str("\r\n");
        head + body
    }

    fn chat_request(stream: bool) -> axum::extract::Request {
        let body = format!(
            r#"{{"model":"test-model","stream":{stream},"messages":[{{"role":"user","content":"hi"}}]}}"#
        );
        axum::extract::Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(body))
            .unwrap()
    }

    async fn latest_log(db: &Database) -> Option<(String, String, i64, Option<i64>)> {
        sqlx::query_as("SELECT id,outcome,attempt_count,final_status_code FROM request_logs ORDER BY started_at DESC LIMIT 1")
            .fetch_optional(db.pool())
            .await
            .unwrap()
    }

    async fn wait_for_outcome(
        db: &Database,
        expected: &str,
        timeout: Duration,
    ) -> (String, String, i64, Option<i64>) {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some((id, outcome, attempts, status)) = latest_log(db).await
                && outcome == expected
            {
                return (id, outcome, attempts, status);
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for outcome {expected:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// P1-4: auth failures are countable circuit failures; plain 4xx are not.
    #[test]
    fn status_kind_classifies_auth_errors_countable() {
        assert_eq!(status_kind(StatusCode::UNAUTHORIZED), ("auth_error", true));
        assert_eq!(status_kind(StatusCode::FORBIDDEN), ("auth_error", true));
        assert_eq!(
            status_kind(StatusCode::BAD_REQUEST),
            ("upstream_4xx", false)
        );
        assert_eq!(
            status_kind(StatusCode::TOO_MANY_REQUESTS),
            ("rate_limit", true)
        );
        assert_eq!(status_kind(StatusCode::REQUEST_TIMEOUT), ("timeout", true));
        assert_eq!(status_kind(StatusCode::GATEWAY_TIMEOUT), ("timeout", true));
        assert_eq!(
            status_kind(StatusCode::INTERNAL_SERVER_ERROR),
            ("upstream_5xx", true)
        );
    }

    #[tokio::test]
    async fn cancel_aware_terminates_exactly_once() {
        use std::sync::atomic::AtomicUsize;

        let calls = Arc::new(AtomicUsize::new(0));

        // (i) Stream polled to None then dropped: no cancel callback.
        let completed = Arc::new(AtomicBool::new(false));
        let finalized = Arc::new(AtomicBool::new(false));
        let responded = Arc::new(AtomicBool::new(false));
        let calls_i = calls.clone();
        let cancel_aware = CancelAware {
            inner: stream::iter(vec![
                Ok::<Bytes, Infallible>(Bytes::from("a")),
                Ok::<Bytes, Infallible>(Bytes::from("b")),
            ]),
            completed: completed.clone(),
            finalized: finalized.clone(),
            responded,
            on_cancel: Some(Box::new(move || {
                calls_i.fetch_add(1, Ordering::SeqCst);
            })),
        };
        let mut cancel_aware = Box::pin(cancel_aware);
        while cancel_aware.next().await.is_some() {}
        drop(cancel_aware);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(completed.load(Ordering::SeqCst));

        // (ii) Dropped before completion: cancel callback fires exactly once.
        let completed = Arc::new(AtomicBool::new(false));
        let finalized = Arc::new(AtomicBool::new(false));
        let responded = Arc::new(AtomicBool::new(false));
        let calls_ii = calls.clone();
        let cancel_aware = CancelAware {
            inner: stream::iter(vec![
                Ok::<Bytes, Infallible>(Bytes::from("a")),
                Ok::<Bytes, Infallible>(Bytes::from("b")),
            ]),
            completed: completed.clone(),
            finalized: finalized.clone(),
            responded,
            on_cancel: Some(Box::new(move || {
                calls_ii.fetch_add(1, Ordering::SeqCst);
            })),
        };
        drop(cancel_aware);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // (iii) Finalized before drop: no cancel callback.
        let completed = Arc::new(AtomicBool::new(false));
        let finalized = Arc::new(AtomicBool::new(true));
        let responded = Arc::new(AtomicBool::new(false));
        let calls_iii = calls.clone();
        let cancel_aware = CancelAware {
            inner: stream::iter(vec![
                Ok::<Bytes, Infallible>(Bytes::from("a")),
                Ok::<Bytes, Infallible>(Bytes::from("b")),
            ]),
            completed: completed.clone(),
            finalized: finalized.clone(),
            responded,
            on_cancel: Some(Box::new(move || {
                calls_iii.fetch_add(1, Ordering::SeqCst);
            })),
        };
        drop(cancel_aware);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    /// Report §8 scenario 1: upstream closes mid-stream. The request must be
    /// finalized exactly once as `stream_interrupted` — never overwritten by
    /// a spurious `cancelled` finish from the generator's Drop path.
    #[tokio::test]
    async fn upstream_error_mid_stream_records_single_interrupted_finish() {
        let body =
            sse_event(r#"{"id":"1","choices":[{"delta":{"content":"hi"},"finish_reason":null}]}"#);
        let response = stream_response(&body, Some(1000));
        let port = spawn_upstream(response.into_bytes(), false).await;
        let gateway = test_gateway(port, &[]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request(true), "openai_compatible", None)
            .await;
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(
            !bytes.is_empty(),
            "partial upstream data must reach the client"
        );
        let (_, outcome, attempts, status) =
            wait_for_outcome(&gateway.db, "stream_interrupted", Duration::from_secs(5)).await;
        assert_eq!(outcome, "stream_interrupted");
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(200));
        let attempt_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM request_attempts WHERE request_id=(SELECT id FROM request_logs ORDER BY started_at DESC LIMIT 1)")
                .fetch_one(gateway.db.pool())
                .await
                .unwrap();
        assert_eq!(attempt_count, 1);
        let attempt_outcome: String = sqlx::query_scalar(
            "SELECT outcome FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(attempt_outcome, "stream_interrupted");
        let failures: i64 = sqlx::query_scalar(
            "SELECT consecutive_failures FROM channel_health WHERE channel_id='ch-1'",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(failures, 1, "mid-stream errors count toward the circuit");
        gateway.shutdown().await;
    }

    /// Report §8 scenario 2: client disconnects mid-stream. Exactly one
    /// `cancelled` finish is recorded and the cancellation is not countable.
    #[tokio::test]
    async fn client_disconnect_records_single_cancelled_finish() {
        let body =
            sse_event(r#"{"id":"1","choices":[{"delta":{"content":"hi"},"finish_reason":null}]}"#);
        let response = stream_response(&body, Some(1000));
        let port = spawn_upstream(response.into_bytes(), true).await;
        let gateway = test_gateway(port, &[]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request(true), "openai_compatible", None)
            .await;
        let mut frames = response.into_body().into_data_stream();
        let first = tokio::time::timeout(Duration::from_secs(5), frames.next())
            .await
            .expect("first frame must arrive")
            .expect("stream must not end");
        assert!(!first.unwrap().is_empty());
        drop(frames);
        let (_, outcome, attempts, _) =
            wait_for_outcome(&gateway.db, "cancelled", Duration::from_secs(5)).await;
        assert_eq!(outcome, "cancelled");
        assert_eq!(attempts, 1);
        let failures: i64 = sqlx::query_scalar(
            "SELECT consecutive_failures FROM channel_health WHERE channel_id='ch-1'",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(failures, 0, "client cancellation must not open the circuit");
        gateway.shutdown().await;
    }

    /// A client disconnect mid-stream must record what the gateway actually
    /// observed — bytes that flowed and usage that was parsed — instead of a
    /// zeroed `cancelled` row (the Drop path used to hardcode 0 / empty).
    #[tokio::test]
    async fn cancelled_stream_records_observed_bytes_and_usage() {
        let body = sse_event(
            r#"{"id":"1","choices":[{"delta":{"content":"hi"},"finish_reason":null}],"usage":{"prompt_tokens":42,"completion_tokens":7,"total_tokens":49}}"#,
        );
        let response = stream_response(&body, Some(1000));
        let port = spawn_upstream(response.into_bytes(), true).await;
        let gateway = test_gateway(port, &[]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request(true), "openai_compatible", None)
            .await;
        let mut frames = response.into_body().into_data_stream();
        // Read until the upstream stalls (all buffered chunks processed), then
        // hang up like a client that gave up mid-stream.
        while let Ok(Some(Ok(chunk))) =
            tokio::time::timeout(Duration::from_millis(300), frames.next()).await
        {
            assert!(!chunk.is_empty());
        }
        drop(frames);
        let (_, outcome, attempts, _) =
            wait_for_outcome(&gateway.db, "cancelled", Duration::from_secs(5)).await;
        assert_eq!(outcome, "cancelled");
        assert_eq!(attempts, 1);
        let (input, output, response_bytes, first_token_ms): (
            Option<i64>,
            Option<i64>,
            i64,
            Option<i64>,
        ) = sqlx::query_as(
            "SELECT input_tokens, output_tokens, response_bytes, first_token_ms \
             FROM request_attempts LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(
            input, Some(42),
            "usage seen before the disconnect must be recorded"
        );
        assert_eq!(output, Some(7));
        assert!(
            response_bytes > 0,
            "bytes observed before the disconnect must be recorded"
        );
        assert!(
            first_token_ms.is_some(),
            "the observed first token must be recorded"
        );
        gateway.shutdown().await;
    }

    /// Same guarantee on the mapped path: a client that hangs up after the
    /// converter consumed content + usage leaves a cancelled row with the
    /// real bytes and usage, not zeros.
    #[tokio::test]
    async fn mapped_cancelled_stream_records_observed_bytes_and_usage() {
        let body = format!(
            "{}{}",
            sse_event(r#"{"id":"1","choices":[{"delta":{"content":"hi"},"finish_reason":null}]}"#),
            sse_event(r#"{"id":"2","choices":[{"delta":{},"finish_reason":null}],"usage":{"prompt_tokens":42,"completion_tokens":7,"total_tokens":49}}"#),
        );
        let response = stream_response(&body, Some(1000));
        let port = spawn_upstream(response.into_bytes(), true).await;
        let gateway = test_gateway_mapped(port, &[]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request_mapped(true), "claude", None)
            .await;
        let mut frames = response.into_body().into_data_stream();
        while let Ok(Some(Ok(chunk))) =
            tokio::time::timeout(Duration::from_millis(300), frames.next()).await
        {
            assert!(!chunk.is_empty());
        }
        drop(frames);
        let (_, outcome, attempts, _) =
            wait_for_outcome(&gateway.db, "cancelled", Duration::from_secs(5)).await;
        assert_eq!(outcome, "cancelled");
        assert_eq!(attempts, 1);
        let (input, output, response_bytes, first_token_ms): (
            Option<i64>,
            Option<i64>,
            i64,
            Option<i64>,
        ) = sqlx::query_as(
            "SELECT input_tokens, output_tokens, response_bytes, first_token_ms \
             FROM request_attempts LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(
            input, Some(42),
            "converter usage must survive the disconnect"
        );
        assert_eq!(output, Some(7));
        assert!(
            response_bytes > 0,
            "upstream bytes must survive the disconnect"
        );
        assert!(
            first_token_ms.is_some(),
            "the observed first token must survive the disconnect"
        );
        gateway.shutdown().await;
    }

    /// Review finding: a mapped stream whose UPSTREAM is the Responses API
    /// must record input tokens AND the cache-hit count. The Responses
    /// usage shape nests cache hits in `input_tokens_details.cached_tokens`
    /// (there is no Claude-style top-level `cache_read_input_tokens`), so
    /// the converter's usage merge must look there.
    #[tokio::test]
    async fn mapped_responses_upstream_records_cache_read() {
        let body = format!(
            "{}{}",
            sse_event(r#"{"type":"response.output_text.delta","delta":"hi"}"#),
            sse_event(r#"{"type":"response.completed","response":{"id":"r-1","usage":{"input_tokens":100,"output_tokens":50,"input_tokens_details":{"cached_tokens":30}}}}"#),
        );
        let response = stream_response(&body, Some(body.len()));
        let port = spawn_upstream(response.into_bytes(), false).await;
        let gateway = test_gateway_mapped(port, &[]).await;
        // Rewire the scaffold: the mapped channel speaks openai_responses.
        sqlx::query("UPDATE channels SET protocol='openai_responses' WHERE id='ch-1'")
            .execute(gateway.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES('cm-1','openai_responses')")
            .execute(gateway.db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE model_routes SET protocol='openai_responses' WHERE id='route-1'")
            .execute(gateway.db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE claude_model_mappings SET upstream_protocol='openai_responses' WHERE id='map-1'")
            .execute(gateway.db.pool())
            .await
            .unwrap();
        let response = gateway
            .state
            .proxy
            .proxy(chat_request_mapped(true), "claude", None)
            .await;
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(!bytes.is_empty());
        let (_, outcome, attempts, _) =
            wait_for_outcome(&gateway.db, "success", Duration::from_secs(5)).await;
        assert_eq!(outcome, "success");
        assert_eq!(attempts, 1);
        let (input, cache_read, miss, output): (
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
        ) = sqlx::query_as(
            "SELECT input_tokens, cache_read_tokens, cache_miss_input_tokens, output_tokens \
             FROM request_attempts LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(input, Some(100), "input tokens must be recorded");
        assert_eq!(
            cache_read, Some(30),
            "responses cache hits (input_tokens_details.cached_tokens) must be recorded"
        );
        assert_eq!(miss, Some(70), "cache miss = total - hit");
        assert_eq!(output, Some(50));
        gateway.shutdown().await;
    }

    /// Review finding: a same-protocol MAPPED stream (model-renaming
    /// mapping on a claude channel) forwards raw bytes, but must still
    /// record usage — previously the passthrough converter never merged
    /// usage, so input/output/cache were all NULL in the logs.
    #[tokio::test]
    async fn mapped_passthrough_stream_records_usage() {
        let body = format!(
            "{}{}{}{}{}{}",
            sse_event(r#"{"type":"message_start","message":{"usage":{"input_tokens":10,"cache_read_input_tokens":4,"cache_creation_input_tokens":2}}}"#),
            sse_event(r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#),
            sse_event(r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}"#),
            sse_event(r#"{"type":"content_block_stop","index":0}"#),
            sse_event(r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":4,"cache_creation_input_tokens":2}}"#),
            sse_event(r#"{"type":"message_stop"}"#),
        );
        let response = stream_response(&body, Some(body.len()));
        let port = spawn_upstream(response.into_bytes(), false).await;
        let gateway = test_gateway_mapped(port, &[]).await;
        // Rewire the scaffold: a same-protocol mapping (claude -> claude).
        sqlx::query("UPDATE channels SET protocol='claude' WHERE id='ch-1'")
            .execute(gateway.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES('cm-1','claude')")
            .execute(gateway.db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE model_routes SET protocol='claude' WHERE id='route-1'")
            .execute(gateway.db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE claude_model_mappings SET upstream_protocol='claude' WHERE id='map-1'")
            .execute(gateway.db.pool())
            .await
            .unwrap();
        let response = gateway
            .state
            .proxy
            .proxy(chat_request_mapped(true), "claude", None)
            .await;
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(!bytes.is_empty());
        let (_, outcome, attempts, _) =
            wait_for_outcome(&gateway.db, "success", Duration::from_secs(5)).await;
        assert_eq!(outcome, "success");
        assert_eq!(attempts, 1);
        let (input, cache_read, cache_write, output): (
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
        ) = sqlx::query_as(
            "SELECT input_tokens, cache_read_tokens, cache_write_tokens, output_tokens \
             FROM request_attempts LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(input, Some(10), "passthrough input tokens must be recorded");
        assert_eq!(
            cache_read, Some(4),
            "passthrough cache hits must be recorded"
        );
        assert_eq!(cache_write, Some(2));
        assert_eq!(output, Some(5));
        gateway.shutdown().await;
    }

    /// Regression guard: a clean stream records a single success.
    #[tokio::test]
    async fn normal_stream_completes_with_single_success() {
        let body = format!(
            "{}{}",
            sse_event(r#"{"id":"1","choices":[{"delta":{"content":"Hel"},"finish_reason":null}]}"#),
            sse_event(
                r#"{"id":"2","choices":[{"delta":{"content":"lo"},"finish_reason":null}],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}}"#
            )
        );
        let response = stream_response(&body, Some(body.len()));
        let port = spawn_upstream(response.into_bytes(), false).await;
        let gateway = test_gateway(port, &[]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request(true), "openai_compatible", None)
            .await;
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(!bytes.is_empty());
        let (_, outcome, attempts, status) =
            wait_for_outcome(&gateway.db, "success", Duration::from_secs(5)).await;
        assert_eq!(outcome, "success");
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(200));
        let failures: i64 = sqlx::query_scalar(
            "SELECT consecutive_failures FROM channel_health WHERE channel_id='ch-1'",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(failures, 0);
        gateway.shutdown().await;
    }

    /// A mapped-stream client (e.g. the Codex CLI) that hangs up right after
    /// receiving the terminal event (`response.completed`/`message_stop`)
    /// must leave a SUCCESS behind with its usage and bytes — the outcome is
    /// recorded before the terminal event is handed out, so the Drop path
    /// cannot downgrade it to a zeroed `cancelled`.
    #[tokio::test]
    async fn mapped_client_close_after_terminal_event_records_success() {
        let body = format!(
            "{}{}{}",
            sse_event(r#"{"id":"1","choices":[{"delta":{"content":"hi"},"finish_reason":null}]}"#),
            sse_event(r#"{"id":"2","choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}"#),
            "data: [DONE]\n\n",
        );
        let response = stream_response(&body, Some(body.len()));
        let port = spawn_upstream(response.into_bytes(), false).await;
        let gateway = test_gateway_mapped(port, &[]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request_mapped(true), "claude", None)
            .await;
        let mut frames = response.into_body().into_data_stream();
        // Read frames until the terminal claude event arrives, then drop the
        // body exactly like a CLI that considers the turn finished.
        let mut saw_terminal = false;
        for _ in 0..64 {
            let frame = tokio::time::timeout(Duration::from_secs(5), frames.next())
                .await
                .expect("frame must arrive")
                .expect("stream must not end yet");
            let bytes = frame.unwrap();
            if String::from_utf8_lossy(&bytes).contains("message_stop") {
                saw_terminal = true;
                break;
            }
        }
        assert!(saw_terminal, "the client must observe the terminal event");
        drop(frames);
        let (_, outcome, attempts, status) =
            wait_for_outcome(&gateway.db, "success", Duration::from_secs(5)).await;
        assert_eq!(outcome, "success", "the completed stream must stay a success");
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(200));
        let (input, output, response_bytes): (Option<i64>, Option<i64>, i64) = sqlx::query_as(
            "SELECT input_tokens, output_tokens, response_bytes FROM request_attempts LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(input, Some(10), "usage must be recorded, not zeroed");
        assert_eq!(output, Some(5));
        assert!(
            response_bytes > 0,
            "response bytes must be recorded, not zeroed"
        );
        gateway.shutdown().await;
    }

    /// Streaming usage is reported across several chunks; a later chunk that
    /// omits cache details must not wipe the cache fields an earlier chunk
    /// carried (per-field merge, latest-non-None wins).
    #[tokio::test]
    async fn passthrough_usage_merges_fields_across_chunks() {
        let body = format!(
            "{}{}{}",
            sse_event(r#"{"id":"1","choices":[{"delta":{"content":"hi"},"finish_reason":null}],"usage":{"prompt_tokens":100,"completion_tokens":0,"prompt_tokens_details":{"cached_tokens":60},"total_tokens":100}}"#),
            sse_event(r#"{"id":"2","choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":100,"completion_tokens":50,"total_tokens":150}}"#),
            "data: [DONE]\n\n",
        );
        let response = stream_response(&body, Some(body.len()));
        let port = spawn_upstream(response.into_bytes(), false).await;
        let gateway = test_gateway(port, &[]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request(true), "openai_compatible", None)
            .await;
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(!bytes.is_empty());
        let (_, outcome, attempts, _) =
            wait_for_outcome(&gateway.db, "success", Duration::from_secs(5)).await;
        assert_eq!(outcome, "success");
        assert_eq!(attempts, 1);
        let (input, cache_read, cache_miss, output): (
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
        ) = sqlx::query_as(
            "SELECT input_tokens, cache_read_tokens, cache_miss_input_tokens, output_tokens \
             FROM request_attempts LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(input, Some(100), "input from the first chunk must survive");
        assert_eq!(
            cache_read, Some(60),
            "cache_read from the first chunk must survive the second chunk"
        );
        assert_eq!(cache_miss, Some(40), "cache_miss derives from merged values");
        assert_eq!(output, Some(50), "output from the last chunk wins");
        gateway.shutdown().await;
    }

    /// The final SSE line may lack a trailing newline; usage living there
    /// must still be captured when the upstream ends.
    #[tokio::test]
    async fn passthrough_tail_line_without_newline_captures_usage() {
        let body = format!(
            "{}{}",
            sse_event(r#"{"id":"1","choices":[{"delta":{"content":"hi"},"finish_reason":null}]}"#),
            r#"data: {"id":"2","choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":3,"total_tokens":10}}"#,
        );
        let response = stream_response(&body, Some(body.len()));
        let port = spawn_upstream(response.into_bytes(), false).await;
        let gateway = test_gateway(port, &[]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request(true), "openai_compatible", None)
            .await;
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(!bytes.is_empty());
        let (_, outcome, attempts, _) =
            wait_for_outcome(&gateway.db, "success", Duration::from_secs(5)).await;
        assert_eq!(outcome, "success");
        assert_eq!(attempts, 1);
        let (input, output): (Option<i64>, Option<i64>) = sqlx::query_as(
            "SELECT input_tokens, output_tokens FROM request_attempts LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(input, Some(7), "usage in the unterminated tail line");
        assert_eq!(output, Some(3));
        gateway.shutdown().await;
    }

    /// First-token protection: a 200 stream that closes cleanly WITHOUT ever
    /// producing a first token (empty body, no terminal marker) is a
    /// protection violation, not a success — it must count against the
    /// circuit instead of silently "succeeding".
    #[tokio::test]
    async fn plain_stream_empty_close_without_token_fails_circuit() {
        let response = stream_response("", Some(0));
        let port = spawn_upstream(response.into_bytes(), false).await;
        let gateway = test_gateway(port, &[("failure_threshold", "1")]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request(true), "openai_compatible", None)
            .await;
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(bytes.is_empty(), "the client receives the empty stream");
        let (_, outcome, attempts, status) =
            wait_for_outcome(&gateway.db, "stream_interrupted", Duration::from_secs(5)).await;
        assert_eq!(outcome, "stream_interrupted");
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(200));
        let error_kind: Option<String> = sqlx::query_scalar(
            "SELECT error_kind FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(
            error_kind.as_deref(),
            Some("no_first_token"),
            "the missing first token must be named"
        );
        let (failures, state): (i64, String) = sqlx::query_as(
            "SELECT consecutive_failures, state FROM channel_health WHERE channel_id='ch-1'",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(failures, 1, "no-first-token must trip the circuit");
        assert_eq!(state, "open");
        gateway.shutdown().await;
    }

    /// A stream that carries its explicit terminal marker without any
    /// content (`data: [DONE]` only) is a legitimate empty completion — the
    /// upstream explicitly finished, so it must NOT trip the circuit.
    #[tokio::test]
    async fn plain_stream_done_only_is_success() {
        let body = "data: [DONE]\n\n";
        let response = stream_response(body, Some(body.len()));
        let port = spawn_upstream(response.into_bytes(), false).await;
        let gateway = test_gateway(port, &[("failure_threshold", "1")]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request(true), "openai_compatible", None)
            .await;
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(!bytes.is_empty());
        let (_, outcome, attempts, status) =
            wait_for_outcome(&gateway.db, "success", Duration::from_secs(5)).await;
        assert_eq!(outcome, "success");
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(200));
        let failures: i64 = sqlx::query_scalar(
            "SELECT consecutive_failures FROM channel_health WHERE channel_id='ch-1'",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(failures, 0, "an explicit [DONE] is a normal completion");
        gateway.shutdown().await;
    }

    /// First-token protection on the mapped path: a passthrough mapping
    /// (entry == upstream protocol) whose upstream closes without any token
    /// must also fail the circuit.
    #[tokio::test]
    async fn mapped_passthrough_empty_close_fails_circuit() {
        let response = stream_response("", Some(0));
        let port = spawn_upstream(response.into_bytes(), false).await;
        let gateway = test_gateway_mapped(port, &[("failure_threshold", "1")]).await;
        sqlx::query("INSERT INTO claude_model_mappings(id,claude_model_id,display_name,upstream_protocol,upstream_model_id,enabled,created_at,updated_at) VALUES('map-2','passthrough-model','Passthrough','claude','test-model',1,?,?)")
            .bind("2026-08-04T01:00:00+00:00")
            .bind("2026-08-04T01:00:00+00:00")
            .execute(gateway.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES('cm-1','claude')")
            .execute(gateway.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO model_routes(id,protocol,requested_model_id,enabled,created_at,updated_at) VALUES('route-2','claude','test-model',1,?,?)")
            .bind("2026-08-04T01:00:00+00:00")
            .bind("2026-08-04T01:00:00+00:00")
            .execute(gateway.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO route_candidates(id,route_id,channel_model_id,priority,enabled,created_at,updated_at) VALUES('rc-2','route-2','cm-1',1,1,?,?)")
            .bind("2026-08-04T01:00:00+00:00")
            .bind("2026-08-04T01:00:00+00:00")
            .execute(gateway.db.pool())
            .await
            .unwrap();
        let request = axum::extract::Request::builder()
            .method("POST")
            .uri("/claudecode/v1/messages")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(
                r#"{"model":"passthrough-model","max_tokens":10,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#,
            ))
            .unwrap();
        let response = gateway
            .state
            .proxy
            .proxy(request, "claude", None)
            .await;
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(bytes.is_empty());
        let (_, outcome, attempts, _) =
            wait_for_outcome(&gateway.db, "stream_interrupted", Duration::from_secs(5)).await;
        assert_eq!(outcome, "stream_interrupted");
        assert_eq!(attempts, 1);
        let error_kind: Option<String> = sqlx::query_scalar(
            "SELECT error_kind FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(error_kind.as_deref(), Some("no_first_token"));
        let (failures, state): (i64, String) = sqlx::query_as(
            "SELECT consecutive_failures, state FROM channel_health WHERE channel_id='ch-1'",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(failures, 1);
        assert_eq!(state, "open");
        gateway.shutdown().await;
    }

    /// P1-1: a stream that stalls after its first token is finalized by the
    /// generator as 504 transport_timeout instead of hanging forever.
    #[tokio::test]
    async fn plain_stream_idle_timeout_finalizes_as_504() {
        let body =
            sse_event(r#"{"id":"1","choices":[{"delta":{"content":"hi"},"finish_reason":null}]}"#);
        let response = stream_response(&body, Some(1000));
        let port = spawn_upstream(response.into_bytes(), true).await;
        let gateway = test_gateway(port, &[("stream_idle_timeout_seconds", "1")]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request(true), "openai_compatible", None)
            .await;
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(!bytes.is_empty());
        let (_, outcome, attempts, status) =
            wait_for_outcome(&gateway.db, "stream_interrupted", Duration::from_secs(10)).await;
        assert_eq!(outcome, "stream_interrupted");
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(504), "idle timeout must finalize as 504");
        let error_kind: Option<String> = sqlx::query_scalar(
            "SELECT error_kind FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(error_kind.as_deref(), Some("transport_timeout"));
        gateway.shutdown().await;
    }

    /// P1-3: an oversized request body is rejected with a stable 413 payload
    /// (no internal buffer error text) instead of being buffered.
    #[tokio::test]
    async fn oversized_body_rejected_with_stable_413() {
        let port = spawn_upstream(Vec::new(), false).await;
        let gateway = test_gateway(port, &[("max_request_body_mb", "1")]).await;
        let request = axum::extract::Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .body(axum::body::Body::from("x".repeat(2 * 1024 * 1024)))
            .unwrap();
        let response = gateway
            .state
            .proxy
            .proxy(request, "openai_compatible", None)
            .await;
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            value.pointer("/error/message").and_then(Value::as_str),
            Some("Request body exceeds the configured limit.")
        );
        gateway.shutdown().await;
    }

    /// P1-9: a gzip-encoded SSE response is decoded before forwarding: the
    /// client receives the plaintext with no `content-encoding` header while
    /// usage observability runs on the same decoded stream. Raw compressed
    /// forwarding was retired because a truncated upstream stream would
    /// surface as a corrupt compressed body (client-side inflate failure,
    /// e.g. omp's `ZlibError`).
    #[tokio::test]
    async fn gzip_stream_is_decoded_and_forwarded_with_usage_observed() {
        use std::io::Write;
        let plain = format!(
            "{}{}",
            sse_event(r#"{"id":"1","choices":[{"delta":{"content":"Hel"},"finish_reason":null}]}"#),
            sse_event(
                r#"{"id":"2","choices":[{"delta":{"content":"lo"},"finish_reason":null}],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}"#
            )
        );
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(plain.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();
        let mut response_bytes =
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-encoding: gzip\r\n"
                .as_bytes()
                .to_vec();
        response_bytes
            .extend_from_slice(format!("content-length: {}\r\n\r\n", compressed.len()).as_bytes());
        response_bytes.extend_from_slice(&compressed);
        let port = spawn_upstream(response_bytes, false).await;
        let gateway = test_gateway(port, &[]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request(true), "openai_compatible", None)
            .await;
        assert_eq!(
            response
                .headers()
                .get("content-encoding")
                .and_then(|v| v.to_str().ok()),
            None,
            "the encoding header must not leak into the decoded stream"
        );
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert_eq!(
            bytes.as_ref(),
            plain.as_bytes(),
            "the client must receive the decoded plaintext"
        );
        wait_for_outcome(&gateway.db, "success", Duration::from_secs(5)).await;
        let raw_usage: Option<String> = sqlx::query_scalar(
            "SELECT raw_usage_json FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert!(
            raw_usage.is_some(),
            "usage must be extracted from the decoded stream"
        );
        gateway.shutdown().await;
    }

    /// A truncated gzip stream is decoded up to the truncation point and
    /// forwarded as clean plaintext with no `content-encoding` header; the
    /// attempt is recorded as `stream_interrupted` so the channel verdict
    /// matches what the client observed (a body that ends without its
    /// terminal marker). Before the fix the truncated compressed bytes were
    /// forwarded verbatim and crashed client-side inflate (ZlibError) while
    /// the attempt was logged as success.
    #[tokio::test]
    async fn truncated_gzip_stream_ends_cleanly_downstream_and_counts_as_interrupted() {
        use std::io::Write;
        let plain = format!(
            "{}{}",
            sse_event(r#"{"id":"1","choices":[{"delta":{"content":"Hel"},"finish_reason":null}]}"#),
            sse_event(r#"{"id":"2","choices":[{"delta":{"content":"lo"},"finish_reason":null}]}"#)
        );
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(plain.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();
        // Cut the trailer (and part of the final block) so the compressed
        // stream cannot reach its end marker.
        let truncated = &compressed[..compressed.len() - 12];
        let mut response_bytes =
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-encoding: gzip\r\n"
                .as_bytes()
                .to_vec();
        response_bytes
            .extend_from_slice(format!("content-length: {}\r\n\r\n", truncated.len()).as_bytes());
        response_bytes.extend_from_slice(truncated);
        let port = spawn_upstream(response_bytes, false).await;
        let gateway = test_gateway(port, &[]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request(true), "openai_compatible", None)
            .await;
        assert_eq!(
            response
                .headers()
                .get("content-encoding")
                .and_then(|v| v.to_str().ok()),
            None,
            "no content-encoding header on a decoded relay"
        );
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(
            plain.as_bytes().starts_with(&bytes),
            "the client receives a clean prefix of the plaintext, never compressed bytes"
        );
        let (_, outcome, _, _) =
            wait_for_outcome(&gateway.db, "stream_interrupted", Duration::from_secs(10)).await;
        assert_eq!(outcome, "stream_interrupted");
        let error_kind: Option<String> = sqlx::query_scalar(
            "SELECT error_kind FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(error_kind.as_deref(), Some("stream_interrupted"));
        gateway.shutdown().await;
    }

    /// P1-1: the first-token window spans the whole attempt. An upstream that
    /// answers headers + content 2s after the request starts, with a 1s
    /// first-token budget, must finalize as 504 (the deadline expired before
    /// the response head arrived) instead of succeeding because the content
    /// arrived right after the headers.
    #[tokio::test]
    async fn first_token_deadline_counts_from_attempt_start_plain() {
        let body = format!(
            "{}{}",
            sse_event(r#"{"id":"1","choices":[{"delta":{"content":"hi"},"finish_reason":null}]}"#),
            sse_event(r#"{"id":"2","choices":[{"delta":{"content":"!"},"finish_reason":null}]}"#),
        );
        let response = stream_response(&body, Some(body.len()));
        let port = spawn_upstream_delayed(response.into_bytes(), Duration::from_secs(2)).await;
        let gateway = test_gateway(
            port,
            &[
                ("first_byte_timeout_seconds", "3"),
                ("first_token_timeout_seconds", "1"),
            ],
        )
        .await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request(true), "openai_compatible", None)
            .await;
        // Drive the stream to completion: the generator runs its terminal
        // telemetry (504, transport_timeout) once the deadline has expired.
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let _ = bytes;
        let (_, outcome, attempts, status) =
            wait_for_outcome(&gateway.db, "stream_interrupted", Duration::from_secs(10)).await;
        assert_eq!(outcome, "stream_interrupted");
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(504), "deadline runs from attempt start");
        let error_kind: Option<String> = sqlx::query_scalar(
            "SELECT error_kind FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(error_kind.as_deref(), Some("transport_timeout"));
        gateway.shutdown().await;
    }

    /// P1-1 (mapped): the prelude window is anchored at attempt start too.
    /// The 200 + content arrives 2s in, the 1s budget already expired, so the
    /// single candidate fails over into the 504 tail instead of succeeding.
    #[tokio::test]
    async fn first_token_deadline_counts_from_attempt_start_mapped() {
        let body = format!(
            "{}{}",
            sse_event(r#"{"id":"1","choices":[{"delta":{"content":"hi"},"finish_reason":null}]}"#),
            sse_event(r#"{"id":"2","choices":[{"delta":{"content":"!"},"finish_reason":null}]}"#),
        );
        let response = stream_response(&body, Some(body.len()));
        let port = spawn_upstream_delayed(response.into_bytes(), Duration::from_secs(2)).await;
        let gateway = test_gateway_mapped(
            port,
            &[
                ("first_byte_timeout_seconds", "3"),
                ("first_token_timeout_seconds", "1"),
            ],
        )
        .await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request_mapped(true), "claude", None)
            .await;
        assert_eq!(
            response.status(),
            StatusCode::GATEWAY_TIMEOUT,
            "expired prelude deadline + single candidate -> 504"
        );
        let (_, outcome, attempts, status) =
            wait_for_outcome(&gateway.db, "gateway_error", Duration::from_secs(10)).await;
        assert_eq!(outcome, "gateway_error");
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(504));
        gateway.shutdown().await;
    }

    /// P1-2: a mapped stream whose 200 response head arrives but never
    /// produces a first token is a *timeout*, so a single-candidate run ends
    /// in 504 (pre-fix: the prelude timeout was not classified and the tail
    /// wrongly produced 502).
    #[tokio::test]
    async fn mapped_prelude_timeout_classifies_as_504() {
        let headers_only =
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: 1000\r\n\r\n"
                .as_bytes()
                .to_vec();
        let port = spawn_upstream(headers_only, true).await;
        let gateway = test_gateway_mapped(port, &[("first_token_timeout_seconds", "1")]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request_mapped(true), "claude", None)
            .await;
        assert_eq!(
            response.status(),
            StatusCode::GATEWAY_TIMEOUT,
            "prelude timeout must classify as 504, not 502"
        );
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            value.pointer("/error/type").and_then(Value::as_str),
            Some("upstream_timeout")
        );
        let (_, outcome, attempts, status) =
            wait_for_outcome(&gateway.db, "gateway_error", Duration::from_secs(10)).await;
        assert_eq!(outcome, "gateway_error");
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(504));
        let error_kind: Option<String> = sqlx::query_scalar(
            "SELECT error_kind FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(error_kind.as_deref(), Some("timeout"));
        gateway.shutdown().await;
    }

    /// P1-2: a non-streaming 200 whose body dies mid-read (content-length
    /// promised, connection closed early) records its own attempt event and
    /// finalizes as 502 — pre-fix it was lumped into the timeout branch
    /// (504) with no attempt row.
    #[tokio::test]
    async fn non_stream_connection_reset_returns_502_with_attempt() {
        let mut response_bytes =
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 1000\r\n\r\n"
                .as_bytes()
                .to_vec();
        response_bytes.extend_from_slice(&[b'x'; 40]);
        let port = spawn_upstream(response_bytes, false).await;
        let gateway = test_gateway(port, &[]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request(false), "openai_compatible", None)
            .await;
        assert_eq!(
            response.status(),
            StatusCode::BAD_GATEWAY,
            "connection reset mid-body is 502, not 504"
        );
        let (_, outcome, attempts, status) =
            wait_for_outcome(&gateway.db, "gateway_error", Duration::from_secs(5)).await;
        assert_eq!(outcome, "gateway_error");
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(502));
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM request_attempts WHERE request_id=(SELECT id FROM request_logs ORDER BY started_at DESC LIMIT 1)",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(count, 1, "the reset must record its own attempt event");
        let error_kind: Option<String> = sqlx::query_scalar(
            "SELECT error_kind FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(error_kind.as_deref(), Some("transport_error"));
        gateway.shutdown().await;
    }

    /// P1-3: the mapped-stream prelude is decoded exactly once. A compressed
    /// upstream body (all four encodings) must reach the client as converted
    /// plaintext SSE carrying both content texts; re-feeding the decoder with
    /// the prelude bytes would corrupt its state and produce an empty stream.
    #[tokio::test]
    async fn mapped_compressed_stream_decodes_prelude_once() {
        use std::io::Write;
        let plain = format!(
            "{}{}{}",
            sse_event(
                r#"{"id":"1","choices":[{"delta":{"content":"Hello"},"finish_reason":null}]}"#
            ),
            sse_event(
                r#"{"id":"2","choices":[{"delta":{"content":"World"},"finish_reason":null}]}"#
            ),
            "data: [DONE]\n\n",
        );
        let gzip = {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(plain.as_bytes()).unwrap();
            encoder.finish().unwrap()
        };
        let deflate = {
            let mut encoder =
                flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(plain.as_bytes()).unwrap();
            encoder.finish().unwrap()
        };
        let brotli = {
            let mut encoder = brotli::CompressorWriter::new(Vec::new(), 4096, 5, 22);
            encoder.write_all(plain.as_bytes()).unwrap();
            encoder.flush().unwrap();
            encoder.into_inner()
        };
        let zstd = zstd::stream::encode_all(plain.as_bytes(), 3).unwrap();
        for (encoding, compressed) in [
            ("gzip", gzip),
            ("deflate", deflate),
            ("br", brotli),
            ("zstd", zstd),
        ] {
            let mut response_bytes = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-encoding: {encoding}\r\ncontent-length: {}\r\n\r\n",
                compressed.len()
            )
            .into_bytes();
            response_bytes.extend_from_slice(&compressed);
            let port = spawn_upstream(response_bytes, false).await;
            let gateway = test_gateway_mapped(port, &[]).await;
            let response = gateway
                .state
                .proxy
                .proxy(chat_request_mapped(true), "claude", None)
                .await;
            assert_eq!(
                response.headers().get("content-encoding"),
                None,
                "{encoding}: the raw encoding header must not leak into the converted stream"
            );
            let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            let text = String::from_utf8_lossy(&bytes);
            assert!(
                text.contains("Hello") && text.contains("World"),
                "{encoding}: converted stream must carry both content texts, got {text:?}"
            );
            let (_, outcome, attempts, status) =
                wait_for_outcome(&gateway.db, "success", Duration::from_secs(5)).await;
            assert_eq!(outcome, "success", "{encoding}");
            assert_eq!(attempts, 1, "{encoding}");
            assert_eq!(status, Some(200), "{encoding}");
            let first_token_ms: Option<i64> = sqlx::query_scalar(
                "SELECT first_token_ms FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
            )
            .fetch_one(gateway.db.pool())
            .await
            .unwrap();
            assert!(
                first_token_ms.is_some(),
                "{encoding}: first_token_ms must be recorded"
            );
            gateway.shutdown().await;
        }
    }

    /// P1-8: a mapped non-streaming response that cannot be converted (the
    /// upstream body is not valid JSON for the upstream protocol) must reach
    /// the client as the fixed gateway error message — never the upstream's
    /// raw text or the internal conversion error.
    #[tokio::test]
    async fn mapped_conversion_failure_does_not_leak_internal_text() {
        let upstream_body = "not json at all, definitely not a chat completion";
        let response_bytes = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{upstream_body}",
            upstream_body.len()
        )
        .into_bytes();
        let port = spawn_upstream(response_bytes, false).await;
        let gateway = test_gateway_mapped(port, &[]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request_mapped(false), "claude", None)
            .await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "conversion failure keeps the 200 protocol-compat status"
        );
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let text = String::from_utf8_lossy(&bytes);
        assert!(
            text.contains("Response conversion failed"),
            "client must see the fixed message, got: {text:?}"
        );
        assert!(
            !text.contains(upstream_body),
            "upstream body text must not leak into the converted response"
        );
        // P1-5: a conversion failure is a gateway error — never success —
        // and the request/attempt/channel events must agree.
        let (_, _outcome, attempts, status) =
            wait_for_outcome(&gateway.db, "gateway_error", Duration::from_secs(5)).await;
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(200), "protocol-compat 200 is preserved");
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM request_attempts WHERE request_id=(SELECT id FROM request_logs ORDER BY started_at DESC LIMIT 1)",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(count, 1, "the conversion attempt must be recorded");
        let (attempt_outcome, error_kind): (String, Option<String>) = sqlx::query_as(
            "SELECT outcome, error_kind FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(attempt_outcome, "gateway_error");
        assert_eq!(error_kind.as_deref(), Some("conversion_error"));
        // No ChannelSuccess was emitted: the channel verdict must stay
        // untouched (no success, no countable failure).
        let (state, failures): (String, i64) = sqlx::query_as(
            "SELECT state, consecutive_failures FROM channel_health WHERE channel_id='ch-1'",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(state, "active");
        assert_eq!(failures, 0);
        gateway.shutdown().await;
    }

    /// P1-1: a mapped non-stream response declaring a Content-Length above
    /// the buffered-body cap is rejected up front — nothing is read.
    #[tokio::test]
    async fn mapped_non_stream_declared_oversized_body_returns_502() {
        let response_bytes = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
            2 * 1024 * 1024
        )
        .into_bytes();
        let port = spawn_upstream(response_bytes, false).await;
        let gateway = test_gateway_mapped(port, &[("max_buffered_upstream_body_mb", "1")]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request_mapped(false), "claude", None)
            .await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        eprintln!("DEBUG BODY: {}", String::from_utf8_lossy(&body));
        assert!(String::from_utf8_lossy(&body).contains("upstream_response_too_large"));
        let (_, _, attempts, status) =
            wait_for_outcome(&gateway.db, "gateway_error", Duration::from_secs(5)).await;
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(502));
        let error_kind: Option<String> = sqlx::query_scalar(
            "SELECT error_kind FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(error_kind.as_deref(), Some("upstream_response_too_large"));
        gateway.shutdown().await;
    }

    /// P1-1: a mapped non-stream body without Content-Length that grows past
    /// the cap mid-read (raw accumulation) is cut off with 502 — the buffer
    /// never exceeds the cap.
    #[tokio::test]
    async fn mapped_non_stream_unbounded_raw_body_hits_cap() {
        let mut response_bytes = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\r\n"
            .as_bytes()
            .to_vec();
        response_bytes.extend(std::iter::repeat_n(b'x', 2 * 1024 * 1024));
        let port = spawn_upstream(response_bytes, false).await;
        let gateway = test_gateway_mapped(port, &[("max_buffered_upstream_body_mb", "1")]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request_mapped(false), "claude", None)
            .await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let (_, _, attempts, status) =
            wait_for_outcome(&gateway.db, "gateway_error", Duration::from_secs(5)).await;
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(502));
        let error_kind: Option<String> = sqlx::query_scalar(
            "SELECT error_kind FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(error_kind.as_deref(), Some("upstream_response_too_large"));
        gateway.shutdown().await;
    }

    /// P1-1 + P1-2: a compressed mapped body whose plaintext exceeds the cap
    /// trips the RequiredDecoder's cumulative limit → stable 502, not a
    /// truncated conversion.
    #[tokio::test]
    async fn mapped_non_stream_plaintext_over_cap_returns_502() {
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&vec![b'x'; 2 * 1024 * 1024]).unwrap();
        let compressed = encoder.finish().unwrap();
        let response_bytes = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-encoding: gzip\r\ncontent-length: {}\r\n\r\n",
            compressed.len()
        )
        .into_bytes();
        let mut body = response_bytes;
        body.extend_from_slice(&compressed);
        let port = spawn_upstream(body, false).await;
        let gateway = test_gateway_mapped(port, &[("max_buffered_upstream_body_mb", "1")]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request_mapped(false), "claude", None)
            .await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let (_, _, attempts, status) =
            wait_for_outcome(&gateway.db, "gateway_error", Duration::from_secs(5)).await;
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(502));
        let error_kind: Option<String> = sqlx::query_scalar(
            "SELECT error_kind FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(error_kind.as_deref(), Some("upstream_decode_error"));
        gateway.shutdown().await;
    }

    /// P1-2: a mapped non-stream gzip body truncated mid-frame decodes
    /// partially but never finishes → 502, never a partial conversion.
    #[tokio::test]
    async fn mapped_non_stream_truncated_gzip_returns_502() {
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder
            .write_all(br#"{"id":"1","choices":[{"finish_reason":"stop"}]}"#)
            .unwrap();
        let mut compressed = encoder.finish().unwrap();
        compressed.truncate(compressed.len() / 2);
        let response_bytes = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-encoding: gzip\r\ncontent-length: {}\r\n\r\n",
            compressed.len()
        )
        .into_bytes();
        let mut body = response_bytes;
        body.extend_from_slice(&compressed);
        let port = spawn_upstream(body, false).await;
        let gateway = test_gateway_mapped(port, &[("max_buffered_upstream_body_mb", "1")]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request_mapped(false), "claude", None)
            .await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let (_, _, attempts, status) =
            wait_for_outcome(&gateway.db, "gateway_error", Duration::from_secs(5)).await;
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(502));
        let error_kind: Option<String> = sqlx::query_scalar(
            "SELECT error_kind FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(error_kind.as_deref(), Some("upstream_decode_error"));
        gateway.shutdown().await;
    }

    /// P1-2: a mapped stream whose cumulative plaintext exceeds the cap
    /// mid-stream terminates with a fixed gateway error event and a
    /// `gateway_error` outcome — the stream is never silently truncated.
    /// The body is ONE gzip stream (a single-member decoder ignores any
    /// trailing member), split across two TCP writes so the limit trips
    /// after the response has started.
    #[tokio::test]
    async fn mapped_stream_cumulative_limit_fails_attempt() {
        use std::io::Write;
        let event1 =
            sse_event(r#"{"id":"1","choices":[{"delta":{"content":"Hi"},"finish_reason":null}]}"#);
        let event2 = sse_event(&format!(
            r#"{{"id":"2","choices":[{{"delta":{{"content":"{}"}},"finish_reason":null}}]}}"#,
            "x".repeat(2 * 1024 * 1024)
        ));
        let mut plain = event1.into_bytes();
        plain.extend_from_slice(event2.as_bytes());
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&plain).unwrap();
        let compressed = encoder.finish().unwrap();
        assert!(compressed.len() < 16 * 1024, "test payload must stay small");
        // Split at 512 bytes: the first part carries the header + event1
        // (a few hundred bytes of plaintext), so the prelude decodes it
        // without touching the cap; the rest trips it mid-stream.
        let split = 512usize.min(compressed.len() / 2);
        let mut part1 =
            b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-encoding: gzip\r\n\r\n"
                .to_vec();
        part1.extend_from_slice(&compressed[..split]);
        let parts = vec![part1, compressed[split..].to_vec()];
        let port = spawn_upstream_parts(parts, Duration::from_millis(30)).await;
        let gateway = test_gateway_mapped(port, &[("max_buffered_upstream_body_mb", "1")]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request_mapped(true), "claude", None)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 4 * 1024 * 1024)
            .await
            .unwrap();
        let text = String::from_utf8_lossy(&bytes);
        assert!(
            text.contains("could not be decoded"),
            "client must see the fixed gateway error event, got: {text:?}"
        );
        let (_, outcome, attempts, status) =
            wait_for_outcome(&gateway.db, "gateway_error", Duration::from_secs(5)).await;
        assert_eq!(outcome, "gateway_error");
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(200));
        let error_kind: Option<String> = sqlx::query_scalar(
            "SELECT error_kind FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(error_kind.as_deref(), Some("upstream_decode_error"));
        gateway.shutdown().await;
    }

    /// P1-1: two concurrent oversized non-stream responses are both bounded
    /// and both terminate with 502 — no unbounded growth, no deadlock.
    #[tokio::test]
    async fn concurrent_oversized_responses_stay_bounded() {
        let mut response_bytes = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\r\n"
            .as_bytes()
            .to_vec();
        response_bytes.extend(std::iter::repeat_n(b'x', 2 * 1024 * 1024));
        let port_a = spawn_upstream(response_bytes.clone(), false).await;
        let port_b = spawn_upstream(response_bytes, false).await;
        let gateway_a =
            test_gateway_mapped(port_a, &[("max_buffered_upstream_body_mb", "1")]).await;
        let gateway_b =
            test_gateway_mapped(port_b, &[("max_buffered_upstream_body_mb", "1")]).await;
        let (response_a, response_b) = tokio::join!(
            gateway_a
                .state
                .proxy
                .proxy(chat_request_mapped(false), "claude", None),
            gateway_b
                .state
                .proxy
                .proxy(chat_request_mapped(false), "claude", None),
        );
        assert_eq!(response_a.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(response_b.status(), StatusCode::BAD_GATEWAY);
        for gateway in [gateway_a, gateway_b] {
            let (_, _, attempts, status) =
                wait_for_outcome(&gateway.db, "gateway_error", Duration::from_secs(5)).await;
            assert_eq!(attempts, 1);
            assert_eq!(status, Some(502));
            gateway.shutdown().await;
        }
    }

    async fn catalog_ids(response: Response<Body>) -> (StatusCode, Value) {
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        (status, value)
    }

    fn catalog_entry<'a>(value: &'a Value, id: &str) -> &'a Value {
        value["data"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["id"] == id)
            .unwrap()
    }

    fn catalog_model_ids(value: &Value) -> Vec<String> {
        value["data"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["id"].as_str().unwrap().to_owned())
            .collect()
    }

    /// `/v1/models` without a protocol selector aggregates every protocol
    /// pool (openai_compatible + claude here), deduplicated by model id, and
    /// each entry carries the endpoints of every protocol it routes through.
    #[tokio::test]
    async fn v1_models_aggregates_all_protocols() {
        let port = spawn_upstream(Vec::new(), false).await;
        let gateway = test_gateway_claude(port, &[]).await;
        let request = axum::extract::Request::builder()
            .uri("/v1/models")
            .body(Body::empty())
            .unwrap();
        let (status, value) =
            catalog_ids(openai_models(State(gateway.state.clone()), request).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(value["object"], "list");
        assert_eq!(
            catalog_model_ids(&value),
            vec!["claude-model", "test-model"],
            "aggregate must list every protocol pool ordered by id"
        );
        assert_eq!(
            catalog_entry(&value, "test-model")["x_local_gateway"]["supported_endpoints"],
            json!(["/v1/chat/completions"])
        );
        assert_eq!(
            catalog_entry(&value, "claude-model")["x_local_gateway"]["supported_endpoints"],
            json!(["/v1/messages"])
        );
        gateway.shutdown().await;
    }

    /// `/v1/models?protocol=…` returns exactly that protocol's pool.
    #[tokio::test]
    async fn v1_models_protocol_query_selects_protocol() {
        let port = spawn_upstream(Vec::new(), false).await;
        let gateway = test_gateway_claude(port, &[]).await;
        for (query, expected) in [
            ("?protocol=claude", vec!["claude-model".to_owned()]),
            ("?protocol=openai_compatible", vec!["test-model".to_owned()]),
            ("?protocol=openai_responses", Vec::<String>::new()),
        ] {
            let request = axum::extract::Request::builder()
                .uri(format!("/v1/models{query}"))
                .body(Body::empty())
                .unwrap();
            let (status, value) =
                catalog_ids(openai_models(State(gateway.state.clone()), request).await).await;
            assert_eq!(status, StatusCode::OK);
            if query == "?protocol=claude" {
                assert_eq!(value["data"][0]["type"], "model");
            } else {
                assert_eq!(value["object"], "list");
            }
            assert_eq!(
                catalog_model_ids(&value),
                expected,
                "query {query} must select exactly its protocol"
            );
        }
        gateway.shutdown().await;
    }

    /// `X-Local-Gateway-Protocol` and `anthropic-version` both select the
    /// Claude catalog on `/v1/models`; an unknown protocol is rejected.
    #[tokio::test]
    async fn v1_models_headers_select_or_reject_protocol() {
        let port = spawn_upstream(Vec::new(), false).await;
        let gateway = test_gateway_claude(port, &[]).await;
        let request = axum::extract::Request::builder()
            .uri("/v1/models")
            .header("x-local-gateway-protocol", "claude")
            .body(Body::empty())
            .unwrap();
        let (status, value) =
            catalog_ids(openai_models(State(gateway.state.clone()), request).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(catalog_model_ids(&value), vec!["claude-model"]);

        let request = axum::extract::Request::builder()
            .uri("/v1/models")
            .header("anthropic-version", "2023-06-01")
            .body(Body::empty())
            .unwrap();
        let (status, value) =
            catalog_ids(openai_models(State(gateway.state.clone()), request).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(catalog_model_ids(&value), vec!["claude-model"]);

        let request = axum::extract::Request::builder()
            .uri("/v1/models?protocol=gemini")
            .body(Body::empty())
            .unwrap();
        let (status, value) =
            catalog_ids(openai_models(State(gateway.state.clone()), request).await).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            value.pointer("/error/message").and_then(Value::as_str),
            Some("Unsupported model catalog protocol.")
        );
        gateway.shutdown().await;
    }

    /// `/v1/messages/models` answers the Claude native list shape with the
    /// channel's real display name and `x_local_gateway` metadata.
    #[tokio::test]
    async fn claude_catalog_uses_native_shape() {
        let port = spawn_upstream(Vec::new(), false).await;
        let gateway = test_gateway_claude(port, &[]).await;
        let request = axum::extract::Request::builder()
            .uri("/v1/messages/models")
            .body(Body::empty())
            .unwrap();
        let (status, value) =
            catalog_ids(claude_models(State(gateway.state.clone()), request).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(catalog_model_ids(&value), vec!["claude-model"]);
        assert_eq!(value["has_more"], false);
        assert_eq!(value["first_id"], "claude-model");
        assert_eq!(value["last_id"], "claude-model");
        let entry = &value["data"][0];
        assert_eq!(entry["type"], "model");
        assert_eq!(entry["id"], "claude-model");
        assert_eq!(entry["display_name"], "Test Model");
        assert_eq!(entry["created_at"], "2026-08-04T01:00:00+00:00");
        assert_eq!(
            entry["x_local_gateway"]["supported_endpoints"],
            json!(["/v1/messages"])
        );
        gateway.shutdown().await;
    }

    /// Seeds a second openai_compatible candidate (priority 2) for the same
    /// model, pointing at `port_b` — the failover target for `test-model`.
    async fn seed_backup_candidate(db: &Database, secrets: &SecretStore, port_b: u16) {
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("INSERT INTO providers(id,name,base_url,created_at,updated_at) VALUES('prov-2','mock-b',?,?,?)")
            .bind(format!("http://127.0.0.1:{port_b}"))
            .bind(time)
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channels(id,provider_id,name,protocol,api_key_encrypted,api_key_hint,manual_enabled,created_at,updated_at) VALUES('ch-2','prov-2','chan-b','openai_compatible',?,?,1,?,?)")
            .bind(secrets.encrypt("test-key"))
            .bind("...key")
            .bind(time)
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_models(id,channel_id,model_id,display_name,source,available,first_seen_at,created_at,updated_at) VALUES('cm-2','ch-2','test-model','Test Model B','discovered',1,?,?,?)")
            .bind(time)
            .bind(time)
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES('cm-2','openai_compatible')")
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO route_candidates(id,route_id,channel_model_id,priority,enabled,created_at,updated_at) VALUES('rc-2','route-1','cm-2',2,1,?,?)")
            .bind(time)
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_health(channel_id,state,consecutive_failures,updated_at) VALUES('ch-2','active',0,?)")
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
    }

    fn http_error_upstream(status: &str, body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        )
        .into_bytes()
    }

    fn json_ok_upstream(body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        )
        .into_bytes()
    }

    /// A request that fails on the primary candidate and succeeds on the
    /// backup emits exactly one failover notice describing the handover.
    #[tokio::test]
    async fn failover_emits_notification_with_http_error() {
        let port_a = spawn_upstream(
            http_error_upstream("500 Internal Server Error", r#"{"error":"boom"}"#),
            false,
        )
        .await;
        let port_b = spawn_upstream(
            json_ok_upstream(r#"{"choices":[{"finish_reason":"stop","message":{"content":"OK"}}]}"#),
            false,
        )
        .await;
        let gateway = test_gateway(port_a, &[]).await;
        seed_backup_candidate(&gateway.db, &gateway.secrets, port_b).await;

        let response = gateway
            .state
            .proxy
            .proxy(chat_request(false), "openai_compatible", None)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let notices = gateway.notifier.notices();
        assert_eq!(notices.len(), 1, "one failover -> one notice");
        assert_eq!(notices[0].model_id, "test-model");
        assert_eq!(notices[0].failed_channel_name, "chan");
        assert_eq!(notices[0].next_channel_name, "chan-b");
        assert_eq!(notices[0].error_kind.as_deref(), Some("HTTP 500"));
        gateway.shutdown().await;
    }

    /// Every failover is reported: two requests with a broken primary emit
    /// two notices — the notifier batches, it never throttles.
    #[tokio::test]
    async fn consecutive_failovers_each_emit_notice() {
        let port_a = spawn_upstream(
            http_error_upstream("500 Internal Server Error", r#"{"error":"boom"}"#),
            false,
        )
        .await;
        let port_b = spawn_upstream_reusable(
            json_ok_upstream(r#"{"choices":[{"finish_reason":"stop","message":{"content":"OK"}}]}"#),
        )
        .await;
        let gateway = test_gateway(port_a, &[]).await;
        seed_backup_candidate(&gateway.db, &gateway.secrets, port_b).await;

        for _ in 0..2 {
            let response = gateway
                .state
                .proxy
                .proxy(chat_request(false), "openai_compatible", None)
                .await;
            assert_eq!(response.status(), StatusCode::OK);
        }
        assert_eq!(gateway.notifier.notices().len(), 2);
        gateway.shutdown().await;
    }

    /// A transport failure (connection reset) on the primary is labelled
    /// with the transport kind in the notice.
    #[tokio::test]
    async fn failover_transport_reset_labels_error_kind() {
        // Empty response then close: the HTTP parse sees EOF -> transport error.
        let port_a = spawn_upstream(Vec::new(), false).await;
        let port_b = spawn_upstream(
            json_ok_upstream(r#"{"choices":[{"finish_reason":"stop","message":{"content":"OK"}}]}"#),
            false,
        )
        .await;
        let gateway = test_gateway(port_a, &[]).await;
        seed_backup_candidate(&gateway.db, &gateway.secrets, port_b).await;

        let response = gateway
            .state
            .proxy
            .proxy(chat_request(false), "openai_compatible", None)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let notices = gateway.notifier.notices();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].error_kind.as_deref(), Some("connection_reset"));
        gateway.shutdown().await;
    }

    /// With no backup candidate there is no failover: a failing single
    /// candidate answers 502 and emits no notice.
    #[tokio::test]
    async fn single_candidate_failure_emits_no_notice() {
        let port_a = spawn_upstream(
            http_error_upstream("500 Internal Server Error", r#"{"error":"boom"}"#),
            false,
        )
        .await;
        let gateway = test_gateway(port_a, &[]).await;

        let response = gateway
            .state
            .proxy
            .proxy(chat_request(false), "openai_compatible", None)
            .await;
        // No failover: the upstream's 500 error response is passed through.
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(gateway.notifier.notices().is_empty());
        gateway.shutdown().await;
    }

    /// Raw HTTP/1.1 upstream that reports the captured request head to the
    /// test and answers with a minimal OpenAI completion.
    async fn spawn_upstream_capturing_head() -> (u16, tokio::sync::mpsc::UnboundedReceiver<String>)
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 8192];
                let mut total = 0usize;
                loop {
                    match stream.read(&mut buf[total..]).await {
                        Ok(0) => break,
                        Ok(n) => {
                            total += n;
                            if buf[..total].windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                let _ = tx.send(String::from_utf8_lossy(&buf[..total]).into_owned());
                let response = json_ok_upstream(
                    r#"{"choices":[{"finish_reason":"stop","message":{"content":"OK"}}]}"#,
                );
                let _ = stream.write_all(&response).await;
            }
        });
        (port, rx)
    }

    fn session_header_value(head: &str) -> Option<String> {
        head.lines()
            .find(|line| {
                line.to_ascii_lowercase()
                    .starts_with(&format!("{}:", protocol::OPENCODE_SESSION_HEADER))
            })
            .and_then(|line| line.split_once(':'))
            .map(|(_, value)| value.trim().to_owned())
    }

    fn session_header_count(head: &str) -> usize {
        head.lines()
            .filter(|line| {
                line.to_ascii_lowercase()
                    .starts_with(&format!("{}:", protocol::OPENCODE_SESSION_HEADER))
            })
            .count()
    }

    /// OpenCode Zen/Go upstreams reject requests without a session header:
    /// the gateway must inject its persisted install id when the client did
    /// not supply one.
    #[tokio::test]
    async fn opencode_upstream_receives_injected_session_header() {
        let (port, mut heads) = spawn_upstream_capturing_head().await;
        let gateway = test_gateway(port, &[]).await;
        sqlx::query("UPDATE providers SET base_url=? WHERE id='prov-1'")
            .bind(format!("http://127.0.0.1:{port}/zen/go"))
            .execute(gateway.db.pool())
            .await
            .unwrap();

        let response = gateway
            .state
            .proxy
            .proxy(chat_request(false), "openai_compatible", None)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let head = tokio::time::timeout(Duration::from_secs(5), heads.recv())
            .await
            .expect("captured upstream request timed out")
            .expect("captured upstream request missing");
        let session = session_header_value(&head).expect("x-opencode-session must be injected");
        assert!(!session.is_empty(), "injected session id must not be empty");
        assert_eq!(session_header_count(&head), 1, "exactly one session header");
        let stored: String = sqlx::query_scalar(
            "SELECT CAST(value_json AS TEXT) FROM settings WHERE key='opencode_session_id'",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(
            session,
            stored.trim_matches('"'),
            "injected value must be the persisted install id"
        );
        gateway.shutdown().await;
    }

    /// Non-OpenCode upstreams must stay untouched: the session header is
    /// specific to opencode.ai/zen endpoints.
    #[tokio::test]
    async fn non_opencode_upstream_has_no_session_header() {
        let (port, mut heads) = spawn_upstream_capturing_head().await;
        let gateway = test_gateway(port, &[]).await;

        let response = gateway
            .state
            .proxy
            .proxy(chat_request(false), "openai_compatible", None)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let head = tokio::time::timeout(Duration::from_secs(5), heads.recv())
            .await
            .expect("captured upstream request timed out")
            .expect("captured upstream request missing");
        assert!(
            session_header_value(&head).is_none(),
            "plain upstreams must not receive x-opencode-session: {head}"
        );
        gateway.shutdown().await;
    }

    /// A session header supplied by the client always wins over the
    /// gateway's own fallback id.
    #[tokio::test]
    async fn client_session_header_is_forwarded_unchanged() {
        let (port, mut heads) = spawn_upstream_capturing_head().await;
        let gateway = test_gateway(port, &[]).await;
        sqlx::query("UPDATE providers SET base_url=? WHERE id='prov-1'")
            .bind(format!("http://127.0.0.1:{port}/zen/go"))
            .execute(gateway.db.pool())
            .await
            .unwrap();

        let body =
            r#"{"model":"test-model","stream":false,"messages":[{"role":"user","content":"hi"}]}"#;
        let request = axum::extract::Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .header(protocol::OPENCODE_SESSION_HEADER, "client-session-42")
            .body(axum::body::Body::from(body))
            .unwrap();
        let response = gateway
            .state
            .proxy
            .proxy(request, "openai_compatible", None)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let head = tokio::time::timeout(Duration::from_secs(5), heads.recv())
            .await
            .expect("captured upstream request timed out")
            .expect("captured upstream request missing");
        assert_eq!(
            session_header_value(&head).as_deref(),
            Some("client-session-42"),
            "client-supplied session id must be forwarded unchanged"
        );
        assert_eq!(
            session_header_count(&head),
            1,
            "client-supplied header must not be duplicated"
        );
        gateway.shutdown().await;
    }

    #[tokio::test]
    async fn opencode_client_headers_pass_through_non_opencode_upstreams() {
        let (port, mut heads) = spawn_upstream_capturing_head().await;
        let gateway = test_gateway(port, &[]).await;

        let body =
            r#"{"model":"test-model","stream":false,"messages":[{"role":"user","content":"hi"}]}"#;
        let request = axum::extract::Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .header("x-opencode-client", "cli")
            .header("x-opencode-project", "project-7")
            .header("x-opencode-request", "request-9")
            .body(axum::body::Body::from(body))
            .unwrap();
        let response = gateway
            .state
            .proxy
            .proxy(request, "openai_compatible", None)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let head = tokio::time::timeout(Duration::from_secs(5), heads.recv())
            .await
            .expect("captured upstream request timed out")
            .expect("captured upstream request missing");
        for (name, value) in [
            ("x-opencode-client", "cli"),
            ("x-opencode-project", "project-7"),
            ("x-opencode-request", "request-9"),
        ] {
            let forwarded = head.lines().find_map(|line| {
                let (header, found) = line.split_once(':')?;
                if header.eq_ignore_ascii_case(name) {
                    Some(found.trim().to_owned())
                } else {
                    None
                }
            });
            assert_eq!(
                forwarded.as_deref(),
                Some(value),
                "{name} must pass through unchanged"
            );
        }
        assert!(session_header_value(&head).is_none());
        gateway.shutdown().await;
    }
}
