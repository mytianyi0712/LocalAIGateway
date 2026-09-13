use anyhow::{Context, Result, bail};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use serde_json::{Value, json};
use url::Url;

use crate::telemetry::Usage;

/// One health probe request: method, upstream path and optional JSON body.
/// Most protocols probe with a tiny POST; Command Code probes its read-only
/// `GET /alpha/whoami` (authentication only — no token spend, no generation).
#[derive(Debug, Clone)]
pub struct HealthProbe {
    pub method: http::Method,
    pub path: String,
    pub body: Option<Value>,
}

fn post_probe(path: String, body: Value) -> HealthProbe {
    HealthProbe {
        method: http::Method::POST,
        path,
        body: Some(body),
    }
}

/// Per-protocol behavior bundle (P2-3): authentication, entry parsing,
/// discovery, health probes, usage observation and the public error shape.
/// One implementation per protocol, obtained from [`ProtocolId::adapter`];
/// adding a protocol extends the enum AND provides an adapter — the
/// registry replaces the scattered string `match` dispatch.
///
/// Protocol *conversion* (the `(entry, upstream) -> strategy` matrix in
/// convert.rs) is intentionally separate and stays string-keyed for now.
pub trait ProtocolAdapter {
    /// Entry-path model/stream extraction from the raw request.
    fn inspect_request(
        &self,
        path: &str,
        query: Option<&str>,
        body: &[u8],
    ) -> (Option<String>, bool);
    /// Upstream model-catalog path.
    fn discovery_path(&self) -> &'static str;
    /// Parse one catalog page; returns (items, optional next-page URL).
    fn parse_catalog(&self, body: &[u8], current_url: &Url) -> Result<(Vec<Value>, Option<Url>)>;
    /// Minimal health-probe request for `model`.
    fn health_probe(&self, model: &str) -> HealthProbe;
    /// Whether a probe response body counts as healthy.
    fn probe_body_ok(&self, body: &[u8]) -> bool;
    /// Raw usage object inside a streamed event or response root.
    fn usage_value(&self, value: &Value) -> Option<Value>;
    /// Normalize a raw usage object into the gateway's Usage model.
    fn normalize_usage(&self, usage: &Value) -> Usage;
    /// Public gateway-error body shape for this protocol.
    fn error_shape(&self, status: StatusCode, code: &str, message: &str, request_id: &str)
    -> Value;
}

/// Shared OpenAI-family behaviors (chat completions and Responses).
fn openai_usage_value(value: &Value) -> Option<Value> {
    value.get("usage").cloned().or_else(|| {
        value
            .get("response")
            .and_then(|item| item.get("usage"))
            .cloned()
    })
}

fn openai_normalize_usage(usage: &Value) -> Usage {
    let details = usage
        .get("prompt_tokens_details")
        .or_else(|| usage.get("input_tokens_details"));
    let total_input = usage
        .get("prompt_tokens")
        .and_then(Value::as_i64)
        .or_else(|| usage.get("input_tokens").and_then(Value::as_i64));
    let cache_read = details
        .and_then(|item| item.get("cached_tokens"))
        .and_then(Value::as_i64);
    let cache_write = details
        .and_then(|item| {
            item.get("cache_write_tokens")
                .or_else(|| item.get("cached_write_tokens"))
        })
        .and_then(Value::as_i64);
    let miss = match (total_input, cache_read) {
        (Some(total), Some(read)) => Some((total - read - cache_write.unwrap_or(0)).max(0)),
        _ => None,
    };
    let output = usage
        .get("completion_tokens")
        .and_then(Value::as_i64)
        .or_else(|| usage.get("output_tokens").and_then(Value::as_i64));
    Usage {
        input_tokens: total_input,
        cache_read_tokens: cache_read,
        cache_write_tokens: cache_write,
        cache_miss_input_tokens: miss,
        output_tokens: output,
        raw: Some(usage.clone()),
    }
}

fn openai_error_shape(_status: StatusCode, code: &str, message: &str, request_id: &str) -> Value {
    json!({"error": {"message": message, "type": code, "code": code, "request_id": request_id}, "request_id": request_id})
}

fn openai_inspect(path: &str, query: Option<&str>, body: &[u8]) -> (Option<String>, bool) {
    let _ = (path, query);
    let value: Value = match serde_json::from_slice(body) {
        Ok(value) => value,
        Err(_) => return (None, false),
    };
    (
        value
            .get("model")
            .and_then(Value::as_str)
            .map(str::to_owned),
        value
            .get("stream")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    )
}

/// OpenAI chat/completions (also /completions, /embeddings).
pub struct OpenaiChatAdapter;

impl ProtocolAdapter for OpenaiChatAdapter {
    fn inspect_request(
        &self,
        _path: &str,
        _query: Option<&str>,
        body: &[u8],
    ) -> (Option<String>, bool) {
        openai_inspect(_path, _query, body)
    }
    fn discovery_path(&self) -> &'static str {
        "/v1/models"
    }
    fn parse_catalog(&self, body: &[u8], _current_url: &Url) -> Result<(Vec<Value>, Option<Url>)> {
        parse_openai_catalog(body)
    }
    fn health_probe(&self, model: &str) -> HealthProbe {
        post_probe(
            "/v1/chat/completions".to_owned(),
            json!({"model": model, "messages": [{"role": "user", "content": "Reply only OK"}], "max_tokens": 2}),
        )
    }
    fn probe_body_ok(&self, body: &[u8]) -> bool {
        let Ok(value) = serde_json::from_slice::<Value>(body) else {
            return false;
        };
        if !value.is_object() {
            return false;
        }
        value.pointer("/choices/0/finish_reason").is_some()
            || value
                .pointer("/choices/0/message/content")
                .is_some_and(|content| match content {
                    Value::String(text) => !text.is_empty(),
                    Value::Array(parts) => !parts.is_empty(),
                    _ => false,
                })
    }
    fn usage_value(&self, value: &Value) -> Option<Value> {
        openai_usage_value(value)
    }
    fn normalize_usage(&self, usage: &Value) -> Usage {
        openai_normalize_usage(usage)
    }
    fn error_shape(
        &self,
        status: StatusCode,
        code: &str,
        message: &str,
        request_id: &str,
    ) -> Value {
        openai_error_shape(status, code, message, request_id)
    }
}

/// OpenAI Responses API.
pub struct OpenaiResponsesAdapter;

impl ProtocolAdapter for OpenaiResponsesAdapter {
    fn inspect_request(
        &self,
        _path: &str,
        _query: Option<&str>,
        body: &[u8],
    ) -> (Option<String>, bool) {
        openai_inspect(_path, _query, body)
    }
    fn discovery_path(&self) -> &'static str {
        "/v1/models"
    }
    fn parse_catalog(&self, body: &[u8], _current_url: &Url) -> Result<(Vec<Value>, Option<Url>)> {
        parse_openai_catalog(body)
    }
    fn health_probe(&self, model: &str) -> HealthProbe {
        post_probe(
            "/v1/responses".to_owned(),
            json!({"model": model, "input": "Reply only OK", "max_output_tokens": 2}),
        )
    }
    fn probe_body_ok(&self, body: &[u8]) -> bool {
        let Ok(value) = serde_json::from_slice::<Value>(body) else {
            return false;
        };
        if !value.is_object() {
            return false;
        }
        value.get("status").and_then(Value::as_str) == Some("completed")
            || value.get("output").is_some()
            || value
                .get("output_text")
                .and_then(Value::as_str)
                .is_some_and(|text| !text.is_empty())
    }
    fn usage_value(&self, value: &Value) -> Option<Value> {
        openai_usage_value(value)
    }
    fn normalize_usage(&self, usage: &Value) -> Usage {
        openai_normalize_usage(usage)
    }
    fn error_shape(
        &self,
        status: StatusCode,
        code: &str,
        message: &str,
        request_id: &str,
    ) -> Value {
        openai_error_shape(status, code, message, request_id)
    }
}

/// Claude native (/v1/messages).
pub struct ClaudeAdapter;

impl ProtocolAdapter for ClaudeAdapter {
    fn inspect_request(
        &self,
        _path: &str,
        _query: Option<&str>,
        body: &[u8],
    ) -> (Option<String>, bool) {
        let value: Value = match serde_json::from_slice(body) {
            Ok(value) => value,
            Err(_) => return (None, false),
        };
        (
            value
                .get("model")
                .and_then(Value::as_str)
                .map(str::to_owned),
            value
                .get("stream")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        )
    }
    fn discovery_path(&self) -> &'static str {
        "/v1/models"
    }
    fn parse_catalog(&self, body: &[u8], current_url: &Url) -> Result<(Vec<Value>, Option<Url>)> {
        let value: Value = serde_json::from_slice(body).context("模型目录 JSON 无效")?;
        let items = value
            .get("data")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let parsed = items
            .into_iter()
            .filter(|item| item.get("id").and_then(Value::as_str).is_some())
            .collect();
        // Claude paginates only when the response explicitly says
        // `has_more` and carries `last_id`.
        let next = if value.get("has_more").and_then(Value::as_bool) == Some(true) {
            value.get("last_id").and_then(Value::as_str).map(|last_id| {
                let mut next = current_url.clone();
                next.query_pairs_mut().append_pair("after_id", last_id);
                next
            })
        } else {
            None
        };
        Ok((parsed, next))
    }
    fn health_probe(&self, model: &str) -> HealthProbe {
        post_probe(
            "/v1/messages".to_owned(),
            json!({"model": model, "max_tokens": 2, "messages": [{"role": "user", "content": "Reply only OK"}]}),
        )
    }
    fn probe_body_ok(&self, body: &[u8]) -> bool {
        let Ok(value) = serde_json::from_slice::<Value>(body) else {
            return false;
        };
        if !value.is_object() {
            return false;
        }
        value.get("stop_reason").is_some()
            || value
                .get("content")
                .and_then(Value::as_array)
                .is_some_and(|blocks| !blocks.is_empty())
    }
    fn usage_value(&self, value: &Value) -> Option<Value> {
        value
            .get("message")
            .and_then(|message| message.get("usage"))
            .cloned()
            .or_else(|| value.get("usage").cloned())
    }
    fn normalize_usage(&self, usage: &Value) -> Usage {
        let read = usage.get("cache_read_input_tokens").and_then(Value::as_i64);
        let write = usage
            .get("cache_creation_input_tokens")
            .and_then(Value::as_i64);
        let miss = usage.get("input_tokens").and_then(Value::as_i64);
        let total = if miss.is_some() || read.is_some() || write.is_some() {
            Some(miss.unwrap_or(0) + read.unwrap_or(0) + write.unwrap_or(0))
        } else {
            None
        };
        Usage {
            input_tokens: total,
            cache_read_tokens: read,
            cache_write_tokens: write,
            cache_miss_input_tokens: miss,
            output_tokens: usage.get("output_tokens").and_then(Value::as_i64),
            raw: Some(usage.clone()),
        }
    }
    fn error_shape(
        &self,
        _status: StatusCode,
        code: &str,
        message: &str,
        request_id: &str,
    ) -> Value {
        json!({"type": "error", "error": {"type": code, "message": message}, "request_id": request_id})
    }
}

/// Gemini native (/v1beta/models/...).
pub struct GeminiAdapter;

impl ProtocolAdapter for GeminiAdapter {
    fn inspect_request(
        &self,
        path: &str,
        query: Option<&str>,
        _body: &[u8],
    ) -> (Option<String>, bool) {
        let model = path
            .split("/models/")
            .nth(1)
            .and_then(|value| value.split(':').next())
            .map(|value| {
                percent_encoding::percent_decode_str(value)
                    .decode_utf8_lossy()
                    .into_owned()
            });
        let stream = path.contains(":streamGenerateContent")
            || query.unwrap_or_default().contains("alt=sse");
        (model, stream)
    }
    fn discovery_path(&self) -> &'static str {
        "/v1beta/models"
    }
    fn parse_catalog(&self, body: &[u8], current_url: &Url) -> Result<(Vec<Value>, Option<Url>)> {
        let value: Value = serde_json::from_slice(body).context("模型目录 JSON 无效")?;
        let mut parsed: Vec<Value> = Vec::new();
        for mut item in value
            .get("models")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
        {
            let id = item
                .get("name")
                .and_then(Value::as_str)
                .and_then(|name| name.rsplit('/').next())
                .map(str::to_owned);
            match id {
                Some(id) => {
                    if let Some(object) = item.as_object_mut() {
                        object.insert("id".into(), Value::String(id));
                    }
                    parsed.push(item);
                }
                None => continue,
            }
        }
        // Gemini continues on `nextPageToken`.
        let next = value
            .get("nextPageToken")
            .and_then(Value::as_str)
            .map(|token| {
                let mut next = current_url.clone();
                next.query_pairs_mut().append_pair("pageToken", token);
                next
            });
        Ok((parsed, next))
    }
    fn health_probe(&self, model: &str) -> HealthProbe {
        post_probe(
            format!("/v1beta/models/{model}:generateContent"),
            json!({"contents": [{"parts": [{"text": "Reply only OK"}]}], "generationConfig": {"maxOutputTokens": 2}}),
        )
    }
    fn probe_body_ok(&self, body: &[u8]) -> bool {
        let Ok(value) = serde_json::from_slice::<Value>(body) else {
            return false;
        };
        if !value.is_object() {
            return false;
        }
        value.pointer("/candidates/0/finishReason").is_some()
            || value
                .pointer("/candidates/0/content/parts")
                .and_then(Value::as_array)
                .is_some_and(|parts| !parts.is_empty())
    }
    fn usage_value(&self, value: &Value) -> Option<Value> {
        value.get("usageMetadata").cloned()
    }
    fn normalize_usage(&self, usage: &Value) -> Usage {
        let total = usage.get("promptTokenCount").and_then(Value::as_i64);
        let cache = usage.get("cachedContentTokenCount").and_then(Value::as_i64);
        let miss = total.map(|value| (value - cache.unwrap_or(0)).max(0));
        Usage {
            input_tokens: total,
            cache_read_tokens: cache,
            cache_write_tokens: None,
            cache_miss_input_tokens: miss,
            output_tokens: usage.get("candidatesTokenCount").and_then(Value::as_i64),
            raw: Some(usage.clone()),
        }
    }
    fn error_shape(
        &self,
        status: StatusCode,
        code: &str,
        message: &str,
        request_id: &str,
    ) -> Value {
        json!({"error": {"code": status.as_u16(), "message": message, "status": code}, "request_id": request_id})
    }
}

/// Command Code CLI upstream: `/alpha/generate` (NDJSON) on the reverse
/// path, official Provider API for paid plans. Upstream-only protocol — it
/// has no client-facing entry endpoints, so `endpoints()` is empty and the
/// model catalog never advertises it.
pub struct CommandCodeAdapter;

impl ProtocolAdapter for CommandCodeAdapter {
    fn inspect_request(
        &self,
        _path: &str,
        _query: Option<&str>,
        body: &[u8],
    ) -> (Option<String>, bool) {
        let Ok(value) = serde_json::from_slice::<Value>(body) else {
            return (None, false);
        };
        // `/alpha/generate` bodies nest the fields under `params`; the
        // provider API body is plain OpenAI chat (`model` / `stream`). Accept
        // both so one adapter serves the transport router.
        let model = value
            .pointer("/params/model")
            .or_else(|| value.get("model"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let stream = value
            .pointer("/params/stream")
            .or_else(|| value.get("stream"))
            .and_then(Value::as_bool)
            .unwrap_or(true);
        (model, stream)
    }
    /// Official Provider API catalog (OpenAI shape).
    fn discovery_path(&self) -> &'static str {
        "/provider/v1/models"
    }
    fn parse_catalog(&self, body: &[u8], _current_url: &Url) -> Result<(Vec<Value>, Option<Url>)> {
        parse_openai_catalog(body)
    }
    /// Read-only account probe: authentication only, no token spend and no
    /// generation request (plan decision 5).
    fn health_probe(&self, _model: &str) -> HealthProbe {
        HealthProbe {
            method: http::Method::GET,
            path: "/alpha/whoami".to_owned(),
            body: None,
        }
    }
    fn probe_body_ok(&self, body: &[u8]) -> bool {
        let Ok(value) = serde_json::from_slice::<Value>(body) else {
            return false;
        };
        value.is_object()
            && (value.get("user").is_some()
                || value.get("org").is_some()
                || value.get("orgId").is_some())
    }
    fn usage_value(&self, value: &Value) -> Option<Value> {
        // Raw CC usage lives in `finish.totalUsage` / `finish-step.usage`;
        // the stream decoder converts it to OpenAI usage before conversion,
        // but accept the raw shape for non-stream observability.
        value
            .get("totalUsage")
            .or_else(|| value.get("usage"))
            .cloned()
    }
    fn normalize_usage(&self, usage: &Value) -> Usage {
        commandcode_usage(usage)
    }
    fn error_shape(
        &self,
        status: StatusCode,
        code: &str,
        message: &str,
        request_id: &str,
    ) -> Value {
        openai_error_shape(status, code, message, request_id)
    }
}

/// Normalize a raw Command Code usage object. `inputTokens` is the TOTAL
/// (cache hits included); the Anthropic/cache accounting needs the
/// non-cached part (`inputTokenDetails.noCacheTokens`, or a subtraction).
pub fn commandcode_usage(usage: &Value) -> Usage {
    let details = usage.get("inputTokenDetails");
    let total_input = usage.get("inputTokens").and_then(Value::as_i64);
    let cache_read = usage
        .get("cachedInputTokens")
        .and_then(Value::as_i64)
        .or_else(|| {
            details
                .and_then(|value| value.get("cacheReadTokens"))
                .and_then(Value::as_i64)
        });
    let cache_write = details
        .and_then(|value| value.get("cacheWriteTokens"))
        .and_then(Value::as_i64);
    let miss = details
        .and_then(|value| value.get("noCacheTokens"))
        .and_then(Value::as_i64)
        .or_else(|| {
            total_input.map(|total| {
                (total - cache_read.unwrap_or(0) - cache_write.unwrap_or(0)).max(0)
            })
        });
    Usage {
        input_tokens: total_input,
        cache_read_tokens: cache_read,
        cache_write_tokens: cache_write,
        cache_miss_input_tokens: miss,
        output_tokens: usage.get("outputTokens").and_then(Value::as_i64),
        raw: Some(usage.clone()),
    }
}

fn parse_openai_catalog(body: &[u8]) -> Result<(Vec<Value>, Option<Url>)> {
    let value: Value = serde_json::from_slice(body).context("模型目录 JSON 无效")?;
    let items = value
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let parsed = items
        .into_iter()
        .filter(|item| item.get("id").and_then(Value::as_str).is_some())
        .collect();
    Ok((parsed, None))
}

impl ProtocolId {
    /// Registry (P2-3): the per-protocol behavior bundle.
    pub fn adapter(self) -> &'static dyn ProtocolAdapter {
        match self {
            ProtocolId::OpenaiCompatible => &OpenaiChatAdapter,
            ProtocolId::OpenaiResponses => &OpenaiResponsesAdapter,
            ProtocolId::Claude => &ClaudeAdapter,
            ProtocolId::Gemini => &GeminiAdapter,
            ProtocolId::CommandCode => &CommandCodeAdapter,
        }
    }
}

/// Typed protocol identity (P2-3): one enum is the single source of truth
/// for the supported protocol list, entry endpoints and discovery paths —
/// adding a protocol extends exactly this type, and every string boundary
/// (validation, routing, catalog, discovery) derives from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolId {
    OpenaiCompatible,
    OpenaiResponses,
    Claude,
    Gemini,
    /// Upstream-only: Command Code CLI reverse path / official Provider API.
    CommandCode,
}

impl ProtocolId {
    pub const ALL: [ProtocolId; 5] = [
        ProtocolId::OpenaiCompatible,
        ProtocolId::OpenaiResponses,
        ProtocolId::Claude,
        ProtocolId::Gemini,
        ProtocolId::CommandCode,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            ProtocolId::OpenaiCompatible => "openai_compatible",
            ProtocolId::OpenaiResponses => "openai_responses",
            ProtocolId::Claude => "claude",
            ProtocolId::Gemini => "gemini",
            ProtocolId::CommandCode => "command_code",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "openai_compatible" => Some(Self::OpenaiCompatible),
            "openai_responses" => Some(Self::OpenaiResponses),
            "claude" => Some(Self::Claude),
            "gemini" => Some(Self::Gemini),
            "command_code" => Some(Self::CommandCode),
            _ => None,
        }
    }

    /// Entry endpoints served for this protocol (client-facing).
    pub fn endpoints(self) -> &'static [&'static str] {
        match self {
            ProtocolId::OpenaiCompatible => {
                &["/v1/chat/completions", "/v1/completions", "/v1/embeddings"]
            }
            ProtocolId::OpenaiResponses => &["/v1/responses"],
            ProtocolId::Claude => &["/v1/messages"],
            ProtocolId::Gemini => &[
                "/v1beta/models/{model}:generateContent",
                "/v1beta/models/{model}:streamGenerateContent",
            ],
            // Upstream-only: reachable through claude/codex model mappings,
            // never as a client entry endpoint.
            ProtocolId::CommandCode => &[],
        }
    }

    /// Upstream model-catalog path for this protocol (delegates to the
    /// adapter so the registry stays the single source).
    pub fn discovery_path(self) -> &'static str {
        self.adapter().discovery_path()
    }
}

/// Ordered string view of the supported protocols, kept for call sites that
/// serialize the list (system status/protocols endpoints). Literals mirror
/// [`ProtocolId::as_str`] — the enum is the single source of additions.
pub const PROTOCOL_ORDER: [&str; 5] = [
    "openai_compatible",
    "openai_responses",
    "claude",
    "gemini",
    "command_code",
];

const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
];

pub fn valid_protocol(value: &str) -> bool {
    ProtocolId::parse(value).is_some()
}

pub fn normalize_base_url(value: &str) -> Result<String> {
    let mut url =
        Url::parse(value.trim_end_matches('/')).context("base_url 必须是绝对 HTTP(S) URL")?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        bail!("base_url 必须是绝对 HTTP(S) URL");
    }
    let mut path = url.path().trim_end_matches('/').to_owned();
    for suffix in ["/v1beta", "/v1"] {
        if path.ends_with(suffix) {
            path.truncate(path.len() - suffix.len());
            break;
        }
    }
    url.set_path(&path);
    url.set_query(None);
    url.set_fragment(None);
    Ok(url.to_string().trim_end_matches('/').to_owned())
}

pub fn upstream_url(
    base_url: &str,
    path: &str,
    raw_query: Option<&str>,
    protocol: &str,
) -> Result<Url> {
    let mut base = Url::parse(base_url)?;
    // 路径里内联的查询串必须拆出来交给 `set_query`：`set_path` 会把 `?`
    // 百分号转义（/x?orgId=1 → /x%3ForgId=1），把请求打到错误路径上。
    let (path, inline_query) = match path.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (path, None),
    };
    let raw_query = match (raw_query.filter(|query| !query.is_empty()), inline_query) {
        (Some(raw), Some(inline)) if !inline.is_empty() => Some(format!("{raw}&{inline}")),
        (Some(raw), _) => Some(raw.to_owned()),
        (None, Some(inline)) if !inline.is_empty() => Some(inline.to_owned()),
        (None, _) => None,
    };
    let base_path = base.path().trim_end_matches('/');
    let suffix = format!("/{}", path.trim_start_matches('/'));
    let joined = if !base_path.is_empty()
        && (suffix == base_path || suffix.starts_with(&format!("{base_path}/")))
    {
        suffix
    } else {
        format!("{base_path}{suffix}")
    };
    base.set_path(&joined);
    base.set_query(raw_query.as_deref());
    if protocol == "gemini" {
        let retained = base
            .query_pairs()
            .filter(|(key, _)| key != "key")
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect::<Vec<_>>();
        base.set_query(None);
        if !retained.is_empty() {
            base.query_pairs_mut().extend_pairs(retained);
        }
    }
    Ok(base)
}

pub fn outbound_headers(inbound: &HeaderMap, protocol: &str, api_key: &str) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    for (name, value) in inbound {
        let key = name.as_str();
        if HOP_BY_HOP.contains(&key)
            || matches!(
                key,
                "authorization" | "x-api-key" | "x-goog-api-key" | "x-local-gateway-key"
            )
        {
            continue;
        }
        headers.append(name.clone(), value.clone());
    }
    match protocol {
        "openai_compatible" | "openai_responses" => {
            headers.insert(
                axum::http::header::AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {api_key}"))?,
            );
        }
        "claude" => {
            headers.insert(
                HeaderName::from_static("x-api-key"),
                HeaderValue::from_str(api_key)?,
            );
            headers
                .entry(HeaderName::from_static("anthropic-version"))
                .or_insert(HeaderValue::from_static("2023-06-01"));
        }
        "gemini" => {
            headers.insert(
                HeaderName::from_static("x-goog-api-key"),
                HeaderValue::from_str(api_key)?,
            );
        }
        // Command Code keys are `user_...` bearer tokens. Identity headers
        // (session/fingerprint/version) are injected separately by
        // `apply_command_code_identity`, only for `kind='command_code'`
        // providers (plan decision 2).
        "command_code" => {
            headers.insert(
                axum::http::header::AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {api_key}"))?,
            );
        }
        _ => bail!("不支持的协议 {protocol}"),
    }
    Ok(headers)
}

/// Session header OpenCode Zen/Go requires on every request since 2026-09
/// (missing it makes the upstream reject the request before routing).
pub const OPENCODE_SESSION_HEADER: &str = "x-opencode-session";

/// Whether `base_url` points at an OpenCode Zen/Go upstream. Zen is
/// identified by its canonical host (`opencode.ai`) or by a `zen` path
/// segment (`/zen/go`, `/zen/v1`), so relays that keep the canonical path
/// are covered as well.
pub fn requires_opencode_session(base_url: &str) -> bool {
    let Ok(url) = Url::parse(base_url) else {
        return false;
    };
    let host_matches = url.host_str().is_some_and(|host| {
        let host = host.to_ascii_lowercase();
        host == "opencode.ai" || host.ends_with(".opencode.ai")
    });
    let path_matches = url
        .path()
        .split('/')
        .any(|segment| segment.eq_ignore_ascii_case("zen"));
    host_matches || path_matches
}

/// Ensures [`OPENCODE_SESSION_HEADER`] is present for OpenCode upstreams.
/// A non-empty value forwarded from the client always wins (duplicates are
/// collapsed to one); the gateway's stable fallback is only used when the
/// client supplied nothing usable.
pub fn apply_opencode_session(
    headers: &mut HeaderMap,
    base_url: &str,
    session_id: &str,
) -> Result<()> {
    if session_id.is_empty() || !requires_opencode_session(base_url) {
        return Ok(());
    }
    let name = HeaderName::from_static(OPENCODE_SESSION_HEADER);
    let supplied: Option<String> = headers
        .get_all(&name)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .map(|value| value.trim().to_owned())
        .find(|value| !value.is_empty());
    let value = match supplied {
        Some(value) => value,
        None => session_id.to_owned(),
    };
    headers.insert(name, HeaderValue::from_str(&value)?);
    Ok(())
}

/// Headers the Command Code CLI sends on every request. Injected ONLY for
/// providers whose `kind = 'command_code'` (plan decision 2) — base_url is
/// never sniffed, so a self-hosted bridge can never receive a fingerprint by
/// accident.
pub struct CommandCodeIdentity {
    pub cli_version: String,
    pub session_id: String,
    pub project_slug: String,
    /// ZDR opt-in (`x-cmd-zdr: 1`).
    pub zdr: bool,
}

/// Whether a provider row opts into Command Code identity headers.
pub fn requires_command_code_identity(provider_kind: Option<&str>) -> bool {
    provider_kind == Some("command_code")
}

/// Applies the CLI identity headers. The caller has already set
/// `Authorization` via [`outbound_headers`].
pub fn apply_command_code_identity(
    headers: &mut HeaderMap,
    identity: &CommandCodeIdentity,
) -> Result<()> {
    headers.insert(
        HeaderName::from_static("x-cli-environment"),
        HeaderValue::from_static("production"),
    );
    headers.insert(
        HeaderName::from_static("x-command-code-version"),
        HeaderValue::from_str(&identity.cli_version)?,
    );
    if !identity.session_id.is_empty() {
        headers.insert(
            HeaderName::from_static("x-session-id"),
            HeaderValue::from_str(&identity.session_id)?,
        );
    }
    if !identity.project_slug.is_empty() {
        headers.insert(
            HeaderName::from_static("x-project-slug"),
            HeaderValue::from_str(&identity.project_slug)?,
        );
    }
    headers.insert(
        HeaderName::from_static("x-co-flag"),
        HeaderValue::from_static("false"),
    );
    headers.insert(
        HeaderName::from_static("x-taste-learning"),
        HeaderValue::from_static("false"),
    );
    headers.insert(
        HeaderName::from_static("traceparent"),
        HeaderValue::from_str(&generate_traceparent())?,
    );
    if identity.zdr {
        headers.insert(HeaderName::from_static("x-cmd-zdr"), HeaderValue::from_static("1"));
    }
    Ok(())
}

/// One W3C trace context per request: `00-<32hex trace-id>-<16hex span>-01`.
pub fn generate_traceparent() -> String {
    let trace = uuid::Uuid::new_v4().simple().to_string();
    let span = &uuid::Uuid::new_v4().simple().to_string()[..16];
    format!("00-{trace}-{span}-01")
}

/// Deterministic `x-project-slug` derived from the session id (plan §2.4):
/// hex session ids are parsed for an index, anything else falls back to a
/// stable character hash. Never reveals the raw session id.
pub fn derive_project_slug(session_id: &str) -> String {
    let hex_index = session_id
        .trim_start_matches("sess_")
        .chars()
        .take(8)
        .all(|value| value.is_ascii_hexdigit())
        .then(|| {
            u64::from_str_radix(&session_id.trim_start_matches("sess_")[..8], 16).unwrap_or(0)
        });
    let slug = match hex_index {
        Some(index) => {
            const WORDS: [&str; 8] = ["sable", "ember", "quartz", "willow", "harbor", "cypress", "onyx", "delta"];
            format!("{}-{}", WORDS[(index as usize) % WORDS.len()], index % 9973)
        }
        None => {
            // FNV-1a over the session id: stable, cheap, non-reversible.
            let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
            for byte in session_id.as_bytes() {
                hash ^= u64::from(*byte);
                hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
            }
            format!("ws-{:x}", hash & 0xffff_ffff)
        }
    };
    slug
}

pub fn response_headers(inbound: &reqwest::header::HeaderMap) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in inbound {
        if HOP_BY_HOP.contains(&name.as_str()) {
            continue;
        }
        headers.append(name.clone(), value.clone());
    }
    headers
}

pub fn inspect_request(
    protocol: &str,
    path: &str,
    query: Option<&str>,
    body: &[u8],
) -> (Option<String>, bool) {
    // Registry dispatch (P2-3): every protocol — including the nested
    // Command Code body (`params.model` / `params.stream`) — is inspected by
    // its own adapter instead of an inline OpenAI-shaped match.
    ProtocolId::parse(protocol)
        .map(|id| id.adapter().inspect_request(path, query, body))
        .unwrap_or((None, false))
}

pub fn discovery_path(protocol: &str) -> &'static str {
    ProtocolId::parse(protocol)
        .map(|id| id.adapter().discovery_path())
        .unwrap_or("/v1/models")
}

/// Catalog parsing via the protocol adapter (P2-3).
pub fn parse_catalog(
    protocol: &str,
    body: &[u8],
    current_url: &Url,
) -> Result<(Vec<Value>, Option<Url>)> {
    let Some(id) = ProtocolId::parse(protocol) else {
        bail!("不支持的协议 {protocol}");
    };
    id.adapter().parse_catalog(body, current_url)
}

/// Health-probe request via the protocol adapter (P2-3); `None` for an
/// unknown protocol.
pub fn health_probe(protocol: &str, model: &str) -> Option<HealthProbe> {
    ProtocolId::parse(protocol).map(|id| id.adapter().health_probe(model))
}

/// Health-probe verdict via the protocol adapter (P2-3).
pub fn probe_body_ok(protocol: &str, body: &[u8]) -> bool {
    ProtocolId::parse(protocol)
        .map(|id| id.adapter().probe_body_ok(body))
        .unwrap_or(false)
}

/// Raw usage object via the protocol adapter (P2-3).
pub fn usage_value(protocol: &str, value: &Value) -> Option<Value> {
    ProtocolId::parse(protocol).and_then(|id| id.adapter().usage_value(value))
}

/// Usage normalization via the protocol adapter (P2-3).
pub fn normalize_usage(protocol: &str, usage: &Value) -> Usage {
    ProtocolId::parse(protocol)
        .map(|id| id.adapter().normalize_usage(usage))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The inspect registry understands Command Code's nested body (and the
    /// flat Provider API body), so route-model/stream inspection works for
    /// both transports.
    #[test]
    fn command_code_inspect_reads_nested_and_flat_bodies() {
        let (model, stream) = inspect_request(
            "command_code",
            "/alpha/generate",
            None,
            br#"{"params":{"model":"deepseek/deepseek-v4-flash","stream":true}}"#,
        );
        assert_eq!(model.as_deref(), Some("deepseek/deepseek-v4-flash"));
        assert!(stream);
        let (model, stream) = inspect_request(
            "command_code",
            "/provider/v1/chat/completions",
            None,
            br#"{"model":"cc-model","stream":false}"#,
        );
        assert_eq!(model.as_deref(), Some("cc-model"));
        assert!(!stream);
        // Command Code advertises no client-facing endpoints.
        assert!(ProtocolId::CommandCode.endpoints().is_empty());
        assert_eq!(
            ProtocolId::CommandCode.discovery_path(),
            "/provider/v1/models"
        );
    }

    /// Identity headers are complete, W3C-shaped and kind-gated: a provider
    /// without `kind='command_code'` can never trigger them (decision 2).
    #[test]
    fn command_code_identity_headers_are_complete_and_kind_gated() {
        assert!(!requires_command_code_identity(None));
        assert!(!requires_command_code_identity(Some("other")));
        assert!(!requires_command_code_identity(Some("")));
        assert!(requires_command_code_identity(Some("command_code")));

        let mut headers = HeaderMap::new();
        apply_command_code_identity(
            &mut headers,
            &CommandCodeIdentity {
                cli_version: "1.53.1".into(),
                session_id: "sess_abc".into(),
                project_slug: "sable-1".into(),
                zdr: true,
            },
        )
        .unwrap();
        for name in [
            "x-cli-environment",
            "x-command-code-version",
            "x-session-id",
            "x-project-slug",
            "x-co-flag",
            "x-taste-learning",
            "traceparent",
            "x-cmd-zdr",
        ] {
            assert!(headers.contains_key(name), "{name} missing");
        }
        assert_eq!(headers["x-cli-environment"], "production");
        assert_eq!(headers["x-command-code-version"], "1.53.1");
        assert_eq!(headers["x-session-id"], "sess_abc");
        assert_eq!(headers["x-project-slug"], "sable-1");
        assert_eq!(headers["x-co-flag"], "false");
        assert_eq!(headers["x-taste-learning"], "false");
        assert_eq!(headers["x-cmd-zdr"], "1");
        let traceparent = headers["traceparent"].to_str().unwrap();
        assert_eq!(traceparent.len(), 55, "{traceparent}");
        assert!(traceparent.starts_with("00-"), "{traceparent}");
        assert!(traceparent.ends_with("-01"), "{traceparent}");
        assert_ne!(generate_traceparent(), generate_traceparent());

        // ZDR off omits the optional header.
        let mut headers = HeaderMap::new();
        apply_command_code_identity(
            &mut headers,
            &CommandCodeIdentity {
                cli_version: "1.53.1".into(),
                session_id: String::new(),
                project_slug: String::new(),
                zdr: false,
            },
        )
        .unwrap();
        assert!(!headers.contains_key("x-cmd-zdr"));
        assert!(
            !headers.contains_key("x-session-id") && !headers.contains_key("x-project-slug"),
            "empty identity values are omitted"
        );
    }

    /// Inline `?query` in a path must reach the URL as a real query string —
    /// `Url::set_path` would percent-encode the `?` and hit a 404 path.
    #[test]
    fn upstream_url_splits_inline_query_strings() {
        let url = upstream_url(
            "https://api.commandcode.ai",
            "/alpha/billing/credits?orgId=org_1",
            None,
            "command_code",
        )
        .unwrap();
        assert_eq!(
            url.as_str(),
            "https://api.commandcode.ai/alpha/billing/credits?orgId=org_1"
        );
        let url = upstream_url(
            "https://api.commandcode.ai",
            "/alpha/usage/summary?orgId=org_1",
            Some("since=2026-01-01"),
            "command_code",
        )
        .unwrap();
        assert_eq!(
            url.as_str(),
            "https://api.commandcode.ai/alpha/usage/summary?since=2026-01-01&orgId=org_1"
        );
        let url = upstream_url(
            "https://api.commandcode.ai",
            "/provider/v1/models",
            None,
            "command_code",
        )
        .unwrap();
        assert_eq!(
            url.as_str(),
            "https://api.commandcode.ai/provider/v1/models"
        );
    }

    /// Command Code probes the read-only `whoami` endpoint with GET (no
    /// token spend), and its verdict accepts the account shapes the CLI
    /// returns while rejecting error bodies.
    #[test]
    fn command_code_health_probe_is_read_only_get() {
        let probe = health_probe("command_code", "ignored-model").expect("probe");
        assert_eq!(probe.method, http::Method::GET);
        assert_eq!(probe.path, "/alpha/whoami");
        assert!(probe.body.is_none(), "whoami carries no body");
        assert!(probe_body_ok(
            "command_code",
            br#"{"user":{"userName":"u"},"org":{"id":"org_1","login":"me"}}"#
        ));
        assert!(probe_body_ok("command_code", br#"{"orgId":"org_2"}"#));
        assert!(!probe_body_ok(
            "command_code",
            br#"{"error":{"code":"unauthorized"}}"#
        ));
        assert!(!probe_body_ok("command_code", b"not json"));
    }

    #[test]
    fn opencode_session_required_by_host_or_zen_path() {
        assert!(requires_opencode_session("https://opencode.ai/zen/go"));
        assert!(requires_opencode_session("https://api.opencode.ai/zen/v1"));
        assert!(requires_opencode_session("https://opencode.ai"));
        assert!(requires_opencode_session("http://127.0.0.1:8080/zen/go"));
        assert!(!requires_opencode_session(
            "https://opencode.ai.evil.example/v1"
        ));
        assert!(!requires_opencode_session("http://localhost/zenix"));
        assert!(!requires_opencode_session("not a url"));
    }

    fn values(headers: &HeaderMap) -> Vec<String> {
        headers
            .get_all(OPENCODE_SESSION_HEADER)
            .iter()
            .map(|value| value.to_str().unwrap().to_owned())
            .collect()
    }

    #[test]
    fn apply_opencode_session_keeps_client_value_and_collapses_duplicates() {
        let mut headers = HeaderMap::new();
        apply_opencode_session(&mut headers, "https://opencode.ai/zen/go", "install-1").unwrap();
        assert_eq!(values(&headers), ["install-1"]);

        // A valid client value wins, exactly once even when duplicated.
        let mut headers = HeaderMap::new();
        headers.append(
            HeaderName::from_static(OPENCODE_SESSION_HEADER),
            HeaderValue::from_static("client-1"),
        );
        headers.append(
            HeaderName::from_static(OPENCODE_SESSION_HEADER),
            HeaderValue::from_static("client-1"),
        );
        apply_opencode_session(&mut headers, "https://opencode.ai/zen/go", "install-1").unwrap();
        assert_eq!(values(&headers), ["client-1"]);

        // An empty client value is unusable: the fallback replaces it.
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static(OPENCODE_SESSION_HEADER),
            HeaderValue::from_static(""),
        );
        apply_opencode_session(&mut headers, "https://opencode.ai/zen/go", "install-1").unwrap();
        assert_eq!(values(&headers), ["install-1"]);

        let mut headers = HeaderMap::new();
        apply_opencode_session(&mut headers, "http://127.0.0.1:9999/v1", "install-1").unwrap();
        assert!(values(&headers).is_empty());

        let mut headers = HeaderMap::new();
        apply_opencode_session(&mut headers, "https://opencode.ai", "").unwrap();
        assert!(values(&headers).is_empty());
    }
}
