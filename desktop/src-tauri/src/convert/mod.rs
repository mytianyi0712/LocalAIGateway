//! Protocol conversion between the gateway entry formats and the upstream
//! gateway protocols — faithful port of backend/app/adapters/convert.py.
//!
//! Two client entry formats are supported:
//! * `claude`            - Claude /v1/messages (Claude Code / claudecode)
//! * `openai_responses`  - OpenAI Responses API /v1/responses (Codex / codex)
//!
//! Requests are converted as whole JSON documents before being forwarded;
//! streaming responses are converted event-by-event while the upstream stream
//! is being read (see [`MappedStreamConverter`]), so the client always sees a
//! valid SSE stream. Tool calls, images and usage numbers are translated
//! between the formats, and model identifiers are substituted with the
//! upstream model name.

use std::collections::{BTreeMap, HashMap};

use anyhow::{Result, bail};
use serde_json::{Value, json};

const GATEWAY_ERROR_TYPE: &str = "gateway_error";

/// Conversion direction between an entry format and an upstream protocol
/// (P2-3): the `(entry, upstream) -> strategy` matrix is a registry, so
/// adding a conversion direction touches exactly this enum (plus the
/// converter functions) instead of scattered pair matches. The streamed
/// variant of the matrix is [`ConverterKind`]; upstream-only behaviors
/// (SSE scanning, completion detection) stay one-axis dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversionStrategy {
    /// Same protocol both sides: only the model name is substituted.
    Passthrough,
    ClaudeToChat,
    ClaudeToResponses,
    ClaudeToGemini,
    ResponsesToChat,
    ResponsesToClaude,
    ResponsesToGemini,
}

fn new_id(prefix: &str) -> String {
    let hex = uuid::Uuid::new_v4().simple().to_string();
    format!("{prefix}_{}", &hex[..24])
}

fn unix_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_secs() as i64)
        .unwrap_or(0)
}

fn sse(event: &str, payload: &Value) -> Vec<u8> {
    format!(
        "event: {event}\ndata: {}\n\n",
        serde_json::to_string(payload).unwrap_or_default()
    )
    .into_bytes()
}

fn sse_data(payload: &Value) -> Vec<u8> {
    format!(
        "data: {}\n\n",
        serde_json::to_string(payload).unwrap_or_default()
    )
    .into_bytes()
}

// ---------------------------------------------------------------------------
// text helpers
// ---------------------------------------------------------------------------

/// Array of `text`/`output_text` blocks joined, object `.text`, string as-is.
fn text(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Array(values) => values
            .iter()
            .filter_map(|item| {
                let item_type = item.get("type").and_then(Value::as_str);
                if matches!(item_type, Some("text" | "output_text")) {
                    item.get("text").and_then(Value::as_str).map(str::to_owned)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join(""),
        Value::Object(value) => value
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        _ => String::new(),
    }
}

fn system_text(value: Option<&Value>) -> Option<String> {
    value.map(text).filter(|value| !value.is_empty())
}

/// Text of a Claude `content` value (string or list of `text` blocks).
fn content_text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(value) => value.clone(),
        Value::Array(values) => values
            .iter()
            .filter_map(|item| {
                if item.get("type").and_then(Value::as_str) == Some("text") {
                    item.get("text").and_then(Value::as_str).map(str::to_owned)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

/// Plain text from chat-completions `message.content` / stream `delta.content`
/// (string or a list of content parts, e.g. GPT-5.x style).
pub(super) fn chat_message_text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(value) => value.clone(),
        Value::Array(values) => values
            .iter()
            .filter_map(|part| match part {
                Value::String(part) => Some(part.clone()),
                Value::Object(part) => part
                    .get("text")
                    .or_else(|| part.get("output_text"))
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                    .map(str::to_owned),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

// ---------------------------------------------------------------------------
// tool helpers (Claude -> OpenAI directions)
// ---------------------------------------------------------------------------

fn tool_choice_to_openai(tool_choice: &Value) -> Option<Value> {
    let choice_type = tool_choice.get("type").and_then(Value::as_str)?;
    match choice_type {
        "auto" => Some(json!("auto")),
        "any" => Some(json!("required")),
        "tool" => tool_choice
            .get("name")
            .and_then(Value::as_str)
            .map(|name| json!({"type": "function", "function": {"name": name}})),
        _ => None,
    }
}

fn tools_to_openai(tools: &Value) -> Option<Vec<Value>> {
    let mut result = Vec::new();
    for tool in tools.as_array()? {
        let Some(name) = tool.get("name").and_then(Value::as_str) else {
            continue;
        };
        result.push(json!({
            "type": "function",
            "function": {
                "name": name,
                "description": tool.get("description").and_then(Value::as_str).unwrap_or(""),
                "parameters": tool.get("input_schema").cloned()
                    .unwrap_or_else(|| json!({"type": "object", "properties": {}})),
            }
        }));
    }
    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

fn claude_block_text(block: &Value) -> Option<String> {
    block.get("text").and_then(Value::as_str).map(str::to_owned)
}

fn image_data_url(source: &Value, default_media_type: &str) -> String {
    let media_type = source
        .get("media_type")
        .and_then(Value::as_str)
        .unwrap_or(default_media_type);
    let data = source.get("data").and_then(Value::as_str).unwrap_or("");
    format!("data:{media_type};base64,{data}")
}

// ---------------------------------------------------------------------------
// Claude entry -> upstream request conversion
// ---------------------------------------------------------------------------

mod error;
mod request;
mod response;
mod scan;
mod stream;
pub use error::chunk_has_content;
pub use error::convert_error;
pub use request::convert_request;
pub use response::convert_response;
pub use scan::{StreamScan, stream_completed, stream_error_message};
pub use stream::MappedStreamConverter;
