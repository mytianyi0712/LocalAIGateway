//! Codex Remote Compaction protocol support.
//!
//! Codex has two remote compaction protocols:
//!
//! - V1 (legacy): a dedicated unary endpoint `POST /v1/responses/compact`.
//!   The request body is a Responses-like JSON object; the response is
//!   `{"output":[ResponseItem,...]}`.
//! - V2 (`remote_compaction_v2`, default): the normal `POST /v1/responses`
//!   stream endpoint, with a single `{"type":"compaction_trigger"}` item
//!   appended to `input`. The stream must contain exactly one compaction
//!   output item and then `response.completed`.
//!
//! This module owns mode detection and response validation used by both the
//! proxy hot path and the discovery probes.

use serde_json::{Value, json};

/// Remote compaction request mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionMode {
    V1,
    V2,
}

impl CompactionMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
        }
    }
}

/// Parsed tri-state persisted in `channel_protocols`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionSupport {
    Unknown,
    Supported,
    Unsupported,
}

impl CompactionSupport {
    pub fn from_db(value: Option<i64>) -> Self {
        match value {
            Some(1) => Self::Supported,
            Some(2) => Self::Unsupported,
            _ => Self::Unknown,
        }
    }

    pub fn as_db(self) -> i64 {
        match self {
            Self::Unknown => 0,
            Self::Supported => 1,
            Self::Unsupported => 2,
        }
    }

    pub fn is_supported(self) -> bool {
        matches!(self, Self::Supported)
    }
}

/// Error while inspecting or validating a remote-compaction request/response.
#[derive(Debug, Clone)]
pub struct CompactionRequestError(pub String);

impl std::fmt::Display for CompactionRequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for CompactionRequestError {}

/// Detect whether an inbound Responses request is a remote-compaction request.
///
/// Returns:
/// - `Ok(Some(V1))` when the path targets `/responses/compact`;
/// - `Ok(Some(V2))` when `input` ends with exactly one `compaction_trigger`;
/// - `Ok(None)` for ordinary requests;
/// - `Err` for malformed compaction framing (bad JSON, missing input, trigger
///   not last, or more than one trigger).
pub fn detect_compaction(path: &str, body: &[u8]) -> Result<Option<CompactionMode>, CompactionRequestError> {
    if path.ends_with("/responses/compact") {
        let value: Value = serde_json::from_slice(body)
            .map_err(|error| CompactionRequestError(format!("Invalid compact request body: {error}")))?;
        let object = value
            .as_object()
            .ok_or_else(|| CompactionRequestError("Compact request body must be a JSON object".into()))?;
        if !object.get("input").is_some_and(Value::is_array) {
            return Err(CompactionRequestError(
                "Compact request body must contain an input array".into(),
            ));
        }
        return Ok(Some(CompactionMode::V1));
    }

    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return Ok(None);
    };
    let Some(input) = value.get("input").and_then(Value::as_array) else {
        return Ok(None);
    };
    let trigger_positions: Vec<usize> = input
        .iter()
        .enumerate()
        .filter_map(|(index, item)| {
            item.get("type").and_then(Value::as_str).and_then(|kind| {
                if kind == "compaction_trigger" {
                    Some(index)
                } else {
                    None
                }
            })
        })
        .collect();
    if trigger_positions.is_empty() {
        return Ok(None);
    }
    if trigger_positions.len() != 1 || trigger_positions[0] + 1 != input.len() {
        return Err(CompactionRequestError(
            "compaction_trigger must appear exactly once and must be the last input item".into(),
        ));
    }
    Ok(Some(CompactionMode::V2))
}

/// Validate a V1 compact endpoint response body: a JSON object whose `output`
/// is an array.
pub fn validate_v1_response(body: &[u8]) -> bool {
    serde_json::from_slice::<Value>(body)
        .ok()
        .filter(|value| {
            value.is_object()
                && value
                    .get("output")
                    .is_some_and(Value::is_array)
        })
        .is_some()
}

/// Why a V2 compaction stream failed validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactionV2Error {
    /// The upstream sent `response.failed`.
    Upstream(String),
    /// The stream completed without a compaction output item.
    NotCompaction,
    /// The stream completed with more than one compaction output item.
    TooManyCompactions,
    /// The stream ended before `response.completed`.
    Incomplete,
}

impl std::fmt::Display for CompactionV2Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Upstream(message) => write!(formatter, "upstream stream error: {message}"),
            Self::NotCompaction => formatter.write_str("response completed without compaction item"),
            Self::TooManyCompactions => formatter.write_str("response contained multiple compaction items"),
            Self::Incomplete => formatter.write_str("stream ended before response.completed"),
        }
    }
}

impl std::error::Error for CompactionV2Error {}

/// Incremental validator for a V2 compaction SSE stream.
///
/// The proxy buffers a compaction stream before forwarding it to Codex so a
/// malformed/unsupported upstream response can still fail over before any
/// client bytes are emitted. Discovery uses the same validator for probing.
#[derive(Default)]
pub struct CompactionV2Validator {
    buffer: Vec<u8>,
    compaction_items: usize,
    completed: bool,
    failed: Option<String>,
    usage: Option<Value>,
}

impl CompactionV2Validator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed one raw upstream chunk.
    pub fn feed(&mut self, chunk: &[u8]) {
        self.buffer.extend_from_slice(chunk);
        loop {
            let Some(end) = self.buffer.windows(2).position(|window| window == b"\n\n") else {
                break;
            };
            let block = self.buffer.drain(..end).collect::<Vec<_>>();
            self.buffer.drain(..2);
            if !block.is_empty() {
                self.consume_block(&block);
            }
        }
    }

    /// Whether the stream has reached `response.completed`.
    pub fn completed(&self) -> bool {
        self.completed
    }

    /// Whether the stream has seen exactly one compaction item so far.
    pub fn compaction_count(&self) -> usize {
        self.compaction_items
    }

    /// Captured usage from `response.completed` (or the last usage object).
    pub fn usage(&self) -> Option<&Value> {
        self.usage.as_ref()
    }

    /// Validate the collected stream. Call after EOF.
    pub fn finish(&self) -> Result<(), CompactionV2Error> {
        if let Some(message) = self.failed.as_deref() {
            return Err(CompactionV2Error::Upstream(message.to_owned()));
        }
        if !self.completed {
            return Err(CompactionV2Error::Incomplete);
        }
        match self.compaction_items {
            0 => Err(CompactionV2Error::NotCompaction),
            1 => Ok(()),
            _ => Err(CompactionV2Error::TooManyCompactions),
        }
    }

    fn consume_block(&mut self, block: &[u8]) {
        let mut data = Vec::new();
        for line in block.split(|byte| *byte == b'\n') {
            let line = trim_ascii(line);
            if let Some(rest) = line.strip_prefix(b"data:") {
                data.extend_from_slice(trim_ascii(rest));
                data.push(b'\n');
            }
        }
        let payload = trim_ascii(&data);
        if payload == b"[DONE]" {
            return;
        }
        let Ok(value) = serde_json::from_slice::<Value>(payload) else {
            return;
        };
        let Some(kind) = value.get("type").and_then(Value::as_str) else {
            return;
        };
        match kind {
            "response.output_item.done" => {
                if value
                    .get("item")
                    .and_then(|item| item.get("type"))
                    .and_then(Value::as_str)
                    == Some("compaction")
                {
                    self.compaction_items += 1;
                }
            }
            "response.completed" => {
                self.completed = true;
                let usage = value
                    .get("response")
                    .and_then(|response| response.get("usage"))
                    .or_else(|| value.get("usage"));
                if usage.is_some() {
                    self.usage = usage.cloned();
                }
            }
            "response.failed" => {
                let message = value
                    .pointer("/response/error/message")
                    .or_else(|| value.pointer("/error/message"))
                    .and_then(Value::as_str)
                    .unwrap_or("upstream stream failed")
                    .to_owned();
                self.failed = Some(message);
            }
            _ => {}
        }
    }
}

fn trim_ascii(mut value: &[u8]) -> &[u8] {
    while value.first().is_some_and(u8::is_ascii_whitespace) {
        value = &value[1..];
    }
    while value.last().is_some_and(u8::is_ascii_whitespace) {
        value = &value[..value.len() - 1];
    }
    value
}

/// Minimal V1 compact endpoint probe body.
pub fn v1_probe_body(model: &str) -> Value {
    json!({
        "model": model,
        "input": [{
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "Reply OK"}],
        }],
        "parallel_tool_calls": false,
    })
}

/// Minimal V2 compaction stream probe body.
pub fn v2_probe_body(model: &str) -> Value {
    json!({
        "model": model,
        "input": [
            {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "Reply OK"}],
            },
            {"type": "compaction_trigger"},
        ],
        "stream": true,
        "parallel_tool_calls": false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_v1_by_path() {
        let body = br#"{"model":"m","input":[]}"#;
        assert_eq!(
            detect_compaction("/codex/v1/responses/compact", body).unwrap(),
            Some(CompactionMode::V1)
        );
    }

    #[test]
    fn detects_v2_by_trailing_trigger() {
        let body = br#"{"model":"m","stream":true,"input":[{"type":"message","role":"user","content":[]},{"type":"compaction_trigger"}]}"#;
        assert_eq!(
            detect_compaction("/codex/v1/responses", body).unwrap(),
            Some(CompactionMode::V2)
        );
    }

    #[test]
    fn ordinary_responses_is_not_compaction() {
        let body = br#"{"model":"m","stream":true,"input":[{"type":"message","role":"user","content":[]}]}"#;
        assert_eq!(
            detect_compaction("/codex/v1/responses", body).unwrap(),
            None
        );
    }

    #[test]
    fn rejects_non_trailing_or_multiple_triggers() {
        let body = br#"{"model":"m","input":[{"type":"compaction_trigger"},{"type":"message"}]}"#;
        assert!(detect_compaction("/v1/responses", body).is_err());

        let body = br#"{"model":"m","input":[{"type":"compaction_trigger"},{"type":"compaction_trigger"}]}"#;
        assert!(detect_compaction("/v1/responses", body).is_err());
    }

    #[test]
    fn validates_v1_output_array() {
        assert!(validate_v1_response(br#"{"output":[]}"#));
        assert!(validate_v1_response(br#"{"output":[{"type":"message"}]}"#));
        assert!(!validate_v1_response(br#"{"output":{}}"#));
        assert!(!validate_v1_response(br#"{"items":[]}"#));
    }

    #[test]
    fn v2_validator_accepts_exactly_one_compaction() {
        let mut validator = CompactionV2Validator::new();
        validator.feed(
            br#"data: {"type":"response.created","response":{"id":"r"}}

data: {"type":"response.output_item.done","item":{"type":"compaction","id":"c"}}

data: {"type":"response.completed","response":{"id":"r","usage":{"input_tokens":1,"output_tokens":2}}}

"#,
        );
        validator.feed(b"");
        assert_eq!(validator.compaction_count(), 1);
        assert!(validator.completed());
        assert!(validator.finish().is_ok());
        assert!(validator.usage().is_some());
    }

    #[test]
    fn v2_validator_rejects_missing_compaction() {
        let mut validator = CompactionV2Validator::new();
        validator.feed(
            br#"data: {"type":"response.completed","response":{"id":"r"}}

"#,
        );
        validator.feed(b"");
        assert_eq!(validator.finish(), Err(CompactionV2Error::NotCompaction));
    }

    #[test]
    fn v2_validator_rejects_incomplete() {
        let mut validator = CompactionV2Validator::new();
        validator.feed(
            br#"data: {"type":"response.output_item.done","item":{"type":"compaction","id":"c"}}

"#,
        );
        validator.feed(b"");
        assert_eq!(validator.finish(), Err(CompactionV2Error::Incomplete));
    }

    #[test]
    fn v2_validator_reports_upstream_failure() {
        let mut validator = CompactionV2Validator::new();
        validator.feed(
            br#"data: {"type":"response.failed","response":{"error":{"message":"boom"}}}

"#,
        );
        validator.feed(b"");
        assert!(matches!(
            validator.finish(),
            Err(CompactionV2Error::Upstream(message)) if message == "boom"
        ));
    }
}
