//! 错误响应转换（error.rs）。

use super::*;

pub(super) fn error_payload(entry: &str, upstream_protocol: &str, body: &[u8]) -> Vec<u8> {
    if entry == upstream_protocol {
        return body.to_vec();
    }
    let mut message = "Upstream request failed.".to_owned();
    let mut error_type = "upstream_error".to_owned();
    if let Ok(value) = serde_json::from_slice::<Value>(body) {
        if let Some(error) = value.get("error").filter(|error| error.is_object()) {
            if let Some(found) = error.get("message").and_then(Value::as_str) {
                message = found.to_owned();
            }
            if let Some(found) = error
                .get("type")
                .or_else(|| error.get("code"))
                .or_else(|| error.get("status"))
                .and_then(Value::as_str)
            {
                error_type = found.to_ascii_lowercase().replace(' ', "_");
            }
        }
    } else {
        let raw = String::from_utf8_lossy(body).trim().to_owned();
        if !raw.is_empty() {
            message = raw;
        }
    }
    if entry == "claude" {
        serde_json::to_vec(&json!({
            "type": "error",
            "error": {"type": error_type, "message": message},
        }))
        .unwrap_or_else(|_| body.to_vec())
    } else {
        serde_json::to_vec(&json!({
            "error": {"message": message, "type": error_type, "code": error_type, "param": Value::Null},
        }))
        .unwrap_or_else(|_| body.to_vec())
    }
}

/// Entry-agnostic error conversion (convert_mapped_error_response).
pub fn convert_error(entry: &str, upstream_protocol: &str, body: &[u8]) -> Vec<u8> {
    error_payload(entry, upstream_protocol, body)
}

// ---------------------------------------------------------------------------
// stream diagnostics used by the proxy pipeline
// ---------------------------------------------------------------------------

/// True when a streamed chunk carries the first generated content token.
pub fn chunk_has_content(protocol: &str, value: &Value) -> bool {
    match protocol {
        "claude" => value
            .get("delta")
            .and_then(|delta| delta.get("text"))
            .and_then(Value::as_str)
            .is_some_and(|text| !text.is_empty()),
        "gemini" => value
            .get("candidates")
            .and_then(Value::as_array)
            .and_then(|items| items.first())
            .and_then(|candidate| {
                candidate
                    .get("content")
                    .and_then(|content| content.get("parts"))
                    .and_then(Value::as_array)
            })
            .is_some_and(|parts| parts.iter().any(|part| part.get("text").is_some())),
        "openai_responses" => value.get("delta").is_some(),
        _ => value
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|items| items.first())
            .and_then(|choice| choice.get("delta"))
            .and_then(|delta| delta.get("content").and_then(Value::as_str))
            .is_some_and(|text| !text.is_empty()),
    }
}
