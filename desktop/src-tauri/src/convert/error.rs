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

/// True when a streamed chunk carries the first generated token.
///
/// Counts visible text, reasoning/thinking, and tool-call deltas. Newer
/// models (DeepSeek V4, GPT-5.x, Claude thinking) often emit those before
/// any string `delta.content` / `delta.text`; treating only the latter as
/// content left `first_token_ms` null on successful streams.
pub fn chunk_has_content(protocol: &str, value: &Value) -> bool {
    match protocol {
        "claude" => claude_chunk_has_content(value),
        "gemini" => gemini_chunk_has_content(value),
        "openai_responses" => responses_chunk_has_content(value),
        _ => openai_chat_chunk_has_content(value),
    }
}

fn nonempty_text(value: Option<&Value>) -> bool {
    value.is_some_and(|value| !chat_message_text(value).is_empty())
}

fn openai_chat_chunk_has_content(value: &Value) -> bool {
    let Some(choice) = value
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
    else {
        return false;
    };
    let delta = choice.get("delta").unwrap_or(&Value::Null);
    const TEXT_KEYS: &[&str] = &[
        "content",
        "reasoning_content",
        "reasoning",
        "thinking",
        "reasoning_text",
    ];
    if TEXT_KEYS.iter().any(|key| nonempty_text(delta.get(key))) {
        return true;
    }
    if delta
        .get("tool_calls")
        .and_then(Value::as_array)
        .is_some_and(|calls| !calls.is_empty())
        || delta.get("function_call").is_some_and(|call| !call.is_null())
    {
        return true;
    }
    nonempty_text(choice.get("text"))
}

fn claude_chunk_has_content(value: &Value) -> bool {
    if let Some(delta) = value.get("delta")
        && (nonempty_text(delta.get("text"))
            || nonempty_text(delta.get("thinking"))
            || nonempty_text(delta.get("partial_json")))
    {
        return true;
    }
    if value.get("type").and_then(Value::as_str) == Some("content_block_start")
        && let Some(block) = value.get("content_block")
    {
        let block_type = block.get("type").and_then(Value::as_str);
        if matches!(
            block_type,
            Some("tool_use" | "server_tool_use" | "redacted_thinking")
        ) {
            return true;
        }
        if nonempty_text(block.get("text")) || nonempty_text(block.get("thinking")) {
            return true;
        }
    }
    false
}

fn gemini_chunk_has_content(value: &Value) -> bool {
    value
        .get("candidates")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .and_then(|candidate| {
            candidate
                .get("content")
                .and_then(|content| content.get("parts"))
                .and_then(Value::as_array)
        })
        .is_some_and(|parts| {
            parts.iter().any(|part| {
                nonempty_text(part.get("text"))
                    || part.get("functionCall").is_some()
                    || part.get("function_call").is_some()
            })
        })
}

fn responses_chunk_has_content(value: &Value) -> bool {
    if let Some(delta) = value.get("delta") {
        match delta {
            Value::String(text) if !text.is_empty() => return true,
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
            _ => return true,
        }
    }
    let event_type = value.get("type").and_then(Value::as_str).unwrap_or("");
    event_type.contains("output_text")
        || event_type.contains("reasoning")
        || event_type.contains("function_call_arguments")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn openai_chat_string_content_still_counts() {
        assert!(chunk_has_content(
            "openai_compatible",
            &json!({"choices":[{"delta":{"content":"hi"}}]})
        ));
        assert!(!chunk_has_content(
            "openai_compatible",
            &json!({"choices":[{"delta":{"role":"assistant"}}]})
        ));
        assert!(!chunk_has_content(
            "openai_compatible",
            &json!({"choices":[{"delta":{"content":""}}]})
        ));
    }

    #[test]
    fn openai_chat_reasoning_and_array_content_count() {
        assert!(
            chunk_has_content(
                "openai_compatible",
                &json!({"choices":[{"delta":{"reasoning_content":"think"}}]})
            ),
            "DeepSeek-style reasoning_content is the first generated token"
        );
        assert!(chunk_has_content(
            "openai_compatible",
            &json!({"choices":[{"delta":{"content":[{"type":"output_text","text":"hi"}]}}]})
        ));
        assert!(chunk_has_content(
            "openai_compatible",
            &json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"c1"}]}}]})
        ));
    }

    #[test]
    fn claude_thinking_and_tool_use_count() {
        assert!(chunk_has_content(
            "claude",
            &json!({"type":"content_block_delta","delta":{"type":"text_delta","text":"hi"}})
        ));
        assert!(
            chunk_has_content(
                "claude",
                &json!({"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"plan"}})
            ),
            "Claude thinking deltas are first-token signals"
        );
        assert!(chunk_has_content(
            "claude",
            &json!({"type":"content_block_delta","delta":{"type":"input_json_delta","partial_json":"{"}})
        ));
        assert!(chunk_has_content(
            "claude",
            &json!({"type":"content_block_start","content_block":{"type":"tool_use","id":"t1","name":"x"}})
        ));
        assert!(!chunk_has_content(
            "claude",
            &json!({"type":"content_block_start","content_block":{"type":"text","text":""}})
        ));
    }

    #[test]
    fn responses_and_gemini_keep_previous_hits() {
        assert!(chunk_has_content(
            "openai_responses",
            &json!({"type":"response.output_text.delta","delta":"hi"})
        ));
        assert!(chunk_has_content(
            "openai_responses",
            &json!({"type":"response.reasoning_text.delta","delta":"think"})
        ));
        assert!(chunk_has_content(
            "gemini",
            &json!({"candidates":[{"content":{"parts":[{"text":"hi"}]}}]})
        ));
        assert!(chunk_has_content(
            "gemini",
            &json!({"candidates":[{"content":{"parts":[{"functionCall":{"name":"x"}}]}}]})
        ));
    }
}
