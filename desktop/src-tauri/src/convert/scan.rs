//! 流扫描器：StreamScan 与流完成/错误判定（scan.rs）。

use super::stream::*;
use super::*;

/// Incremental scanner over an upstream SSE/JSON stream used for the mapped
/// streaming prelude and mid-stream error detection. Feeds chunks in order;
/// returns the upstream error message when a chunk carries an error object.
pub struct StreamScan {
    buffer: Vec<u8>,
    gemini: bool,
    first_token_latched: bool,
    /// True when the most recent feed() saw a content signal.
    pub first_token_now: bool,
    /// True once a completion signal (finish_reason / message_stop / …) was seen.
    pub saw_completion: bool,
}

impl StreamScan {
    pub fn new(upstream: &str) -> Self {
        Self {
            buffer: Vec::new(),
            gemini: upstream == "gemini",
            first_token_latched: false,
            first_token_now: false,
            saw_completion: false,
        }
    }

    pub fn feed(&mut self, upstream: &str, chunk: &[u8]) -> Option<String> {
        self.first_token_now = false;
        let events = if self.gemini {
            parse_gemini_events(chunk, &mut self.buffer)
        } else {
            self.buffer.extend_from_slice(&normalize_crlf(chunk));
            let mut events = Vec::new();
            for block in split_sse_blocks(&mut self.buffer) {
                if let Some(event) = sse_block_events(&block) {
                    events.push(event);
                }
            }
            events
        };
        let mut error_message = None;
        for event in events {
            match event {
                ParsedEvent::Done => {}
                ParsedEvent::Json(value) => {
                    if error_message.is_none() {
                        error_message = stream_error_message(upstream, &value);
                    }
                    if chunk_has_content(upstream, &value) {
                        self.first_token_now = true;
                        self.first_token_latched = true;
                    }
                    if stream_completed(upstream, &value) {
                        self.saw_completion = true;
                    }
                }
            }
        }
        error_message
    }

    pub fn first_token(&self) -> bool {
        self.first_token_latched
    }
}

/// Detect an error object inside a 2xx streamed event (mirror of the Python
/// `StreamObserver.stream_error_status` heuristics). Returns the message.
pub fn stream_error_message(upstream: &str, value: &Value) -> Option<String> {
    if let Some(error) = value.get("error").filter(|error| error.is_object()) {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| "Upstream stream error".to_owned());
        return Some(message);
    }
    match upstream {
        "claude" => {
            if value.get("type").and_then(Value::as_str) == Some("error") {
                let error = value.get("error").unwrap_or(&Value::Null);
                return Some(
                    error
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("Upstream stream error")
                        .to_owned(),
                );
            }
        }
        "openai_responses"
            if value.get("type").and_then(Value::as_str) == Some("response.failed") =>
        {
            let response = value.get("response").unwrap_or(&Value::Null);
            let error = response.get("error").unwrap_or(&Value::Null);
            return Some(
                error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("Upstream stream error")
                    .to_owned(),
            );
        }
        _ => {}
    }
    None
}

/// True when a streamed event signals the completion of the generation.
pub fn stream_completed(upstream: &str, value: &Value) -> bool {
    match upstream {
        "claude" => value.get("type").and_then(Value::as_str) == Some("message_stop"),
        "openai_responses" => {
            value.get("type").and_then(Value::as_str) == Some("response.completed")
                || value
                    .get("response")
                    .and_then(|response| response.get("status"))
                    .and_then(Value::as_str)
                    == Some("completed")
        }
        "gemini" => {
            value.pointer("/candidates/0/finishReason").is_some()
                || value.pointer("/candidates/0/finish_reason").is_some()
        }
        _ => value.pointer("/choices/0/finish_reason").is_some(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(body: &[u8]) -> Value {
        serde_json::from_slice(body).unwrap()
    }

    /// P2-3: the conversion matrix covers every supported entry/upstream
    /// pair exactly once and rejects anything else.
    #[test]
    fn conversion_strategy_registry_is_exhaustive() {
        use ConversionStrategy::*;
        let pairs = [
            (("claude", "claude"), Some(Passthrough)),
            (("claude", "openai_compatible"), Some(ClaudeToChat)),
            (("claude", "openai_responses"), Some(ClaudeToResponses)),
            (("claude", "gemini"), Some(ClaudeToGemini)),
            (("openai_responses", "openai_responses"), Some(Passthrough)),
            (
                ("openai_responses", "openai_compatible"),
                Some(ResponsesToChat),
            ),
            (("openai_responses", "claude"), Some(ResponsesToClaude)),
            (("openai_responses", "gemini"), Some(ResponsesToGemini)),
            (("gemini", "gemini"), None),
            (("claude", "bogus"), None),
            (("openai_responses", ""), None),
        ];
        for ((entry, upstream), expected) in pairs {
            assert_eq!(
                ConversionStrategy::for_pair(entry, upstream),
                expected,
                "{entry} -> {upstream}"
            );
        }
    }

    #[test]
    fn claude_request_converts_tool_use_to_tool_calls() {
        let input = br#"{"model":"claude-client","max_tokens":32,"stream":true,
            "tools":[{"name":"get_weather","description":"w","input_schema":{"type":"object"}}],
            "tool_choice":{"type":"auto"},
            "messages":[{"role":"user","content":"weather?"},
                        {"role":"assistant","content":[{"type":"tool_use","id":"tu_1","name":"get_weather","input":{"city":"SF"}}]}]}"#;
        let converted = parse(
            &convert_request("claude", "openai_compatible", "upstream-model", input).unwrap(),
        );
        assert_eq!(converted["model"], "upstream-model");
        assert_eq!(converted["messages"][1]["tool_calls"][0]["id"], "tu_1");
        assert_eq!(
            converted["messages"][1]["tool_calls"][0]["function"]["name"],
            "get_weather"
        );
        assert_eq!(converted["tools"][0]["type"], "function");
        assert_eq!(converted["tool_choice"], json!("auto"));
        assert_eq!(converted["stream"], true);
    }

    #[test]
    fn claude_image_uses_png_default() {
        let input = br#"{"model":"m","messages":[{"role":"user","content":[{"type":"image","source":{"data":"AAAA"}}]}]}"#;
        let converted = parse(
            &convert_request("claude", "openai_compatible", "upstream-model", input).unwrap(),
        );
        assert_eq!(
            converted["messages"][0]["content"][0]["image_url"]["url"],
            "data:image/png;base64,AAAA"
        );
    }

    #[test]
    fn claude_to_gemini_maps_tools_and_thinking() {
        let input = br#"{"model":"m","max_tokens":64,"temperature":0.5,"top_k":10,
            "thinking":{"budget_tokens":1024},
            "tools":[{"name":"t","description":"d","input_schema":{"type":"object"}}],
            "messages":[{"role":"assistant","content":[{"type":"tool_use","id":"tu_1","name":"t","input":{"a":1}}]}]}"#;
        let converted =
            parse(&convert_request("claude", "gemini", "upstream-model", input).unwrap());
        assert_eq!(converted["generationConfig"]["maxOutputTokens"], 64);
        assert_eq!(converted["generationConfig"]["topK"], 10);
        assert_eq!(converted["thinkingConfig"]["thinkingBudget"], 1024);
        assert_eq!(converted["contents"][0]["role"], "model");
        assert_eq!(
            converted["contents"][0]["parts"][0]["functionCall"]["name"],
            "t"
        );
        assert_eq!(
            converted["tools"][0]["functionDeclarations"][0]["name"],
            "t"
        );
    }

    #[test]
    fn responses_request_converts_function_call_to_tool_calls() {
        let input = br#"{"model":"codex","max_output_tokens":32,
            "input":[{"type":"function_call","call_id":"fc_1","name":"t","arguments":"{\"a\":1}"}]}"#;
        let converted = parse(
            &convert_request(
                "openai_responses",
                "openai_compatible",
                "upstream-model",
                input,
            )
            .unwrap(),
        );
        assert_eq!(converted["messages"][0]["role"], "assistant");
        assert_eq!(
            converted["messages"][0]["tool_calls"][0]["function"]["name"],
            "t"
        );
        assert_eq!(converted["max_tokens"], 32);
    }

    #[test]
    fn openai_response_converts_to_claude_with_thinking_and_tools() {
        let upstream = br#"{"id":"chat-1","choices":[{"message":{"reasoning_content":"think...",
            "content":"Done","tool_calls":[{"id":"call_1","type":"function","function":{"name":"t","arguments":"{\"a\":1}"}}]},
            "finish_reason":"tool_calls"}],"usage":{"prompt_tokens":4,"completion_tokens":2,
            "prompt_tokens_details":{"cached_tokens":1}}}"#;
        let converted = parse(
            &convert_response("claude", "openai_compatible", "claude-client", upstream).unwrap(),
        );
        assert_eq!(converted["content"][0]["type"], "thinking");
        assert_eq!(converted["content"][1]["type"], "text");
        assert_eq!(converted["content"][2]["type"], "tool_use");
        assert_eq!(converted["stop_reason"], "tool_use");
        assert_eq!(converted["usage"]["cache_read_input_tokens"], 1);
    }

    #[test]
    fn chat_converts_to_responses_with_full_envelope() {
        let upstream = br#"{"id":"chat-1","choices":[{"message":{"content":"Hello"},"finish_reason":"length"}],
            "usage":{"prompt_tokens":4,"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":1}}}"#;
        let converted = parse(
            &convert_response(
                "openai_responses",
                "openai_compatible",
                "codex-model",
                upstream,
            )
            .unwrap(),
        );
        assert_eq!(converted["object"], "response");
        assert_eq!(converted["status"], "incomplete");
        assert_eq!(
            converted["incomplete_details"]["reason"],
            "max_output_tokens"
        );
        assert_eq!(converted["output"][0]["content"][0]["text"], "Hello");
        assert_eq!(
            converted["usage"]["input_tokens_details"]["cached_tokens"],
            1
        );
        assert_eq!(converted["parallel_tool_calls"], true);
    }

    #[test]
    fn dsml_parses_invocations_in_chat_text() {
        let upstream = br#"{"id":"chat-1","choices":[{"message":{"content":"<|DSML|tool_calls><|DSML|invoke name=\"t\"><|DSML|parameter name=\"a\" string=\"true\">v</|DSML|parameter></|DSML|invoke></|DSML|tool_calls>"},"finish_reason":"stop"}],"usage":{}}"#;
        let converted = parse(
            &convert_response(
                "openai_responses",
                "openai_compatible",
                "codex-model",
                upstream,
            )
            .unwrap(),
        );
        assert_eq!(converted["output"][0]["type"], "function_call");
        assert_eq!(converted["output"][0]["name"], "t");
        let arguments: Value =
            serde_json::from_str(converted["output"][0]["arguments"].as_str().unwrap()).unwrap();
        assert_eq!(arguments["a"], "v");
    }

    #[test]
    fn claude_stream_converts_openai_events_incrementally() {
        let mut converter =
            MappedStreamConverter::new("claude", "openai_compatible", "claude-client").unwrap();
        let mut out = Vec::new();
        out.extend(converter.feed(
            br#"data: {"choices":[{"delta":{"content":"Hi"},"finish_reason":null}]}

"#,
        ));
        out.extend(converter.feed(
            br#"data: {"choices":[{"delta":{},"finish_reason":"stop"}]}

data: [DONE]

"#,
        ));
        out.extend(converter.flush());
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("event: message_start"));
        assert!(text.contains("\"text_delta\""));
        assert!(text.contains("Hi"));
        assert!(text.contains("event: message_stop"));
        assert!(text.contains("\"stop_reason\":\"end_turn\""));
    }

    #[test]
    fn claude_stream_merges_tool_calls_by_index() {
        let mut converter =
            MappedStreamConverter::new("claude", "openai_compatible", "claude-client").unwrap();
        let mut out = Vec::new();
        out.extend(converter.feed(br#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"t","arguments":""}}]},"finish_reason":null}]}

"#));
        out.extend(converter.feed(br#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"a\":1}"}}]},"finish_reason":null}]}

"#));
        out.extend(converter.feed(
            br#"data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}

data: [DONE]

"#,
        ));
        out.extend(converter.flush());
        let text = String::from_utf8(out).unwrap();
        assert_eq!(text.matches("event: content_block_start").count(), 1);
        assert!(text.contains("\"type\":\"tool_use\""));
        assert!(text.contains("call_1"));
        assert!(text.contains("input_json_delta"));
        assert!(text.contains("\"stop_reason\":\"tool_use\""));
    }

    #[test]
    fn responses_stream_emits_sequence_numbered_events() {
        let mut converter =
            MappedStreamConverter::new("openai_responses", "openai_compatible", "codex-model")
                .unwrap();
        let mut out = Vec::new();
        out.extend(converter.feed(
            br#"data: {"choices":[{"delta":{"content":"Hi"},"finish_reason":null}]}

"#,
        ));
        out.extend(converter.feed(
            br#"data: {"choices":[{"delta":{},"finish_reason":"stop"}]}

data: [DONE]

"#,
        ));
        out.extend(converter.flush());
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("\"type\":\"response.created\""));
        assert!(text.contains("\"type\":\"response.output_text.delta\""));
        assert!(text.contains("\"type\":\"response.completed\""));
        assert!(text.contains("\"sequence_number\":0"));
        assert!(text.contains("\"status\":\"completed\""));
    }

    #[test]
    fn gemini_stream_converts_to_claude() {
        let mut converter =
            MappedStreamConverter::new("claude", "gemini", "claude-client").unwrap();
        let mut out = Vec::new();
        out.extend(
            converter.feed(b"{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Gem\"}]}}]}\n"),
        );
        out.extend(converter.feed(b"{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"ini\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":5,\"candidatesTokenCount\":2}}\n"));
        out.extend(converter.flush());
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("Gem"));
        assert!(text.contains("ini"));
        assert!(text.contains("event: message_stop"));
        assert!(text.contains("\"input_tokens\":5"));
    }

    #[test]
    fn error_conversion_wraps_upstream_message() {
        let body = br#"{"error":{"message":"boom","type":"server_error"}}"#;
        let claude = String::from_utf8(convert_error("claude", "openai_compatible", body)).unwrap();
        assert!(claude.contains("\"type\":\"error\""));
        assert!(claude.contains("boom"));
        let codex = String::from_utf8(convert_error("openai_responses", "claude", body)).unwrap();
        assert!(codex.contains("\"code\":\"server_error\""));
        assert!(codex.contains("\"param\":null"));
        // same-protocol passthrough
        assert_eq!(convert_error("claude", "claude", body), body);
    }

    #[test]
    fn stream_error_and_completion_detection() {
        let error = json!({"type":"error","error":{"message":"stream broke"}});
        assert_eq!(
            stream_error_message("claude", &error).as_deref(),
            Some("stream broke")
        );
        let failed = json!({"type":"response.failed","response":{"error":{"message":"nope"}}});
        assert_eq!(
            stream_error_message("openai_responses", &failed).as_deref(),
            Some("nope")
        );
        assert!(stream_completed("claude", &json!({"type":"message_stop"})));
        assert!(stream_completed(
            "openai_compatible",
            &json!({"choices":[{"finish_reason":"stop"}]})
        ));
        assert!(!stream_completed(
            "openai_compatible",
            &json!({"choices":[{"delta":{"content":"x"}}]})
        ));
    }

    #[test]
    fn dsml_fullwidth_bars_do_not_panic() {
        // Regression: byte-offset slicing inside the 3-byte fullwidth bar '｜'
        // used to panic.
        let mut converter =
            MappedStreamConverter::new("openai_responses", "openai_compatible", "codex-model")
                .unwrap();
        let out = converter.feed("data: {\"choices\":[{\"delta\":{\"content\":\"<｜DSML｜invoke\"},\"finish_reason\":null}]}\n\n".as_bytes());
        let out = String::from_utf8(out).unwrap();
        assert!(!out.is_empty());
    }
}
