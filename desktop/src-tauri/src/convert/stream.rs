//! 流式转换：MappedStreamConverter 按块增量转换 SSE（stream.rs）。

use super::response::*;
use super::*;

pub(super) enum ParsedEvent {
    Done,
    Json(Value),
}

pub(super) fn split_sse_blocks(buffer: &mut Vec<u8>) -> Vec<Vec<u8>> {
    let mut blocks = Vec::new();
    while let Some(marker) = buffer.windows(2).position(|window| window == b"\n\n") {
        let block = buffer.drain(..marker).collect::<Vec<_>>();
        buffer.drain(..2);
        if !block.is_empty() {
            blocks.push(block);
        }
    }
    blocks
}

pub(super) fn sse_block_events(block: &[u8]) -> Option<ParsedEvent> {
    let mut data_lines: Vec<&[u8]> = Vec::new();
    for line in block.split(|byte| *byte == b'\n') {
        let line = trim_ascii(line);
        if let Some(rest) = line.strip_prefix(b"data:") {
            data_lines.push(trim_ascii(rest));
        }
    }
    if data_lines.is_empty() {
        return None;
    }
    let payload = data_lines.join(&b'\n');
    let payload = trim_ascii(&payload);
    if payload == b"[DONE]" {
        return Some(ParsedEvent::Done);
    }
    match serde_json::from_slice::<Value>(payload) {
        Ok(value) => Some(ParsedEvent::Json(value)),
        Err(_) => None,
    }
}

pub(super) fn normalize_crlf(chunk: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(chunk.len());
    let mut iter = chunk.iter().copied().peekable();
    while let Some(byte) = iter.next() {
        if byte == b'\r' && iter.peek() == Some(&b'\n') {
            out.push(b'\n');
            iter.next();
        } else {
            out.push(byte);
        }
    }
    out
}

pub(super) fn trim_ascii(mut value: &[u8]) -> &[u8] {
    while value.first().is_some_and(u8::is_ascii_whitespace) {
        value = &value[1..];
    }
    while value.last().is_some_and(u8::is_ascii_whitespace) {
        value = &value[..value.len() - 1];
    }
    value
}

/// Gemini streams are newline-separated JSON objects (some proxies wrap them
/// in SSE `data:` frames), so split on single newlines.
pub(super) fn parse_gemini_events(chunk: &[u8], buffer: &mut Vec<u8>) -> Vec<ParsedEvent> {
    buffer.extend_from_slice(&normalize_crlf(chunk));
    let mut events = Vec::new();
    while let Some(marker) = buffer.iter().position(|byte| *byte == b'\n') {
        let line = buffer.drain(..marker).collect::<Vec<_>>();
        if !buffer.is_empty() {
            buffer.remove(0);
        }
        let line = trim_ascii(&line);
        if line.is_empty() {
            continue;
        }
        let line = line.strip_prefix(b"data:").map(trim_ascii).unwrap_or(line);
        if line == b"[DONE]" {
            events.push(ParsedEvent::Done);
            continue;
        }
        if let Ok(value) = serde_json::from_slice::<Value>(line) {
            events.push(ParsedEvent::Json(value));
        }
    }
    events
}

#[derive(Default, Clone)]
pub(super) struct UsageAcc {
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    cache_read: Option<i64>,
    cache_write: Option<i64>,
    reasoning: Option<i64>,
    /// True when `input_tokens` is the TOTAL including cache hits (Command
    /// Code emits `prompt_tokens_includes_cache`). Anthropic `input_tokens`
    /// must then be the non-cached part (hard pit 9).
    input_includes_cache: bool,
}

impl UsageAcc {
    fn merge_openai(&mut self, usage: &Value) {
        if let Some(value) = usage.get("prompt_tokens").and_then(Value::as_i64) {
            self.input_tokens = Some(value);
        }
        if usage
            .get(super::commandcode::INPUT_INCLUDES_CACHE)
            .and_then(Value::as_bool)
            == Some(true)
        {
            self.input_includes_cache = true;
        }
        if let Some(value) = usage.get("completion_tokens").and_then(Value::as_i64) {
            self.output_tokens = Some(value);
        }
        if let Some(details) = usage
            .get("prompt_tokens_details")
            .or_else(|| usage.get("input_tokens_details"))
            .filter(|value| value.is_object())
        {
            let cache_read = details
                .get("cached_tokens")
                .or_else(|| details.get("prompt_cache_hit_tokens"))
                .or_else(|| usage.get("prompt_cache_hit_tokens"))
                .and_then(Value::as_i64);
            if cache_read.is_some_and(|value| value > 0) {
                self.cache_read = cache_read;
            }
            if let Some(value) = details.get("cache_write_tokens").and_then(Value::as_i64)
                && value > 0
            {
                self.cache_write = Some(value);
            }
        }
        if let Some(details) = usage
            .get("completion_tokens_details")
            .filter(|value| value.is_object())
        {
            let reasoning = details
                .get("reasoning_tokens")
                .or_else(|| usage.get("reasoning_tokens"))
                .and_then(Value::as_i64);
            if reasoning.is_some_and(|value| value > 0) {
                self.reasoning = reasoning;
            }
        }
    }

    fn merge_responses(&mut self, usage: &Value) {
        if let Some(value) = usage.get("input_tokens").and_then(Value::as_i64) {
            self.input_tokens = Some(value);
        }
        if let Some(value) = usage.get("output_tokens").and_then(Value::as_i64) {
            self.output_tokens = Some(value);
        }
        // The Responses API nests cache hits in
        // `input_tokens_details.cached_tokens` (there is no Claude-style
        // top-level `cache_read_input_tokens`); some compatible providers
        // use `prompt_tokens_details` instead.
        if let Some(details) = usage
            .get("input_tokens_details")
            .or_else(|| usage.get("prompt_tokens_details"))
            .filter(|value| value.is_object())
        {
            if let Some(value) = details.get("cached_tokens").and_then(Value::as_i64)
                && value > 0
            {
                self.cache_read = Some(value);
            }
            if let Some(value) = details
                .get("cache_write_tokens")
                .or_else(|| details.get("cached_write_tokens"))
                .or_else(|| details.get("cache_creation_input_tokens"))
                .and_then(Value::as_i64)
                && value > 0
            {
                self.cache_write = Some(value);
            }
        }
        if let Some(value) = usage
            .get("output_tokens_details")
            .and_then(|details| details.get("reasoning_tokens"))
            .or_else(|| usage.get("reasoning_tokens"))
            .and_then(Value::as_i64)
        {
            self.reasoning = Some(value);
        }
    }

    fn merge_claude(&mut self, usage: &Value) {
        for (key, slot) in [
            ("input_tokens", &mut self.input_tokens),
            ("output_tokens", &mut self.output_tokens),
            ("cache_read_input_tokens", &mut self.cache_read),
            ("cache_creation_input_tokens", &mut self.cache_write),
        ] {
            if let Some(value) = usage.get(key).and_then(Value::as_i64) {
                *slot = Some(value);
            }
        }
    }

    fn merge_gemini(&mut self, usage: &Value) {
        if let Some(value) = usage.get("promptTokenCount").and_then(Value::as_i64) {
            self.input_tokens = Some(value);
        }
        if let Some(value) = usage.get("candidatesTokenCount").and_then(Value::as_i64) {
            self.output_tokens = Some(value);
        }
        if let Some(value) = usage.get("cachedContentTokenCount").and_then(Value::as_i64) {
            self.cache_read = Some(value);
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
pub(super) enum ConverterKind {
    ClaudeOpenai,
    ClaudeResponses,
    ClaudeGemini,
    ClaudePassthrough,
    /// OpenAI chat entry passthrough (Command Code silent conversion target).
    ChatPassthrough,
    ResponsesOpenai,
    ResponsesClaude,
    ResponsesGemini,
    ResponsesPassthrough,
}

#[derive(Clone)]
pub(super) struct FunctionState {
    item_id: String,
    output_index: i64,
    call_id: String,
    name: String,
    arguments: Vec<String>,
}

/// Stateful converter turning an upstream SSE stream into the entry protocol's
/// SSE events — port of the Python `ClaudeSSEConverter` / `ResponsesSSEConverter`
/// hierarchies. Feed upstream chunks with [`MappedStreamConverter::feed`] and
/// yield the converted bytes immediately, so the client sees a live stream.
pub struct MappedStreamConverter {
    kind: ConverterKind,
    model: String,
    buffer: Vec<u8>,
    started: bool,
    finished: bool,
    message_id: String,
    request_id: String,
    response_id: String,
    created_at: i64,
    usage: UsageAcc,
    // claude side
    open_blocks: BTreeMap<i64, String>,
    next_index: i64,
    pending_finish_reason: Option<String>,
    tool_index_by_id: HashMap<String, i64>,
    tool_index_by_upstream: HashMap<i64, i64>,
    item_index_by_id: HashMap<String, i64>,
    // responses side
    output: Vec<Value>,
    next_output_index: i64,
    text_item_id: Option<String>,
    text_index: Option<i64>,
    text_content: Vec<String>,
    functions: HashMap<String, FunctionState>,
    next_sequence: i64,
    status: String,
    tool_key_by_id: HashMap<String, String>,
    tool_key_by_index: HashMap<i64, String>,
    tool_key_index: i64,
    open_block_types: HashMap<i64, String>,
    function_key_by_block: HashMap<i64, String>,
    /// Accumulated thinking text per open block, used to synthesize the
    /// Anthropic `signature_delta` when the upstream provides no signature
    /// (Command Code / OpenAI-compatible reasoning paths, hard pit 8).
    thinking_text: HashMap<i64, String>,
    // DSML state
    dsml_text_buffer: String,
    dsml_start_marker: Option<String>,
    dsml_end_marker: Option<String>,
    dsml_tool_index: i64,
}

impl MappedStreamConverter {
    pub fn new(entry: &str, upstream_protocol: &str, model: &str) -> Result<Self> {
        use ConversionStrategy::*;
        let kind = match ConversionStrategy::for_pair(entry, upstream_protocol) {
            Some(ClaudeToChat) => ConverterKind::ClaudeOpenai,
            Some(ClaudeToResponses) => ConverterKind::ClaudeResponses,
            Some(Passthrough) if entry == "claude" => ConverterKind::ClaudePassthrough,
            Some(Passthrough) if entry == "openai_compatible" => ConverterKind::ChatPassthrough,
            Some(ClaudeToGemini) => ConverterKind::ClaudeGemini,
            Some(ResponsesToChat) => ConverterKind::ResponsesOpenai,
            Some(Passthrough) => ConverterKind::ResponsesPassthrough,
            Some(ResponsesToClaude) => ConverterKind::ResponsesClaude,
            Some(ResponsesToGemini) => ConverterKind::ResponsesGemini,
            Some(ClaudeToCommandCode)
            | Some(ResponsesToCommandCode)
            | Some(ChatToCommandCode) => bail!(
                "Command Code streams are decoded to openai_compatible before conversion"
            ),
            None => bail!("Unsupported upstream protocol: {upstream_protocol}"),
        };
        Ok(Self {
            kind,
            model: model.to_owned(),
            buffer: Vec::new(),
            started: false,
            finished: false,
            message_id: new_id("msg"),
            request_id: new_id("req"),
            response_id: new_id("resp"),
            created_at: unix_timestamp(),
            usage: UsageAcc::default(),
            open_blocks: BTreeMap::new(),
            next_index: 0,
            pending_finish_reason: None,
            tool_index_by_id: HashMap::new(),
            tool_index_by_upstream: HashMap::new(),
            item_index_by_id: HashMap::new(),
            output: Vec::new(),
            next_output_index: 0,
            text_item_id: None,
            text_index: None,
            text_content: Vec::new(),
            functions: HashMap::new(),
            next_sequence: 0,
            status: "completed".into(),
            tool_key_by_id: HashMap::new(),
            tool_key_by_index: HashMap::new(),
            tool_key_index: 0,
            open_block_types: HashMap::new(),
            function_key_by_block: HashMap::new(),
            thinking_text: HashMap::new(),
            dsml_text_buffer: String::new(),
            dsml_start_marker: None,
            dsml_end_marker: None,
            dsml_tool_index: 0,
        })
    }

    pub fn finished(&self) -> bool {
        self.finished
    }

    pub fn usage(&self) -> (Option<i64>, Option<i64>, Option<i64>, Option<i64>) {
        (
            self.usage.input_tokens,
            self.usage.output_tokens,
            self.usage.cache_read,
            self.usage.cache_write,
        )
    }

    pub fn feed(&mut self, chunk: &[u8]) -> Vec<u8> {
        if self.finished {
            return Vec::new();
        }
        let passthrough = matches!(
            self.kind,
            ConverterKind::ClaudePassthrough
                | ConverterKind::ChatPassthrough
                | ConverterKind::ResponsesPassthrough
        );
        if passthrough {
            // Same-protocol mapped streams forward the raw bytes untouched,
            // but usage must still be observed: a model-renaming mapping on
            // a claude/responses channel would otherwise record zero tokens
            // in the logs.
            self.observe_passthrough_usage(chunk);
            return chunk.to_vec();
        }
        self.feed_impl(chunk)
    }

    /// Parse the raw chunk for usage events (the same shapes the transparent
    /// path scans) without altering the forwarded bytes.
    fn observe_passthrough_usage(&mut self, chunk: &[u8]) {
        self.buffer.extend_from_slice(&normalize_crlf(chunk));
        for block in split_sse_blocks(&mut self.buffer) {
            if let Some(ParsedEvent::Json(value)) = sse_block_events(&block) {
                let usage = match self.kind {
                    ConverterKind::ClaudePassthrough => value
                        .get("message")
                        .and_then(|message| message.get("usage"))
                        .or_else(|| value.get("usage")),
                    ConverterKind::ChatPassthrough => value.get("usage"),
                    _ => value
                        .get("usage")
                        .or_else(|| value.get("response").and_then(|item| item.get("usage"))),
                };
                if let Some(usage) = usage.filter(|item| item.is_object()) {
                    match self.kind {
                        ConverterKind::ClaudePassthrough => self.usage.merge_claude(usage),
                        ConverterKind::ChatPassthrough => self.usage.merge_openai(usage),
                        _ => self.usage.merge_responses(usage),
                    }
                }
            }
        }
    }

    fn feed_impl(&mut self, chunk: &[u8]) -> Vec<u8> {
        // For non-gemini kinds the buffer must include the new chunk.
        let events = if matches!(
            self.kind,
            ConverterKind::ClaudeGemini | ConverterKind::ResponsesGemini
        ) {
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
        let mut output = Vec::new();
        for event in events {
            match event {
                ParsedEvent::Done => match self.kind {
                    ConverterKind::ClaudeOpenai
                    | ConverterKind::ClaudeResponses
                    | ConverterKind::ClaudeGemini => {
                        let reason = self
                            .pending_finish_reason
                            .clone()
                            .unwrap_or_else(|| "end_turn".into());
                        output.extend(self.finish_claude(&reason));
                    }
                    ConverterKind::ResponsesOpenai => {
                        output.extend(self.drain_dsml_text(None, true));
                        output.extend(self.finish_responses(self.status.clone()));
                    }
                    _ => {
                        output.extend(self.finish_responses(self.status.clone()));
                    }
                },
                ParsedEvent::Json(value) => output.extend(self.consume(&value)),
            }
        }
        output
    }

    pub fn flush(&mut self) -> Vec<u8> {
        if self.finished {
            return Vec::new();
        }
        match self.kind {
            ConverterKind::ClaudeOpenai
            | ConverterKind::ClaudeResponses
            | ConverterKind::ClaudeGemini => {
                let reason = self
                    .pending_finish_reason
                    .clone()
                    .unwrap_or_else(|| "end_turn".into());
                self.finish_claude(&reason)
            }
            ConverterKind::ResponsesOpenai => {
                let mut output = self.drain_dsml_text(None, true);
                output.extend(self.finish_responses(self.status.clone()));
                output
            }
            ConverterKind::ResponsesClaude | ConverterKind::ResponsesGemini => {
                self.finish_responses(self.status.clone())
            }
            ConverterKind::ClaudePassthrough
            | ConverterKind::ChatPassthrough
            | ConverterKind::ResponsesPassthrough => Vec::new(),
        }
    }

    pub fn error_event(&mut self, message: &str) -> Vec<u8> {
        match self.kind {
            ConverterKind::ClaudePassthrough => sse(
                "error",
                &json!({
                    "type": "error",
                    "error": {"type": GATEWAY_ERROR_TYPE, "message": message},
                    "request_id": self.request_id,
                }),
            ),
            ConverterKind::ResponsesPassthrough => {
                let mut output = self.ensure_start_responses();
                output.extend(self.responses_event("response.failed", &json!({
                    "type": "response.failed",
                    "response": self.envelope("failed", Some(json!({"code": "gateway_error", "message": message}))),
                })));
                self.finished = true;
                output
            }
            ConverterKind::ChatPassthrough => {
                self.finished = true;
                sse(
                    "error",
                    &json!({
                        "error": {
                            "type": GATEWAY_ERROR_TYPE,
                            "code": "gateway_error",
                            "message": message,
                        }
                    }),
                )
            }
            ConverterKind::ResponsesOpenai => {
                let mut output = self.drain_dsml_text(None, true);
                output.extend(self.error_event_responses(message));
                output
            }
            _ => {
                if matches!(
                    self.kind,
                    ConverterKind::ClaudeOpenai
                        | ConverterKind::ClaudeResponses
                        | ConverterKind::ClaudeGemini
                ) {
                    if !self.started {
                        self.ensure_start_claude();
                    }
                    self.finished = true;
                    return sse(
                        "error",
                        &json!({
                            "type": "error",
                            "error": {"type": GATEWAY_ERROR_TYPE, "message": message},
                            "request_id": self.request_id,
                        }),
                    );
                }
                self.error_event_responses(message)
            }
        }
    }

    // -- claude-side helpers ----------------------------------------------

    fn ensure_start_claude(&mut self) -> Vec<u8> {
        if self.started {
            return Vec::new();
        }
        self.started = true;
        sse(
            "message_start",
            &json!({
                "type": "message_start",
                "message": {
                    "id": self.message_id,
                    "type": "message",
                    "role": "assistant",
                    "model": self.model,
                    "content": [],
                    "stop_reason": Value::Null,
                    "stop_sequence": Value::Null,
                    "usage": claude_usage(Some(0), Some(0), None, None),
                },
            }),
        )
    }

    fn start_block(&mut self, index: i64, block: &Value) -> Vec<u8> {
        if self.open_blocks.contains_key(&index) {
            return Vec::new();
        }
        self.open_blocks.insert(
            index,
            block
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("text")
                .to_owned(),
        );
        sse(
            "content_block_start",
            &json!({"type": "content_block_start", "index": index, "content_block": block}),
        )
    }

    fn delta(&self, index: i64, delta: &Value) -> Vec<u8> {
        sse(
            "content_block_delta",
            &json!({"type": "content_block_delta", "index": index, "delta": delta}),
        )
    }

    fn stop_block(&mut self, index: i64) -> Vec<u8> {
        if !self.open_blocks.contains_key(&index) {
            return Vec::new();
        }
        let block_type = self
            .open_blocks
            .remove(&index)
            .unwrap_or_else(|| "text".to_owned());
        let mut output = Vec::new();
        if block_type == "thinking" {
            // Hard pit 8: the upstream reasoning feed carries no signature,
            // but Anthropic clients expect a `signature_delta` before the
            // thinking block closes. Synthesize a deterministic placeholder
            // over the accumulated thinking text (never a secret).
            use base64::Engine as _;
            use sha2::{Digest, Sha256};
            let text = self.thinking_text.remove(&index).unwrap_or_default();
            let mut hasher = Sha256::new();
            hasher.update(text.as_bytes());
            hasher.update(index.to_le_bytes());
            let signature = base64::engine::general_purpose::STANDARD.encode(hasher.finalize());
            output.extend(self.delta(
                index,
                &json!({"type": "signature_delta", "signature": signature}),
            ));
        }
        output.extend(sse(
            "content_block_stop",
            &json!({"type": "content_block_stop", "index": index}),
        ));
        output
    }

    /// 追加一段 thinking：同一段思考必须复用同一个 content block。上游每个
    /// delta 都开一个新 block 会让 Claude 客户端看到 N 段独立的思考（真实
    /// 观测：每个 token 一个 `content_block_start`）。
    fn thinking_delta(&mut self, text: &str) -> Vec<u8> {
        let index = match self.current_block_of_type("thinking") {
            Some(index) => index,
            None => {
                let index = self.next_block_index();
                let mut output =
                    self.start_block(index, &json!({"type": "thinking", "thinking": ""}));
                output.extend(self.emit_thinking_delta(index, text));
                return output;
            }
        };
        self.emit_thinking_delta(index, text)
    }

    fn emit_thinking_delta(&mut self, index: i64, text: &str) -> Vec<u8> {
        self.thinking_text
            .entry(index)
            .or_default()
            .push_str(text);
        self.delta(
            index,
            &json!({"type": "thinking_delta", "thinking": text}),
        )
    }

    /// Claude 的 content block 必须顺序开闭：非 thinking 内容开始前先关闭
    /// 打开中的 thinking block（`stop_block` 会补上 signature_delta）。
    fn close_thinking(&mut self) -> Vec<u8> {
        self.current_block_of_type("thinking")
            .map(|index| self.stop_block(index))
            .unwrap_or_default()
    }

    fn current_block_of_type(&self, block_type: &str) -> Option<i64> {
        self.open_blocks
            .iter()
            .find(|(_, current)| current.as_str() == block_type)
            .map(|(index, _)| *index)
    }

    /// 单调递增，且**不复用**已关闭块的 index：Anthropic 客户端按 index
    /// 累积 content，thinking 关闭后若 text 又用回 0，会让文本覆盖思考块。
    fn next_block_index(&mut self) -> i64 {
        while self.open_blocks.contains_key(&self.next_index) {
            self.next_index += 1;
        }
        let index = self.next_index;
        self.next_index += 1;
        index
    }

    fn anthropic_input_tokens(&self) -> Option<i64> {
        if self.usage.input_includes_cache {
            // Hard pit 9: OpenAI/CC `prompt_tokens` is a total; Anthropic
            // wants the non-cached part only.
            self.usage.input_tokens.map(|total| {
                (total
                    - self.usage.cache_read.unwrap_or(0)
                    - self.usage.cache_write.unwrap_or(0))
                .max(0)
            })
        } else {
            self.usage.input_tokens
        }
    }

    fn finish_claude(&mut self, stop_reason: &str) -> Vec<u8> {
        if self.finished {
            return Vec::new();
        }
        self.finished = true;
        let mut output = Vec::new();
        for index in self.open_blocks.keys().copied().collect::<Vec<_>>() {
            output.extend(self.stop_block(index));
        }
        output.extend(sse(
            "message_delta",
            &json!({
                "type": "message_delta",
                "delta": {"stop_reason": stop_reason, "stop_sequence": Value::Null},
                "usage": claude_usage(
                    self.anthropic_input_tokens(),
                    self.usage.output_tokens,
                    self.usage.cache_read,
                    self.usage.cache_write,
                ),
            }),
        ));
        output.extend(sse("message_stop", &json!({"type": "message_stop"})));
        output
    }

    fn merge_openai_usage(&mut self, usage: &Value) {
        self.usage.merge_openai(usage);
    }

    // -- responses-side helpers -------------------------------------------

    fn responses_event(&mut self, _event: &str, payload: &Value) -> Vec<u8> {
        let mut enriched = payload.clone();
        enriched["sequence_number"] = json!(self.next_sequence);
        self.next_sequence += 1;
        sse_data(&enriched)
    }

    fn envelope(&self, status: &str, error: Option<Value>) -> Value {
        responses_envelope(
            &self.model,
            &self.response_id,
            self.created_at,
            status,
            self.output.clone(),
            responses_usage(
                self.usage.input_tokens,
                self.usage.output_tokens,
                self.usage.cache_read,
                self.usage.reasoning,
            ),
            error,
        )
    }

    fn ensure_start_responses(&mut self) -> Vec<u8> {
        if self.started {
            return Vec::new();
        }
        self.started = true;
        self.responses_event(
            "response.created",
            &json!({"type": "response.created", "response": self.envelope("in_progress", None)}),
        )
    }

    fn open_text_item(&mut self) -> Vec<u8> {
        if self.text_item_id.is_some() {
            return Vec::new();
        }
        let index = self.next_output_index;
        self.next_output_index += 1;
        let item_id = new_id("msg");
        self.text_item_id = Some(item_id.clone());
        self.text_index = Some(index);
        self.text_content.clear();
        let item = json!({
            "type": "message",
            "id": item_id,
            "status": "in_progress",
            "role": "assistant",
            "content": [],
        });
        let mut output = self.responses_event(
            "response.output_item.added",
            &json!({"type": "response.output_item.added", "output_index": index, "item": item}),
        );
        output.extend(self.responses_event(
            "response.content_part.added",
            &json!({
                "type": "response.content_part.added",
                "item_id": item_id,
                "output_index": index,
                "content_index": 0,
                "part": {"type": "output_text", "text": "", "annotations": []},
            }),
        ));
        output
    }

    fn append_text_delta(&mut self, delta_text: &str) -> Vec<u8> {
        if delta_text.is_empty() {
            return Vec::new();
        }
        let mut output = self.ensure_start_responses();
        if self.text_item_id.is_none() {
            output.extend(self.open_text_item());
        }
        output.extend(self.responses_event(
            "response.output_text.delta",
            &json!({
                "type": "response.output_text.delta",
                "item_id": self.text_item_id,
                "output_index": self.text_index,
                "content_index": 0,
                "delta": delta_text,
            }),
        ));
        self.text_content.push(delta_text.to_owned());
        output
    }

    fn close_text_item(&mut self) -> Vec<u8> {
        let Some(item_id) = self.text_item_id.clone() else {
            return Vec::new();
        };
        let text = self.text_content.join("");
        let item = json!({
            "type": "message",
            "id": item_id,
            "status": "completed",
            "role": "assistant",
            "content": [{"type": "output_text", "text": text, "annotations": []}],
        });
        let mut output = self.responses_event(
            "response.output_text.done",
            &json!({
                "type": "response.output_text.done",
                "item_id": item_id,
                "output_index": self.text_index,
                "content_index": 0,
                "text": text,
            }),
        );
        output.extend(self.responses_event(
            "response.content_part.done",
            &json!({
                "type": "response.content_part.done",
                "item_id": item_id,
                "output_index": self.text_index,
                "content_index": 0,
                "part": {"type": "output_text", "text": text, "annotations": []},
            }),
        ));
        output.extend(self.responses_event(
            "response.output_item.done",
            &json!({
                "type": "response.output_item.done",
                "output_index": self.text_index,
                "item": item,
            }),
        ));
        self.output.push(item);
        self.text_item_id = None;
        self.text_index = None;
        self.text_content.clear();
        output
    }

    fn open_function_item(&mut self, key: String, call_id: &str, name: &str) -> Vec<u8> {
        if self.functions.contains_key(&key) {
            return Vec::new();
        }
        let index = self.next_output_index;
        self.next_output_index += 1;
        let item_id = new_id("fc");
        self.functions.insert(
            key.clone(),
            FunctionState {
                item_id: item_id.clone(),
                output_index: index,
                call_id: call_id.to_owned(),
                name: name.to_owned(),
                arguments: Vec::new(),
            },
        );
        let item = json!({
            "type": "function_call",
            "id": item_id,
            "call_id": call_id,
            "name": name,
            "arguments": "",
            "status": "in_progress",
        });
        self.responses_event(
            "response.output_item.added",
            &json!({"type": "response.output_item.added", "output_index": index, "item": item}),
        )
    }

    fn append_function_arguments(&mut self, key: &str, delta: &str) -> Vec<u8> {
        if delta.is_empty() {
            return Vec::new();
        }
        let (item_id, output_index) = match self.functions.get_mut(key) {
            Some(state) => {
                state.arguments.push(delta.to_owned());
                (state.item_id.clone(), state.output_index)
            }
            None => return Vec::new(),
        };
        self.responses_event(
            "response.function_call_arguments.delta",
            &json!({
                "type": "response.function_call_arguments.delta",
                "item_id": item_id,
                "output_index": output_index,
                "delta": delta,
            }),
        )
    }

    fn close_function_item(&mut self, key: &str) -> Vec<u8> {
        let Some(state) = self.functions.remove(key) else {
            return Vec::new();
        };
        let arguments = state.arguments.join("");
        let item = json!({
            "type": "function_call",
            "id": state.item_id,
            "call_id": state.call_id,
            "name": state.name,
            "arguments": arguments,
            "status": "completed",
        });
        let mut output = self.responses_event(
            "response.function_call_arguments.done",
            &json!({
                "type": "response.function_call_arguments.done",
                "item_id": state.item_id,
                "output_index": state.output_index,
                "arguments": arguments,
            }),
        );
        output.extend(self.responses_event(
            "response.output_item.done",
            &json!({
                "type": "response.output_item.done",
                "output_index": state.output_index,
                "item": item,
            }),
        ));
        self.output.push(item);
        output
    }

    fn finish_responses(&mut self, status: String) -> Vec<u8> {
        if self.finished {
            return Vec::new();
        }
        self.finished = true;
        let mut output = self.ensure_start_responses();
        if self.text_item_id.is_some() {
            output.extend(self.close_text_item());
        }
        for key in self.functions.keys().cloned().collect::<Vec<_>>() {
            output.extend(self.close_function_item(&key));
        }
        output.extend(self.responses_event(
            "response.completed",
            &json!({"type": "response.completed", "response": self.envelope(&status, None)}),
        ));
        output
    }

    fn error_event_responses(&mut self, message: &str) -> Vec<u8> {
        if self.finished {
            return Vec::new();
        }
        self.finished = true;
        let mut output = self.ensure_start_responses();
        if self.text_item_id.is_some() {
            output.extend(self.close_text_item());
        }
        for key in self.functions.keys().cloned().collect::<Vec<_>>() {
            output.extend(self.close_function_item(&key));
        }
        output.extend(self.responses_event(
            "response.failed",
            &json!({
                "type": "response.failed",
                "response": self.envelope(
                    "failed",
                    Some(json!({"code": "gateway_error", "message": message})),
                ),
            }),
        ));
        output
    }

    // -- DSML drain (ResponsesOpenai) -------------------------------------

    fn emit_dsml_calls(&mut self, calls: &[Value]) -> Vec<u8> {
        let mut output = self.close_text_item();
        for call in calls {
            let key = format!("dsml_{}", self.dsml_tool_index);
            self.dsml_tool_index += 1;
            output.extend(self.open_function_item(
                key.clone(),
                &new_id("call"),
                call.get("name").and_then(Value::as_str).unwrap_or(""),
            ));
            output.extend(self.append_function_arguments(
                &key,
                call.get("arguments").and_then(Value::as_str).unwrap_or(""),
            ));
            output.extend(self.close_function_item(&key));
        }
        output
    }

    fn drain_dsml_text(&mut self, text: Option<&str>, is_final: bool) -> Vec<u8> {
        if let Some(text) = text {
            self.dsml_text_buffer.push_str(text);
        }
        let mut output = Vec::new();
        loop {
            if self.dsml_text_buffer.is_empty() {
                break;
            }
            if let Some(end_marker) = self.dsml_end_marker.clone() {
                match self.dsml_text_buffer.find(&end_marker) {
                    None => {
                        if is_final {
                            let raw = format!(
                                "{}{}",
                                self.dsml_start_marker.clone().unwrap_or_default(),
                                self.dsml_text_buffer
                            );
                            output.extend(self.append_text_delta(&raw));
                            self.dsml_text_buffer.clear();
                            self.dsml_start_marker = None;
                            self.dsml_end_marker = None;
                        }
                        break;
                    }
                    Some(end_index) => {
                        let block = self.dsml_text_buffer[..end_index].to_owned();
                        let raw = format!(
                            "{}{}{}",
                            self.dsml_start_marker.clone().unwrap_or_default(),
                            block,
                            end_marker
                        );
                        self.dsml_text_buffer =
                            self.dsml_text_buffer[end_index + end_marker.len()..].to_owned();
                        self.dsml_start_marker = None;
                        self.dsml_end_marker = None;
                        let calls = parse_dsml_invocations(&block);
                        if !calls.is_empty() {
                            output.extend(self.emit_dsml_calls(&calls));
                        } else {
                            output.extend(self.append_text_delta(&raw));
                        }
                        continue;
                    }
                }
            }
            if let Some(noise_index) = find_dsml_noise_start(&self.dsml_text_buffer) {
                if noise_index > 0 {
                    let prefix = self.dsml_text_buffer[..noise_index].to_owned();
                    output.extend(self.append_text_delta(&prefix));
                }
                let rest = &self.dsml_text_buffer[noise_index..];
                match rest.find('\n') {
                    Some(line_end) => {
                        self.dsml_text_buffer = rest[line_end + 1..].to_owned();
                        continue;
                    }
                    None => {
                        if is_final {
                            self.dsml_text_buffer.clear();
                        } else {
                            self.dsml_text_buffer = rest.to_owned();
                        }
                        break;
                    }
                }
            }
            if let Some((start_index, start_marker, end_marker)) =
                find_dsml_start(&self.dsml_text_buffer)
            {
                if start_index > 0 {
                    let prefix = self.dsml_text_buffer[..start_index].to_owned();
                    output.extend(self.append_text_delta(&prefix));
                }
                self.dsml_text_buffer =
                    self.dsml_text_buffer[start_index + start_marker.len()..].to_owned();
                self.dsml_start_marker = Some(start_marker);
                self.dsml_end_marker = Some(end_marker);
                continue;
            }
            if is_final {
                let tail = self.dsml_text_buffer.clone();
                output.extend(self.append_text_delta(&tail));
                self.dsml_text_buffer.clear();
                break;
            }
            let keep = dsml_partial_prefix_length(&self.dsml_text_buffer);
            let safe_length = self.dsml_text_buffer.len() - keep;
            if safe_length > 0 {
                let safe = self.dsml_text_buffer[..safe_length].to_owned();
                output.extend(self.append_text_delta(&safe));
                self.dsml_text_buffer = self.dsml_text_buffer[safe_length..].to_owned();
            }
            break;
        }
        output
    }

    // -- per-event consumption --------------------------------------------

    fn consume(&mut self, event: &Value) -> Vec<u8> {
        match self.kind {
            ConverterKind::ClaudeOpenai => self.consume_claude_openai(event),
            ConverterKind::ClaudeResponses => self.consume_claude_responses(event),
            ConverterKind::ClaudeGemini => self.consume_gemini_to_claude(event),
            ConverterKind::ResponsesOpenai => self.consume_responses_openai(event),
            ConverterKind::ResponsesClaude => self.consume_responses_claude(event),
            ConverterKind::ResponsesGemini => self.consume_gemini_to_responses(event),
            ConverterKind::ClaudePassthrough
            | ConverterKind::ChatPassthrough
            | ConverterKind::ResponsesPassthrough => Vec::new(),
        }
    }

    fn consume_claude_openai(&mut self, event: &Value) -> Vec<u8> {
        let mut output = self.ensure_start_claude();
        if let Some(usage) = event.get("usage").filter(|value| value.is_object()) {
            self.merge_openai_usage(usage);
        }
        let Some(choice) = event
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|choices| choices.first())
        else {
            return output;
        };
        let delta = choice.get("delta").unwrap_or(&Value::Null);
        let reasoning = delta
            .get("reasoning_content")
            .or_else(|| delta.get("reasoning"));
        if let Some(reasoning) = reasoning
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
        {
            output.extend(self.thinking_delta(reasoning));
        }
        let text = chat_message_text(delta.get("content").unwrap_or(&Value::Null));
        if !text.is_empty() {
            let index = match self.current_block_of_type("text") {
                Some(index) => index,
                None => {
                    output.extend(self.close_thinking());
                    let index = self.next_block_index();
                    output.extend(self.start_block(index, &json!({"type": "text", "text": ""})));
                    index
                }
            };
            output.extend(self.delta(index, &json!({"type": "text_delta", "text": text})));
        }
        if let Some(tool_calls) = delta.get("tool_calls").and_then(Value::as_array) {
            for tool_call in tool_calls {
                let tool_id = tool_call.get("id").and_then(Value::as_str);
                let upstream_index = tool_call.get("index").and_then(Value::as_i64);
                let index = tool_id
                    .and_then(|id| self.tool_index_by_id.get(id).copied())
                    .or_else(|| {
                        upstream_index
                            .and_then(|index| self.tool_index_by_upstream.get(&index).copied())
                    });
                let index =
                    match index {
                        Some(index) => index,
                        None => {
                            output.extend(self.close_thinking());
                            let index = self.next_block_index();
                            if let Some(tool_id) = tool_id {
                                self.tool_index_by_id.insert(tool_id.to_owned(), index);
                            }
                            if let Some(upstream_index) = upstream_index {
                                self.tool_index_by_upstream.insert(upstream_index, index);
                            }
                            let function = tool_call.get("function").unwrap_or(&Value::Null);
                            output.extend(self.start_block(index, &json!({
                            "type": "tool_use",
                            "id": tool_id.map(str::to_owned).unwrap_or_else(|| new_id("toolu")),
                            "name": function.get("name").and_then(Value::as_str).unwrap_or(""),
                            "input": {},
                        })));
                            index
                        }
                    };
                if let Some(arguments) = tool_call
                    .get("function")
                    .and_then(|function| function.get("arguments"))
                    .and_then(Value::as_str)
                    .filter(|arguments| !arguments.is_empty())
                {
                    output.extend(self.delta(
                        index,
                        &json!({"type": "input_json_delta", "partial_json": arguments}),
                    ));
                }
            }
        }
        if let Some(finish_reason) = choice.get("finish_reason").and_then(Value::as_str) {
            self.pending_finish_reason = Some(stop_reason_openai(finish_reason).to_owned());
        }
        output
    }

    fn consume_claude_responses(&mut self, event: &Value) -> Vec<u8> {
        let mut output = self.ensure_start_claude();
        let event_type = event.get("type").and_then(Value::as_str).unwrap_or("");
        match event_type {
            "response.output_item.added" => {
                let item = event.get("item").unwrap_or(&Value::Null);
                if item.get("type").and_then(Value::as_str) == Some("function_call") {
                    let index = self.next_block_index();
                    let call_id = item
                        .get("call_id")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                        .unwrap_or_else(|| new_id("toolu"));
                    let key = item
                        .get("id")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                        .unwrap_or_else(|| call_id.clone());
                    self.item_index_by_id.insert(key, index);
                    self.next_index = index + 1;
                    output.extend(self.start_block(
                        index,
                        &json!({
                            "type": "tool_use",
                            "id": call_id,
                            "name": item.get("name").and_then(Value::as_str).unwrap_or(""),
                            "input": {},
                        }),
                    ));
                }
            }
            "response.function_call_arguments.delta" => {
                if let Some(index) = event
                    .get("item_id")
                    .and_then(Value::as_str)
                    .and_then(|item_id| self.item_index_by_id.get(item_id).copied())
                {
                    output.extend(self.delta(index, &json!({
                        "type": "input_json_delta",
                        "partial_json": event.get("delta").and_then(Value::as_str).unwrap_or(""),
                    })));
                }
            }
            "response.output_text.delta" => {
                let index = match self.current_block_of_type("text") {
                    Some(index) => index,
                    None => {
                        let index = self.next_block_index();
                        output
                            .extend(self.start_block(index, &json!({"type": "text", "text": ""})));
                        index
                    }
                };
                output.extend(self.delta(
                    index,
                    &json!({
                        "type": "text_delta",
                        "text": event.get("delta").and_then(Value::as_str).unwrap_or(""),
                    }),
                ));
            }
            "response.output_text.done" => {
                if let Some(index) = self.current_block_of_type("text") {
                    output.extend(self.stop_block(index));
                }
            }
            "response.function_call_arguments.done" => {
                if let Some(index) = event
                    .get("item_id")
                    .and_then(Value::as_str)
                    .and_then(|item_id| self.item_index_by_id.get(item_id).copied())
                {
                    output.extend(self.stop_block(index));
                }
            }
            "response.completed" => {
                let response = event.get("response").unwrap_or(&Value::Null);
                if let Some(usage) = response.get("usage").filter(|value| value.is_object()) {
                    self.usage.merge_responses(usage);
                }
                output.extend(self.finish_claude("end_turn"));
            }
            _ => {}
        }
        output
    }

    fn consume_gemini_to_claude(&mut self, event: &Value) -> Vec<u8> {
        let mut output = self.ensure_start_claude();
        let Some(candidates) = event.get("candidates").and_then(Value::as_array) else {
            self.usage
                .merge_gemini(event.get("usageMetadata").unwrap_or(&Value::Null));
            return output;
        };
        let Some(candidate) = candidates.first() else {
            self.usage
                .merge_gemini(event.get("usageMetadata").unwrap_or(&Value::Null));
            return output;
        };
        let parts = candidate
            .get("content")
            .and_then(|content| content.get("parts"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for part in &parts {
            if let Some(thought) = part.get("thought").filter(|value| !value.is_null()) {
                output.extend(self.thinking_delta(thought.as_str().unwrap_or_default()));
            } else if part.get("text").is_some() {
                let index = match self.current_block_of_type("text") {
                    Some(index) => index,
                    None => {
                        output.extend(self.close_thinking());
                        let index = self.next_block_index();
                        output
                            .extend(self.start_block(index, &json!({"type": "text", "text": ""})));
                        index
                    }
                };
                output.extend(self.delta(
                    index,
                    &json!({
                        "type": "text_delta",
                        "text": part.get("text").and_then(Value::as_str).unwrap_or(""),
                    }),
                ));
            }
            if let Some(function_call) = part.get("functionCall").filter(|value| value.is_object())
            {
                let index = self.next_block_index();
                output.extend(self.start_block(
                    index,
                    &json!({
                        "type": "tool_use",
                        "id": new_id("toolu"),
                        "name": function_call.get("name").and_then(Value::as_str).unwrap_or(""),
                        "input": {},
                    }),
                ));
                let arguments =
                    serde_json::to_string(function_call.get("args").unwrap_or(&Value::Null))
                        .unwrap_or_else(|_| "{}".into());
                if !arguments.is_empty() {
                    output.extend(self.delta(
                        index,
                        &json!({"type": "input_json_delta", "partial_json": arguments}),
                    ));
                }
                output.extend(self.stop_block(index));
            }
        }
        self.usage
            .merge_gemini(event.get("usageMetadata").unwrap_or(&Value::Null));
        if let Some(finish_reason) = candidate.get("finishReason").and_then(Value::as_str) {
            output.extend(self.finish_claude(stop_reason_gemini(finish_reason)));
        }
        output
    }

    fn consume_responses_openai(&mut self, event: &Value) -> Vec<u8> {
        let mut output = self.ensure_start_responses();
        if let Some(usage) = event.get("usage").filter(|value| value.is_object()) {
            self.merge_openai_usage(usage);
        }
        let Some(choice) = event
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|choices| choices.first())
        else {
            return output;
        };
        let delta = choice.get("delta").unwrap_or(&Value::Null);
        let text = chat_message_text(delta.get("content").unwrap_or(&Value::Null));
        if !text.is_empty() {
            output.extend(self.drain_dsml_text(Some(&text), false));
        }
        if let Some(tool_calls) = delta.get("tool_calls").and_then(Value::as_array) {
            for tool_call in tool_calls {
                let function = tool_call.get("function").unwrap_or(&Value::Null);
                let tool_id = tool_call.get("id").and_then(Value::as_str);
                let upstream_index = tool_call.get("index").and_then(Value::as_i64);
                let key = tool_id
                    .and_then(|id| self.tool_key_by_id.get(id).cloned())
                    .or_else(|| {
                        upstream_index.and_then(|index| self.tool_key_by_index.get(&index).cloned())
                    });
                let key = match key {
                    Some(key) => key,
                    None => {
                        let key = format!("tool_{}", self.tool_key_index);
                        self.tool_key_index += 1;
                        if let Some(tool_id) = tool_id {
                            self.tool_key_by_id.insert(tool_id.to_owned(), key.clone());
                        }
                        if let Some(upstream_index) = upstream_index {
                            self.tool_key_by_index.insert(upstream_index, key.clone());
                        }
                        let call_id = tool_id.map(str::to_owned).unwrap_or_else(|| new_id("call"));
                        let name = function.get("name").and_then(Value::as_str).unwrap_or("");
                        output.extend(self.open_function_item(key.clone(), &call_id, name));
                        key
                    }
                };
                if let Some(arguments) = function.get("arguments").and_then(Value::as_str) {
                    output.extend(self.append_function_arguments(&key, arguments));
                }
            }
        }
        if choice.get("finish_reason").and_then(Value::as_str) == Some("length") {
            self.status = "incomplete".into();
        }
        output
    }

    fn consume_responses_claude(&mut self, event: &Value) -> Vec<u8> {
        let mut output = self.ensure_start_responses();
        let event_type = event.get("type").and_then(Value::as_str).unwrap_or("");
        match event_type {
            "message_start" => {
                if let Some(usage) = event
                    .get("message")
                    .and_then(|message| message.get("usage"))
                    .filter(|value| value.is_object())
                {
                    self.usage.merge_claude(usage);
                }
            }
            "content_block_start" => {
                let index = event.get("index").and_then(Value::as_i64).unwrap_or(0);
                let block = event.get("content_block").unwrap_or(&Value::Null);
                let block_type = block
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned();
                self.open_block_types.insert(index, block_type.clone());
                if block_type == "tool_use" {
                    let key = format!("block_{}", index);
                    self.function_key_by_block.insert(index, key.clone());
                    output.extend(self.open_function_item(
                        key,
                        block.get("id").and_then(Value::as_str).unwrap_or(""),
                        block.get("name").and_then(Value::as_str).unwrap_or(""),
                    ));
                }
            }
            "content_block_delta" => {
                let index = event.get("index").and_then(Value::as_i64).unwrap_or(0);
                let delta = event.get("delta").unwrap_or(&Value::Null);
                match delta.get("type").and_then(Value::as_str) {
                    Some("text_delta") => {
                        output.extend(self.append_text_delta(
                            delta.get("text").and_then(Value::as_str).unwrap_or(""),
                        ));
                    }
                    Some("input_json_delta") => {
                        if let Some(key) = self.function_key_by_block.get(&index).cloned() {
                            output.extend(
                                self.append_function_arguments(
                                    &key,
                                    delta
                                        .get("partial_json")
                                        .and_then(Value::as_str)
                                        .unwrap_or(""),
                                ),
                            );
                        }
                    }
                    _ => {}
                }
            }
            "content_block_stop" => {
                let index = event.get("index").and_then(Value::as_i64).unwrap_or(0);
                match self.open_block_types.get(&index).map(String::as_str) {
                    Some("text") => output.extend(self.close_text_item()),
                    Some("tool_use") => {
                        if let Some(key) = self.function_key_by_block.remove(&index) {
                            output.extend(self.close_function_item(&key));
                        }
                    }
                    _ => {}
                }
                self.open_block_types.remove(&index);
            }
            "message_delta" => {
                let delta = event.get("delta").unwrap_or(&Value::Null);
                if delta.get("stop_reason").and_then(Value::as_str) == Some("max_tokens") {
                    self.status = "incomplete".into();
                }
                if let Some(usage) = event.get("usage").filter(|value| value.is_object()) {
                    self.usage.merge_claude(usage);
                }
            }
            _ => {}
        }
        output
    }

    fn consume_gemini_to_responses(&mut self, event: &Value) -> Vec<u8> {
        let mut output = self.ensure_start_responses();
        let Some(candidates) = event.get("candidates").and_then(Value::as_array) else {
            self.usage
                .merge_gemini(event.get("usageMetadata").unwrap_or(&Value::Null));
            return output;
        };
        let Some(candidate) = candidates.first() else {
            self.usage
                .merge_gemini(event.get("usageMetadata").unwrap_or(&Value::Null));
            return output;
        };
        let parts = candidate
            .get("content")
            .and_then(|content| content.get("parts"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for part in &parts {
            if part.get("text").is_some() {
                output.extend(
                    self.append_text_delta(part.get("text").and_then(Value::as_str).unwrap_or("")),
                );
            }
            if let Some(function_call) = part.get("functionCall").filter(|value| value.is_object())
            {
                let key = new_id("fc");
                output.extend(
                    self.open_function_item(
                        key.clone(),
                        &new_id("call"),
                        function_call
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or(""),
                    ),
                );
                let arguments =
                    serde_json::to_string(function_call.get("args").unwrap_or(&Value::Null))
                        .unwrap_or_else(|_| "{}".into());
                if !arguments.is_empty() {
                    output.extend(self.append_function_arguments(&key, &arguments));
                }
                output.extend(self.close_function_item(&key));
            }
        }
        self.usage
            .merge_gemini(event.get("usageMetadata").unwrap_or(&Value::Null));
        if candidate.get("finishReason").and_then(Value::as_str) == Some("MAX_TOKENS") {
            self.status = "incomplete".into();
        }
        output
    }
}

// ---------------------------------------------------------------------------
// upstream error -> entry error conversion
// ---------------------------------------------------------------------------
