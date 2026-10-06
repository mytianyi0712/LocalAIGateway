//! 流式转换：`MappedStreamConverter` 按块增量把上游 SSE 转换为入口协议的 SSE。
//!
//! 职责：解析上游（`openai_compatible` / `claude` / `gemini`）的流式分片，
//! 增量产出入口协议（`claude` / `openai_compatible` / `openai_responses`）的事件，
//! 并累积 usage。边界：只做协议形状转换，不管连接、重试与错误码；
//! 同协议直通不转换分片、原样转发字节（仅旁路观察 usage）。
//! 关键不变量：content block 顺序开闭、index 单调递增且不复用、
//! 同一段 thinking 复用同一个 block。
//!
//! 文件划分：`usage`（用量累加）、`claude`（Claude 侧事件发射）、
//! `responses`（Responses 侧事件发射）、`dsml`（DSML 工具调用解析）。
use super::response::*;
use super::*;

mod claude;
mod dsml;
mod responses;
mod usage;

use usage::UsageAcc;

pub(super) enum ParsedEvent {
    Done,
    Json(Value),
}

pub(super) fn sse_block_events(block: &[u8]) -> Option<ParsedEvent> {
    let payload = crate::sse::block_data(block)?;
    if payload == b"[DONE]" {
        return Some(ParsedEvent::Done);
    }
    match serde_json::from_slice::<Value>(&payload) {
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

/// Gemini 流是换行分隔的 JSON 对象（部分代理会包一层 SSE `data:` 帧），
/// 因此按单个换行切分。
pub(super) fn parse_gemini_events(chunk: &[u8], buffer: &mut Vec<u8>) -> Vec<ParsedEvent> {
    buffer.extend_from_slice(&normalize_crlf(chunk));
    let mut events = Vec::new();
    while let Some(marker) = buffer.iter().position(|byte| *byte == b'\n') {
        let line = buffer.drain(..marker).collect::<Vec<_>>();
        if !buffer.is_empty() {
            buffer.remove(0);
        }
        let line = crate::sse::trim_ascii(&line);
        if line.is_empty() {
            continue;
        }
        let line = crate::sse::line_data(line).unwrap_or(line);
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

#[derive(Clone, Copy, PartialEq)]
pub(super) enum ConverterKind {
    ClaudeOpenai,
    /// entry 为 OpenAI chat 时的直通（Command Code 的静默转换目标）。
    ChatPassthrough,
    ResponsesOpenai,
}

#[derive(Clone)]
pub(super) struct FunctionState {
    item_id: String,
    output_index: i64,
    call_id: String,
    name: String,
    arguments: Vec<String>,
}

/// 有状态转换器：把上游 SSE 流转换为入口协议的 SSE 事件。每次用
/// [`MappedStreamConverter::feed`] 喂入一块上游分片，立即返回该分片转换出的字节，
/// 让客户端看到实时流。
///
/// 同协议直通不转换分片、原样转发字节（仅解析分片中的 usage 事件做旁路统计）；
/// `finished` 置位后 feed 一律返回空，保证终态只发一次。
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
    // claude 侧状态
    open_blocks: BTreeMap<i64, String>,
    next_index: i64,
    pending_finish_reason: Option<String>,
    tool_index_by_id: HashMap<String, i64>,
    tool_index_by_upstream: HashMap<i64, i64>,
    // responses 侧状态
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
    /// 每个打开中的 block 累积的 thinking 文本；上游不提供 signature 时
    /// 用它合成 Anthropic 的 `signature_delta`（Command Code / OpenAI 兼容
    /// 的推理路径）。
    thinking_text: HashMap<i64, String>,
    // DSML 状态
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
            Some(Passthrough) if entry == "openai_compatible" => ConverterKind::ChatPassthrough,
            Some(ResponsesToChat) => ConverterKind::ResponsesOpenai,
            Some(Passthrough) => bail!(
                "Unsupported stream conversion pair: {entry} -> {upstream_protocol}"
            ),
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
        let passthrough = self.kind == ConverterKind::ChatPassthrough;
        if passthrough {
            // 直通（Command Code 的静默转换目标）原样转发字节，但仍须观察 usage：
            // 否则 openai_compatible 入口的流会在日志里记成 0 token。
            self.observe_passthrough_usage(chunk);
            return chunk.to_vec();
        }
        self.feed_impl(chunk)
    }

    /// 在不改动转发字节的前提下，从原始分片中解析 usage 事件
    /// （与透明转发路径扫描的形状一致）。
    fn observe_passthrough_usage(&mut self, chunk: &[u8]) {
        self.buffer.extend_from_slice(&normalize_crlf(chunk));
        for block in crate::sse::split_blocks(&mut self.buffer) {
            if let Some(ParsedEvent::Json(value)) = sse_block_events(&block) {
                let usage = value.get("usage");
                if let Some(usage) = usage.filter(|item| item.is_object()) {
                    self.usage.merge_openai(usage);
                }
            }
        }
    }

    fn feed_impl(&mut self, chunk: &[u8]) -> Vec<u8> {
        self.buffer.extend_from_slice(&normalize_crlf(chunk));
        let mut events = Vec::new();
        for block in crate::sse::split_blocks(&mut self.buffer) {
            if let Some(event) = sse_block_events(&block) {
                events.push(event);
            }
        }
        let mut output = Vec::new();
        for event in events {
            match event {
                ParsedEvent::Done => match self.kind {
                    ConverterKind::ClaudeOpenai => {
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
                    ConverterKind::ChatPassthrough => {}
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
            ConverterKind::ClaudeOpenai => {
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
            ConverterKind::ChatPassthrough => Vec::new(),
        }
    }

    pub fn error_event(&mut self, message: &str) -> Vec<u8> {
        match self.kind {
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
            ConverterKind::ClaudeOpenai => {
                if !self.started {
                    self.ensure_start_claude();
                }
                self.finished = true;
                sse(
                    "error",
                    &json!({
                        "type": "error",
                        "error": {"type": GATEWAY_ERROR_TYPE, "message": message},
                        "request_id": self.request_id,
                    }),
                )
            }
        }
    }

    // -- claude 侧辅助函数 ------------------------------------------------

    fn merge_openai_usage(&mut self, usage: &Value) {
        self.usage.merge_openai(usage);
    }

    // -- responses 侧辅助函数 ---------------------------------------------

    fn consume(&mut self, event: &Value) -> Vec<u8> {
        match self.kind {
            ConverterKind::ClaudeOpenai => self.consume_claude_openai(event),
            ConverterKind::ResponsesOpenai => self.consume_responses_openai(event),
            ConverterKind::ChatPassthrough => Vec::new(),
        }
    }

}