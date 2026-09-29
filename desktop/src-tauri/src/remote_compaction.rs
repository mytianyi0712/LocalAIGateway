//! Codex 远端压缩（Remote Compaction）协议支持：V1（专用一元端点
//! `POST /v1/responses/compact`，响应为 `{"output":[ResponseItem,...]}`）与 V2
//! （`remote_compaction_v2`，默认：在普通 `POST /v1/responses` 流式请求的 `input`
//! 末尾追加一个 `{"type":"compaction_trigger"}`，流内必须恰好含一个 compaction
//! 输出项并以 `response.completed` 收尾）。
//! 职责：模式识别（[`detect_compaction`]）与 V2 流的逐块校验（[`CompactionV2Validator`]：
//! 解析 SSE、统计 compaction、判定完成与格式错误）。边界：只做识别与校验，不发请求。
//! 不变量：校验器判定完成后即可停止读取，尾部残块在 [`CompactionV2Validator::finish`] 中被消费。

use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use serde_json::{Value, json};

use crate::domain::{CompactionMode, TransportFailure};
use crate::ports::UpstreamError;

/// 检查或校验远端压缩请求/响应时的错误。
#[derive(Debug, Clone)]
pub struct CompactionRequestError(pub String);

impl std::fmt::Display for CompactionRequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for CompactionRequestError {}

/// 判断入站的 Responses 请求是否为远端压缩请求。
///
/// 返回值：
/// - 路径指向 `/responses/compact` 时 `Ok(Some(V1))`；
/// - `input` 以恰好一个 `compaction_trigger` 结尾时 `Ok(Some(V2))`；
/// - 普通请求 `Ok(None)`；
/// - 压缩分帧非法（JSON 错误、缺少 input、trigger 不在最后、或存在多个 trigger）时
///   `Err`。
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

/// 校验 V1 compact 端点的响应体：一个 `output` 字段为数组的 JSON 对象。
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

/// V2 压缩流校验失败的原因。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactionV2Error {
    /// 上游发送了 `response.failed`。
    Upstream(String),
    /// 流已结束却没有 compaction 输出项。
    NotCompaction,
    /// 流已结束但含多个 compaction 输出项。
    TooManyCompactions,
    /// 流在 `response.completed` 之前结束。
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

/// V2 压缩 SSE 流的增量校验器。
///
/// 代理会先把压缩流缓冲起来再转发给 Codex，这样格式错误/不受支持的上游响应仍能在
/// 输出任何客户端字节之前失败切换。发现流程的探测复用同一校验器。
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

    /// 喂入一段原始上游分块。
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

    /// 流是否已到达 `response.completed`。
    pub fn completed(&self) -> bool {
        self.completed
    }

    /// 截至目前流是否已见到恰好一个 compaction 项。
    pub fn compaction_count(&self) -> usize {
        self.compaction_items
    }

    /// 从 `response.completed`（或最后一个 usage 对象）捕获到的 usage。
    pub fn usage(&self) -> Option<&Value> {
        self.usage.as_ref()
    }

    /// 校验已收集的流（EOF 之后调用）。
    ///
    /// 先消费尾部残留块：SSE 规范允许流在最后一个事件之后不再补空行，
    /// 此时 `buffer` 里还留着完整的 `response.completed` 块。旧实现只看
    /// 已切分的块，会把这种合法流误判成 [`CompactionV2Error::Incomplete`]。
    pub fn finish(&mut self) -> Result<(), CompactionV2Error> {
        if !self.buffer.is_empty() {
            let tail = std::mem::take(&mut self.buffer);
            self.consume_block(&tail);
        }
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
        let Some(payload) = crate::sse::block_data(block) else {
            return;
        };
        if payload == b"[DONE]" {
            return;
        }
        let Ok(value) = serde_json::from_slice::<Value>(&payload) else {
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

/// V1 compact 端点的最小探测请求体。
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

/// V2 压缩流的最小探测请求体。
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

/// 压缩路径带判读的流式读取结果（探测路径也用，故对 crate 可见）。
pub(crate) enum ValidatedCompactionStream {
    /// 读到的明文（无解码器时即原始字节）；`truncated` 表示撞上缓冲上限。
    Read { payload: Vec<u8>, truncated: bool },
    /// 解码失败；`read` 是已读到的原始字节数（遥测用）。
    Decode {
        error: crate::compression::DecodeError,
        read: usize,
    },
    /// 传输层失败（读取字节数对判定没有意义，故不携带）。
    Transport { kind: TransportFailure },
}

/// 读取期间的失败分类：解码与传输分开归类，错误码字符串才不会被合并。
pub(super) enum ValidatedStreamError {
    Decode(crate::compression::DecodeError),
    Transport(TransportFailure),
}

/// V2 压缩流的读取骨架：读、判读、**早停**都在这里。
///
/// - `validator` 始终只看明文：`decoder` 为 `Some` 时先增量解码，为 `None` 时
///   按明文处理（探测路径不处理压缩）。
/// - 早停条件：校验器已看到 `response.completed` 且至少一个 compaction 块——
///   上游发完事件后保持连接时，等 EOF 会一直等到超时。
/// - 超时与读取错误沿用既有分类（`FirstByteTimeout` / `ConnectionReset`）。
pub(crate) async fn read_validated_compaction_stream<S>(
    mut stream: S,
    deadline: Duration,
    cap: usize,
    validator: &mut CompactionV2Validator,
    mut decoder: Option<&mut crate::compression::RequiredDecoder>,
) -> ValidatedCompactionStream
where
    S: futures_util::Stream<Item = Result<Bytes, UpstreamError>> + Unpin,
{
    let mut payload = Vec::with_capacity(cap.min(64 * 1024));
    let mut read = 0usize;
    let mut truncated = false;
    let read_stream = async {
        while let Some(item) = stream.next().await {
            let chunk = item.map_err(|_| {
                ValidatedStreamError::Transport(TransportFailure::ConnectionReset)
            })?;
            read += chunk.len();
            let piece = match decoder.as_deref_mut() {
                Some(decoder) => decoder
                    .feed_required(&chunk)
                    .map_err(ValidatedStreamError::Decode)?,
                None => chunk.to_vec(),
            };
            validator.feed(&piece);
            if payload.len() + piece.len() > cap {
                payload.extend_from_slice(&piece[..cap - payload.len()]);
                truncated = true;
                break;
            }
            payload.extend_from_slice(&piece);
            if validator.completed() && validator.compaction_count() >= 1 {
                break;
            }
        }
        Ok::<(), ValidatedStreamError>(())
    };
    match tokio::time::timeout(deadline, read_stream).await {
        Ok(Ok(())) => ValidatedCompactionStream::Read { payload, truncated },
        Ok(Err(ValidatedStreamError::Decode(error))) => {
            ValidatedCompactionStream::Decode { error, read }
        }
        Ok(Err(ValidatedStreamError::Transport(kind))) => {
            ValidatedCompactionStream::Transport { kind }
        }
        Err(_) => ValidatedCompactionStream::Transport {
            kind: TransportFailure::FirstByteTimeout,
        },
    }
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

    /// 完整的 `response.completed` 块后面没有空行时，尾部残块仍要被消费，
    /// `finish()` 必须判为合法——旧实现只在块切分时消费，会误报 Incomplete。
    #[test]
    fn finish_consumes_a_tail_block_without_trailing_blank_line() {
        let mut validator = CompactionV2Validator::new();
        let stream = concat!(
            "event: response.output_item.done\n",
            "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"compaction\"}}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":3}}}",
        );
        validator.feed(stream.as_bytes());
        assert!(
            validator.finish().is_ok(),
            "a complete stream without a trailing blank line must validate: {:?}",
            validator.finish()
        );
        assert_eq!(validator.compaction_count(), 1);
        assert_eq!(
            validator
                .usage()
                .and_then(|usage| usage.get("input_tokens"))
                .and_then(serde_json::Value::as_i64),
            Some(3)
        );
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
