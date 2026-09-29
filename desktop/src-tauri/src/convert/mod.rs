//! 入口协议与上游协议之间的请求/响应转换。
//!
//! 职责：入口报文 → 上游报文，并把上游响应（含流式事件）转回入口协议。
//! 支持四种入口协议：`claude`（`/v1/messages`）、`openai_responses`
//! （`/v1/responses`）、`openai_compatible`（`/v1/chat/completions`）、`gemini`。
//! 边界：只做报文形态转换，不管路由/渠道选择、鉴权与密钥。
//! 关键不变量：请求整体转换为 JSON，流式响应逐事件转换（见
//! [`MappedStreamConverter`]），客户端始终收到合法 SSE；模型标识替换为上游模型名。

use std::collections::{BTreeMap, HashMap};

use anyhow::{Result, bail};
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// 子模块与再导出
// ---------------------------------------------------------------------------

mod commandcode;
mod error;
mod request;
mod response;
mod scan;
mod stream;
pub use commandcode::{CommandCodeDecoder, ndjson_to_chat_completion};
pub use error::chunk_has_content;
pub use error::convert_error;
pub use request::convert_request;
pub use response::convert_response;
pub use scan::{StreamScan, stream_completed, stream_error_message};
pub use stream::MappedStreamConverter;

const GATEWAY_ERROR_TYPE: &str = "gateway_error";

/// Command Code usage 根部的标记：`prompt_tokens` 是含缓存命中的总数。
/// 流式/非流式转换据此换算 Anthropic 的 `input_tokens`（只算未命中部分）；
/// 其它上游不带这个标记，语义不受影响。
pub const INPUT_INCLUDES_CACHE: &str = "prompt_tokens_includes_cache";

/// 一条转换方向，由 `(入口协议, 上游协议)` 唯一确定。
///
/// `(entry, upstream) -> strategy` 矩阵以本枚举为注册表：新增一个方向只需在此
/// 加一个变体并补上对应的转换函数，无需在各处散落成对的 `match`；覆盖完备性
/// 由 `scan.rs` 的 `conversion_strategy_registry_is_exhaustive` 测试守护。
/// 流式转换对应的枚举是 [`ConverterKind`]；仅与上游有关的行为（SSE 扫描、
/// 完成判定）仍按单轴分发。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversionStrategy {
    /// 两端协议相同：只替换模型名。
    Passthrough,
    ClaudeToChat,
    ClaudeToResponses,
    ClaudeToGemini,
    /// 入口报文 → Command Code `/alpha/generate` 请求体（仅上游侧协议）。
    ClaudeToCommandCode,
    /// OpenAI chat 入口 → Command Code `/alpha/generate` 请求体
    /// （直连 `/v1/chat/completions` 请求的静默转换；不经映射）。
    ChatToCommandCode,
    ResponsesToChat,
    ResponsesToClaude,
    ResponsesToGemini,
    /// 入口报文 → Command Code `/alpha/generate` 请求体（仅上游侧协议）。
    ResponsesToCommandCode,
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
// 文本助手
// ---------------------------------------------------------------------------

/// 拼接 `text`/`output_text` 块数组；对象取 `.text`，字符串原样返回。
// 与 content_text/chat_message_text/openai_content_text 的差异：多认 output_text 块、
// 用 "" 连接、独有 Object→.text 分支；四处块类型集与连接符各不相同，故不合并。
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

/// 取 Claude `content` 的文本（字符串或 `text` 块列表）。
// 与 chat_message_text 的差异：只认 type=="text" 的对象块、不做空文本过滤；不合并。
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

/// 取 chat-completions `message.content` / 流式 `delta.content` 的纯文本
/// （字符串或内容块数组，如 GPT-5.x 风格）。
// 与 content_text 的差异：额外接受裸字符串元素与 output_text 来源、丢弃空文本；不合并。
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
// 工具助手（Claude → OpenAI 方向）
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

// 与 request.rs 的 responses_tools_to_* 及 commandcode.rs 的 *_to_cc 差异：
// 不过滤 tool.type，参数取自 input_schema 并写成 OpenAI 的 parameters；不合并。
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

// 与 request.rs::responses_text 的差异：只读 block.text（不判别 String/Object、不处理数组）；不合并。
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
