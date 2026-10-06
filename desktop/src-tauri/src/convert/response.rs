//! 非流式响应转换：上游协议 → 入口协议，含 DSML 工具调用解析。
//!
//! 职责：把非流式上游响应体转换为入口协议响应；把文本中的 DSML（DeepSeek
//! 风格）工具调用标记解析成结构化 function_call。
//! 边界：只处理非流式响应，流式转换在 `stream.rs`。
//! 关键不变量：同协议直通按字节透传；usage 换算保持上游与入口语义一致。

use super::*;

pub(super) fn stop_reason_openai(reason: &str) -> &'static str {
    match reason {
        "stop" | "null" => "end_turn",
        "length" => "max_tokens",
        "tool_calls" | "function_call" => "tool_use",
        "content_filter" => "refusal",
        _ => "end_turn",
    }
}

// 与 responses_usage 的差异：输出 Anthropic 形，且仅当缓存数 > 0 才写出缓存字段；不合并。
pub(super) fn claude_usage(
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    cache_read: Option<i64>,
    cache_write: Option<i64>,
) -> Value {
    let mut usage = json!({
        "input_tokens": input_tokens.unwrap_or(0),
        "output_tokens": output_tokens.unwrap_or(0),
    });
    if let Some(value) = cache_read.filter(|value| *value > 0) {
        usage["cache_read_input_tokens"] = json!(value);
    }
    if let Some(value) = cache_write.filter(|value| *value > 0) {
        usage["cache_creation_input_tokens"] = json!(value);
    }
    usage
}

/// 上游把 `prompt_tokens` 当作含缓存命中的总数，而 Anthropic 的
/// `input_tokens` 只算未命中部分。非流式（[`openai_compatible_to_claude`]）与
/// 流式（`stream::anthropic_input_tokens`）的换算式完全一致，故共用本函数；
/// 两边仅「是否启用换算」的判据来源不同（JSON 标记 vs 累积标志）。
pub(super) fn uncached_input_tokens(
    total: Option<i64>,
    cache_read: Option<i64>,
    cache_write: Option<i64>,
) -> Option<i64> {
    total.map(|total| (total - cache_read.unwrap_or(0) - cache_write.unwrap_or(0)).max(0))
}

pub(super) fn openai_compatible_to_claude(claude_model: &str, data: &Value) -> Value {
    let choice = data
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .unwrap_or(&Value::Null);
    let message = choice.get("message").unwrap_or(&Value::Null);
    let mut content: Vec<Value> = Vec::new();
    let reasoning = message
        .get("reasoning_content")
        .or_else(|| message.get("reasoning"));
    if let Some(reasoning) = reasoning
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
    {
        content.push(json!({"type": "thinking", "thinking": reasoning}));
    }
    let text = chat_message_text(message.get("content").unwrap_or(&Value::Null));
    if !text.is_empty() {
        content.push(json!({"type": "text", "text": text}));
    }
    if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
        for tool_call in tool_calls {
            let function = tool_call.get("function").unwrap_or(&Value::Null);
            let arguments = function
                .get("arguments")
                .and_then(Value::as_str)
                .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
                .filter(|value| value.is_object())
                .unwrap_or_else(|| json!({}));
            content.push(json!({
                "type": "tool_use",
                "id": tool_call.get("id").and_then(Value::as_str).map(str::to_owned)
                    .unwrap_or_else(|| new_id("toolu")),
                "name": function.get("name").and_then(Value::as_str).unwrap_or(""),
                "input": arguments,
            }));
        }
    }
    let usage = data.get("usage").unwrap_or(&Value::Null);
    let prompt_details = usage
        .get("prompt_tokens_details")
        .or_else(|| usage.get("input_tokens_details"))
        .unwrap_or(&Value::Null);
    let cache_read = prompt_details
        .get("cached_tokens")
        .or_else(|| prompt_details.get("cache_read_input_tokens"))
        .or_else(|| prompt_details.get("prompt_cache_hit_tokens"))
        .and_then(Value::as_i64);
    let cache_write = prompt_details
        .get("cache_write_tokens")
        .or_else(|| prompt_details.get("cached_write_tokens"))
        .and_then(Value::as_i64);
    let input_tokens = usage
        .get("prompt_tokens")
        .or_else(|| usage.get("input_tokens"))
        .and_then(Value::as_i64);
    // Command Code 把 `prompt_tokens` 视为含缓存命中的总数，而 Anthropic 的
    // `input_tokens` 只算未命中部分。该标记只出现在 Command Code 报文里，
    // 其它上游保持原有语义。
    let input_tokens = if usage
        .get(INPUT_INCLUDES_CACHE)
        .and_then(Value::as_bool)
        == Some(true)
    {
        uncached_input_tokens(input_tokens, cache_read, cache_write)
    } else {
        input_tokens
    };
    json!({
        "id": data.get("id").and_then(Value::as_str).map(str::to_owned).unwrap_or_else(|| new_id("msg")),
        "type": "message",
        "role": "assistant",
        "model": claude_model,
        "content": content,
        "stop_reason": stop_reason_openai(
            choice.get("finish_reason").and_then(Value::as_str).unwrap_or("stop")
        ),
        "stop_sequence": Value::Null,
        "usage": claude_usage(
            input_tokens,
            usage.get("completion_tokens").or_else(|| usage.get("output_tokens")).and_then(Value::as_i64),
            cache_read,
            cache_write,
        ),
    })
}

// ---------------------------------------------------------------------------
// Responses 信封 / usage / DSML
// ---------------------------------------------------------------------------

// 与 claude_usage 的差异：输出 Responses 形（含 total_tokens 与 *_details），缓存/推理缺省写 0；不合并。
pub(super) fn responses_usage(
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    cached_tokens: Option<i64>,
    reasoning_tokens: Option<i64>,
) -> Value {
    json!({
        "input_tokens": input_tokens.unwrap_or(0),
        "output_tokens": output_tokens.unwrap_or(0),
        "total_tokens": input_tokens.unwrap_or(0) + output_tokens.unwrap_or(0),
        "input_tokens_details": {"cached_tokens": cached_tokens.unwrap_or(0)},
        "output_tokens_details": {"reasoning_tokens": reasoning_tokens.unwrap_or(0)},
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn responses_envelope(
    codex_model: &str,
    response_id: &str,
    created_at: i64,
    status: &str,
    output: Vec<Value>,
    usage: Value,
    error: Option<Value>,
) -> Value {
    json!({
        "id": response_id,
        "object": "response",
        "created_at": created_at,
        "status": status,
        "error": error,
        "incomplete_details": if status == "incomplete" {
            json!({"reason": "max_output_tokens"})
        } else {
            Value::Null
        },
        "instructions": Value::Null,
        "max_output_tokens": Value::Null,
        "model": codex_model,
        "output": output,
        "parallel_tool_calls": true,
        "previous_response_id": Value::Null,
        "reasoning": Value::Null,
        "store": false,
        "temperature": Value::Null,
        "text": Value::Null,
        "tool_choice": Value::Null,
        "tools": [],
        "top_p": Value::Null,
        "truncation": Value::Null,
        "usage": usage,
        "user": Value::Null,
        "metadata": {},
    })
}

// -- DSML（DeepSeek 工具调用标记）解析 -----------------------------------------

pub(super) const DSML_BLOCK_NAMES: [&str; 2] = ["tool_calls", "function_calls"];
pub(super) const DSML_BAR_VARIANTS: [&str; 4] = ["|", "｜", "||", "｜｜"];

pub(super) fn bars_len(text: &str) -> usize {
    text.chars()
        .take_while(|c| *c == '|' || *c == '｜')
        .map(|c| c.len_utf8())
        .sum()
}

pub(super) struct TagMatch {
    name: String,
    attrs: String,
    end: usize,
    closing: bool,
}

/// 在 `at` 处匹配 `<{bars}DSML{bars}{name} ...>` / `</{bars}DSML{bars}{name}>`
/// （`at` 必须指向 `<`）。返回标签名、属性文本以及 `>` 之后一字节的索引。
pub(super) fn match_tag(text: &str, at: usize) -> Option<TagMatch> {
    let rest = &text[at + 1..];
    let (closing, body) = if let Some(rest) = rest.strip_prefix('/') {
        (true, rest)
    } else {
        (false, rest)
    };
    let bars1 = bars_len(body);
    if bars1 == 0 {
        return None;
    }
    let after = &body[bars1..];
    if !after.starts_with("DSML") {
        return None;
    }
    let after2 = &after[4..];
    let bars2 = bars_len(after2);
    if bars2 == 0 {
        return None;
    }
    let after3 = &after2[bars2..];
    let name_end = after3
        .find(['>', ' ', '\t', '\n', '\r'])
        .unwrap_or(after3.len());
    let name = &after3[..name_end];
    if name.is_empty() {
        return None;
    }
    let gt = after3[name_end..].find('>')?;
    let attrs = after3[name_end..name_end + gt].trim().to_owned();
    let end = at + 1 + if closing { 1 } else { 0 } + bars1 + 4 + bars2 + name_end + gt + 1;
    Some(TagMatch {
        name: name.to_owned(),
        attrs,
        end,
        closing,
    })
}

/// 用子串匹配定位 DSML 块的起点。标记有意省略结尾的竖线串，因此真实的
/// `<|DSML|tool_calls|>` 会前缀匹配 `<|DSML|tool_calls>`。
pub(super) fn find_dsml_start(text: &str) -> Option<(usize, String, String)> {
    let mut found: Option<(usize, String, String)> = None;
    for name in DSML_BLOCK_NAMES {
        for bars in DSML_BAR_VARIANTS {
            let start = format!("<{bars}DSML{bars}{name}>");
            if let Some(index) = text.find(&start) {
                let end = format!("</{bars}DSML{bars}{name}>");
                if found.as_ref().is_none_or(|(at, _, _)| index < *at) {
                    found = Some((index, start, end));
                }
            }
        }
    }
    found
}

/// `<bars>DSML<bars>memory pass:` 噪声标记的最早出现位置。
pub(super) fn find_dsml_noise_start(text: &str) -> Option<usize> {
    let mut found: Vec<usize> = Vec::new();
    for bars in DSML_BAR_VARIANTS {
        let marker = format!("<{bars}DSML{bars}memory pass:");
        if let Some(index) = text.find(&marker) {
            found.push(index);
        }
    }
    found.into_iter().min()
}

pub(super) fn dsml_partial_prefix_length(text: &str) -> usize {
    let mut keep = 0;
    for name in DSML_BLOCK_NAMES {
        for bars in DSML_BAR_VARIANTS {
            for marker in [
                format!("<{bars}DSML{bars}{name}>"),
                format!("<{bars}DSML{bars}memory pass:"),
            ] {
                // 只测字符边界上的前缀（标记里可能含多字节的全角竖线 '｜'）。
                for (index, _) in marker.char_indices() {
                    if index == 0 || index >= marker.len() {
                        continue;
                    }
                    if text.ends_with(&marker[..index]) {
                        keep = keep.max(index);
                    }
                }
            }
        }
    }
    keep
}

pub(super) fn html_unescape(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] == b'&'
            && let Some(relative) = input[at..].find(';')
        {
            let entity = &input[at + 1..at + relative];
            let decoded: Option<String> = match entity {
                "amp" => Some("&".into()),
                "lt" => Some("<".into()),
                "gt" => Some(">".into()),
                "quot" => Some("\"".into()),
                "apos" => Some("'".into()),
                "nbsp" => Some("\u{00a0}".into()),
                _ => entity
                    .strip_prefix("#x")
                    .or_else(|| entity.strip_prefix("#X"))
                    .and_then(|hex| u32::from_str_radix(hex, 16).ok())
                    .and_then(char::from_u32)
                    .map(|c| c.to_string())
                    .or_else(|| {
                        entity
                            .strip_prefix('#')
                            .and_then(|num| num.parse::<u32>().ok())
                            .and_then(char::from_u32)
                            .map(|c| c.to_string())
                    }),
            };
            if let Some(decoded) = decoded {
                out.push_str(&decoded);
                at += relative + 1;
                continue;
            }
        }
        let ch = input[at..].chars().next().unwrap_or('\u{fffd}');
        out.push(ch);
        at += ch.len_utf8();
    }
    out
}

pub(super) fn dsml_attributes(source: &str) -> HashMap<String, String> {
    let mut attributes = HashMap::new();
    let bytes = source.as_bytes();
    let mut at = 0;
    while at < bytes.len() {
        while at < bytes.len() && bytes[at].is_ascii_whitespace() {
            at += 1;
        }
        let name_start = at;
        while at < bytes.len()
            && (bytes[at].is_ascii_alphanumeric() || bytes[at] == b'_' || bytes[at] == b'-')
        {
            at += 1;
        }
        if at == name_start {
            at += 1;
            continue;
        }
        let name = source[name_start..at].to_owned();
        while at < bytes.len() && bytes[at] == b' ' {
            at += 1;
        }
        if at >= bytes.len() || bytes[at] != b'=' {
            continue;
        }
        at += 1;
        while at < bytes.len() && bytes[at] == b' ' {
            at += 1;
        }
        if at >= bytes.len() {
            break;
        }
        let value = if bytes[at] == b'"' || bytes[at] == b'\'' {
            let quote = bytes[at];
            at += 1;
            let start = at;
            while at < bytes.len() && bytes[at] != quote {
                at += 1;
            }
            let raw = &source[start..at.min(bytes.len())];
            if at < bytes.len() {
                at += 1;
            }
            html_unescape(raw)
        } else {
            let start = at;
            while at < bytes.len() && !bytes[at].is_ascii_whitespace() && bytes[at] != b'>' {
                at += 1;
            }
            source[start..at].to_owned()
        };
        attributes.insert(name, value);
    }
    attributes
}

pub(super) fn dsml_arguments(body: &str) -> Value {
    let mut arguments = serde_json::Map::new();
    let mut at = 0;
    while let Some(relative) = body[at..].find('<') {
        let pos = at + relative;
        if let Some(tag) = match_tag(body, pos) {
            if !tag.closing
                && tag.name == "parameter"
                && let Some(close) = find_closing_tag(body, tag.end, "parameter")
            {
                let raw = html_unescape(body[tag.end..close.0].trim());
                let attrs = dsml_attributes(&tag.attrs);
                if let Some(name) = attrs.get("name") {
                    let value = if attrs
                        .get("string")
                        .is_some_and(|flag| flag.eq_ignore_ascii_case("true"))
                    {
                        Value::String(raw)
                    } else {
                        serde_json::from_str::<Value>(&raw).unwrap_or(Value::String(raw))
                    };
                    arguments.insert(name.clone(), value);
                }
                at = close.1;
                continue;
            }
            if !tag.closing
                && tag.name == "command"
                && let Some(close) = find_closing_tag(body, tag.end, "command")
            {
                arguments.insert(
                    "cmd".into(),
                    Value::String(html_unescape(body[tag.end..close.0].trim())),
                );
                at = close.1;
                continue;
            }
        }
        at = pos + 1;
    }
    Value::Object(arguments)
}

/// 找到 `name` 的配对闭合标签；返回 (内容结束, 标签结束)。
pub(super) fn find_closing_tag(text: &str, from: usize, name: &str) -> Option<(usize, usize)> {
    let mut at = from;
    while let Some(relative) = text[at..].find('<') {
        let pos = at + relative;
        if let Some(tag) = match_tag(text, pos)
            && tag.closing
            && tag.name == name
        {
            return Some((pos, tag.end));
        }
        at = pos + 1;
    }
    None
}

/// 解析一个 DSML 块内的工具调用（invoke 与 message 两种形式）。
pub(super) fn parse_dsml_invocations(block: &str) -> Vec<Value> {
    let mut matches: Vec<(usize, String, Value)> = Vec::new();
    let mut at = 0;
    while let Some(relative) = block[at..].find('<') {
        let pos = at + relative;
        if let Some(tag) = match_tag(block, pos) {
            if !tag.closing
                && tag.name == "invoke"
                && let Some(close) = find_closing_tag(block, tag.end, "invoke")
            {
                let attrs = dsml_attributes(&tag.attrs);
                if let Some(name) = attrs.get("name") {
                    matches.push((pos, name.clone(), dsml_arguments(&block[tag.end..close.0])));
                }
                at = close.1;
                continue;
            }
            // message 块用 </...invoke> 闭合（沿用既有实现约定）。
            if !tag.closing
                && tag.name == "message"
                && let Some(close) = find_closing_tag(block, tag.end, "invoke")
            {
                let attrs = dsml_attributes(&tag.attrs);
                let name = attrs.get("to").or_else(|| attrs.get("name")).cloned();
                if let Some(name) = name {
                    matches.push((pos, name, dsml_arguments(&block[tag.end..close.0])));
                }
                at = close.1;
                continue;
            }
        }
        at = pos + 1;
    }
    matches.sort_by_key(|(start, _, _)| *start);
    matches
        .into_iter()
        .map(|(_, name, arguments)| {
            json!({
                "name": name,
                "arguments": serde_json::to_string(&arguments).unwrap_or_else(|_| "{}".into()),
            })
        })
        .collect()
}

/// 把 chat 文本拆成纯文本段与 DSML function_call 段。
pub(super) fn split_dsml_content(text: &str) -> Vec<(String, Value)> {
    let mut segments: Vec<(String, Value)> = Vec::new();
    let mut remaining = text;
    loop {
        if remaining.is_empty() {
            break;
        }
        let noise = find_dsml_noise_start(remaining);
        let start = find_dsml_start(remaining);
        if let Some(noise_index) = noise
            && start
                .as_ref()
                .is_none_or(|(start_index, _, _)| noise_index < *start_index)
        {
            if noise_index > 0 {
                segments.push((
                    "text".into(),
                    Value::String(remaining[..noise_index].to_owned()),
                ));
            }
            let rest = &remaining[noise_index..];
            match rest.find('\n') {
                Some(line_end) => {
                    remaining = &rest[line_end + 1..];
                    continue;
                }
                None => break,
            }
        }
        let Some((start_index, start_marker, end_marker)) = start else {
            segments.push(("text".into(), Value::String(remaining.to_owned())));
            break;
        };
        let after_start = &remaining[start_index + start_marker.len()..];
        let Some(end_relative) = after_start.find(&end_marker) else {
            segments.push(("text".into(), Value::String(remaining.to_owned())));
            break;
        };
        let end_index = start_index + start_marker.len() + end_relative;
        if start_index > 0 {
            segments.push((
                "text".into(),
                Value::String(remaining[..start_index].to_owned()),
            ));
        }
        let block = &remaining[start_index + start_marker.len()..end_index];
        let calls = parse_dsml_invocations(block);
        if !calls.is_empty() {
            for call in calls {
                segments.push(("function_call".into(), call));
            }
        } else {
            segments.push((
                "text".into(),
                Value::String(remaining[start_index..end_index + end_marker.len()].to_owned()),
            ));
        }
        remaining = &remaining[end_index + end_marker.len()..];
    }
    segments
}

pub(super) fn openai_compatible_to_responses(codex_model: &str, data: &Value) -> Value {
    let choice = data
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .unwrap_or(&Value::Null);
    let message = choice.get("message").unwrap_or(&Value::Null);
    let text = chat_message_text(message.get("content").unwrap_or(&Value::Null));
    let mut output: Vec<Value> = Vec::new();
    for (segment_type, segment) in split_dsml_content(&text) {
        if segment_type == "text" {
            if let Some(text) = segment.as_str().filter(|text| !text.is_empty()) {
                output.push(json!({
                    "type": "message",
                    "id": new_id("msg"),
                    "status": "completed",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": text, "annotations": []}],
                }));
            }
        } else if segment_type == "function_call" {
            output.push(json!({
                "type": "function_call",
                "id": new_id("fc"),
                "call_id": new_id("call"),
                "name": segment.get("name").and_then(Value::as_str).unwrap_or(""),
                "arguments": segment.get("arguments").and_then(Value::as_str).unwrap_or("{}"),
                "status": "completed",
            }));
        }
    }
    if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
        for tool_call in tool_calls {
            let function = tool_call.get("function").unwrap_or(&Value::Null);
            output.push(json!({
                "type": "function_call",
                "id": new_id("fc"),
                "call_id": tool_call.get("id").and_then(Value::as_str).map(str::to_owned)
                    .unwrap_or_else(|| new_id("call")),
                "name": function.get("name").and_then(Value::as_str).unwrap_or(""),
                "arguments": function.get("arguments").and_then(Value::as_str).unwrap_or("{}"),
                "status": "completed",
            }));
        }
    }
    let usage = data.get("usage").unwrap_or(&Value::Null);
    let prompt_details = usage.get("prompt_tokens_details").unwrap_or(&Value::Null);
    let output_details = usage
        .get("completion_tokens_details")
        .unwrap_or(&Value::Null);
    let status = if choice.get("finish_reason").and_then(Value::as_str) == Some("length") {
        "incomplete"
    } else {
        "completed"
    };
    responses_envelope(
        codex_model,
        &new_id("resp"),
        unix_timestamp(),
        status,
        output,
        responses_usage(
            usage
                .get("prompt_tokens")
                .or_else(|| usage.get("input_tokens"))
                .and_then(Value::as_i64),
            usage
                .get("completion_tokens")
                .or_else(|| usage.get("output_tokens"))
                .and_then(Value::as_i64),
            prompt_details
                .get("cached_tokens")
                .or_else(|| prompt_details.get("cache_read_input_tokens"))
                .or_else(|| prompt_details.get("prompt_cache_hit_tokens"))
                .or_else(|| usage.get("prompt_cache_hit_tokens"))
                .and_then(Value::as_i64),
            output_details
                .get("reasoning_tokens")
                .or_else(|| usage.get("reasoning_tokens"))
                .and_then(Value::as_i64),
        ),
        None,
    )
}

/// 与入口协议无关的非流式响应转换入口。
pub fn convert_response(
    entry: &str,
    upstream_protocol: &str,
    mapped_model: &str,
    body: &[u8],
) -> Result<Vec<u8>> {
    use ConversionStrategy::*;
    let strategy = ConversionStrategy::for_pair(entry, upstream_protocol).ok_or_else(|| {
        anyhow::anyhow!("Unsupported conversion pair: {entry} -> {upstream_protocol}")
    })?;
    // 同协议直通按字节透传：重新序列化会丢失工具调用、推理项与 usage。
    if strategy == Passthrough {
        return Ok(body.to_vec());
    }
    let data: Value = serde_json::from_slice(body)
        .map_err(|error| anyhow::anyhow!("Invalid upstream response body: {error}"))?;
    if !data.is_object() {
        bail!("Upstream response body must be a JSON object");
    }
    let converted = match strategy {
        Passthrough => unreachable!("handled above"),
        ClaudeToChat => openai_compatible_to_claude(mapped_model, &data),
        ClaudeToCommandCode | ResponsesToCommandCode | ChatToCommandCode => bail!(
            "Command Code responses are decoded to openai_compatible before conversion"
        ),
        ResponsesToChat => openai_compatible_to_responses(mapped_model, &data),
    };
    Ok(serde_json::to_vec(&converted)?)
}

// ---------------------------------------------------------------------------
// 流式转换（MappedStreamConverter）
// ---------------------------------------------------------------------------
