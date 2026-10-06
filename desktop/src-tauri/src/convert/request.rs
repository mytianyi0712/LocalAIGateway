//! 请求方向转换：入口协议 → 上游协议。
//!
//! 职责：按 `ConversionStrategy` 注册表把入口请求体转换为目标上游协议的请求体。
//! 边界：只做报文形态转换；不支持的方向直接报错，不在此处做路由或模型映射。
//! 关键不变量：转换结果的顶层 `model` 一律替换为上游模型名；未知协议对报错而非
//! 静默透传。

use super::*;

impl ConversionStrategy {
    /// 注册表：`entry` 的请求由哪种策略转换给 `upstream`。
    pub fn for_pair(entry: &str, upstream: &str) -> Option<Self> {
        use ConversionStrategy::*;
        match (entry, upstream) {
            ("claude", "claude") | ("openai_responses", "openai_responses") => Some(Passthrough),
            ("claude", "openai_compatible") => Some(ClaudeToChat),
            ("claude", "command_code") => Some(ClaudeToCommandCode),
            // OpenAI chat 入口：Provider API 伴生体保持原样，
            // 直连请求则静默转换为 Command Code。
            ("openai_compatible", "openai_compatible") => Some(Passthrough),
            ("openai_compatible", "command_code") => Some(ChatToCommandCode),
            ("openai_responses", "openai_compatible") => Some(ResponsesToChat),
            ("openai_responses", "command_code") => Some(ResponsesToCommandCode),
            _ => None,
        }
    }
}

pub(super) fn claude_to_chat(upstream_model: &str, data: &Value) -> Value {
    let mut messages: Vec<Value> = Vec::new();
    if let Some(system) = system_text(data.get("system")) {
        messages.push(json!({"role": "system", "content": system}));
    }
    if let Some(items) = data.get("messages").and_then(Value::as_array) {
        for message in items {
            let role = message.get("role").and_then(Value::as_str).unwrap_or("");
            if !matches!(role, "user" | "assistant") {
                continue;
            }
            let content = message.get("content").unwrap_or(&Value::Null);
            if let Value::String(content) = content {
                messages.push(json!({"role": role, "content": content}));
                continue;
            }
            let Some(blocks) = content.as_array() else {
                continue;
            };
            let mut text_parts: Vec<String> = Vec::new();
            let mut image_parts: Vec<Value> = Vec::new();
            let mut tool_calls: Vec<Value> = Vec::new();
            let mut tool_results: Vec<Value> = Vec::new();
            for block in blocks {
                let block_type = block.get("type").and_then(Value::as_str).unwrap_or("");
                match block_type {
                    "text" => {
                        if let Some(text) = claude_block_text(block) {
                            text_parts.push(text);
                        }
                    }
                    "image" => {
                        let source = block.get("source").unwrap_or(&Value::Null);
                        image_parts.push(json!({
                            "type": "image_url",
                            "image_url": {"url": image_data_url(source, "image/png")},
                        }));
                    }
                    "tool_use" => {
                        tool_calls.push(json!({
                            "id": block.get("id").and_then(Value::as_str).map(str::to_owned)
                                .unwrap_or_else(|| new_id("call")),
                            "type": "function",
                            "function": {
                                "name": block.get("name").and_then(Value::as_str).unwrap_or(""),
                                "arguments": serde_json::to_string(
                                    block.get("input").unwrap_or(&Value::Null)
                                ).unwrap_or_else(|_| "{}".into()),
                            }
                        }));
                    }
                    "tool_result" => {
                        tool_results.push(json!({
                            "role": "tool",
                            "tool_call_id": block.get("tool_use_id").and_then(Value::as_str).unwrap_or(""),
                            "content": content_text(block.get("content").unwrap_or(&Value::Null)),
                        }));
                    }
                    _ => {}
                }
            }
            messages.extend(tool_results);
            let mut content_parts: Vec<Value> = Vec::new();
            if !text_parts.is_empty() {
                content_parts.push(json!({"type": "text", "text": text_parts.join("\n")}));
            }
            content_parts.extend(image_parts);
            if tool_calls.is_empty() && content_parts.is_empty() {
                continue;
            }
            let mut converted = json!({"role": role});
            if !tool_calls.is_empty() {
                converted["tool_calls"] = Value::Array(tool_calls);
                converted["content"] = if content_parts.len() == 1 {
                    content_parts[0].get("text").cloned().unwrap_or(Value::Null)
                } else if content_parts.is_empty() {
                    Value::Null
                } else {
                    Value::Array(content_parts)
                };
            } else if content_parts.len() == 1 {
                converted["content"] = content_parts[0]
                    .get("text")
                    .cloned()
                    .unwrap_or(Value::Array(content_parts));
            } else {
                converted["content"] = Value::Array(content_parts);
            }
            messages.push(converted);
        }
    }
    let mut payload = json!({
        "model": upstream_model,
        "messages": messages,
        "stream": data.get("stream").and_then(Value::as_bool).unwrap_or(false),
    });
    if let Some(max_tokens) = data.get("max_tokens").filter(|value| value.is_i64()) {
        payload["max_tokens"] = max_tokens.clone();
    }
    for key in ["temperature", "top_p"] {
        if let Some(value) = data.get(key) {
            payload[key] = value.clone();
        }
    }
    if let Some(stop) = data
        .get("stop_sequences")
        .filter(|value| value.as_array().is_some_and(|items| !items.is_empty()))
    {
        payload["stop"] = stop.clone();
    }
    if let Some(tools) = tools_to_openai(data.get("tools").unwrap_or(&Value::Null)) {
        payload["tools"] = Value::Array(tools);
    }
    if let Some(tool_choice) = data.get("tool_choice").and_then(tool_choice_to_openai) {
        payload["tool_choice"] = tool_choice;
    }
    payload
}

// ---------------------------------------------------------------------------
// Responses 入口助手
// ---------------------------------------------------------------------------

// 与 mod.rs::text / content_text 的差异：Option 入参、只认 input_text/text 块、
// 空结果回落 None（而非空串）；不合并。
pub(super) fn responses_instructions_text(value: Option<&Value>) -> Option<String> {
    match value {
        None => None,
        Some(Value::String(value)) => Some(value.clone()),
        Some(Value::Array(items)) => {
            let parts = items
                .iter()
                .filter_map(|part| {
                    if matches!(
                        part.get("type").and_then(Value::as_str),
                        Some("input_text" | "text")
                    ) {
                        part.get("text").and_then(Value::as_str).map(str::to_owned)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
            if parts.is_empty() { None } else { Some(parts) }
        }
        _ => None,
    }
}

// 与 mod.rs::text 的差异：不处理数组、Object 只取 .text、缺失时返回 None（text 返回空串）；不合并。
pub(super) fn responses_text(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Object(value) => value.get("text").and_then(Value::as_str).map(str::to_owned),
        _ => None,
    }
}

// 与 commandcode.rs::responses_tools_to_cc 的差异：输出为嵌套的
// {"type":"function","function":{...}} 而非 CC 的扁平 name/input_schema；不合并。
pub(super) fn responses_tools_to_openai(tools: &Value) -> Option<Vec<Value>> {
    let mut result = Vec::new();
    for tool in tools.as_array()? {
        if tool.get("type").and_then(Value::as_str) != Some("function") {
            continue;
        }
        let Some(name) = tool.get("name").and_then(Value::as_str) else {
            continue;
        };
        result.push(json!({
            "type": "function",
            "function": {
                "name": name,
                "description": tool.get("description").and_then(Value::as_str).unwrap_or(""),
                "parameters": tool.get("parameters").cloned()
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

pub(super) fn responses_tool_choice(tool_choice: &Value) -> Option<Value> {
    if let Some(choice) = tool_choice.as_str() {
        return Some(Value::String(choice.to_owned()));
    }
    if tool_choice.get("type").and_then(Value::as_str) == Some("function")
        && let Some(name) = tool_choice.get("name").and_then(Value::as_str)
    {
        return Some(json!({"type": "function", "function": {"name": name}}));
    }
    None
}

// 与 responses_tools_to_openai 的差异：输出 Anthropic 形（顶层 name/description/input_schema）；不合并。


// 与 responses_tools_to_openai/claude 的差异：整体包一层 {"functionDeclarations": [...]}；不合并。


pub(super) fn parse_arguments(value: &Value) -> Value {
    if let Some(object) = value.as_object() {
        return Value::Object(object.clone());
    }
    let text = value.as_str().unwrap_or("{}");
    serde_json::from_str::<Value>(text).unwrap_or_else(|_| json!({}))
}

pub(super) fn responses_image_url(part: &Value) -> Option<String> {
    let image_url = part.get("image_url")?;
    if let Some(url) = image_url.as_str().filter(|url| !url.is_empty()) {
        return Some(url.to_owned());
    }
    image_url
        .get("url")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

pub(super) fn responses_to_chat(upstream_model: &str, data: &Value) -> Value {
    let mut messages: Vec<Value> = Vec::new();
    if let Some(instructions) = responses_instructions_text(data.get("instructions")) {
        messages.push(json!({"role": "system", "content": instructions}));
    }
    if let Some(items) = data.get("input").and_then(Value::as_array) {
        for item in items {
            let item_type = item.get("type").and_then(Value::as_str).unwrap_or("");
            if item_type == "message" {
                let mut role = item.get("role").and_then(Value::as_str).unwrap_or("user");
                if role == "developer" {
                    role = "system";
                } else if !matches!(role, "user" | "assistant" | "system" | "tool") {
                    role = "user";
                }
                let content = item.get("content").unwrap_or(&Value::Null);
                if let Value::String(content) = content {
                    messages.push(json!({"role": role, "content": content}));
                    continue;
                }
                let Some(parts_value) = content.as_array() else {
                    continue;
                };
                let mut text_parts: Vec<String> = Vec::new();
                let mut image_parts: Vec<Value> = Vec::new();
                for part in parts_value {
                    match part.get("type").and_then(Value::as_str) {
                        Some("input_text") => {
                            if let Some(text) = part.get("text").and_then(Value::as_str) {
                                text_parts.push(text.to_owned());
                            }
                        }
                        Some("input_image") => {
                            if let Some(url) = responses_image_url(part) {
                                image_parts
                                    .push(json!({"type": "image_url", "image_url": {"url": url}}));
                            }
                        }
                        _ => {}
                    }
                }
                let mut parts: Vec<Value> = Vec::new();
                if !text_parts.is_empty() {
                    parts.push(json!({"type": "text", "text": text_parts.join("\n")}));
                }
                parts.extend(image_parts);
                if !parts.is_empty() {
                    messages.push(json!({"role": role, "content": parts}));
                }
            } else if item_type == "function_call" {
                let tool_call = json!({
                    "id": item.get("call_id").and_then(Value::as_str).map(str::to_owned)
                        .unwrap_or_else(|| new_id("call")),
                    "type": "function",
                    "function": {
                        "name": item.get("name").and_then(Value::as_str).unwrap_or(""),
                        "arguments": item.get("arguments").and_then(Value::as_str).unwrap_or("{}"),
                    }
                });
                let can_merge = messages.last().is_some_and(|last| {
                    last.get("role").and_then(Value::as_str) == Some("assistant")
                        && last.get("tool_calls").is_some()
                });
                if can_merge {
                    if let Some(last) = messages.last_mut() {
                        if let Some(calls) =
                            last.get_mut("tool_calls").and_then(Value::as_array_mut)
                        {
                            calls.push(tool_call);
                        }
                        if let Some(reasoning) = item.get("reasoning_content") {
                            last["reasoning_content"] = reasoning.clone();
                        }
                    }
                } else {
                    messages.push(json!({
                        "role": "assistant",
                        "content": Value::Null,
                        "reasoning_content": item.get("reasoning_content").cloned().unwrap_or_else(|| Value::String(String::new())),
                        "tool_calls": [tool_call],
                    }));
                }
            } else if item_type == "function_call_output" {
                messages.push(json!({
                    "role": "tool",
                    "tool_call_id": item.get("call_id").and_then(Value::as_str).unwrap_or(""),
                    "content": responses_text(item.get("output").unwrap_or(&Value::Null)).unwrap_or_default(),
                }));
            }
        }
    }
    let mut payload = json!({
        "model": upstream_model,
        "messages": messages,
        "stream": data.get("stream").and_then(Value::as_bool).unwrap_or(false),
    });
    if let Some(value) = data
        .get("max_output_tokens")
        .filter(|value| value.is_i64())
    {
        payload["max_tokens"] = value.clone();
    }
    for key in ["temperature", "top_p"] {
        if let Some(value) = data.get(key) {
            payload[key] = value.clone();
        }
    }
    if let Some(tools) = responses_tools_to_openai(data.get("tools").unwrap_or(&Value::Null)) {
        payload["tools"] = Value::Array(tools);
    }
    if let Some(tool_choice) = data.get("tool_choice").and_then(responses_tool_choice) {
        payload["tool_choice"] = tool_choice;
    }
    payload
}

/// 与入口协议无关的请求转换入口。
pub fn convert_request(
    entry: &str,
    upstream_protocol: &str,
    upstream_model: &str,
    body: &[u8],
) -> Result<Vec<u8>> {
    let data: Value = serde_json::from_slice(body).map_err(|error| {
        let label = if entry == "claude" {
            "Claude"
        } else {
            "Responses"
        };
        anyhow::anyhow!("Invalid {label} request body: {error}")
    })?;
    if !data.is_object() {
        let label = if entry == "claude" {
            "Claude"
        } else {
            "Responses"
        };
        bail!("{label} request body must be a JSON object");
    }
    use ConversionStrategy::*;
    let strategy = ConversionStrategy::for_pair(entry, upstream_protocol)
        .ok_or_else(|| anyhow::anyhow!("Unsupported upstream protocol: {upstream_protocol}"))?;
    let converted = match strategy {
        Passthrough => {
            let mut value = data;
            value["model"] = json!(upstream_model);
            value
        }
        ClaudeToChat => claude_to_chat(upstream_model, &data),
        ClaudeToCommandCode => super::commandcode::claude_to_commandcode(upstream_model, &data),
        ChatToCommandCode => super::commandcode::openai_chat_to_commandcode(upstream_model, &data),
        ResponsesToChat => responses_to_chat(upstream_model, &data),
        ResponsesToCommandCode => {
            super::commandcode::responses_to_commandcode(upstream_model, &data)
        }
    };
    Ok(serde_json::to_vec(&converted)?)
}

// ---------------------------------------------------------------------------
// 上游 → Claude 响应转换（非流式）
// ---------------------------------------------------------------------------
