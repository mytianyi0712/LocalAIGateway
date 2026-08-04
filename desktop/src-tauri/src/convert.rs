//! Protocol conversion between the gateway entry formats and the upstream
//! gateway protocols — faithful port of backend/app/adapters/convert.py.
//!
//! Two client entry formats are supported:
//! * `claude`            - Claude /v1/messages (Claude Code / claudecode)
//! * `openai_responses`  - OpenAI Responses API /v1/responses (Codex / codex)
//!
//! Requests are converted as whole JSON documents before being forwarded;
//! streaming responses are converted event-by-event while the upstream stream
//! is being read (see [`MappedStreamConverter`]), so the client always sees a
//! valid SSE stream. Tool calls, images and usage numbers are translated
//! between the formats, and model identifiers are substituted with the
//! upstream model name.

use std::collections::{BTreeMap, HashMap};

use anyhow::{Result, bail};
use serde_json::{Value, json};

const GATEWAY_ERROR_TYPE: &str = "gateway_error";

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
    format!("data: {}\n\n", serde_json::to_string(payload).unwrap_or_default()).into_bytes()
}

// ---------------------------------------------------------------------------
// text helpers
// ---------------------------------------------------------------------------

/// Array of `text`/`output_text` blocks joined, object `.text`, string as-is.
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

/// Text of a Claude `content` value (string or list of `text` blocks).
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

/// Plain text from chat-completions `message.content` / stream `delta.content`
/// (string or a list of content parts, e.g. GPT-5.x style).
fn chat_message_text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(value) => value.clone(),
        Value::Array(values) => values
            .iter()
            .filter_map(|part| match part {
                Value::String(part) => Some(part.clone()),
                Value::Object(part) => part
                    .get("text")
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
// tool helpers (Claude -> OpenAI directions)
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
    if result.is_empty() { None } else { Some(result) }
}

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

// ---------------------------------------------------------------------------
// Claude entry -> upstream request conversion
// ---------------------------------------------------------------------------

fn claude_to_chat(upstream_model: &str, data: &Value) -> Value {
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
                    .unwrap_or_else(|| Value::Array(content_parts));
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
    if let Some(stop) = data.get("stop_sequences").filter(|value| {
        value.as_array().is_some_and(|items| !items.is_empty())
    }) {
        payload["stop"] = stop.clone();
    }
    if let Some(tools) = tools_to_openai(data.get("tools").unwrap_or(&Value::Null)) {
        payload["tools"] = Value::Array(tools);
    }
    if let Some(tool_choice) = data
        .get("tool_choice")
        .and_then(tool_choice_to_openai)
    {
        payload["tool_choice"] = tool_choice;
    }
    payload
}

fn claude_to_responses_value(upstream_model: &str, data: &Value) -> Value {
    let mut items: Vec<Value> = Vec::new();
    if let Some(messages) = data.get("messages").and_then(Value::as_array) {
        for message in messages {
            let role = message.get("role").and_then(Value::as_str).unwrap_or("");
            if !matches!(role, "user" | "assistant") {
                continue;
            }
            let content = message.get("content").unwrap_or(&Value::Null);
            if let Value::String(content) = content {
                items.push(json!({
                    "type": "message",
                    "role": role,
                    "content": [{"type": "input_text", "text": content}],
                }));
                continue;
            }
            let Some(blocks) = content.as_array() else {
                continue;
            };
            let mut text_parts: Vec<String> = Vec::new();
            let mut image_parts: Vec<Value> = Vec::new();
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
                            "type": "input_image",
                            "image_url": image_data_url(source, "image/png"),
                        }));
                    }
                    "tool_use" => {
                        items.push(json!({
                            "type": "function_call",
                            "call_id": block.get("id").and_then(Value::as_str).map(str::to_owned)
                                .unwrap_or_else(|| new_id("call")),
                            "name": block.get("name").and_then(Value::as_str).unwrap_or(""),
                            "arguments": serde_json::to_string(
                                block.get("input").unwrap_or(&Value::Null)
                            ).unwrap_or_else(|_| "{}".into()),
                        }));
                    }
                    "tool_result" => {
                        items.push(json!({
                            "type": "function_call_output",
                            "call_id": block.get("tool_use_id").and_then(Value::as_str).unwrap_or(""),
                            "output": content_text(block.get("content").unwrap_or(&Value::Null)),
                        }));
                    }
                    _ => {}
                }
            }
            if !text_parts.is_empty() || !image_parts.is_empty() {
                let mut content: Vec<Value> = Vec::new();
                if !text_parts.is_empty() {
                    content.push(json!({"type": "input_text", "text": text_parts.join("\n")}));
                }
                content.extend(image_parts);
                items.push(json!({"type": "message", "role": role, "content": content}));
            }
        }
    }
    let mut payload = json!({
        "model": upstream_model,
        "input": items,
        "stream": data.get("stream").and_then(Value::as_bool).unwrap_or(false),
    });
    if data.get("max_tokens").is_some_and(|value| value.is_i64()) {
        payload["max_output_tokens"] = data.get("max_tokens").cloned().unwrap();
    }
    for key in ["temperature", "top_p"] {
        if let Some(value) = data.get(key) {
            payload[key] = value.clone();
        }
    }
    if let Some(instructions) = system_text(data.get("system")) {
        payload["instructions"] = Value::String(instructions);
    }
    if let Some(tools) = tools_to_openai(data.get("tools").unwrap_or(&Value::Null)) {
        let flattened: Vec<Value> = tools
            .into_iter()
            .filter_map(|tool| {
                let function = tool.get("function")?.clone();
                let mut converted = function;
                converted["type"] = json!("function");
                Some(converted)
            })
            .collect();
        payload["tools"] = Value::Array(flattened);
    }
    if let Some(tool_choice) = data
        .get("tool_choice")
        .and_then(tool_choice_to_openai)
    {
        payload["tool_choice"] = tool_choice;
    }
    payload
}

fn claude_to_gemini(upstream_model: &str, data: &Value) -> Value {
    let mut tool_names: HashMap<&str, &str> = HashMap::new();
    if let Some(messages) = data.get("messages").and_then(Value::as_array) {
        for message in messages {
            let Some(blocks) = message.get("content").and_then(Value::as_array) else {
                continue;
            };
            for block in blocks {
                if block.get("type").and_then(Value::as_str) == Some("tool_use") {
                    if let Some(id) = block.get("id").and_then(Value::as_str) {
                        tool_names.insert(id, block.get("name").and_then(Value::as_str).unwrap_or("unknown_tool"));
                    }
                }
            }
        }
    }
    let mut contents: Vec<Value> = Vec::new();
    if let Some(messages) = data.get("messages").and_then(Value::as_array) {
        for message in messages {
            let role = if message.get("role").and_then(Value::as_str) == Some("assistant") {
                "model"
            } else {
                "user"
            };
            let content = message.get("content").unwrap_or(&Value::Null);
            let mut parts: Vec<Value> = Vec::new();
            if let Value::String(content) = content {
                parts.push(json!({"text": content}));
            } else if let Some(blocks) = content.as_array() {
                for block in blocks {
                    let block_type = block.get("type").and_then(Value::as_str).unwrap_or("");
                    match block_type {
                        "text" => {
                            if let Some(text) = claude_block_text(block) {
                                parts.push(json!({"text": text}));
                            }
                        }
                        "image" => {
                            let source = block.get("source").unwrap_or(&Value::Null);
                            parts.push(json!({
                                "inlineData": {
                                    "mimeType": source.get("media_type").and_then(Value::as_str).unwrap_or("image/png"),
                                    "data": source.get("data").and_then(Value::as_str).unwrap_or(""),
                                }
                            }));
                        }
                        "tool_use" => {
                            parts.push(json!({
                                "functionCall": {
                                    "name": block.get("name").and_then(Value::as_str).unwrap_or(""),
                                    "args": block.get("input").cloned().unwrap_or(Value::Null),
                                }
                            }));
                        }
                        "tool_result" => {
                            let name = block
                                .get("tool_use_id")
                                .and_then(Value::as_str)
                                .and_then(|id| tool_names.get(id).copied())
                                .unwrap_or("unknown_tool");
                            parts.push(json!({
                                "functionResponse": {
                                    "name": name,
                                    "response": {
                                        "result": content_text(block.get("content").unwrap_or(&Value::Null)),
                                    }
                                }
                            }));
                        }
                        _ => {}
                    }
                }
            }
            if !parts.is_empty() {
                contents.push(json!({"role": role, "parts": parts}));
            }
        }
    }
    let mut payload = json!({"contents": contents});
    if let Some(system) = system_text(data.get("system")) {
        payload["systemInstruction"] = json!({"parts": [{"text": system}]});
    }
    let mut tools: Vec<Value> = Vec::new();
    if let Some(items) = data.get("tools").and_then(Value::as_array) {
        for tool in items {
            let Some(name) = tool.get("name").and_then(Value::as_str) else {
                continue;
            };
            tools.push(json!({
                "functionDeclarations": [{
                    "name": name,
                    "description": tool.get("description").and_then(Value::as_str).unwrap_or(""),
                    "parameters": tool.get("input_schema").cloned()
                        .unwrap_or_else(|| json!({"type": "object", "properties": {}})),
                }]
            }));
        }
    }
    if !tools.is_empty() {
        payload["tools"] = Value::Array(tools);
    }
    let mut generation_config = serde_json::Map::new();
    if data.get("max_tokens").is_some_and(|value| value.is_i64()) {
        generation_config.insert("maxOutputTokens".into(), data.get("max_tokens").cloned().unwrap());
    }
    for (from, to) in [("temperature", "temperature"), ("top_p", "topP"), ("top_k", "topK")] {
        if let Some(value) = data.get(from) {
            generation_config.insert(to.into(), value.clone());
        }
    }
    if let Some(stop) = data.get("stop_sequences").filter(|value| {
        value.as_array().is_some_and(|items| !items.is_empty())
    }) {
        generation_config.insert("stopSequences".into(), stop.clone());
    }
    if !generation_config.is_empty() {
        payload["generationConfig"] = Value::Object(generation_config);
    }
    if let Some(thinking) = data
        .get("thinking")
        .filter(|value| value.is_object())
        .and_then(|value| value.get("budget_tokens"))
        .filter(|value| value.is_i64())
    {
        payload["thinkingConfig"] = json!({"thinkingBudget": thinking});
    }
    let _ = upstream_model;
    payload
}

// ---------------------------------------------------------------------------
// Responses entry helpers
// ---------------------------------------------------------------------------

fn responses_instructions_text(value: Option<&Value>) -> Option<String> {
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

fn responses_text(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Object(value) => value.get("text").and_then(Value::as_str).map(str::to_owned),
        _ => None,
    }
}

fn responses_tools_to_openai(tools: &Value) -> Option<Vec<Value>> {
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
    if result.is_empty() { None } else { Some(result) }
}

fn responses_tool_choice(tool_choice: &Value) -> Option<Value> {
    if let Some(choice) = tool_choice.as_str() {
        return Some(Value::String(choice.to_owned()));
    }
    if tool_choice.get("type").and_then(Value::as_str) == Some("function") {
        if let Some(name) = tool_choice.get("name").and_then(Value::as_str) {
            return Some(json!({"type": "function", "function": {"name": name}}));
        }
    }
    None
}

fn responses_tools_to_claude(tools: &Value) -> Option<Vec<Value>> {
    let mut result = Vec::new();
    for tool in tools.as_array()? {
        if tool.get("type").and_then(Value::as_str) != Some("function") {
            continue;
        }
        let Some(name) = tool.get("name").and_then(Value::as_str) else {
            continue;
        };
        result.push(json!({
            "name": name,
            "description": tool.get("description").and_then(Value::as_str).unwrap_or(""),
            "input_schema": tool.get("parameters").cloned()
                .unwrap_or_else(|| json!({"type": "object", "properties": {}})),
        }));
    }
    if result.is_empty() { None } else { Some(result) }
}

fn responses_tools_to_gemini(tools: &Value) -> Option<Vec<Value>> {
    let mut declarations = Vec::new();
    for tool in tools.as_array()? {
        if tool.get("type").and_then(Value::as_str) != Some("function") {
            continue;
        }
        let Some(name) = tool.get("name").and_then(Value::as_str) else {
            continue;
        };
        declarations.push(json!({
            "name": name,
            "description": tool.get("description").and_then(Value::as_str).unwrap_or(""),
            "parameters": tool.get("parameters").cloned()
                .unwrap_or_else(|| json!({"type": "object", "properties": {}})),
        }));
    }
    if declarations.is_empty() {
        None
    } else {
        Some(vec![json!({"functionDeclarations": declarations})])
    }
}

fn parse_arguments(value: &Value) -> Value {
    if let Some(object) = value.as_object() {
        return Value::Object(object.clone());
    }
    let text = value.as_str().unwrap_or("{}");
    serde_json::from_str::<Value>(text).unwrap_or_else(|_| json!({}))
}

fn parse_data_url(value: &Value) -> (String, String) {
    let Some(url) = value.as_str().filter(|url| url.starts_with("data:")) else {
        return ("image/png".into(), String::new());
    };
    let (meta, data) = url["data:".len()..]
        .split_once(',')
        .unwrap_or((&url["data:".len()..], ""));
    let media_type = meta.split(';').next().unwrap_or("image/png");
    let media_type = if media_type.is_empty() { "image/png" } else { media_type };
    (media_type.to_owned(), data.to_owned())
}

fn responses_image_url(part: &Value) -> Option<String> {
    let image_url = part.get("image_url")?;
    if let Some(url) = image_url.as_str().filter(|url| !url.is_empty()) {
        return Some(url.to_owned());
    }
    image_url.get("url").and_then(Value::as_str).map(str::to_owned)
}

fn responses_to_chat(upstream_model: &str, data: &Value) -> Value {
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
                                image_parts.push(json!({"type": "image_url", "image_url": {"url": url}}));
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
                let can_merge = messages
                    .last()
                    .is_some_and(|last| last.get("role").and_then(Value::as_str) == Some("assistant")
                        && last.get("tool_calls").is_some());
                if can_merge {
                    if let Some(last) = messages.last_mut() {
                        if let Some(calls) = last.get_mut("tool_calls").and_then(Value::as_array_mut) {
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
    if data.get("max_output_tokens").is_some_and(|value| value.is_i64()) {
        payload["max_tokens"] = data.get("max_output_tokens").cloned().unwrap();
    }
    for key in ["temperature", "top_p"] {
        if let Some(value) = data.get(key) {
            payload[key] = value.clone();
        }
    }
    if let Some(tools) = responses_tools_to_openai(data.get("tools").unwrap_or(&Value::Null)) {
        payload["tools"] = Value::Array(tools);
    }
    if let Some(tool_choice) = data
        .get("tool_choice")
        .and_then(responses_tool_choice)
    {
        payload["tool_choice"] = tool_choice;
    }
    payload
}

fn responses_to_claude(upstream_model: &str, data: &Value) -> Value {
    let mut messages: Vec<Value> = Vec::new();
    if let Some(items) = data.get("input").and_then(Value::as_array) {
        for item in items {
            let item_type = item.get("type").and_then(Value::as_str).unwrap_or("");
            if item_type == "message" {
                let mut role = item.get("role").and_then(Value::as_str).unwrap_or("user");
                if matches!(role, "system" | "developer") || !matches!(role, "user" | "assistant") {
                    role = "user";
                }
                let content = item.get("content").unwrap_or(&Value::Null);
                if let Value::String(content) = content {
                    messages.push(json!({"role": role, "content": [{"type": "text", "text": content}]}));
                    continue;
                }
                let Some(parts_value) = content.as_array() else {
                    continue;
                };
                let mut blocks: Vec<Value> = Vec::new();
                for part in parts_value {
                    match part.get("type").and_then(Value::as_str) {
                        Some("input_text") => {
                            blocks.push(json!({"type": "text", "text": part.get("text").and_then(Value::as_str).unwrap_or("")}));
                        }
                        Some("input_image") => {
                            let (media_type, data_b64) = parse_data_url(
                                &responses_image_url(part).map(Value::String).unwrap_or(Value::Null),
                            );
                            if !data_b64.is_empty() {
                                blocks.push(json!({
                                    "type": "image",
                                    "source": {"type": "base64", "media_type": media_type, "data": data_b64},
                                }));
                            }
                        }
                        _ => {}
                    }
                }
                if !blocks.is_empty() {
                    messages.push(json!({"role": role, "content": blocks}));
                }
            } else if item_type == "function_call" {
                messages.push(json!({
                    "role": "assistant",
                    "content": [{
                        "type": "tool_use",
                        "id": item.get("call_id").and_then(Value::as_str).map(str::to_owned)
                            .unwrap_or_else(|| new_id("toolu")),
                        "name": item.get("name").and_then(Value::as_str).unwrap_or(""),
                        "input": parse_arguments(item.get("arguments").unwrap_or(&Value::Null)),
                    }]
                }));
            } else if item_type == "function_call_output" {
                messages.push(json!({
                    "role": "user",
                    "content": [{
                        "type": "tool_result",
                        "tool_use_id": item.get("call_id").and_then(Value::as_str).unwrap_or(""),
                        "content": responses_text(item.get("output").unwrap_or(&Value::Null)).unwrap_or_default(),
                    }]
                }));
            }
        }
    }
    let mut payload = json!({
        "model": upstream_model,
        "messages": messages,
        "stream": data.get("stream").and_then(Value::as_bool).unwrap_or(false),
    });
    if data.get("max_output_tokens").is_some_and(|value| value.is_i64()) {
        payload["max_tokens"] = data.get("max_output_tokens").cloned().unwrap();
    }
    for key in ["temperature", "top_p"] {
        if let Some(value) = data.get(key) {
            payload[key] = value.clone();
        }
    }
    if let Some(system) = responses_instructions_text(data.get("instructions")) {
        payload["system"] = Value::String(system);
    }
    if let Some(tools) = responses_tools_to_claude(data.get("tools").unwrap_or(&Value::Null)) {
        payload["tools"] = Value::Array(tools);
    }
    payload
}

fn responses_to_gemini(upstream_model: &str, data: &Value) -> Value {
    let mut call_names: HashMap<&str, &str> = HashMap::new();
    if let Some(items) = data.get("input").and_then(Value::as_array) {
        for item in items {
            if item.get("type").and_then(Value::as_str) == Some("function_call") {
                if let Some(call_id) = item.get("call_id").and_then(Value::as_str) {
                    call_names.insert(call_id, item.get("name").and_then(Value::as_str).unwrap_or("unknown_tool"));
                }
            }
        }
    }
    let mut contents: Vec<Value> = Vec::new();
    if let Some(items) = data.get("input").and_then(Value::as_array) {
        for item in items {
            let item_type = item.get("type").and_then(Value::as_str).unwrap_or("");
            if item_type == "message" {
                let role = if item.get("role").and_then(Value::as_str) == Some("assistant") {
                    "model"
                } else {
                    "user"
                };
                let content = item.get("content").unwrap_or(&Value::Null);
                let mut parts: Vec<Value> = Vec::new();
                if let Value::String(content) = content {
                    parts.push(json!({"text": content}));
                } else if let Some(parts_value) = content.as_array() {
                    for part in parts_value {
                        match part.get("type").and_then(Value::as_str) {
                            Some("input_text") => {
                                parts.push(json!({"text": part.get("text").and_then(Value::as_str).unwrap_or("")}));
                            }
                            Some("input_image") => {
                                let (media_type, data_b64) = parse_data_url(
                                    &responses_image_url(part).map(Value::String).unwrap_or(Value::Null),
                                );
                                if !data_b64.is_empty() {
                                    parts.push(json!({"inlineData": {"mimeType": media_type, "data": data_b64}}));
                                }
                            }
                            _ => {}
                        }
                    }
                }
                if !parts.is_empty() {
                    contents.push(json!({"role": role, "parts": parts}));
                }
            } else if item_type == "function_call" {
                contents.push(json!({
                    "role": "model",
                    "parts": [{
                        "functionCall": {
                            "name": item.get("name").and_then(Value::as_str).unwrap_or(""),
                            "args": parse_arguments(item.get("arguments").unwrap_or(&Value::Null)),
                        }
                    }]
                }));
            } else if item_type == "function_call_output" {
                let name = item
                    .get("call_id")
                    .and_then(Value::as_str)
                    .and_then(|id| call_names.get(id).copied())
                    .unwrap_or("unknown_tool");
                contents.push(json!({
                    "role": "user",
                    "parts": [{
                        "functionResponse": {
                            "name": name,
                            "response": {
                                "result": responses_text(item.get("output").unwrap_or(&Value::Null)).unwrap_or_default(),
                            }
                        }
                    }]
                }));
            }
        }
    }
    let mut payload = json!({"contents": contents});
    if let Some(instructions) = responses_instructions_text(data.get("instructions")) {
        payload["systemInstruction"] = json!({"parts": [{"text": instructions}]});
    }
    if let Some(tools) = responses_tools_to_gemini(data.get("tools").unwrap_or(&Value::Null)) {
        payload["tools"] = Value::Array(tools);
    }
    let mut generation_config = serde_json::Map::new();
    if data.get("max_output_tokens").is_some_and(|value| value.is_i64()) {
        generation_config.insert("maxOutputTokens".into(), data.get("max_output_tokens").cloned().unwrap());
    }
    for key in ["temperature", "topP"] {
        if let Some(value) = data.get(key) {
            generation_config.insert(key.into(), value.clone());
        }
    }
    if !generation_config.is_empty() {
        payload["generationConfig"] = Value::Object(generation_config);
    }
    let _ = upstream_model;
    payload
}

/// Entry-agnostic request conversion (mirrors convert_mapped_request).
pub fn convert_request(
    entry: &str,
    upstream_protocol: &str,
    upstream_model: &str,
    body: &[u8],
) -> Result<Vec<u8>> {
    let data: Value = serde_json::from_slice(body).map_err(|error| {
        let label = if entry == "claude" { "Claude" } else { "Responses" };
        anyhow::anyhow!("Invalid {label} request body: {error}")
    })?;
    if !data.is_object() {
        let label = if entry == "claude" { "Claude" } else { "Responses" };
        bail!("{label} request body must be a JSON object");
    }
    let converted = match (entry, upstream_protocol) {
        ("claude", "claude") => {
            let mut value = data;
            value["model"] = json!(upstream_model);
            value
        }
        ("claude", "openai_compatible") => claude_to_chat(upstream_model, &data),
        ("claude", "openai_responses") => claude_to_responses_value(upstream_model, &data),
        ("claude", "gemini") => claude_to_gemini(upstream_model, &data),
        ("openai_responses", "openai_responses") => {
            let mut value = data;
            value["model"] = json!(upstream_model);
            value
        }
        ("openai_responses", "openai_compatible") => responses_to_chat(upstream_model, &data),
        ("openai_responses", "claude") => responses_to_claude(upstream_model, &data),
        ("openai_responses", "gemini") => responses_to_gemini(upstream_model, &data),
        _ => bail!("Unsupported upstream protocol: {upstream_protocol}"),
    };
    Ok(serde_json::to_vec(&converted)?)
}

// ---------------------------------------------------------------------------
// upstream -> Claude response conversion (non-streaming)
// ---------------------------------------------------------------------------

fn stop_reason_openai(reason: &str) -> &'static str {
    match reason {
        "stop" | "null" => "end_turn",
        "length" => "max_tokens",
        "tool_calls" | "function_call" => "tool_use",
        "content_filter" => "refusal",
        _ => "end_turn",
    }
}

fn stop_reason_gemini(reason: &str) -> &'static str {
    match reason {
        "STOP" | "FINISH_REASON_UNSPECIFIED" => "end_turn",
        "MAX_TOKENS" => "max_tokens",
        "SAFETY" | "RECITATION" => "refusal",
        "TOOL_CALL" | "FUNCTION_CALL" | "MALFORMED_FUNCTION_CALL" => "tool_use",
        _ => "end_turn",
    }
}

fn claude_usage(
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    cache_read: Option<i64>,
    cache_write: Option<i64>,
) -> Value {
    let mut usage = json!({
        "input_tokens": input_tokens.unwrap_or(0),
        "output_tokens": output_tokens.unwrap_or(0),
    });
    if cache_read.is_some_and(|value| value > 0) {
        usage["cache_read_input_tokens"] = json!(cache_read.unwrap());
    }
    if cache_write.is_some_and(|value| value > 0) {
        usage["cache_creation_input_tokens"] = json!(cache_write.unwrap());
    }
    usage
}

fn openai_compatible_to_claude(claude_model: &str, data: &Value) -> Value {
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
    if let Some(reasoning) = reasoning.and_then(Value::as_str).filter(|text| !text.is_empty()) {
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
            usage.get("prompt_tokens").or_else(|| usage.get("input_tokens")).and_then(Value::as_i64),
            usage.get("completion_tokens").or_else(|| usage.get("output_tokens")).and_then(Value::as_i64),
            cache_read,
            cache_write,
        ),
    })
}

fn openai_responses_to_claude(claude_model: &str, data: &Value) -> Value {
    let mut content: Vec<Value> = Vec::new();
    if let Some(output) = data.get("output").and_then(Value::as_array) {
        for item in output {
            let item_type = item.get("type").and_then(Value::as_str).unwrap_or("");
            if matches!(item_type, "message" | "reasoning") {
                if let Some(parts) = item.get("content").and_then(Value::as_array) {
                    for part in parts {
                        match part.get("type").and_then(Value::as_str) {
                            Some("output_text" | "summary_text") => {
                                if let Some(text) = part.get("text").and_then(Value::as_str) {
                                    content.push(json!({"type": "text", "text": text}));
                                }
                            }
                            _ => {}
                        }
                    }
                }
            } else if item_type == "function_call" {
                let arguments = item
                    .get("arguments")
                    .and_then(Value::as_str)
                    .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
                    .filter(|value| value.is_object())
                    .unwrap_or_else(|| json!({}));
                content.push(json!({
                    "type": "tool_use",
                    "id": item.get("call_id").and_then(Value::as_str).map(str::to_owned)
                        .unwrap_or_else(|| new_id("toolu")),
                    "name": item.get("name").and_then(Value::as_str).unwrap_or(""),
                    "input": arguments,
                }));
            }
        }
    }
    let usage = data.get("usage").unwrap_or(&Value::Null);
    let input_details = usage.get("input_tokens_details").unwrap_or(&Value::Null);
    let stop_reason = if content
        .last()
        .is_some_and(|block| block.get("type").and_then(Value::as_str) == Some("tool_use"))
    {
        "tool_use"
    } else {
        "end_turn"
    };
    json!({
        "id": data.get("id").and_then(Value::as_str).map(str::to_owned).unwrap_or_else(|| new_id("msg")),
        "type": "message",
        "role": "assistant",
        "model": claude_model,
        "content": content,
        "stop_reason": stop_reason,
        "stop_sequence": Value::Null,
        "usage": claude_usage(
            usage.get("input_tokens").and_then(Value::as_i64),
            usage.get("output_tokens").and_then(Value::as_i64),
            input_details.get("cached_tokens").and_then(Value::as_i64),
            input_details.get("cache_write_tokens").and_then(Value::as_i64),
        ),
    })
}

fn gemini_to_claude(claude_model: &str, data: &Value) -> Value {
    let candidate = data
        .get("candidates")
        .and_then(Value::as_array)
        .and_then(|candidates| candidates.first())
        .unwrap_or(&Value::Null);
    let parts = candidate
        .get("content")
        .and_then(|content| content.get("parts"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut content: Vec<Value> = Vec::new();
    for part in &parts {
        if let Some(thought) = part.get("thought") {
            if !thought.is_null() {
                content.push(json!({"type": "thinking", "thinking": thought}));
            }
        }
        if part.get("text").is_some() {
            content.push(json!({"type": "text", "text": part.get("text").and_then(Value::as_str).unwrap_or("")}));
        }
        if let Some(function_call) = part.get("functionCall").filter(|value| value.is_object()) {
            content.push(json!({
                "type": "tool_use",
                "id": new_id("toolu"),
                "name": function_call.get("name").and_then(Value::as_str).unwrap_or(""),
                "input": function_call.get("args").cloned().unwrap_or_else(|| json!({})),
            }));
        }
    }
    let usage = data.get("usageMetadata").unwrap_or(&Value::Null);
    json!({
        "id": new_id("msg"),
        "type": "message",
        "role": "assistant",
        "model": claude_model,
        "content": content,
        "stop_reason": stop_reason_gemini(
            candidate.get("finishReason").and_then(Value::as_str).unwrap_or("STOP")
        ),
        "stop_sequence": Value::Null,
        "usage": claude_usage(
            usage.get("promptTokenCount").and_then(Value::as_i64),
            usage.get("candidatesTokenCount").and_then(Value::as_i64),
            usage.get("cachedContentTokenCount").and_then(Value::as_i64),
            None,
        ),
    })
}

// ---------------------------------------------------------------------------
// Responses envelope / usage / DSML
// ---------------------------------------------------------------------------

fn responses_usage(
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
fn responses_envelope(
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

// -- DSML (DeepSeek tool-invocation markup) parsing -------------------------

const DSML_BLOCK_NAMES: [&str; 2] = ["tool_calls", "function_calls"];
const DSML_BAR_VARIANTS: [&str; 4] = ["|", "｜", "||", "｜｜"];

fn bars_len(text: &str) -> usize {
    text.chars()
        .take_while(|c| *c == '|' || *c == '｜')
        .map(|c| c.len_utf8())
        .sum()
}

struct TagMatch {
    name: String,
    attrs: String,
    end: usize,
    closing: bool,
}

/// Match `<{bars}DSML{bars}{name} ...>` / `</{bars}DSML{bars}{name}>` at `at`
/// (which must point at `<`). Returns the tag name, its attribute text and the
/// byte index just past `>`.
fn match_tag(text: &str, at: usize) -> Option<TagMatch> {
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

/// Find the start of a DSML block via substring matching — mirrors the Python
/// `_find_dsml_start` (markers intentionally omit the trailing bar run, so the
/// real `<|DSML|tool_calls|>` prefix-matches `<|DSML|tool_calls>`).
fn find_dsml_start(text: &str) -> Option<(usize, String, String)> {
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

/// Earliest occurrence of a `<bars>DSML<bars>memory pass:` noise marker.
fn find_dsml_noise_start(text: &str) -> Option<usize> {
    let mut found: Vec<usize> = Vec::new();
    for bars in DSML_BAR_VARIANTS {
        let marker = format!("<{bars}DSML{bars}memory pass:");
        if let Some(index) = text.find(&marker) {
            found.push(index);
        }
    }
    found.into_iter().min()
}

fn dsml_partial_prefix_length(text: &str) -> usize {
    let mut keep = 0;
    for name in DSML_BLOCK_NAMES {
        for bars in DSML_BAR_VARIANTS {
            for marker in [
                format!("<{bars}DSML{bars}{name}>"),
                format!("<{bars}DSML{bars}memory pass:"),
            ] {
                // Test only char-boundary prefixes (markers may contain the
                // multi-byte fullwidth bar '｜').
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

fn html_unescape(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] == b'&' {
            if let Some(relative) = input[at..].find(';') {
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
        }
        let ch = input[at..].chars().next().unwrap_or('\u{fffd}');
        out.push(ch);
        at += ch.len_utf8();
    }
    out
}

fn dsml_attributes(source: &str) -> HashMap<String, String> {
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

fn dsml_arguments(body: &str) -> Value {
    let mut arguments = serde_json::Map::new();
    let mut at = 0;
    while let Some(relative) = body[at..].find('<') {
        let pos = at + relative;
        if let Some(tag) = match_tag(body, pos) {
            if !tag.closing && tag.name == "parameter" {
                if let Some(close) = find_closing_tag(body, tag.end, "parameter") {
                    let raw = html_unescape(body[tag.end..close.0].trim());
                    let attrs = dsml_attributes(&tag.attrs);
                    if let Some(name) = attrs.get("name") {
                        let value = if attrs
                            .get("string")
                            .is_some_and(|flag| flag.eq_ignore_ascii_case("true"))
                        {
                            Value::String(raw)
                        } else {
                            serde_json::from_str::<Value>(&raw)
                                .unwrap_or_else(|_| Value::String(raw))
                        };
                        arguments.insert(name.clone(), value);
                    }
                    at = close.1;
                    continue;
                }
            }
            if !tag.closing && tag.name == "command" {
                if let Some(close) = find_closing_tag(body, tag.end, "command") {
                    arguments.insert(
                        "cmd".into(),
                        Value::String(html_unescape(body[tag.end..close.0].trim())),
                    );
                    at = close.1;
                    continue;
                }
            }
        }
        at = pos + 1;
    }
    Value::Object(arguments)
}

/// Find the matching closing tag for `name`; returns (content_end, tag_end).
fn find_closing_tag(text: &str, from: usize, name: &str) -> Option<(usize, usize)> {
    let mut at = from;
    while let Some(relative) = text[at..].find('<') {
        let pos = at + relative;
        if let Some(tag) = match_tag(text, pos) {
            if tag.closing && tag.name == name {
                return Some((pos, tag.end));
            }
        }
        at = pos + 1;
    }
    None
}

/// Parse the tool invocations inside one DSML block (invoke + message forms).
fn parse_dsml_invocations(block: &str) -> Vec<Value> {
    let mut matches: Vec<(usize, String, Value)> = Vec::new();
    let mut at = 0;
    while let Some(relative) = block[at..].find('<') {
        let pos = at + relative;
        if let Some(tag) = match_tag(block, pos) {
            if !tag.closing && tag.name == "invoke" {
                if let Some(close) = find_closing_tag(block, tag.end, "invoke") {
                    let attrs = dsml_attributes(&tag.attrs);
                    if let Some(name) = attrs.get("name") {
                        matches.push((pos, name.clone(), dsml_arguments(&block[tag.end..close.0])));
                    }
                    at = close.1;
                    continue;
                }
            }
            // message blocks close with </...invoke> (Python parity)
            if !tag.closing && tag.name == "message" {
                if let Some(close) = find_closing_tag(block, tag.end, "invoke") {
                    let attrs = dsml_attributes(&tag.attrs);
                    let name = attrs
                        .get("to")
                        .or_else(|| attrs.get("name"))
                        .cloned();
                    if let Some(name) = name {
                        matches.push((pos, name, dsml_arguments(&block[tag.end..close.0])));
                    }
                    at = close.1;
                    continue;
                }
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

/// Split chat text into plain-text and DSML function-call segments.
fn split_dsml_content(text: &str) -> Vec<(String, Value)> {
    let mut segments: Vec<(String, Value)> = Vec::new();
    let mut remaining = text;
    loop {
        if remaining.is_empty() {
            break;
        }
        let noise = find_dsml_noise_start(remaining);
        let start = find_dsml_start(remaining);
        if let Some(noise_index) = noise {
            if start.as_ref().is_none_or(|(start_index, _, _)| noise_index < *start_index) {
                if noise_index > 0 {
                    segments.push(("text".into(), Value::String(remaining[..noise_index].to_owned())));
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
            segments.push(("text".into(), Value::String(remaining[..start_index].to_owned())));
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

fn openai_compatible_to_responses(codex_model: &str, data: &Value) -> Value {
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
    let output_details = usage.get("completion_tokens_details").unwrap_or(&Value::Null);
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
            usage.get("prompt_tokens").or_else(|| usage.get("input_tokens")).and_then(Value::as_i64),
            usage.get("completion_tokens").or_else(|| usage.get("output_tokens")).and_then(Value::as_i64),
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

fn claude_to_responses(codex_model: &str, data: &Value) -> Value {
    let mut output: Vec<Value> = Vec::new();
    if let Some(blocks) = data.get("content").and_then(Value::as_array) {
        for block in blocks {
            match block.get("type").and_then(Value::as_str) {
                Some("text") => {
                    output.push(json!({
                        "type": "message",
                        "id": new_id("msg"),
                        "status": "completed",
                        "role": "assistant",
                        "content": [{
                            "type": "output_text",
                            "text": block.get("text").and_then(Value::as_str).unwrap_or(""),
                            "annotations": [],
                        }],
                    }));
                }
                Some("tool_use") => {
                    output.push(json!({
                        "type": "function_call",
                        "id": new_id("fc"),
                        "call_id": block.get("id").and_then(Value::as_str).map(str::to_owned)
                            .unwrap_or_else(|| new_id("call")),
                        "name": block.get("name").and_then(Value::as_str).unwrap_or(""),
                        "arguments": serde_json::to_string(block.get("input").unwrap_or(&Value::Null))
                            .unwrap_or_else(|_| "{}".into()),
                        "status": "completed",
                    }));
                }
                _ => {}
            }
        }
    }
    let usage = data.get("usage").unwrap_or(&Value::Null);
    let status = if data.get("stop_reason").and_then(Value::as_str) == Some("max_tokens") {
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
            usage.get("input_tokens").and_then(Value::as_i64),
            usage.get("output_tokens").and_then(Value::as_i64),
            usage.get("cache_read_input_tokens").and_then(Value::as_i64),
            None,
        ),
        None,
    )
}

fn gemini_to_responses(codex_model: &str, data: &Value) -> Value {
    let candidate = data
        .get("candidates")
        .and_then(Value::as_array)
        .and_then(|candidates| candidates.first())
        .unwrap_or(&Value::Null);
    let parts = candidate
        .get("content")
        .and_then(|content| content.get("parts"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut output: Vec<Value> = Vec::new();
    for part in &parts {
        if part.get("text").is_some() {
            output.push(json!({
                "type": "message",
                "id": new_id("msg"),
                "status": "completed",
                "role": "assistant",
                "content": [{
                    "type": "output_text",
                    "text": part.get("text").and_then(Value::as_str).unwrap_or(""),
                    "annotations": [],
                }],
            }));
        }
        if let Some(function_call) = part.get("functionCall").filter(|value| value.is_object()) {
            output.push(json!({
                "type": "function_call",
                "id": new_id("fc"),
                "call_id": new_id("call"),
                "name": function_call.get("name").and_then(Value::as_str).unwrap_or(""),
                "arguments": serde_json::to_string(function_call.get("args").unwrap_or(&Value::Null))
                    .unwrap_or_else(|_| "{}".into()),
                "status": "completed",
            }));
        }
    }
    let usage = data.get("usageMetadata").unwrap_or(&Value::Null);
    let status = if candidate.get("finishReason").and_then(Value::as_str).unwrap_or("STOP") == "MAX_TOKENS"
    {
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
            usage.get("promptTokenCount").and_then(Value::as_i64),
            usage.get("candidatesTokenCount").and_then(Value::as_i64),
            usage.get("cachedContentTokenCount").and_then(Value::as_i64),
            None,
        ),
        None,
    )
}

/// Entry-agnostic non-streaming response conversion (convert_mapped_response).
pub fn convert_response(
    entry: &str,
    upstream_protocol: &str,
    mapped_model: &str,
    body: &[u8],
) -> Result<Vec<u8>> {
    // Same-protocol mapping is a pure pass-through: re-synthesizing would drop
    // tool calls, reasoning items and usage (mirrors the Python adapters).
    if entry == upstream_protocol {
        return Ok(body.to_vec());
    }
    let data: Value = serde_json::from_slice(body)
        .map_err(|error| anyhow::anyhow!("Invalid upstream response body: {error}"))?;
    if !data.is_object() {
        bail!("Upstream response body must be a JSON object");
    }
    let converted = match entry {
        "claude" => match upstream_protocol {
            "openai_compatible" => openai_compatible_to_claude(mapped_model, &data),
            "openai_responses" => openai_responses_to_claude(mapped_model, &data),
            "gemini" => gemini_to_claude(mapped_model, &data),
            _ => bail!("Unsupported upstream protocol: {upstream_protocol}"),
        },
        "openai_responses" => match upstream_protocol {
            "openai_compatible" => openai_compatible_to_responses(mapped_model, &data),
            "claude" => claude_to_responses(mapped_model, &data),
            "gemini" => gemini_to_responses(mapped_model, &data),
            _ => bail!("Unsupported upstream protocol: {upstream_protocol}"),
        },
        _ => bail!("Unsupported entry protocol: {entry}"),
    };
    Ok(serde_json::to_vec(&converted)?)
}

// ---------------------------------------------------------------------------
// streaming conversion (MappedStreamConverter)
// ---------------------------------------------------------------------------

enum ParsedEvent {
    Done,
    Json(Value),
}

fn split_sse_blocks(buffer: &mut Vec<u8>) -> Vec<Vec<u8>> {
    let mut blocks = Vec::new();
    loop {
        let Some(marker) = buffer.windows(2).position(|window| window == b"\n\n") else {
            break;
        };
        let block = buffer.drain(..marker).collect::<Vec<_>>();
        buffer.drain(..2);
        if !block.is_empty() {
            blocks.push(block);
        }
    }
    blocks
}

fn sse_block_events(block: &[u8]) -> Option<ParsedEvent> {
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

fn normalize_crlf(chunk: &[u8]) -> Vec<u8> {
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

fn trim_ascii(mut value: &[u8]) -> &[u8] {
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
fn parse_gemini_events(chunk: &[u8], buffer: &mut Vec<u8>) -> Vec<ParsedEvent> {
    buffer.extend_from_slice(&normalize_crlf(chunk));
    let mut events = Vec::new();
    loop {
        let Some(marker) = buffer.iter().position(|byte| *byte == b'\n') else {
            break;
        };
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
struct UsageAcc {
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    cache_read: Option<i64>,
    cache_write: Option<i64>,
    reasoning: Option<i64>,
}

impl UsageAcc {
    fn merge_openai(&mut self, usage: &Value) {
        if let Some(value) = usage.get("prompt_tokens").and_then(Value::as_i64) {
            self.input_tokens = Some(value);
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
            if let Some(value) = details.get("cache_write_tokens").and_then(Value::as_i64) {
                if value > 0 {
                    self.cache_write = Some(value);
                }
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
        for (key, slot) in [
            ("input_tokens", &mut self.input_tokens),
            ("output_tokens", &mut self.output_tokens),
            ("cache_read_input_tokens", &mut self.cache_read),
            ("reasoning_tokens", &mut self.reasoning),
        ] {
            if let Some(value) = usage.get(key).and_then(Value::as_i64) {
                *slot = Some(value);
            }
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
enum ConverterKind {
    ClaudeOpenai,
    ClaudeResponses,
    ClaudeGemini,
    ClaudePassthrough,
    ResponsesOpenai,
    ResponsesClaude,
    ResponsesGemini,
    ResponsesPassthrough,
}

#[derive(Clone)]
struct FunctionState {
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
    // DSML state
    dsml_text_buffer: String,
    dsml_start_marker: Option<String>,
    dsml_end_marker: Option<String>,
    dsml_tool_index: i64,
}

impl MappedStreamConverter {
    pub fn new(entry: &str, upstream_protocol: &str, model: &str) -> Result<Self> {
        let kind = match (entry, upstream_protocol) {
            ("claude", "openai_compatible") => ConverterKind::ClaudeOpenai,
            ("claude", "openai_responses") => ConverterKind::ClaudeResponses,
            ("claude", "claude") => ConverterKind::ClaudePassthrough,
            ("claude", "gemini") => ConverterKind::ClaudeGemini,
            ("openai_responses", "openai_compatible") => ConverterKind::ResponsesOpenai,
            ("openai_responses", "openai_responses") => ConverterKind::ResponsesPassthrough,
            ("openai_responses", "claude") => ConverterKind::ResponsesClaude,
            ("openai_responses", "gemini") => ConverterKind::ResponsesGemini,
            _ => bail!("Unsupported upstream protocol: {upstream_protocol}"),
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
            ConverterKind::ClaudePassthrough | ConverterKind::ResponsesPassthrough
        );
        if passthrough {
            return chunk.to_vec();
        }
        self.feed_impl(chunk)
    }

    fn feed_impl(&mut self, chunk: &[u8]) -> Vec<u8> {
        // For non-gemini kinds the buffer must include the new chunk.
        let events = if matches!(self.kind, ConverterKind::ClaudeGemini | ConverterKind::ResponsesGemini)
        {
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
            ConverterKind::ClaudePassthrough | ConverterKind::ResponsesPassthrough => Vec::new(),
        }
    }

    pub fn error_event(&mut self, message: &str) -> Vec<u8> {
        match self.kind {
            ConverterKind::ClaudePassthrough => {
                return sse(
                    "error",
                    &json!({
                        "type": "error",
                        "error": {"type": GATEWAY_ERROR_TYPE, "message": message},
                        "request_id": self.request_id,
                    }),
                );
            }
            ConverterKind::ResponsesPassthrough => {
                let mut output = self.ensure_start_responses();
                output.extend(self.responses_event("response.failed", &json!({
                    "type": "response.failed",
                    "response": self.envelope("failed", Some(json!({"code": "gateway_error", "message": message}))),
                })));
                self.finished = true;
                return output;
            }
            ConverterKind::ResponsesOpenai => {
                let mut output = self.drain_dsml_text(None, true);
                output.extend(self.error_event_responses(message));
                return output;
            }
            _ => {
                if matches!(self.kind, ConverterKind::ClaudeOpenai | ConverterKind::ClaudeResponses | ConverterKind::ClaudeGemini) {
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
        self.open_blocks.insert(index, block.get("type").and_then(Value::as_str).unwrap_or("text").to_owned());
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
        self.open_blocks.remove(&index);
        sse(
            "content_block_stop",
            &json!({"type": "content_block_stop", "index": index}),
        )
    }

    fn current_block_of_type(&self, block_type: &str) -> Option<i64> {
        self.open_blocks
            .iter()
            .find(|(_, current)| current.as_str() == block_type)
            .map(|(index, _)| *index)
    }

    fn next_block_index(&mut self) -> i64 {
        while self.open_blocks.contains_key(&self.next_index) {
            self.next_index += 1;
        }
        self.next_index
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
                    self.usage.input_tokens,
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
            output.extend(self.open_function_item(key.clone(), &new_id("call"), call.get("name").and_then(Value::as_str).unwrap_or("")));
            output.extend(self.append_function_arguments(&key, call.get("arguments").and_then(Value::as_str).unwrap_or("")));
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
            if let Some((start_index, start_marker, end_marker)) = find_dsml_start(&self.dsml_text_buffer) {
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
            ConverterKind::ClaudePassthrough | ConverterKind::ResponsesPassthrough => Vec::new(),
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
        if let Some(reasoning) = reasoning.and_then(Value::as_str).filter(|text| !text.is_empty()) {
            let index = self.next_block_index();
            output.extend(self.start_block(index, &json!({"type": "thinking", "thinking": ""})));
            output.extend(self.delta(index, &json!({"type": "thinking_delta", "thinking": reasoning})));
        }
        let text = chat_message_text(delta.get("content").unwrap_or(&Value::Null));
        if !text.is_empty() {
            let index = match self.current_block_of_type("text") {
                Some(index) => index,
                None => {
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
                    .or_else(|| upstream_index.and_then(|index| self.tool_index_by_upstream.get(&index).copied()));
                let index = match index {
                    Some(index) => index,
                    None => {
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
                    output.extend(self.delta(index, &json!({"type": "input_json_delta", "partial_json": arguments})));
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
                    output.extend(self.start_block(index, &json!({
                        "type": "tool_use",
                        "id": call_id,
                        "name": item.get("name").and_then(Value::as_str).unwrap_or(""),
                        "input": {},
                    })));
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
                        output.extend(self.start_block(index, &json!({"type": "text", "text": ""})));
                        index
                    }
                };
                output.extend(self.delta(index, &json!({
                    "type": "text_delta",
                    "text": event.get("delta").and_then(Value::as_str).unwrap_or(""),
                })));
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
            self.usage.merge_gemini(event.get("usageMetadata").unwrap_or(&Value::Null));
            return output;
        };
        let Some(candidate) = candidates.first() else {
            self.usage.merge_gemini(event.get("usageMetadata").unwrap_or(&Value::Null));
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
                let index = self.next_block_index();
                output.extend(self.start_block(index, &json!({"type": "thinking", "thinking": ""})));
                output.extend(self.delta(index, &json!({"type": "thinking_delta", "thinking": thought})));
            } else if part.get("text").is_some() {
                let index = match self.current_block_of_type("text") {
                    Some(index) => index,
                    None => {
                        let index = self.next_block_index();
                        output.extend(self.start_block(index, &json!({"type": "text", "text": ""})));
                        index
                    }
                };
                output.extend(self.delta(index, &json!({
                    "type": "text_delta",
                    "text": part.get("text").and_then(Value::as_str).unwrap_or(""),
                })));
            }
            if let Some(function_call) = part.get("functionCall").filter(|value| value.is_object()) {
                let index = self.next_block_index();
                output.extend(self.start_block(index, &json!({
                    "type": "tool_use",
                    "id": new_id("toolu"),
                    "name": function_call.get("name").and_then(Value::as_str).unwrap_or(""),
                    "input": {},
                })));
                let arguments = serde_json::to_string(function_call.get("args").unwrap_or(&Value::Null))
                    .unwrap_or_else(|_| "{}".into());
                if !arguments.is_empty() {
                    output.extend(self.delta(index, &json!({"type": "input_json_delta", "partial_json": arguments})));
                }
                output.extend(self.stop_block(index));
            }
        }
        self.usage.merge_gemini(event.get("usageMetadata").unwrap_or(&Value::Null));
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
                    .or_else(|| upstream_index.and_then(|index| self.tool_key_by_index.get(&index).cloned()));
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
                let block_type = block.get("type").and_then(Value::as_str).unwrap_or("").to_owned();
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
                        output.extend(self.append_text_delta(delta.get("text").and_then(Value::as_str).unwrap_or("")));
                    }
                    Some("input_json_delta") => {
                        if let Some(key) = self.function_key_by_block.get(&index).cloned() {
                            output.extend(self.append_function_arguments(&key, delta.get("partial_json").and_then(Value::as_str).unwrap_or("")));
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
            self.usage.merge_gemini(event.get("usageMetadata").unwrap_or(&Value::Null));
            return output;
        };
        let Some(candidate) = candidates.first() else {
            self.usage.merge_gemini(event.get("usageMetadata").unwrap_or(&Value::Null));
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
                output.extend(self.append_text_delta(part.get("text").and_then(Value::as_str).unwrap_or("")));
            }
            if let Some(function_call) = part.get("functionCall").filter(|value| value.is_object()) {
                let key = new_id("fc");
                output.extend(self.open_function_item(
                    key.clone(),
                    &new_id("call"),
                    function_call.get("name").and_then(Value::as_str).unwrap_or(""),
                ));
                let arguments = serde_json::to_string(function_call.get("args").unwrap_or(&Value::Null))
                    .unwrap_or_else(|_| "{}".into());
                if !arguments.is_empty() {
                    output.extend(self.append_function_arguments(&key, &arguments));
                }
                output.extend(self.close_function_item(&key));
            }
        }
        self.usage.merge_gemini(event.get("usageMetadata").unwrap_or(&Value::Null));
        if candidate.get("finishReason").and_then(Value::as_str) == Some("MAX_TOKENS") {
            self.status = "incomplete".into();
        }
        output
    }
}

// ---------------------------------------------------------------------------
// upstream error -> entry error conversion
// ---------------------------------------------------------------------------

fn error_payload(entry: &str, upstream_protocol: &str, body: &[u8]) -> Vec<u8> {
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
        "openai_responses" => {
            if value.get("type").and_then(Value::as_str) == Some("response.failed") {
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

    #[test]
    fn claude_request_converts_tool_use_to_tool_calls() {
        let input = br#"{"model":"claude-client","max_tokens":32,"stream":true,
            "tools":[{"name":"get_weather","description":"w","input_schema":{"type":"object"}}],
            "tool_choice":{"type":"auto"},
            "messages":[{"role":"user","content":"weather?"},
                        {"role":"assistant","content":[{"type":"tool_use","id":"tu_1","name":"get_weather","input":{"city":"SF"}}]}]}"#;
        let converted = parse(&convert_request("claude", "openai_compatible", "upstream-model", input).unwrap());
        assert_eq!(converted["model"], "upstream-model");
        assert_eq!(converted["messages"][1]["tool_calls"][0]["id"], "tu_1");
        assert_eq!(converted["messages"][1]["tool_calls"][0]["function"]["name"], "get_weather");
        assert_eq!(converted["tools"][0]["type"], "function");
        assert_eq!(converted["tool_choice"], json!("auto"));
        assert_eq!(converted["stream"], true);
    }

    #[test]
    fn claude_image_uses_png_default() {
        let input = br#"{"model":"m","messages":[{"role":"user","content":[{"type":"image","source":{"data":"AAAA"}}]}]}"#;
        let converted = parse(&convert_request("claude", "openai_compatible", "upstream-model", input).unwrap());
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
        let converted = parse(&convert_request("claude", "gemini", "upstream-model", input).unwrap());
        assert_eq!(converted["generationConfig"]["maxOutputTokens"], 64);
        assert_eq!(converted["generationConfig"]["topK"], 10);
        assert_eq!(converted["thinkingConfig"]["thinkingBudget"], 1024);
        assert_eq!(converted["contents"][0]["role"], "model");
        assert_eq!(converted["contents"][0]["parts"][0]["functionCall"]["name"], "t");
        assert_eq!(converted["tools"][0]["functionDeclarations"][0]["name"], "t");
    }

    #[test]
    fn responses_request_converts_function_call_to_tool_calls() {
        let input = br#"{"model":"codex","max_output_tokens":32,
            "input":[{"type":"function_call","call_id":"fc_1","name":"t","arguments":"{\"a\":1}"}]}"#;
        let converted = parse(&convert_request("openai_responses", "openai_compatible", "upstream-model", input).unwrap());
        assert_eq!(converted["messages"][0]["role"], "assistant");
        assert_eq!(converted["messages"][0]["tool_calls"][0]["function"]["name"], "t");
        assert_eq!(converted["max_tokens"], 32);
    }

    #[test]
    fn openai_response_converts_to_claude_with_thinking_and_tools() {
        let upstream = br#"{"id":"chat-1","choices":[{"message":{"reasoning_content":"think...",
            "content":"Done","tool_calls":[{"id":"call_1","type":"function","function":{"name":"t","arguments":"{\"a\":1}"}}]},
            "finish_reason":"tool_calls"}],"usage":{"prompt_tokens":4,"completion_tokens":2,
            "prompt_tokens_details":{"cached_tokens":1}}}"#;
        let converted = parse(&convert_response("claude", "openai_compatible", "claude-client", upstream).unwrap());
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
        let converted = parse(&convert_response("openai_responses", "openai_compatible", "codex-model", upstream).unwrap());
        assert_eq!(converted["object"], "response");
        assert_eq!(converted["status"], "incomplete");
        assert_eq!(converted["incomplete_details"]["reason"], "max_output_tokens");
        assert_eq!(converted["output"][0]["content"][0]["text"], "Hello");
        assert_eq!(converted["usage"]["input_tokens_details"]["cached_tokens"], 1);
        assert_eq!(converted["parallel_tool_calls"], true);
    }

    #[test]
    fn dsml_parses_invocations_in_chat_text() {
        let upstream = br#"{"id":"chat-1","choices":[{"message":{"content":"<|DSML|tool_calls><|DSML|invoke name=\"t\"><|DSML|parameter name=\"a\" string=\"true\">v</|DSML|parameter></|DSML|invoke></|DSML|tool_calls>"},"finish_reason":"stop"}],"usage":{}}"#;
        let converted = parse(&convert_response("openai_responses", "openai_compatible", "codex-model", upstream).unwrap());
        assert_eq!(converted["output"][0]["type"], "function_call");
        assert_eq!(converted["output"][0]["name"], "t");
        let arguments: Value = serde_json::from_str(converted["output"][0]["arguments"].as_str().unwrap()).unwrap();
        assert_eq!(arguments["a"], "v");
    }

    #[test]
    fn claude_stream_converts_openai_events_incrementally() {
        let mut converter = MappedStreamConverter::new("claude", "openai_compatible", "claude-client").unwrap();
        let mut out = Vec::new();
        out.extend(converter.feed(br#"data: {"choices":[{"delta":{"content":"Hi"},"finish_reason":null}]}

"#));
        out.extend(converter.feed(br#"data: {"choices":[{"delta":{},"finish_reason":"stop"}]}

data: [DONE]

"#));
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
        let mut converter = MappedStreamConverter::new("claude", "openai_compatible", "claude-client").unwrap();
        let mut out = Vec::new();
        out.extend(converter.feed(br#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"t","arguments":""}}]},"finish_reason":null}]}

"#));
        out.extend(converter.feed(br#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"a\":1}"}}]},"finish_reason":null}]}

"#));
        out.extend(converter.feed(br#"data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}

data: [DONE]

"#));
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
        let mut converter = MappedStreamConverter::new("openai_responses", "openai_compatible", "codex-model").unwrap();
        let mut out = Vec::new();
        out.extend(converter.feed(br#"data: {"choices":[{"delta":{"content":"Hi"},"finish_reason":null}]}

"#));
        out.extend(converter.feed(br#"data: {"choices":[{"delta":{},"finish_reason":"stop"}]}

data: [DONE]

"#));
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
        let mut converter = MappedStreamConverter::new("claude", "gemini", "claude-client").unwrap();
        let mut out = Vec::new();
        out.extend(converter.feed(b"{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Gem\"}]}}]}\n"));
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
        assert_eq!(stream_error_message("claude", &error).as_deref(), Some("stream broke"));
        let failed = json!({"type":"response.failed","response":{"error":{"message":"nope"}}});
        assert_eq!(stream_error_message("openai_responses", &failed).as_deref(), Some("nope"));
        assert!(stream_completed("claude", &json!({"type":"message_stop"})));
        assert!(stream_completed("openai_compatible", &json!({"choices":[{"finish_reason":"stop"}]})));
        assert!(!stream_completed("openai_compatible", &json!({"choices":[{"delta":{"content":"x"}}]})));
    }

    #[test]
    fn dsml_fullwidth_bars_do_not_panic() {
        // Regression: byte-offset slicing inside the 3-byte fullwidth bar '｜'
        // used to panic.
        let mut converter = MappedStreamConverter::new("openai_responses", "openai_compatible", "codex-model").unwrap();
        let out = converter.feed("data: {\"choices\":[{\"delta\":{\"content\":\"<｜DSML｜invoke\"},\"finish_reason\":null}]}\n\n".as_bytes());
        let out = String::from_utf8(out).unwrap();
        assert!(!out.is_empty());
    }
}
