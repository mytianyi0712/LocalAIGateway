//! 请求方向转换：入口协议 → 上游协议（request.rs）。

use super::*;

impl ConversionStrategy {
    /// Registry: which strategy converts `entry` requests for `upstream`.
    pub fn for_pair(entry: &str, upstream: &str) -> Option<Self> {
        use ConversionStrategy::*;
        match (entry, upstream) {
            ("claude", "claude") | ("openai_responses", "openai_responses") => Some(Passthrough),
            ("claude", "openai_compatible") => Some(ClaudeToChat),
            ("claude", "openai_responses") => Some(ClaudeToResponses),
            ("claude", "gemini") => Some(ClaudeToGemini),
            ("claude", "command_code") => Some(ClaudeToCommandCode),
            // OpenAI chat entry: identity for the Provider-API companion body,
            // and silent conversion into Command Code for direct requests.
            ("openai_compatible", "openai_compatible") => Some(Passthrough),
            ("openai_compatible", "command_code") => Some(ChatToCommandCode),
            ("openai_responses", "openai_compatible") => Some(ResponsesToChat),
            ("openai_responses", "claude") => Some(ResponsesToClaude),
            ("openai_responses", "gemini") => Some(ResponsesToGemini),
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

pub(super) fn claude_to_responses_value(upstream_model: &str, data: &Value) -> Value {
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
    if let Some(tool_choice) = data.get("tool_choice").and_then(tool_choice_to_openai) {
        payload["tool_choice"] = tool_choice;
    }
    payload
}

pub(super) fn claude_to_gemini(upstream_model: &str, data: &Value) -> Value {
    let mut tool_names: HashMap<&str, &str> = HashMap::new();
    if let Some(messages) = data.get("messages").and_then(Value::as_array) {
        for message in messages {
            let Some(blocks) = message.get("content").and_then(Value::as_array) else {
                continue;
            };
            for block in blocks {
                if block.get("type").and_then(Value::as_str) == Some("tool_use")
                    && let Some(id) = block.get("id").and_then(Value::as_str)
                {
                    tool_names.insert(
                        id,
                        block
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown_tool"),
                    );
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
        generation_config.insert(
            "maxOutputTokens".into(),
            data.get("max_tokens").cloned().unwrap(),
        );
    }
    for (from, to) in [
        ("temperature", "temperature"),
        ("top_p", "topP"),
        ("top_k", "topK"),
    ] {
        if let Some(value) = data.get(from) {
            generation_config.insert(to.into(), value.clone());
        }
    }
    if let Some(stop) = data
        .get("stop_sequences")
        .filter(|value| value.as_array().is_some_and(|items| !items.is_empty()))
    {
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

pub(super) fn responses_text(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Object(value) => value.get("text").and_then(Value::as_str).map(str::to_owned),
        _ => None,
    }
}

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

pub(super) fn responses_tools_to_claude(tools: &Value) -> Option<Vec<Value>> {
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
    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

pub(super) fn responses_tools_to_gemini(tools: &Value) -> Option<Vec<Value>> {
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

pub(super) fn parse_arguments(value: &Value) -> Value {
    if let Some(object) = value.as_object() {
        return Value::Object(object.clone());
    }
    let text = value.as_str().unwrap_or("{}");
    serde_json::from_str::<Value>(text).unwrap_or_else(|_| json!({}))
}

pub(super) fn parse_data_url(value: &Value) -> (String, String) {
    let Some(url) = value.as_str().filter(|url| url.starts_with("data:")) else {
        return ("image/png".into(), String::new());
    };
    let (meta, data) = url["data:".len()..]
        .split_once(',')
        .unwrap_or((&url["data:".len()..], ""));
    let media_type = meta.split(';').next().unwrap_or("image/png");
    let media_type = if media_type.is_empty() {
        "image/png"
    } else {
        media_type
    };
    (media_type.to_owned(), data.to_owned())
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
    if data
        .get("max_output_tokens")
        .is_some_and(|value| value.is_i64())
    {
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
    if let Some(tool_choice) = data.get("tool_choice").and_then(responses_tool_choice) {
        payload["tool_choice"] = tool_choice;
    }
    payload
}

pub(super) fn responses_to_claude(upstream_model: &str, data: &Value) -> Value {
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
                    messages.push(
                        json!({"role": role, "content": [{"type": "text", "text": content}]}),
                    );
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
                                &responses_image_url(part)
                                    .map(Value::String)
                                    .unwrap_or(Value::Null),
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
    if data
        .get("max_output_tokens")
        .is_some_and(|value| value.is_i64())
    {
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

pub(super) fn responses_to_gemini(upstream_model: &str, data: &Value) -> Value {
    let mut call_names: HashMap<&str, &str> = HashMap::new();
    if let Some(items) = data.get("input").and_then(Value::as_array) {
        for item in items {
            if item.get("type").and_then(Value::as_str) == Some("function_call")
                && let Some(call_id) = item.get("call_id").and_then(Value::as_str)
            {
                call_names.insert(
                    call_id,
                    item.get("name")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown_tool"),
                );
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
                                    &responses_image_url(part)
                                        .map(Value::String)
                                        .unwrap_or(Value::Null),
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
    if data
        .get("max_output_tokens")
        .is_some_and(|value| value.is_i64())
    {
        generation_config.insert(
            "maxOutputTokens".into(),
            data.get("max_output_tokens").cloned().unwrap(),
        );
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
        ClaudeToResponses => claude_to_responses_value(upstream_model, &data),
        ClaudeToGemini => claude_to_gemini(upstream_model, &data),
        ClaudeToCommandCode => super::commandcode::claude_to_commandcode(upstream_model, &data),
        ChatToCommandCode => super::commandcode::openai_chat_to_commandcode(upstream_model, &data),
        ResponsesToChat => responses_to_chat(upstream_model, &data),
        ResponsesToClaude => responses_to_claude(upstream_model, &data),
        ResponsesToGemini => responses_to_gemini(upstream_model, &data),
        ResponsesToCommandCode => {
            super::commandcode::responses_to_commandcode(upstream_model, &data)
        }
    };
    Ok(serde_json::to_vec(&converted)?)
}

// ---------------------------------------------------------------------------
// upstream -> Claude response conversion (non-streaming)
// ---------------------------------------------------------------------------
