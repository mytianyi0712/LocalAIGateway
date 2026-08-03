use anyhow::{Result, bail};
use serde_json::{Value, json};
use std::time::{SystemTime, UNIX_EPOCH};

fn object(body: &[u8]) -> Result<Value> {
    let value: Value = serde_json::from_slice(body)?;
    if !value.is_object() {
        bail!("请求体必须是 JSON 对象");
    }
    Ok(value)
}

fn text(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Array(values) => values
            .iter()
            .filter_map(|item| {
                if item.get("type").and_then(Value::as_str) == Some("text") {
                    item.get("text").and_then(Value::as_str).map(str::to_owned)
                } else if item.get("type").and_then(Value::as_str) == Some("output_text") {
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

fn claude_content(value: &Value) -> Value {
    match value {
        Value::String(value) => json!(value),
        Value::Array(parts) => {
            let converted: Vec<Value> = parts.iter().filter_map(|part| {
                match part.get("type").and_then(Value::as_str) {
                    Some("text") => Some(json!({"type":"text","text":part.get("text").and_then(Value::as_str).unwrap_or_default()})),
                    Some("image") => part.get("source").map(|source| json!({"type":"image_url","image_url":{"url":format!("data:{};base64,{}", source.get("media_type").and_then(Value::as_str).unwrap_or("application/octet-stream"), source.get("data").and_then(Value::as_str).unwrap_or_default())}})),
                    Some("tool_result") => Some(json!({"type":"text","text":text(part.get("content").unwrap_or(&Value::Null))})),
                    _ => None,
                }
            }).collect();
            if converted.len() == 1
                && converted[0].get("type").and_then(Value::as_str) == Some("text")
            {
                converted[0]
                    .get("text")
                    .cloned()
                    .unwrap_or(Value::String(String::new()))
            } else {
                Value::Array(converted)
            }
        }
        _ => Value::String(text(value)),
    }
}

fn claude_to_chat(model: &str, data: &Value) -> Value {
    let mut messages = Vec::new();
    if let Some(system) = system_text(data.get("system")) {
        messages.push(json!({"role":"system","content":system}));
    }
    if let Some(items) = data.get("messages").and_then(Value::as_array) {
        for item in items {
            let role = item.get("role").and_then(Value::as_str).unwrap_or("user");
            let content = item
                .get("content")
                .map(claude_content)
                .unwrap_or(Value::String(String::new()));
            messages.push(json!({"role":role,"content":content}));
        }
    }
    let mut payload = json!({"model":model,"messages":messages});
    let object = payload.as_object_mut().expect("object");
    for (from, to) in [
        ("max_tokens", "max_tokens"),
        ("temperature", "temperature"),
        ("top_p", "top_p"),
        ("stream", "stream"),
    ] {
        if let Some(value) = data.get(from) {
            object.insert(to.to_owned(), value.clone());
        }
    }
    if let Some(stop) = data.get("stop_sequences") {
        object.insert("stop".into(), stop.clone());
    }
    if let Some(tools) = data.get("tools") {
        object.insert("tools".into(), tools.clone());
    }
    object.insert("model".into(), json!(model));
    payload
}

fn responses_messages(data: &Value) -> Vec<Value> {
    let mut messages = Vec::new();
    if let Some(instructions) = system_text(data.get("instructions")) {
        messages.push(json!({"role":"system","content":instructions}));
    }
    match data.get("input") {
        Some(Value::String(value)) => messages.push(json!({"role":"user","content":value})),
        Some(Value::Array(items)) => {
            for item in items {
                let item_type = item.get("type").and_then(Value::as_str).unwrap_or("");
                if item_type == "message" || item.get("role").is_some() {
                    let role = item.get("role").and_then(Value::as_str).unwrap_or("user");
                    messages.push(json!({"role":role,"content":claude_content(item.get("content").unwrap_or(&Value::Null))}));
                } else if item_type == "function_call_output" {
                    messages.push(json!({"role":"tool","content":item.get("output").cloned().unwrap_or(Value::Null),"tool_call_id":item.get("call_id").cloned().unwrap_or(Value::Null)}));
                }
            }
        }
        _ => {}
    }
    messages
}

fn responses_to_chat(model: &str, data: &Value) -> Value {
    let mut payload = json!({"model":model,"messages":responses_messages(data)});
    let object = payload.as_object_mut().expect("object");
    for (from, to) in [
        ("temperature", "temperature"),
        ("top_p", "top_p"),
        ("stream", "stream"),
    ] {
        if let Some(value) = data.get(from) {
            object.insert(to.to_owned(), value.clone());
        }
    }
    if let Some(value) = data.get("max_output_tokens") {
        object.insert("max_tokens".into(), value.clone());
    }
    if let Some(value) = data.get("tools") {
        object.insert("tools".into(), value.clone());
    }
    payload
}

fn messages_to_claude(model: &str, data: &Value) -> Value {
    let mut messages = Vec::new();
    let mut system = None;
    for item in responses_messages(data) {
        let role = item.get("role").and_then(Value::as_str).unwrap_or("user");
        if role == "system" {
            system = item.get("content").cloned();
        } else {
            messages.push(json!({"role":role,"content":[{"type":"text","text":text(item.get("content").unwrap_or(&Value::Null))}]}));
        }
    }
    let mut payload = json!({"model":model,"messages":messages,"max_tokens":data.get("max_output_tokens").cloned().unwrap_or(json!(1024))});
    if let Some(value) = system {
        payload["system"] = value;
    }
    if let Some(value) = data.get("stream") {
        payload["stream"] = value.clone();
    }
    if let Some(value) = data.get("temperature") {
        payload["temperature"] = value.clone();
    }
    if let Some(value) = data.get("top_p") {
        payload["top_p"] = value.clone();
    }
    payload
}

fn claude_to_gemini(model: &str, data: &Value) -> Value {
    let mut contents = Vec::new();
    if let Some(system) = system_text(data.get("system")) {
        contents.push(
            json!({"role":"user","parts":[{"text":format!("System instructions:\n{system}")}]}),
        );
    }
    if let Some(items) = data.get("messages").and_then(Value::as_array) {
        for item in items {
            let role = if item.get("role").and_then(Value::as_str) == Some("assistant") {
                "model"
            } else {
                "user"
            };
            contents.push(json!({"role":role,"parts":[{"text":text(item.get("content").unwrap_or(&Value::Null))}]}));
        }
    }
    let mut payload = json!({"contents":contents,"generationConfig":{"maxOutputTokens":data.get("max_tokens").cloned().unwrap_or(json!(1024))}});
    if let Some(value) = data.get("temperature") {
        payload["generationConfig"]["temperature"] = value.clone();
    }
    if let Some(value) = data.get("top_p") {
        payload["generationConfig"]["topP"] = value.clone();
    }
    if let Some(value) = data.get("stop_sequences") {
        payload["generationConfig"]["stopSequences"] = value.clone();
    }
    if let Some(tools) = data.get("tools") {
        payload["tools"] = json!([{"functionDeclarations":tools}]);
    }
    let _ = model;
    payload
}

fn responses_to_gemini(data: &Value) -> Value {
    let messages = responses_messages(data);
    let contents: Vec<Value> = messages.into_iter().filter_map(|item| {
        let role = if item.get("role").and_then(Value::as_str) == Some("assistant") { "model" } else { "user" };
        Some(json!({"role":role,"parts":[{"text":text(item.get("content").unwrap_or(&Value::Null))}]}))
    }).collect();
    json!({"contents":contents,"generationConfig":{"maxOutputTokens":data.get("max_output_tokens").cloned().unwrap_or(json!(1024))}})
}

pub fn convert_request(
    entry: &str,
    upstream_protocol: &str,
    upstream_model: &str,
    body: &[u8],
) -> Result<Vec<u8>> {
    let data = object(body)?;
    let converted = match (entry, upstream_protocol) {
        ("claude", "openai_compatible") => claude_to_chat(upstream_model, &data),
        ("claude", "openai_responses") => claude_to_responses_value(upstream_model, &data),
        ("claude", "claude") => {
            let mut value = data;
            value["model"] = json!(upstream_model);
            value
        }
        ("claude", "gemini") => claude_to_gemini(upstream_model, &data),
        ("openai_responses", "openai_compatible") => responses_to_chat(upstream_model, &data),
        ("openai_responses", "openai_responses") => {
            let mut value = data;
            value["model"] = json!(upstream_model);
            value
        }
        ("openai_responses", "claude") => messages_to_claude(upstream_model, &data),
        ("openai_responses", "gemini") => responses_to_gemini(&data),
        _ => bail!("不支持的映射：{entry} -> {upstream_protocol}"),
    };
    Ok(serde_json::to_vec(&converted)?)
}

fn claude_to_responses_value(model: &str, data: &Value) -> Value {
    let mut input = Vec::new();
    if let Some(system) = system_text(data.get("system")) {
        input.push(json!({"type":"message","role":"developer","content":[{"type":"input_text","text":system}]}));
    }
    if let Some(items) = data.get("messages").and_then(Value::as_array) {
        for item in items {
            input.push(json!({"type":"message","role":item.get("role").cloned().unwrap_or(json!("user")),"content":[{"type":"input_text","text":text(item.get("content").unwrap_or(&Value::Null))}]}));
        }
    }
    json!({"model":model,"input":input,"max_output_tokens":data.get("max_tokens").cloned().unwrap_or(json!(1024)),"stream":data.get("stream").cloned().unwrap_or(json!(false))})
}

fn output_text(data: &Value) -> String {
    if let Some(value) = data
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|v| v.first())
        .and_then(|v| v.get("message"))
        .and_then(|v| v.get("content"))
    {
        return text(value);
    }
    if let Some(value) = data.get("output_text").and_then(Value::as_str) {
        return value.to_owned();
    }
    if let Some(items) = data.get("output").and_then(Value::as_array) {
        return items
            .iter()
            .filter_map(|item| item.get("content"))
            .map(text)
            .collect::<Vec<_>>()
            .join("");
    }
    data.get("candidates")
        .and_then(Value::as_array)
        .and_then(|v| v.first())
        .and_then(|v| v.get("content"))
        .and_then(|v| v.get("parts"))
        .map(text)
        .unwrap_or_default()
}

fn usage(data: &Value) -> Value {
    let value = data.get("usage").unwrap_or(&Value::Null);
    let input = value
        .get("input_tokens")
        .or_else(|| value.get("prompt_tokens"))
        .or_else(|| value.get("promptTokenCount"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let output = value
        .get("output_tokens")
        .or_else(|| value.get("completion_tokens"))
        .or_else(|| value.get("candidatesTokenCount"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    json!({"input_tokens":input,"output_tokens":output})
}

fn to_claude(model: &str, data: &Value) -> Value {
    let message = output_text(data);
    let stop_reason = data
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|v| v.first())
        .and_then(|v| v.get("finish_reason"))
        .and_then(Value::as_str)
        .unwrap_or("end_turn");
    let u = usage(data);
    json!({"id":data.get("id").cloned().unwrap_or_else(||json!("msg_gateway")),"type":"message","role":"assistant","model":model,"content":[{"type":"text","text":message}],"stop_reason":stop_reason,"stop_sequence":null,"usage":{"input_tokens":u["input_tokens"],"output_tokens":u["output_tokens"]}})
}

fn to_responses(model: &str, data: &Value) -> Value {
    let message = output_text(data);
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|v| v.as_secs())
        .unwrap_or_default();
    let u = usage(data);
    json!({"id":data.get("id").cloned().unwrap_or_else(||json!("resp_gateway")),"object":"response","created_at":timestamp,"status":"completed","model":model,"output":[{"id":"msg_gateway","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":message,"annotations":[]}]}],"output_text":message,"usage":{"input_tokens":u["input_tokens"],"output_tokens":u["output_tokens"]}})
}

pub fn convert_response(
    entry: &str,
    upstream_protocol: &str,
    mapped_model: &str,
    body: &[u8],
) -> Result<Vec<u8>> {
    let data = object(body)?;
    let converted = match entry {
        "claude" => to_claude(mapped_model, &data),
        "openai_responses" => to_responses(mapped_model, &data),
        _ => bail!("不支持的入口协议：{entry}"),
    };
    let _ = upstream_protocol;
    Ok(serde_json::to_vec(&converted)?)
}

fn sse_data(body: &[u8]) -> Vec<Value> {
    String::from_utf8_lossy(body)
        .split("\n\n")
        .filter_map(|event| {
            let data = event
                .lines()
                .filter_map(|line| line.strip_prefix("data:"))
                .map(str::trim)
                .collect::<Vec<_>>()
                .join("\n");
            if data.is_empty() || data == "[DONE]" {
                None
            } else {
                serde_json::from_str(&data).ok()
            }
        })
        .collect()
}

fn stream_text(upstream_protocol: &str, body: &[u8]) -> String {
    sse_data(body)
        .iter()
        .filter_map(|value| match upstream_protocol {
            "openai_compatible" => value
                .pointer("/choices/0/delta/content")
                .and_then(Value::as_str)
                .or_else(|| value.pointer("/choices/0/text").and_then(Value::as_str)),
            "openai_responses" => value
                .get("delta")
                .and_then(Value::as_str)
                .or_else(|| value.get("text").and_then(Value::as_str)),
            "claude" => value.pointer("/delta/text").and_then(Value::as_str),
            "gemini" => value
                .pointer("/candidates/0/content/parts/0/text")
                .and_then(Value::as_str),
            _ => None,
        })
        .collect()
}

fn sse(event: &str, value: Value) -> String {
    format!("event: {event}\ndata: {}\n\n", value)
}

pub fn convert_stream(
    entry: &str,
    upstream_protocol: &str,
    mapped_model: &str,
    body: &[u8],
) -> Result<Vec<u8>> {
    let joined = stream_text(upstream_protocol, body);
    if entry == "claude" {
        let mut output = String::new();
        output.push_str(&sse("message_start", json!({"type":"message_start","message":{"id":"msg_gateway","type":"message","role":"assistant","content":[],"model":mapped_model,"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":0,"output_tokens":0}}})));
        output.push_str(&sse("content_block_start", json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}})));
        if !joined.is_empty() {
            output.push_str(&sse("content_block_delta", json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":joined}})));
        }
        output.push_str(&sse(
            "content_block_stop",
            json!({"type":"content_block_stop","index":0}),
        ));
        output.push_str(&sse("message_delta", json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":0}})));
        output.push_str(&sse("message_stop", json!({"type":"message_stop"})));
        Ok(output.into_bytes())
    } else {
        let response = json!({"id":"resp_gateway","object":"response","status":"completed","model":mapped_model,"output_text":joined});
        let mut output = String::new();
        output.push_str(&format!(
            "data: {}\n\n",
            json!({"type":"response.created","response":response})
        ));
        if !joined.is_empty() {
            output.push_str(&format!("data: {}\n\n", json!({"type":"response.output_text.delta","delta":joined,"output_index":0,"content_index":0})));
        }
        output.push_str(&format!(
            "data: {}\n\n",
            json!({"type":"response.completed","response":response})
        ));
        output.push_str("data: [DONE]\n\n");
        Ok(output.into_bytes())
    }
}

pub fn convert_error(entry: &str, body: &[u8]) -> Vec<u8> {
    let message = serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|value| {
            value
                .pointer("/error/message")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| String::from_utf8_lossy(body).to_string());
    if entry == "claude" {
        serde_json::to_vec(&json!({"type":"error","error":{"type":"api_error","message":message}}))
            .unwrap_or_else(|_| body.to_vec())
    } else {
        serde_json::to_vec(&json!({"error":{"type":"upstream_error","message":message}}))
            .unwrap_or_else(|_| body.to_vec())
    }
}

pub fn inspect_model(entry: &str, body: &[u8]) -> Option<String> {
    let value = serde_json::from_slice::<Value>(body).ok()?;
    value
        .get("model")
        .and_then(Value::as_str)
        .or_else(|| {
            if entry == "openai_responses" {
                value.get("model").and_then(Value::as_str)
            } else {
                None
            }
        })
        .map(str::to_owned)
}

pub fn stream_requested(body: &[u8]) -> bool {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|value| value.get("stream").and_then(Value::as_bool))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claude_request_maps_system_and_messages() {
        let input = br#"{"model":"claude-client","system":"Be concise","max_tokens":32,"messages":[{"role":"user","content":"Hello"}]}"#;
        let value: Value = serde_json::from_slice(
            &convert_request("claude", "openai_compatible", "upstream-model", input).unwrap(),
        )
        .unwrap();
        assert_eq!(value["model"], "upstream-model");
        assert_eq!(value["messages"][0]["role"], "system");
        assert_eq!(value["messages"][1]["content"], "Hello");
    }

    #[test]
    fn claude_request_maps_to_responses_input() {
        let input=br#"{"model":"claude-client","max_tokens":32,"messages":[{"role":"user","content":"Hello"}]}"#;
        let value: Value = serde_json::from_slice(
            &convert_request("claude", "openai_responses", "upstream-model", input).unwrap(),
        )
        .unwrap();
        assert_eq!(value["model"], "upstream-model");
        assert_eq!(value["input"][0]["type"], "message");
        assert_eq!(value["input"][0]["content"][0]["text"], "Hello");
    }

    #[test]
    fn openai_response_maps_to_claude_message() {
        let upstream=br#"{"id":"chat-1","choices":[{"message":{"content":"Done"},"finish_reason":"stop"}],"usage":{"prompt_tokens":4,"completion_tokens":2}}"#;
        let value: Value = serde_json::from_slice(
            &convert_response("claude", "openai_compatible", "claude-client", upstream).unwrap(),
        )
        .unwrap();
        assert_eq!(value["model"], "claude-client");
        assert_eq!(value["content"][0]["text"], "Done");
        assert_eq!(value["usage"]["input_tokens"], 4);
    }

    #[test]
    fn mapped_stream_emits_entry_events() {
        let upstream = br#"data: {"choices":[{"delta":{"content":"Hi"}}]}

data: [DONE]

"#;
        let output = String::from_utf8(
            convert_stream("claude", "openai_compatible", "claude-client", upstream).unwrap(),
        )
        .unwrap();
        assert!(output.contains("event: message_start"));
        assert!(output.contains("text_delta"));
        assert!(output.contains("Hi"));
        assert!(output.contains("event: message_stop"));
    }
}
