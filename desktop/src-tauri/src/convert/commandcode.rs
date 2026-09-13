//! Command Code（CC）上游协议转换（硬坑 1/2/3/4/6/7 的落点）。
//!
//! 三条通路：
//! 1. 入口请求 → CC `/alpha/generate` 请求体（[`claude_to_commandcode`] /
//!    [`responses_to_commandcode`]）；
//! 2. CC NDJSON → canonical OpenAI chat SSE（[`CommandCodeDecoder`]），
//!    交给既有 `(entry, "openai_compatible")` 流转换复用；
//! 3. CC NDJSON → 单个 OpenAI chat completion（[`ndjson_to_chat_completion`]），
//!    供非流式入口复用既有响应转换。
//!
//! 事件表与硬坑清单见 `docs/command-code-protocol.md`。

use std::collections::{HashMap, HashSet};

use serde_json::{Value, json};

use super::*;

/// `params.system` 的空占位（硬坑 4）：缺省会让上游注入 ~7.5K token 默认提示词。
pub const EMPTY_SYSTEM_PLACEHOLDER: &str = " ";
/// 社区实现夹取的 `max_tokens` 上限。
pub const MAX_TOKENS_CAP: i64 = 200_000;
/// 缺失 tool result 的合成文案（硬坑 3）。
pub const MISSING_TOOL_RESULT: &str =
    "No result — the tool call did not complete (interrupted or lost).";

/// CC usage 根部的标记：`prompt_tokens` 是 TOTAL（含缓存命中）。
/// 流/非流转换据此换算 Anthropic 的 `input_tokens`（硬坑 9），
/// 不影响其他上游（它们不带这个标记）。
pub const INPUT_INCLUDES_CACHE: &str = "prompt_tokens_includes_cache";

// ---------------------------------------------------------------------------
// 请求构造
// ---------------------------------------------------------------------------

fn envelope(params: Value) -> Value {
    json!({
        "config": {
            "workingDir": "/",
            "date": chrono::Utc::now().format("%Y-%m-%d").to_string(),
            "environment": format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
            "structure": [],
            "isGitRepo": false,
            "currentBranch": "",
            "mainBranch": "",
            "gitStatus": "",
            "recentCommits": []
        },
        "memory": Value::Null,
        "taste": Value::Null,
        "skills": "",
        "permissionMode": "standard",
        "params": params
    })
}

fn clamp_max_tokens(value: Option<&Value>, fallback: i64) -> i64 {
    value
        .and_then(Value::as_i64)
        .unwrap_or(fallback)
        .clamp(1, MAX_TOKENS_CAP)
}

/// `params.system` 恒为字符串；空提示用空格占位（硬坑 4）。
fn system_param(system: Option<String>) -> Value {
    match system {
        Some(text) if !text.trim().is_empty() => json!(text),
        _ => json!(EMPTY_SYSTEM_PLACEHOLDER),
    }
}

fn tool_result_output(value: &Value, is_error: bool) -> Value {
    let text = match value {
        Value::String(text) => text.clone(),
        Value::Array(_) | Value::Object(_) => {
            let joined = content_text(value);
            if joined.is_empty() {
                serde_json::to_string(value).unwrap_or_default()
            } else {
                joined
            }
        }
        Value::Null => String::new(),
        other => other.to_string(),
    };
    json!({
        "type": if is_error { "error-text" } else { "text" },
        "value": text
    })
}

/// Claude tools → CC（Anthropic 形）。
fn claude_tools_to_cc(tools: Option<&Value>) -> Option<Value> {
    let tools = tools?.as_array()?;
    let converted: Vec<Value> = tools
        .iter()
        .filter_map(|tool| {
            let name = tool.get("name").and_then(Value::as_str)?;
            Some(json!({
                "type": tool.get("type").and_then(Value::as_str).unwrap_or("function"),
                "name": name,
                "description": tool.get("description").and_then(Value::as_str).unwrap_or(""),
                "input_schema": tool.get("input_schema").cloned()
                    .unwrap_or_else(|| json!({"type": "object", "properties": {}})),
            }))
        })
        .collect();
    (!converted.is_empty()).then(|| Value::Array(converted))
}

fn claude_tool_choice_to_cc(choice: Option<&Value>) -> Option<Value> {
    let choice = choice?;
    let kind = choice.get("type").and_then(Value::as_str).unwrap_or("auto");
    Some(match kind {
        "any" => json!({"type": "any"}),
        "none" => json!({"type": "none"}),
        "tool" => json!({
            "type": "tool",
            "name": choice.get("name").and_then(Value::as_str).unwrap_or("")
        }),
        _ => json!({"type": "auto"}),
    })
}

/// OpenAI chat `content`（string 或 parts 数组）→ 纯文本。
fn openai_content_text(content: &Value) -> String {
    match content {
        Value::String(text) => text.clone(),
        Value::Array(parts) => parts
            .iter()
            .filter_map(|part| match part.get("type").and_then(Value::as_str) {
                Some("text" | "input_text" | "output_text") => {
                    part.get("text").and_then(Value::as_str)
                }
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(""),
        _ => String::new(),
    }
}

/// OpenAI chat tools（`{type:"function",function:{...}}`）→ CC。
fn openai_tools_to_cc(tools: Option<&Value>) -> Option<Value> {
    let tools = tools?.as_array()?;
    let converted: Vec<Value> = tools
        .iter()
        .filter_map(|tool| {
            let function = tool.get("function").unwrap_or(tool);
            let name = function.get("name").and_then(Value::as_str)?;
            Some(json!({
                "type": "function",
                "name": name,
                "description": function.get("description").and_then(Value::as_str).unwrap_or(""),
                "input_schema": function.get("parameters").cloned()
                    .unwrap_or_else(|| json!({"type": "object", "properties": {}})),
            }))
        })
        .collect();
    (!converted.is_empty()).then(|| Value::Array(converted))
}

fn openai_tool_choice_to_cc(choice: Option<&Value>) -> Option<Value> {
    let choice = choice?;
    let kind = match choice {
        Value::String(kind) => kind.as_str(),
        _ => choice.get("type").and_then(Value::as_str).unwrap_or("auto"),
    };
    Some(match kind {
        "none" => json!({"type": "none"}),
        "required" => json!({"type": "any"}),
        "function" => json!({
            "type": "tool",
            "name": choice
                .pointer("/function/name")
                .and_then(Value::as_str)
                .unwrap_or("")
        }),
        _ => json!({"type": "auto"}),
    })
}

/// Responses tools（扁平 `{type,name,parameters}`）→ CC。
fn responses_tools_to_cc(tools: Option<&Value>) -> Option<Value> {
    let tools = tools?.as_array()?;
    let converted: Vec<Value> = tools
        .iter()
        .filter(|tool| tool.get("type").and_then(Value::as_str) == Some("function"))
        .filter_map(|tool| {
            let name = tool.get("name").and_then(Value::as_str)?;
            Some(json!({
                "type": "function",
                "name": name,
                "description": tool.get("description").and_then(Value::as_str).unwrap_or(""),
                "input_schema": tool.get("parameters").cloned()
                    .unwrap_or_else(|| json!({"type": "object", "properties": {}})),
            }))
        })
        .collect();
    (!converted.is_empty()).then(|| Value::Array(converted))
}

fn responses_tool_choice_to_cc(choice: Option<&Value>) -> Option<Value> {
    let choice = choice?;
    let kind = choice.get("type").and_then(Value::as_str).unwrap_or("auto");
    Some(match kind {
        "function" => json!({
            "type": "tool",
            "name": choice.get("name")
                .or_else(|| choice.pointer("/function/name"))
                .and_then(Value::as_str)
                .unwrap_or("")
        }),
        "required" => json!({"type": "any"}),
        "none" => json!({"type": "none"}),
        _ => json!({"type": "auto"}),
    })
}

/// CC 消息装配（硬坑 1/2/3/7）：
/// - assistant parts 顺序固定 `[reasoning, text, tool-call]`；
/// - tool result 紧跟对应 assistant 消息（缺失时合成 `error-text`）；
/// - 角色只输出 user/assistant/tool。
#[derive(Default)]
struct CcMessages {
    out: Vec<Value>,
    result_ids: HashSet<String>,
    emitted_results: HashSet<String>,
    tool_names: HashMap<String, String>,
}

impl CcMessages {
    fn note_result(&mut self, call_id: &str) {
        if !call_id.is_empty() {
            self.result_ids.insert(call_id.to_owned());
        }
    }

    fn note_call(&mut self, call_id: &str, tool_name: &str) {
        if !call_id.is_empty() {
            self.tool_names
                .insert(call_id.to_owned(), tool_name.to_owned());
        }
    }

    fn tool_name(&self, call_id: &str) -> String {
        self.tool_names.get(call_id).cloned().unwrap_or_default()
    }

    fn push_user(&mut self, parts: Vec<Value>) {
        let parts: Vec<Value> = parts.into_iter().filter(|part| !part.is_null()).collect();
        if !parts.is_empty() {
            self.out
                .push(json!({"role": "user", "content": parts}));
        }
    }

    fn push_tool(&mut self, parts: Vec<Value>) {
        if !parts.is_empty() {
            self.out
                .push(json!({"role": "tool", "content": parts}));
        }
    }

    /// Assistant 消息 + 该消息中缺失结果的合成 tool 回复（硬坑 3）。
    fn push_assistant(&mut self, parts: Vec<Value>, calls: Vec<(String, String)>) {
        let mut parts: Vec<Value> = parts.into_iter().filter(|part| !part.is_null()).collect();
        if parts.is_empty() && calls.is_empty() {
            return;
        }
        let missing: Vec<Value> = calls
            .iter()
            .filter(|(id, _)| !self.result_ids.contains(id) && !id.is_empty())
            .map(|(id, name)| {
                json!({
                    "type": "tool-result",
                    "toolCallId": id,
                    "toolName": name,
                    "output": {"type": "error-text", "value": MISSING_TOOL_RESULT}
                })
            })
            .collect();
        if parts.is_empty() && missing.is_empty() {
            return;
        }
        // A tool-only assistant turn still needs a valid parts array.
        if parts.is_empty() {
            parts.push(json!({"type": "text", "text": ""}));
        }
        self.out.push(json!({"role": "assistant", "content": parts}));
        if !missing.is_empty() {
            self.push_tool(missing);
        }
    }

    /// 已存在 assistant 消息时追加 parts（Responses 的连续 function_call）。
    fn append_assistant(&mut self, parts: Vec<Value>, calls: Vec<(String, String)>) -> bool {
        let is_assistant = self
            .out
            .last()
            .and_then(|message| message.get("role"))
            .and_then(Value::as_str)
            == Some("assistant");
        if !is_assistant {
            return false;
        }
        if let Some(message) = self.out.last_mut()
            && let Some(content) = message.get_mut("content").and_then(Value::as_array_mut)
        {
            let empty_text = |part: &Value| {
                part.get("type").and_then(Value::as_str) == Some("text")
                    && part.get("text").and_then(Value::as_str).is_none_or(str::is_empty)
            };
            let had_text = content.iter().any(|part| !empty_text(part));
            for part in parts {
                if empty_text(&part) && had_text {
                    continue;
                }
                content.retain(|current| !(empty_text(current) && empty_text(&part)));
                content.push(part);
            }
        }
        // 合并后仍缺失的 tool result：与 push_assistant 相同的合成规则。
        let missing: Vec<Value> = calls
            .iter()
            .filter(|(id, _)| !self.result_ids.contains(id) && !id.is_empty())
            .map(|(id, name)| {
                json!({
                    "type": "tool-result",
                    "toolCallId": id,
                    "toolName": name,
                    "output": {"type": "error-text", "value": MISSING_TOOL_RESULT}
                })
            })
            .collect();
        if !missing.is_empty() {
            self.push_tool(missing);
        }
        true
    }

    /// 显式 tool-result part（已经过 dedupe）。
    fn tool_result_part(
        &mut self,
        call_id: &str,
        tool_name: Option<&str>,
        output: Value,
    ) -> Option<Value> {
        if call_id.is_empty() || self.emitted_results.contains(call_id) {
            return None;
        }
        self.emitted_results.insert(call_id.to_owned());
        Some(json!({
            "type": "tool-result",
            "toolCallId": call_id,
            "toolName": tool_name
                .map(str::to_owned)
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| self.tool_name(call_id)),
            "output": output
        }))
    }

    fn finish(self) -> Vec<Value> {
        self.out
    }
}

/// Claude `/v1/messages` 请求 → CC `/alpha/generate` 请求体。
pub(super) fn claude_to_commandcode(upstream_model: &str, data: &Value) -> Value {
    let mut builder = CcMessages::default();
    let messages = data
        .get("messages")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    // 预扫描所有 tool_result / tool_use，供「缺失 result 补齐」判断（硬坑 3）。
    for message in &messages {
        let Some(blocks) = message.get("content").and_then(Value::as_array) else {
            continue;
        };
        for block in blocks {
            match block.get("type").and_then(Value::as_str) {
                Some("tool_result") => {
                    if let Some(id) = block.get("tool_use_id").and_then(Value::as_str) {
                        builder.note_result(id);
                    }
                }
                Some("tool_use") => {
                    if let (Some(id), Some(name)) = (
                        block.get("id").and_then(Value::as_str),
                        block.get("name").and_then(Value::as_str),
                    ) {
                        builder.note_call(id, name);
                    }
                }
                _ => {}
            }
        }
    }
    for message in &messages {
        let role = message.get("role").and_then(Value::as_str).unwrap_or("user");
        let content = message.get("content").unwrap_or(&Value::Null);
        match role {
            "assistant" => {
                let mut parts: Vec<Value> = Vec::new();
                let mut calls: Vec<(String, String)> = Vec::new();
                if let Some(blocks) = content.as_array() {
                    for block in blocks {
                        match block.get("type").and_then(Value::as_str) {
                            // 硬坑 2：thinking 必须回传成 reasoning。
                            Some("thinking") => {
                                let text = block
                                    .get("thinking")
                                    .or_else(|| block.get("text"))
                                    .and_then(Value::as_str)
                                    .unwrap_or("");
                                if !text.is_empty() {
                                    parts.push(json!({"type": "reasoning", "text": text}));
                                }
                            }
                            Some("redacted_thinking") => {
                                if let Some(value) = block.get("data").and_then(Value::as_str) {
                                    parts.push(json!({"type": "reasoning", "text": value}));
                                }
                            }
                            Some("text") => {
                                if let Some(text) = block.get("text").and_then(Value::as_str) {
                                    parts.push(json!({"type": "text", "text": text}));
                                }
                            }
                            Some("tool_use") => {
                                let id = block
                                    .get("id")
                                    .and_then(Value::as_str)
                                    .map(str::to_owned)
                                    .unwrap_or_else(|| new_id("call"));
                                let name = block
                                    .get("name")
                                    .and_then(Value::as_str)
                                    .unwrap_or("")
                                    .to_owned();
                                builder.note_call(&id, &name);
                                parts.push(json!({
                                    "type": "tool-call",
                                    "toolCallId": id.clone(),
                                    "toolName": name.clone(),
                                    "input": block.get("input").cloned().unwrap_or_else(|| json!({})),
                                }));
                                calls.push((id, name));
                            }
                            _ => {}
                        }
                    }
                } else if let Some(text) = content.as_str() {
                    parts.push(json!({"type": "text", "text": text}));
                }
                builder.push_assistant(parts, calls);
            }
            "user" => {
                // 硬坑 7：tool_result 先于 user 文本；已有 assistant 调用后紧跟。
                let mut tool_parts: Vec<Value> = Vec::new();
                let mut user_parts: Vec<Value> = Vec::new();
                if let Some(blocks) = content.as_array() {
                    for block in blocks {
                        match block.get("type").and_then(Value::as_str) {
                            Some("tool_result") => {
                                let id = block
                                    .get("tool_use_id")
                                    .and_then(Value::as_str)
                                    .unwrap_or("");
                                let is_error = block
                                    .get("is_error")
                                    .and_then(Value::as_bool)
                                    .unwrap_or(false);
                                if let Some(part) = builder.tool_result_part(
                                    id,
                                    None,
                                    tool_result_output(
                                        block.get("content").unwrap_or(&Value::Null),
                                        is_error,
                                    ),
                                ) {
                                    tool_parts.push(part);
                                }
                            }
                            Some("text") => {
                                if let Some(text) = block.get("text").and_then(Value::as_str) {
                                    user_parts.push(json!({"type": "text", "text": text}));
                                }
                            }
                            // 硬坑 6：图片是 data URL 的 `{type:'image', image}`。
                            Some("image") => {
                                let source = block.get("source").unwrap_or(&Value::Null);
                                user_parts.push(json!({
                                    "type": "image",
                                    "image": image_data_url(source, "image/png"),
                                }));
                            }
                            _ => {}
                        }
                    }
                } else if let Some(text) = content.as_str() {
                    user_parts.push(json!({"type": "text", "text": text}));
                }
                if !tool_parts.is_empty() {
                    builder.push_tool(tool_parts);
                }
                if !user_parts.is_empty() {
                    builder.push_user(user_parts);
                }
            }
            // 硬坑：role 仅接受 user/assistant/tool，未知角色降级为 user。
            _ => {
                let text = content_text(content);
                if !text.is_empty() {
                    builder.push_user(vec![json!({"type": "text", "text": text})]);
                }
            }
        }
    }
    let mut params = json!({
        "model": upstream_model,
        "messages": builder.finish(),
        "max_tokens": clamp_max_tokens(data.get("max_tokens"), 32000),
        "stream": true,
        "system": system_param(system_text(data.get("system"))),
    });
    if let Some(value) = data.get("temperature") {
        params["temperature"] = value.clone();
    }
    if let Some(value) = data.get("reasoning_effort").or_else(|| data.get("effort")) {
        params["reasoning_effort"] = value.clone();
    }
    if let Some(tools) = claude_tools_to_cc(data.get("tools")) {
        params["tools"] = tools;
    }
    if let Some(choice) = claude_tool_choice_to_cc(data.get("tool_choice")) {
        params["tool_choice"] = choice;
    }
    envelope(params)
}

/// OpenAI Chat Completions 请求 → CC `/alpha/generate` 请求体。
///
/// 与 [`claude_to_commandcode`] 共用 [`CcMessages`]（硬坑 1/2/3/7），差异在
/// 输入形状：`tool_calls[].function.arguments` 是 JSON 字符串、图片在
/// `image_url.url`、system 是消息而不是顶层字段。
pub(super) fn openai_chat_to_commandcode(upstream_model: &str, data: &Value) -> Value {
    let mut builder = CcMessages::default();
    let messages = data
        .get("messages")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    // 预扫描 assistant.tool_calls / role=tool（硬坑 3 的缺失结果判定）。
    for message in &messages {
        match message.get("role").and_then(Value::as_str).unwrap_or("user") {
            "assistant" => {
                if let Some(calls) = message.get("tool_calls").and_then(Value::as_array) {
                    for call in calls {
                        builder.note_call(
                            call.get("id").and_then(Value::as_str).unwrap_or(""),
                            call.pointer("/function/name")
                                .and_then(Value::as_str)
                                .unwrap_or(""),
                        );
                    }
                }
            }
            "tool" => {
                if let Some(id) = message.get("tool_call_id").and_then(Value::as_str) {
                    builder.note_result(id);
                }
            }
            _ => {}
        }
    }
    let mut system_parts: Vec<String> = Vec::new();
    let mut conversation_started = false;
    for message in &messages {
        let role = message.get("role").and_then(Value::as_str).unwrap_or("user");
        let content = message.get("content").unwrap_or(&Value::Null);
        match role {
            "system" | "developer" if !conversation_started => {
                let text = openai_content_text(content);
                if !text.trim().is_empty() {
                    system_parts.push(text);
                }
            }
            "assistant" => {
                conversation_started = true;
                let mut parts: Vec<Value> = Vec::new();
                let mut calls: Vec<(String, String)> = Vec::new();
                if let Some(reasoning) = message
                    .get("reasoning_content")
                    .or_else(|| message.get("reasoning"))
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                {
                    // 硬坑 2：思考历史先于 text/tool-call 回传。
                    parts.push(json!({"type": "reasoning", "text": reasoning}));
                }
                let text = openai_content_text(content);
                if !text.is_empty() {
                    parts.push(json!({"type": "text", "text": text}));
                }
                if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
                    for call in tool_calls {
                        let id = call
                            .get("id")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                            .unwrap_or_else(|| new_id("call"));
                        let name = call
                            .pointer("/function/name")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_owned();
                        let input = call
                            .pointer("/function/arguments")
                            .and_then(Value::as_str)
                            .map(|raw| super::request::parse_arguments(&json!(raw)))
                            .unwrap_or_else(|| json!({}));
                        builder.note_call(&id, &name);
                        parts.push(json!({
                            "type": "tool-call",
                            "toolCallId": id.clone(),
                            "toolName": name.clone(),
                            "input": input,
                        }));
                        calls.push((id, name));
                    }
                }
                builder.push_assistant(parts, calls);
            }
            "tool" => {
                conversation_started = true;
                let id = message
                    .get("tool_call_id")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let value = match content {
                    Value::String(text) => text.clone(),
                    other => openai_content_text(other),
                };
                if let Some(part) =
                    builder.tool_result_part(id, None, json!({"type": "text", "value": value}))
                {
                    builder.push_tool(vec![part]);
                }
            }
            // user，以及会话中途出现的 system/developer：降级为 user。
            _ => {
                conversation_started = true;
                let mut parts: Vec<Value> = Vec::new();
                if let Some(blocks) = content.as_array() {
                    for block in blocks {
                        match block.get("type").and_then(Value::as_str) {
                            Some("text" | "input_text" | "output_text") => {
                                if let Some(text) = block.get("text").and_then(Value::as_str) {
                                    parts.push(json!({"type": "text", "text": text}));
                                }
                            }
                            // 硬坑 6：图片放进 `image`（data URL 或原始 URL）。
                            Some("image_url") => {
                                let url = block
                                    .pointer("/image_url/url")
                                    .and_then(Value::as_str)
                                    .unwrap_or("");
                                if !url.is_empty() {
                                    parts.push(json!({"type": "image", "image": url}));
                                }
                            }
                            _ => {}
                        }
                    }
                } else {
                    let text = openai_content_text(content);
                    if !text.is_empty() {
                        parts.push(json!({"type": "text", "text": text}));
                    }
                }
                builder.push_user(parts);
            }
        }
    }
    let mut params = json!({
        "model": upstream_model,
        "messages": builder.finish(),
        "max_tokens": clamp_max_tokens(
            data.get("max_tokens").or_else(|| data.get("max_completion_tokens")),
            32000,
        ),
        "stream": true,
        "system": system_param((!system_parts.is_empty()).then(|| system_parts.join("\n"))),
    });
    if let Some(value) = data.get("temperature") {
        params["temperature"] = value.clone();
    }
    if let Some(value) = data.get("reasoning_effort").or_else(|| data.get("effort")) {
        params["reasoning_effort"] = value.clone();
    }
    if let Some(tools) = openai_tools_to_cc(data.get("tools")) {
        params["tools"] = tools;
    }
    if let Some(choice) = openai_tool_choice_to_cc(data.get("tool_choice")) {
        params["tool_choice"] = choice;
    }
    envelope(params)
}

/// OpenAI Responses 请求 → CC `/alpha/generate` 请求体。
pub(super) fn responses_to_commandcode(upstream_model: &str, data: &Value) -> Value {
    let mut builder = CcMessages::default();
    let input = data
        .get("input")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    // 预扫描：所有 function_call_output / function_call。
    for item in &input {
        match item.get("type").and_then(Value::as_str) {
            Some("function_call_output") => {
                if let Some(id) = item.get("call_id").and_then(Value::as_str) {
                    builder.note_result(id);
                }
            }
            Some("function_call") => {
                if let (Some(id), Some(name)) = (
                    item.get("call_id").and_then(Value::as_str),
                    item.get("name").and_then(Value::as_str),
                ) {
                    builder.note_call(id, name);
                }
            }
            _ => {}
        }
    }
    let mut pending_reasoning = String::new();
    for item in &input {
        match item.get("type").and_then(Value::as_str).unwrap_or("") {
            "message" => {
                let mut role = item.get("role").and_then(Value::as_str).unwrap_or("user");
                // 会话中途的 developer/system：降级为 user 而不是丢弃（计划 §2.6）。
                if matches!(role, "developer" | "system") {
                    role = "user";
                }
                let mut parts: Vec<Value> = Vec::new();
                match item.get("content").unwrap_or(&Value::Null) {
                    Value::String(text) => parts.push(json!({"type": "text", "text": text})),
                    Value::Array(blocks) => {
                        for block in blocks {
                            match block.get("type").and_then(Value::as_str) {
                                Some("input_text") => {
                                    if let Some(text) =
                                        block.get("text").and_then(Value::as_str)
                                    {
                                        parts.push(json!({"type": "text", "text": text}));
                                    }
                                }
                                Some("output_text") => {
                                    if let Some(text) =
                                        block.get("text").and_then(Value::as_str)
                                    {
                                        parts.push(json!({"type": "text", "text": text}));
                                    }
                                }
                                Some("input_image") => {
                                    if let Some(url) = block
                                        .get("image_url")
                                        .and_then(|value| {
                                            value.as_str().map(str::to_owned).or_else(|| {
                                                value
                                                    .get("url")
                                                    .and_then(Value::as_str)
                                                    .map(str::to_owned)
                                            })
                                        })
                                        .filter(|url| !url.is_empty())
                                    {
                                        parts.push(json!({"type": "image", "image": url}));
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    _ => {}
                }
                if role == "assistant" {
                    if !pending_reasoning.is_empty() {
                        parts.insert(0, json!({"type": "reasoning", "text": pending_reasoning}));
                        pending_reasoning.clear();
                    }
                    builder.push_assistant(parts, Vec::new());
                } else {
                    builder.push_user(parts);
                }
            }
            "function_call" => {
                let id = item
                    .get("call_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .unwrap_or_else(|| new_id("call"));
                let name = item
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned();
                let input = item
                    .get("arguments")
                    .and_then(Value::as_str)
                    .map(|raw| super::request::parse_arguments(&json!(raw)))
                    .unwrap_or_else(|| json!({}));
                builder.note_call(&id, &name);
                let mut parts = vec![json!({
                    "type": "tool-call",
                    "toolCallId": id.clone(),
                    "toolName": name.clone(),
                    "input": input,
                })];
                if !pending_reasoning.is_empty() {
                    parts.insert(0, json!({"type": "reasoning", "text": pending_reasoning}));
                    pending_reasoning.clear();
                }
                if !builder.append_assistant(parts.clone(), vec![(id.clone(), name.clone())]) {
                    builder.push_assistant(parts, vec![(id, name)]);
                }
            }
            "function_call_output" => {
                let id = item.get("call_id").and_then(Value::as_str).unwrap_or("");
                let output = item.get("output").unwrap_or(&Value::Null);
                let value = match output {
                    Value::String(text) => text.clone(),
                    other => super::request::responses_text(other)
                        .unwrap_or_else(|| other.to_string()),
                };
                if let Some(part) = builder.tool_result_part(
                    id,
                    None,
                    json!({"type": "text", "value": value}),
                ) {
                    builder.push_tool(vec![part]);
                }
            }
            "reasoning" => {
                let mut text = String::new();
                if let Some(summary) = item.get("summary").and_then(Value::as_array) {
                    for part in summary {
                        if let Some(value) = part.get("text").and_then(Value::as_str) {
                            text.push_str(value);
                        }
                    }
                }
                if let Some(content) = item.get("content").and_then(Value::as_array) {
                    for part in content {
                        if let Some(value) = part.get("text").and_then(Value::as_str) {
                            text.push_str(value);
                        }
                    }
                }
                if !text.is_empty() {
                    pending_reasoning.push_str(&text);
                }
            }
            _ => {}
        }
    }
    let mut params = json!({
        "model": upstream_model,
        "messages": builder.finish(),
        "max_tokens": clamp_max_tokens(data.get("max_output_tokens"), 32000),
        "stream": true,
        "system": system_param(
            data.get("instructions")
                .map(|value| text(value))
                .filter(|value| !value.trim().is_empty())
        ),
    });
    if let Some(value) = data.get("temperature") {
        params["temperature"] = value.clone();
    }
    if let Some(value) = data
        .pointer("/reasoning/effort")
        .or_else(|| data.get("reasoning_effort"))
    {
        params["reasoning_effort"] = value.clone();
    }
    if data
        .get("parallel_tool_calls")
        .and_then(Value::as_bool)
        .is_some()
    {
        params["parallel_tool_calls"] = data["parallel_tool_calls"].clone();
    }
    if let Some(tools) = responses_tools_to_cc(data.get("tools")) {
        params["tools"] = tools;
    }
    if let Some(choice) = responses_tool_choice_to_cc(data.get("tool_choice")) {
        params["tool_choice"] = choice;
    }
    envelope(params)
}

// ---------------------------------------------------------------------------
// usage
// ---------------------------------------------------------------------------

fn i64_field(value: &Value, key: &str) -> Option<i64> {
    value.get(key).and_then(Value::as_i64)
}

/// CC usage → OpenAI usage（总数语义），带 [`INPUT_INCLUDES_CACHE`] 标记。
pub fn cc_usage_to_openai(usage: &Value) -> Value {
    let input = i64_field(usage, "inputTokens").unwrap_or(0);
    let output = i64_field(usage, "outputTokens").unwrap_or(0);
    let details = usage.get("inputTokenDetails");
    let cached = i64_field(usage, "cachedInputTokens")
        .or_else(|| details.and_then(|value| i64_field(value, "cacheReadTokens")))
        .unwrap_or(0);
    let cache_write = details
        .and_then(|value| i64_field(value, "cacheWriteTokens"))
        .unwrap_or(0);
    json!({
        "prompt_tokens": input,
        "completion_tokens": output,
        "total_tokens": input + output,
        "prompt_tokens_details": {
            "cached_tokens": cached,
            "cache_write_tokens": cache_write,
        },
        INPUT_INCLUDES_CACHE: true,
    })
}

/// finishReason 词表 → OpenAI。
pub fn map_finish_reason(reason: &str) -> String {
    match reason {
        "tool-calls" => "tool_calls",
        "length" => "length",
        "stop" | "" => "stop",
        other => other,
    }
    .to_owned()
}

fn event_error_message(event: &Value) -> String {
    event
        .pointer("/error/message")
        .or_else(|| event.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("Command Code stream error")
        .to_owned()
}

// ---------------------------------------------------------------------------
// NDJSON → OpenAI SSE（流式）
// ---------------------------------------------------------------------------

/// 逐块消费 CC NDJSON，输出 canonical OpenAI chat SSE（含 `[DONE]`）。
///
/// - 未知事件类型忽略并计数（前向兼容）；
/// - `error` 事件输出 `data: {"error":...}`，**不**产生 finish_reason（硬坑 5）；
/// - `tool-call` 已带完整 input，单次给出完整 `arguments`（硬坑 10 的回退基础）。
pub struct CommandCodeDecoder {
    model: String,
    buffer: Vec<u8>,
    completion_id: String,
    created_at: i64,
    chunk_index: i64,
    tool_index: i64,
    finish_reason: Option<String>,
    usage: Option<Value>,
    upstream_error: Option<String>,
    saw_finish: bool,
    finished: bool,
    produced_content: bool,
    unknown_events: usize,
    /// Hard pit 10 fallback: `tool-input-delta` events are normally ignored
    /// because `tool-call` already carries the full input. If an upstream
    /// only emits increments, the accumulated JSON is used instead.
    pending_tool_inputs: HashMap<String, String>,
    pending_tool_names: HashMap<String, String>,
    emitted_tool_calls: HashSet<String>,
}

impl CommandCodeDecoder {
    pub fn new(model: &str) -> Self {
        Self {
            model: model.to_owned(),
            buffer: Vec::new(),
            completion_id: new_id("chatcmpl"),
            created_at: unix_timestamp(),
            chunk_index: 0,
            tool_index: 0,
            finish_reason: None,
            usage: None,
            upstream_error: None,
            saw_finish: false,
            finished: false,
            produced_content: false,
            unknown_events: 0,
            pending_tool_inputs: HashMap::new(),
            pending_tool_names: HashMap::new(),
            emitted_tool_calls: HashSet::new(),
        }
    }

    pub fn finished(&self) -> bool {
        self.finished
    }

    pub fn saw_finish(&self) -> bool {
        self.saw_finish
    }

    pub fn upstream_error(&self) -> Option<&str> {
        self.upstream_error.as_deref()
    }

    pub fn produced_content(&self) -> bool {
        self.produced_content
    }

    pub fn unknown_events(&self) -> usize {
        self.unknown_events
    }

    /// Feeds raw upstream bytes; returns OpenAI SSE bytes ready for the
    /// existing `(entry, openai_compatible)` converter.
    pub fn feed(&mut self, bytes: &[u8]) -> Vec<u8> {
        if self.finished || bytes.is_empty() {
            return Vec::new();
        }
        let has_newline = bytes.contains(&b'\n');
        self.buffer.extend_from_slice(bytes);
        // 与社区实现相同：只有新到数据含换行才切分（buffer 中永不残留
        // 换行），避免对增长中的超长单行反复做全量扫描 —— O(n²) → O(n)。
        if !has_newline {
            return Vec::new();
        }
        let mut out = Vec::new();
        let mut consumed = 0usize;
        while let Some(offset) = self.buffer[consumed..].iter().position(|byte| *byte == b'\n') {
            let end = consumed + offset;
            let line = self.buffer[consumed..end].to_vec();
            consumed = end + 1;
            self.handle_line(&line, &mut out);
        }
        self.buffer.drain(..consumed);
        out
    }

    /// Handles a trailing unterminated line at upstream EOF.
    pub fn flush(&mut self) -> Vec<u8> {
        if self.finished || self.buffer.is_empty() {
            return Vec::new();
        }
        let line = std::mem::take(&mut self.buffer);
        let mut out = Vec::new();
        self.handle_line(&line, &mut out);
        out
    }

    fn chunk(&self, delta: Value, finish_reason: Option<&str>, usage: Option<Value>) -> Vec<u8> {
        let mut payload = json!({
            "id": self.completion_id,
            "object": "chat.completion.chunk",
            "created": self.created_at,
            "model": self.model,
            "choices": [{
                "index": 0,
                "delta": delta,
                "finish_reason": finish_reason,
            }],
        });
        if let Some(usage) = usage {
            payload["usage"] = usage;
        }
        sse_data(&payload)
    }

    /// Emits one canonical OpenAI `tool_calls` delta for a complete tool call.
    fn emit_tool_call(&mut self, id: String, name: String, arguments: String, out: &mut Vec<u8>) {
        if !self.emitted_tool_calls.insert(id.clone()) {
            return;
        }
        let entry = json!({
            "index": self.tool_index,
            "id": id,
            "type": "function",
            "function": {"name": name, "arguments": arguments},
        });
        self.tool_index += 1;
        let delta = if self.chunk_index == 0 {
            json!({"role": "assistant", "tool_calls": [entry]})
        } else {
            json!({"tool_calls": [entry]})
        };
        self.chunk_index += 1;
        out.extend(self.chunk(delta, None, None));
    }

    fn handle_line(&mut self, line: &[u8], out: &mut Vec<u8>) {
        if self.finished {
            return;
        }
        let mut text = String::from_utf8_lossy(line).trim().to_owned();
        if text.is_empty() || text.starts_with(':') {
            return;
        }
        if let Some(rest) = text.strip_prefix("data:") {
            text = rest.trim().to_owned();
        }
        if text.is_empty() || text == "[DONE]" {
            return;
        }
        let Ok(event) = serde_json::from_str::<Value>(&text) else {
            return;
        };
        let Some(kind) = event.get("type").and_then(Value::as_str) else {
            return;
        };
        match kind {
            "start" | "start-step" | "text-start" | "reasoning-start" | "reasoning-end"
            | "provider-metadata" | "text-end" => {}
            "tool-input-start" => {
                // Hard pit 10 fallback bookkeeping (normally silent).
                if let Some(id) = event.get("toolCallId").and_then(Value::as_str) {
                    self.pending_tool_inputs.entry(id.to_owned()).or_default();
                    if let Some(name) = event.get("toolName").and_then(Value::as_str) {
                        self.pending_tool_names.insert(id.to_owned(), name.to_owned());
                    }
                }
            }
            "tool-input-delta" => {
                if let Some(id) = event.get("toolCallId").and_then(Value::as_str) {
                    let delta = event
                        .get("delta")
                        .or_else(|| event.get("input"))
                        .map(|value| match value {
                            Value::String(text) => text.clone(),
                            other => serde_json::to_string(other).unwrap_or_default(),
                        })
                        .unwrap_or_default();
                    self.pending_tool_inputs
                        .entry(id.to_owned())
                        .or_default()
                        .push_str(&delta);
                }
            }
            "tool-input-end" => {
                // Fallback: an upstream that ONLY streams increments ends the
                // tool input without a full `tool-call` event.
                let Some(id) = event
                    .get("toolCallId")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                else {
                    return;
                };
                let accumulated = self
                    .pending_tool_inputs
                    .remove(&id)
                    .unwrap_or_default();
                if !self.emitted_tool_calls.contains(&id) && !accumulated.is_empty() {
                    let name = event
                        .get("toolName")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                        .or_else(|| self.pending_tool_names.remove(&id))
                        .unwrap_or_default();
                    self.emit_tool_call(id, name, accumulated, out);
                }
            }
            "tool-error" => {}
            "text-delta" => {
                let text = event
                    .get("text")
                    .or_else(|| event.get("delta"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if text.is_empty() {
                    return;
                }
                self.produced_content = true;
                let delta = if self.chunk_index == 0 {
                    json!({"role": "assistant", "content": text})
                } else {
                    json!({"content": text})
                };
                self.chunk_index += 1;
                out.extend(self.chunk(delta, None, None));
            }
            "reasoning-delta" => {
                let text = event.get("text").and_then(Value::as_str).unwrap_or("");
                if text.is_empty() {
                    return;
                }
                self.produced_content = true;
                let delta = if self.chunk_index == 0 {
                    json!({"role": "assistant", "reasoning_content": text})
                } else {
                    json!({"reasoning_content": text})
                };
                self.chunk_index += 1;
                out.extend(self.chunk(delta, None, None));
            }
            "tool-call" => {
                let id = event
                    .get("toolCallId")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("call_{}_{}", unix_timestamp(), self.tool_index));
                let name = event
                    .get("toolName")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned();
                let inline = match event.get("input") {
                    Some(Value::String(raw)) if !raw.is_empty() && raw != "{}" => Some(raw.clone()),
                    Some(value) if !value.is_null() && value != &json!({}) => {
                        Some(serde_json::to_string(value).unwrap_or_else(|_| "{}".into()))
                    }
                    _ => None,
                };
                // Hard pit 10 fallback: prefer the accumulated increments
                // when the full-input event is empty.
                let arguments = inline
                    .or_else(|| {
                        self.pending_tool_inputs
                            .remove(&id)
                            .filter(|value| !value.is_empty())
                    })
                    .unwrap_or_else(|| "{}".into());
                self.pending_tool_names.remove(&id);
                self.produced_content = true;
                self.emit_tool_call(id, name, arguments, out);
            }
            "finish-step" => {
                if let Some(reason) = event.get("finishReason").and_then(Value::as_str) {
                    self.finish_reason = Some(map_finish_reason(reason));
                }
                if let Some(usage) = event.get("usage").filter(|value| value.is_object()) {
                    self.usage = Some(usage.clone());
                }
            }
            "finish" => {
                let reason = self.finish_reason.clone().unwrap_or_else(|| {
                    map_finish_reason(
                        event.get("finishReason").and_then(Value::as_str).unwrap_or("stop"),
                    )
                });
                let usage = event
                    .get("totalUsage")
                    .or_else(|| event.get("usage"))
                    .filter(|value| value.is_object())
                    .cloned()
                    .or_else(|| self.usage.clone())
                    .map(|value| cc_usage_to_openai(&value));
                out.extend(self.chunk(json!({}), Some(&reason), usage));
                out.extend_from_slice(b"data: [DONE]\n\n");
                self.finished = true;
                self.saw_finish = true;
            }
            "error" => {
                // 硬坑 5：错误不产生 finish_reason，交给流扫描器以 error 事件收尾。
                let message = event_error_message(&event);
                tracing::warn!(message, "command code stream error");
                self.upstream_error = Some(message.clone());
                out.extend(sse_data(&json!({"error": {"message": message}})));
            }
            other => {
                self.unknown_events += 1;
                tracing::warn!(event_type = other, "unknown command code event ignored");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// NDJSON → OpenAI chat completion（非流式）
// ---------------------------------------------------------------------------

/// Aggregates a full NDJSON body into one OpenAI chat completion. Returns
/// `Err(message)` when the stream carried an `error` event (the caller maps
/// it to an upstream failure instead of a fake success).
pub fn ndjson_to_chat_completion(model: &str, bytes: &[u8]) -> Result<Value, String> {
    let mut text = String::new();
    let mut reasoning = String::new();
    let mut tool_calls: Vec<Value> = Vec::new();
    let mut finish_reason = "stop".to_owned();
    let mut usage: Option<Value> = None;
    let mut upstream_error: Option<String> = None;
    let mut pending_inputs: HashMap<String, String> = HashMap::new();
    let mut pending_names: HashMap<String, String> = HashMap::new();
    let mut emitted_calls: HashSet<String> = HashSet::new();

    for line in bytes.split(|byte| *byte == b'\n') {
        let mut line = String::from_utf8_lossy(line).trim().to_owned();
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("data:") {
            line = rest.trim().to_owned();
        }
        if line.is_empty() || line == "[DONE]" {
            continue;
        }
        let Ok(event) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        match event.get("type").and_then(Value::as_str).unwrap_or("") {
            "text-delta" => {
                if let Some(value) = event
                    .get("text")
                    .or_else(|| event.get("delta"))
                    .and_then(Value::as_str)
                {
                    text.push_str(value);
                }
            }
            "reasoning-delta" => {
                if let Some(value) = event.get("text").and_then(Value::as_str) {
                    reasoning.push_str(value);
                }
            }
            "tool-input-start" => {
                if let Some(id) = event.get("toolCallId").and_then(Value::as_str) {
                    pending_inputs.entry(id.to_owned()).or_default();
                    if let Some(name) = event.get("toolName").and_then(Value::as_str) {
                        pending_names.insert(id.to_owned(), name.to_owned());
                    }
                }
            }
            "tool-input-delta" => {
                if let Some(id) = event.get("toolCallId").and_then(Value::as_str) {
                    let delta = event
                        .get("delta")
                        .or_else(|| event.get("input"))
                        .map(|value| match value {
                            Value::String(text) => text.clone(),
                            other => serde_json::to_string(other).unwrap_or_default(),
                        })
                        .unwrap_or_default();
                    pending_inputs.entry(id.to_owned()).or_default().push_str(&delta);
                }
            }
            "tool-input-end" => {
                if let Some(id) = event
                    .get("toolCallId")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    && !emitted_calls.contains(&id)
                {
                    let arguments = pending_inputs.remove(&id).unwrap_or_default();
                    if !arguments.is_empty() {
                        let name = event
                            .get("toolName")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                            .or_else(|| pending_names.remove(&id))
                            .unwrap_or_default();
                        emitted_calls.insert(id.clone());
                        tool_calls.push(json!({
                            "id": id,
                            "type": "function",
                            "function": {"name": name, "arguments": arguments},
                        }));
                    }
                }
            }
            "tool-call" => {
                let id = event
                    .get("toolCallId")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .unwrap_or_else(|| new_id("call"));
                let name = event
                    .get("toolName")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned();
                let inline = match event.get("input") {
                    Some(Value::String(raw)) if !raw.is_empty() && raw != "{}" => Some(raw.clone()),
                    Some(value) if !value.is_null() && value != &json!({}) => {
                        Some(serde_json::to_string(value).unwrap_or_else(|_| "{}".into()))
                    }
                    _ => None,
                };
                if !emitted_calls.insert(id.clone()) {
                    continue;
                }
                let arguments = inline
                    .or_else(|| pending_inputs.remove(&id))
                    .unwrap_or_else(|| "{}".into());
                pending_names.remove(&id);
                tool_calls.push(json!({
                    "id": id,
                    "type": "function",
                    "function": {"name": name, "arguments": arguments},
                }));
            }
            "finish-step" => {
                if let Some(reason) = event.get("finishReason").and_then(Value::as_str) {
                    finish_reason = map_finish_reason(reason);
                }
                if let Some(value) = event.get("usage").filter(|value| value.is_object()) {
                    usage = Some(value.clone());
                }
            }
            "finish" => {
                if let Some(reason) = event.get("finishReason").and_then(Value::as_str) {
                    finish_reason = map_finish_reason(reason);
                }
                if let Some(value) = event
                    .get("totalUsage")
                    .or_else(|| event.get("usage"))
                    .filter(|value| value.is_object())
                {
                    usage = Some(value.clone());
                }
            }
            "error" => {
                if upstream_error.is_none() {
                    upstream_error = Some(event_error_message(&event));
                }
            }
            _ => {}
        }
    }
    if let Some(message) = upstream_error {
        return Err(message);
    }
    let mut message = json!({
        "role": "assistant",
        "content": if text.is_empty() { Value::Null } else { json!(text) },
    });
    if !reasoning.is_empty() {
        message["reasoning_content"] = json!(reasoning);
    }
    if !tool_calls.is_empty() {
        message["tool_calls"] = Value::Array(tool_calls);
    }
    Ok(json!({
        "id": new_id("chatcmpl"),
        "object": "chat.completion",
        "created": unix_timestamp(),
        "model": model,
        "choices": [{"index": 0, "message": message, "finish_reason": finish_reason}],
        "usage": usage.map(|value| cc_usage_to_openai(&value)).unwrap_or(Value::Null),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assistant_message(body: &Value) -> Value {
        body.pointer("/params/messages")
            .and_then(Value::as_array)
            .and_then(|messages| {
                messages
                    .iter()
                    .find(|message| message.get("role").and_then(Value::as_str) == Some("assistant"))
            })
            .cloned()
            .unwrap()
    }

    #[test]
    fn claude_thinking_is_preserved_first_and_missing_result_synthesized() {
        let request = json!({
            "model": "claude-sonnet-4-5",
            "max_tokens": 64,
            "system": [{"type": "text", "text": "be brief"}],
            "messages": [
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": [
                    {"type": "thinking", "thinking": "plan"},
                    {"type": "text", "text": "calling"},
                    {"type": "tool_use", "id": "toolu_1", "name": "read", "input": {"path": "a"}}
                ]}
            ]
        });
        let body = claude_to_commandcode("deepseek/deepseek-v4-flash", &request);
        assert_eq!(body.pointer("/params/system").unwrap(), "be brief");
        let messages = body.pointer("/params/messages").unwrap().as_array().unwrap();
        assert_eq!(messages[0]["role"], "user");
        let parts = messages[1]["content"].as_array().unwrap();
        // 硬坑 1：reasoning 在最前，随后 text、tool-call。
        assert_eq!(parts[0]["type"], "reasoning");
        assert_eq!(parts[0]["text"], "plan");
        assert_eq!(parts[1]["type"], "text");
        assert_eq!(parts[2]["type"], "tool-call");
        assert_eq!(parts[2]["input"]["path"], "a");
        // 硬坑 3：缺失 tool result 合成 error-text。
        assert_eq!(messages[2]["role"], "tool");
        assert_eq!(messages[2]["content"][0]["type"], "tool-result");
        assert_eq!(messages[2]["content"][0]["output"]["type"], "error-text");
        assert_eq!(messages[2]["content"][0]["output"]["value"], MISSING_TOOL_RESULT);
    }

    #[test]
    fn claude_tool_result_is_reordered_before_user_text() {
        let request = json!({
            "messages": [
                {"role": "assistant", "content": [
                    {"type": "tool_use", "id": "toolu_9", "name": "read", "input": {}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "toolu_9", "content": "ok"},
                    {"type": "text", "text": "next"}
                ]}
            ]
        });
        let body = claude_to_commandcode("m", &request);
        let messages = body.pointer("/params/messages").unwrap().as_array().unwrap();
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0]["role"], "assistant");
        assert_eq!(messages[1]["role"], "tool");
        assert_eq!(messages[1]["content"][0]["toolCallId"], "toolu_9");
        assert_eq!(messages[2]["role"], "user");
        assert_eq!(messages[2]["content"][0]["text"], "next");
    }

    #[test]
    fn claude_image_and_empty_system_placeholder() {
        let request = json!({
            "messages": [{"role": "user", "content": [
                {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": "AAA"}}
            ]}]
        });
        let body = claude_to_commandcode("m", &request);
        assert_eq!(body.pointer("/params/system").unwrap(), " ");
        let part = &body.pointer("/params/messages/0/content/0").unwrap();
        assert_eq!(part["type"], "image");
        assert_eq!(part["image"], "data:image/jpeg;base64,AAA");
    }

    #[test]
    fn responses_developer_mid_conversation_degrades_to_user() {
        let request = json!({
            "instructions": "sys",
            "input": [
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hi"}]},
                {"type": "message", "role": "developer", "content": [{"type": "input_text", "text": "note"}]}
            ]
        });
        let body = responses_to_commandcode("m", &request);
        assert_eq!(body.pointer("/params/system").unwrap(), "sys");
        let messages = body.pointer("/params/messages").unwrap().as_array().unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[1]["role"], "user");
        assert_eq!(messages[1]["content"][0]["text"], "note");
    }

    #[test]
    fn responses_reasoning_attaches_to_function_call() {
        let request = json!({
            "input": [
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "go"}]},
                {"type": "reasoning", "summary": [{"type": "summary_text", "text": "thinking"}]},
                {"type": "function_call", "call_id": "call_1", "name": "read", "arguments": "{\"path\":\"a\"}"}
            ]
        });
        let body = responses_to_commandcode("m", &request);
        let parts = assistant_message(&body)["content"]
            .as_array()
            .unwrap()
            .clone();
        assert_eq!(parts[0]["type"], "reasoning");
        assert_eq!(parts[0]["text"], "thinking");
        assert_eq!(parts[1]["type"], "tool-call");
        // 缺失结果合成在 assistant 之后。
        let messages = body.pointer("/params/messages").unwrap().as_array().unwrap();
        assert_eq!(messages[2]["role"], "tool");
    }

    #[test]
    fn decoder_emits_openai_chunks_and_usage_with_cache_marker() {
        let mut decoder = CommandCodeDecoder::new("deepseek/deepseek-v4-flash");
        let fixture = concat!(
            "{\"type\":\"start\"}\n",
            "{\"type\":\"reasoning-delta\",\"text\":\"plan\"}\n",
            "{\"type\":\"text-delta\",\"text\":\"Hello\"}\n",
            "{\"type\":\"text-delta\",\"delta\":\" world\"}\n",
            "{\"type\":\"tool-call\",\"toolCallId\":\"call_1\",\"toolName\":\"read\",\"input\":{\"path\":\"a\"}}\n",
            "{\"type\":\"finish-step\",\"finishReason\":\"tool-calls\",\"usage\":{\"inputTokens\":120,\"outputTokens\":7,\"cachedInputTokens\":100,\"inputTokenDetails\":{\"noCacheTokens\":20,\"cacheReadTokens\":100}}}\n",
            "{\"type\":\"finish\",\"finishReason\":\"tool-calls\",\"totalUsage\":{\"inputTokens\":120,\"outputTokens\":7,\"cachedInputTokens\":100,\"inputTokenDetails\":{\"noCacheTokens\":20,\"cacheReadTokens\":100}}}\n"
        );
        let out = String::from_utf8(decoder.feed(fixture.as_bytes())).unwrap();
        assert!(decoder.finished());
        assert!(decoder.produced_content());
        assert!(out.contains("\"reasoning_content\":\"plan\""));
        assert!(out.contains("\"content\":\"Hello\""));
        assert!(out.contains("\"tool_calls\""));
        assert!(out.contains("\"finish_reason\":\"tool_calls\""));
        assert!(out.contains("\"prompt_tokens\":120"));
        assert!(out.contains("\"cached_tokens\":100"));
        assert!(out.contains(&format!("\"{INPUT_INCLUDES_CACHE}\":true")));
        assert!(out.trim_end().ends_with("data: [DONE]"));
    }

    #[test]
    fn reasoning_stream_becomes_one_thinking_block_before_text() {
        // 回归（真实观测）：CC 的每个 reasoning-delta 都曾新开一个
        // content_block，Claude 客户端会看到 N 段独立思考。整段思考必须
        // 复用同一个 block，并在 text block 开始前 stop（带 signature）。
        let mut decoder = CommandCodeDecoder::new("deepseek/deepseek-v4-flash");
        let openai = decoder.feed(concat!(
            "{\"type\":\"reasoning-delta\",\"text\":\"The \"}\n",
            "{\"type\":\"reasoning-delta\",\"text\":\"user\"}\n",
            "{\"type\":\"text-delta\",\"text\":\"OK\"}\n"
        )
        .as_bytes());
        let mut converter =
            MappedStreamConverter::new("claude", "openai_compatible", "claude-sonnet-4-5")
                .unwrap();
        let mut claude = converter.feed(&openai);
        claude.extend(converter.flush());
        let text = String::from_utf8(claude).unwrap();
        assert_eq!(
            text.matches("\"content_block\":{\"thinking\":\"").count(),
            1,
            "{text}"
        );
        assert_eq!(text.matches("\"type\":\"thinking_delta\"").count(), 2, "{text}");
        // thinking + text 两个 block。
        assert_eq!(text.matches("\"type\":\"content_block_start\"").count(), 2, "{text}");
        let stop = text.find("\"type\":\"content_block_stop\"").unwrap();
        let start_text = text.find("\"content_block\":{\"text\":\"").unwrap();
        assert!(stop < start_text, "thinking 必须先关闭再开 text block:\n{text}");
        // index 单调递增，text 不能复用已关闭的 thinking index(0)。
        assert!(
            text.contains("\"content_block\":{\"text\":\"\",\"type\":\"text\"},\"index\":1"),
            "text block 必须使用 index 1:\n{text}"
        );
    }

    #[test]
    fn openai_chat_converts_system_tools_results_and_images() {
        let request = json!({
            "model": "deepseek/deepseek-v4.1-flash",
            "max_tokens": 64,
            "stream": true,
            "reasoning_effort": "max",
            "messages": [
                {"role": "system", "content": "be brief"},
                {"role": "user", "content": [
                    {"type": "text", "text": "look"},
                    {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}}
                ]},
                {"role": "assistant", "reasoning_content": "plan", "content": "calling",
                 "tool_calls": [{"id": "call_1", "type": "function",
                   "function": {"name": "read", "arguments": "{\"path\":\"a\"}"}}]},
                {"role": "tool", "tool_call_id": "call_1", "content": "ok"},
                {"role": "user", "content": "next"}
            ],
            "tools": [{"type": "function", "function": {
                "name": "read", "description": "r", "parameters": {"type": "object"}
            }}],
            "tool_choice": {"type": "function", "function": {"name": "read"}}
        });
        let body = openai_chat_to_commandcode("deepseek/deepseek-v4.1-flash", &request);
        assert_eq!(body.pointer("/params/system").unwrap(), "be brief");
        assert_eq!(body.pointer("/params/reasoning_effort").unwrap(), "max");
        assert_eq!(body.pointer("/params/max_tokens").unwrap(), 64);
        let messages = body.pointer("/params/messages").unwrap().as_array().unwrap();
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[0]["role"], "user");
        assert_eq!(messages[0]["content"][0]["text"], "look");
        assert_eq!(messages[0]["content"][1]["type"], "image");
        assert_eq!(messages[0]["content"][1]["image"], "data:image/png;base64,AAAA");
        assert_eq!(messages[1]["role"], "assistant");
        let parts = messages[1]["content"].as_array().unwrap();
        assert_eq!(parts[0]["type"], "reasoning");
        assert_eq!(parts[0]["text"], "plan");
        assert_eq!(parts[1]["type"], "text");
        assert_eq!(parts[2]["type"], "tool-call");
        assert_eq!(parts[2]["toolCallId"], "call_1");
        assert_eq!(parts[2]["input"]["path"], "a");
        assert_eq!(messages[2]["role"], "tool");
        assert_eq!(messages[2]["content"][0]["toolCallId"], "call_1");
        assert_eq!(messages[2]["content"][0]["output"]["value"], "ok");
        assert_eq!(messages[3]["role"], "user");
        assert_eq!(messages[3]["content"][0]["text"], "next");
        assert_eq!(body.pointer("/params/tools/0/input_schema/type").unwrap(), "object");
        assert_eq!(body.pointer("/params/tool_choice/type").unwrap(), "tool");
        assert_eq!(body.pointer("/params/tool_choice/name").unwrap(), "read");
    }

    #[test]
    fn openai_chat_synthesizes_missing_tool_result_and_clamps_tokens() {
        let request = json!({
            "model": "m",
            "max_completion_tokens": 999_999,
            "messages": [
                {"role": "user", "content": "hi"},
                {"role": "developer", "content": "mid"},
                {"role": "assistant", "content": Value::Null, "tool_calls": [
                    {"id": "call_9", "function": {"name": "read", "arguments": "{}"}}
                ]}
            ]
        });
        let body = openai_chat_to_commandcode("m", &request);
        assert_eq!(body.pointer("/params/max_tokens").unwrap(), MAX_TOKENS_CAP);
        let messages = body.pointer("/params/messages").unwrap().as_array().unwrap();
        // user、会话中途 developer 降级 user、assistant、合成 tool。
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[1]["role"], "user");
        assert_eq!(messages[1]["content"][0]["text"], "mid");
        assert_eq!(messages[2]["role"], "assistant");
        assert_eq!(messages[3]["role"], "tool");
        assert_eq!(messages[3]["content"][0]["output"]["type"], "error-text");
        assert_eq!(messages[3]["content"][0]["output"]["value"], MISSING_TOOL_RESULT);
    }

    #[test]
    fn chat_passthrough_forwards_openai_sse_and_records_usage() {
        let mut converter =
            MappedStreamConverter::new("openai_compatible", "openai_compatible", "m").unwrap();
        let chunk = concat!(
            "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2,\"total_tokens\":7}}\n\n",
            "data: [DONE]\n\n"
        );
        let out = converter.feed(chunk.as_bytes());
        assert_eq!(out, chunk.as_bytes());
        let (input, output, cache_read, cache_write) = converter.usage();
        assert_eq!(input, Some(5));
        assert_eq!(output, Some(2));
        assert_eq!(cache_read, None);
        assert_eq!(cache_write, None);
    }

    #[test]
    fn decoder_error_event_never_emits_finish_reason() {
        let mut decoder = CommandCodeDecoder::new("m");
        let out = String::from_utf8(decoder.feed(
            b"{\"type\":\"text-delta\",\"text\":\"partial\"}\n{\"type\":\"error\",\"error\":{\"message\":\"boom\"}}\n",
        ))
        .unwrap();
        assert_eq!(decoder.upstream_error(), Some("boom"));
        assert!(!decoder.finished());
        assert!(!out.contains("\"finish_reason\":\""));
        assert!(out.contains("\"error\":{\"message\":\"boom\"}"));
    }

    #[test]
    fn decoder_ignores_unknown_events_and_tolerates_split_lines() {
        let mut decoder = CommandCodeDecoder::new("m");
        let first = decoder.feed(b"{\"type\":\"mys");
        assert!(first.is_empty());
        let second = decoder.feed(b"tery\",\"x\":1}\n{\"type\":\"text-delta\",\"text\":\"ok\"}\n");
        assert_eq!(decoder.unknown_events(), 1);
        assert!(String::from_utf8(second).unwrap().contains("ok"));
    }

    #[test]
    fn decoder_flush_handles_unterminated_tail() {
        let mut decoder = CommandCodeDecoder::new("m");
        assert!(decoder.feed(b"{\"type\":\"text-delta\",\"text\":\"tail\"}").is_empty());
        let out = String::from_utf8(decoder.flush()).unwrap();
        assert!(out.contains("tail"));
    }

    #[test]
    fn decoder_falls_back_to_incremental_tool_input() {
        // Hard pit 10 fallback: no full `tool-call` event, only increments.
        let fixture = concat!(
            "{\"type\":\"tool-input-start\",\"toolCallId\":\"c9\",\"toolName\":\"read\"}\n",
            "{\"type\":\"tool-input-delta\",\"toolCallId\":\"c9\",\"delta\":\"{\\\"a\\\":\"}\n",
            "{\"type\":\"tool-input-delta\",\"toolCallId\":\"c9\",\"delta\":\"1}\"}\n",
            "{\"type\":\"tool-input-end\",\"toolCallId\":\"c9\"}\n"
        );
        let mut decoder = CommandCodeDecoder::new("m");
        let out = String::from_utf8(decoder.feed(fixture.as_bytes())).unwrap();
        assert!(out.contains("\"name\":\"read\""), "{out}");
        assert!(
            out.contains("\"arguments\":\"{\\\"a\\\":1}\""),
            "{out}"
        );
    }

    #[test]
    fn non_stream_falls_back_to_incremental_tool_input() {
        let fixture = concat!(
            "{\"type\":\"tool-input-start\",\"toolCallId\":\"c9\",\"toolName\":\"read\"}\n",
            "{\"type\":\"tool-input-delta\",\"toolCallId\":\"c9\",\"delta\":\"{\\\"a\\\":\"}\n",
            "{\"type\":\"tool-input-delta\",\"toolCallId\":\"c9\",\"delta\":\"1}\"}\n",
            "{\"type\":\"tool-input-end\",\"toolCallId\":\"c9\"}\n",
            "{\"type\":\"finish\",\"finishReason\":\"tool-calls\"}\n"
        );
        let completion = ndjson_to_chat_completion("m", fixture.as_bytes()).unwrap();
        assert_eq!(
            completion
                .pointer("/choices/0/message/tool_calls/0/function/name")
                .unwrap(),
            "read"
        );
        assert_eq!(
            completion
                .pointer("/choices/0/message/tool_calls/0/function/arguments")
                .unwrap(),
            "{\"a\":1}"
        );
        assert_eq!(
            completion.pointer("/choices/0/finish_reason").unwrap(),
            "tool_calls"
        );
    }

    #[test]
    fn non_stream_aggregation_builds_completion_and_rejects_error() {
        let fixture = concat!(
            "{\"type\":\"text-delta\",\"text\":\"Hello\"}\n",
            "{\"type\":\"tool-call\",\"toolCallId\":\"c1\",\"toolName\":\"read\",\"input\":{\"a\":1}}\n",
            "{\"type\":\"finish\",\"finishReason\":\"tool-calls\",\"totalUsage\":{\"inputTokens\":10,\"outputTokens\":3,\"cachedInputTokens\":2,\"inputTokenDetails\":{\"noCacheTokens\":8,\"cacheReadTokens\":2}}}\n"
        );
        let completion = ndjson_to_chat_completion("m", fixture.as_bytes()).unwrap();
        assert_eq!(completion.pointer("/choices/0/message/content").unwrap(), "Hello");
        assert_eq!(
            completion.pointer("/choices/0/message/tool_calls/0/function/name").unwrap(),
            "read"
        );
        assert_eq!(completion.pointer("/usage/prompt_tokens").unwrap(), 10);
        let error = ndjson_to_chat_completion(
            "m",
            b"{\"type\":\"error\",\"error\":{\"message\":\"nope\"}}\n",
        )
        .unwrap_err();
        assert_eq!(error, "nope");
    }

    #[test]
    fn finish_reason_vocabulary_matches_plan() {
        assert_eq!(map_finish_reason("tool-calls"), "tool_calls");
        assert_eq!(map_finish_reason("length"), "length");
        assert_eq!(map_finish_reason("stop"), "stop");
        assert_eq!(map_finish_reason("weird"), "weird");
    }
    #[test]
    fn decoder_uses_finish_step_usage_when_finish_lacks_it() {
        let mut decoder = CommandCodeDecoder::new("m");
        let out = String::from_utf8(decoder.feed(
            b"{\"type\":\"text-delta\",\"text\":\"x\"}\n\
              {\"type\":\"finish-step\",\"finishReason\":\"length\",\"usage\":{\"inputTokens\":9,\"outputTokens\":1,\"cachedInputTokens\":4,\"inputTokenDetails\":{\"noCacheTokens\":5,\"cacheReadTokens\":4}}}\n\
              {\"type\":\"finish\",\"finishReason\":\"length\"}\n",
        ))
        .unwrap();
        assert!(out.contains("\"prompt_tokens\":9"), "{out}");
        assert!(out.contains("\"cached_tokens\":4"), "{out}");
        assert!(out.contains("\"finish_reason\":\"length\""), "{out}");
    }

    #[test]
    fn decoder_accepts_crlf_and_data_prefix_and_ignores_post_finish_events() {
        let mut decoder = CommandCodeDecoder::new("m");
        let out = String::from_utf8(decoder.feed(
            b"data: {\"type\":\"text-delta\",\"text\":\"hi\"}\r\n\
              \r\n\
              {\"type\":\"finish\",\"finishReason\":\"stop\"}\r\n\
              {\"type\":\"text-delta\",\"text\":\"late\"}\r\n",
        ))
        .unwrap();
        assert!(out.contains("hi"), "{out}");
        assert!(!out.contains("late"), "events after finish must be ignored");
        assert!(decoder.finished());
        // The terminal marker is emitted exactly once.
        assert_eq!(out.matches("data: [DONE]").count(), 1);
    }

    #[test]
    fn decoder_first_chunk_carries_the_assistant_role_once() {
        let mut decoder = CommandCodeDecoder::new("m");
        let out = String::from_utf8(decoder.feed(
            b"{\"type\":\"text-delta\",\"text\":\"a\"}\n{\"type\":\"text-delta\",\"text\":\"b\"}\n",
        ))
        .unwrap();
        assert_eq!(out.matches("\"role\":\"assistant\"").count(), 1, "{out}");
    }

    #[test]
    fn decoder_silent_event_types_do_not_count_as_unknown() {
        let mut decoder = CommandCodeDecoder::new("m");
        let _ = decoder.feed(
            b"{\"type\":\"start\"}\n\
              {\"type\":\"text-start\"}\n\
              {\"type\":\"reasoning-start\"}\n\
              {\"type\":\"reasoning-end\"}\n\
              {\"type\":\"provider-metadata\",\"x\":1}\n\
              {\"type\":\"tool-input-start\",\"toolCallId\":\"c\"}\n\
              {\"type\":\"tool-input-delta\",\"toolCallId\":\"c\",\"delta\":\"{}\"}\n\
              {\"type\":\"tool-input-end\",\"toolCallId\":\"c\"}\n\
              {\"type\":\"tool-error\",\"toolCallId\":\"c\"}\n\
              {\"type\":\"text-end\"}\n",
        );
        // `tool-input-end` may legitimately emit the fallback tool-call, so
        // only assert no event was classified as unknown.
        assert_eq!(decoder.unknown_events(), 0);
    }

    #[test]
    fn claude_tools_and_tool_choice_are_mapped() {
        let request = json!({
            "tools": [{
                "name": "read",
                "description": "read a file",
                "input_schema": {"type": "object", "properties": {"path": {"type": "string"}}}
            }],
            "tool_choice": {"type": "tool", "name": "read"},
            "messages": [{"role": "user", "content": "hi"}]
        });
        let body = claude_to_commandcode("m", &request);
        assert_eq!(body.pointer("/params/tools/0/name").unwrap(), "read");
        assert_eq!(
            body.pointer("/params/tools/0/input_schema/properties/path/type")
                .unwrap(),
            "string"
        );
        assert_eq!(body.pointer("/params/tool_choice/type").unwrap(), "tool");
        assert_eq!(body.pointer("/params/tool_choice/name").unwrap(), "read");
        // auto/any/none keep their Anthropic vocabulary.
        let body = claude_to_commandcode("m", &json!({"tool_choice": {"type": "any"}}));
        assert_eq!(body.pointer("/params/tool_choice/type").unwrap(), "any");
    }

    #[test]
    fn claude_unknown_roles_degrade_to_user() {
        let request = json!({
            "messages": [
                {"role": "developer", "content": "note"},
                {"role": "system", "content": "mid-conversation"}
            ]
        });
        let body = claude_to_commandcode("m", &request);
        let messages = body.pointer("/params/messages").unwrap().as_array().unwrap();
        assert_eq!(messages.len(), 2);
        assert!(messages.iter().all(|message| message["role"] == "user"));
    }

    #[test]
    fn responses_merges_consecutive_function_calls_into_one_turn() {
        let request = json!({
            "input": [
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "go"}]},
                {"type": "function_call", "call_id": "c1", "name": "read", "arguments": "{\"a\":1}"},
                {"type": "function_call", "call_id": "c2", "name": "write", "arguments": "{\"b\":2}"},
                {"type": "function_call_output", "call_id": "c1", "output": "ok-1"},
                {"type": "function_call_output", "call_id": "c2", "output": "ok-2"}
            ]
        });
        let body = responses_to_commandcode("m", &request);
        let messages = body.pointer("/params/messages").unwrap().as_array().unwrap();
        let assistants: Vec<&Value> = messages
            .iter()
            .filter(|message| message["role"] == "assistant")
            .collect();
        assert_eq!(assistants.len(), 1, "consecutive calls share one assistant turn");
        let calls: Vec<&Value> = assistants[0]["content"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|part| part["type"] == "tool-call")
            .collect();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0]["toolCallId"], "c1");
        assert_eq!(calls[1]["toolName"], "write");
        let tools: Vec<&Value> = messages
            .iter()
            .filter(|message| message["role"] == "tool")
            .collect();
        assert_eq!(tools.len(), 2, "each output is its own tool message");
    }

    #[test]
    fn responses_without_instructions_uses_space_placeholder() {
        let body = responses_to_commandcode("m", &json!({"input": []}));
        assert_eq!(body.pointer("/params/system").unwrap(), " ");
    }

    #[test]
    fn non_stream_aggregation_includes_reasoning_and_finish_step_usage() {
        let fixture = concat!(
            "{\"type\":\"reasoning-delta\",\"text\":\"think\"}\n",
            "{\"type\":\"text-delta\",\"text\":\"answer\"}\n",
            "{\"type\":\"finish-step\",\"finishReason\":\"stop\",\"usage\":{\"inputTokens\":7,\"outputTokens\":3,\"cachedInputTokens\":1,\"inputTokenDetails\":{\"noCacheTokens\":6,\"cacheReadTokens\":1}}}\n",
            "{\"type\":\"finish\",\"finishReason\":\"stop\"}\n"
        );
        let completion = ndjson_to_chat_completion("m", fixture.as_bytes()).unwrap();
        assert_eq!(
            completion
                .pointer("/choices/0/message/reasoning_content")
                .unwrap(),
            "think"
        );
        assert_eq!(
            completion.pointer("/choices/0/message/content").unwrap(),
            "answer"
        );
        assert_eq!(completion.pointer("/usage/prompt_tokens").unwrap(), 7);
        assert_eq!(
            completion
                .pointer("/usage/prompt_tokens_details/cached_tokens")
                .unwrap(),
            1
        );
    }

}
