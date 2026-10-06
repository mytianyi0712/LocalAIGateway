//! 协议注册表与边界助手：`ProtocolId` 枚举是受支持协议的权威标识，
//! 每个协议通过 [`ProtocolAdapter`] 提供入口解析、目录发现、健康探测、
//! 用量归一化与错误体形状；本模块同时集中协议字符串 ↔ 枚举的转换、
//! 上游 URL 拼装、出站/响应头过滤，以及 Command Code / OpenCode 的专有头注入。
//!
//! 边界：本模块不发起网络请求，也不读写数据库，只做纯函数式的策略与编码。
//! 关键不变量：字符串边界一律经 `ProtocolId::parse` 校验；`PROTOCOL_ORDER`
//! 与 `PROTOCOL_ORDER_SQL` 的字面量必须与 `ProtocolId::as_str` 保持同步。
use anyhow::{Context, Result, bail};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use serde_json::{Value, json};
use url::Url;

use crate::domain::Usage;

/// 一次健康探测请求：方法、上游路径与可选 JSON body。
/// 多数协议用一次极小的 POST 探测；Command Code 探测其只读的
/// `GET /alpha/whoami`（仅鉴权——不消耗 token，也不触发生成）。
#[derive(Debug, Clone)]
pub struct HealthProbe {
    pub method: http::Method,
    pub path: String,
    pub body: Option<Value>,
}

fn post_probe(path: String, body: Value) -> HealthProbe {
    HealthProbe {
        method: http::Method::POST,
        path,
        body: Some(body),
    }
}

/// 单个协议的行为包：鉴权、入口解析、目录发现、健康探测、用量观测
/// 与对外错误体形状。每个协议一份实现，经 [`ProtocolId::adapter`] 获取；
/// 新增协议需同时扩展枚举并提供适配器——该注册表取代了散落的字符串
/// `match` 分派。
///
/// 协议*转换*（`convert.rs` 中的 `(entry, upstream) -> strategy` 矩阵）
/// 有意保持独立，目前仍以字符串为键。
pub trait ProtocolAdapter {
    /// 从原始请求的入口路径中提取 model / stream。
    fn inspect_request(
        &self,
        path: &str,
        query: Option<&str>,
        body: &[u8],
    ) -> (Option<String>, bool);
    /// 上游模型目录路径。
    fn discovery_path(&self) -> &'static str;
    /// 解析一页目录；返回 (条目列表, 可选的下一页 URL)。
    fn parse_catalog(&self, body: &[u8], current_url: &Url) -> Result<(Vec<Value>, Option<Url>)>;
    /// 针对 `model` 的最小健康探测请求。
    fn health_probe(&self, model: &str) -> HealthProbe;
    /// 判断探测响应体是否算作健康。
    fn probe_body_ok(&self, body: &[u8]) -> bool;
    /// 流式事件或响应根节点内的原始用量对象。
    fn usage_value(&self, value: &Value) -> Option<Value>;
    /// 把原始用量对象归一化为网关的 Usage 模型。
    fn normalize_usage(&self, usage: &Value) -> Usage;
    /// 本协议对外暴露的网关错误体形状。
    fn error_shape(&self, status: StatusCode, code: &str, message: &str, request_id: &str)
    -> Value;
    /// 为单个候选重写出站 model id。返回（可能未变的）请求路径，以及
    /// 仅在 body 确实变化时才给出的新 body——`None` 表示“保留原始字节”。
    ///
    /// 仅当面向网关的 model 与候选上游 `channel_models.model_id` 不同时，
    /// 调用方才调用本方法，因此常规的逐字节透传永远不会重新序列化请求。
    fn retarget_model(&self, path: &str, body: &[u8], model: &str) -> (String, Option<Vec<u8>>) {
        (path.to_owned(), set_json_model(body, model))
    }
}

/// 设置 JSON 请求体顶层的 `model` 字段，保留其它所有字段。
/// 当 body 不是 JSON 对象时返回 `None`（调用方此时保留原始字节）。
fn set_json_model(body: &[u8], model: &str) -> Option<Vec<u8>> {
    let mut value: Value = serde_json::from_slice(body).ok()?;
    value.as_object_mut()?;
    value["model"] = json!(model);
    serde_json::to_vec(&value).ok()
}

/// 对 Gemini `/models/{model}` 路径段中需要保留的字符做百分号编码，
/// 与 `inspect_request` 中使用的 `percent_decode_str` 相对应。
fn encode_path_segment(model: &str) -> String {
    const UNRESERVED: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'.')
        .remove(b'_')
        .remove(b'~');
    percent_encoding::utf8_percent_encode(model, UNRESERVED).to_string()
}

/// 替换 `/…/models/{model}:action` 中的 `{model}` 段，保留 action 后缀
/// 与内联查询串。无法识别的路径形状原样返回。
fn rewrite_path_model(path: &str, model: &str) -> String {
    let Some(marker) = path.find("/models/") else {
        return path.to_owned();
    };
    let start = marker + "/models/".len();
    let rest = &path[start..];
    let end = rest.find([':', '/', '?']).unwrap_or(rest.len());
    format!("{}{}{}", &path[..start], encode_path_segment(model), &rest[end..])
}

/// OpenAI 系（chat completions 与 Responses）共用的行为。
fn openai_usage_value(value: &Value) -> Option<Value> {
    value.get("usage").cloned().or_else(|| {
        value
            .get("response")
            .and_then(|item| item.get("usage"))
            .cloned()
    })
}

fn openai_normalize_usage(usage: &Value) -> Usage {
    let details = usage
        .get("prompt_tokens_details")
        .or_else(|| usage.get("input_tokens_details"));
    let total_input = usage
        .get("prompt_tokens")
        .and_then(Value::as_i64)
        .or_else(|| usage.get("input_tokens").and_then(Value::as_i64));
    let cache_read = details
        .and_then(|item| item.get("cached_tokens"))
        .and_then(Value::as_i64);
    let cache_write = details
        .and_then(|item| {
            item.get("cache_write_tokens")
                .or_else(|| item.get("cached_write_tokens"))
        })
        .and_then(Value::as_i64);
    let miss = crate::domain::cache_miss_input(total_input, cache_read, cache_write);
    let output = usage
        .get("completion_tokens")
        .and_then(Value::as_i64)
        .or_else(|| usage.get("output_tokens").and_then(Value::as_i64));
    Usage {
        input_tokens: total_input,
        cache_read_tokens: cache_read,
        cache_write_tokens: cache_write,
        cache_miss_input_tokens: miss,
        output_tokens: output,
        raw: Some(usage.clone()),
    }
}

fn openai_error_shape(_status: StatusCode, code: &str, message: &str, request_id: &str) -> Value {
    json!({"error": {"message": message, "type": code, "code": code, "request_id": request_id}, "request_id": request_id})
}

fn openai_inspect(path: &str, query: Option<&str>, body: &[u8]) -> (Option<String>, bool) {
    let _ = (path, query);
    let value: Value = match serde_json::from_slice(body) {
        Ok(value) => value,
        Err(_) => return (None, false),
    };
    (
        value
            .get("model")
            .and_then(Value::as_str)
            .map(str::to_owned),
        value
            .get("stream")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    )
}

/// OpenAI chat/completions（也覆盖 /completions、/embeddings）。
pub struct OpenaiChatAdapter;

impl ProtocolAdapter for OpenaiChatAdapter {
    fn inspect_request(
        &self,
        _path: &str,
        _query: Option<&str>,
        body: &[u8],
    ) -> (Option<String>, bool) {
        openai_inspect(_path, _query, body)
    }
    fn discovery_path(&self) -> &'static str {
        "/v1/models"
    }
    fn parse_catalog(&self, body: &[u8], _current_url: &Url) -> Result<(Vec<Value>, Option<Url>)> {
        parse_openai_catalog(body)
    }
    fn health_probe(&self, model: &str) -> HealthProbe {
        post_probe(
            "/v1/chat/completions".to_owned(),
            json!({"model": model, "messages": [{"role": "user", "content": "Reply only OK"}], "max_tokens": 2}),
        )
    }
    fn probe_body_ok(&self, body: &[u8]) -> bool {
        let Ok(value) = serde_json::from_slice::<Value>(body) else {
            return false;
        };
        if !value.is_object() {
            return false;
        }
        value.pointer("/choices/0/finish_reason").is_some()
            || value
                .pointer("/choices/0/message/content")
                .is_some_and(|content| match content {
                    Value::String(text) => !text.is_empty(),
                    Value::Array(parts) => !parts.is_empty(),
                    _ => false,
                })
    }
    fn usage_value(&self, value: &Value) -> Option<Value> {
        openai_usage_value(value)
    }
    fn normalize_usage(&self, usage: &Value) -> Usage {
        openai_normalize_usage(usage)
    }
    fn error_shape(
        &self,
        status: StatusCode,
        code: &str,
        message: &str,
        request_id: &str,
    ) -> Value {
        openai_error_shape(status, code, message, request_id)
    }
}

/// OpenAI Responses API。
pub struct OpenaiResponsesAdapter;

impl ProtocolAdapter for OpenaiResponsesAdapter {
    fn inspect_request(
        &self,
        _path: &str,
        _query: Option<&str>,
        body: &[u8],
    ) -> (Option<String>, bool) {
        openai_inspect(_path, _query, body)
    }
    fn discovery_path(&self) -> &'static str {
        "/v1/models"
    }
    fn parse_catalog(&self, body: &[u8], _current_url: &Url) -> Result<(Vec<Value>, Option<Url>)> {
        parse_openai_catalog(body)
    }
    fn health_probe(&self, model: &str) -> HealthProbe {
        post_probe(
            "/v1/responses".to_owned(),
            json!({"model": model, "input": "Reply only OK", "max_output_tokens": 2}),
        )
    }
    fn probe_body_ok(&self, body: &[u8]) -> bool {
        let Ok(value) = serde_json::from_slice::<Value>(body) else {
            return false;
        };
        if !value.is_object() {
            return false;
        }
        value.get("status").and_then(Value::as_str) == Some("completed")
            || value.get("output").is_some()
            || value
                .get("output_text")
                .and_then(Value::as_str)
                .is_some_and(|text| !text.is_empty())
    }
    fn usage_value(&self, value: &Value) -> Option<Value> {
        openai_usage_value(value)
    }
    fn normalize_usage(&self, usage: &Value) -> Usage {
        openai_normalize_usage(usage)
    }
    fn error_shape(
        &self,
        status: StatusCode,
        code: &str,
        message: &str,
        request_id: &str,
    ) -> Value {
        openai_error_shape(status, code, message, request_id)
    }
}

/// Claude 原生协议（/v1/messages）。
pub struct ClaudeAdapter;

impl ProtocolAdapter for ClaudeAdapter {
    fn inspect_request(
        &self,
        _path: &str,
        _query: Option<&str>,
        body: &[u8],
    ) -> (Option<String>, bool) {
        let value: Value = match serde_json::from_slice(body) {
            Ok(value) => value,
            Err(_) => return (None, false),
        };
        (
            value
                .get("model")
                .and_then(Value::as_str)
                .map(str::to_owned),
            value
                .get("stream")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        )
    }
    fn discovery_path(&self) -> &'static str {
        "/v1/models"
    }
    fn parse_catalog(&self, body: &[u8], current_url: &Url) -> Result<(Vec<Value>, Option<Url>)> {
        let value: Value = serde_json::from_slice(body).context("模型目录 JSON 无效")?;
        let items = value
            .get("data")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let parsed = items
            .into_iter()
            .filter(|item| item.get("id").and_then(Value::as_str).is_some())
            .collect();
        // Claude 仅在响应显式给出 `has_more` 并带有 `last_id` 时才分页。
        let next = if value.get("has_more").and_then(Value::as_bool) == Some(true) {
            value.get("last_id").and_then(Value::as_str).map(|last_id| {
                let mut next = current_url.clone();
                next.query_pairs_mut().append_pair("after_id", last_id);
                next
            })
        } else {
            None
        };
        Ok((parsed, next))
    }
    fn health_probe(&self, model: &str) -> HealthProbe {
        post_probe(
            "/v1/messages".to_owned(),
            json!({"model": model, "max_tokens": 2, "messages": [{"role": "user", "content": "Reply only OK"}]}),
        )
    }
    fn probe_body_ok(&self, body: &[u8]) -> bool {
        let Ok(value) = serde_json::from_slice::<Value>(body) else {
            return false;
        };
        if !value.is_object() {
            return false;
        }
        value.get("stop_reason").is_some()
            || value
                .get("content")
                .and_then(Value::as_array)
                .is_some_and(|blocks| !blocks.is_empty())
    }
    fn usage_value(&self, value: &Value) -> Option<Value> {
        value
            .get("message")
            .and_then(|message| message.get("usage"))
            .cloned()
            .or_else(|| value.get("usage").cloned())
    }
    fn normalize_usage(&self, usage: &Value) -> Usage {
        let read = usage.get("cache_read_input_tokens").and_then(Value::as_i64);
        let write = usage
            .get("cache_creation_input_tokens")
            .and_then(Value::as_i64);
        let miss = usage.get("input_tokens").and_then(Value::as_i64);
        let total = if miss.is_some() || read.is_some() || write.is_some() {
            Some(miss.unwrap_or(0) + read.unwrap_or(0) + write.unwrap_or(0))
        } else {
            None
        };
        Usage {
            input_tokens: total,
            cache_read_tokens: read,
            cache_write_tokens: write,
            cache_miss_input_tokens: miss,
            output_tokens: usage.get("output_tokens").and_then(Value::as_i64),
            raw: Some(usage.clone()),
        }
    }
    fn error_shape(
        &self,
        _status: StatusCode,
        code: &str,
        message: &str,
        request_id: &str,
    ) -> Value {
        json!({"type": "error", "error": {"type": code, "message": message}, "request_id": request_id})
    }
}

/// Gemini 原生协议（/v1beta/models/...）。
pub struct GeminiAdapter;

impl ProtocolAdapter for GeminiAdapter {
    fn inspect_request(
        &self,
        path: &str,
        query: Option<&str>,
        _body: &[u8],
    ) -> (Option<String>, bool) {
        let model = path
            .split("/models/")
            .nth(1)
            .and_then(|value| value.split(':').next())
            .map(|value| {
                percent_encoding::percent_decode_str(value)
                    .decode_utf8_lossy()
                    .into_owned()
            });
        let stream = path.contains(":streamGenerateContent")
            || query.unwrap_or_default().contains("alt=sse");
        (model, stream)
    }
    /// Gemini 把 model 放在 URL 路径里，而非 body。
    fn retarget_model(&self, path: &str, _body: &[u8], model: &str) -> (String, Option<Vec<u8>>) {
        (rewrite_path_model(path, model), None)
    }
    fn discovery_path(&self) -> &'static str {
        "/v1beta/models"
    }
    fn parse_catalog(&self, body: &[u8], current_url: &Url) -> Result<(Vec<Value>, Option<Url>)> {
        let value: Value = serde_json::from_slice(body).context("模型目录 JSON 无效")?;
        let mut parsed: Vec<Value> = Vec::new();
        for mut item in value
            .get("models")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
        {
            let id = item
                .get("name")
                .and_then(Value::as_str)
                .and_then(|name| name.rsplit('/').next())
                .map(str::to_owned);
            match id {
                Some(id) => {
                    if let Some(object) = item.as_object_mut() {
                        object.insert("id".into(), Value::String(id));
                    }
                    parsed.push(item);
                }
                None => continue,
            }
        }
        // Gemini 通过 `nextPageToken` 续页。
        let next = value
            .get("nextPageToken")
            .and_then(Value::as_str)
            .map(|token| {
                let mut next = current_url.clone();
                next.query_pairs_mut().append_pair("pageToken", token);
                next
            });
        Ok((parsed, next))
    }
    fn health_probe(&self, model: &str) -> HealthProbe {
        post_probe(
            format!("/v1beta/models/{model}:generateContent"),
            json!({"contents": [{"parts": [{"text": "Reply only OK"}]}], "generationConfig": {"maxOutputTokens": 2}}),
        )
    }
    fn probe_body_ok(&self, body: &[u8]) -> bool {
        let Ok(value) = serde_json::from_slice::<Value>(body) else {
            return false;
        };
        if !value.is_object() {
            return false;
        }
        value.pointer("/candidates/0/finishReason").is_some()
            || value
                .pointer("/candidates/0/content/parts")
                .and_then(Value::as_array)
                .is_some_and(|parts| !parts.is_empty())
    }
    fn usage_value(&self, value: &Value) -> Option<Value> {
        value.get("usageMetadata").cloned()
    }
    fn normalize_usage(&self, usage: &Value) -> Usage {
        let total = usage.get("promptTokenCount").and_then(Value::as_i64);
        let cache = usage.get("cachedContentTokenCount").and_then(Value::as_i64);
        let miss = crate::domain::cache_miss_input(total, cache, None);
        Usage {
            input_tokens: total,
            cache_read_tokens: cache,
            cache_write_tokens: None,
            cache_miss_input_tokens: miss,
            output_tokens: usage.get("candidatesTokenCount").and_then(Value::as_i64),
            raw: Some(usage.clone()),
        }
    }
    fn error_shape(
        &self,
        status: StatusCode,
        code: &str,
        message: &str,
        request_id: &str,
    ) -> Value {
        json!({"error": {"code": status.as_u16(), "message": message, "status": code}, "request_id": request_id})
    }
}

/// Command Code CLI 上游：反向路径为 `/alpha/generate`（NDJSON），
/// 付费计划走官方 Provider API。它是仅上游协议——没有面向客户端的入口端点，
/// 因此 `endpoints()` 为空，模型目录也从不对外公布它。
pub struct CommandCodeAdapter;

impl ProtocolAdapter for CommandCodeAdapter {
    fn inspect_request(
        &self,
        _path: &str,
        _query: Option<&str>,
        body: &[u8],
    ) -> (Option<String>, bool) {
        let Ok(value) = serde_json::from_slice::<Value>(body) else {
            return (None, false);
        };
        // `/alpha/generate` 的 body 把字段嵌在 `params` 下；Provider API 的
        // body 则是普通 OpenAI chat（`model` / `stream`）。两者都接受，
        // 这样一个适配器即可服务传输路由。
        let model = value
            .pointer("/params/model")
            .or_else(|| value.get("model"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let stream = value
            .pointer("/params/stream")
            .or_else(|| value.get("stream"))
            .and_then(Value::as_bool)
            .unwrap_or(true);
        (model, stream)
    }
    fn retarget_model(&self, path: &str, body: &[u8], model: &str) -> (String, Option<Vec<u8>>) {
        let Ok(mut value) = serde_json::from_slice::<Value>(body) else {
            return (path.to_owned(), None);
        };
        let Some(object) = value.as_object_mut() else {
            return (path.to_owned(), None);
        };
        // `/alpha/generate` 把字段嵌在 `params` 下；Provider API 的 body
        // 则把 `model` 放在根层（与 `inspect_request` 一致）。
        if let Some(params) = object.get_mut("params").and_then(Value::as_object_mut) {
            params.insert("model".to_owned(), Value::String(model.to_owned()));
        } else {
            object.insert("model".to_owned(), Value::String(model.to_owned()));
        }
        (path.to_owned(), serde_json::to_vec(&value).ok())
    }
    /// 官方 Provider API 目录（OpenAI 形状）。
    fn discovery_path(&self) -> &'static str {
        "/provider/v1/models"
    }
    fn parse_catalog(&self, body: &[u8], _current_url: &Url) -> Result<(Vec<Value>, Option<Url>)> {
        parse_openai_catalog(body)
    }
    /// 只读账户探测：仅鉴权，不消耗 token，也不发起生成请求。
    fn health_probe(&self, _model: &str) -> HealthProbe {
        HealthProbe {
            method: http::Method::GET,
            path: "/alpha/whoami".to_owned(),
            body: None,
        }
    }
    fn probe_body_ok(&self, body: &[u8]) -> bool {
        let Ok(value) = serde_json::from_slice::<Value>(body) else {
            return false;
        };
        value.is_object()
            && (value.get("user").is_some()
                || value.get("org").is_some()
                || value.get("orgId").is_some())
    }
    fn usage_value(&self, value: &Value) -> Option<Value> {
        // CC 原始用量位于 `finish.totalUsage` / `finish-step.usage`；流式解码器
        // 会在转换前把它转成 OpenAI usage，但这里也接受原始形状，
        // 以便非流式场景可观测。
        value
            .get("totalUsage")
            .or_else(|| value.get("usage"))
            .cloned()
    }
    fn normalize_usage(&self, usage: &Value) -> Usage {
        commandcode_usage(usage)
    }
    fn error_shape(
        &self,
        status: StatusCode,
        code: &str,
        message: &str,
        request_id: &str,
    ) -> Value {
        openai_error_shape(status, code, message, request_id)
    }
}

/// 归一化 Command Code 的原始用量对象。`inputTokens` 是总量（含缓存命中）；
/// Anthropic/缓存计费需要非缓存部分（`inputTokenDetails.noCacheTokens`，
/// 或用减法得出）。
pub fn commandcode_usage(usage: &Value) -> Usage {
    let details = usage.get("inputTokenDetails");
    let total_input = usage.get("inputTokens").and_then(Value::as_i64);
    let cache_read = usage
        .get("cachedInputTokens")
        .and_then(Value::as_i64)
        .or_else(|| {
            details
                .and_then(|value| value.get("cacheReadTokens"))
                .and_then(Value::as_i64)
        });
    let cache_write = details
        .and_then(|value| value.get("cacheWriteTokens"))
        .and_then(Value::as_i64);
    let miss = details
        .and_then(|value| value.get("noCacheTokens"))
        .and_then(Value::as_i64)
        .or_else(|| {
            total_input.map(|total| {
                (total - cache_read.unwrap_or(0) - cache_write.unwrap_or(0)).max(0)
            })
        });
    Usage {
        input_tokens: total_input,
        cache_read_tokens: cache_read,
        cache_write_tokens: cache_write,
        cache_miss_input_tokens: miss,
        output_tokens: usage.get("outputTokens").and_then(Value::as_i64),
        raw: Some(usage.clone()),
    }
}

fn parse_openai_catalog(body: &[u8]) -> Result<(Vec<Value>, Option<Url>)> {
    let value: Value = serde_json::from_slice(body).context("模型目录 JSON 无效")?;
    let items = value
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let parsed = items
        .into_iter()
        .filter(|item| item.get("id").and_then(Value::as_str).is_some())
        .collect();
    Ok((parsed, None))
}

impl ProtocolId {
    /// 注册表：单个协议的行为包。
    pub fn adapter(self) -> &'static dyn ProtocolAdapter {
        match self {
            ProtocolId::OpenaiCompatible => &OpenaiChatAdapter,
            ProtocolId::OpenaiResponses => &OpenaiResponsesAdapter,
            ProtocolId::Claude => &ClaudeAdapter,
            ProtocolId::Gemini => &GeminiAdapter,
            ProtocolId::CommandCode => &CommandCodeAdapter,
        }
    }
}

/// 带类型的协议标识：该枚举是受支持协议列表、入口端点与发现路径的
/// 权威来源——新增协议只需扩展此类型，所有字符串边界
/// （校验、路由、目录、发现）都从它派生。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolId {
    OpenaiCompatible,
    OpenaiResponses,
    Claude,
    Gemini,
    /// 仅上游：Command Code CLI 反向路径 / 官方 Provider API。
    CommandCode,
}

impl ProtocolId {
    pub const ALL: [ProtocolId; 5] = [
        ProtocolId::OpenaiCompatible,
        ProtocolId::OpenaiResponses,
        ProtocolId::Claude,
        ProtocolId::Gemini,
        ProtocolId::CommandCode,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            ProtocolId::OpenaiCompatible => "openai_compatible",
            ProtocolId::OpenaiResponses => "openai_responses",
            ProtocolId::Claude => "claude",
            ProtocolId::Gemini => "gemini",
            ProtocolId::CommandCode => "command_code",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "openai_compatible" => Some(Self::OpenaiCompatible),
            "openai_responses" => Some(Self::OpenaiResponses),
            "claude" => Some(Self::Claude),
            "gemini" => Some(Self::Gemini),
            "command_code" => Some(Self::CommandCode),
            _ => None,
        }
    }

    /// 本协议对外（面向客户端）提供的入口端点。
    pub fn endpoints(self) -> &'static [&'static str] {
        match self {
            ProtocolId::OpenaiCompatible => {
                &["/v1/chat/completions", "/v1/completions", "/v1/embeddings"]
            }
            ProtocolId::OpenaiResponses => &["/v1/responses"],
            ProtocolId::Claude => &["/v1/messages"],
            ProtocolId::Gemini => &[
                "/v1beta/models/{model}:generateContent",
                "/v1beta/models/{model}:streamGenerateContent",
            ],
            // 仅上游：绝不作为客户端入口端点，只能由普通入口协议静默转换抵达。
            ProtocolId::CommandCode => &[],
        }
    }

    /// 本协议的上游模型目录路径（委托给适配器，使注册表保持唯一来源）。
    pub fn discovery_path(self) -> &'static str {
        self.adapter().discovery_path()
    }
}

/// 协议的「主路径」：目录公布与映射入口的上游改写共用同一张表（[`ProtocolId::endpoints`]）。
///
/// `stream` 只影响 gemini 的动词（`generateContent` / `streamGenerateContent`）；
/// 其它协议取端点清单的第一项。未知协议或没有上游入口的协议（`command_code`）
/// 返回 `None`，由调用方决定回退路径。
pub fn main_path(protocol: &str, model: &str, stream: bool) -> Option<String> {
    let id = ProtocolId::parse(protocol)?;
    let endpoints = id.endpoints();
    let template = match id {
        ProtocolId::Gemini if stream => *endpoints.get(1)?,
        _ => *endpoints.first()?,
    };
    Some(template.replace("{model}", model))
}

/// 受支持协议的有序字符串视图，供需要序列化该列表的调用点使用
/// （system status / protocols 端点）。这里的字面量只是 [`ProtocolId::as_str`] 的
/// 镜像，并非唯一来源——枚举才是新增协议的权威来源，字符串需手工同步。
pub const PROTOCOL_ORDER: [&str; 5] = [
    "openai_compatible",
    "openai_responses",
    "claude",
    "gemini",
    "command_code",
];

/// 目录查询按协议排序时使用的 `ORDER BY` 子句：数组顺序与 [`PROTOCOL_ORDER`] 一致；
/// `PROTOCOL_ORDER_SQL` 里未知协议与 `gemini` 同为 3，`command_code` 为 4 排在最后。
///
/// 只有在**列名正好是 `protocol`**（不带表限定符）的查询里才能直接插值；
/// `infrastructure` 的路由查询用到 `mr.protocol`，因此那里保留自己的字面量。
pub const PROTOCOL_ORDER_SQL: &str = "ORDER BY CASE protocol WHEN 'openai_compatible' THEN 0 WHEN 'openai_responses' THEN 1 WHEN 'claude' THEN 2 WHEN 'command_code' THEN 4 ELSE 3 END";

/// 单个渠道的模型级协议绑定：配置的协议集合，外加——对 Command Code 提供商
/// （`providers.kind = 'command_code'`）——转换层能以 `command_code` 服务的
/// 每个客户端入口协议。这类渠道接受它们全部，因为网关会在出站前转换请求体
/// （见 proxy 中逐候选的协议覆盖），所以路由候选池不得把它过滤掉。
///
/// 刻意**不**扩展渠道级 [`PROTOCOL_ORDER`] 绑定（`channel_protocols`，供
/// 健康/发现探测使用）：只有路由查询所连接的目录绑定会增长。
pub fn model_binding_protocols(provider_kind: Option<&str>, configured: &[String]) -> Vec<String> {
    let mut out: Vec<String> = configured.to_vec();
    if !requires_command_code_identity(provider_kind) {
        return out;
    }
    for entry in COMMAND_CODE_ENTRY_CANDIDATES {
        if converts_to_command_code(entry) && !out.iter().any(|bound| bound == entry) {
            out.push(entry.to_owned());
        }
    }
    out
}

/// 请求转换层能用 `command_code` 驱动的客户端入口协议。
///
/// 这里只是候选集合：所有调用点都会再用 [`converts_to_command_code`] 过滤一遍，
/// `convert::scan` 的注册表测试保证它与 [`crate::convert::ConversionStrategy`] 同步，
/// 因此新增转换方向不会被静默漏掉。
pub const COMMAND_CODE_ENTRY_CANDIDATES: [&str; 4] =
    ["openai_compatible", "openai_responses", "claude", "gemini"];

/// `entry` 是否存在指向 `command_code` 的请求转换。
/// 判定完全来自转换注册表（唯一事实来源），不额外维护白名单。
pub fn converts_to_command_code(entry: &str) -> bool {
    crate::convert::ConversionStrategy::for_pair(entry, "command_code").is_some()
}

const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
];

pub fn valid_protocol(value: &str) -> bool {
    ProtocolId::parse(value).is_some()
}

pub fn normalize_base_url(value: &str) -> Result<String> {
    let mut url =
        Url::parse(value.trim_end_matches('/')).context("base_url 必须是绝对 HTTP(S) URL")?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        bail!("base_url 必须是绝对 HTTP(S) URL");
    }
    let mut path = url.path().trim_end_matches('/').to_owned();
    for suffix in ["/v1beta", "/v1"] {
        if path.ends_with(suffix) {
            path.truncate(path.len() - suffix.len());
            break;
        }
    }
    url.set_path(&path);
    url.set_query(None);
    url.set_fragment(None);
    Ok(url.to_string().trim_end_matches('/').to_owned())
}

pub fn upstream_url(
    base_url: &str,
    path: &str,
    raw_query: Option<&str>,
    protocol: &str,
) -> Result<Url> {
    let mut base = Url::parse(base_url)?;
    // 路径里内联的查询串必须拆出来交给 `set_query`：`set_path` 会把 `?`
    // 百分号转义（/x?orgId=1 → /x%3ForgId=1），把请求打到错误路径上。
    let (path, inline_query) = match path.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (path, None),
    };
    let raw_query = match (raw_query.filter(|query| !query.is_empty()), inline_query) {
        (Some(raw), Some(inline)) if !inline.is_empty() => Some(format!("{raw}&{inline}")),
        (Some(raw), _) => Some(raw.to_owned()),
        (None, Some(inline)) if !inline.is_empty() => Some(inline.to_owned()),
        (None, _) => None,
    };
    let base_path = base.path().trim_end_matches('/');
    let suffix = format!("/{}", path.trim_start_matches('/'));
    let joined = if !base_path.is_empty()
        && (suffix == base_path || suffix.starts_with(&format!("{base_path}/")))
    {
        suffix
    } else {
        format!("{base_path}{suffix}")
    };
    base.set_path(&joined);
    base.set_query(raw_query.as_deref());
    if protocol == "gemini" {
        let retained = base
            .query_pairs()
            .filter(|(key, _)| key != "key")
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect::<Vec<_>>();
        base.set_query(None);
        if !retained.is_empty() {
            base.query_pairs_mut().extend_pairs(retained);
        }
    }
    Ok(base)
}

/// 构造 `Authorization: Bearer <token>` 头。
///
/// 上游凭据来自运行时输入，含非法头字符时返回错误而不是 panic；
/// 各调用方自行把错误映射到本地错误类型（代理路径是 `anyhow`，
/// 余额/登录探测各有自己的失败分类）。
pub fn bearer_header(token: &str) -> Result<HeaderValue, http::header::InvalidHeaderValue> {
    HeaderValue::from_str(&format!("Bearer {token}"))
}

pub fn outbound_headers(inbound: &HeaderMap, protocol: &str, api_key: &str) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    for (name, value) in inbound {
        let key = name.as_str();
        if HOP_BY_HOP.contains(&key)
            || matches!(
                key,
                "authorization" | "x-api-key" | "x-goog-api-key" | "x-local-gateway-key"
            )
        {
            continue;
        }
        headers.append(name.clone(), value.clone());
    }
    match protocol {
        "openai_compatible" | "openai_responses" => {
            headers.insert(axum::http::header::AUTHORIZATION, bearer_header(api_key)?);
        }
        "claude" => {
            headers.insert(
                HeaderName::from_static("x-api-key"),
                HeaderValue::from_str(api_key)?,
            );
            headers
                .entry(HeaderName::from_static("anthropic-version"))
                .or_insert(HeaderValue::from_static("2023-06-01"));
        }
        "gemini" => {
            headers.insert(
                HeaderName::from_static("x-goog-api-key"),
                HeaderValue::from_str(api_key)?,
            );
        }
        // Command Code 的 key 是 `user_...` 形式的 Bearer token。身份头
        // （session/fingerprint/version）由 `apply_command_code_identity` 单独注入，
        // 且仅对 `kind='command_code'` 的提供商注入。
        "command_code" => {
            headers.insert(axum::http::header::AUTHORIZATION, bearer_header(api_key)?);
        }
        _ => bail!("不支持的协议 {protocol}"),
    }
    Ok(headers)
}

/// OpenCode Zen/Go 自 2026-09 起要求每个请求都携带的 session 头
/// （缺失会让上游在路由前就拒绝请求）。
pub const OPENCODE_SESSION_HEADER: &str = "x-opencode-session";

/// 判断 `base_url` 是否指向 OpenCode Zen/Go 上游。Zen 通过其规范主机名
/// （`opencode.ai`）或 `zen` 路径段（`/zen/go`、`/zen/v1`）来识别，
/// 因此保留规范路径的中转也能被覆盖。
pub fn requires_opencode_session(base_url: &str) -> bool {
    let Ok(url) = Url::parse(base_url) else {
        return false;
    };
    let host_matches = url.host_str().is_some_and(|host| {
        let host = host.to_ascii_lowercase();
        host == "opencode.ai" || host.ends_with(".opencode.ai")
    });
    let path_matches = url
        .path()
        .split('/')
        .any(|segment| segment.eq_ignore_ascii_case("zen"));
    host_matches || path_matches
}

/// 确保 OpenCode 上游请求带有 [`OPENCODE_SESSION_HEADER`]。客户端转发的非空值
/// 始终优先（重复值折叠为一个）；只有在客户端未提供可用值时，
/// 才使用网关的稳定兜底值。
pub fn apply_opencode_session(
    headers: &mut HeaderMap,
    base_url: &str,
    session_id: &str,
) -> Result<()> {
    if session_id.is_empty() || !requires_opencode_session(base_url) {
        return Ok(());
    }
    let name = HeaderName::from_static(OPENCODE_SESSION_HEADER);
    let supplied: Option<String> = headers
        .get_all(&name)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .map(|value| value.trim().to_owned())
        .find(|value| !value.is_empty());
    let value = match supplied {
        Some(value) => value,
        None => session_id.to_owned(),
    };
    headers.insert(name, HeaderValue::from_str(&value)?);
    Ok(())
}

/// Command Code CLI 在每个请求上发送的头部。仅对 `kind = 'command_code'` 的
/// 提供商注入——绝不嗅探 base_url，因此自建中转永远不会意外收到指纹。
pub struct CommandCodeIdentity {
    pub cli_version: String,
    pub session_id: String,
    pub project_slug: String,
    /// ZDR 选用开关（`x-cmd-zdr: 1`）。
    pub zdr: bool,
}

/// 判断某提供商行是否选用 Command Code 身份头。
pub fn requires_command_code_identity(provider_kind: Option<&str>) -> bool {
    provider_kind == Some("command_code")
}

/// 应用 CLI 身份头。调用方已通过 [`outbound_headers`] 设置好 `Authorization`。
pub fn apply_command_code_identity(
    headers: &mut HeaderMap,
    identity: &CommandCodeIdentity,
) -> Result<()> {
    headers.insert(
        HeaderName::from_static("x-cli-environment"),
        HeaderValue::from_static("production"),
    );
    headers.insert(
        HeaderName::from_static("x-command-code-version"),
        HeaderValue::from_str(&identity.cli_version)?,
    );
    if !identity.session_id.is_empty() {
        headers.insert(
            HeaderName::from_static("x-session-id"),
            HeaderValue::from_str(&identity.session_id)?,
        );
    }
    if !identity.project_slug.is_empty() {
        headers.insert(
            HeaderName::from_static("x-project-slug"),
            HeaderValue::from_str(&identity.project_slug)?,
        );
    }
    headers.insert(
        HeaderName::from_static("x-co-flag"),
        HeaderValue::from_static("false"),
    );
    headers.insert(
        HeaderName::from_static("x-taste-learning"),
        HeaderValue::from_static("false"),
    );
    headers.insert(
        HeaderName::from_static("traceparent"),
        HeaderValue::from_str(&generate_traceparent())?,
    );
    if identity.zdr {
        headers.insert(HeaderName::from_static("x-cmd-zdr"), HeaderValue::from_static("1"));
    }
    Ok(())
}

/// 每个请求一个 W3C trace context：`00-<32hex trace-id>-<16hex span>-01`。
pub fn generate_traceparent() -> String {
    let trace = uuid::Uuid::new_v4().simple().to_string();
    let span = &uuid::Uuid::new_v4().simple().to_string()[..16];
    format!("00-{trace}-{span}-01")
}

/// 由 session id 确定性派生的 `x-project-slug`：十六进制的 session id 会
/// 解析出一个索引，其它情况回退到稳定的字符哈希。绝不泄露原始 session id。
pub fn derive_project_slug(session_id: &str) -> String {
    let hex_index = session_id
        .trim_start_matches("sess_")
        .chars()
        .take(8)
        .all(|value| value.is_ascii_hexdigit())
        .then(|| {
            u64::from_str_radix(&session_id.trim_start_matches("sess_")[..8], 16).unwrap_or(0)
        });
    let slug = match hex_index {
        Some(index) => {
            const WORDS: [&str; 8] = ["sable", "ember", "quartz", "willow", "harbor", "cypress", "onyx", "delta"];
            format!("{}-{}", WORDS[(index as usize) % WORDS.len()], index % 9973)
        }
        None => {
            // 对 session id 做 FNV-1a：稳定、廉价、不可逆。
            let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
            for byte in session_id.as_bytes() {
                hash ^= u64::from(*byte);
                hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
            }
            format!("ws-{:x}", hash & 0xffff_ffff)
        }
    };
    slug
}

pub fn response_headers(inbound: &reqwest::header::HeaderMap) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in inbound {
        if HOP_BY_HOP.contains(&name.as_str()) {
            continue;
        }
        headers.append(name.clone(), value.clone());
    }
    headers
}

pub fn inspect_request(
    protocol: &str,
    path: &str,
    query: Option<&str>,
    body: &[u8],
) -> (Option<String>, bool) {
    // 注册表分派：每个协议——包括嵌套的 Command Code body
    // （`params.model` / `params.stream`）——都由各自的适配器解析，
    // 而不是内联的 OpenAI 形状 match。
    ProtocolId::parse(protocol)
        .map(|id| id.adapter().inspect_request(path, query, body))
        .unwrap_or((None, false))
}

pub fn discovery_path(protocol: &str) -> &'static str {
    ProtocolId::parse(protocol)
        .map(|id| id.adapter().discovery_path())
        .unwrap_or("/v1/models")
}

/// 逐候选的上游 model 重写（自定义模型）。body 为 `None` 表示 body 未变，
/// 调用方保留原始字节。
pub fn retarget_model(
    protocol: &str,
    path: &str,
    body: &[u8],
    model: &str,
) -> (String, Option<Vec<u8>>) {
    ProtocolId::parse(protocol)
        .map(|id| id.adapter().retarget_model(path, body, model))
        .unwrap_or_else(|| (path.to_owned(), None))
}

/// 经协议适配器解析目录。
pub fn parse_catalog(
    protocol: &str,
    body: &[u8],
    current_url: &Url,
) -> Result<(Vec<Value>, Option<Url>)> {
    let Some(id) = ProtocolId::parse(protocol) else {
        bail!("不支持的协议 {protocol}");
    };
    id.adapter().parse_catalog(body, current_url)
}

/// 经协议适配器构造健康探测请求；未知协议返回 `None`。
pub fn health_probe(protocol: &str, model: &str) -> Option<HealthProbe> {
    ProtocolId::parse(protocol).map(|id| id.adapter().health_probe(model))
}

/// 经协议适配器判定健康探测结果。
pub fn probe_body_ok(protocol: &str, body: &[u8]) -> bool {
    ProtocolId::parse(protocol)
        .map(|id| id.adapter().probe_body_ok(body))
        .unwrap_or(false)
}

/// 经协议适配器取原始用量对象。
pub fn usage_value(protocol: &str, value: &Value) -> Option<Value> {
    ProtocolId::parse(protocol).and_then(|id| id.adapter().usage_value(value))
}

/// 经协议适配器归一化用量。
pub fn normalize_usage(protocol: &str, usage: &Value) -> Usage {
    ProtocolId::parse(protocol)
        .map(|id| id.adapter().normalize_usage(usage))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// cache-miss 只有一个来源：适配器归一化与 [`crate::domain::cache_miss_input`]
    /// 在同一输入上必须给出同一个 `cache_miss_input_tokens`。
    #[test]
    fn cache_miss_input_matches_adapter_normalization() {
        let openai = serde_json::json!({
            "prompt_tokens": 1000,
            "completion_tokens": 10,
            "prompt_tokens_details": {"cached_tokens": 400, "cache_write_tokens": 100},
        });
        let normalized = normalize_usage("openai_compatible", &openai);
        assert_eq!(
            normalized.cache_miss_input_tokens,
            crate::domain::cache_miss_input(
                normalized.input_tokens,
                normalized.cache_read_tokens,
                normalized.cache_write_tokens,
            )
        );
        assert_eq!(normalized.cache_miss_input_tokens, Some(500));

        let gemini = serde_json::json!({
            "promptTokenCount": 900,
            "cachedContentTokenCount": 300,
            "candidatesTokenCount": 7,
        });
        let normalized = normalize_usage("gemini", &gemini);
        assert_eq!(
            normalized.cache_miss_input_tokens,
            crate::domain::cache_miss_input(
                normalized.input_tokens,
                normalized.cache_read_tokens,
                normalized.cache_write_tokens,
            )
        );
        assert_eq!(normalized.cache_miss_input_tokens, Some(600));

        // 缺缓存读数：miss 未知（None），而不是把总量当成 miss。
        let without_cache = serde_json::json!({"prompt_tokens": 42});
        assert_eq!(
            normalize_usage("openai_compatible", &without_cache).cache_miss_input_tokens,
            None
        );
        // 缓存读超过总量：钳到 0。
        assert_eq!(
            crate::domain::cache_miss_input(Some(10), Some(25), None),
            Some(0)
        );
    }

    /// 解析注册表同时理解 Command Code 的嵌套 body 与扁平的 Provider API body，
    /// 因此两种传输的路由 model / stream 解析都能工作。
    #[test]
    fn command_code_inspect_reads_nested_and_flat_bodies() {
        let (model, stream) = inspect_request(
            "command_code",
            "/alpha/generate",
            None,
            br#"{"params":{"model":"deepseek/deepseek-v4-flash","stream":true}}"#,
        );
        assert_eq!(model.as_deref(), Some("deepseek/deepseek-v4-flash"));
        assert!(stream);
        let (model, stream) = inspect_request(
            "command_code",
            "/provider/v1/chat/completions",
            None,
            br#"{"model":"cc-model","stream":false}"#,
        );
        assert_eq!(model.as_deref(), Some("cc-model"));
        assert!(!stream);
        // Command Code 不对外公布任何客户端入口端点。
        assert!(ProtocolId::CommandCode.endpoints().is_empty());
        assert_eq!(
            ProtocolId::CommandCode.discovery_path(),
            "/provider/v1/models"
        );
    }

    /// 逐候选 model 重写：body 携带 model 的协议只改 `model`，gemini 改路径段，
    /// command code 则改 `params.model`。
    #[test]
    fn retarget_model_rewrites_body_per_protocol() {
        let body = br#"{"model":"my-gpt","messages":[],"temperature":0.5}"#;
        let (path, rewritten) = retarget_model("openai_compatible", "/v1/chat/completions", body, "gpt-4o");
        assert_eq!(path, "/v1/chat/completions");
        let value: Value = serde_json::from_slice(rewritten.as_deref().unwrap()).unwrap();
        assert_eq!(value["model"], json!("gpt-4o"));
        assert_eq!(value["temperature"], json!(0.5));

        let (_, rewritten) = retarget_model("claude", "/v1/messages", body, "claude-sonnet-5");
        let value: Value = serde_json::from_slice(rewritten.as_deref().unwrap()).unwrap();
        assert_eq!(value["model"], json!("claude-sonnet-5"));

        let (_, rewritten) = retarget_model("openai_responses", "/v1/responses", body, "gpt-5");
        let value: Value = serde_json::from_slice(rewritten.as_deref().unwrap()).unwrap();
        assert_eq!(value["model"], json!("gpt-5"));
    }

    #[test]
    fn retarget_model_handles_command_code_nested_and_flat() {
        let (_, rewritten) = retarget_model(
            "command_code",
            "/alpha/generate",
            br#"{"params":{"model":"old","stream":true}}"#,
            "deepseek/real",
        );
        let value: Value = serde_json::from_slice(rewritten.as_deref().unwrap()).unwrap();
        assert_eq!(value["params"]["model"], json!("deepseek/real"));
        assert_eq!(value["params"]["stream"], json!(true));

        let (_, rewritten) = retarget_model(
            "command_code",
            "/provider/v1/chat/completions",
            br#"{"model":"old"}"#,
            "real",
        );
        let value: Value = serde_json::from_slice(rewritten.as_deref().unwrap()).unwrap();
        assert_eq!(value["model"], json!("real"));
    }

    #[test]
    fn retarget_model_rewrites_gemini_path_only() {
        let body = br#"{"contents":[]}"#;
        let (path, rewritten) = retarget_model(
            "gemini",
            "/v1beta/models/my-gpt:generateContent",
            body,
            "gemini-2.5-pro",
        );
        assert_eq!(path, "/v1beta/models/gemini-2.5-pro:generateContent");
        assert!(rewritten.is_none(), "gemini carries the model in the path");

        let (path, _) = retarget_model(
            "gemini",
            "/v1beta/models/my-gpt:streamGenerateContent?alt=sse",
            body,
            "gemini-2.5-pro",
        );
        assert_eq!(
            path,
            "/v1beta/models/gemini-2.5-pro:streamGenerateContent?alt=sse"
        );

        let (path, _) = retarget_model(
            "gemini",
            "/v1beta/models/models%2Fgemini-pro:generateContent",
            body,
            "a b/c",
        );
        assert_eq!(path, "/v1beta/models/a%20b%2Fc:generateContent");
    }

    #[test]
    fn retarget_model_leaves_non_object_bodies_untouched() {
        // 非 JSON / 非对象 body 保留原始字节（逐字节透传路径绝不能被破坏）。
        let (path, rewritten) = retarget_model("openai_compatible", "/v1/chat/completions", b"not json", "x");
        assert_eq!(path, "/v1/chat/completions");
        assert!(rewritten.is_none());
        let (_, rewritten) = retarget_model("openai_compatible", "/v1/chat/completions", b"[1,2]", "x");
        assert!(rewritten.is_none());
        // 未知协议绝不改动请求。
        let (path, rewritten) = retarget_model("bogus", "/x", br#"{"model":"a"}"#, "b");
        assert_eq!(path, "/x");
        assert!(rewritten.is_none());
    }

    /// 身份头完整、符合 W3C 形状且按 kind 门禁：`kind='command_code'` 之外的
    /// 提供商永远不会触发它们。
    #[test]
    fn command_code_identity_headers_are_complete_and_kind_gated() {
        assert!(!requires_command_code_identity(None));
        assert!(!requires_command_code_identity(Some("other")));
        assert!(!requires_command_code_identity(Some("")));
        assert!(requires_command_code_identity(Some("command_code")));

        let mut headers = HeaderMap::new();
        apply_command_code_identity(
            &mut headers,
            &CommandCodeIdentity {
                cli_version: "1.53.1".into(),
                session_id: "sess_abc".into(),
                project_slug: "sable-1".into(),
                zdr: true,
            },
        )
        .unwrap();
        for name in [
            "x-cli-environment",
            "x-command-code-version",
            "x-session-id",
            "x-project-slug",
            "x-co-flag",
            "x-taste-learning",
            "traceparent",
            "x-cmd-zdr",
        ] {
            assert!(headers.contains_key(name), "{name} missing");
        }
        assert_eq!(headers["x-cli-environment"], "production");
        assert_eq!(headers["x-command-code-version"], "1.53.1");
        assert_eq!(headers["x-session-id"], "sess_abc");
        assert_eq!(headers["x-project-slug"], "sable-1");
        assert_eq!(headers["x-co-flag"], "false");
        assert_eq!(headers["x-taste-learning"], "false");
        assert_eq!(headers["x-cmd-zdr"], "1");
        let traceparent = headers["traceparent"].to_str().unwrap();
        assert_eq!(traceparent.len(), 55, "{traceparent}");
        assert!(traceparent.starts_with("00-"), "{traceparent}");
        assert!(traceparent.ends_with("-01"), "{traceparent}");
        assert_ne!(generate_traceparent(), generate_traceparent());

        // 关闭 ZDR 时省略该可选头。
        let mut headers = HeaderMap::new();
        apply_command_code_identity(
            &mut headers,
            &CommandCodeIdentity {
                cli_version: "1.53.1".into(),
                session_id: String::new(),
                project_slug: String::new(),
                zdr: false,
            },
        )
        .unwrap();
        assert!(!headers.contains_key("x-cmd-zdr"));
        assert!(
            !headers.contains_key("x-session-id") && !headers.contains_key("x-project-slug"),
            "empty identity values are omitted"
        );
    }

    /// 路径内联的 `?query` 必须以真正的查询串抵达 URL——
    /// `Url::set_path` 会把 `?` 百分号转义，从而打到 404 路径。
    #[test]
    fn upstream_url_splits_inline_query_strings() {
        let url = upstream_url(
            "https://api.commandcode.ai",
            "/alpha/billing/credits?orgId=org_1",
            None,
            "command_code",
        )
        .unwrap();
        assert_eq!(
            url.as_str(),
            "https://api.commandcode.ai/alpha/billing/credits?orgId=org_1"
        );
        let url = upstream_url(
            "https://api.commandcode.ai",
            "/alpha/usage/summary?orgId=org_1",
            Some("since=2026-01-01"),
            "command_code",
        )
        .unwrap();
        assert_eq!(
            url.as_str(),
            "https://api.commandcode.ai/alpha/usage/summary?since=2026-01-01&orgId=org_1"
        );
        let url = upstream_url(
            "https://api.commandcode.ai",
            "/provider/v1/models",
            None,
            "command_code",
        )
        .unwrap();
        assert_eq!(
            url.as_str(),
            "https://api.commandcode.ai/provider/v1/models"
        );
    }

    /// Command Code 用 GET 探测只读的 `whoami` 端点（不消耗 token），
    /// 其判定接受 CLI 返回的账户形状，同时拒绝错误体。
    #[test]
    fn command_code_health_probe_is_read_only_get() {
        let probe = health_probe("command_code", "ignored-model").expect("probe");
        assert_eq!(probe.method, http::Method::GET);
        assert_eq!(probe.path, "/alpha/whoami");
        assert!(probe.body.is_none(), "whoami carries no body");
        assert!(probe_body_ok(
            "command_code",
            br#"{"user":{"userName":"u"},"org":{"id":"org_1","login":"me"}}"#
        ));
        assert!(probe_body_ok("command_code", br#"{"orgId":"org_2"}"#));
        assert!(!probe_body_ok(
            "command_code",
            br#"{"error":{"code":"unauthorized"}}"#
        ));
        assert!(!probe_body_ok("command_code", b"not json"));
    }

    #[test]
    fn opencode_session_required_by_host_or_zen_path() {
        assert!(requires_opencode_session("https://opencode.ai/zen/go"));
        assert!(requires_opencode_session("https://api.opencode.ai/zen/v1"));
        assert!(requires_opencode_session("https://opencode.ai"));
        assert!(requires_opencode_session("http://127.0.0.1:8080/zen/go"));
        assert!(!requires_opencode_session(
            "https://opencode.ai.evil.example/v1"
        ));
        assert!(!requires_opencode_session("http://localhost/zenix"));
        assert!(!requires_opencode_session("not a url"));
    }

    /// Command Code 提供商把其目录行暴露给网关能转换的每个入口协议
    /// （以便路由候选池找到它们），而其它提供商则严格保持配置的绑定。
    #[test]
    fn command_code_model_bindings_cover_convertible_entries() {
        let configured = vec!["command_code".to_owned()];
        assert_eq!(
            model_binding_protocols(Some("command_code"), &configured),
            [
                "command_code",
                "openai_compatible",
                "openai_responses",
                "claude"
            ]
        );
        // 幂等：已绑定的入口不会重复。
        let already = vec![
            "command_code".to_owned(),
            "claude".to_owned(),
            "openai_compatible".to_owned(),
        ];
        let expanded = model_binding_protocols(Some("command_code"), &already);
        assert_eq!(
            expanded,
            [
                "command_code",
                "claude",
                "openai_compatible",
                "openai_responses"
            ]
        );
        // gemini 没有指向 command_code 的转换器：永不加入。
        assert!(!expanded.iter().any(|protocol| protocol == "gemini"));

        let openai = ["openai_compatible".to_owned(), "openai_responses".to_owned()];
        assert_eq!(model_binding_protocols(None, &openai), openai);
        assert_eq!(model_binding_protocols(Some("other"), &openai), openai);
        assert_eq!(model_binding_protocols(Some(""), &configured), configured);
    }

    fn values(headers: &HeaderMap) -> Vec<String> {
        headers
            .get_all(OPENCODE_SESSION_HEADER)
            .iter()
            .map(|value| value.to_str().unwrap().to_owned())
            .collect()
    }

    #[test]
    fn apply_opencode_session_keeps_client_value_and_collapses_duplicates() {
        let mut headers = HeaderMap::new();
        apply_opencode_session(&mut headers, "https://opencode.ai/zen/go", "install-1").unwrap();
        assert_eq!(values(&headers), ["install-1"]);

        // 有效的客户端值优先，即使重复也只保留一个。
        let mut headers = HeaderMap::new();
        headers.append(
            HeaderName::from_static(OPENCODE_SESSION_HEADER),
            HeaderValue::from_static("client-1"),
        );
        headers.append(
            HeaderName::from_static(OPENCODE_SESSION_HEADER),
            HeaderValue::from_static("client-1"),
        );
        apply_opencode_session(&mut headers, "https://opencode.ai/zen/go", "install-1").unwrap();
        assert_eq!(values(&headers), ["client-1"]);

        // 空的客户端值不可用：由兜底值替换。
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static(OPENCODE_SESSION_HEADER),
            HeaderValue::from_static(""),
        );
        apply_opencode_session(&mut headers, "https://opencode.ai/zen/go", "install-1").unwrap();
        assert_eq!(values(&headers), ["install-1"]);

        let mut headers = HeaderMap::new();
        apply_opencode_session(&mut headers, "http://127.0.0.1:9999/v1", "install-1").unwrap();
        assert!(values(&headers).is_empty());

        let mut headers = HeaderMap::new();
        apply_opencode_session(&mut headers, "https://opencode.ai", "").unwrap();
        assert!(values(&headers).is_empty());
    }
}
