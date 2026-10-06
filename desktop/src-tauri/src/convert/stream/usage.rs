//! 流式用量累加：openai chat 上游的用量形状、键集与换算。
use super::*;

#[derive(Default, Clone)]
pub(super) struct UsageAcc {
    pub(super) input_tokens: Option<i64>,
    pub(super) output_tokens: Option<i64>,
    pub(super) cache_read: Option<i64>,
    pub(super) cache_write: Option<i64>,
    pub(super) reasoning: Option<i64>,
    /// 为 true 时 `input_tokens` 是含缓存命中的 TOTAL（Command Code 会带
    /// `prompt_tokens_includes_cache` 标记）。此时 Anthropic 的 `input_tokens`
    /// 必须换算为去缓存后的部分。
    pub(super) input_includes_cache: bool,
}

impl UsageAcc {
    // 与 merge_responses/claude/gemini 的差异：读 prompt_tokens/completion_tokens 与
    // prompt_tokens_details，并据此锁定 input_includes_cache 标记；键集不同，不合并。
    pub(super) fn merge_openai(&mut self, usage: &Value) {
        if let Some(value) = usage.get("prompt_tokens").and_then(Value::as_i64) {
            self.input_tokens = Some(value);
        }
        if usage
            .get(INPUT_INCLUDES_CACHE)
            .and_then(Value::as_bool)
            == Some(true)
        {
            self.input_includes_cache = true;
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
            if let Some(value) = details.get("cache_write_tokens").and_then(Value::as_i64)
                && value > 0
            {
                self.cache_write = Some(value);
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

    // 与 merge_openai 的差异：读 input_tokens/output_tokens 与 input_tokens_details，
    // 且不参与 input_includes_cache 标记；键集不同，不合并。
    

    // 与 merge_openai/responses 的差异：按 Claude 顶层键（含 cache_read_input_tokens）直接覆盖；不合并。
    

    // 与其它 merge_* 的差异：读 Gemini 的 promptTokenCount/candidatesTokenCount/cachedContentTokenCount；不合并。
    
}
