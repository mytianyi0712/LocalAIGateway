//! Claude 侧事件发射与上游消费：content block 开闭、thinking、usage 与
//! `message_stop`；并消费 openai chat 上游事件。
use super::*;

impl MappedStreamConverter {
    pub(super) fn ensure_start_claude(&mut self) -> Vec<u8> {
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

    pub(super) fn start_block(&mut self, index: i64, block: &Value) -> Vec<u8> {
        if self.open_blocks.contains_key(&index) {
            return Vec::new();
        }
        self.open_blocks.insert(
            index,
            block
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("text")
                .to_owned(),
        );
        sse(
            "content_block_start",
            &json!({"type": "content_block_start", "index": index, "content_block": block}),
        )
    }

    pub(super) fn delta(&self, index: i64, delta: &Value) -> Vec<u8> {
        sse(
            "content_block_delta",
            &json!({"type": "content_block_delta", "index": index, "delta": delta}),
        )
    }

    pub(super) fn stop_block(&mut self, index: i64) -> Vec<u8> {
        if !self.open_blocks.contains_key(&index) {
            return Vec::new();
        }
        let block_type = self
            .open_blocks
            .remove(&index)
            .unwrap_or_else(|| "text".to_owned());
        let mut output = Vec::new();
        if block_type == "thinking" {
            // 上游推理流不带 signature，但 Anthropic 客户端期望 thinking block
            // 关闭前先来一个 `signature_delta`。于是基于累积的 thinking 文本
            // 合成一个确定性占位签名（不涉及任何密钥）。
            use base64::Engine as _;
            use sha2::{Digest, Sha256};
            let text = self.thinking_text.remove(&index).unwrap_or_default();
            let mut hasher = Sha256::new();
            hasher.update(text.as_bytes());
            hasher.update(index.to_le_bytes());
            let signature = base64::engine::general_purpose::STANDARD.encode(hasher.finalize());
            output.extend(self.delta(
                index,
                &json!({"type": "signature_delta", "signature": signature}),
            ));
        }
        output.extend(sse(
            "content_block_stop",
            &json!({"type": "content_block_stop", "index": index}),
        ));
        output
    }

    /// 追加一段 thinking：同一段思考必须复用同一个 content block。上游每个
    /// delta 都开一个新 block 会让 Claude 客户端看到 N 段独立的思考（真实
    /// 观测：每个 token 一个 `content_block_start`）。
    pub(super) fn thinking_delta(&mut self, text: &str) -> Vec<u8> {
        let index = match self.current_block_of_type("thinking") {
            Some(index) => index,
            None => {
                let index = self.next_block_index();
                let mut output =
                    self.start_block(index, &json!({"type": "thinking", "thinking": ""}));
                output.extend(self.emit_thinking_delta(index, text));
                return output;
            }
        };
        self.emit_thinking_delta(index, text)
    }

    pub(super) fn emit_thinking_delta(&mut self, index: i64, text: &str) -> Vec<u8> {
        self.thinking_text
            .entry(index)
            .or_default()
            .push_str(text);
        self.delta(
            index,
            &json!({"type": "thinking_delta", "thinking": text}),
        )
    }

    /// Claude 的 content block 必须顺序开闭：非 thinking 内容开始前先关闭
    /// 打开中的 thinking block（`stop_block` 会补上 signature_delta）。
    pub(super) fn close_thinking(&mut self) -> Vec<u8> {
        self.current_block_of_type("thinking")
            .map(|index| self.stop_block(index))
            .unwrap_or_default()
    }

    pub(super) fn current_block_of_type(&self, block_type: &str) -> Option<i64> {
        self.open_blocks
            .iter()
            .find(|(_, current)| current.as_str() == block_type)
            .map(|(index, _)| *index)
    }

    /// 单调递增，且**不复用**已关闭块的 index：Anthropic 客户端按 index
    /// 累积 content，thinking 关闭后若 text 又用回 0，会让文本覆盖思考块。
    pub(super) fn next_block_index(&mut self) -> i64 {
        while self.open_blocks.contains_key(&self.next_index) {
            self.next_index += 1;
        }
        let index = self.next_index;
        self.next_index += 1;
        index
    }

    pub(super) fn anthropic_input_tokens(&self) -> Option<i64> {
        if self.usage.input_includes_cache {
            // OpenAI/CC 的 `prompt_tokens` 是总数；Anthropic 只要去缓存后的部分。
            // 换算式与非流式路径完全一致，见 response::uncached_input_tokens。
            uncached_input_tokens(
                self.usage.input_tokens,
                self.usage.cache_read,
                self.usage.cache_write,
            )
        } else {
            self.usage.input_tokens
        }
    }

    pub(super) fn finish_claude(&mut self, stop_reason: &str) -> Vec<u8> {
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
                    self.anthropic_input_tokens(),
                    self.usage.output_tokens,
                    self.usage.cache_read,
                    self.usage.cache_write,
                ),
            }),
        ));
        output.extend(sse("message_stop", &json!({"type": "message_stop"})));
        output
    }

    pub(super) fn consume_claude_openai(&mut self, event: &Value) -> Vec<u8> {
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
        if let Some(reasoning) = reasoning
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
        {
            output.extend(self.thinking_delta(reasoning));
        }
        let text = chat_message_text(delta.get("content").unwrap_or(&Value::Null));
        if !text.is_empty() {
            let index = match self.current_block_of_type("text") {
                Some(index) => index,
                None => {
                    output.extend(self.close_thinking());
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
                    .or_else(|| {
                        upstream_index
                            .and_then(|index| self.tool_index_by_upstream.get(&index).copied())
                    });
                let index =
                    match index {
                        Some(index) => index,
                        None => {
                            output.extend(self.close_thinking());
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
                    output.extend(self.delta(
                        index,
                        &json!({"type": "input_json_delta", "partial_json": arguments}),
                    ));
                }
            }
        }
        if let Some(finish_reason) = choice.get("finish_reason").and_then(Value::as_str) {
            self.pending_finish_reason = Some(stop_reason_openai(finish_reason).to_owned());
        }
        output
    }

    

    

}