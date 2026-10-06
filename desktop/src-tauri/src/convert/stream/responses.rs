//! Responses 侧事件发射与上游消费：output item（文本/函数调用）、
//! 序列号与 `response.completed`；并消费 openai chat 上游事件。
use super::*;

impl MappedStreamConverter {
    pub(super) fn responses_event(&mut self, _event: &str, payload: &Value) -> Vec<u8> {
        let mut enriched = payload.clone();
        enriched["sequence_number"] = json!(self.next_sequence);
        self.next_sequence += 1;
        sse_data(&enriched)
    }

    pub(super) fn envelope(&self, status: &str, error: Option<Value>) -> Value {
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

    pub(super) fn ensure_start_responses(&mut self) -> Vec<u8> {
        if self.started {
            return Vec::new();
        }
        self.started = true;
        self.responses_event(
            "response.created",
            &json!({"type": "response.created", "response": self.envelope("in_progress", None)}),
        )
    }

    pub(super) fn open_text_item(&mut self) -> Vec<u8> {
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

    pub(super) fn append_text_delta(&mut self, delta_text: &str) -> Vec<u8> {
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

    pub(super) fn close_text_item(&mut self) -> Vec<u8> {
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

    pub(super) fn open_function_item(&mut self, key: String, call_id: &str, name: &str) -> Vec<u8> {
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

    pub(super) fn append_function_arguments(&mut self, key: &str, delta: &str) -> Vec<u8> {
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

    pub(super) fn close_function_item(&mut self, key: &str) -> Vec<u8> {
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

    pub(super) fn finish_responses(&mut self, status: String) -> Vec<u8> {
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

    pub(super) fn error_event_responses(&mut self, message: &str) -> Vec<u8> {
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

    // -- DSML 抽取（ResponsesOpenai） -------------------------------------

    pub(super) fn consume_responses_openai(&mut self, event: &Value) -> Vec<u8> {
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
                    .or_else(|| {
                        upstream_index.and_then(|index| self.tool_key_by_index.get(&index).cloned())
                    });
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

    

    
}