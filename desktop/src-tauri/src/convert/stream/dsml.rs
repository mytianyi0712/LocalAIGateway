//! DSML 工具调用解析：从流式文本中识别工具调用标记并合成为
//! 入口协议的 tool-call 事件。
use super::*;

impl MappedStreamConverter {
    pub(super) fn emit_dsml_calls(&mut self, calls: &[Value]) -> Vec<u8> {
        let mut output = self.close_text_item();
        for call in calls {
            let key = format!("dsml_{}", self.dsml_tool_index);
            self.dsml_tool_index += 1;
            output.extend(self.open_function_item(
                key.clone(),
                &new_id("call"),
                call.get("name").and_then(Value::as_str).unwrap_or(""),
            ));
            output.extend(self.append_function_arguments(
                &key,
                call.get("arguments").and_then(Value::as_str).unwrap_or(""),
            ));
            output.extend(self.close_function_item(&key));
        }
        output
    }

    pub(super) fn drain_dsml_text(&mut self, text: Option<&str>, is_final: bool) -> Vec<u8> {
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
            if let Some((start_index, start_marker, end_marker)) =
                find_dsml_start(&self.dsml_text_buffer)
            {
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

    // -- 逐事件消费 -------------------------------------------------------

}