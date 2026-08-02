import json

from app.adapters.convert import (
    GeminiStreamToClaude,
    OpenAICompatibleStreamToClaude,
    OpenAICompatibleStreamToResponses,
    OpenAIResponsesStreamToClaude,
    claude_to_gemini,
    claude_to_openai_compatible,
    claude_to_openai_responses,
    convert_codex_request,
    gemini_to_claude,
    openai_compatible_to_claude,
    openai_compatible_to_responses,
    openai_responses_to_claude,
    responses_to_openai_compatible,
)


def _parse_sse(data: bytes):
    events = []
    for line in data.splitlines():
        if line.startswith(b"data:"):
            events.append(json.loads(line[5:].strip()))
    return events


def _parse_json_lines(data: bytes):
    events = []
    for line in data.splitlines():
        line = line.strip()
        if not line.startswith(b"data:"):
            continue
        payload = line[5:].strip()
        if payload == b"[DONE]":
            continue
        events.append(json.loads(payload))
    return events

def test_codex_responses_maps_developer_message_to_system_for_chat_compat():
    converted = json.loads(
        convert_codex_request(
            "openai_compatible",
            "deepseek-v4-flash",
            json.dumps(
                {
                    "model": "gpt-5-codex",
                    "input": [
                        {
                            "type": "message",
                            "role": "developer",
                            "content": [
                                {"type": "input_text", "text": "Follow project instructions"}
                            ],
                        },
                        {
                            "type": "message",
                            "role": "user",
                            "content": [{"type": "input_text", "text": "hello"}],
                        },
                    ],
                }
            ).encode(),
        )
    )

    assert [message["role"] for message in converted["messages"]] == ["system", "user"]


def test_responses_tool_replay_includes_deepseek_reasoning_content():
    converted = responses_to_openai_compatible(
        "deepseek-v4-flash",
        {
            "input": [
                {"type": "message", "role": "user", "content": "查天气"},
                {
                    "type": "function_call",
                    "call_id": "call_1",
                    "name": "get_weather",
                    "arguments": '{"city":"Hangzhou"}',
                },
                {"type": "function_call_output", "call_id": "call_1", "output": "晴"},
            ]
        },
    )

    assistant = converted["messages"][1]
    assert assistant["reasoning_content"] == ""
    assert assistant["tool_calls"][0]["function"]["name"] == "get_weather"
    assert converted["messages"][2]["role"] == "tool"


def test_responses_tool_replay_groups_parallel_calls_before_outputs():
    converted = responses_to_openai_compatible(
        "deepseek-v4-flash",
        {
            "input": [
                {"type": "message", "role": "user", "content": "查两项"},
                {
                    "type": "function_call",
                    "call_id": "call_1",
                    "name": "functions.exec",
                    "arguments": '{"cmd":"one"}',
                },
                {
                    "type": "function_call",
                    "call_id": "call_2",
                    "name": "functions.exec",
                    "arguments": '{"cmd":"two"}',
                },
                {"type": "function_call_output", "call_id": "call_1", "output": "1"},
                {"type": "function_call_output", "call_id": "call_2", "output": "2"},
            ]
        },
    )

    assert [message["role"] for message in converted["messages"]] == [
        "user",
        "assistant",
        "tool",
        "tool",
    ]
    assert [
        call["id"] for call in converted["messages"][1]["tool_calls"]
    ] == ["call_1", "call_2"]


def test_openai_compatible_stream_tool_calls():
    converter = OpenAICompatibleStreamToClaude("claude-opus-5")
    chunks = [
        b'data: {"id":"x","choices":[{"index":0,"delta":{"role":"assistant","content":"Let me look"},"finish_reason":null}]}\n\n',
        b'data: {"id":"x","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":""}}]},"finish_reason":null}]}\n\n',
        b'data: {"id":"x","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\\"city\\":\\"Beijing\\"}"}}]},"finish_reason":null}]}\n\n',
        b'data: {"id":"x","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}\n\n',
        b"data: [DONE]\n\n",
    ]
    events = []
    for chunk in chunks:
        events.extend(_parse_sse(converter.feed(chunk)))
    events.extend(_parse_sse(converter.flush()))

    types = [event["type"] for event in events]
    assert types[0] == "message_start"
    assert types[-1] == "message_stop"
    starts = [e for e in events if e["type"] == "content_block_start"]
    assert [block["content_block"]["type"] for block in starts] == ["text", "tool_use"]
    tool_start = starts[1]["content_block"]
    assert tool_start["id"] == "call_1"
    assert tool_start["name"] == "get_weather"
    deltas = [e for e in events if e["type"] == "content_block_delta"]
    json_deltas = "".join(
        d["delta"]["partial_json"] for d in deltas if d["delta"]["type"] == "input_json_delta"
    )
    assert json.loads(json_deltas) == {"city": "Beijing"}
    final = [e for e in events if e["type"] == "message_delta"][-1]
    assert final["delta"]["stop_reason"] == "tool_use"


def test_openai_compatible_stream_reasoning_and_text_use_distinct_indices():
    converter = OpenAICompatibleStreamToClaude("claude-opus-5")
    chunks = [
        b'data: {"id":"x","choices":[{"index":0,"delta":{"reasoning_content":"think hard"},"finish_reason":null}]}\n\n',
        b'data: {"id":"x","choices":[{"index":0,"delta":{"content":"answer"},"finish_reason":"stop"}]}\n\n',
        b'data: {"id":"x","choices":[],"usage":{"prompt_tokens":2,"completion_tokens":1}}\n\n',
        b"data: [DONE]\n\n",
    ]
    events = []
    for chunk in chunks:
        events.extend(_parse_sse(converter.feed(chunk)))
    events.extend(_parse_sse(converter.flush()))

    starts = [e for e in events if e["type"] == "content_block_start"]
    assert [block["content_block"]["type"] for block in starts] == ["thinking", "text"]
    assert starts[0]["index"] != starts[1]["index"]
    deltas = [e for e in events if e["type"] == "content_block_delta"]
    assert any(d["delta"]["type"] == "thinking_delta" for d in deltas)
    assert any(d["delta"]["type"] == "text_delta" for d in deltas)
    final = [e for e in events if e["type"] == "message_delta"][-1]
    assert final["usage"]["output_tokens"] == 1


def test_non_streaming_openai_to_claude():
    data = {
        "id": "chatcmpl-9",
        "choices": [
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "hi",
                    "tool_calls": [
                        {
                            "id": "call_7",
                            "type": "function",
                            "function": {"name": "f", "arguments": '{"a": 1}'},
                        }
                    ],
                },
                "finish_reason": "tool_calls",
            }
        ],
        "usage": {"prompt_tokens": 3, "completion_tokens": 2},
    }
    converted = openai_compatible_to_claude("claude-opus-5", data)
    assert converted["content"][0] == {"type": "text", "text": "hi"}
    assert converted["content"][1]["type"] == "tool_use"
    assert converted["content"][1]["input"] == {"a": 1}
    assert converted["stop_reason"] == "tool_use"
    assert converted["usage"]["input_tokens"] == 3


def test_claude_to_gemini_roundtrip():
    claude_request = {
        "model": "claude-fable-5",
        "max_tokens": 100,
        "system": [{"type": "text", "text": "sys"}],
        "messages": [
            {
                "role": "assistant",
                "content": [{"type": "tool_use", "id": "toolu_1", "name": "calc", "input": {"x": 2}}],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "toolu_1",
                        "content": [{"type": "text", "text": "4"}],
                    },
                    {"type": "text", "text": "now?"},
                ],
            },
        ],
    }
    converted = claude_to_gemini("gemini-2.5-pro", claude_request)
    assert converted["systemInstruction"] == {"parts": [{"text": "sys"}]}
    assert converted["contents"][0]["role"] == "model"
    assert converted["contents"][0]["parts"][0]["functionCall"] == {
        "name": "calc",
        "args": {"x": 2},
    }
    assert converted["contents"][1]["role"] == "user"
    assert converted["contents"][1]["parts"][0]["functionResponse"]["name"] == "calc"
    assert converted["contents"][1]["parts"][1] == {"text": "now?"}
    assert converted["generationConfig"]["maxOutputTokens"] == 100

    gemini_response = {
        "candidates": [
            {
                "content": {
                    "role": "model",
                    "parts": [
                        {"text": "answer"},
                        {"functionCall": {"name": "calc", "args": {"x": 3}}},
                    ],
                },
                "finishReason": "TOOL_CALL",
            }
        ],
        "usageMetadata": {"promptTokenCount": 8, "candidatesTokenCount": 5},
    }
    claude = gemini_to_claude("claude-fable-5", gemini_response)
    assert claude["content"][0] == {"type": "text", "text": "answer"}
    assert claude["content"][1]["type"] == "tool_use"
    assert claude["content"][1]["input"] == {"x": 3}
    assert claude["stop_reason"] == "tool_use"
    assert claude["usage"]["input_tokens"] == 8
    assert claude["usage"]["output_tokens"] == 5


def test_gemini_stream_to_claude():
    converter = GeminiStreamToClaude("claude-fable-5")
    chunks = [
        b'{"candidates":[{"content":{"role":"model","parts":[{"text":"Hel"}]},"finishReason":null}]}\n',
        b'{"candidates":[{"content":{"role":"model","parts":[{"text":"lo"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":2}}\n',
    ]
    events = []
    for chunk in chunks:
        events.extend(_parse_json_lines(converter.feed(chunk)))
    events.extend(_parse_json_lines(converter.flush()))
    types = [event["type"] for event in events]
    assert types[0] == "message_start"
    assert types[-1] == "message_stop"
    deltas = [e for e in events if e["type"] == "content_block_delta"]
    assert "".join(d["delta"]["text"] for d in deltas if d["delta"]["type"] == "text_delta") == "Hello"
    final = [e for e in events if e["type"] == "message_delta"][-1]
    assert final["delta"]["stop_reason"] == "end_turn"
    assert final["usage"]["output_tokens"] == 2


def test_openai_responses_conversions():
    request = claude_to_openai_responses(
        "gpt-5.3",
        {
            "model": "claude-opus-5",
            "max_tokens": 64,
            "system": "be brief",
            "messages": [
                {"role": "user", "content": "hello"},
                {
                    "role": "assistant",
                    "content": [
                        {"type": "tool_use", "id": "toolu_2", "name": "search", "input": {"q": "x"}}
                    ],
                },
                {
                    "role": "user",
                    "content": [{"type": "tool_result", "tool_use_id": "toolu_2", "content": "none"}],
                },
            ],
        },
    )
    assert request["model"] == "gpt-5.3"
    assert request["instructions"] == "be brief"
    assert request["max_output_tokens"] == 64
    assert request["input"][0]["content"][0] == {"type": "input_text", "text": "hello"}
    assert request["input"][1]["type"] == "function_call"
    assert request["input"][1]["name"] == "search"
    assert request["input"][2]["type"] == "function_call_output"
    assert request["input"][2]["output"] == "none"

    response = openai_responses_to_claude(
        "claude-opus-5",
        {
            "id": "resp_1",
            "output": [
                {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "hi"}]},
                {"type": "function_call", "call_id": "call_1", "name": "search", "arguments": '{"q":"y"}'},
            ],
            "usage": {"input_tokens": 4, "output_tokens": 3},
        },
    )
    assert response["content"][0] == {"type": "text", "text": "hi"}
    assert response["content"][1]["type"] == "tool_use"
    assert response["content"][1]["name"] == "search"
    assert response["content"][1]["input"] == {"q": "y"}
    assert response["stop_reason"] == "tool_use"


def test_responses_stream_to_claude():
    converter = OpenAIResponsesStreamToClaude("claude-opus-5")
    chunks = [
        b'data: {"type":"response.created","response":{"id":"resp_1"}}\n\n',
        b'data: {"type":"response.output_item.added","item":{"type":"function_call","id":"fc_1","call_id":"call_1","name":"search","arguments":""}}\n\n',
        b'data: {"type":"response.function_call_arguments.delta","item_id":"fc_1","delta":"{\\"q\\":"}\n\n',
        b'data: {"type":"response.function_call_arguments.delta","item_id":"fc_1","delta":"\\"y\\"}"}\n\n',
        b'data: {"type":"response.output_text.delta","delta":"done"}\n\n',
        b'data: {"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":2,"output_tokens":1}}}\n\n',
    ]
    events = []
    for chunk in chunks:
        events.extend(_parse_sse(converter.feed(chunk)))
    events.extend(_parse_sse(converter.flush()))
    types = [event["type"] for event in events]
    assert types[0] == "message_start"
    assert types[-1] == "message_stop"
    starts = [e for e in events if e["type"] == "content_block_start"]
    block_types = [block["content_block"]["type"] for block in starts]
    assert block_types == ["tool_use", "text"]
    deltas = [e for e in events if e["type"] == "content_block_delta"]
    partial = "".join(d["delta"]["partial_json"] for d in deltas if d["delta"]["type"] == "input_json_delta")
    assert json.loads(partial) == {"q": "y"}
    final = [e for e in events if e["type"] == "message_delta"][-1]
    assert final["usage"]["output_tokens"] == 1


def test_claude_to_openai_compatible_image_and_tool_choice():
    converted = claude_to_openai_compatible(
        "deepseek-v3",
        {
            "model": "claude-opus-5",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "look"},
                        {
                            "type": "image",
                            "source": {"type": "base64", "media_type": "image/png", "data": "AAAA"},
                        },
                    ],
                }
            ],
            "tool_choice": {"type": "tool", "name": "search"},
        },
    )
    content = converted["messages"][0]["content"]
    assert content[0] == {"type": "text", "text": "look"}
    assert content[1]["type"] == "image_url"
    assert content[1]["image_url"]["url"] == "data:image/png;base64,AAAA"
    assert converted["tool_choice"] == {"type": "function", "function": {"name": "search"}}


def _sse_events(converter, chunks):
    events = []
    for chunk in chunks:
        for line in converter.feed(chunk).splitlines():
            if line.startswith(b"data:") and line[5:].strip() != b"[DONE]":
                events.append(json.loads(line[5:]))
    for line in converter.flush().splitlines():
        if line.startswith(b"data:") and line[5:].strip() != b"[DONE]":
            events.append(json.loads(line[5:]))
    return events


def test_responses_stream_merges_tool_calls_across_id_and_index_chunks():
    """OpenAI/DeepSeek streaming sends the tool id only in the first chunk and
    index-only deltas afterwards; the converter must merge them into one
    function_call item instead of emitting a duplicate empty-name item."""
    converter = OpenAICompatibleStreamToResponses("gpt-5-codex")
    chunks = [
        b'data: {"id":"x","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":""}}]},"finish_reason":null}]}\n\n',
        b'data: {"id":"x","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\\"city\\":\\"Bei"}}]},"finish_reason":null}]}\n\n',
        b'data: {"id":"x","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"jing\\"}"}}]},"finish_reason":null}]}\n\n',
        b'data: {"id":"x","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}\n\n',
        b"data: [DONE]\n\n",
    ]
    events = _sse_events(converter, chunks)
    completed = next(e for e in events if e["type"] == "response.completed")
    output = completed["response"]["output"]
    assert len(output) == 1
    call = output[0]
    assert call["type"] == "function_call"
    assert call["name"] == "get_weather"
    assert call["call_id"] == "call_1"
    assert json.loads(call["arguments"]) == {"city": "Beijing"}


def test_responses_stream_parallel_tool_calls_keep_distinct_arguments():
    converter = OpenAICompatibleStreamToResponses("gpt-5-codex")
    chunks = [
        b'data: {"id":"x","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_a","type":"function","function":{"name":"fa","arguments":""}},{"index":1,"id":"call_b","type":"function","function":{"name":"fb","arguments":""}}]},"finish_reason":null}]}\n\n',
        b'data: {"id":"x","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\\"a\\":"}},{"index":1,"function":{"arguments":"{\\"b\\":"}}]},"finish_reason":null}]}\n\n',
        b'data: {"id":"x","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"1}"}},{"index":1,"function":{"arguments":"2}"}}]},"finish_reason":null}]}\n\n',
        b'data: {"id":"x","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}\n\n',
        b"data: [DONE]\n\n",
    ]
    events = _sse_events(converter, chunks)
    completed = next(e for e in events if e["type"] == "response.completed")
    output = completed["response"]["output"]
    assert len(output) == 2
    by_name = {item["name"]: item for item in output}
    assert json.loads(by_name["fa"]["arguments"]) == {"a": 1}
    assert json.loads(by_name["fb"]["arguments"]) == {"b": 2}


def test_responses_conversion_handles_list_content_non_streaming():
    """GPT-5.x chat upstreams may return message.content as a list of parts;
    it must be flattened to plain text in the Responses output_text item."""
    converted = openai_compatible_to_responses(
        "gpt-5-codex",
        {
            "id": "x",
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": [
                            {"type": "text", "text": "hello"},
                            {"type": "text", "text": "world"},
                        ],
                    },
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 1, "completion_tokens": 2},
        },
    )
    message = next(item for item in converted["output"] if item["type"] == "message")
    assert message["content"][0]["type"] == "output_text"
    assert message["content"][0]["text"] == "hello\nworld"


def test_responses_stream_handles_list_content_delta():
    converter = OpenAICompatibleStreamToResponses("gpt-5-codex")
    events = _sse_events(
        converter,
        [
            b'data: {"id":"x","choices":[{"index":0,"delta":{"content":[{"type":"text","text":"hi "},{"type":"text","text":"there"}]},"finish_reason":null}]}\n\n',
            b'data: {"id":"x","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\n',
            b"data: [DONE]\n\n",
        ],
    )
    deltas = [e for e in events if e["type"] == "response.output_text.delta"]
    assert "".join(d["delta"] for d in deltas) == "hi \nthere"
    done = next(e for e in events if e["type"] == "response.output_item.done")
    assert done["item"]["content"][0]["text"] == "hi \nthere"


def test_responses_stream_accepts_crlf_line_endings():
    """Some upstreams/proxies emit SSE with \r\n line endings; chunks must be
    parsed immediately instead of being buffered until flush and lost."""
    converter = OpenAICompatibleStreamToResponses("gpt-5-codex")
    out = converter.feed(
        b'data: {"id":"x","choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":null}]}\r\n\r\n'
    )
    assert b"response.output_text.delta" in out
    events = _sse_events(converter, [])
    completed = next(e for e in events if e["type"] == "response.completed")
    message = next(item for item in completed["response"]["output"] if item["type"] == "message")
    assert message["content"][0]["text"] == "Hello"


def test_responses_conversion_reads_deepseek_cache_hit_tokens():
    converted = openai_compatible_to_responses(
        "gpt-5-codex",
        {
            "id": "x",
            "choices": [
                {"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}
            ],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 3,
                "prompt_tokens_details": {"prompt_cache_hit_tokens": 7},
            },
        },
    )
    assert converted["usage"]["input_tokens_details"]["cached_tokens"] == 7
    assert converted["usage"]["total_tokens"] == 13


def test_responses_stream_usage_always_has_required_detail_counters():
    converter = OpenAICompatibleStreamToResponses("gpt-5-codex")
    events = _sse_events(
        converter,
        [
            b'data: {"id":"x","choices":[{"index":0,"delta":{"content":"ok"},"finish_reason":"stop"}]}\n\n',
            b'data: {"id":"x","choices":[],"usage":{"prompt_tokens":4,"completion_tokens":1}}\n\n',
            b"data: [DONE]\n\n",
        ],
    )

    completed = next(event for event in events if event["type"] == "response.completed")
    usage = completed["response"]["usage"]
    assert usage["input_tokens_details"] == {"cached_tokens": 0}
    assert usage["output_tokens_details"] == {"reasoning_tokens": 0}


def test_responses_stream_converts_fragmented_dsml_tool_calls():
    converter = OpenAICompatibleStreamToResponses("gpt-5-codex")

    def delta(text):
        payload = {
            "id": "x",
            "choices": [
                {"index": 0, "delta": {"content": text}, "finish_reason": None}
            ],
        }
        return b"data: " + json.dumps(payload, ensure_ascii=False).encode() + b"\n\n"

    events = _sse_events(
        converter,
        [
            delta("我先抓取官方页面。\n<|DSM"),
            delta(
                'L|tool_calls>\n<|DSML|invoke name="functions.exec">\n'
                '<|DSML|parameter name="cmd" string="true">curl -sL https://archlinux.org/news/</|DSML|parameter>\n'
                '</|DSML|invoke>\n<|DSML|invoke name="functions.exec">\n'
            ),
            delta(
                '<|DSML|parameter name="cmd" string="true">curl -sL https://kde.org/announcements/</|DSML|parameter>\n'
                '</|DSML|invoke>\n</|DSML|tool_calls>'
            ),
            b'data: {"id":"x","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\n',
            b"data: [DONE]\n\n",
        ],
    )

    visible_text = "".join(
        event["delta"]
        for event in events
        if event["type"] == "response.output_text.delta"
    )
    assert visible_text == "我先抓取官方页面。\n"
    assert "DSML" not in visible_text

    completed = next(event for event in events if event["type"] == "response.completed")
    calls = [item for item in completed["response"]["output"] if item["type"] == "function_call"]
    assert [call["name"] for call in calls] == ["functions.exec", "functions.exec"]
    assert [json.loads(call["arguments"]) for call in calls] == [
        {"cmd": "curl -sL https://archlinux.org/news/"},
        {"cmd": "curl -sL https://kde.org/announcements/"},
    ]

def test_responses_non_stream_converts_dsml_function_calls_and_json_parameters():
    converted = openai_compatible_to_responses(
        "gpt-5-codex",
        {
            "id": "x",
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": (
                            "准备调用工具。\n"
                            '<|DSML|function_calls><|DSML|invoke name="search">'
                            '<|DSML|parameter name="query" string="true">Arch &amp; KDE</|DSML|parameter>'
                            '<|DSML|parameter name="options" string="false">{"depth": 2}</|DSML|parameter>'
                            "</|DSML|invoke></|DSML|function_calls>"
                        ),
                    },
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 5, "completion_tokens": 3},
        },
    )

    assert [item["type"] for item in converted["output"]] == ["message", "function_call"]
    message, call = converted["output"]
    assert message["content"][0]["text"] == "准备调用工具。\n"
    assert call["name"] == "search"
    assert json.loads(call["arguments"]) == {
        "query": "Arch & KDE",
        "options": {"depth": 2},
    }


def test_responses_non_stream_converts_fullwidth_dsml_markers():
    converted = openai_compatible_to_responses(
        "gpt-5-codex",
        {
            "choices": [
                {
                    "message": {
                        "content": (
                            "开始。"
                            '<｜｜DSML｜｜function_calls><｜｜DSML｜｜invoke name="search">'
                            '<｜｜DSML｜｜parameter name="query" string="true">Arch</｜｜DSML｜｜parameter>'
                            "</｜｜DSML｜｜invoke></｜｜DSML｜｜function_calls>"
                        )
                    }
                }
            ]
        },
    )

    assert [item["type"] for item in converted["output"]] == ["message", "function_call"]
    assert converted["output"][1]["name"] == "search"
    assert json.loads(converted["output"][1]["arguments"]) == {"query": "Arch"}


def test_responses_stream_converts_message_style_dsml_calls():
    converter = OpenAICompatibleStreamToResponses("gpt-5-codex")
    content = (
        "前置说明。\n"
        "<｜｜DSML｜｜memory pass: search MEMORY.md for KDE/niri relevant entries.\n"
        "\nLet me search quickly.\n\n"
        "<｜｜DSML｜｜tool_calls>\n"
        '<｜｜DSML｜｜message name="exec_command" to=functions.exec>\n'
        "<｜｜DSML｜｜command>\n"
        'rg -n -i "niri|kde" /home/mytianyi/.codex/memories/MEMORY.md | head -50\n'
        "</｜｜DSML｜｜command>\n"
        "</｜｜DSML｜｜invoke>\n"
        "</｜｜DSML｜｜tool_calls>"
    )

    def delta(text):
        payload = {
            "id": "x",
            "choices": [{"index": 0, "delta": {"content": text}, "finish_reason": None}],
        }
        return b"data: " + json.dumps(payload, ensure_ascii=False).encode() + b"\n\n"

    events = _sse_events(converter, [delta(content[:37]), delta(content[37:119]), delta(content[119:])])
    visible = "".join(
        event["delta"]
        for event in events
        if event["type"] == "response.output_text.delta"
    )
    assert "DSML" not in visible
    completed = next(event for event in events if event["type"] == "response.completed")
    calls = [item for item in completed["response"]["output"] if item["type"] == "function_call"]
    assert len(calls) == 1
    assert calls[0]["name"] == "functions.exec"
    assert json.loads(calls[0]["arguments"]) == {
        "cmd": 'rg -n -i "niri|kde" /home/mytianyi/.codex/memories/MEMORY.md | head -50'
    }