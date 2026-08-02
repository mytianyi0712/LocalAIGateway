import json

import httpx
import pytest
import respx


class ChunkStream(httpx.AsyncByteStream):
    def __init__(self, chunks):
        self.chunks = chunks

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk


def create_mapping(
    client,
    admin_headers,
    *,
    codex_model_id="gpt-5-codex",
    upstream_protocol="openai_compatible",
    provider_name="Mock upstream",
    upstream_model="deepseek-v3",
    channel_name="primary",
    api_key="key-primary",
    with_route=True,
):
    """Create a provider + channel + channel model + route + codex mapping.

    The mapping references the existing system model (upstream_model) whose
    route provides the candidates, mirroring how the feature works in prod.
    """
    provider = client.post(
        "/api/admin/v1/providers",
        headers=admin_headers,
        json={"name": provider_name, "base_url": "https://upstream.test"},
    ).json()
    channel = client.post(
        "/api/admin/v1/channels",
        headers=admin_headers,
        json={
            "provider_id": provider["id"],
            "name": channel_name,
            "protocol": upstream_protocol,
            "api_key": api_key,
        },
    )
    assert channel.status_code == 201, channel.text
    channel = channel.json()
    model = client.post(
        f"/api/admin/v1/channels/{channel['id']}/models",
        headers=admin_headers,
        json={"model_id": upstream_model},
    )
    assert model.status_code == 201, model.text
    model = model.json()
    route = None
    if with_route:
        route_response = client.post(
            "/api/admin/v1/routes",
            headers=admin_headers,
            json={"protocol": upstream_protocol, "requested_model_id": upstream_model},
        )
        assert route_response.status_code == 201, route_response.text
        route = route_response.json()
        candidates_response = client.put(
            f"/api/admin/v1/routes/{route['id']}/candidates",
            headers=admin_headers,
            json={
                "candidates": [
                    {"channel_model_id": model["id"], "priority": 0},
                ]
            },
        )
        assert candidates_response.status_code == 200, candidates_response.text
    mapping = client.post(
        "/api/admin/v1/codex-mappings",
        headers=admin_headers,
        json={
            "codex_model_id": codex_model_id,
            "display_name": "GPT-5 Codex",
            "upstream_protocol": upstream_protocol,
            "upstream_model_id": upstream_model,
        },
    )
    assert mapping.status_code == 201, mapping.text
    return provider, channel, model, mapping.json(), route


def test_mapping_admin_crud_and_catalog(client, admin_headers):
    _, _, _, mapping, _ = create_mapping(client, admin_headers)

    # list
    items = client.get("/api/admin/v1/codex-mappings", headers=admin_headers).json()["items"]
    assert len(items) == 1
    assert items[0]["codex_model_id"] == "gpt-5-codex"
    assert items[0]["upstream_protocol"] == "openai_compatible"
    assert items[0]["upstream_model_id"] == "deepseek-v3"
    assert len(items[0]["candidates"]) == 1
    assert items[0]["candidates"][0]["model_id"] == "deepseek-v3"

    # patch display name only
    patched = client.patch(
        f"/api/admin/v1/codex-mappings/{mapping['id']}",
        headers=admin_headers,
        json={"display_name": "Renamed"},
    )
    assert patched.status_code == 200
    assert patched.json()["display_name"] == "Renamed"

    # invalid protocol rejected
    bad = client.patch(
        f"/api/admin/v1/codex-mappings/{mapping['id']}",
        headers=admin_headers,
        json={"upstream_protocol": "bogus"},
    )
    assert bad.status_code == 422

    # duplicate name rejected
    duplicate = client.post(
        "/api/admin/v1/codex-mappings",
        headers=admin_headers,
        json={
            "codex_model_id": "gpt-5-codex",
            "upstream_protocol": "openai_compatible",
            "upstream_model_id": "deepseek-v3",
        },
    )
    assert duplicate.status_code == 409

    # /codex catalog exposes the mapped model (Codex CLI probing entry).
    # The OpenAI-style catalog omits display_name; metadata lives in x_local_gateway.
    catalog = client.get("/codex/v1/models")
    assert catalog.status_code == 200
    ids = [item["id"] for item in catalog.json()["data"]]
    assert "gpt-5-codex" in ids
    item = next(item for item in catalog.json()["data"] if item["id"] == "gpt-5-codex")
    assert item["object"] == "model"
    assert item["x_local_gateway"]["mapping"]["upstream_protocol"] == "openai_compatible"
    assert item["x_local_gateway"]["mapping"]["upstream_model_id"] == "deepseek-v3"
    assert "/v1/responses" in item["x_local_gateway"]["supported_endpoints"]

    # /codex/v1/responses/models alias also lists mapped models
    alias = client.get("/codex/v1/responses/models")
    assert alias.status_code == 200
    alias_ids = [item["id"] for item in alias.json()["data"]]
    assert "gpt-5-codex" in alias_ids

    # codex info endpoint
    info = client.get("/codex")
    assert info.status_code == 200
    assert info.json()["endpoints"] == [
        "GET /codex/v1/models",
        "GET /codex/v1/responses/models",
        "POST /codex/v1/responses",
    ]

    # delete
    deleted = client.delete(f"/api/admin/v1/codex-mappings/{mapping['id']}", headers=admin_headers)
    assert deleted.status_code == 204
    catalog = client.get("/codex/v1/models")
    ids = [item["id"] for item in catalog.json()["data"]]
    assert "gpt-5-codex" not in ids


def test_mapping_requires_existing_route(client, admin_headers):
    provider = client.post(
        "/api/admin/v1/providers",
        headers=admin_headers,
        json={"name": "Mock upstream", "base_url": "https://upstream.test"},
    ).json()
    channel = client.post(
        "/api/admin/v1/channels",
        headers=admin_headers,
        json={
            "provider_id": provider["id"],
            "name": "primary",
            "protocol": "openai_compatible",
            "api_key": "key-primary",
        },
    ).json()
    model = client.post(
        f"/api/admin/v1/channels/{channel['id']}/models",
        headers=admin_headers,
        json={"model_id": "deepseek-v3"},
    ).json()

    # upstream model that does not exist in the system -> rejected
    missing = client.post(
        "/api/admin/v1/codex-mappings",
        headers=admin_headers,
        json={
            "codex_model_id": "gpt-5-codex",
            "upstream_protocol": "openai_compatible",
            "upstream_model_id": "no-such-model",
        },
    )
    assert missing.status_code == 422

    # model exists but has no route for the protocol -> rejected
    no_route = client.post(
        "/api/admin/v1/codex-mappings",
        headers=admin_headers,
        json={
            "codex_model_id": "gpt-5-codex",
            "upstream_protocol": "openai_compatible",
            "upstream_model_id": "deepseek-v3",
        },
    )
    assert no_route.status_code == 422
    assert "route" in no_route.json()["detail"].lower()

    # model routed for a different protocol -> rejected
    claude_route = client.post(
        "/api/admin/v1/routes",
        headers=admin_headers,
        json={"protocol": "claude", "requested_model_id": "deepseek-v3"},
    )
    assert claude_route.status_code == 201
    no_protocol_route = client.post(
        "/api/admin/v1/codex-mappings",
        headers=admin_headers,
        json={
            "codex_model_id": "gpt-5-codex",
            "upstream_protocol": "gemini",
            "upstream_model_id": "deepseek-v3",
        },
    )
    assert no_protocol_route.status_code == 422

    # route exists for the protocol -> accepted
    route = client.post(
        "/api/admin/v1/routes",
        headers=admin_headers,
        json={"protocol": "openai_compatible", "requested_model_id": "deepseek-v3"},
    ).json()
    client.put(
        f"/api/admin/v1/routes/{route['id']}/candidates",
        headers=admin_headers,
        json={"candidates": [{"channel_model_id": model["id"], "priority": 0}]},
    )
    ok = client.post(
        "/api/admin/v1/codex-mappings",
        headers=admin_headers,
        json={
            "codex_model_id": "gpt-5-codex",
            "upstream_protocol": "openai_compatible",
            "upstream_model_id": "deepseek-v3",
        },
    )
    assert ok.status_code == 201
    assert ok.json()["upstream_model_id"] == "deepseek-v3"


def test_mapped_proxy_converts_responses_to_openai_and_back(client, admin_headers):
    create_mapping(client, admin_headers)
    original_body = {
        "model": "gpt-5-codex",
        "max_output_tokens": 1024,
        "instructions": "You are helpful",
        "input": [
            {"type": "message", "role": "user", "content": "hello"},
            {
                "type": "function_call",
                "call_id": "call_9",
                "name": "get_weather",
                "arguments": '{"city": "Beijing"}',
            },
            {
                "type": "function_call_output",
                "call_id": "call_9",
                "output": "sunny",
            },
        ],
        "stream": False,
    }
    seen = []

    def upstream(request):
        seen.append((request.headers.get("authorization"), request.content))
        return httpx.Response(
            200,
            json={
                "id": "chatcmpl-1",
                "model": "deepseek-v3",
                "choices": [
                    {
                        "index": 0,
                        "message": {
                            "role": "assistant",
                            "content": "Hello there",
                            "tool_calls": [
                                {
                                    "id": "call_9",
                                    "type": "function",
                                    "function": {
                                        "name": "get_weather",
                                        "arguments": '{"city": "Shanghai"}',
                                    },
                                }
                            ],
                        },
                        "finish_reason": "tool_calls",
                    }
                ],
                "usage": {"prompt_tokens": 12, "completion_tokens": 7},
            },
            headers={"content-type": "application/json"},
        )

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/chat/completions").mock(side_effect=upstream)
        response = client.post(
            "/codex/v1/responses",
            headers={
                "Authorization": "Bearer client-key",
                "Content-Type": "application/json",
            },
            content=json.dumps(original_body),
        )

    assert response.status_code == 200
    auth, body = seen[0]
    assert auth == "Bearer key-primary"
    converted = json.loads(body)
    assert converted["model"] == "deepseek-v3"
    assert converted["stream"] is False
    assert converted["max_tokens"] == 1024
    assert converted["messages"][0] == {"role": "system", "content": "You are helpful"}
    assert converted["messages"][1] == {"role": "user", "content": "hello"}
    assert converted["messages"][2]["role"] == "assistant"
    assert converted["messages"][2]["content"] is None
    assert converted["messages"][2]["tool_calls"][0]["function"]["name"] == "get_weather"
    assert converted["messages"][2]["tool_calls"][0]["function"]["arguments"] == '{"city": "Beijing"}'
    assert converted["messages"][3] == {
        "role": "tool",
        "tool_call_id": "call_9",
        "content": "sunny",
    }

    envelope = response.json()
    assert envelope["object"] == "response"
    assert envelope["model"] == "gpt-5-codex"
    assert envelope["status"] == "completed"
    msg = next(item for item in envelope["output"] if item["type"] == "message")
    assert msg["role"] == "assistant"
    assert msg["content"][0]["type"] == "output_text"
    assert msg["content"][0]["text"] == "Hello there"
    fc = next(item for item in envelope["output"] if item["type"] == "function_call")
    assert fc["name"] == "get_weather"
    assert fc["arguments"] == '{"city": "Shanghai"}'
    assert fc["status"] == "completed"
    assert envelope["usage"]["input_tokens"] == 12
    assert envelope["usage"]["output_tokens"] == 7
    assert response.headers["content-type"].startswith("application/json")

def test_mapped_proxy_normalizes_developer_role_for_chat_upstream(client, admin_headers):
    create_mapping(
        client,
        admin_headers,
        upstream_model="deepseek-v4-flash",
    )
    seen = []

    def upstream(request):
        seen.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "id": "chatcmpl-developer-role",
                "model": "deepseek-v4-flash",
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "ok"},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"prompt_tokens": 3, "completion_tokens": 1},
            },
            headers={"content-type": "application/json"},
        )

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/chat/completions").mock(side_effect=upstream)
        response = client.post(
            "/codex/v1/responses",
            headers={"Authorization": "Bearer client-key", "Content-Type": "application/json"},
            content=json.dumps(
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
            ),
        )

    assert response.status_code == 200
    assert seen[0]["messages"][0] == {
        "role": "system",
        "content": [{"type": "text", "text": "Follow project instructions"}],
    }
    assert seen[0]["messages"][1] == {
        "role": "user",
        "content": [{"type": "text", "text": "hello"}],
    }


def test_mapped_proxy_streams_converted_responses_sse(client, admin_headers):
    create_mapping(client, admin_headers)
    chunks = [
        b'data: {"id":"chatcmpl-2","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":"Hel"},"finish_reason":null}]}\n\n',
        b'data: {"id":"chatcmpl-2","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"lo"},"finish_reason":null}]}\n\n',
        b'data: {"id":"chatcmpl-2","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":" world"},"finish_reason":"stop"}]}\n\n',
        b'data: {"id":"chatcmpl-2","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":5,"completion_tokens":3}}\n\n',
        b"data: [DONE]\n\n",
    ]

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/chat/completions").mock(
            return_value=httpx.Response(
                200,
                stream=ChunkStream(chunks),
                headers={"content-type": "text/event-stream"},
            )
        )
        response = client.post(
            "/codex/v1/responses",
            headers={
                "Authorization": "Bearer client-key",
                "Content-Type": "application/json",
            },
            content=json.dumps(
                {
                    "model": "gpt-5-codex",
                    "max_output_tokens": 100,
                    "input": [{"type": "message", "role": "user", "content": "hi"}],
                    "stream": True,
                }
            ),
        )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    events = []
    for line in response.text.splitlines():
        if line.startswith("data:"):
            events.append(json.loads(line[5:].strip()))
    types = [event["type"] for event in events]
    assert types[0] == "response.created"
    assert types[-1] == "response.completed"
    assert "response.output_item.added" in types
    assert "response.output_text.delta" in types
    assert "response.output_text.done" in types
    assert "response.output_item.done" in types
    assert events[0]["response"]["model"] == "gpt-5-codex"
    assert events[0]["response"]["status"] == "in_progress"
    deltas = [e for e in events if e["type"] == "response.output_text.delta"]
    assert "".join(d["delta"] for d in deltas) == "Hello world"
    completed = [e for e in events if e["type"] == "response.completed"][-1]
    assert completed["response"]["status"] == "completed"
    assert completed["response"]["usage"]["output_tokens"] == 3

def test_mapped_proxy_stream_converts_deepseek_dsml_and_complete_usage(client, admin_headers):
    create_mapping(
        client,
        admin_headers,
        upstream_model="deepseek-v4-flash",
    )
    seen_upstream = []

    def delta(text):
        payload = {
            "id": "deepseek-v4-flash-stream",
            "choices": [
                {"index": 0, "delta": {"content": text}, "finish_reason": None}
            ],
        }
        return b"data: " + json.dumps(payload, ensure_ascii=False).encode() + b"\n\n"

    chunks = [
        delta("我先抓取官方页面。\n<｜｜DSM"),
        delta(
            'L｜｜tool_calls><｜｜DSML｜｜invoke name="functions.exec">'
            '<｜｜DSML｜｜parameter name="cmd" string="true">curl -sL https://archlinux.org/news/</｜｜DSML｜｜parameter>'
            "</｜｜DSML｜｜invoke></｜｜DSML｜｜tool_calls>"
        ),
        b'data: {"id":"deepseek-v4-flash-stream","choices":[],"usage":{"prompt_tokens":4,"completion_tokens":1}}\n\n',
        b"data: [DONE]\n\n",
    ]

    def upstream(request):
        seen_upstream.append(json.loads(request.content))
        return httpx.Response(
            200,
            stream=ChunkStream(chunks),
            headers={"content-type": "text/event-stream"},
        )

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/chat/completions").mock(side_effect=upstream)
        response = client.post(
            "/codex/v1/responses",
            headers={"Authorization": "Bearer client-key", "Content-Type": "application/json"},
            content=json.dumps(
                {
                    "model": "gpt-5-codex",
                    "input": [
                        {"type": "message", "role": "developer", "content": "Use tools"},
                        {"type": "message", "role": "user", "content": "查官方更新"},
                    ],
                    "tools": [
                        {
                            "type": "function",
                            "name": "functions.exec",
                            "description": "Run a command",
                            "parameters": {
                                "type": "object",
                                "properties": {"cmd": {"type": "string"}},
                                "required": ["cmd"],
                            },
                        }
                    ],
                    "stream": True,
                }
            ),
        )

    assert response.status_code == 200
    assert "<|DSML|" not in response.text
    assert [message["role"] for message in seen_upstream[0]["messages"]] == [
        "system",
        "user",
    ]
    events = [
        json.loads(line[5:].strip())
        for line in response.text.splitlines()
        if line.startswith("data:")
    ]
    sequence_numbers = [event["sequence_number"] for event in events]
    assert sequence_numbers == list(range(len(events)))
    visible_text = "".join(
        event["delta"]
        for event in events
        if event["type"] == "response.output_text.delta"
    )
    assert visible_text == "我先抓取官方页面。\n"
    completed = next(event for event in events if event["type"] == "response.completed")
    call = next(item for item in completed["response"]["output"] if item["type"] == "function_call")
    assert call["name"] == "functions.exec"
    assert json.loads(call["arguments"]) == {
        "cmd": "curl -sL https://archlinux.org/news/"
    }
    assert completed["response"]["usage"]["input_tokens_details"] == {
        "cached_tokens": 0
    }
    assert completed["response"]["usage"]["output_tokens_details"] == {
        "reasoning_tokens": 0
    }


def test_mapped_proxy_streams_tool_calls_with_id_then_index_deltas(client, admin_headers):
    """DeepSeek/GPT-5 style streaming: the first tool-call chunk carries the
    id, subsequent chunks only the index. The converted Responses stream must
    contain a single complete function_call item."""
    create_mapping(client, admin_headers)
    chunks = [
        b'data: {"id":"x","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_7","type":"function","function":{"name":"get_weather","arguments":""}}]},"finish_reason":null}]}\n\n',
        b'data: {"id":"x","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\\"city\\":\\"Bei"}}]},"finish_reason":null}]}\n\n',
        b'data: {"id":"x","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"jing\\"}"}}]},"finish_reason":null}]}\n\n',
        b'data: {"id":"x","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}\n\n',
        b"data: [DONE]\n\n",
    ]

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/chat/completions").mock(
            return_value=httpx.Response(
                200,
                stream=ChunkStream(chunks),
                headers={"content-type": "text/event-stream"},
            )
        )
        response = client.post(
            "/codex/v1/responses",
            headers={
                "Authorization": "Bearer client-key",
                "Content-Type": "application/json",
            },
            content=json.dumps(
                {
                    "model": "gpt-5-codex",
                    "input": [{"type": "message", "role": "user", "content": "weather?"}],
                    "stream": True,
                }
            ),
        )

    assert response.status_code == 200
    events = []
    for line in response.text.splitlines():
        if line.startswith("data:"):
            events.append(json.loads(line[5:].strip()))
    completed = [e for e in events if e["type"] == "response.completed"][-1]
    calls = [item for item in completed["response"]["output"] if item["type"] == "function_call"]
    assert len(calls) == 1
    assert calls[0]["name"] == "get_weather"
    assert calls[0]["call_id"] == "call_7"
    assert json.loads(calls[0]["arguments"]) == {"city": "Beijing"}
    assert completed["response"]["status"] == "completed"


def test_mapping_per_model_protocol_selection(client, admin_headers):
    # Mapping A -> openai_compatible, Mapping B -> claude passthrough
    create_mapping(client, admin_headers, codex_model_id="gpt-5-codex", upstream_protocol="openai_compatible")
    create_mapping(
        client,
        admin_headers,
        codex_model_id="claude-codex",
        upstream_protocol="claude",
        provider_name="Claude upstream",
        upstream_model="claude-sonnet-4-5",
        channel_name="claude-primary",
    )

    openai_seen = []
    claude_seen = []

    def openai_upstream(request):
        openai_seen.append(json.loads(request.content))
        return httpx.Response(200, json={"id": "c1", "choices": [{"index": 0, "message": {"role": "assistant", "content": "from openai"}, "finish_reason": "stop"}]})

    def claude_upstream(request):
        claude_seen.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "id": "msg_1",
                "type": "message",
                "role": "assistant",
                "model": "claude-sonnet-4-5",
                "content": [{"type": "text", "text": "from claude"}],
                "stop_reason": "end_turn",
                "stop_sequence": None,
                "usage": {"input_tokens": 1, "output_tokens": 1},
            },
        )

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/chat/completions").mock(side_effect=openai_upstream)
        mock.post("https://upstream.test/v1/messages").mock(side_effect=claude_upstream)
        r1 = client.post(
            "/codex/v1/responses",
            headers={"Authorization": "Bearer k", "Content-Type": "application/json"},
            content=json.dumps({"model": "gpt-5-codex", "input": [{"type": "message", "role": "user", "content": "hi"}]}),
        )
        r2 = client.post(
            "/codex/v1/responses",
            headers={"Authorization": "Bearer k", "Content-Type": "application/json"},
            content=json.dumps({"model": "claude-codex", "input": [{"type": "message", "role": "user", "content": "hi"}]}),
        )

    assert r1.status_code == 200
    envelope1 = r1.json()
    assert next(i for i in envelope1["output"] if i["type"] == "message")["content"][0]["text"] == "from openai"
    assert openai_seen[0]["model"] == "deepseek-v3"
    assert "system" not in openai_seen[0]
    assert r2.status_code == 200
    envelope2 = r2.json()
    assert next(i for i in envelope2["output"] if i["type"] == "message")["content"][0]["text"] == "from claude"
    # claude passthrough: upstream receives the *upstream* model name
    assert claude_seen[0]["model"] == "claude-sonnet-4-5"


def test_mapped_proxy_gemini_conversion(client, admin_headers):
    create_mapping(
        client,
        admin_headers,
        codex_model_id="gemini-codex",
        upstream_protocol="gemini",
        upstream_model="gemini-2.5-pro",
    )
    seen = []

    def gemini_upstream(request):
        seen.append((request.url.path, json.loads(request.content)))
        return httpx.Response(
            200,
            json={
                "candidates": [
                    {
                        "content": {"role": "model", "parts": [{"text": "gemini says hi"}]},
                        "finishReason": "STOP",
                    }
                ],
                "usageMetadata": {"promptTokenCount": 9, "candidatesTokenCount": 4},
            },
        )

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1beta/models/gemini-2.5-pro:generateContent").mock(
            side_effect=gemini_upstream
        )
        response = client.post(
            "/codex/v1/responses",
            headers={"Authorization": "Bearer k", "Content-Type": "application/json"},
            content=json.dumps(
                {
                    "model": "gemini-codex",
                    "max_output_tokens": 512,
                    "instructions": "Be concise",
                    "input": [{"type": "message", "role": "user", "content": "hi"}],
                }
            ),
        )

    assert response.status_code == 200
    path, body = seen[0]
    assert path == "/v1beta/models/gemini-2.5-pro:generateContent"
    assert body["contents"][0] == {"role": "user", "parts": [{"text": "hi"}]}
    assert body["systemInstruction"] == {"parts": [{"text": "Be concise"}]}
    assert body["generationConfig"]["maxOutputTokens"] == 512
    envelope = response.json()
    msg = next(i for i in envelope["output"] if i["type"] == "message")
    assert msg["content"][0]["text"] == "gemini says hi"
    assert envelope["status"] == "completed"
    assert envelope["usage"]["input_tokens"] == 9
    assert envelope["usage"]["output_tokens"] == 4


def test_mapped_proxy_error_conversion_and_failover(client, admin_headers):
    provider = client.post(
        "/api/admin/v1/providers",
        headers=admin_headers,
        json={"name": "Failover upstream", "base_url": "https://upstream.test"},
    ).json()
    configured = []
    for name, key in (("primary", "key-primary"), ("backup", "key-backup")):
        channel = client.post(
            "/api/admin/v1/channels",
            headers=admin_headers,
            json={
                "provider_id": provider["id"],
                "name": name,
                "protocol": "openai_compatible",
                "api_key": key,
            },
        ).json()
        model = client.post(
            f"/api/admin/v1/channels/{channel['id']}/models",
            headers=admin_headers,
            json={"model_id": "deepseek-v3"},
        ).json()
        configured.append((channel, model))
    route = client.post(
        "/api/admin/v1/routes",
        headers=admin_headers,
        json={"protocol": "openai_compatible", "requested_model_id": "deepseek-v3"},
    ).json()
    response = client.put(
        f"/api/admin/v1/routes/{route['id']}/candidates",
        headers=admin_headers,
        json={
            "candidates": [
                {"channel_model_id": configured[0][1]["id"], "priority": 0},
                {"channel_model_id": configured[1][1]["id"], "priority": 1},
            ]
        },
    )
    assert response.status_code == 200
    mapping = client.post(
        "/api/admin/v1/codex-mappings",
        headers=admin_headers,
        json={
            "codex_model_id": "gpt-5-codex",
            "upstream_protocol": "openai_compatible",
            "upstream_model_id": "deepseek-v3",
        },
    )
    assert mapping.status_code == 201, mapping.text

    seen = []

    def upstream(request):
        seen.append(request.headers["authorization"])
        if request.headers["authorization"] == "Bearer key-primary":
            return httpx.Response(
                500, content=b'{"error":{"message":"boom","type":"server_error"}}',
                headers={"content-type": "application/json"},
            )
        return httpx.Response(
            200,
            json={"id": "c2", "choices": [{"index": 0, "message": {"role": "assistant", "content": "recovered"}, "finish_reason": "stop"}]},
        )

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/chat/completions").mock(side_effect=upstream)
        response = client.post(
            "/codex/v1/responses",
            headers={"Authorization": "Bearer k", "Content-Type": "application/json"},
            content=json.dumps({"model": "gpt-5-codex", "input": [{"type": "message", "role": "user", "content": "hi"}]}),
        )

    assert response.status_code == 200
    envelope = response.json()
    assert next(i for i in envelope["output"] if i["type"] == "message")["content"][0]["text"] == "recovered"
    assert seen == ["Bearer key-primary", "Bearer key-backup"]


def test_mapped_proxy_upstream_error_returns_codex_error(client, admin_headers):
    create_mapping(client, admin_headers)
    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/chat/completions").mock(
            return_value=httpx.Response(
                429,
                content=b'{"error":{"message":"rate limited","type":"rate_limit_error"}}',
                headers={"content-type": "application/json"},
            )
        )
        response = client.post(
            "/codex/v1/responses",
            headers={"Authorization": "Bearer k", "Content-Type": "application/json"},
            content=json.dumps({"model": "gpt-5-codex", "input": [{"type": "message", "role": "user", "content": "hi"}]}),
        )

    assert response.status_code == 429
    payload = response.json()
    assert payload["error"]["type"] == "rate_limit_error"
    assert payload["error"]["message"] == "rate limited"
    assert payload["error"]["code"] == "rate_limit_error"
    assert payload["error"]["param"] is None


def test_disabled_mapping_returns_unknown_model(client, admin_headers):
    _, _, _, mapping, _ = create_mapping(client, admin_headers)
    client.patch(
        f"/api/admin/v1/codex-mappings/{mapping['id']}",
        headers=admin_headers,
        json={"enabled": False},
    )
    response = client.post(
        "/codex/v1/responses",
        headers={"Authorization": "Bearer k", "Content-Type": "application/json"},
        content=json.dumps({"model": "gpt-5-codex", "input": [{"type": "message", "role": "user", "content": "hi"}]}),
    )
    assert response.status_code == 404
    payload = response.json()
    assert payload["error"]["type"] == "gateway_error"
    assert payload["error"]["code"] == "unknown_mapped_model"
    assert "request_id" in payload


def test_regular_responses_not_affected_by_mappings(client, admin_headers):
    """Mapped models are only served under /codex; the regular /v1/responses
    endpoint must not intercept them (falls through to normal routing)."""
    create_mapping(client, admin_headers)
    response = client.post(
        "/v1/responses",
        headers={"Authorization": "Bearer k", "Content-Type": "application/json"},
        content=json.dumps({"model": "gpt-5-codex", "input": [{"type": "message", "role": "user", "content": "hi"}]}),
    )
    assert response.status_code == 503


def test_mapping_logs_include_upstream_info(client, admin_headers):
    create_mapping(client, admin_headers)
    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/chat/completions").mock(
            return_value=httpx.Response(
                200,
                json={"id": "c1", "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}]},
            )
        )
        response = client.post(
            "/codex/v1/responses",
            headers={"Authorization": "Bearer k", "Content-Type": "application/json"},
            content=json.dumps({"model": "gpt-5-codex", "input": [{"type": "message", "role": "user", "content": "hi"}]}),
        )
    assert response.status_code == 200

    import time

    time.sleep(0.05)
    all_logs = client.get("/api/admin/v1/requests", headers=admin_headers).json()
    logs = all_logs["items"]
    assert logs[0]["model_id"] == "gpt-5-codex"
    assert logs[0]["protocol"] == "openai_responses"
    assert logs[0]["upstream_protocol"] == "openai_compatible"
    assert logs[0]["upstream_model_id"] == "deepseek-v3"
    assert logs[0]["response_channels"] == ["primary"]

    detail = client.get(f"/api/admin/v1/requests/{logs[0]['id']}", headers=admin_headers).json()
    attempt = detail["attempts"][0]
    assert attempt["upstream_protocol"] == "openai_compatible"
    assert attempt["upstream_model_id"] == "deepseek-v3"

    # filter by upstream model / upstream protocol
    filtered = client.get(
        "/api/admin/v1/requests?upstream_model_id=deepseek-v3",
        headers=admin_headers,
    ).json()
    assert filtered["total"] >= 1
    assert all(
        item["upstream_model_id"] == "deepseek-v3" for item in filtered["items"]
    )
    filtered_protocol = client.get(
        "/api/admin/v1/requests?upstream_protocol=openai_compatible",
        headers=admin_headers,
    ).json()
    assert filtered_protocol["total"] >= 1
    assert all(
        item["upstream_protocol"] == "openai_compatible"
        for item in filtered_protocol["items"]
    )


def test_mapping_usage_not_double_counted(client, admin_headers):
    """Mapped requests must record usage exactly once in the dashboard stats:
    one non-streaming request + one streaming request + one failover request.
    """
    create_mapping(client, admin_headers)
    # add a second candidate channel for the failover case
    provider = client.get("/api/admin/v1/providers", headers=admin_headers).json()["items"][0]
    channel = client.post(
        "/api/admin/v1/channels",
        headers=admin_headers,
        json={
            "provider_id": provider["id"],
            "name": "backup",
            "protocol": "openai_compatible",
            "api_key": "key-backup",
        },
    ).json()
    model = client.post(
        f"/api/admin/v1/channels/{channel['id']}/models",
        headers=admin_headers,
        json={"model_id": "deepseek-v3"},
    ).json()
    # add a second candidate to the upstream model's route
    routes = client.get("/api/admin/v1/routes", headers=admin_headers).json()["items"]
    route = next(r for r in routes if r["requested_model_id"] == "deepseek-v3")

    # candidate channel_model ids: find from channel-models
    models = client.get("/api/admin/v1/channel-models", headers=admin_headers).json()["items"]
    primary_cm = next(m["id"] for m in models if m["channel_name"] == "primary")
    backup_cm = next(m["id"] for m in models if m["channel_name"] == "backup")
    client.put(
        f"/api/admin/v1/routes/{route['id']}/candidates",
        headers=admin_headers,
        json={
            "candidates": [
                {"channel_model_id": primary_cm, "priority": 0},
                {"channel_model_id": backup_cm, "priority": 1},
            ]
        },
    )

    # 1) non-streaming mapped request, upstream reports 10 input / 5 output tokens
    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/chat/completions").mock(
            return_value=httpx.Response(
                200,
                json={
                    "id": "c1",
                    "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
                    "usage": {
                        "prompt_tokens": 10,
                        "completion_tokens": 5,
                        "prompt_tokens_details": {"cached_tokens": 4},
                    },
                },
            )
        )
        response = client.post(
            "/codex/v1/responses",
            headers={"Authorization": "Bearer k", "Content-Type": "application/json"},
            content=json.dumps({"model": "gpt-5-codex", "input": [{"type": "message", "role": "user", "content": "hi"}]}),
        )
        assert response.status_code == 200

    # 2) streaming mapped request, upstream reports 6 input / 3 output tokens
    chunks = [
        b'data: {"id":"c2","choices":[{"index":0,"delta":{"content":"Hel"},"finish_reason":null}]}\n\n',
        b'data: {"id":"c2","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\n',
        b'data: {"id":"c2","choices":[],"usage":{"prompt_tokens":6,"completion_tokens":3}}\n\n',
        b"data: [DONE]\n\n",
    ]
    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/chat/completions").mock(
            return_value=httpx.Response(
                200,
                stream=ChunkStream(chunks),
                headers={"content-type": "text/event-stream"},
            )
        )
        response = client.post(
            "/codex/v1/responses",
            headers={"Authorization": "Bearer k", "Content-Type": "application/json"},
            content=json.dumps({"model": "gpt-5-codex", "input": [{"type": "message", "role": "user", "content": "hi"}], "stream": True}),
        )
        assert response.status_code == 200

    # 3) failover mapped request: primary fails with 500 (no usage), backup succeeds with 4 output tokens
    def failover_upstream(request):
        if request.headers["authorization"] == "Bearer key-primary":
            return httpx.Response(500, content=b'{"error":{"message":"boom"}}')
        return httpx.Response(
            200,
            json={"id": "c3", "choices": [{"index": 0, "message": {"role": "assistant", "content": "recovered"}, "finish_reason": "stop"}], "usage": {"prompt_tokens": 8, "completion_tokens": 4}},
        )

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/chat/completions").mock(side_effect=failover_upstream)
        response = client.post(
            "/codex/v1/responses",
            headers={"Authorization": "Bearer k", "Content-Type": "application/json"},
            content=json.dumps({"model": "gpt-5-codex", "input": [{"type": "message", "role": "user", "content": "hi"}]}),
        )
        assert response.status_code == 200

    import time

    time.sleep(0.1)
    stats = client.get("/api/admin/v1/stats/summary", headers=admin_headers).json()
    assert stats["requests"] == 3
    # each request contributes its usage exactly once (no double counting)
    assert stats["output_tokens"] == 5 + 3 + 4
    # the first request reported 4 cached input tokens (10 total, 4 cached, 6 miss)
    assert stats["cache_read_tokens"] == 4
    assert (stats["cache_write_tokens"] or 0) == 0
    assert stats["cache_miss_input_tokens"] == 6

    # per-request detail: exactly 3 attempts with usage across 3 requests (one per request)
    logs = client.get("/api/admin/v1/requests", headers=admin_headers).json()["items"]
    assert len(logs) == 3
    for item in logs:
        detail = client.get(f"/api/admin/v1/requests/{item['id']}", headers=admin_headers).json()
        usages = [a for a in detail["attempts"] if (a["output_tokens"] or 0) > 0]
        assert len(usages) == 1  # exactly one attempt with usage per request


def test_mapping_failed_attempt_usage_not_counted(client, admin_headers):
    """If the first mapped attempt parses a usage event but dies before any
    response is delivered (prelude transport error), its tokens must NOT be
    counted: the dashboard should only reflect the successful attempt."""
    provider = client.post(
        "/api/admin/v1/providers",
        headers=admin_headers,
        json={"name": "Failover upstream", "base_url": "https://upstream.test"},
    ).json()
    configured = []
    for name, key in (("primary", "key-primary"), ("backup", "key-backup")):
        channel = client.post(
            "/api/admin/v1/channels",
            headers=admin_headers,
            json={
                "provider_id": provider["id"],
                "name": name,
                "protocol": "openai_compatible",
                "api_key": key,
            },
        ).json()
        model = client.post(
            f"/api/admin/v1/channels/{channel['id']}/models",
            headers=admin_headers,
            json={"model_id": "deepseek-v3"},
        ).json()
        configured.append((channel, model))
    route = client.post(
        "/api/admin/v1/routes",
        headers=admin_headers,
        json={"protocol": "openai_compatible", "requested_model_id": "deepseek-v3"},
    ).json()
    client.put(
        f"/api/admin/v1/routes/{route['id']}/candidates",
        headers=admin_headers,
        json={
            "candidates": [
                {"channel_model_id": configured[0][1]["id"], "priority": 0},
                {"channel_model_id": configured[1][1]["id"], "priority": 1},
            ]
        },
    )
    client.post(
        "/api/admin/v1/codex-mappings",
        headers=admin_headers,
        json={
            "codex_model_id": "gpt-5-codex",
            "upstream_protocol": "openai_compatible",
            "upstream_model_id": "deepseek-v3",
        },
    )

    class UsageThenErrorStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'data: {"id":"c","choices":[],"usage":{"prompt_tokens":100,"completion_tokens":1}}\n\n'
            raise httpx.ConnectError("connection reset mid-prelude")

    def upstream(request):
        if request.headers["authorization"] == "Bearer key-primary":
            return httpx.Response(
                200,
                stream=UsageThenErrorStream(),
                headers={"content-type": "text/event-stream"},
            )
        return httpx.Response(
            200,
            json={
                "id": "c2",
                "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 8, "completion_tokens": 4},
            },
        )

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/chat/completions").mock(side_effect=upstream)
        response = client.post(
            "/codex/v1/responses",
            headers={"Authorization": "Bearer k", "Content-Type": "application/json"},
            content=json.dumps({"model": "gpt-5-codex", "input": [{"type": "message", "role": "user", "content": "hi"}]}),
        )
    assert response.status_code == 200
    envelope = response.json()
    assert next(i for i in envelope["output"] if i["type"] == "message")["content"][0]["text"] == "ok"

    import time

    time.sleep(0.1)
    stats = client.get("/api/admin/v1/stats/summary", headers=admin_headers).json()
    # only the successful attempt's usage counts: 4 output tokens, 8 input (no cache)
    assert stats["requests"] == 1
    assert stats["output_tokens"] == 4
    assert (stats["cache_miss_input_tokens"] or 0) == 0

    # the failed attempt must not carry token values either
    logs = client.get("/api/admin/v1/requests", headers=admin_headers).json()["items"]
    detail = client.get(f"/api/admin/v1/requests/{logs[0]['id']}", headers=admin_headers).json()
    failed = next(a for a in detail["attempts"] if a["outcome"] == "transport_error")
    assert failed["response_started"] is False
    assert failed["output_tokens"] is None
    assert failed["input_tokens"] is None
    # raw usage from the dead stream is kept only for debugging
    assert (failed["raw_usage_json"] or {}).get("prompt_tokens") == 100


def test_codex_presets_endpoint(client, admin_headers):
    response = client.get("/api/admin/v1/codex-presets", headers=admin_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "defaults"
    ids = [item["id"] for item in payload["items"]]
    assert "gpt-5-codex" in ids

    queued = client.post(
        "/api/admin/v1/codex-presets/refresh", headers=admin_headers
    )
    assert queued.status_code == 202
    assert queued.json()["status"] == "queued"


def test_mapping_inherits_route_candidates_immediately(client, admin_headers):
    """The mapping inherits candidates from the upstream model's route; route
    changes take effect immediately (real-time)."""
    provider, channel, model, mapping, route = create_mapping(client, admin_headers)

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/chat/completions").mock(
            return_value=httpx.Response(
                200,
                json={"id": "c1", "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}]},
            )
        )
        response = client.post(
            "/codex/v1/responses",
            headers={"Authorization": "Bearer k", "Content-Type": "application/json"},
            content=json.dumps({"model": "gpt-5-codex", "input": [{"type": "message", "role": "user", "content": "hi"}]}),
        )
    assert response.status_code == 200
    envelope = response.json()
    assert next(i for i in envelope["output"] if i["type"] == "message")["content"][0]["text"] == "ok"

    # disable the upstream model's route -> mapping stops resolving (real-time)
    client.patch(f"/api/admin/v1/routes/{route['id']}", headers=admin_headers, json={"enabled": False})
    response = client.post(
        "/codex/v1/responses",
        headers={"Authorization": "Bearer k", "Content-Type": "application/json"},
        content=json.dumps({"model": "gpt-5-codex", "input": [{"type": "message", "role": "user", "content": "hi"}]}),
    )
    assert response.status_code == 503

    # re-enable -> works again immediately
    client.patch(f"/api/admin/v1/routes/{route['id']}", headers=admin_headers, json={"enabled": True})
    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/chat/completions").mock(
            return_value=httpx.Response(
                200,
                json={"id": "c1", "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok again"}, "finish_reason": "stop"}]},
            )
        )
        response = client.post(
            "/codex/v1/responses",
            headers={"Authorization": "Bearer k", "Content-Type": "application/json"},
            content=json.dumps({"model": "gpt-5-codex", "input": [{"type": "message", "role": "user", "content": "hi"}]}),
        )
    assert response.status_code == 200
    envelope = response.json()
    assert next(i for i in envelope["output"] if i["type"] == "message")["content"][0]["text"] == "ok again"
