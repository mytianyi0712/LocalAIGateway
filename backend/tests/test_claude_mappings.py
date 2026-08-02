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
    claude_model_id="claude-opus-5",
    upstream_protocol="openai_compatible",
    provider_name="Mock upstream",
    upstream_model="deepseek-v3",
    channel_name="primary",
    api_key="key-primary",
    with_route=True,
):
    """Create a provider + channel + channel model + route + mapping.

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
        "/api/admin/v1/claude-mappings",
        headers=admin_headers,
        json={
            "claude_model_id": claude_model_id,
            "display_name": "Claude Opus 5",
            "upstream_protocol": upstream_protocol,
            "upstream_model_id": upstream_model,
        },
    )
    assert mapping.status_code == 201, mapping.text
    return provider, channel, model, mapping.json(), route


def test_mapping_admin_crud_and_catalog(client, admin_headers):
    _, _, _, mapping, _ = create_mapping(client, admin_headers)

    # list
    items = client.get("/api/admin/v1/claude-mappings", headers=admin_headers).json()["items"]
    assert len(items) == 1
    assert items[0]["claude_model_id"] == "claude-opus-5"
    assert items[0]["upstream_protocol"] == "openai_compatible"
    assert items[0]["upstream_model_id"] == "deepseek-v3"
    assert len(items[0]["candidates"]) == 1
    assert items[0]["candidates"][0]["model_id"] == "deepseek-v3"

    # patch display name only
    patched = client.patch(
        f"/api/admin/v1/claude-mappings/{mapping['id']}",
        headers=admin_headers,
        json={"display_name": "Renamed"},
    )
    assert patched.status_code == 200
    assert patched.json()["display_name"] == "Renamed"

    # invalid protocol rejected
    bad = client.patch(
        f"/api/admin/v1/claude-mappings/{mapping['id']}",
        headers=admin_headers,
        json={"upstream_protocol": "bogus"},
    )
    assert bad.status_code == 422

    # duplicate name rejected
    duplicate = client.post(
        "/api/admin/v1/claude-mappings",
        headers=admin_headers,
        json={
            "claude_model_id": "claude-opus-5",
            "upstream_protocol": "openai_compatible",
            "upstream_model_id": "deepseek-v3",
        },
    )
    assert duplicate.status_code == 409

    # /claudecode catalog exposes the mapped model (Claude Code probing entry)
    catalog = client.get("/claudecode/v1/models", headers={"anthropic-version": "2023-06-01"})
    assert catalog.status_code == 200
    ids = [item["id"] for item in catalog.json()["data"]]
    assert "claude-opus-5" in ids
    item = next(item for item in catalog.json()["data"] if item["id"] == "claude-opus-5")
    assert item["display_name"] == "Renamed"
    assert item["x_local_gateway"]["mapping"]["upstream_protocol"] == "openai_compatible"
    assert item["x_local_gateway"]["mapping"]["upstream_model_id"] == "deepseek-v3"

    # regular gateway catalog must NOT include mapped models
    regular = client.get("/v1/models", headers={"anthropic-version": "2023-06-01"})
    regular_ids = [item["id"] for item in regular.json()["data"]]
    assert "claude-opus-5" not in regular_ids

    # claudecode info endpoint
    info = client.get("/claudecode")
    assert info.status_code == 200
    assert info.json()["endpoints"] == [
        "GET /claudecode/v1/models",
        "GET /claudecode/v1/messages/models",
        "POST /claudecode/v1/messages",
    ]

    # delete
    deleted = client.delete(f"/api/admin/v1/claude-mappings/{mapping['id']}", headers=admin_headers)
    assert deleted.status_code == 204
    catalog = client.get("/claudecode/v1/models", headers={"anthropic-version": "2023-06-01"})
    ids = [item["id"] for item in catalog.json()["data"]]
    assert "claude-opus-5" not in ids


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
        "/api/admin/v1/claude-mappings",
        headers=admin_headers,
        json={
            "claude_model_id": "claude-opus-5",
            "upstream_protocol": "openai_compatible",
            "upstream_model_id": "no-such-model",
        },
    )
    assert missing.status_code == 422

    # model exists but has no route for the protocol -> rejected
    no_route = client.post(
        "/api/admin/v1/claude-mappings",
        headers=admin_headers,
        json={
            "claude_model_id": "claude-opus-5",
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
        "/api/admin/v1/claude-mappings",
        headers=admin_headers,
        json={
            "claude_model_id": "claude-opus-5",
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
        "/api/admin/v1/claude-mappings",
        headers=admin_headers,
        json={
            "claude_model_id": "claude-opus-5",
            "upstream_protocol": "openai_compatible",
            "upstream_model_id": "deepseek-v3",
        },
    )
    assert ok.status_code == 201
    assert ok.json()["upstream_model_id"] == "deepseek-v3"


def test_mapped_proxy_converts_to_openai_and_back(client, admin_headers):
    create_mapping(client, admin_headers)
    original_body = {
        "model": "claude-opus-5",
        "max_tokens": 1024,
        "system": "You are helpful",
        "messages": [
            {"role": "user", "content": "hello"},
            {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "Let me check"},
                    {
                        "type": "tool_use",
                        "id": "toolu_01",
                        "name": "get_weather",
                        "input": {"city": "Beijing"},
                    },
                ],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "toolu_01",
                        "content": "sunny",
                    }
                ],
            },
        ],
        "stream": False,
    }
    upstream_body = {"model": "deepseek-v3", "messages": [], "choices": []}
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
            "/claudecode/v1/messages",
            headers={
                "anthropic-version": "2023-06-01",
                "x-api-key": "client-key",
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
    assert converted["messages"][2]["content"] == "Let me check"
    assert converted["messages"][2]["tool_calls"][0]["function"]["name"] == "get_weather"
    assert converted["messages"][3] == {
        "role": "tool",
        "tool_call_id": "toolu_01",
        "content": "sunny",
    }

    claude = response.json()
    assert claude["type"] == "message"
    assert claude["model"] == "claude-opus-5"
    assert claude["stop_reason"] == "tool_use"
    assert claude["content"][0] == {"type": "text", "text": "Hello there"}
    assert claude["content"][1]["type"] == "tool_use"
    assert claude["content"][1]["name"] == "get_weather"
    assert claude["content"][1]["input"] == {"city": "Shanghai"}
    assert claude["usage"]["input_tokens"] == 12
    assert claude["usage"]["output_tokens"] == 7
    assert response.headers["content-type"].startswith("application/json")


def test_mapped_proxy_streams_converted_claude_sse(client, admin_headers):
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
            "/claudecode/v1/messages",
            headers={
                "anthropic-version": "2023-06-01",
                "x-api-key": "client-key",
                "Content-Type": "application/json",
            },
            content=json.dumps(
                {
                    "model": "claude-opus-5",
                    "max_tokens": 100,
                    "messages": [{"role": "user", "content": "hi"}],
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
    assert types[0] == "message_start"
    assert types[-1] == "message_stop"
    assert "content_block_start" in types
    assert "content_block_delta" in types
    assert "message_delta" in types
    assert events[0]["message"]["model"] == "claude-opus-5"
    deltas = [e for e in events if e["type"] == "content_block_delta"]
    assert "".join(d["delta"]["text"] for d in deltas if d["delta"]["type"] == "text_delta") == "Hello world"
    final = [e for e in events if e["type"] == "message_delta"][-1]
    assert final["delta"]["stop_reason"] == "end_turn"
    assert final["usage"]["output_tokens"] == 3


def test_mapping_per_model_protocol_selection(client, admin_headers):
    # Mapping A -> openai_compatible, Mapping B -> claude passthrough
    create_mapping(client, admin_headers, claude_model_id="claude-opus-5", upstream_protocol="openai_compatible")
    provider, channel, model, mapping_b, _ = create_mapping(
        client,
        admin_headers,
        claude_model_id="claude-sonnet-5",
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
            "/claudecode/v1/messages",
            headers={"x-api-key": "k", "Content-Type": "application/json"},
            content=json.dumps({"model": "claude-opus-5", "messages": [{"role": "user", "content": "hi"}]}),
        )
        r2 = client.post(
            "/claudecode/v1/messages",
            headers={"x-api-key": "k", "Content-Type": "application/json"},
            content=json.dumps({"model": "claude-sonnet-5", "messages": [{"role": "user", "content": "hi"}]}),
        )

    assert r1.status_code == 200
    assert r1.json()["content"][0]["text"] == "from openai"
    assert openai_seen[0]["model"] == "deepseek-v3"
    assert "system" not in openai_seen[0]
    assert r2.status_code == 200
    assert r2.json()["content"][0]["text"] == "from claude"
    # passthrough: upstream receives the *upstream* model name, response passes through
    assert claude_seen[0]["model"] == "claude-sonnet-4-5"
    assert r2.json()["model"] == "claude-sonnet-4-5"


def test_mapped_proxy_gemini_conversion(client, admin_headers):
    create_mapping(
        client,
        admin_headers,
        claude_model_id="claude-fable-5",
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
            "/claudecode/v1/messages",
            headers={"x-api-key": "k", "Content-Type": "application/json"},
            content=json.dumps(
                {
                    "model": "claude-fable-5",
                    "max_tokens": 512,
                    "system": "Be concise",
                    "messages": [{"role": "user", "content": "hi"}],
                }
            ),
        )

    assert response.status_code == 200
    path, body = seen[0]
    assert path == "/v1beta/models/gemini-2.5-pro:generateContent"
    assert body["contents"][0] == {"role": "user", "parts": [{"text": "hi"}]}
    assert body["systemInstruction"] == {"parts": [{"text": "Be concise"}]}
    assert body["generationConfig"]["maxOutputTokens"] == 512
    claude = response.json()
    assert claude["content"][0]["text"] == "gemini says hi"
    assert claude["stop_reason"] == "end_turn"
    assert claude["usage"]["input_tokens"] == 9
    assert claude["usage"]["output_tokens"] == 4


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
        "/api/admin/v1/claude-mappings",
        headers=admin_headers,
        json={
            "claude_model_id": "claude-opus-5",
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
            "/claudecode/v1/messages",
            headers={"x-api-key": "k", "Content-Type": "application/json"},
            content=json.dumps({"model": "claude-opus-5", "messages": [{"role": "user", "content": "hi"}]}),
        )

    assert response.status_code == 200
    assert response.json()["content"][0]["text"] == "recovered"
    assert seen == ["Bearer key-primary", "Bearer key-backup"]


def test_mapped_proxy_upstream_error_returns_claude_error(client, admin_headers):
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
            "/claudecode/v1/messages",
            headers={"x-api-key": "k", "Content-Type": "application/json"},
            content=json.dumps({"model": "claude-opus-5", "messages": [{"role": "user", "content": "hi"}]}),
        )

    assert response.status_code == 429
    payload = response.json()
    assert payload["type"] == "error"
    assert payload["error"]["type"] == "rate_limit_error"
    assert payload["error"]["message"] == "rate limited"


def test_disabled_mapping_returns_unknown_model(client, admin_headers):
    _, _, _, mapping, _ = create_mapping(client, admin_headers)
    client.patch(
        f"/api/admin/v1/claude-mappings/{mapping['id']}",
        headers=admin_headers,
        json={"enabled": False},
    )
    response = client.post(
        "/claudecode/v1/messages",
        headers={"x-api-key": "k", "Content-Type": "application/json"},
        content=json.dumps({"model": "claude-opus-5", "messages": [{"role": "user", "content": "hi"}]}),
    )
    assert response.status_code == 404


def test_regular_messages_not_affected_by_mappings(client, admin_headers):
    """Mapped models are only served under /claudecode; the regular /v1/messages
    endpoint must not intercept them (falls through to normal routing)."""
    create_mapping(client, admin_headers)
    response = client.post(
        "/v1/messages",
        headers={"x-api-key": "k", "Content-Type": "application/json"},
        content=json.dumps({"model": "claude-opus-5", "messages": [{"role": "user", "content": "hi"}]}),
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
            "/claudecode/v1/messages",
            headers={"x-api-key": "k", "Content-Type": "application/json"},
            content=json.dumps({"model": "claude-opus-5", "messages": [{"role": "user", "content": "hi"}]}),
        )
    assert response.status_code == 200

    import time

    time.sleep(0.05)
    all_logs = client.get("/api/admin/v1/requests", headers=admin_headers).json()
    logs_total = all_logs["total"]
    logs = all_logs["items"]
    assert logs[0]["model_id"] == "claude-opus-5"
    assert logs[0]["protocol"] == "claude"
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
    mappings = client.get("/api/admin/v1/claude-mappings", headers=admin_headers).json()["items"]
    mapping = mappings[0]
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
            "/claudecode/v1/messages",
            headers={"x-api-key": "k", "Content-Type": "application/json"},
            content=json.dumps({"model": "claude-opus-5", "messages": [{"role": "user", "content": "hi"}]}),
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
            "/claudecode/v1/messages",
            headers={"x-api-key": "k", "Content-Type": "application/json"},
            content=json.dumps({"model": "claude-opus-5", "messages": [{"role": "user", "content": "hi"}], "stream": True}),
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
            "/claudecode/v1/messages",
            headers={"x-api-key": "k", "Content-Type": "application/json"},
            content=json.dumps({"model": "claude-opus-5", "messages": [{"role": "user", "content": "hi"}]}),
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
        "/api/admin/v1/claude-mappings",
        headers=admin_headers,
        json={
            "claude_model_id": "claude-opus-5",
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
            "/claudecode/v1/messages",
            headers={"x-api-key": "k", "Content-Type": "application/json"},
            content=json.dumps({"model": "claude-opus-5", "messages": [{"role": "user", "content": "hi"}]}),
        )
    assert response.status_code == 200
    assert response.json()["content"][0]["text"] == "ok"

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


def test_claude_presets_endpoint(client, admin_headers):
    response = client.get("/api/admin/v1/claude-presets", headers=admin_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "defaults"
    ids = [item["id"] for item in payload["items"]]
    assert "claude-opus-5" in ids
    assert "claude-fable-5" in ids
    assert "claude-sonnet-5" in ids
    assert "claude-mythos-5" in ids
    assert "claude-haiku-5" in ids

    queued = client.post(
        "/api/admin/v1/claude-presets/refresh", headers=admin_headers
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
            "/claudecode/v1/messages",
            headers={"x-api-key": "k", "Content-Type": "application/json"},
            content=json.dumps({"model": "claude-opus-5", "messages": [{"role": "user", "content": "hi"}]}),
        )
    assert response.status_code == 200
    assert response.json()["content"][0]["text"] == "ok"

    # disable the upstream model's route -> mapping stops resolving (real-time)
    client.patch(f"/api/admin/v1/routes/{route['id']}", headers=admin_headers, json={"enabled": False})
    response = client.post(
        "/claudecode/v1/messages",
        headers={"x-api-key": "k", "Content-Type": "application/json"},
        content=json.dumps({"model": "claude-opus-5", "messages": [{"role": "user", "content": "hi"}]}),
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
            "/claudecode/v1/messages",
            headers={"x-api-key": "k", "Content-Type": "application/json"},
            content=json.dumps({"model": "claude-opus-5", "messages": [{"role": "user", "content": "hi"}]}),
        )
    assert response.status_code == 200
    assert response.json()["content"][0]["text"] == "ok again"
