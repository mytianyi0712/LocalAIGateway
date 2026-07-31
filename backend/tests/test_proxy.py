import asyncio
import json

import httpx
import pytest
import respx

from app.services.health import probe_channel
from tests.conftest import configure_route


class ChunkStream(httpx.AsyncByteStream):
    def __init__(self, chunks, error=None):
        self.chunks = chunks
        self.error = error

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk
        if self.error:
            raise self.error


class MetadataThenDelayedTokenStream(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield b'data: {"type":"response.created","response":{"status":"in_progress"}}\n\n'
        await asyncio.sleep(0.1)
        yield b'data: {"type":"response.output_text.delta","delta":"late"}\n\n'


def test_same_protocol_failover_preserves_request_and_success_bytes(client, admin_headers):
    configure_route(client, admin_headers)
    original_body = b'{"model":"model-x","messages":[{"role":"user","content":"hello"}]}'
    success_body = b'{"result":"raw-success","spacing":  true}'
    seen = []

    def upstream(request):
        seen.append((request.headers["authorization"], request.content))
        if request.headers["authorization"] == "Bearer key-primary":
            return httpx.Response(
                500, content=b'{"upstream":"first"}', headers={"content-type": "application/json"}
            )
        return httpx.Response(
            200, content=success_body, headers={"content-type": "application/json"}
        )

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/chat/completions").mock(side_effect=upstream)
        response = client.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer test-gateway", "Content-Type": "application/json"},
            content=original_body,
        )

    assert response.status_code == 200
    assert response.content == success_body
    assert [key for key, _ in seen] == ["Bearer key-primary", "Bearer key-backup"]
    assert all(body == original_body for _, body in seen)

    import time

    time.sleep(0.05)
    logs = client.get("/api/admin/v1/requests", headers=admin_headers).json()["items"]
    assert logs[0]["response_channels"] == ["primary", "backup"]


def test_first_token_timeout_triggers_failover_after_stream_metadata(
    client, admin_headers, monkeypatch
):
    configure_route(client, admin_headers, protocol="openai_responses")
    from app.services import proxy as proxy_service

    get_runtime_settings = proxy_service.get_runtime_settings

    async def runtime_with_short_first_token_timeout(session):
        runtime = await get_runtime_settings(session)
        runtime["first_token_timeout_seconds"] = 0.02
        return runtime

    monkeypatch.setattr(
        proxy_service, "get_runtime_settings", runtime_with_short_first_token_timeout
    )
    backup_chunks = [
        b'data: {"type":"response.output_text.delta","delta":"OK"}\n\n',
        b'data: {"type":"response.completed","response":{"usage":{"output_tokens":1}}}\n\n',
    ]
    seen = []

    def upstream(request):
        seen.append(request.headers["authorization"])
        stream = (
            MetadataThenDelayedTokenStream()
            if request.headers["authorization"] == "Bearer key-primary"
            else ChunkStream(backup_chunks)
        )
        return httpx.Response(
            200,
            stream=stream,
            headers={"content-type": "text/event-stream"},
        )

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/responses").mock(side_effect=upstream)
        response = client.post(
            "/v1/responses",
            headers={"Authorization": "Bearer test-gateway", "Content-Type": "application/json"},
            content=b'{"model":"model-x","input":"hello","stream":true}',
        )

    assert response.status_code == 200
    assert response.content == b"".join(backup_chunks)
    assert seen == ["Bearer key-primary", "Bearer key-backup"]

    import time

    time.sleep(0.05)
    logs = client.get("/api/admin/v1/requests", headers=admin_headers).json()["items"]
    detail = client.get(f"/api/admin/v1/requests/{logs[0]['id']}", headers=admin_headers).json()
    assert detail["attempt_count"] == 2
    assert detail["attempts"][0]["outcome"] == "transport_error"
    assert detail["attempts"][0]["error_kind"] == "transport_timeout"
    assert detail["attempts"][0]["response_started"] is False


def test_responses_stream_error_before_content_triggers_failover(client, admin_headers):
    configure_route(client, admin_headers, protocol="openai_responses")
    failed_chunks = [
        b'data: {"type":"response.created","response":{"status":"in_progress"}}\n\n',
        b'data: {"type":"response.failed","response":{"status":"failed","error":{"code":503,"message":"upstream unavailable"}}}\n\n',
    ]
    success_chunks = [
        b'data: {"type":"response.output_text.delta","delta":"OK"}\n\n',
        b'data: {"type":"response.completed","response":{"usage":{"input_tokens":1,"output_tokens":2}}}\n\n',
    ]
    seen = []

    def upstream(request):
        seen.append(request.headers["authorization"])
        if request.headers["authorization"] == "Bearer key-primary":
            return httpx.Response(
                200,
                stream=ChunkStream(failed_chunks),
                headers={"content-type": "text/event-stream"},
            )
        return httpx.Response(
            200,
            stream=ChunkStream(success_chunks),
            headers={"content-type": "text/event-stream"},
        )

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/responses").mock(side_effect=upstream)
        response = client.post(
            "/v1/responses",
            headers={"Authorization": "Bearer test-gateway", "Content-Type": "application/json"},
            content=b'{"model":"model-x","input":"hello","stream":true}',
        )

    assert response.status_code == 200
    assert response.content == b"".join(success_chunks)
    assert seen == ["Bearer key-primary", "Bearer key-backup"]

    import time

    time.sleep(0.05)
    logs = client.get("/api/admin/v1/requests", headers=admin_headers).json()["items"]
    detail = client.get(f"/api/admin/v1/requests/{logs[0]['id']}", headers=admin_headers).json()
    assert detail["final_status_code"] == 200
    assert detail["outcome"] == "success"
    assert detail["attempt_count"] == 2
    first, second = detail["attempts"]
    assert first["status_code"] == 503
    assert first["outcome"] == "http_error"
    assert first["error_kind"] == "upstream_error"
    assert first["failover_eligible"] is True
    assert first["response_started"] is False
    assert second["status_code"] == 200
    assert second["outcome"] == "success"


def test_responses_stream_error_after_content_is_returned_and_logged(client, admin_headers):
    configure_route(client, admin_headers, protocol="openai_responses")
    content_chunk = b'data: {"type":"response.output_text.delta","delta":"partial"}\n\n'
    error_chunk = (
        b'data: {"type":"response.failed","response":{"status":"failed",'
        b'"error":{"code":503,"message":"upstream unavailable"}}}\n\n'
    )
    unseen_backup = b'data: {"type":"response.output_text.delta","delta":"backup"}\n\n'
    seen = []

    def upstream(request):
        seen.append(request.headers["authorization"])
        if request.headers["authorization"] == "Bearer key-primary":
            return httpx.Response(
                200,
                stream=ChunkStream([content_chunk, error_chunk]),
                headers={"content-type": "text/event-stream"},
            )
        return httpx.Response(
            200,
            stream=ChunkStream([unseen_backup]),
            headers={"content-type": "text/event-stream"},
        )

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/responses").mock(side_effect=upstream)
        response = client.post(
            "/v1/responses",
            headers={"Authorization": "Bearer test-gateway", "Content-Type": "application/json"},
            content=b'{"model":"model-x","input":"hello","stream":true}',
        )

    assert response.status_code == 200
    assert response.content == content_chunk + error_chunk
    assert seen == ["Bearer key-primary"]

    import time

    time.sleep(0.05)
    logs = client.get("/api/admin/v1/requests", headers=admin_headers).json()["items"]
    detail = client.get(f"/api/admin/v1/requests/{logs[0]['id']}", headers=admin_headers).json()
    assert detail["final_status_code"] == 503
    assert detail["outcome"] == "upstream_error"
    assert detail["attempt_count"] == 1
    attempt = detail["attempts"][0]
    assert attempt["status_code"] == 503
    assert attempt["outcome"] == "upstream_error"
    assert attempt["error_kind"] == "upstream_error"
    assert attempt["failover_eligible"] is False
    assert attempt["response_started"] is True


def test_tool_call_stream_cancellation_is_logged_as_success(client, admin_headers):
    configure_route(client, admin_headers, protocol="openai_responses")
    chunks = [
        b'data: {"type":"response.output_item.added","item":{"type":"function_call","name":"lookup","call_id":"call_1"}}\n\n',
    ]
    seen = []

    def upstream(request):
        seen.append(request.headers["authorization"])
        return httpx.Response(
            200,
            stream=ChunkStream(chunks, asyncio.CancelledError()),
            headers={"content-type": "text/event-stream"},
        )

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/responses").mock(side_effect=upstream)
        response = client.post(
            "/v1/responses",
            headers={"Authorization": "Bearer test-gateway", "Content-Type": "application/json"},
            content=b'{"model":"model-x","input":"hello","stream":true}',
        )

    assert response.status_code == 200
    assert response.content == b"".join(chunks)
    assert seen == ["Bearer key-primary"]

    import time

    time.sleep(0.05)
    logs = client.get("/api/admin/v1/requests", headers=admin_headers).json()["items"]
    detail = client.get(f"/api/admin/v1/requests/{logs[0]['id']}", headers=admin_headers).json()
    assert detail["final_status_code"] == 200
    assert detail["outcome"] == "success"
    assert detail["attempt_count"] == 1
    attempt = detail["attempts"][0]
    assert attempt["status_code"] == 200
    assert attempt["outcome"] == "success"
    assert attempt["response_started"] is True


def test_cancel_after_request_start_before_attempt_is_finalized(
    client, admin_headers, monkeypatch
):
    configure_route(client, admin_headers)

    from app.services import proxy as proxy_service

    async def cancelled_resolve_candidates(*args, **kwargs):
        raise asyncio.CancelledError()

    monkeypatch.setattr(
        proxy_service, "resolve_candidates", cancelled_resolve_candidates
    )
    response = client.post(
        "/v1/chat/completions",
        headers={"Authorization": "Bearer test-gateway", "Content-Type": "application/json"},
        content=b'{"model":"model-x","messages":[]}',
    )

    assert response.status_code == 204

    import time

    time.sleep(0.05)
    logs = client.get("/api/admin/v1/requests", headers=admin_headers).json()["items"]
    detail = client.get(f"/api/admin/v1/requests/{logs[0]['id']}", headers=admin_headers).json()
    assert detail["final_status_code"] == 499
    assert detail["outcome"] == "cancelled"
    assert detail["attempt_count"] == 0
    assert detail["attempts"] == []


def test_cancel_during_upstream_send_is_finalized(client, admin_headers, monkeypatch):
    configure_route(client, admin_headers)

    async def cancelled_send(*args, **kwargs):
        raise asyncio.CancelledError()

    monkeypatch.setattr(client.app.state.http, "send", cancelled_send)
    response = client.post(
        "/v1/chat/completions",
        headers={"Authorization": "Bearer test-gateway", "Content-Type": "application/json"},
        content=b'{"model":"model-x","messages":[]}',
    )

    assert response.status_code == 204

    import time

    time.sleep(0.05)
    logs = client.get("/api/admin/v1/requests", headers=admin_headers).json()["items"]
    detail = client.get(f"/api/admin/v1/requests/{logs[0]['id']}", headers=admin_headers).json()
    assert detail["final_status_code"] == 499
    assert detail["outcome"] == "cancelled"
    assert detail["attempt_count"] == 1
    attempt = detail["attempts"][0]
    assert attempt["status_code"] is None
    assert attempt["outcome"] == "cancelled"
    assert attempt["response_started"] is False


def test_cancel_after_responses_completion_is_logged_as_success(client, admin_headers):
    configure_route(client, admin_headers, protocol="openai_responses")
    chunks = [
        b'data: {"type":"response.output_text.delta","delta":"OK"}\n\n',
        b'data: {"type":"response.completed","response":{"status":"completed","usage":{"output_tokens":2}}}\n\n',
    ]

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/responses").mock(
            return_value=httpx.Response(
                200,
                stream=ChunkStream(chunks, asyncio.CancelledError()),
                headers={"content-type": "text/event-stream"},
            )
        )
        response = client.post(
            "/v1/responses",
            headers={"Authorization": "Bearer test-gateway", "Content-Type": "application/json"},
            content=b'{"model":"model-x","input":"hello","stream":true}',
        )

    assert response.status_code == 200
    assert response.content == b"".join(chunks)

    import time

    time.sleep(0.05)
    logs = client.get("/api/admin/v1/requests", headers=admin_headers).json()["items"]
    detail = client.get(f"/api/admin/v1/requests/{logs[0]['id']}", headers=admin_headers).json()
    assert detail["outcome"] == "success"
    assert detail["attempts"][0]["outcome"] == "success"


def test_model_priority_is_shared_and_filtered_by_endpoint_protocol(
    client, admin_headers
):
    provider = client.post(
        "/api/admin/v1/providers",
        headers=admin_headers,
        json={"name": "Priority upstream", "base_url": "https://priority.test"},
    ).json()
    configured = {}
    for name, protocols, key in (
        ("A", ["openai_compatible", "openai_responses"], "key-a"),
        ("B", ["openai_compatible", "openai_responses"], "key-b"),
        ("C", ["openai_responses"], "key-c"),
    ):
        channel = client.post(
            "/api/admin/v1/channels",
            headers=admin_headers,
            json={
                "provider_id": provider["id"],
                "name": name,
                "protocols": protocols,
                "api_key": key,
            },
        ).json()
        configured[name] = client.post(
            f"/api/admin/v1/channels/{channel['id']}/models",
            headers=admin_headers,
            json={"model_id": "gpt-5.6-sol"},
        ).json()
    route = client.post(
        "/api/admin/v1/routes",
        headers=admin_headers,
        json={"requested_model_id": "gpt-5.6-sol"},
    ).json()
    client.put(
        f"/api/admin/v1/routes/{route['id']}/candidates",
        headers=admin_headers,
        json={
            "candidates": [
                {"channel_model_id": configured["A"]["id"], "priority": 0},
                {"channel_model_id": configured["B"]["id"], "priority": 2},
                {"channel_model_id": configured["C"]["id"], "priority": 1},
            ]
        },
    ).raise_for_status()

    chat_seen = []
    response_seen = []

    def chat_upstream(request):
        key = request.headers["authorization"]
        chat_seen.append(key)
        return httpx.Response(200 if key == "Bearer key-b" else 500, content=b"chat")

    def responses_upstream(request):
        key = request.headers["authorization"]
        response_seen.append(key)
        return httpx.Response(200 if key == "Bearer key-b" else 500, content=b"response")

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://priority.test/v1/chat/completions").mock(
            side_effect=chat_upstream
        )
        mock.post("https://priority.test/v1/responses").mock(
            side_effect=responses_upstream
        )
        chat_response = client.post(
            "/v1/chat/completions",
            headers={"Content-Type": "application/json"},
            content=b'{"model":"gpt-5.6-sol","messages":[]}',
        )
        responses_response = client.post(
            "/v1/responses",
            headers={"Content-Type": "application/json"},
            content=b'{"model":"gpt-5.6-sol","input":"hello"}',
        )

    assert chat_response.status_code == 200
    assert responses_response.status_code == 200
    assert chat_seen == ["Bearer key-a", "Bearer key-b"]
    assert response_seen == ["Bearer key-a", "Bearer key-c", "Bearer key-b"]


def test_large_request_rolls_to_replayable_file_without_byte_changes(client, admin_headers):
    configure_route(client, admin_headers)
    original_body = (
        b'{"model":"model-x","stream":false,"payload":"' + (b"x" * (8 * 1024 * 1024 + 1024)) + b'"}'
    )
    seen_hashes = []

    import hashlib

    def upstream(request):
        seen_hashes.append(hashlib.sha256(request.content).hexdigest())
        if request.headers["authorization"] == "Bearer key-primary":
            return httpx.Response(500, content=b"retry")
        return httpx.Response(200, content=b"ok")

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/chat/completions").mock(side_effect=upstream)
        response = client.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer test-gateway", "Content-Type": "application/json"},
            content=original_body,
        )

    expected_hash = hashlib.sha256(original_body).hexdigest()
    assert response.content == b"ok"
    assert seen_hashes == [expected_hash, expected_hash]


def test_final_upstream_error_is_returned_raw(client, admin_headers):
    configure_route(client, admin_headers)
    final_error = b'{  "error" : {"vendor":true} }'

    def upstream(request):
        if request.headers["authorization"] == "Bearer key-primary":
            return httpx.Response(
                400, content=b'{"error":"request"}', headers={"content-type": "application/json"}
            )
        return httpx.Response(
            429,
            content=final_error,
            headers={"content-type": "application/json", "x-upstream": "yes"},
        )

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/chat/completions").mock(side_effect=upstream)
        response = client.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer test-gateway", "Content-Type": "application/json"},
            content=b'{"model":"model-x","messages":[]}',
        )

    assert response.status_code == 429
    assert response.content == final_error
    assert response.headers["x-upstream"] == "yes"


def test_failover_attempts_are_bounded_by_runtime_setting(client, admin_headers):
    configure_route(client, admin_headers)
    client.patch(
        "/api/admin/v1/settings",
        headers=admin_headers,
        json={"max_failover_attempts": 1},
    ).raise_for_status()
    seen = []

    def upstream(request):
        seen.append(request.headers["authorization"])
        return httpx.Response(500, content=b"only-first-error")

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/chat/completions").mock(side_effect=upstream)
        response = client.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer test-gateway", "Content-Type": "application/json"},
            content=b'{"model":"model-x","messages":[]}',
        )

    assert response.status_code == 500
    assert response.content == b"only-first-error"
    assert seen == ["Bearer key-primary"]


def test_bad_request_does_not_open_channel(client, admin_headers):
    _, configured, _ = configure_route(client, admin_headers)

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/chat/completions").mock(
            return_value=httpx.Response(400, content=b'{"error":"bad body"}')
        )
        for _ in range(4):
            response = client.post(
                "/v1/chat/completions",
                headers={
                    "Authorization": "Bearer test-gateway",
                    "Content-Type": "application/json",
                },
                content=b'{"model":"model-x","messages":[]}',
            )
            assert response.status_code == 400

    channels = client.get("/api/admin/v1/channels", headers=admin_headers).json()["items"]
    assert all(channel["health"]["state"] == "active" for channel in channels)
    assert all(channel["health"]["consecutive_failures"] == 0 for channel in channels)


def test_consecutive_errors_open_primary_channel(client, admin_headers):
    configure_route(client, admin_headers)
    seen = []

    def upstream(request):
        key = request.headers["authorization"]
        seen.append(key)
        if key == "Bearer key-primary":
            return httpx.Response(500, content=b"primary failed")
        return httpx.Response(200, content=b"backup ok")

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/chat/completions").mock(side_effect=upstream)
        for _ in range(4):
            response = client.post(
                "/v1/chat/completions",
                headers={
                    "Authorization": "Bearer test-gateway",
                    "Content-Type": "application/json",
                },
                content=b'{"model":"model-x","messages":[]}',
            )
            assert response.status_code == 200

    assert seen.count("Bearer key-primary") == 3
    assert seen.count("Bearer key-backup") == 4
    channels = client.get("/api/admin/v1/channels", headers=admin_headers).json()["items"]
    primary = next(item for item in channels if item["name"] == "primary")
    assert primary["health"]["state"] == "open"
    assert primary["health"]["consecutive_failures"] == 3


def test_due_open_channel_is_probed_and_recovers(client, admin_headers):
    configure_route(client, admin_headers)
    client.portal.call(client.app.state.health_supervisor.stop)
    client.patch(
        "/api/admin/v1/settings",
        headers=admin_headers,
        json={"failure_threshold": 1, "circuit_open_seconds": 0},
    ).raise_for_status()

    def first_request(request):
        if request.headers["authorization"] == "Bearer key-primary":
            return httpx.Response(500, content=b"failed")
        return httpx.Response(200, content=b"backup")

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/chat/completions").mock(side_effect=first_request)
        response = client.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer test-gateway", "Content-Type": "application/json"},
            content=b'{"model":"model-x","messages":[]}',
        )
    assert response.status_code == 200
    channels = client.get("/api/admin/v1/channels", headers=admin_headers).json()["items"]
    assert next(item for item in channels if item["name"] == "primary")["health"]["state"] == "open"

    with respx.mock(assert_all_called=False) as mock:
        probe = mock.post("https://upstream.test/v1/chat/completions").mock(
            return_value=httpx.Response(
                200,
                stream=ChunkStream(
                    [b'data: {"choices":[{"message":{"content":"OK"}}]}\n\n',
                     b'data: [DONE]\n\n'],
                ),
                headers={"content-type": "text/event-stream"},
            )
        )
        client.portal.call(client.app.state.health_supervisor._probe_due_channels)
        assert probe.call_count == 1

    channels = client.get("/api/admin/v1/channels", headers=admin_headers).json()["items"]
    primary = next(item for item in channels if item["name"] == "primary")
    assert primary["health"]["state"] == "active"
    assert primary["health"]["consecutive_failures"] == 0


def test_health_probe_uses_selected_or_first_channel_model(client, admin_headers):
    _, configured, _ = configure_route(client, admin_headers)
    channel_id = configured[0][0]["id"]
    for model_id in ("a-first", "z-selected"):
        response = client.post(
            f"/api/admin/v1/channels/{channel_id}/models",
            headers=admin_headers,
            json={"model_id": model_id},
        )
        assert response.status_code == 201, response.text

    selected = client.patch(
        f"/api/admin/v1/channels/{channel_id}",
        headers=admin_headers,
        json={"health_check_model_id": "z-selected"},
    )
    assert selected.status_code == 200, selected.text

    with respx.mock(assert_all_called=False) as mock:
        probe = mock.post("https://upstream.test/v1/chat/completions").mock(
            return_value=httpx.Response(
                200,
                stream=ChunkStream(
                    [b'data: {"choices":[{"message":{"content":"OK"}}]}\n\n',
                     b'data: [DONE]\n\n'],
                ),
                headers={"content-type": "text/event-stream"},
            )
        )
        assert client.portal.call(probe_channel, client.app, channel_id) is True
    assert json.loads(probe.calls[0].request.content)["model"] == "z-selected"

    cleared = client.patch(
        f"/api/admin/v1/channels/{channel_id}",
        headers=admin_headers,
        json={"health_check_model_id": None},
    )
    assert cleared.status_code == 200, cleared.text

    with respx.mock(assert_all_called=False) as mock:
        probe = mock.post("https://upstream.test/v1/chat/completions").mock(
            return_value=httpx.Response(
                200,
                stream=ChunkStream(
                    [b'data: {"choices":[{"message":{"content":"OK"}}]}\n\n',
                     b'data: [DONE]\n\n'],
                ),
                headers={"content-type": "text/event-stream"},
            )
        )
        assert client.portal.call(probe_channel, client.app, channel_id) is True
    assert json.loads(probe.calls[0].request.content)["model"] == "a-first"

    invalid = client.patch(
        f"/api/admin/v1/channels/{channel_id}",
        headers=admin_headers,
        json={"health_check_model_id": "not-a-channel-model"},
    )
    assert invalid.status_code == 422


def test_route_pool_never_crosses_protocol(client, admin_headers):
    configure_route(client, admin_headers, protocol="claude", model_id="model-x")
    with respx.mock(assert_all_called=False) as mock:
        upstream = mock.post("https://upstream.test/v1/messages").mock(
            return_value=httpx.Response(200, content=b"should-not-run")
        )
        response = client.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer test-gateway", "Content-Type": "application/json"},
            content=b'{"model":"model-x","messages":[]}',
        )
    assert response.status_code == 400
    error = response.json()["error"]
    assert error["code"] == "unsupported_model_endpoint"
    assert error["requested_protocol"] == "openai_compatible"
    assert error["supported_protocols"] == ["claude"]
    assert error["supported_endpoints"] == ["/v1/messages"]
    assert upstream.call_count == 0


@pytest.mark.parametrize(
    (
        "protocol",
        "client_path",
        "upstream_path",
        "local_headers",
        "body",
        "auth_header",
        "auth_value",
    ),
    [
        (
            "openai_responses",
            "/v1/responses",
            "/v1/responses",
            {"Authorization": "Bearer test-gateway"},
            b'{"model":"model-x","input":"hello"}',
            "authorization",
            "Bearer key-primary",
        ),
        (
            "claude",
            "/v1/messages",
            "/v1/messages",
            {"x-api-key": "test-gateway", "anthropic-version": "2023-06-01"},
            b'{"model":"model-x","max_tokens":2,"messages":[]}',
            "x-api-key",
            "key-primary",
        ),
        (
            "gemini",
            "/v1beta/models/model-x:generateContent",
            "/v1beta/models/model-x:generateContent",
            {"x-goog-api-key": "test-gateway"},
            b'{"contents":[]}',
            "x-goog-api-key",
            "key-primary",
        ),
    ],
)
def test_native_protocol_endpoints_preserve_bytes_and_replace_credentials(
    client,
    admin_headers,
    protocol,
    client_path,
    upstream_path,
    local_headers,
    body,
    auth_header,
    auth_value,
):
    configure_route(client, admin_headers, protocol=protocol, model_id="model-x")
    response_body = b'{"native":true}'
    seen = []

    def upstream(request):
        seen.append((request.headers[auth_header], request.content))
        return httpx.Response(
            200, content=response_body, headers={"content-type": "application/json"}
        )

    with respx.mock(assert_all_called=False) as mock:
        mock.post(f"https://upstream.test{upstream_path}").mock(side_effect=upstream)
        response = client.post(
            client_path,
            headers={**local_headers, "Content-Type": "application/json"},
            content=body,
        )

    assert response.status_code == 200
    assert response.content == response_body
    assert seen == [(auth_value, body)]


def test_streaming_usage_is_observed_without_changing_sse_bytes(client, admin_headers):
    configure_route(client, admin_headers)
    chunks = [
        b'data: {"choices":[{"delta":{"content":"O"}}]}\n\n',
        b'data: {"choices":[{"delta":{"content":"K"}}],"usage":{"prompt_tokens":5,',
        b'"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":3}}}\n\n',
        b"data: [DONE]\n\n",
    ]
    expected = b"".join(chunks)

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/chat/completions").mock(
            return_value=httpx.Response(
                200,
                stream=ChunkStream(chunks),
                headers={"content-type": "text/event-stream"},
            )
        )
        response = client.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer test-gateway", "Content-Type": "application/json"},
            content=b'{"model":"model-x","messages":[],"stream":true}',
        )

    assert response.status_code == 200
    assert response.content == expected
    import time

    time.sleep(0.05)
    logs = client.get("/api/admin/v1/requests", headers=admin_headers).json()["items"]
    detail = client.get(f"/api/admin/v1/requests/{logs[0]['id']}", headers=admin_headers).json()
    attempt = detail["attempts"][0]
    assert attempt["first_token_ms"] is not None
    assert attempt["cache_read_tokens"] == 3
    assert attempt["cache_miss_input_tokens"] == 2
    assert attempt["output_tokens"] == 2


def test_stream_failure_after_headers_never_calls_backup(client, admin_headers):
    configure_route(client, admin_headers)
    keys = []

    def upstream(request):
        keys.append(request.headers["authorization"])
        return httpx.Response(
            200,
            stream=ChunkStream(
                [b'data: {"choices":[{"delta":{"content":"partial"}}]}\n\n'],
                httpx.ReadError("upstream stream broke"),
            ),
            headers={"content-type": "text/event-stream"},
        )

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/chat/completions").mock(side_effect=upstream)
        with pytest.raises(Exception):
            client.post(
                "/v1/chat/completions",
                headers={
                    "Authorization": "Bearer test-gateway",
                    "Content-Type": "application/json",
                },
                content=b'{"model":"model-x","messages":[],"stream":true}',
            )

    assert keys == ["Bearer key-primary"]

    import time

    time.sleep(0.05)
    logs = client.get("/api/admin/v1/requests", headers=admin_headers).json()["items"]
    detail = client.get(f"/api/admin/v1/requests/{logs[0]['id']}", headers=admin_headers).json()
    assert detail["final_status_code"] == 502
    assert detail["outcome"] == "stream_interrupted"
    assert detail["attempt_count"] == 1
    attempt = detail["attempts"][0]
    assert attempt["status_code"] == 502
    assert attempt["outcome"] == "stream_interrupted"
    assert attempt["error_kind"] == "transport_error"
    assert attempt["response_started"] is True
