import sqlite3
import time
import uuid
from datetime import datetime, timezone

import httpx
import respx

from app.services.maintenance import reconcile_completed_stream_cancellations
from tests.conftest import configure_route


def test_configuration_and_unique_priority(client, admin_headers):
    _, configured, route = configure_route(client, admin_headers)
    response = client.put(
        f"/api/admin/v1/routes/{route['id']}/candidates",
        headers=admin_headers,
        json={
            "candidates": [
                {"channel_model_id": configured[0][1]["id"], "priority": 0},
                {"channel_model_id": configured[1][1]["id"], "priority": 0},
            ]
        },
    )
    assert response.status_code == 409
    current = client.get("/api/admin/v1/routes", headers=admin_headers).json()["items"][0]
    assert [item["priority"] for item in current["candidates"]] == [0, 1]


def test_deleting_channel_removes_its_route_candidates(client, admin_headers):
    _, configured, _ = configure_route(client, admin_headers)

    response = client.delete(
        f"/api/admin/v1/channels/{configured[0][0]['id']}", headers=admin_headers
    )

    assert response.status_code == 204, response.text
    channels = client.get("/api/admin/v1/channels", headers=admin_headers).json()["items"]
    assert [item["id"] for item in channels] == [configured[1][0]["id"]]
    routes = client.get("/api/admin/v1/routes", headers=admin_headers).json()["items"]
    assert [item["channel_model_id"] for item in routes[0]["candidates"]] == [
        configured[1][1]["id"]
    ]


def test_deleting_provider_removes_channels_and_route_candidates(client, admin_headers):
    provider, _, _ = configure_route(client, admin_headers)

    response = client.delete(
        f"/api/admin/v1/providers/{provider['id']}", headers=admin_headers
    )

    assert response.status_code == 204, response.text
    assert client.get("/api/admin/v1/providers", headers=admin_headers).json()["items"] == []
    assert client.get("/api/admin/v1/channels", headers=admin_headers).json()["items"] == []
    routes = client.get("/api/admin/v1/routes", headers=admin_headers).json()["items"]
    assert len(routes) == 1
    assert routes[0]["candidates"] == []


def test_api_key_never_echoes(client, admin_headers):
    provider = client.post(
        "/api/admin/v1/providers",
        headers=admin_headers,
        json={"name": "Provider", "base_url": "https://example.test"},
    ).json()
    key = "a-secret-key-value"
    response = client.post(
        "/api/admin/v1/channels",
        headers=admin_headers,
        json={
            "provider_id": provider["id"],
            "name": "account",
            "protocol": "claude",
            "api_key": key,
        },
    )
    assert response.status_code == 201
    assert key not in response.text
    listed = client.get("/api/admin/v1/channels", headers=admin_headers)
    assert key not in listed.text
    assert listed.json()["items"][0]["api_key_hint"] == "...alue"


def test_stats_and_request_details_recompute_tps_from_duration(client, admin_headers):
    request_id = str(uuid.uuid4())
    second_request_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    db_path = client.app.state.settings.data_dir / "gateway.db"
    with sqlite3.connect(db_path) as connection:
        connection.executemany(
            """
            INSERT INTO request_logs (
                id, protocol, model_id, endpoint, stream, started_at, finished_at,
                total_duration_ms, final_status_code, outcome, attempt_count,
                final_channel_id, request_bytes, response_bytes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    request_id,
                    "openai_compatible",
                    "model-x",
                    "/v1/chat/completions",
                    1,
                    now,
                    now,
                    1000,
                    200,
                    "success",
                    1,
                    None,
                    10,
                    100,
                ),
                (
                    second_request_id,
                    "openai_compatible",
                    "model-x",
                    "/v1/chat/completions",
                    1,
                    now,
                    now,
                    5000,
                    200,
                    "success",
                    1,
                    None,
                    10,
                    100,
                ),
            ],
        )
        connection.executemany(
            """
            INSERT INTO request_attempts (
                id, request_id, channel_id, channel_name, attempt_no, priority_snapshot,
                started_at, finished_at, status_code, outcome, error_kind,
                failover_eligible, response_started, first_byte_ms, first_token_ms,
                duration_ms, input_tokens, cache_read_tokens, cache_write_tokens,
                cache_miss_input_tokens, output_tokens, tps, raw_usage_json,
                response_bytes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    str(uuid.uuid4()),
                    request_id,
                    None,
                    "historic-fast",
                    1,
                    0,
                    now,
                    now,
                    200,
                    "success",
                    None,
                    0,
                    1,
                    100,
                    999,
                    1000,
                    10,
                    0,
                    0,
                    10,
                    100,
                    999999.0,
                    None,
                    100,
                ),
                (
                    str(uuid.uuid4()),
                    second_request_id,
                    None,
                    "historic-slow",
                    1,
                    0,
                    now,
                    now,
                    200,
                    "success",
                    None,
                    0,
                    1,
                    100,
                    4999,
                    5000,
                    10,
                    0,
                    0,
                    10,
                    200,
                    999999.0,
                    None,
                    100,
                ),
            ],
        )

    summary = client.get("/api/admin/v1/stats/summary", headers=admin_headers).json()
    detail = client.get(f"/api/admin/v1/requests/{request_id}", headers=admin_headers).json()

    assert summary["average_tps"] == 50.0
    assert detail["attempts"][0]["tps"] == 100.0


def test_reconcile_completed_stream_cancellations_leaves_incomplete_final_attempts(
    client, admin_headers
):
    request_id = str(uuid.uuid4())
    cancelled_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    db_path = client.app.state.settings.data_dir / "gateway.db"
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            INSERT INTO request_logs (
                id, protocol, model_id, endpoint, stream, started_at, finished_at,
                total_duration_ms, final_status_code, outcome, attempt_count,
                final_channel_id, request_bytes, response_bytes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request_id,
                "openai_responses",
                "model-x",
                "/v1/responses",
                1,
                now,
                now,
                1000,
                200,
                "cancelled",
                1,
                None,
                10,
                100,
            ),
        )
        connection.executemany(
            """
            INSERT INTO request_attempts (
                id, request_id, channel_id, channel_name, attempt_no, priority_snapshot,
                started_at, finished_at, status_code, outcome, error_kind,
                failover_eligible, response_started, first_byte_ms, first_token_ms,
                duration_ms, input_tokens, cache_read_tokens, cache_write_tokens,
                cache_miss_input_tokens, output_tokens, tps, raw_usage_json,
                response_bytes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    str(uuid.uuid4()), request_id, None, "completed", 1, 0, now, now, 200,
                    "cancelled", None, 0, 1, 10, 20, 1000, 10, 0, 0, 10, 2, 2.0,
                    '{"output_tokens": 2}', 100,
                ),
                (
                    cancelled_id, request_id, None, "incomplete", 2, 1, now, now, 200,
                    "cancelled", None, 0, 1, 10, 20, 1000, None, None, None, None, None, None,
                    None, 100,
                ),
            ],
        )

    repaired = client.portal.call(reconcile_completed_stream_cancellations, client.app.state.db.sessions)
    detail = client.get(f"/api/admin/v1/requests/{request_id}", headers=admin_headers).json()

    assert repaired == 0
    assert detail["outcome"] == "cancelled"
    assert [attempt["outcome"] for attempt in detail["attempts"]] == ["cancelled", "cancelled"]


def test_reconcile_completed_stream_cancellations_repairs_completed_final_attempt(
    client, admin_headers
):
    request_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    db_path = client.app.state.settings.data_dir / "gateway.db"
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            INSERT INTO request_logs (
                id, protocol, model_id, endpoint, stream, started_at, finished_at,
                total_duration_ms, final_status_code, outcome, attempt_count,
                final_channel_id, request_bytes, response_bytes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request_id,
                "openai_responses",
                "model-x",
                "/v1/responses",
                1,
                now,
                now,
                1000,
                200,
                "cancelled",
                1,
                None,
                10,
                100,
            ),
        )
        connection.execute(
            """
            INSERT INTO request_attempts (
                id, request_id, channel_id, channel_name, attempt_no, priority_snapshot,
                started_at, finished_at, status_code, outcome, error_kind,
                failover_eligible, response_started, first_byte_ms, first_token_ms,
                duration_ms, input_tokens, cache_read_tokens, cache_write_tokens,
                cache_miss_input_tokens, output_tokens, tps, raw_usage_json,
                response_bytes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()), request_id, None, "completed", 1, 0, now, now, 200,
                "cancelled", None, 0, 1, 10, 20, 1000, 10, 0, 0, 10, 2, 2.0,
                '{"output_tokens": 2}', 100,
            ),
        )

    repaired = client.portal.call(reconcile_completed_stream_cancellations, client.app.state.db.sessions)
    detail = client.get(f"/api/admin/v1/requests/{request_id}", headers=admin_headers).json()

    assert repaired == 1
    assert detail["outcome"] == "success"
    assert detail["attempts"][0]["outcome"] == "success"


def test_summary_groups_cache_hit_rate_by_provider_protocol(client, admin_headers):
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    request_ids = [str(uuid.uuid4()) for _ in range(3)]
    db_path = client.app.state.settings.data_dir / "gateway.db"
    with sqlite3.connect(db_path) as connection:
        connection.executemany(
            """
            INSERT INTO request_logs (
                id, protocol, model_id, endpoint, stream, started_at, finished_at,
                total_duration_ms, final_status_code, outcome, attempt_count,
                final_channel_id, request_bytes, response_bytes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (request_ids[0], "openai_compatible", "gpt", "/v1/chat/completions", 0, now, now, 10, 200, "success", 1, None, 1, 1),
                (request_ids[1], "openai_responses", "gpt", "/v1/responses", 0, now, now, 10, 200, "success", 1, None, 1, 1),
                (request_ids[2], "claude", "claude", "/v1/messages", 0, now, now, 10, 200, "success", 1, None, 1, 1),
            ],
        )
        connection.executemany(
            """
            INSERT INTO request_attempts (
                id, request_id, channel_id, channel_name, attempt_no, priority_snapshot,
                started_at, finished_at, status_code, outcome, error_kind,
                failover_eligible, response_started, first_byte_ms, first_token_ms,
                duration_ms, input_tokens, cache_read_tokens, cache_write_tokens,
                cache_miss_input_tokens, output_tokens, tps, raw_usage_json,
                response_bytes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (str(uuid.uuid4()), request_ids[0], None, "OpenAI", 1, 0, now, now, 200, "success", None, 0, 1, 1, 1, 10, 100, 80, 10, 10, 1, 1.0, None, 1),
                (str(uuid.uuid4()), request_ids[1], None, "OpenAI", 1, 0, now, now, 200, "success", None, 0, 1, 1, 1, 10, 50, 20, 5, 25, 1, 1.0, None, 1),
                (str(uuid.uuid4()), request_ids[2], None, "Claude", 1, 0, now, now, 200, "success", None, 0, 1, 1, 1, 10, 10, 8, 0, 2, 1, 1.0, None, 1),
            ],
        )

    summary = client.get("/api/admin/v1/stats/summary", headers=admin_headers).json()
    by_provider = {item["provider"]: item for item in summary["cache_by_provider"]}

    assert by_provider["OpenAI"] == {
        "provider": "OpenAI",
        "request_count": 2,
        "cache_read_tokens": 100,
        "cache_write_tokens": 15,
        "cache_miss_input_tokens": 35,
        "total_input_tokens": 150,
        "cache_hit_rate": 0.6667,
    }
    assert by_provider["Claude"]["cache_hit_rate"] == 0.8
    assert "Gemini" not in by_provider


def test_cross_protocol_model_cannot_be_bound(client, admin_headers):
    _, configured, route = configure_route(client, admin_headers)
    provider_id = configured[0][0]["provider_id"]
    other = client.post(
        "/api/admin/v1/channels",
        headers=admin_headers,
        json={
            "provider_id": provider_id,
            "name": "claude-account",
            "protocol": "claude",
            "api_key": "claude-key",
        },
    ).json()
    model = client.post(
        f"/api/admin/v1/channels/{other['id']}/models",
        headers=admin_headers,
        json={"model_id": "model-x"},
    ).json()
    response = client.put(
        f"/api/admin/v1/routes/{route['id']}/candidates",
        headers=admin_headers,
        json={"candidates": [{"channel_model_id": model["id"], "priority": 0}]},
    )
    assert response.status_code == 422


def test_channel_supports_multiple_protocols_and_normalizes_provider_root(
    client, admin_headers
):
    provider = client.post(
        "/api/admin/v1/providers",
        headers=admin_headers,
        json={"name": "Multi API", "base_url": "https://example.test/gateway/v1"},
    ).json()
    assert provider["base_url"] == "https://example.test/gateway"

    channel = client.post(
        "/api/admin/v1/channels",
        headers=admin_headers,
        json={
            "provider_id": provider["id"],
            "name": "multi-account",
            "protocols": ["openai_compatible", "openai_responses"],
            "api_key": "secret",
        },
    ).json()
    assert channel["protocol"] == "openai_compatible"
    assert channel["protocols"] == ["openai_compatible", "openai_responses"]

    model = client.post(
        f"/api/admin/v1/channels/{channel['id']}/models",
        headers=admin_headers,
        json={"model_id": "shared-model"},
    ).json()
    assert model["protocols"] == ["openai_compatible", "openai_responses"]

    for protocol in channel["protocols"]:
        route = client.post(
            "/api/admin/v1/routes",
            headers=admin_headers,
            json={"protocol": protocol, "requested_model_id": "shared-model"},
        ).json()
        response = client.put(
            f"/api/admin/v1/routes/{route['id']}/candidates",
            headers=admin_headers,
            json={"candidates": [{"channel_model_id": model["id"], "priority": 0}]},
        )
        assert response.status_code == 200, response.text

    aggregate = client.get("/v1/models").json()["data"]
    assert len(aggregate) == 1
    assert aggregate[0]["id"] == "shared-model"
    assert aggregate[0]["x_local_gateway"] == {
        "supported_endpoints": ["/v1/chat/completions", "/v1/responses"]
    }


def test_multi_protocol_discovery_reuses_shared_openai_catalog(client, admin_headers):
    provider = client.post(
        "/api/admin/v1/providers",
        headers=admin_headers,
        json={"name": "Discovery API", "base_url": "https://discover.test/v1"},
    ).json()
    channel = client.post(
        "/api/admin/v1/channels",
        headers=admin_headers,
        json={
            "provider_id": provider["id"],
            "name": "multi-account",
            "protocols": ["openai_compatible", "openai_responses", "claude"],
            "api_key": "upstream-key",
        },
    ).json()

    def model_catalog(request):
        if request.headers.get("x-api-key"):
            return httpx.Response(
                200,
                json={
                    "data": [
                        {"id": "claude-model", "type": "model", "display_name": "Claude"}
                    ],
                    "has_more": False,
                },
            )
        return httpx.Response(200, json={"object": "list", "data": [{"id": "shared"}]})

    with respx.mock(assert_all_called=False) as mock:
        upstream = mock.get("https://discover.test/v1/models").mock(side_effect=model_catalog)
        run_id = client.post(
            f"/api/admin/v1/channels/{channel['id']}/discover-models",
            headers=admin_headers,
        ).json()["run_id"]
        for _ in range(50):
            run = client.get(
                f"/api/admin/v1/discovery-runs/{run_id}", headers=admin_headers
            ).json()
            if run["status"] != "running":
                break
            time.sleep(0.01)

    assert run["status"] == "succeeded"
    assert upstream.call_count == 2
    models = client.get("/api/admin/v1/channel-models", headers=admin_headers).json()[
        "items"
    ]
    by_id = {item["model_id"]: item for item in models}
    assert by_id["shared"]["protocols"] == ["openai_compatible", "openai_responses"]
    assert by_id["claude-model"]["protocols"] == ["claude"]


def test_existing_channel_can_enable_an_additional_protocol(client, admin_headers):
    provider = client.post(
        "/api/admin/v1/providers",
        headers=admin_headers,
        json={"name": "Editable API", "base_url": "https://editable.test"},
    ).json()
    channel = client.post(
        "/api/admin/v1/channels",
        headers=admin_headers,
        json={
            "provider_id": provider["id"],
            "name": "editable",
            "protocol": "openai_compatible",
            "api_key": "secret",
        },
    ).json()
    model = client.post(
        f"/api/admin/v1/channels/{channel['id']}/models",
        headers=admin_headers,
        json={"model_id": "shared-model"},
    ).json()
    assert model["protocols"] == ["openai_compatible"]
    source_route = client.post(
        "/api/admin/v1/routes",
        headers=admin_headers,
        json={"protocol": "openai_compatible", "requested_model_id": "shared-model"},
    ).json()
    client.put(
        f"/api/admin/v1/routes/{source_route['id']}/candidates",
        headers=admin_headers,
        json={"candidates": [{"channel_model_id": model["id"], "priority": 3}]},
    ).raise_for_status()

    response = client.patch(
        f"/api/admin/v1/channels/{channel['id']}",
        headers=admin_headers,
        json={"protocols": ["openai_compatible", "openai_responses"]},
    )

    assert response.status_code == 200, response.text
    updated = client.get(
        f"/api/admin/v1/channels/{channel['id']}", headers=admin_headers
    ).json()
    assert updated["protocols"] == ["openai_compatible", "openai_responses"]
    models = client.get(
        f"/api/admin/v1/channel-models?channel_id={channel['id']}",
        headers=admin_headers,
    ).json()["items"]
    assert models[0]["protocols"] == ["openai_compatible", "openai_responses"]

    routes = client.get("/api/admin/v1/routes", headers=admin_headers).json()["items"]
    assert len(routes) == 1
    assert routes[0]["protocols"] == ["openai_compatible", "openai_responses"]
    candidates = routes[0]["candidates"]
    assert len(candidates) == 1
    assert candidates[0]["channel_model_id"] == model["id"]
    assert candidates[0]["priority"] == 3
    assert candidates[0]["enabled"] is True
    assert candidates[0]["protocols"] == ["openai_compatible", "openai_responses"]
