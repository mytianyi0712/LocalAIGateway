import pytest

from tests.conftest import configure_route


@pytest.mark.parametrize(
    ("protocol", "path"),
    [
        ("openai_compatible", "/v1/models"),
        ("openai_responses", "/v1/responses/models"),
        ("claude", "/v1/messages/models"),
        ("gemini", "/v1beta/models"),
    ],
)
def test_native_model_catalog_formats(client, admin_headers, protocol, path):
    configure_route(client, admin_headers, protocol=protocol, model_id=f"{protocol}-model")

    response = client.get(path)

    assert response.status_code == 200
    payload = response.json()
    if protocol in {"openai_compatible", "openai_responses"}:
        assert payload["object"] == "list"
        assert [item["id"] for item in payload["data"]] == [f"{protocol}-model"]
        assert payload["data"][0]["object"] == "model"
    elif protocol == "claude":
        assert [item["id"] for item in payload["data"]] == ["claude-model"]
        assert payload["data"][0]["type"] == "model"
        assert payload["has_more"] is False
    else:
        assert [item["name"] for item in payload["models"]] == ["models/gemini-model"]
        assert payload["models"][0]["baseModelId"] == "gemini-model"


def test_shared_v1_models_endpoint_aggregates_protocol_pools(client, admin_headers):
    configure_route(
        client,
        admin_headers,
        protocol="openai_compatible",
        model_id="chat-only",
        provider_name="Chat provider",
    )
    configure_route(
        client,
        admin_headers,
        protocol="openai_responses",
        model_id="responses-only",
        provider_name="Responses provider",
    )
    configure_route(
        client,
        admin_headers,
        protocol="claude",
        model_id="claude-only",
        provider_name="Claude provider",
    )

    default_models = client.get("/v1/models").json()
    responses_models = client.get("/v1/models?protocol=openai_responses").json()
    claude_models = client.get(
        "/v1/models", headers={"anthropic-version": "2023-06-01"}
    ).json()

    assert [item["id"] for item in default_models["data"]] == [
        "chat-only",
        "claude-only",
        "responses-only",
    ]
    by_id = {item["id"]: item for item in default_models["data"]}
    assert by_id["responses-only"]["object"] == "model"
    assert by_id["responses-only"]["x_local_gateway"] == {
        "supported_endpoints": ["/v1/responses"],
    }
    assert [item["id"] for item in responses_models["data"]] == ["responses-only"]
    assert [item["id"] for item in claude_models["data"]] == ["claude-only"]


def test_aggregate_catalog_deduplicates_routed_multi_protocol_models(
    client, admin_headers
):
    provider = client.post(
        "/api/admin/v1/providers",
        headers=admin_headers,
        json={"name": "Catalog provider", "base_url": "https://catalog.test"},
    ).json()
    channel = client.post(
        "/api/admin/v1/channels",
        headers=admin_headers,
        json={
            "provider_id": provider["id"],
            "name": "catalog-channel",
            "protocols": ["openai_compatible", "openai_responses"],
            "api_key": "secret",
        },
    ).json()
    model = client.post(
        f"/api/admin/v1/channels/{channel['id']}/models",
        headers=admin_headers,
        json={"model_id": "shared-unrouted"},
    ).json()

    assert client.get("/v1/models").json()["data"] == []

    route = client.post(
        "/api/admin/v1/routes",
        headers=admin_headers,
        json={"requested_model_id": "shared-unrouted"},
    ).json()
    client.put(
        f"/api/admin/v1/routes/{route['id']}/candidates",
        headers=admin_headers,
        json={"candidates": [{"channel_model_id": model["id"], "priority": 0}]},
    ).raise_for_status()

    data = client.get("/v1/models").json()["data"]

    assert len(data) == 1
    assert data[0]["id"] == "shared-unrouted"
    assert data[0]["x_local_gateway"] == {
        "supported_endpoints": ["/v1/chat/completions", "/v1/responses"]
    }


def test_v1_models_falls_back_to_responses_when_compatible_pool_is_empty(
    client, admin_headers
):
    configure_route(
        client,
        admin_headers,
        protocol="openai_responses",
        model_id="responses-only",
    )

    response = client.get("/v1/models")

    assert response.status_code == 200
    assert [item["id"] for item in response.json()["data"]] == ["responses-only"]


def test_model_catalog_excludes_disabled_routes(client, admin_headers):
    _, _, route = configure_route(client, admin_headers, model_id="disabled-model")
    client.patch(
        f"/api/admin/v1/routes/{route['id']}",
        headers=admin_headers,
        json={"enabled": False},
    ).raise_for_status()

    assert client.get("/v1/models").json()["data"] == []


def test_model_catalog_uses_gateway_access_policy(client, admin_headers):
    configure_route(
        client,
        admin_headers,
        protocol="openai_responses",
        model_id="secured-model",
    )
    client.patch(
        "/api/admin/v1/settings",
        headers=admin_headers,
        json={
            "trust_local_network": False,
            "admin_access_key": "admin-runtime-key",
            "gateway_access_key": "gateway-runtime-key",
        },
    ).raise_for_status()

    assert client.get("/v1/responses/models").status_code == 401
    response = client.get(
        "/v1/responses/models",
        headers={"Authorization": "Bearer gateway-runtime-key"},
    )
    assert response.status_code == 200
    assert [item["id"] for item in response.json()["data"]] == ["secured-model"]
    aggregate = client.get("/v1/models", headers={"x-api-key": "gateway-runtime-key"})
    assert aggregate.status_code == 200
    assert [item["id"] for item in aggregate.json()["data"]] == ["secured-model"]
