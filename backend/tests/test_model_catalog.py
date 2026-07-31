import anyio
import pytest
from sqlalchemy import select

from app.db.models import ChannelModel
from app.main import app
from app.services.capabilities import extract_capabilities, pi_model_config
from tests.conftest import configure_route


def test_model_catalog_allows_local_desktop_cors_preflight(client):
    response = client.options(
        "/v1/models",
        headers={
            "Origin": "onlyoffice://desktop",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "authorization,content-type",
        },
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "onlyoffice://desktop"
    assert "authorization" in response.headers["access-control-allow-headers"].lower()


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


def test_model_catalog_exposes_manual_pi_capabilities(client, admin_headers):
    configure_route(client, admin_headers, model_id="capable-model")
    response = client.put(
        "/api/admin/v1/model-capabilities/capable-model",
        headers=admin_headers,
        json={
            "source": "manual",
            "context_window": 128000,
            "max_tokens": 8192,
            "supports_image_input": True,
            "reasoning": True,
            "thinking_level_map": {"high": "default", "max": "max"},
            "cost_input": 1.25,
            "cost_output": 5,
            "cost_cache_read": 0.2,
            "cost_cache_write": 1,
        },
    )
    assert response.status_code == 200, response.text

    item = client.get("/v1/models").json()["data"][0]

    metadata = item["x_local_gateway"]
    assert metadata["capabilities"]["context_window"] == 128000
    assert metadata["capabilities"]["supports_image_input"] is True
    assert metadata["pi_model_config"] == {
        "input": ["text", "image"],
        "reasoning": True,
        "cost": {
            "input": 1.25,
            "output": 5.0,
            "cacheRead": 0.2,
            "cacheWrite": 1.0,
        },
        "contextWindow": 128000,
        "maxTokens": 8192,
        "thinkingLevelMap": {"high": "default", "max": "max"},
    }


def test_auto_capability_detection_uses_conservative_values(client, admin_headers):
    configure_route(client, admin_headers, model_id="auto-capable")

    async def seed_metadata():
        async with app.state.db.sessions() as session:
            rows = (
                await session.execute(
                    select(ChannelModel).where(ChannelModel.model_id == "auto-capable")
                )
            ).scalars().all()
            rows[0].metadata_json = {
                "openai_compatible": {
                    "context_window": 128000,
                    "max_tokens": 16000,
                    "input_modalities": ["text", "image"],
                    "reasoning": True,
                }
            }
            rows[1].metadata_json = {
                "openai_compatible": {
                    "context_window": 64000,
                    "max_tokens": 8000,
                    "input_modalities": ["text"],
                    "reasoning": True,
                }
            }
            await session.commit()

    anyio.run(seed_metadata)

    response = client.post(
        "/api/admin/v1/model-capabilities/detect/auto-capable",
        headers=admin_headers,
    )
    assert response.status_code == 200, response.text

    capabilities = response.json()
    assert capabilities["source"] == "auto"
    assert capabilities["context_window"] == 64000
    assert capabilities["max_tokens"] == 8000
    assert capabilities["supports_image_input"] is False
    assert capabilities["reasoning"] is True

    item = client.get("/v1/models").json()["data"][0]
    assert item["x_local_gateway"]["pi_model_config"]["input"] == ["text"]
    assert item["x_local_gateway"]["pi_model_config"]["contextWindow"] == 64000


def test_capability_detection_understands_reasoning_effort_catalog_shape():
    capabilities = extract_capabilities(
        {
            "supportsReasoningEffort": True,
            "reasoningEffort": "high",
            "reasoningEfforts": [
                {"value": "low", "label": "Low"},
                {"value": "medium", "label": "Medium"},
                {"value": "high", "label": "High", "default": True},
            ],
        }
    )

    assert capabilities["reasoning"] is True
    assert capabilities["thinking_level_map"] == {
        "low": "low",
        "medium": "medium",
        "high": "high",
    }
    assert pi_model_config(capabilities) == {
        "reasoning": True,
        "thinkingLevelMap": {
            "low": "low",
            "medium": "medium",
            "high": "high",
        },
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
