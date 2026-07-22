import pytest
from fastapi.testclient import TestClient

from app.core.config import get_settings
from app.main import app


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("AI_GATEWAY_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("AI_GATEWAY_ADMIN_TOKEN", "test-admin")
    monkeypatch.setenv("AI_GATEWAY_GATEWAY_KEY", "test-gateway")
    get_settings.cache_clear()
    with TestClient(app) as test_client:
        yield test_client
    get_settings.cache_clear()


@pytest.fixture
def admin_headers():
    return {"Authorization": "Bearer test-admin"}


def configure_route(
    client,
    admin_headers,
    protocol="openai_compatible",
    model_id="model-x",
    provider_name="Mock upstream",
):
    provider = client.post(
        "/api/admin/v1/providers",
        headers=admin_headers,
        json={"name": provider_name, "base_url": "https://upstream.test"},
    ).json()
    configured = []
    for name, key in (("primary", "key-primary"), ("backup", "key-backup")):
        channel_response = client.post(
            "/api/admin/v1/channels",
            headers=admin_headers,
            json={
                "provider_id": provider["id"],
                "name": name,
                "protocol": protocol,
                "api_key": key,
            },
        )
        assert channel_response.status_code == 201, channel_response.text
        channel = channel_response.json()
        model_response = client.post(
            f"/api/admin/v1/channels/{channel['id']}/models",
            headers=admin_headers,
            json={"model_id": model_id},
        )
        assert model_response.status_code == 201, model_response.text
        configured.append((channel, model_response.json()))
    route_response = client.post(
        "/api/admin/v1/routes",
        headers=admin_headers,
        json={"protocol": protocol, "requested_model_id": model_id},
    )
    assert route_response.status_code == 201, route_response.text
    route = route_response.json()
    candidates_response = client.put(
        f"/api/admin/v1/routes/{route['id']}/candidates",
        headers=admin_headers,
        json={
            "candidates": [
                {"channel_model_id": configured[0][1]["id"], "priority": 0},
                {"channel_model_id": configured[1][1]["id"], "priority": 1},
            ]
        },
    )
    assert candidates_response.status_code == 200, candidates_response.text
    return provider, configured, route
