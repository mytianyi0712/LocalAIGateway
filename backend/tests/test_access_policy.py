import httpx
import respx

from tests.conftest import configure_route


def test_trusted_local_network_allows_missing_or_arbitrary_keys(client, admin_headers):
    configure_route(client, admin_headers)
    assert client.get("/api/admin/v1/system/status").status_code == 200

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/chat/completions").mock(
            return_value=httpx.Response(200, content=b"trusted")
        )
        response = client.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer arbitrary-client-value"},
            content=b'{"model":"model-x","messages":[]}',
        )
    assert response.status_code == 200
    assert response.content == b"trusted"


def test_disabling_trust_requires_separate_admin_and_gateway_keys(client, admin_headers):
    configure_route(client, admin_headers)
    client.patch(
        "/api/admin/v1/settings",
        headers=admin_headers,
        json={
            "trust_local_network": False,
            "admin_access_key": "admin-runtime-key",
            "gateway_access_key": "gateway-runtime-key",
        },
    ).raise_for_status()

    assert client.get("/api/admin/v1/system/status").status_code == 401
    assert client.get(
        "/api/admin/v1/system/status",
        headers={"Authorization": "Bearer admin-runtime-key"},
    ).status_code == 200

    with respx.mock(assert_all_called=False) as mock:
        mock.post("https://upstream.test/v1/chat/completions").mock(
            return_value=httpx.Response(200, content=b"secure")
        )
        body = b'{"model":"model-x","messages":[]}'
        assert client.post("/v1/chat/completions", content=body).status_code == 401
        assert client.post(
            "/v1/chat/completions",
            headers={"X-Local-Gateway-Key": "wrong"},
            content=body,
        ).status_code == 401
        response = client.post(
            "/v1/chat/completions",
            headers={"X-Local-Gateway-Key": "gateway-runtime-key"},
            content=body,
        )
    assert response.status_code == 200
    assert response.content == b"secure"


def test_access_key_generation_returns_keys_once(client, admin_headers):
    result = client.post(
        "/api/admin/v1/settings/access-keys/generate",
        headers=admin_headers,
    )
    assert result.status_code == 200
    payload = result.json()
    assert len(payload["admin_access_key"]) >= 40
    assert len(payload["gateway_access_key"]) >= 40
    settings = client.get("/api/admin/v1/settings", headers=admin_headers).json()
    assert settings["admin_key_hint"].endswith(payload["admin_access_key"][-4:])
    assert "admin_access_key" not in settings


def test_trust_cannot_be_disabled_without_access_keys(client):
    client.app.state.settings.admin_token = ""
    client.app.state.settings.gateway_key = ""

    response = client.patch(
        "/api/admin/v1/settings",
        json={"trust_local_network": False},
    )

    assert response.status_code == 422
    assert client.get("/api/admin/v1/system/status").json()["trust_local_network"] is True
