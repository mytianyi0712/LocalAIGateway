"""Tests for reusable capability profiles (capability_profiles table)."""

from tests.conftest import configure_route


def create_profile(client, admin_headers, name="GPT-5.6 系列", **overrides):
    payload = {
        "name": name,
        "description": "GPT-5.6 三档共用能力",
        "context_window": 1050000,
        "max_tokens": 128000,
        "supports_image_input": True,
        "reasoning": True,
        "thinking_level_map": {"off": "none", "low": "low", "high": "high"},
    }
    payload.update(overrides)
    response = client.post("/api/admin/v1/capability-profiles", headers=admin_headers, json=payload)
    assert response.status_code == 201, response.text
    return response.json()


def bind_profile(client, admin_headers, model_id, profile_id):
    return client.put(
        f"/api/admin/v1/model-capabilities/{model_id}",
        headers=admin_headers,
        json={
            "source": "manual",
            "profile_id": profile_id,
            "cost_input": 5,
            "cost_output": 30,
        },
    )


def test_profile_crud_and_unique_name(client, admin_headers):
    profile = create_profile(client, admin_headers)
    assert profile["name"] == "GPT-5.6 系列"
    assert profile["capabilities"]["context_window"] == 1050000
    assert profile["usage_count"] == 0
    assert profile["used_by"] == []

    listed = client.get("/api/admin/v1/capability-profiles", headers=admin_headers).json()
    assert listed["total"] == 1
    assert listed["items"][0]["id"] == profile["id"]

    duplicate = client.post(
        "/api/admin/v1/capability-profiles",
        headers=admin_headers,
        json={"name": "GPT-5.6 系列"},
    )
    assert duplicate.status_code == 409

    updated = client.put(
        f"/api/admin/v1/capability-profiles/{profile['id']}",
        headers=admin_headers,
        json={
            "name": "GPT-5.6 三档",
            "description": "改名",
            "context_window": 1500000,
            "max_tokens": 128000,
            "supports_image_input": True,
            "reasoning": True,
            "thinking_level_map": None,
        },
    )
    assert updated.status_code == 200, updated.text
    assert updated.json()["name"] == "GPT-5.6 三档"
    assert updated.json()["capabilities"]["context_window"] == 1500000

    deleted = client.delete(
        f"/api/admin/v1/capability-profiles/{profile['id']}", headers=admin_headers
    )
    assert deleted.status_code == 204
    assert client.get("/api/admin/v1/capability-profiles", headers=admin_headers).json()["total"] == 0


def test_apply_profile_to_model_binds_and_fills_capabilities(client, admin_headers):
    configure_route(client, admin_headers, model_id="gpt-5.6-sol")
    profile = create_profile(client, admin_headers)

    response = bind_profile(client, admin_headers, "gpt-5.6-sol", profile["id"])
    assert response.status_code == 200, response.text
    caps = response.json()
    assert caps["source"] == "manual"
    assert caps["profile_id"] == profile["id"]
    assert caps["profile_name"] == "GPT-5.6 系列"
    assert caps["context_window"] == 1050000
    assert caps["max_tokens"] == 128000
    assert caps["supports_image_input"] is True
    assert caps["reasoning"] is True
    # 成本不在档案内，保留请求里的独立配置。
    assert caps["cost"]["input"] == 5
    assert caps["cost"]["output"] == 30

    read_back = client.get(
        "/api/admin/v1/model-capabilities/gpt-5.6-sol", headers=admin_headers
    ).json()
    assert read_back["profile_id"] == profile["id"]
    assert read_back["profile_name"] == "GPT-5.6 系列"

    bundle = client.get("/api/admin/v1/routes", headers=admin_headers).json()["items"][0]
    assert bundle["capabilities"]["profile_id"] == profile["id"]

    listed = client.get("/api/admin/v1/capability-profiles", headers=admin_headers).json()
    assert listed["items"][0]["usage_count"] == 1
    assert listed["items"][0]["used_by"] == ["gpt-5.6-sol"]


def test_profile_update_propagates_to_bound_models(client, admin_headers):
    configure_route(client, admin_headers, model_id="gpt-5.6-sol")
    configure_route(
        client, admin_headers, model_id="gpt-5.6-terra", provider_name="Mock upstream 2"
    )
    profile = create_profile(client, admin_headers)
    bind_profile(client, admin_headers, "gpt-5.6-sol", profile["id"])
    bind_profile(client, admin_headers, "gpt-5.6-terra", profile["id"])

    response = client.put(
        f"/api/admin/v1/capability-profiles/{profile['id']}",
        headers=admin_headers,
        json={
            "name": "GPT-5.6 系列",
            "description": "共享能力",
            "context_window": 1500000,
            "max_tokens": 256000,
            "supports_image_input": True,
            "reasoning": True,
            "thinking_level_map": {"max": "max"},
        },
    )
    assert response.status_code == 200, response.text
    for model_id in ("gpt-5.6-sol", "gpt-5.6-terra"):
        caps = client.get(
            f"/api/admin/v1/model-capabilities/{model_id}", headers=admin_headers
        ).json()
        assert caps["context_window"] == 1500000
        assert caps["max_tokens"] == 256000
        assert caps["thinking_level_map"] == {"max": "max"}
        # 成本不受档案更新影响。
        assert caps["cost"]["input"] == 5


def test_manual_edit_without_profile_detaches_model(client, admin_headers):
    configure_route(client, admin_headers, model_id="model-x")
    profile = create_profile(client, admin_headers)
    bind_profile(client, admin_headers, "model-x", profile["id"])

    response = client.put(
        "/api/admin/v1/model-capabilities/model-x",
        headers=admin_headers,
        json={"source": "manual", "context_window": 400000},
    )
    assert response.status_code == 200, response.text
    caps = response.json()
    assert caps["profile_id"] is None
    assert caps["profile_name"] is None
    assert caps["context_window"] == 400000
    listed = client.get("/api/admin/v1/capability-profiles", headers=admin_headers).json()
    assert listed["items"][0]["usage_count"] == 0


def test_invalid_profile_id_rejected_and_auto_source_detaches(client, admin_headers):
    configure_route(client, admin_headers, model_id="model-x")
    missing = client.put(
        "/api/admin/v1/model-capabilities/model-x",
        headers=admin_headers,
        json={"source": "manual", "profile_id": "no-such-profile"},
    )
    assert missing.status_code == 404

    profile = create_profile(client, admin_headers)
    bind_profile(client, admin_headers, "model-x", profile["id"])
    auto = client.put(
        "/api/admin/v1/model-capabilities/model-x",
        headers=admin_headers,
        json={"source": "auto"},
    )
    assert auto.status_code == 200, auto.text
    assert auto.json()["profile_id"] is None
    assert auto.json()["source"] == "auto"


def test_delete_profile_unbinds_but_keeps_applied_values(client, admin_headers):
    configure_route(client, admin_headers, model_id="model-x")
    profile = create_profile(client, admin_headers)
    bind_profile(client, admin_headers, "model-x", profile["id"])

    deleted = client.delete(
        f"/api/admin/v1/capability-profiles/{profile['id']}", headers=admin_headers
    )
    assert deleted.status_code == 204

    caps = client.get("/api/admin/v1/model-capabilities/model-x", headers=admin_headers).json()
    assert caps["profile_id"] is None
    assert caps["profile_name"] is None
    # 已应用的能力值保留。
    assert caps["context_window"] == 1050000
    assert caps["max_tokens"] == 128000
    assert caps["reasoning"] is True


def test_profile_explicit_fields_override_profile_values(client, admin_headers):
    configure_route(client, admin_headers, model_id="model-x")
    profile = create_profile(client, admin_headers)

    response = client.put(
        "/api/admin/v1/model-capabilities/model-x",
        headers=admin_headers,
        json={
            "source": "manual",
            "profile_id": profile["id"],
            "context_window": 300000,
            "cost_input": 9,
        },
    )
    assert response.status_code == 200, response.text
    caps = response.json()
    assert caps["profile_id"] == profile["id"]
    assert caps["context_window"] == 300000
    assert caps["max_tokens"] == 128000  # 未给出，从档案补齐
    assert caps["cost"]["input"] == 9
