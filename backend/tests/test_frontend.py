from pathlib import Path

import pytest


def test_spa_history_route_returns_frontend(client):
    frontend_public = Path(__file__).resolve().parents[2] / "frontend" / "public"
    if not frontend_public.exists():
        pytest.skip("native frontend is not present")
    response = client.get("/providers")
    assert response.status_code == 200
    assert "<title>Local AI Gateway</title>" in response.text


def test_api_paths_are_not_swallowed_by_spa_fallback(client):
    """旧后端缺路由时，/api、/v1 等路径必须返回真实 404 JSON，
    不能被 SPA 回退成 200 + index.html（否则前端会报 null.items）。"""
    for path in (
        "/api/admin/v1/nonexistent-route",
        "/v1/nonexistent-endpoint",
        "/v1beta/nonexistent",
        "/claudecode/nonexistent",
        "/codex/nonexistent",
    ):
        response = client.get(path)
        assert response.status_code == 404, path
        assert response.headers["content-type"].startswith("application/json"), path


def test_native_frontend_assets_are_served(client):
    script = client.get("/assets/app.js")
    stylesheet = client.get("/assets/app.css")

    assert script.status_code == 200
    assert "const API_ROOT = '/api/admin/v1';" in script.text
    assert 'name="health_check_model_id"' in script.text
    assert "自动（模型列表第一个）" in script.text
    assert "action: 'edit-provider'" in script.text
    assert "await patch(`/providers/${providerId}`, payload);" in script.text
    assert "function tokenCount(value)" in script.text
    assert "1_000_000_000_000" in script.text
    assert "响应渠道" in script.text
    assert "response_channels" in script.text
    assert "function responseChannelTags(items = [])" in script.text
    assert "log-route-tag" in script.text
    assert "function parseStandardTime(value)" in script.text
    assert "`${raw}Z`" in script.text
    assert "function formatLogTime(value)" in script.text
    assert "logs-cell-route" in script.text
    assert "data-log-request-id" in script.text
    assert "function openRequestLog(requestId)" in script.text
    assert "dashboard-token-section" in script.text
    assert "cache-hit-section" in script.text
    assert "data-candidate-drag-handle" in script.text
    assert "priority, enabled: enabledInput.checked" in script.text
    assert "列表越靠上，调度优先级越高" in script.text
    assert "function capabilityEditorMarkup(route)" in script.text
    assert "结果已显示在下方表单中" in script.text
    assert stylesheet.status_code == 200
    assert ".app-shell" in stylesheet.text
    assert ".token-metric" in stylesheet.text
    assert ".cache-hit-ring" in stylesheet.text
    assert ".candidate-drag-handle" in stylesheet.text
