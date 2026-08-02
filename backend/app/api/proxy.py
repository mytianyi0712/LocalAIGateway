from typing import Literal

from fastapi import APIRouter, Depends, Header, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_session
from app.adapters import PROTOCOL_ENDPOINTS
from app.services.capabilities import pi_model_config
from app.services.catalog import (
    iso_timestamp,
    list_all_routable_models,
    list_claude_mapping_models,
    list_codex_mapping_models,
    list_routable_models,
    unix_timestamp,
)
from app.services.proxy import (
    gateway_access_error,
    proxy_codex_entry,
    proxy_mapped_entry,
    proxy_request,
)


router = APIRouter()


def has_capability_data(capabilities: dict) -> bool:
    cost = capabilities.get("cost") or {}
    return any(
        capabilities.get(key) is not None
        for key in ("context_window", "max_tokens", "supports_image_input", "reasoning", "thinking_level_map")
    ) or any(cost.get(key) is not None for key in ("input", "output", "cacheRead", "cacheWrite"))


def gateway_metadata(item: dict, protocols: list[str] | None = None) -> dict:
    capabilities = item.get("capabilities") or {}
    selected_protocols = protocols or item.get("protocols") or []
    metadata = {
        "supported_endpoints": [
            endpoint
            for protocol in selected_protocols
            for endpoint in PROTOCOL_ENDPOINTS[protocol]
        ],
    }
    item_gateway = item.get("x_local_gateway")
    if isinstance(item_gateway, dict) and item_gateway.get("mapping"):
        metadata["mapping"] = item_gateway["mapping"]
    if has_capability_data(capabilities):
        metadata["capabilities"] = capabilities
        metadata["pi_model_config"] = pi_model_config(capabilities)
    return metadata


async def aggregate_model_catalog_response(request: Request, session: AsyncSession):
    access_error = await gateway_access_error(request, "catalog")
    if access_error:
        return access_error
    models = await list_all_routable_models(session)
    return {
        "object": "list",
        "data": [
            {
                "id": item["id"],
                "object": "model",
                "created": unix_timestamp(item["created_at"]),
                "owned_by": "local-ai-gateway",
                "x_local_gateway": gateway_metadata(item),
            }
            for item in models
        ],
    }


async def model_catalog_response(
    request: Request,
    session: AsyncSession,
    protocol: str,
    models: list[dict] | None = None,
):
    access_error = await gateway_access_error(request, protocol)
    if access_error:
        return access_error
    if models is None:
        models = await list_routable_models(session, protocol)
    if protocol in {"openai_compatible", "openai_responses"}:
        return {
            "object": "list",
            "data": [
                {
                    "id": item["id"],
                    "object": "model",
                    "created": unix_timestamp(item["created_at"]),
                    "owned_by": "local-ai-gateway",
                    "x_local_gateway": gateway_metadata(item, [protocol]),
                }
                for item in models
            ],
        }
    if protocol == "claude":
        data = [
            {
                "type": "model",
                "id": item["id"],
                "display_name": item["display_name"],
                "created_at": iso_timestamp(item["created_at"]),
                "x_local_gateway": gateway_metadata(item, [protocol]),
            }
            for item in models
        ]
        return {
            "data": data,
            "has_more": False,
            "first_id": data[0]["id"] if data else None,
            "last_id": data[-1]["id"] if data else None,
        }
    return {
        "models": [
            {
                "name": f"models/{item['id']}",
                "baseModelId": item["id"],
                "version": item["id"],
                "displayName": item["display_name"],
                "supportedGenerationMethods": ["generateContent", "streamGenerateContent"],
                "x_local_gateway": gateway_metadata(item, [protocol]),
            }
            for item in models
        ]
    }


@router.get("/v1/models")
async def openai_or_claude_models(
    request: Request,
    protocol: Literal["openai_compatible", "openai_responses", "claude"] | None = Query(
        default=None
    ),
    gateway_protocol: str | None = Header(default=None, alias="X-Local-Gateway-Protocol"),
    session: AsyncSession = Depends(get_session),
):
    selected = protocol or gateway_protocol
    if selected is None:
        if request.headers.get("anthropic-version"):
            selected = "claude"
        else:
            return await aggregate_model_catalog_response(request, session)
    if selected not in {"openai_compatible", "openai_responses", "claude"}:
        return JSONResponse(
            {"error": {"message": "Unsupported model catalog protocol."}}, status_code=400
        )
    return await model_catalog_response(request, session, selected)


@router.get("/v1/responses/models")
async def openai_responses_models(
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    return await model_catalog_response(request, session, "openai_responses")


@router.get("/v1/messages/models")
async def claude_models(
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    return await model_catalog_response(request, session, "claude")


@router.get("/v1beta/models")
async def gemini_models(
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    return await model_catalog_response(request, session, "gemini")


@router.post("/v1/chat/completions")
@router.post("/v1/completions")
@router.post("/v1/embeddings")
async def openai_compatible_proxy(request: Request):
    return await proxy_request(request, "openai_compatible")


@router.post("/v1/responses")
async def openai_responses_proxy(request: Request):
    return await proxy_request(request, "openai_responses")


@router.post("/v1/messages")
async def claude_proxy(request: Request):
    return await proxy_request(request, "claude")


@router.get("/claudecode")
async def claudecode_info(request: Request):
    access_error = await gateway_access_error(request, "claude")
    if access_error:
        return access_error
    return {
        "name": "Local AI Gateway Claude model mapping",
        "base_url": "/claudecode",
        "endpoints": [
            "GET /claudecode/v1/models",
            "GET /claudecode/v1/messages/models",
            "POST /claudecode/v1/messages",
        ],
        "configure": {
            "ANTHROPIC_BASE_URL": "http://<host>:3000/claudecode",
            "ANTHROPIC_API_KEY": "<any value>",
            "ANTHROPIC_MODEL": "<mapped model id, e.g. claude-opus-5>",
        },
    }


@router.get("/claudecode/v1/models")
@router.get("/claudecode/v1/messages/models")
async def claudecode_models(
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    access_error = await gateway_access_error(request, "claude")
    if access_error:
        return access_error
    models = await list_claude_mapping_models(session)
    return await model_catalog_response(request, session, "claude", models)


@router.post("/claudecode/v1/messages")
async def claudecode_messages(request: Request):
    return await proxy_mapped_entry(request)


@router.get("/codex")
async def codex_info(request: Request):
    access_error = await gateway_access_error(request, "openai_responses")
    if access_error:
        return access_error
    # Codex CLI appends `/responses` and `/models` to the configured base URL,
    # so the base must include the `/v1` prefix (like https://api.openai.com/v1).
    # The canonical way to point Codex at a proxy is `openai_base_url` (or a
    # custom `[model_providers.<id>]` entry) in `~/.codex/config.toml`.
    return {
        "name": "Local AI Gateway Codex model mapping",
        "base_url": "/codex/v1",
        "endpoints": [
            "GET /codex/v1/models",
            "GET /codex/v1/responses/models",
            "POST /codex/v1/responses",
        ],
        "configure": {
            "config_toml": {
                "openai_base_url": "http://<host>:3000/codex/v1",
                "model": "<mapped model id, e.g. gpt-5-codex>",
            },
            "OPENAI_API_KEY": "<any value>",
        },
    }


@router.get("/codex/v1/models")
@router.get("/codex/v1/responses/models")
async def codex_models(
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    access_error = await gateway_access_error(request, "openai_responses")
    if access_error:
        return access_error
    models = await list_codex_mapping_models(session)
    return await model_catalog_response(request, session, "openai_responses", models)


@router.post("/codex/v1/responses")
async def codex_responses(request: Request):
    return await proxy_codex_entry(request)


@router.post("/v1beta/models/{model_action:path}")
async def gemini_proxy(request: Request, model_action: str):
    if not (
        model_action.endswith(":generateContent") or model_action.endswith(":streamGenerateContent")
    ):
        return JSONResponse(
            {
                "error": {
                    "code": 404,
                    "message": "Unsupported Gemini endpoint",
                    "status": "NOT_FOUND",
                }
            },
            status_code=404,
        )
    return await proxy_request(request, "gemini")
