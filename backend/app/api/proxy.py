from typing import Literal

from fastapi import APIRouter, Depends, Header, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_session
from app.adapters import PROTOCOL_ENDPOINTS
from app.services.catalog import (
    iso_timestamp,
    list_all_routable_models,
    list_routable_models,
    unix_timestamp,
)
from app.services.proxy import gateway_access_error, proxy_request


router = APIRouter()


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
                "x_local_gateway": {
                    "supported_endpoints": [
                        endpoint
                        for protocol in item["protocols"]
                        for endpoint in PROTOCOL_ENDPOINTS[protocol]
                    ],
                },
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
