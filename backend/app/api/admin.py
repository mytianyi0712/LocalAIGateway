import asyncio
import secrets as stdlib_secrets
from datetime import datetime
from typing import Any, Literal
from urllib.parse import urlsplit

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field, field_validator, model_validator
from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.adapters import (
    PROTOCOL_ENDPOINTS,
    PROTOCOL_ORDER,
    PROTOCOLS,
    SHARED_DISCOVERY_PROTOCOL_GROUPS,
)
from app.adapters.base import normalize_base_url
from app.api.deps import get_session, require_admin
from app.core.access import resolve_access_policy
from app.core.security import SecretStore
from app.db.models import (
    AppSetting,
    Channel,
    ChannelHealth,
    ChannelModel,
    ChannelModelProtocol,
    ChannelProtocol,
    DiscoveryRun,
    HealthProbeLog,
    ModelRoute,
    Provider,
    RequestAttempt,
    RequestLog,
    RouteCandidate,
)
from app.services.discovery import discover_channel_models
from app.services.health import probe_channel
from app.services.routing import synchronize_shared_protocol_routes
from app.services.settings import get_access_policy, get_runtime_settings, update_runtime_settings


router = APIRouter(prefix="/api/admin/v1", dependencies=[Depends(require_admin)])


def output_tps(output_tokens: int | None, duration_ms: int | None) -> float | None:
    if output_tokens is None or duration_ms is None or duration_ms <= 0:
        return None
    return round(output_tokens * 1000 / duration_ms, 3)


CACHE_PROVIDER_PROTOCOLS = {
    "openai_compatible": "OpenAI",
    "openai_responses": "OpenAI",
    "claude": "Claude",
    "gemini": "Gemini",
}
CACHE_PROVIDER_ORDER = ("OpenAI", "Claude", "Gemini")


class ProviderInput(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    base_url: str

    @field_validator("base_url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        value = value.rstrip("/")
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("base_url must be an absolute HTTP(S) URL")
        return normalize_base_url(value)


class ProviderPatch(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=120)
    base_url: str | None = None


class ChannelInput(BaseModel):
    provider_id: str
    name: str = Field(min_length=1, max_length=120)
    protocol: str | None = None
    protocols: list[str] = Field(default_factory=list)
    api_key: str = Field(min_length=1)
    manual_enabled: bool = True
    health_check_model_id: str | None = None

    @model_validator(mode="after")
    def validate_protocols(self):
        selected = self.protocols or ([self.protocol] if self.protocol else [])
        selected = list(dict.fromkeys(selected))
        if not selected:
            raise ValueError("At least one protocol is required")
        if any(value not in PROTOCOLS for value in selected):
            raise ValueError("Unsupported protocol")
        self.protocols = selected
        self.protocol = selected[0]
        return self


class ChannelPatch(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=120)
    protocol: str | None = None
    protocols: list[str] | None = None
    manual_enabled: bool | None = None
    health_check_model_id: str | None = None

    @model_validator(mode="after")
    def validate_protocols(self):
        selected = self.protocols
        if selected is None and self.protocol is not None:
            selected = [self.protocol]
        if selected is not None:
            selected = list(dict.fromkeys(selected))
            if not selected:
                raise ValueError("At least one protocol is required")
            if any(value not in PROTOCOLS for value in selected):
                raise ValueError("Unsupported protocol")
            self.protocols = selected
            self.protocol = selected[0]
        return self


class ApiKeyInput(BaseModel):
    api_key: str = Field(min_length=1)


class ManualModelInput(BaseModel):
    model_id: str = Field(min_length=1, max_length=255)
    display_name: str | None = None
    protocols: list[str] | None = None

    @field_validator("protocols")
    @classmethod
    def validate_protocols(cls, value: list[str] | None):
        if value is not None and (not value or any(item not in PROTOCOLS for item in value)):
            raise ValueError("Unsupported or empty protocols")
        return list(dict.fromkeys(value)) if value is not None else None


class ChannelModelPatch(BaseModel):
    display_name: str | None = None
    available: bool | None = None
    protocols: list[str] | None = None

    @field_validator("protocols")
    @classmethod
    def validate_protocols(cls, value: list[str] | None):
        if value is not None and (not value or any(item not in PROTOCOLS for item in value)):
            raise ValueError("Unsupported or empty protocols")
        return list(dict.fromkeys(value)) if value is not None else None


class RouteInput(BaseModel):
    protocol: str | None = None
    requested_model_id: str = Field(min_length=1)
    enabled: bool = True


class RoutePatch(BaseModel):
    enabled: bool


class CandidateInput(BaseModel):
    channel_model_id: str
    priority: int = Field(ge=0)
    enabled: bool = True


class CandidateListInput(BaseModel):
    candidates: list[CandidateInput]


def provider_json(row: Provider) -> dict:
    return {
        "id": row.id,
        "name": row.name,
        "base_url": row.base_url,
        "channel_count": len(row.channels) if "channels" in row.__dict__ else None,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def health_json(row: ChannelHealth | None) -> dict:
    return {
        "state": row.state if row else "active",
        "consecutive_failures": row.consecutive_failures if row else 0,
        "disabled_until": row.disabled_until if row else None,
        "last_success_at": row.last_success_at if row else None,
        "last_failure_at": row.last_failure_at if row else None,
        "last_error_kind": row.last_error_kind if row else None,
        "last_status_code": row.last_status_code if row else None,
    }


def ordered_protocols(values) -> list[str]:
    selected = {value.protocol if hasattr(value, "protocol") else value for value in values}
    return [protocol for protocol in PROTOCOL_ORDER if protocol in selected]


def channel_json(row: Channel) -> dict:
    protocols = (
        ordered_protocols(row.protocol_bindings)
        if "protocol_bindings" in row.__dict__
        else [row.protocol]
    )
    return {
        "id": row.id,
        "provider_id": row.provider_id,
        "provider_name": row.provider.name if "provider" in row.__dict__ else None,
        "name": row.name,
        "protocol": row.protocol,
        "protocols": protocols,
        "manual_enabled": row.manual_enabled,
        "health_check_model_id": row.health_check_model_id,
        "has_api_key": bool(row.api_key_encrypted),
        "api_key_hint": row.api_key_hint,
        "health": health_json(row.health if "health" in row.__dict__ else None),
        "model_count": len(row.models) if "models" in row.__dict__ else None,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def channel_model_json(row: ChannelModel) -> dict:
    protocols = (
        ordered_protocols(row.protocol_bindings)
        if "protocol_bindings" in row.__dict__
        else [row.channel.protocol]
    )
    return {
        "id": row.id,
        "channel_id": row.channel_id,
        "channel_name": row.channel.name if "channel" in row.__dict__ else None,
        "protocol": protocols[0] if protocols else None,
        "protocols": protocols,
        "model_id": row.model_id,
        "display_name": row.display_name,
        "source": row.source,
        "available": row.available,
        "last_seen_at": row.last_seen_at,
    }


def route_bundle_json(rows: list[ModelRoute]) -> dict:
    ordered_rows = sorted(rows, key=lambda row: PROTOCOL_ORDER.index(row.protocol))
    candidates_by_model: dict[str, dict] = {}
    for row in ordered_rows:
        for item in row.candidates:
            channel_model = item.channel_model
            channel = channel_model.channel
            candidate = candidates_by_model.get(channel_model.id)
            if candidate is None:
                candidate = {
                    "id": item.id,
                    "channel_model_id": channel_model.id,
                    "channel_id": channel.id,
                    "channel_name": channel.name,
                    "provider_name": channel.provider.name,
                    "priority": item.priority,
                    "enabled": item.enabled,
                    "health_state": channel.health.state,
                    "manual_enabled": channel.manual_enabled,
                    "protocols": [],
                }
                candidates_by_model[channel_model.id] = candidate
            else:
                candidate["priority"] = min(candidate["priority"], item.priority)
                candidate["enabled"] = candidate["enabled"] and item.enabled
            candidate["protocols"].append(row.protocol)
    candidates = sorted(
        candidates_by_model.values(), key=lambda item: (item["priority"], item["channel_name"])
    )
    representative = ordered_rows[0]
    return {
        "id": representative.id,
        "route_ids": {row.protocol: row.id for row in ordered_rows},
        "protocols": [row.protocol for row in ordered_rows],
        "requested_model_id": representative.requested_model_id,
        "enabled": all(row.enabled for row in ordered_rows),
        "candidates": candidates,
        "created_at": min(row.created_at for row in ordered_rows),
        "updated_at": max(row.updated_at for row in ordered_rows),
    }


async def load_route_bundle(session: AsyncSession, model_id: str) -> list[ModelRoute]:
    statement = (
        select(ModelRoute)
        .options(
            selectinload(ModelRoute.candidates)
            .selectinload(RouteCandidate.channel_model)
            .selectinload(ChannelModel.channel)
            .selectinload(Channel.provider),
            selectinload(ModelRoute.candidates)
            .selectinload(RouteCandidate.channel_model)
            .selectinload(ChannelModel.channel)
            .selectinload(Channel.health),
            selectinload(ModelRoute.candidates)
            .selectinload(RouteCandidate.channel_model)
            .selectinload(ChannelModel.protocol_bindings),
        )
        .where(ModelRoute.requested_model_id == model_id)
    )
    return (await session.execute(statement)).scalars().all()


@router.get("/providers")
async def list_providers(session: AsyncSession = Depends(get_session)):
    rows = (
        (
            await session.execute(
                select(Provider).options(selectinload(Provider.channels)).order_by(Provider.name)
            )
        )
        .scalars()
        .all()
    )
    return {
        "items": [provider_json(row) for row in rows],
        "total": len(rows),
        "page": 1,
        "page_size": len(rows),
    }


@router.post("/providers", status_code=201)
async def create_provider(payload: ProviderInput, session: AsyncSession = Depends(get_session)):
    row = Provider(name=payload.name, base_url=payload.base_url)
    session.add(row)
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise HTTPException(409, "Provider name already exists") from exc
    await session.refresh(row)
    return provider_json(row)


@router.get("/providers/{provider_id}")
async def get_provider(provider_id: str, session: AsyncSession = Depends(get_session)):
    statement = (
        select(Provider).options(selectinload(Provider.channels)).where(Provider.id == provider_id)
    )
    row = (await session.execute(statement)).scalar_one_or_none()
    if not row:
        raise HTTPException(404, "Provider not found")
    return provider_json(row)


@router.patch("/providers/{provider_id}")
async def patch_provider(
    provider_id: str, payload: ProviderPatch, session: AsyncSession = Depends(get_session)
):
    row = await session.get(Provider, provider_id)
    if not row:
        raise HTTPException(404, "Provider not found")
    values = payload.model_dump(exclude_unset=True)
    if "base_url" in values:
        values["base_url"] = ProviderInput(name=row.name, base_url=values["base_url"]).base_url
    for key, value in values.items():
        setattr(row, key, value)
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise HTTPException(409, "Provider name already exists") from exc
    return provider_json(row)


@router.delete("/providers/{provider_id}", status_code=204)
async def delete_provider(provider_id: str, session: AsyncSession = Depends(get_session)):
    row = await session.get(Provider, provider_id)
    if not row:
        raise HTTPException(404, "Provider not found")
    count = await session.scalar(
        select(func.count()).select_from(Channel).where(Channel.provider_id == provider_id)
    )
    if count:
        raise HTTPException(409, "Delete provider channels first")
    await session.delete(row)
    await session.commit()
    return Response(status_code=204)


@router.get("/channels")
async def list_channels(
    provider_id: str | None = None,
    protocol: str | None = None,
    session: AsyncSession = Depends(get_session),
):
    statement = select(Channel).options(
        selectinload(Channel.provider),
        selectinload(Channel.health),
        selectinload(Channel.models),
        selectinload(Channel.protocol_bindings),
    )
    if provider_id:
        statement = statement.where(Channel.provider_id == provider_id)
    if protocol:
        statement = statement.join(ChannelProtocol).where(ChannelProtocol.protocol == protocol)
    rows = (await session.execute(statement.order_by(Channel.name))).scalars().all()
    return {
        "items": [channel_json(row) for row in rows],
        "total": len(rows),
        "page": 1,
        "page_size": len(rows),
    }


@router.post("/channels", status_code=201)
async def create_channel(
    payload: ChannelInput, request: Request, session: AsyncSession = Depends(get_session)
):
    if not await session.get(Provider, payload.provider_id):
        raise HTTPException(404, "Provider not found")
    row = Channel(
        provider_id=payload.provider_id,
        name=payload.name,
        protocol=payload.protocol,
        api_key_encrypted=request.app.state.secrets.encrypt(payload.api_key),
        api_key_hint=SecretStore.hint(payload.api_key),
        manual_enabled=payload.manual_enabled,
        health_check_model_id=payload.health_check_model_id,
    )
    row.health = ChannelHealth()
    row.protocol_bindings = [ChannelProtocol(protocol=value) for value in payload.protocols]
    session.add(row)
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise HTTPException(409, "Channel name already exists for this provider") from exc
    statement = (
        select(Channel)
        .options(
            selectinload(Channel.provider),
            selectinload(Channel.health),
            selectinload(Channel.models),
            selectinload(Channel.protocol_bindings),
        )
        .where(Channel.id == row.id)
    )
    row = (await session.execute(statement)).scalar_one()
    return channel_json(row)


@router.get("/channels/{channel_id}")
async def get_channel(channel_id: str, session: AsyncSession = Depends(get_session)):
    statement = (
        select(Channel)
        .options(
            selectinload(Channel.provider),
            selectinload(Channel.health),
            selectinload(Channel.models),
            selectinload(Channel.protocol_bindings),
        )
        .where(Channel.id == channel_id)
    )
    row = (await session.execute(statement)).scalar_one_or_none()
    if not row:
        raise HTTPException(404, "Channel not found")
    return channel_json(row)


@router.patch("/channels/{channel_id}")
async def patch_channel(
    channel_id: str, payload: ChannelPatch, session: AsyncSession = Depends(get_session)
):
    row = (
        await session.execute(
            select(Channel)
            .options(
                selectinload(Channel.protocol_bindings),
                selectinload(Channel.models).selectinload(ChannelModel.protocol_bindings),
            )
            .where(Channel.id == channel_id)
        )
    ).scalar_one_or_none()
    if not row:
        raise HTTPException(404, "Channel not found")
    values = payload.model_dump(exclude_unset=True)
    selected_protocols = values.pop("protocols", None)
    values.pop("protocol", None)
    health_check_model_id = values.get("health_check_model_id")
    if health_check_model_id is not None:
        health_probe_protocol = selected_protocols[0] if selected_protocols else row.protocol
        selected_model = await session.scalar(
            select(ChannelModel.id)
            .join(ChannelModel.protocol_bindings)
            .where(
                ChannelModel.channel_id == channel_id,
                ChannelModel.model_id == health_check_model_id,
                ChannelModel.available.is_(True),
                ChannelModel.protocol_bindings.any(protocol=health_probe_protocol),
            )
        )
        if not selected_model:
            raise HTTPException(422, "Health check model must be an available channel model")
    if selected_protocols is not None:
        existing = {item.protocol for item in row.protocol_bindings}
        selected = set(selected_protocols)
        removed = existing - selected
        if removed:
            route_count = await session.scalar(
                select(func.count())
                .select_from(RouteCandidate)
                .join(ModelRoute, ModelRoute.id == RouteCandidate.route_id)
                .join(ChannelModel, ChannelModel.id == RouteCandidate.channel_model_id)
                .where(
                    ChannelModel.channel_id == channel_id,
                    ModelRoute.protocol.in_(removed),
                )
            )
            if route_count:
                raise HTTPException(
                    409, "Remove route candidates before disabling a channel protocol"
                )
            await session.execute(
                delete(ChannelModelProtocol).where(
                    ChannelModelProtocol.channel_model_id.in_(
                        select(ChannelModel.id).where(ChannelModel.channel_id == channel_id)
                    ),
                    ChannelModelProtocol.protocol.in_(removed),
                )
            )
        for model in row.models:
            bound = {item.protocol for item in model.protocol_bindings}
            projected = bound - removed
            for group in SHARED_DISCOVERY_PROTOCOL_GROUPS:
                if bound & group:
                    projected.update(selected & group)
            for protocol in projected - bound:
                session.add(
                    ChannelModelProtocol(
                        channel_model_id=model.id,
                        protocol=protocol,
                    )
                )
            model.available = bool(projected)
        await session.execute(
            delete(ChannelProtocol).where(ChannelProtocol.channel_id == channel_id)
        )
        await session.flush()
        for protocol in selected_protocols:
            session.add(ChannelProtocol(channel_id=channel_id, protocol=protocol))
        row.protocol = selected_protocols[0]
        await synchronize_shared_protocol_routes(session, row.models, selected)
    for key, value in values.items():
        setattr(row, key, value)
    await session.commit()
    return {
        "id": row.id,
        "protocol": row.protocol,
        "protocols": selected_protocols or ordered_protocols(row.protocol_bindings),
        **values,
    }


@router.put("/channels/{channel_id}/api-key")
async def replace_api_key(
    channel_id: str,
    payload: ApiKeyInput,
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    row = await session.get(Channel, channel_id)
    if not row:
        raise HTTPException(404, "Channel not found")
    row.api_key_encrypted = request.app.state.secrets.encrypt(payload.api_key)
    row.api_key_hint = SecretStore.hint(payload.api_key)
    await session.commit()
    return {"id": row.id, "has_api_key": True, "api_key_hint": row.api_key_hint}


@router.post("/channels/{channel_id}/reset-health")
async def reset_health(channel_id: str, session: AsyncSession = Depends(get_session)):
    health = await session.get(ChannelHealth, channel_id)
    if not health:
        raise HTTPException(404, "Channel not found")
    health.state = "active"
    health.consecutive_failures = 0
    health.disabled_until = None
    health.last_error_kind = None
    health.last_status_code = None
    await session.commit()
    return health_json(health)


@router.post("/channels/{channel_id}/probe", status_code=202)
async def manual_probe(
    channel_id: str, request: Request, session: AsyncSession = Depends(get_session)
):
    if not await session.get(Channel, channel_id):
        raise HTTPException(404, "Channel not found")
    asyncio.create_task(probe_channel(request.app, channel_id))
    return {"status": "queued"}


@router.delete("/channels/{channel_id}", status_code=204)
async def delete_channel(channel_id: str, session: AsyncSession = Depends(get_session)):
    row = await session.get(Channel, channel_id)
    if not row:
        raise HTTPException(404, "Channel not found")
    count = await session.scalar(
        select(func.count())
        .select_from(RouteCandidate)
        .join(ChannelModel)
        .where(ChannelModel.channel_id == channel_id)
    )
    if count:
        raise HTTPException(409, "Remove route candidates before deleting channel")
    await session.delete(row)
    await session.commit()
    return Response(status_code=204)


@router.post("/channels/{channel_id}/discover-models", status_code=202)
async def discover_models(
    channel_id: str, request: Request, session: AsyncSession = Depends(get_session)
):
    if not await session.get(Channel, channel_id):
        raise HTTPException(404, "Channel not found")
    run = DiscoveryRun(channel_id=channel_id, trigger="manual")
    session.add(run)
    await session.commit()
    asyncio.create_task(
        discover_channel_models(
            request.app.state.db.sessions,
            request.app.state.http,
            request.app.state.secrets,
            channel_id,
            run.id,
        )
    )
    return {"run_id": run.id, "status": "queued"}


@router.get("/discovery-runs/{run_id}")
async def get_discovery_run(run_id: str, session: AsyncSession = Depends(get_session)):
    row = await session.get(DiscoveryRun, run_id)
    if not row:
        raise HTTPException(404, "Discovery run not found")
    status = "running" if row.finished_at is None else ("succeeded" if row.success else "failed")
    return {
        "id": row.id,
        "channel_id": row.channel_id,
        "status": status,
        "model_count": row.model_count,
        "status_code": row.status_code,
        "error_kind": row.error_kind,
        "started_at": row.started_at,
        "finished_at": row.finished_at,
    }


@router.get("/channels/{channel_id}/discovery-runs")
async def list_discovery_runs(channel_id: str, session: AsyncSession = Depends(get_session)):
    rows = (
        (
            await session.execute(
                select(DiscoveryRun)
                .where(DiscoveryRun.channel_id == channel_id)
                .order_by(DiscoveryRun.started_at.desc())
                .limit(100)
            )
        )
        .scalars()
        .all()
    )
    return {
        "items": [
            {
                "id": row.id,
                "channel_id": row.channel_id,
                "trigger": row.trigger,
                "started_at": row.started_at,
                "finished_at": row.finished_at,
                "success": row.success,
                "model_count": row.model_count,
                "status_code": row.status_code,
                "error_kind": row.error_kind,
            }
            for row in rows
        ],
        "total": len(rows),
        "page": 1,
        "page_size": len(rows),
    }


@router.get("/channel-models")
async def list_channel_models(
    channel_id: str | None = None,
    protocol: str | None = None,
    session: AsyncSession = Depends(get_session),
):
    statement = select(ChannelModel).options(
        selectinload(ChannelModel.channel), selectinload(ChannelModel.protocol_bindings)
    )
    if channel_id:
        statement = statement.where(ChannelModel.channel_id == channel_id)
    if protocol:
        statement = statement.join(ChannelModelProtocol).where(
            ChannelModelProtocol.protocol == protocol
        )
    rows = (await session.execute(statement.order_by(ChannelModel.model_id))).scalars().all()
    return {
        "items": [channel_model_json(row) for row in rows],
        "total": len(rows),
        "page": 1,
        "page_size": len(rows),
    }


@router.post("/channels/{channel_id}/models", status_code=201)
async def create_manual_model(
    channel_id: str, payload: ManualModelInput, session: AsyncSession = Depends(get_session)
):
    channel = (
        await session.execute(
            select(Channel)
            .options(selectinload(Channel.protocol_bindings))
            .where(Channel.id == channel_id)
        )
    ).scalar_one_or_none()
    if not channel:
        raise HTTPException(404, "Channel not found")
    channel_protocols = ordered_protocols(channel.protocol_bindings)
    model_protocols = payload.protocols or channel_protocols
    if not set(model_protocols).issubset(channel_protocols):
        raise HTTPException(422, "Model protocols must be enabled on the channel")
    row = ChannelModel(
        channel_id=channel_id,
        model_id=payload.model_id,
        display_name=payload.display_name,
        source="manual",
        available=True,
    )
    row.protocol_bindings = [
        ChannelModelProtocol(protocol=protocol) for protocol in model_protocols
    ]
    session.add(row)
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise HTTPException(409, "Model already exists for this channel") from exc
    return {
        "id": row.id,
        "channel_id": row.channel_id,
        "model_id": row.model_id,
        "protocols": model_protocols,
        "source": row.source,
        "available": row.available,
    }


@router.patch("/channel-models/{channel_model_id}")
async def patch_channel_model(
    channel_model_id: str,
    payload: ChannelModelPatch,
    session: AsyncSession = Depends(get_session),
):
    row = (
        await session.execute(
            select(ChannelModel)
            .options(
                selectinload(ChannelModel.protocol_bindings),
                selectinload(ChannelModel.channel).selectinload(Channel.protocol_bindings),
            )
            .where(ChannelModel.id == channel_model_id)
        )
    ).scalar_one_or_none()
    if not row:
        raise HTTPException(404, "Channel model not found")
    values = payload.model_dump(exclude_unset=True)
    model_protocols = values.pop("protocols", None)
    if model_protocols is not None:
        channel_protocols = ordered_protocols(row.channel.protocol_bindings)
        if not set(model_protocols).issubset(channel_protocols):
            raise HTTPException(422, "Model protocols must be enabled on the channel")
        await session.execute(
            delete(ChannelModelProtocol).where(
                ChannelModelProtocol.channel_model_id == channel_model_id
            )
        )
        await session.flush()
        for protocol in model_protocols:
            session.add(
                ChannelModelProtocol(
                    channel_model_id=channel_model_id, protocol=protocol
                )
            )
    for key, value in values.items():
        setattr(row, key, value)
    await session.commit()
    return {
        "id": row.id,
        "channel_id": row.channel_id,
        "model_id": row.model_id,
        "protocols": model_protocols or ordered_protocols(row.protocol_bindings),
        "display_name": row.display_name,
        "source": row.source,
        "available": row.available,
    }


@router.delete("/channel-models/{channel_model_id}", status_code=204)
async def delete_channel_model(channel_model_id: str, session: AsyncSession = Depends(get_session)):
    row = await session.get(ChannelModel, channel_model_id)
    if not row:
        raise HTTPException(404, "Channel model not found")
    count = await session.scalar(
        select(func.count())
        .select_from(RouteCandidate)
        .where(RouteCandidate.channel_model_id == channel_model_id)
    )
    if count:
        raise HTTPException(409, "Remove route candidates before deleting this model")
    await session.delete(row)
    await session.commit()
    return Response(status_code=204)


@router.get("/routes")
async def list_routes(session: AsyncSession = Depends(get_session)):
    model_ids = (
        (
            await session.execute(
                select(ModelRoute.requested_model_id)
                .distinct()
                .order_by(ModelRoute.requested_model_id)
            )
        )
        .scalars()
        .all()
    )
    bundles = [await load_route_bundle(session, model_id) for model_id in model_ids]
    return {
        "items": [route_bundle_json(rows) for rows in bundles if rows],
        "total": len(bundles),
        "page": 1,
        "page_size": len(bundles),
    }


@router.post("/routes", status_code=201)
async def create_route(payload: RouteInput, session: AsyncSession = Depends(get_session)):
    if payload.protocol is not None and payload.protocol not in PROTOCOLS:
        raise HTTPException(422, "Unsupported protocol")
    if payload.protocol is not None:
        protocols = [payload.protocol]
    else:
        values = set(
            (
                await session.execute(
                    select(ChannelModelProtocol.protocol)
                    .join(
                        ChannelModel,
                        ChannelModel.id == ChannelModelProtocol.channel_model_id,
                    )
                    .where(
                        ChannelModel.model_id == payload.requested_model_id,
                        ChannelModel.available.is_(True),
                    )
                    .distinct()
                )
            ).scalars()
        )
        protocols = [protocol for protocol in PROTOCOL_ORDER if protocol in values]
        if not protocols:
            raise HTTPException(422, "No available channel supports this model")
    existing = set(
        (
            await session.execute(
                select(ModelRoute.protocol).where(
                    ModelRoute.requested_model_id == payload.requested_model_id,
                    ModelRoute.protocol.in_(protocols),
                )
            )
        ).scalars()
    )
    if existing:
        raise HTTPException(409, "Route already exists")
    for protocol in protocols:
        session.add(
            ModelRoute(
                protocol=protocol,
                requested_model_id=payload.requested_model_id,
                enabled=payload.enabled,
            )
        )
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise HTTPException(409, "Route already exists") from exc
    rows = await load_route_bundle(session, payload.requested_model_id)
    return route_bundle_json(rows)


@router.patch("/routes/{route_id}")
async def patch_route(
    route_id: str, payload: RoutePatch, session: AsyncSession = Depends(get_session)
):
    row = await session.get(ModelRoute, route_id)
    if not row:
        raise HTTPException(404, "Route not found")
    sibling_rows = (
        await session.execute(
            select(ModelRoute).where(ModelRoute.requested_model_id == row.requested_model_id)
        )
    ).scalars()
    for sibling in sibling_rows:
        sibling.enabled = payload.enabled
    await session.commit()
    return {"id": row.id, "enabled": payload.enabled}


@router.put("/routes/{route_id}/candidates")
async def replace_candidates(
    route_id: str, payload: CandidateListInput, session: AsyncSession = Depends(get_session)
):
    route = await session.get(ModelRoute, route_id)
    if not route:
        raise HTTPException(404, "Route not found")
    priorities = [item.priority for item in payload.candidates]
    model_ids = [item.channel_model_id for item in payload.candidates]
    if len(priorities) != len(set(priorities)):
        raise HTTPException(409, "Candidate priorities must be unique")
    if len(model_ids) != len(set(model_ids)):
        raise HTTPException(409, "Channel models must be unique")
    sibling_routes = (
        await session.execute(
            select(ModelRoute).where(
                ModelRoute.requested_model_id == route.requested_model_id
            )
        )
    ).scalars().all()
    route_protocols = {item.protocol for item in sibling_routes}
    protocols_by_model: dict[str, set[str]] = {}
    if model_ids:
        rows = (
            (
                await session.execute(
                    select(ChannelModel)
                    .options(
                        selectinload(ChannelModel.channel),
                        selectinload(ChannelModel.protocol_bindings),
                    )
                    .where(ChannelModel.id.in_(model_ids))
                )
            )
            .scalars()
            .all()
        )
        if len(rows) != len(model_ids):
            raise HTTPException(422, "One or more channel models do not exist")
        for row in rows:
            model_protocols = {item.protocol for item in row.protocol_bindings}
            if not model_protocols & route_protocols or row.model_id != route.requested_model_id:
                raise HTTPException(422, "Candidate protocol and model must match the route")
            protocols_by_model[row.id] = model_protocols
    sibling_ids = [item.id for item in sibling_routes]
    await session.execute(
        delete(RouteCandidate).where(RouteCandidate.route_id.in_(sibling_ids))
    )
    await session.flush()
    for sibling in sibling_routes:
        for item in payload.candidates:
            if sibling.protocol not in protocols_by_model[item.channel_model_id]:
                continue
            session.add(RouteCandidate(route_id=sibling.id, **item.model_dump()))
    await session.commit()
    rows = await load_route_bundle(session, route.requested_model_id)
    return route_bundle_json(rows)


@router.delete("/routes/{route_id}", status_code=204)
async def delete_route(route_id: str, session: AsyncSession = Depends(get_session)):
    row = await session.get(ModelRoute, route_id)
    if not row:
        raise HTTPException(404, "Route not found")
    sibling_rows = (
        await session.execute(
            select(ModelRoute).where(ModelRoute.requested_model_id == row.requested_model_id)
        )
    ).scalars()
    for sibling in sibling_rows:
        await session.delete(sibling)
    await session.commit()
    return Response(status_code=204)


@router.get("/requests")
async def list_requests(
    protocol: str | None = None,
    model_id: str | None = None,
    outcome: str | None = None,
    status_code: int | None = None,
    min_duration_ms: int | None = None,
    channel_id: str | None = None,
    date_from: datetime | None = Query(default=None, alias="from"),
    date_to: datetime | None = Query(default=None, alias="to"),
    page: int = 1,
    page_size: int = 50,
    session: AsyncSession = Depends(get_session),
):
    page_size = min(max(page_size, 1), 200)
    statement = select(RequestLog).options(selectinload(RequestLog.attempts))
    count_statement = select(func.count()).select_from(RequestLog)
    for condition in [
        RequestLog.protocol == protocol if protocol else None,
        RequestLog.model_id == model_id if model_id else None,
        RequestLog.outcome == outcome if outcome else None,
        RequestLog.final_channel_id == channel_id if channel_id else None,
        RequestLog.final_status_code == status_code if status_code is not None else None,
        RequestLog.total_duration_ms >= min_duration_ms if min_duration_ms is not None else None,
        RequestLog.started_at >= date_from if date_from else None,
        RequestLog.started_at < date_to if date_to else None,
    ]:
        if condition is not None:
            statement = statement.where(condition)
            count_statement = count_statement.where(condition)
    total = await session.scalar(count_statement) or 0
    rows = (
        (
            await session.execute(
                statement.order_by(RequestLog.started_at.desc())
                .offset((page - 1) * page_size)
                .limit(page_size)
            )
        )
        .scalars()
        .all()
    )
    fields = [
        "id",
        "protocol",
        "model_id",
        "endpoint",
        "stream",
        "started_at",
        "finished_at",
        "total_duration_ms",
        "final_status_code",
        "outcome",
        "attempt_count",
        "final_channel_id",
        "request_bytes",
        "response_bytes",
    ]
    items = []
    for row in rows:
        item = {key: getattr(row, key) for key in fields}
        item["response_channels"] = [
            attempt.channel_name for attempt in sorted(row.attempts, key=lambda value: value.attempt_no)
        ]
        items.append(item)
    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.delete("/logs", status_code=204)
async def clear_logs(
    confirm: bool = False,
    before: datetime | None = None,
    session: AsyncSession = Depends(get_session),
):
    if not confirm:
        raise HTTPException(400, "confirm=true is required")
    request_ids = select(RequestLog.id)
    if before:
        request_ids = request_ids.where(RequestLog.started_at < before)
    await session.execute(delete(RequestAttempt).where(RequestAttempt.request_id.in_(request_ids)))
    statement = delete(RequestLog)
    if before:
        statement = statement.where(RequestLog.started_at < before)
    await session.execute(statement)
    await session.execute(delete(HealthProbeLog))
    await session.execute(delete(DiscoveryRun))
    await session.commit()
    return Response(status_code=204)


@router.get("/requests/{request_id}")
async def get_request_log(request_id: str, session: AsyncSession = Depends(get_session)):
    statement = (
        select(RequestLog)
        .options(selectinload(RequestLog.attempts))
        .where(RequestLog.id == request_id)
    )
    row = (await session.execute(statement)).scalar_one_or_none()
    if not row:
        raise HTTPException(404, "Request log not found")
    request_data = {
        column.name: getattr(row, column.name) for column in RequestLog.__table__.columns
    }
    attempts = []
    for attempt in sorted(row.attempts, key=lambda value: value.attempt_no):
        attempt_data = {
            column.name: getattr(attempt, column.name)
            for column in RequestAttempt.__table__.columns
        }
        attempt_data["tps"] = output_tps(attempt.output_tokens, attempt.duration_ms)
        attempts.append(attempt_data)
    request_data["attempts"] = attempts
    return request_data


@router.get("/health-probes")
async def list_health_probes(session: AsyncSession = Depends(get_session)):
    rows = (
        (
            await session.execute(
                select(HealthProbeLog).order_by(HealthProbeLog.started_at.desc()).limit(200)
            )
        )
        .scalars()
        .all()
    )
    return {
        "items": [
            {column.name: getattr(row, column.name) for column in HealthProbeLog.__table__.columns}
            for row in rows
        ],
        "total": len(rows),
        "page": 1,
        "page_size": len(rows),
    }


@router.get("/stats/summary")
async def stats_summary(session: AsyncSession = Depends(get_session)):
    total = await session.scalar(select(func.count()).select_from(RequestLog)) or 0
    success = (
        await session.scalar(
            select(func.count()).select_from(RequestLog).where(RequestLog.outcome == "success")
        )
        or 0
    )
    avg_duration = await session.scalar(select(func.avg(RequestLog.total_duration_ms)))
    token_row = (
        await session.execute(
            select(
                func.sum(RequestAttempt.cache_read_tokens),
                func.sum(RequestAttempt.cache_write_tokens),
                func.sum(RequestAttempt.cache_miss_input_tokens),
                func.sum(RequestAttempt.output_tokens),
                func.avg(RequestAttempt.first_token_ms),
                func.sum(
                    func.iif(
                        RequestAttempt.output_tokens.is_not(None)
                        & (RequestAttempt.duration_ms > 0),
                        RequestAttempt.output_tokens,
                        0,
                    )
                ),
                func.sum(
                    func.iif(
                        RequestAttempt.output_tokens.is_not(None)
                        & (RequestAttempt.duration_ms > 0),
                        RequestAttempt.duration_ms,
                        0,
                    )
                ),
            )
        )
    ).one()
    channels = await session.scalar(select(func.count()).select_from(Channel)) or 0
    active_channels = (
        await session.scalar(
            select(func.count()).select_from(ChannelHealth).where(ChannelHealth.state == "active")
        )
        or 0
    )
    cache_rows = (
        await session.execute(
            select(
                RequestLog.protocol,
                func.count(func.distinct(RequestAttempt.request_id)),
                func.sum(RequestAttempt.cache_read_tokens),
                func.sum(RequestAttempt.cache_write_tokens),
                func.sum(RequestAttempt.cache_miss_input_tokens),
            )
            .join(RequestAttempt, RequestAttempt.request_id == RequestLog.id)
            .group_by(RequestLog.protocol)
        )
    ).all()
    cache_by_provider: dict[str, dict] = {}
    for protocol, request_count, cache_read, cache_write, cache_miss in cache_rows:
        provider = CACHE_PROVIDER_PROTOCOLS.get(protocol, protocol)
        item = cache_by_provider.setdefault(
            provider,
            {
                "provider": provider,
                "request_count": 0,
                "cache_read_tokens": 0,
                "cache_write_tokens": 0,
                "cache_miss_input_tokens": 0,
            },
        )
        item["request_count"] += request_count or 0
        item["cache_read_tokens"] += cache_read or 0
        item["cache_write_tokens"] += cache_write or 0
        item["cache_miss_input_tokens"] += cache_miss or 0
    cache_provider_items = []
    for provider in (*CACHE_PROVIDER_ORDER, *sorted(set(cache_by_provider) - set(CACHE_PROVIDER_ORDER))):
        item = cache_by_provider.get(provider)
        if not item:
            continue
        total_input = (
            item["cache_read_tokens"]
            + item["cache_write_tokens"]
            + item["cache_miss_input_tokens"]
        )
        cache_provider_items.append(
            {
                **item,
                "total_input_tokens": total_input,
                "cache_hit_rate": round(item["cache_read_tokens"] / total_input, 4)
                if total_input
                else None,
            }
        )
    return {
        "requests": total,
        "success_rate": round(success / total, 4) if total else None,
        "average_duration_ms": round(avg_duration, 2) if avg_duration is not None else None,
        "average_first_token_ms": round(token_row[4], 2) if token_row[4] is not None else None,
        "average_tps": (
            round(token_row[5] * 1000 / token_row[6], 3) if token_row[6] else None
        ),
        "cache_read_tokens": token_row[0],
        "cache_write_tokens": token_row[1],
        "cache_miss_input_tokens": token_row[2],
        "output_tokens": token_row[3],
        "cache_by_provider": cache_provider_items,
        "channels": channels,
        "active_channels": active_channels,
    }


@router.get("/stats/cache")
async def stats_cache(session: AsyncSession = Depends(get_session)):
    row = (
        await session.execute(
            select(
                func.sum(RequestAttempt.cache_read_tokens),
                func.sum(RequestAttempt.cache_write_tokens),
                func.sum(RequestAttempt.cache_miss_input_tokens),
                func.sum(RequestAttempt.output_tokens),
                func.sum(
                    func.iif(
                        RequestAttempt.input_tokens.is_(None)
                        & RequestAttempt.output_tokens.is_(None),
                        1,
                        0,
                    )
                ),
            )
        )
    ).one()
    return {
        "cache_read_tokens": row[0],
        "cache_write_tokens": row[1],
        "cache_miss_input_tokens": row[2],
        "output_tokens": row[3],
        "unknown_attempts": row[4] or 0,
    }


@router.get("/stats/models")
async def stats_models(session: AsyncSession = Depends(get_session)):
    rows = (
        await session.execute(
            select(
                RequestLog.protocol,
                RequestLog.model_id,
                func.count(RequestLog.id),
                func.avg(RequestLog.total_duration_ms),
                func.sum(func.iif(RequestLog.outcome == "success", 1, 0)),
            )
            .group_by(RequestLog.protocol, RequestLog.model_id)
            .order_by(func.count(RequestLog.id).desc())
        )
    ).all()
    return {
        "items": [
            {
                "protocol": protocol,
                "model_id": model_id,
                "requests": count,
                "average_duration_ms": round(avg_duration, 2) if avg_duration is not None else None,
                "success_rate": round(success_count / count, 4) if count else None,
            }
            for protocol, model_id, count, avg_duration, success_count in rows
        ]
    }


@router.get("/stats/channels")
async def stats_channels(session: AsyncSession = Depends(get_session)):
    rows = (
        await session.execute(
            select(
                RequestAttempt.channel_id,
                RequestAttempt.channel_name,
                func.count(RequestAttempt.id),
                func.avg(RequestAttempt.duration_ms),
                func.sum(func.iif(RequestAttempt.outcome == "success", 1, 0)),
            )
            .group_by(RequestAttempt.channel_id, RequestAttempt.channel_name)
            .order_by(func.count(RequestAttempt.id).desc())
        )
    ).all()
    return {
        "items": [
            {
                "channel_id": channel_id,
                "channel_name": channel_name,
                "attempts": count,
                "average_duration_ms": round(avg_duration, 2) if avg_duration is not None else None,
                "success_rate": round(success_count / count, 4) if count else None,
            }
            for channel_id, channel_name, count, avg_duration, success_count in rows
        ]
    }


@router.get("/stats/timeseries")
async def stats_timeseries(
    interval: Literal["hour", "day"] = "hour",
    session: AsyncSession = Depends(get_session),
):
    bucket_format = "%Y-%m-%dT%H:00:00Z" if interval == "hour" else "%Y-%m-%dT00:00:00Z"
    bucket = func.strftime(bucket_format, RequestLog.started_at)
    rows = (
        await session.execute(
            select(
                bucket,
                func.count(RequestLog.id),
                func.sum(func.iif(RequestLog.outcome == "success", 1, 0)),
                func.avg(RequestLog.total_duration_ms),
            )
            .group_by(bucket)
            .order_by(bucket)
        )
    ).all()
    return {
        "items": [
            {
                "time": time_bucket,
                "requests": count,
                "successes": success_count,
                "average_duration_ms": round(avg_duration, 2) if avg_duration is not None else None,
            }
            for time_bucket, count, success_count, avg_duration in rows
        ]
    }


@router.get("/settings")
async def get_settings(request: Request, session: AsyncSession = Depends(get_session)):
    values = await get_runtime_settings(session)
    policy = await get_access_policy(
        session,
        request.app.state.secrets,
        request.app.state.settings.admin_token,
        request.app.state.settings.gateway_key,
    )
    values.update(
        {
            "admin_key_hint": SecretStore.hint(policy["admin_key"])
            if policy["admin_key"]
            else "",
            "gateway_key_hint": SecretStore.hint(policy["gateway_key"])
            if policy["gateway_key"]
            else "",
            "access_keys_configured": bool(policy["admin_key"] and policy["gateway_key"]),
        }
    )
    return values


@router.patch("/settings")
async def patch_settings(
    payload: dict[str, Any],
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    payload = dict(payload)
    current_policy = await get_access_policy(
        session,
        request.app.state.secrets,
        request.app.state.settings.admin_token,
        request.app.state.settings.gateway_key,
    )
    submitted_keys: dict[str, str] = {}
    for key in ("admin_access_key", "gateway_access_key"):
        value = payload.pop(key, None)
        if value:
            submitted_keys[key] = str(value)

    if payload.get("trust_local_network") is False:
        effective_admin_key = submitted_keys.get("admin_access_key", current_policy["admin_key"])
        effective_gateway_key = submitted_keys.get(
            "gateway_access_key", current_policy["gateway_key"]
        )
        if not effective_admin_key or not effective_gateway_key:
            raise HTTPException(
                422,
                "关闭局域网信任前，请手动设置管理密钥和代理密钥，或先随机生成密钥。",
            )

    for key, value in submitted_keys.items():
        row = await session.get(AppSetting, key)
        encrypted = request.app.state.secrets.encrypt(value).decode("utf-8")
        if row:
            row.value_json = encrypted
        else:
            session.add(AppSetting(key=key, value_json=encrypted))
    try:
        values = await update_runtime_settings(session, payload)
    except ValueError as exc:
        await session.rollback()
        raise HTTPException(422, str(exc)) from exc
    return values


@router.post("/settings/access-keys/generate")
async def generate_access_keys(request: Request, session: AsyncSession = Depends(get_session)):
    admin_key = stdlib_secrets.token_urlsafe(32)
    gateway_key = stdlib_secrets.token_urlsafe(32)
    for key, value in (("admin_access_key", admin_key), ("gateway_access_key", gateway_key)):
        row = await session.get(AppSetting, key)
        encrypted = request.app.state.secrets.encrypt(value).decode("utf-8")
        if row:
            row.value_json = encrypted
        else:
            session.add(AppSetting(key=key, value_json=encrypted))
    await session.commit()
    return {
        "admin_access_key": admin_key,
        "gateway_access_key": gateway_key,
        "warning": "这些密钥只在本次响应中返回，请立即保存。",
    }


@router.get("/system/status")
async def system_status(request: Request):
    policy = await resolve_access_policy(request.app)
    return {
        "status": "ok",
        "database": "ok",
        "telemetry_queue_size": request.app.state.telemetry.queue.qsize(),
        "telemetry_dropped": request.app.state.telemetry.dropped,
        "protocols": sorted(PROTOCOLS),
        "trust_local_network": policy["trust_local_network"],
    }


@router.get("/system/protocols")
async def system_protocols():
    return {
        "items": [
            {
                "id": protocol,
                "endpoints": list(PROTOCOL_ENDPOINTS[protocol]),
                "model_discovery_endpoint": "/v1/models",
            }
            if protocol != "gemini"
            else {
                "id": protocol,
                "endpoints": list(PROTOCOL_ENDPOINTS[protocol]),
                "model_discovery_endpoint": "/v1beta/models",
            }
            for protocol in PROTOCOL_ORDER
        ]
    }
