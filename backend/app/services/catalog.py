from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.adapters import PROTOCOL_ORDER
from app.db.models import (
    Channel,
    ChannelModel,
    ChannelModelProtocol,
    ClaudeModelMapping,
    CodexModelMapping,
    ModelRoute,
    RouteCandidate,
)
from app.services.capabilities import get_or_detect_caps
from app.services.routing import resolve_candidates


async def list_routable_models(session: AsyncSession, protocol: str) -> list[dict]:
    statement = (
        select(
            ModelRoute.requested_model_id,
            ModelRoute.created_at,
            func.max(ChannelModel.display_name),
        )
        .join(RouteCandidate, RouteCandidate.route_id == ModelRoute.id)
        .join(ChannelModel, ChannelModel.id == RouteCandidate.channel_model_id)
        .join(
            ChannelModelProtocol,
            ChannelModelProtocol.channel_model_id == ChannelModel.id,
        )
        .join(Channel, Channel.id == ChannelModel.channel_id)
        .where(
            ModelRoute.protocol == protocol,
            ModelRoute.enabled.is_(True),
            RouteCandidate.enabled.is_(True),
            ChannelModel.available.is_(True),
            Channel.manual_enabled.is_(True),
            ChannelModelProtocol.protocol == protocol,
        )
        .group_by(ModelRoute.id, ModelRoute.requested_model_id, ModelRoute.created_at)
        .order_by(ModelRoute.requested_model_id)
    )
    rows = (await session.execute(statement)).all()
    result = []
    for model_id, created_at, display_name in rows:
        result.append(
            {
                "id": model_id,
                "display_name": display_name or model_id,
                "created_at": created_at,
                "capabilities": await get_or_detect_caps(session, model_id),
            }
        )
    return result


async def list_all_routable_models(session: AsyncSession) -> list[dict]:
    merged: dict[str, dict] = {}
    for protocol in PROTOCOL_ORDER:
        for item in await list_routable_models(session, protocol):
            current = merged.get(item["id"])
            if current is None:
                current = {**item, "protocols": []}
                merged[item["id"]] = current
            current["protocols"].append(protocol)
            if item["created_at"] < current["created_at"]:
                current["created_at"] = item["created_at"]
    return [merged[model_id] for model_id in sorted(merged)]


async def list_claude_mapping_models(session: AsyncSession) -> list[dict]:
    """Claude-standard model names exposed through the Claude model mapping
    assistant. Each entry is routable with per-model protocol conversion;
    candidates are inherited from the referenced system model's route."""
    mappings = (
        (
            await session.execute(
                select(ClaudeModelMapping)
                .where(ClaudeModelMapping.enabled.is_(True))
                .order_by(ClaudeModelMapping.claude_model_id)
            )
        )
        .scalars()
        .all()
    )
    result = []
    for mapping in mappings:
        candidates = await resolve_candidates(
            session, mapping.upstream_protocol, mapping.upstream_model_id, 1
        )
        if not candidates:
            continue
        result.append(
            {
                "id": mapping.claude_model_id,
                "display_name": mapping.display_name or mapping.claude_model_id,
                "created_at": mapping.created_at,
                "capabilities": await get_or_detect_caps(session, mapping.claude_model_id),
                "protocols": ["claude"],
                "x_local_gateway": {
                    "mapping": {
                        "upstream_protocol": mapping.upstream_protocol,
                        "upstream_model_id": mapping.upstream_model_id,
                    }
                },
            }
        )
    return result


async def list_codex_mapping_models(session: AsyncSession) -> list[dict]:
    """Codex-standard model names exposed through the Codex model mapping.
    Each entry is routable with per-model protocol conversion; candidates are
    inherited from the referenced system model's route."""
    mappings = (
        (
            await session.execute(
                select(CodexModelMapping)
                .where(CodexModelMapping.enabled.is_(True))
                .order_by(CodexModelMapping.codex_model_id)
            )
        )
        .scalars()
        .all()
    )
    result = []
    for mapping in mappings:
        candidates = await resolve_candidates(
            session, mapping.upstream_protocol, mapping.upstream_model_id, 1
        )
        if not candidates:
            continue
        result.append(
            {
                "id": mapping.codex_model_id,
                "display_name": mapping.display_name or mapping.codex_model_id,
                "created_at": mapping.created_at,
                "capabilities": await get_or_detect_caps(session, mapping.codex_model_id),
                "protocols": ["openai_responses"],
                "x_local_gateway": {
                    "mapping": {
                        "upstream_protocol": mapping.upstream_protocol,
                        "upstream_model_id": mapping.upstream_model_id,
                    }
                },
            }
        )
    return result


async def supported_protocols_for_model(session: AsyncSession, model_id: str) -> list[str]:
    statement = (
        select(ModelRoute.protocol)
        .join(RouteCandidate, RouteCandidate.route_id == ModelRoute.id)
        .join(ChannelModel, ChannelModel.id == RouteCandidate.channel_model_id)
        .join(ChannelModelProtocol, ChannelModelProtocol.channel_model_id == ChannelModel.id)
        .where(
            ModelRoute.requested_model_id == model_id,
            ModelRoute.enabled.is_(True),
            RouteCandidate.enabled.is_(True),
            ChannelModel.available.is_(True),
            ChannelModelProtocol.protocol == ModelRoute.protocol,
        )
        .distinct()
    )
    values = set((await session.execute(statement)).scalars())
    return [protocol for protocol in PROTOCOL_ORDER if protocol in values]


def unix_timestamp(value: datetime) -> int:
    normalized = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return int(normalized.timestamp())


def iso_timestamp(value: datetime) -> str:
    normalized = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return normalized.isoformat().replace("+00:00", "Z")
