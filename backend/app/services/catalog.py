from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.adapters import PROTOCOL_ORDER
from app.db.models import (
    Channel,
    ChannelModel,
    ChannelModelProtocol,
    ModelRoute,
    RouteCandidate,
)


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
    return [
        {
            "id": model_id,
            "display_name": display_name or model_id,
            "created_at": created_at,
        }
        for model_id, created_at, display_name in rows
    ]


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
