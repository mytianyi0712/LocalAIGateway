from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.adapters import PROTOCOL_ORDER, SHARED_DISCOVERY_PROTOCOL_GROUPS
from app.db.models import (
    Channel,
    ChannelHealth,
    ChannelModel,
    ChannelModelProtocol,
    ModelRoute,
    Provider,
    RouteCandidate,
)


async def synchronize_shared_protocol_routes(
    session: AsyncSession,
    models: list[ChannelModel],
    enabled_protocols: set[str],
) -> int:
    """Copy configured candidates across protocols that share one model catalog."""
    if not models:
        return 0
    shared_protocols = set().union(*SHARED_DISCOVERY_PROTOCOL_GROUPS)
    model_ids = {model.model_id for model in models}
    routes = (
        await session.execute(
            select(ModelRoute).where(
                ModelRoute.requested_model_id.in_(model_ids),
                ModelRoute.protocol.in_(shared_protocols),
            )
        )
    ).scalars().all()
    route_by_key = {(route.protocol, route.requested_model_id): route for route in routes}
    route_ids = [route.id for route in routes]
    candidates = (
        (
            await session.execute(
                select(RouteCandidate).where(RouteCandidate.route_id.in_(route_ids))
            )
        ).scalars().all()
        if route_ids
        else []
    )
    candidates_by_route: dict[str, list[RouteCandidate]] = {
        route_id: [] for route_id in route_ids
    }
    for candidate in candidates:
        candidates_by_route[candidate.route_id].append(candidate)

    added = 0
    protocol_rank = {protocol: index for index, protocol in enumerate(PROTOCOL_ORDER)}
    for model in models:
        for group in SHARED_DISCOVERY_PROTOCOL_GROUPS:
            targets = sorted(enabled_protocols & group, key=protocol_rank.get)
            sources: list[tuple[ModelRoute, RouteCandidate]] = []
            for protocol in sorted(group, key=protocol_rank.get):
                route = route_by_key.get((protocol, model.model_id))
                if not route:
                    continue
                for candidate in candidates_by_route.get(route.id, []):
                    if candidate.channel_model_id == model.id:
                        sources.append((route, candidate))
            if not sources:
                continue
            source_route, source_candidate = sources[0]
            for protocol in targets:
                route = route_by_key.get((protocol, model.model_id))
                if route is None:
                    route = ModelRoute(
                        protocol=protocol,
                        requested_model_id=model.model_id,
                        enabled=source_route.enabled,
                    )
                    session.add(route)
                    await session.flush()
                    route_by_key[(protocol, model.model_id)] = route
                    candidates_by_route[route.id] = []
                route_candidates = candidates_by_route[route.id]
                if any(item.channel_model_id == model.id for item in route_candidates):
                    continue
                used_priorities = {item.priority for item in route_candidates}
                priority = source_candidate.priority
                while priority in used_priorities:
                    priority += 1
                candidate = RouteCandidate(
                    route_id=route.id,
                    channel_model_id=model.id,
                    priority=priority,
                    enabled=source_candidate.enabled,
                )
                session.add(candidate)
                route_candidates.append(candidate)
                added += 1
    return added


@dataclass(frozen=True)
class CandidateSnapshot:
    candidate_id: str
    channel_id: str
    channel_name: str
    protocol: str
    priority: int
    base_url: str
    api_key_encrypted: bytes
    model_id: str


async def resolve_candidates(
    session: AsyncSession,
    protocol: str,
    model_id: str,
    limit: int,
) -> list[CandidateSnapshot]:
    statement = (
        select(RouteCandidate, ChannelModel, Channel, Provider)
        .join(ModelRoute, ModelRoute.id == RouteCandidate.route_id)
        .join(ChannelModel, ChannelModel.id == RouteCandidate.channel_model_id)
        .join(
            ChannelModelProtocol,
            ChannelModelProtocol.channel_model_id == ChannelModel.id,
        )
        .join(Channel, Channel.id == ChannelModel.channel_id)
        .join(Provider, Provider.id == Channel.provider_id)
        .join(ChannelHealth, ChannelHealth.channel_id == Channel.id)
        .where(
            ModelRoute.protocol == protocol,
            ModelRoute.requested_model_id == model_id,
            ModelRoute.enabled.is_(True),
            RouteCandidate.enabled.is_(True),
            ChannelModel.available.is_(True),
            Channel.manual_enabled.is_(True),
            ChannelModelProtocol.protocol == protocol,
            ChannelHealth.state == "active",
        )
        .order_by(RouteCandidate.priority.asc())
        .limit(limit)
    )
    rows = (await session.execute(statement)).all()
    return [
        CandidateSnapshot(
            candidate_id=candidate.id,
            channel_id=channel.id,
            channel_name=channel.name,
            protocol=protocol,
            priority=candidate.priority,
            base_url=provider.base_url,
            api_key_encrypted=channel.api_key_encrypted,
            model_id=channel_model.model_id,
        )
        for candidate, channel_model, channel, provider in rows
    ]

