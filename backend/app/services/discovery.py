import httpx
from sqlalchemy import delete, select
from sqlalchemy.orm import selectinload

from app.adapters import PROTOCOL_ORDER, get_adapter
from app.core.security import SecretStore
from app.db.models import (
    Channel,
    ChannelModel,
    ChannelModelProtocol,
    DiscoveryRun,
    utcnow,
)
from app.services.circuit_breaker import classify_exception


async def _fetch_all_models(
    http: httpx.AsyncClient,
    adapter,
    base_url: str,
    api_key: str,
) -> tuple[list[dict], int]:
    url = adapter.discovery_url(base_url)
    headers = adapter.discovery_headers(api_key)
    models: list[dict] = []
    visited: set[str] = set()
    status_code = 0
    for _ in range(50):
        if url in visited:
            break
        visited.add(url)
        response = await http.get(url, headers=headers)
        status_code = response.status_code
        if not response.is_success:
            raise httpx.HTTPStatusError(
                "model discovery failed", request=response.request, response=response
            )
        models.extend(adapter.parse_models(response.content))
        next_url = adapter.next_discovery_url(url, response.content)
        if not next_url:
            break
        url = next_url
    return models, status_code


async def discover_channel_models(
    session_factory,
    http: httpx.AsyncClient,
    secrets: SecretStore,
    channel_id: str,
    run_id: str,
) -> None:
    async with session_factory() as session:
        run = await session.get(DiscoveryRun, run_id)
        statement = (
            select(Channel)
            .options(selectinload(Channel.provider), selectinload(Channel.protocol_bindings))
            .where(Channel.id == channel_id)
        )
        channel = (await session.execute(statement)).scalar_one_or_none()
        if not run or not channel:
            return

        configured_set = {item.protocol for item in channel.protocol_bindings}
        configured = [protocol for protocol in PROTOCOL_ORDER if protocol in configured_set]
        if not configured:
            configured = [channel.protocol]
        api_key = secrets.decrypt(channel.api_key_encrypted)

        groups: dict[tuple[str, tuple[tuple[str, str], ...]], dict] = {}
        for protocol in configured:
            adapter = get_adapter(protocol)
            key = (
                adapter.discovery_url(channel.provider.base_url),
                tuple(adapter.discovery_headers(api_key)),
            )
            group = groups.setdefault(key, {"adapter": adapter, "protocols": []})
            group["protocols"].append(protocol)

        succeeded: set[str] = set()
        failures: dict[str, tuple[str, int | None]] = {}
        seen_by_protocol: dict[str, dict[str, dict]] = {
            protocol: {} for protocol in configured
        }
        last_status_code = None
        for group in groups.values():
            protocols = group["protocols"]
            try:
                models, status_code = await _fetch_all_models(
                    http, group["adapter"], channel.provider.base_url, api_key
                )
                last_status_code = status_code
                for protocol in protocols:
                    succeeded.add(protocol)
                    seen_by_protocol[protocol].update(
                        {item["id"]: item for item in models if item.get("id")}
                    )
            except httpx.HTTPStatusError as exc:
                for protocol in protocols:
                    failures[protocol] = ("http_error", exc.response.status_code)
                last_status_code = exc.response.status_code
            except Exception as exc:
                error_kind = classify_exception(exc)
                for protocol in protocols:
                    failures[protocol] = (error_kind, None)

        existing_rows = (
            await session.execute(
                select(ChannelModel)
                .options(selectinload(ChannelModel.protocol_bindings))
                .where(ChannelModel.channel_id == channel_id)
            )
        ).scalars()
        existing = {row.model_id: row for row in existing_rows}
        protocol_sets = {
            row.id: {binding.protocol for binding in row.protocol_bindings}
            for row in existing.values()
        }
        now = utcnow()

        for protocol in succeeded:
            for model_id, item in seen_by_protocol[protocol].items():
                row = existing.get(model_id)
                metadata = dict(row.metadata_json or {}) if row else {}
                metadata[protocol] = {key: value for key, value in item.items() if key != "id"}
                if row:
                    row.available = True
                    row.last_seen_at = now
                    row.metadata_json = metadata
                    if not row.display_name:
                        row.display_name = item.get("display_name") or item.get("displayName")
                else:
                    row = ChannelModel(
                        channel_id=channel_id,
                        model_id=model_id,
                        display_name=item.get("display_name") or item.get("displayName"),
                        source="discovered",
                        available=True,
                        metadata_json=metadata,
                        first_seen_at=now,
                        last_seen_at=now,
                    )
                    session.add(row)
                    await session.flush()
                    existing[model_id] = row
                    protocol_sets[row.id] = set()
                if protocol not in protocol_sets[row.id]:
                    session.add(
                        ChannelModelProtocol(channel_model_id=row.id, protocol=protocol)
                    )
                    protocol_sets[row.id].add(protocol)

        for row in existing.values():
            if row.source != "discovered":
                continue
            stale = {
                protocol
                for protocol in succeeded
                if protocol in protocol_sets[row.id]
                and row.model_id not in seen_by_protocol[protocol]
            }
            if stale:
                await session.execute(
                    delete(ChannelModelProtocol).where(
                        ChannelModelProtocol.channel_model_id == row.id,
                        ChannelModelProtocol.protocol.in_(stale),
                    )
                )
            protocol_sets[row.id] -= stale
            row.available = bool(protocol_sets[row.id])

        run.success = not failures
        run.model_count = len({model_id for values in seen_by_protocol.values() for model_id in values})
        run.status_code = last_status_code
        run.error_kind = (
            f"protocol_discovery_failed:{','.join(failures)}" if failures else None
        )
        run.finished_at = utcnow()
        await session.commit()
