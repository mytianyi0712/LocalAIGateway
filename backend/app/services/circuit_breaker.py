from datetime import datetime, timedelta, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import ChannelHealth, utcnow


NON_COUNTABLE_STATUS = {400, 404, 405, 413, 415, 422}


def classify_http_status(status_code: int) -> tuple[str, bool]:
    if 200 <= status_code < 300:
        return "success", False
    if status_code in NON_COUNTABLE_STATUS:
        return "request_error", False
    if status_code in {401, 403}:
        return "auth_error", True
    if status_code == 408:
        return "upstream_timeout", True
    if status_code == 429:
        return "rate_limited", True
    if status_code >= 500:
        return "upstream_error", True
    return "http_error", False


def classify_exception(exc: Exception) -> str:
    name = exc.__class__.__name__.lower()
    if "timeout" in name:
        return "transport_timeout"
    if "connect" in name:
        return "connection_error"
    if "network" in name:
        return "network_error"
    return "transport_error"


async def record_success(session: AsyncSession, channel_id: str) -> None:
    health = await session.get(ChannelHealth, channel_id)
    if not health:
        return
    health.state = "active"
    health.consecutive_failures = 0
    health.disabled_until = None
    health.last_success_at = utcnow()
    health.last_error_kind = None
    health.last_status_code = None
    await session.commit()


async def record_failure(
    session: AsyncSession,
    channel_id: str,
    error_kind: str,
    status_code: int | None,
    countable: bool,
    threshold: int,
    open_seconds: int,
) -> bool:
    health = await session.get(ChannelHealth, channel_id)
    if not health:
        return False
    health.last_failure_at = utcnow()
    health.last_error_kind = error_kind
    health.last_status_code = status_code
    if countable:
        health.consecutive_failures += 1
    opened = countable and health.consecutive_failures >= threshold
    if opened:
        health.state = "open"
        health.disabled_until = utcnow() + timedelta(seconds=open_seconds)
    await session.commit()
    return opened


def as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return (
        value.replace(tzinfo=timezone.utc)
        if value.tzinfo is None
        else value.astimezone(timezone.utc)
    )
