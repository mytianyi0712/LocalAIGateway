from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import SecretStore
from app.db.models import AppSetting


DEFAULT_SETTINGS = {
    "trust_local_network": True,
    "failure_threshold": 3,
    "circuit_open_seconds": 900,
    "max_failover_attempts": 3,
    "connect_timeout_seconds": 10,
    "first_byte_timeout_seconds": 60,
    "first_token_timeout_seconds": 60,
    "stream_idle_timeout_seconds": 300,
    "non_stream_total_timeout_seconds": 600,
    "model_discovery_interval_hours": 24,
    "log_retention_days": 30,
}

PRIVATE_ACCESS_SETTINGS = {"admin_access_key", "gateway_access_key"}

SETTING_RANGES = {
    "failure_threshold": (1, 20),
    "circuit_open_seconds": (0, 86400),
    "max_failover_attempts": (1, 20),
    "connect_timeout_seconds": (1, 120),
    "first_byte_timeout_seconds": (1, 600),
    "first_token_timeout_seconds": (1, 600),
    "stream_idle_timeout_seconds": (10, 3600),
    "non_stream_total_timeout_seconds": (10, 3600),
    "model_discovery_interval_hours": (1, 168),
    "log_retention_days": (1, 365),
}


async def get_runtime_settings(session: AsyncSession) -> dict:
    rows = (await session.execute(select(AppSetting))).scalars().all()
    values = dict(DEFAULT_SETTINGS)
    values.update({row.key: row.value_json for row in rows if row.key not in PRIVATE_ACCESS_SETTINGS})
    return values


async def get_access_policy(session: AsyncSession, secrets: SecretStore, fallback_admin: str, fallback_gateway: str):
    runtime = await get_runtime_settings(session)
    rows = (
        await session.execute(
            select(AppSetting).where(AppSetting.key.in_(PRIVATE_ACCESS_SETTINGS))
        )
    ).scalars().all()
    encrypted = {row.key: row.value_json for row in rows}

    def resolve(key: str, fallback: str) -> str:
        value = encrypted.get(key)
        if isinstance(value, str) and value:
            return secrets.decrypt(value.encode("utf-8"))
        return fallback

    return {
        "trust_local_network": bool(runtime.get("trust_local_network", True)),
        "admin_key": resolve("admin_access_key", fallback_admin),
        "gateway_key": resolve("gateway_access_key", fallback_gateway),
    }


async def update_runtime_settings(session: AsyncSession, updates: dict) -> dict:
    unknown = set(updates) - set(DEFAULT_SETTINGS)
    if unknown:
        raise ValueError(f"Unknown settings: {', '.join(sorted(unknown))}")
    for key, value in updates.items():
        if key == "trust_local_network":
            if not isinstance(value, bool):
                raise ValueError("trust_local_network must be boolean")
            row = await session.get(AppSetting, key)
            if row:
                row.value_json = value
            else:
                session.add(AppSetting(key=key, value_json=value))
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{key} must be a number")
        minimum, maximum = SETTING_RANGES[key]
        if not minimum <= value <= maximum:
            raise ValueError(f"{key} must be between {minimum} and {maximum}")
        row = await session.get(AppSetting, key)
        if row:
            row.value_json = value
        else:
            session.add(AppSetting(key=key, value_json=value))
    await session.commit()
    return await get_runtime_settings(session)
