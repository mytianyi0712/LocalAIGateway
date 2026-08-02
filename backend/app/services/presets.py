"""模型预设目录（Claude Code / Codex）。

预设用于在控制台便捷创建映射：对外暴露 Claude Code 标准的默认模型名
（例如 ``claude-opus-5``）以及 Codex 标准的 OpenAI 模型名（例如
``gpt-5-codex``）。预设来源：

1. 内置精选的默认名。
2. 通过网关已配置的对应协议渠道实时查询其 ``/v1/models`` 目录聚合而来，
   结果缓存到 settings 表，可手动刷新。
"""

import asyncio
import logging
from datetime import datetime, timezone

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.adapters import get_adapter
from app.core.security import SecretStore
from app.db.models import AppSetting, Channel, ChannelHealth
from app.services.discovery import _fetch_all_models

logger = logging.getLogger(__name__)

PRESETS_SETTING_KEY = "claude_presets"
CODEX_PRESETS_SETTING_KEY = "codex_presets"

# 内置精选：当前 Claude 家族默认模型名（Anthropic 现行命名，2026）。
DEFAULT_CLAUDE_PRESETS: list[dict[str, str]] = [
    {"id": "claude-opus-5", "display_name": "Claude Opus 5（默认）"},
    {"id": "claude-fable-5", "display_name": "Claude Fable 5"},
    {"id": "claude-sonnet-5", "display_name": "Claude Sonnet 5"},
    {"id": "claude-mythos-5", "display_name": "Claude Mythos 5"},
    {"id": "claude-haiku-5", "display_name": "Claude Haiku 5"},
    {"id": "claude-opus-4-8", "display_name": "Claude Opus 4.8"},
    {"id": "claude-opus-4-6", "display_name": "Claude Opus 4.6"},
    {"id": "claude-sonnet-4-6", "display_name": "Claude Sonnet 4.6"},
    {"id": "claude-opus-4-5", "display_name": "Claude Opus 4.5"},
    {"id": "claude-sonnet-4-5", "display_name": "Claude Sonnet 4.5"},
    {"id": "claude-haiku-4-5", "display_name": "Claude Haiku 4.5"},
    {"id": "claude-3-7-sonnet-latest", "display_name": "Claude 3.7 Sonnet（旧）"},
    {"id": "claude-3-5-haiku-latest", "display_name": "Claude 3.5 Haiku（旧）"},
]

# 内置精选：Codex CLI 使用的 OpenAI Responses API 标准模型名（2026）。
DEFAULT_CODEX_PRESETS: list[dict[str, str]] = [
    {"id": "gpt-5-codex", "display_name": "GPT-5 Codex（默认）"},
    {"id": "gpt-5", "display_name": "GPT-5"},
    {"id": "gpt-5-mini", "display_name": "GPT-5 Mini"},
    {"id": "gpt-5-nano", "display_name": "GPT-5 Nano"},
    {"id": "o3", "display_name": "o3"},
    {"id": "o4-mini", "display_name": "o4-mini"},
    {"id": "gpt-4.1", "display_name": "GPT-4.1"},
    {"id": "gpt-4.1-mini", "display_name": "GPT-4.1 Mini"},
    {"id": "gpt-4.1-nano", "display_name": "GPT-4.1 Nano"},
    {"id": "gpt-4o", "display_name": "GPT-4o"},
    {"id": "gpt-4o-mini", "display_name": "GPT-4o Mini"},
]


def merge_presets(primary: list[dict], secondary: list[dict]) -> list[dict]:
    merged: dict[str, dict] = {}
    for item in [*primary, *secondary]:
        if not item or not item.get("id"):
            continue
        merged.setdefault(item["id"], {"id": item["id"], "display_name": item.get("display_name")})
    return sorted(merged.values(), key=lambda item: item["id"])


async def _get_presets(session: AsyncSession, setting_key: str, defaults: list[dict]) -> dict:
    """返回预设目录：内置精选 + 已缓存的渠道查询结果。"""
    cached = await session.get(AppSetting, setting_key)
    discovered: list[dict] = []
    refreshed_at = None
    if cached and isinstance(cached.value_json, dict):
        discovered = cached.value_json.get("items") or []
        refreshed_at = cached.value_json.get("refreshed_at")
    items = merge_presets(defaults, discovered)
    return {
        "items": items,
        "source": "channels" if discovered else "defaults",
        "refreshed_at": refreshed_at,
    }


async def get_claude_presets(session: AsyncSession) -> dict:
    return await _get_presets(session, PRESETS_SETTING_KEY, DEFAULT_CLAUDE_PRESETS)


async def get_codex_presets(session: AsyncSession) -> dict:
    return await _get_presets(session, CODEX_PRESETS_SETTING_KEY, DEFAULT_CODEX_PRESETS)


async def _refresh_channel_presets(app, protocol: str, setting_key: str) -> None:
    """通过已配置的对应协议渠道实时查询模型目录并缓存预设。"""
    session_factory = app.state.db.sessions
    http: httpx.AsyncClient = app.state.http
    secrets: SecretStore = app.state.secrets
    try:
        async with session_factory() as session:
            channels = (
                (
                    await session.execute(
                        select(Channel)
                        .options(selectinload(Channel.provider))
                        .join(ChannelHealth, ChannelHealth.channel_id == Channel.id)
                        .where(
                            Channel.manual_enabled.is_(True),
                            ChannelHealth.state == "active",
                            Channel.protocol == protocol,
                        )
                    )
                )
                .scalars()
                .all()
            )
            discovered: dict[str, dict] = {}
            adapter = get_adapter(protocol)
            for channel in channels:
                try:
                    api_key = secrets.decrypt(channel.api_key_encrypted)
                    async with asyncio.timeout(20):
                        models, _ = await _fetch_all_models(
                            http, adapter, channel.provider.base_url, api_key
                        )
                    for item in models:
                        model_id = item.get("id")
                        if not model_id:
                            continue
                        discovered.setdefault(
                            model_id,
                            {
                                "id": model_id,
                                "display_name": item.get("display_name")
                                or item.get("displayName")
                                or model_id,
                            },
                        )
                except Exception as exc:
                    logger.warning(
                        "preset refresh failed for channel %s: %s",
                        channel.name,
                        exc,
                    )
                    continue
            cached = await session.get(AppSetting, setting_key)
            payload = {
                "items": sorted(discovered.values(), key=lambda item: item["id"]),
                "refreshed_at": datetime.now(timezone.utc).isoformat(),
            }
            if cached:
                cached.value_json = payload
            else:
                session.add(AppSetting(key=setting_key, value_json=payload))
            await session.commit()
    except Exception as exc:
        logger.error("preset refresh aborted: %s", exc, exc_info=True)
        # 刷新失败不影响网关，仅保留现有预设。
        return


async def refresh_claude_presets(app) -> None:
    """通过已配置的 Claude 渠道实时查询模型目录并缓存预设。"""
    await _refresh_channel_presets(app, "claude", PRESETS_SETTING_KEY)


async def refresh_codex_presets(app) -> None:
    """通过已配置的 OpenAI Responses 渠道实时查询模型目录并缓存预设。"""
    await _refresh_channel_presets(app, "openai_responses", CODEX_PRESETS_SETTING_KEY)
