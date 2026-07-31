from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.db.models import Channel, ChannelModel, ModelCaps, ModelRoute, RouteCandidate


THINKING_LEVELS = ("off", "minimal", "low", "medium", "high", "xhigh", "max")
CAPABILITY_FIELDS = (
    "context_window",
    "max_tokens",
    "supports_image_input",
    "reasoning",
    "thinking_level_map",
    "cost_input",
    "cost_output",
    "cost_cache_read",
    "cost_cache_write",
)


def _nested_get(value: dict[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = value
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _first_int(value: dict[str, Any], paths: tuple[tuple[str, ...], ...]) -> int | None:
    for path in paths:
        raw = _nested_get(value, path)
        if isinstance(raw, bool):
            continue
        if isinstance(raw, int) and raw > 0:
            return raw
        if isinstance(raw, float) and raw > 0:
            return int(raw)
        if isinstance(raw, str) and raw.strip().isdigit():
            parsed = int(raw.strip())
            if parsed > 0:
                return parsed
    return None


def _first_bool(value: dict[str, Any], paths: tuple[tuple[str, ...], ...]) -> bool | None:
    for path in paths:
        raw = _nested_get(value, path)
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, str):
            normalized = raw.strip().lower()
            if normalized in {"true", "yes", "1", "supported"}:
                return True
            if normalized in {"false", "no", "0", "unsupported"}:
                return False
    return None


def _contains_image(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"image", "vision", "multimodal"}:
            return True
        return None
    if isinstance(value, list):
        normalized = {str(item).strip().lower() for item in value}
        return bool(normalized & {"image", "vision", "multimodal", "image_url"})
    if isinstance(value, dict):
        for path in (
            ("input",),
            ("inputs",),
            ("input_types",),
            ("inputTypes",),
            ("modalities",),
            ("input_modalities",),
            ("inputModalities",),
            ("architecture", "input_modalities"),
            ("architecture", "modality"),
            ("capabilities", "input"),
            ("capabilities", "modalities"),
        ):
            detected = _contains_image(_nested_get(value, path))
            if detected is not None:
                return detected
    return None


def _thinking_level_map_from_efforts(value: Any) -> dict[str, str] | None:
    if not isinstance(value, list):
        return None
    result: dict[str, str] = {}
    for item in value:
        raw = item
        if isinstance(item, dict):
            raw = item.get("value") or item.get("id") or item.get("name") or item.get("level")
        if raw is None:
            continue
        effort = str(raw).strip().lower()
        if effort in THINKING_LEVELS:
            result[effort] = effort
        elif effort == "none":
            result["off"] = "none"
    return result or None


def _cost_value(value: dict[str, Any], paths: tuple[tuple[str, ...], ...]) -> float | None:
    for path in paths:
        raw = _nested_get(value, path)
        if isinstance(raw, bool):
            continue
        if isinstance(raw, (int, float)) and raw >= 0:
            return float(raw)
        if isinstance(raw, str):
            try:
                parsed = float(raw.strip())
            except ValueError:
                continue
            if parsed >= 0:
                return parsed
    return None


def extract_capabilities(metadata: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(metadata, dict):
        return {}
    context_window = _first_int(
        metadata,
        (
            ("context_window",),
            ("contextWindow",),
            ("context_length",),
            ("contextLength",),
            ("max_context_tokens",),
            ("maxContextTokens",),
            ("inputTokenLimit",),
            ("input_token_limit",),
            ("limits", "context_window"),
            ("limits", "contextWindow"),
        ),
    )
    max_tokens = _first_int(
        metadata,
        (
            ("max_tokens",),
            ("maxTokens",),
            ("max_output_tokens",),
            ("maxOutputTokens",),
            ("outputTokenLimit",),
            ("output_token_limit",),
            ("max_completion_tokens",),
            ("limits", "max_tokens"),
            ("limits", "maxTokens"),
        ),
    )
    image = _first_bool(
        metadata,
        (
            ("supports_image_input",),
            ("supportsImageInput",),
            ("supports_vision",),
            ("supportsVision",),
            ("vision",),
            ("capabilities", "vision"),
        ),
    )
    if image is None:
        image = _contains_image(metadata)
    reasoning = _first_bool(
        metadata,
        (
            ("reasoning",),
            ("supports_reasoning",),
            ("supportsReasoning",),
            ("supports_reasoning_effort",),
            ("supportsReasoningEffort",),
            ("thinking",),
            ("supports_thinking",),
            ("supportsThinking",),
            ("capabilities", "reasoning"),
            ("capabilities", "thinking"),
        ),
    )
    thinking_level_map = metadata.get("thinkingLevelMap") or metadata.get("thinking_level_map")
    if isinstance(thinking_level_map, dict):
        thinking_level_map = {
            key: thinking_level_map.get(key)
            for key in THINKING_LEVELS
            if key in thinking_level_map
        }
        if reasoning is None:
            reasoning = any(value is not None for value in thinking_level_map.values())
    else:
        thinking_level_map = _thinking_level_map_from_efforts(
            metadata.get("reasoningEfforts") or metadata.get("reasoning_efforts")
        )
        if reasoning is None and thinking_level_map:
            reasoning = True

    cost = metadata.get("cost") or metadata.get("pricing") or {}
    if not isinstance(cost, dict):
        cost = {}
    result = {
        "context_window": context_window,
        "max_tokens": max_tokens,
        "supports_image_input": image,
        "reasoning": reasoning,
        "thinking_level_map": thinking_level_map,
        "cost_input": _cost_value(cost, (("input",), ("prompt",))),
        "cost_output": _cost_value(cost, (("output",), ("completion",))),
        "cost_cache_read": _cost_value(cost, (("cacheRead",), ("cache_read",), ("cached_input",))),
        "cost_cache_write": _cost_value(cost, (("cacheWrite",), ("cache_write",))),
    }
    return {key: value for key, value in result.items() if value is not None}


def _merge_values(values: list[Any]) -> Any:
    known = [value for value in values if value is not None]
    if not known:
        return None
    if all(isinstance(value, bool) for value in known):
        return all(known)
    if all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in known):
        return min(known)
    return known[0]


def _merge_thinking_level_maps(values: list[dict[str, Any] | None]) -> dict[str, Any] | None:
    maps = [value for value in values if isinstance(value, dict)]
    if not maps:
        return None
    merged: dict[str, Any] = {}
    for level in THINKING_LEVELS:
        if all(level in item and item[level] is not None for item in maps):
            merged[level] = maps[0][level]
        elif any(level in item for item in maps):
            merged[level] = None
    return merged or None


def aggregate_capabilities(items: list[dict[str, Any]]) -> dict[str, Any]:
    result = {
        key: _merge_values([item.get(key) for item in items])
        for key in CAPABILITY_FIELDS
        if key != "thinking_level_map"
    }
    result["thinking_level_map"] = _merge_thinking_level_maps(
        [item.get("thinking_level_map") for item in items]
    )
    return {key: value for key, value in result.items() if value is not None}


async def detect_model_capabilities(session: AsyncSession, requested_model_id: str) -> dict[str, Any]:
    rows = (
        await session.execute(
            select(ChannelModel)
            .options(selectinload(ChannelModel.channel), selectinload(ChannelModel.protocol_bindings))
            .join(RouteCandidate, RouteCandidate.channel_model_id == ChannelModel.id)
            .join(ModelRoute, ModelRoute.id == RouteCandidate.route_id)
            .join(Channel, Channel.id == ChannelModel.channel_id)
            .where(
                ModelRoute.requested_model_id == requested_model_id,
                ModelRoute.enabled.is_(True),
                RouteCandidate.enabled.is_(True),
                ChannelModel.available.is_(True),
                Channel.manual_enabled.is_(True),
            )
        )
    ).scalars().unique().all()
    detected: list[dict[str, Any]] = []
    for row in rows:
        metadata = row.metadata_json or {}
        protocols = [binding.protocol for binding in row.protocol_bindings]
        protocol_caps = [
            extract_capabilities(metadata.get(protocol))
            for protocol in protocols
            if isinstance(metadata.get(protocol), dict)
        ]
        if protocol_caps:
            detected.append(aggregate_capabilities(protocol_caps))
        else:
            detected.append(extract_capabilities(metadata))
    return aggregate_capabilities(detected)


def caps_json(row: ModelCaps | None, auto_caps: dict[str, Any] | None = None) -> dict[str, Any]:
    stored_values = {key: getattr(row, key) for key in CAPABILITY_FIELDS} if row else {}
    if row is None:
        values = {key: (auto_caps or {}).get(key) for key in CAPABILITY_FIELDS}
    elif row.source == "auto" and auto_caps is not None:
        values = {key: auto_caps.get(key) for key in CAPABILITY_FIELDS}
    else:
        values = stored_values
    return {
        "source": row.source if row else "auto",
        "context_window": values.get("context_window"),
        "max_tokens": values.get("max_tokens"),
        "supports_image_input": values.get("supports_image_input"),
        "reasoning": values.get("reasoning"),
        "thinking_level_map": values.get("thinking_level_map"),
        "cost": {
            "input": values.get("cost_input"),
            "output": values.get("cost_output"),
            "cacheRead": values.get("cost_cache_read"),
            "cacheWrite": values.get("cost_cache_write"),
        },
        "updated_at": row.updated_at if row else None,
    }


def pi_model_config(capabilities: dict[str, Any] | None) -> dict[str, Any]:
    capabilities = capabilities or {}
    cost = capabilities.get("cost") or {}
    result: dict[str, Any] = {}
    if capabilities.get("supports_image_input") is not None:
        result["input"] = (
            ["text", "image"] if capabilities["supports_image_input"] else ["text"]
        )
    if capabilities.get("reasoning") is not None:
        result["reasoning"] = bool(capabilities["reasoning"])
    if any(cost.get(key) is not None for key in ("input", "output", "cacheRead", "cacheWrite")):
        result["cost"] = {
            "input": cost.get("input") or 0,
            "output": cost.get("output") or 0,
            "cacheRead": cost.get("cacheRead") or 0,
            "cacheWrite": cost.get("cacheWrite") or 0,
        }
    if capabilities.get("context_window") is not None:
        result["contextWindow"] = capabilities["context_window"]
    if capabilities.get("max_tokens") is not None:
        result["maxTokens"] = capabilities["max_tokens"]
    if capabilities.get("thinking_level_map") is not None:
        result["thinkingLevelMap"] = capabilities["thinking_level_map"]
    return result


async def get_or_detect_caps(session: AsyncSession, requested_model_id: str) -> dict[str, Any]:
    row = await session.get(ModelCaps, requested_model_id)
    auto_caps = None
    if row is None or row.source == "auto":
        auto_caps = await detect_model_capabilities(session, requested_model_id)
    return caps_json(row, auto_caps)
