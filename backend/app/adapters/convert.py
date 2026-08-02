"""Protocol conversion between the gateway entry formats and the upstream
gateway protocols.

Two client entry formats are supported:

* ``claude``          - Claude ``/v1/messages`` (Claude Code / claudecode)
* ``openai_responses``- OpenAI Responses API ``/v1/responses`` (Codex / codex)

The conversion approach follows the CLIProxyAPI project
(https://github.com/router-for-me/CLIProxyAPI):

* requests are converted as whole JSON documents before being forwarded,
* streaming responses are converted event-by-event while the upstream stream
  is being read, so the client always sees a valid SSE stream,
* tool calls, images and usage numbers are translated between the formats,
  and model identifiers are substituted with the upstream model name.

Supported upstream protocols:

* ``openai_compatible`` - entry <-> OpenAI chat completions
* ``openai_responses``  - entry <-> OpenAI Responses API
* ``claude``            - passthrough, only the model name is substituted
* ``gemini``            - entry <-> Gemini generateContent
"""

import html
import json
import re
import time
import uuid
from typing import Any

GATEWAY_PROTOCOLS = ("openai_compatible", "openai_responses", "claude", "gemini")

_GATEWAY_ERROR_TYPE = "gateway_error"


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:24]}"


# ---------------------------------------------------------------------------
# Claude -> upstream request conversion
# ---------------------------------------------------------------------------


def _system_text(data: dict[str, Any]) -> str | None:
    system = data.get("system")
    if system is None:
        return None
    if isinstance(system, str):
        return system
    parts = [
        block.get("text", "")
        for block in system
        if isinstance(block, dict) and block.get("type") == "text"
    ]
    return "\n".join(part for part in parts if part) or None


def _content_text(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    return "\n".join(
        block.get("text", "")
        for block in content
        if isinstance(block, dict) and block.get("type") == "text"
    )


def _chat_message_text(content: Any) -> str:
    """Extract plain text from chat-completions ``message.content`` / stream
    ``delta.content``, which may be a string or a list of content parts
    (e.g. ``[{"type": "text", "text": "..."}]`` on GPT-5.x upstreams)."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for part in content:
            if isinstance(part, str):
                parts.append(part)
            elif isinstance(part, dict):
                text = part.get("text")
                if isinstance(text, str) and text:
                    parts.append(text)
        return "\n".join(parts)
    return ""


def _tool_choice_to_openai(tool_choice: Any) -> Any:
    if tool_choice is None or not isinstance(tool_choice, dict):
        return None
    choice_type = tool_choice.get("type")
    if choice_type == "auto":
        return "auto"
    if choice_type == "any":
        return "required"
    if choice_type == "tool" and tool_choice.get("name"):
        return {"type": "function", "function": {"name": tool_choice["name"]}}
    return None


def _tools_to_openai(tools: Any) -> list[dict[str, Any]] | None:
    if not isinstance(tools, list) or not tools:
        return None
    result = []
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        name = tool.get("name")
        if not name:
            continue
        result.append(
            {
                "type": "function",
                "function": {
                    "name": name,
                    "description": tool.get("description") or "",
                    "parameters": tool.get("input_schema") or {"type": "object", "properties": {}},
                },
            }
        )
    return result or None


def claude_to_openai_compatible(upstream_model: str, data: dict[str, Any]) -> dict[str, Any]:
    """Convert a Claude /v1/messages request into OpenAI chat completions format."""
    messages: list[dict[str, Any]] = []
    system_text = _system_text(data)
    if system_text:
        messages.append({"role": "system", "content": system_text})

    for message in data.get("messages", []):
        role = message.get("role")
        content = message.get("content")
        if role not in ("user", "assistant"):
            continue
        if isinstance(content, str):
            messages.append({"role": role, "content": content})
            continue
        if not isinstance(content, list):
            continue

        text_parts: list[str] = []
        image_parts: list[dict[str, Any]] = []
        tool_calls: list[dict[str, Any]] = []
        tool_results: list[dict[str, Any]] = []
        for block in content:
            if not isinstance(block, dict):
                continue
            block_type = block.get("type")
            if block_type == "text":
                text_parts.append(block.get("text", ""))
            elif block_type == "image":
                source = block.get("source") or {}
                image_parts.append(
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": (
                                f"data:{source.get('media_type', 'image/png')}"
                                f";base64,{source.get('data', '')}"
                            )
                        },
                    }
                )
            elif block_type == "tool_use":
                tool_calls.append(
                    {
                        "id": block.get("id") or _new_id("call"),
                        "type": "function",
                        "function": {
                            "name": block.get("name", ""),
                            "arguments": json.dumps(
                                block.get("input") or {}, ensure_ascii=False
                            ),
                        },
                    }
                )
            elif block_type == "tool_result":
                tool_results.append(
                    {
                        "role": "tool",
                        "tool_call_id": block.get("tool_use_id") or "",
                        "content": _content_text(block.get("content")),
                    }
                )

        messages.extend(tool_results)
        content_parts: list[dict[str, Any]] = []
        if text_parts:
            content_parts.append({"type": "text", "text": "\n".join(text_parts)})
        content_parts.extend(image_parts)
        if tool_calls or content_parts:
            converted: dict[str, Any] = {"role": role}
            if tool_calls:
                converted["tool_calls"] = tool_calls
                if content_parts:
                    converted["content"] = (
                        content_parts[0]["text"] if len(content_parts) == 1 else content_parts
                    )
                else:
                    converted["content"] = None
            elif len(content_parts) == 1:
                converted["content"] = content_parts[0]["text"]
            else:
                converted["content"] = content_parts
            messages.append(converted)

    payload: dict[str, Any] = {
        "model": upstream_model,
        "messages": messages,
        "stream": bool(data.get("stream", False)),
    }
    if "max_tokens" in data and isinstance(data["max_tokens"], int):
        payload["max_tokens"] = data["max_tokens"]
    if "temperature" in data:
        payload["temperature"] = data["temperature"]
    if "top_p" in data:
        payload["top_p"] = data["top_p"]
    stop_sequences = data.get("stop_sequences")
    if isinstance(stop_sequences, list) and stop_sequences:
        payload["stop"] = stop_sequences
    tools = _tools_to_openai(data.get("tools"))
    if tools:
        payload["tools"] = tools
    tool_choice = _tool_choice_to_openai(data.get("tool_choice"))
    if tool_choice is not None:
        payload["tool_choice"] = tool_choice
    return payload


def claude_to_openai_responses(upstream_model: str, data: dict[str, Any]) -> dict[str, Any]:
    """Convert a Claude /v1/messages request into OpenAI Responses format."""
    items: list[dict[str, Any]] = []
    for message in data.get("messages", []):
        role = message.get("role")
        content = message.get("content")
        if role not in ("user", "assistant"):
            continue
        if isinstance(content, str):
            items.append(
                {
                    "type": "message",
                    "role": role,
                    "content": [{"type": "input_text", "text": content}],
                }
            )
            continue
        if not isinstance(content, list):
            continue
        text_parts: list[str] = []
        image_parts: list[dict[str, Any]] = []
        for block in content:
            if not isinstance(block, dict):
                continue
            block_type = block.get("type")
            if block_type == "text":
                text_parts.append(block.get("text", ""))
            elif block_type == "image":
                source = block.get("source") or {}
                image_parts.append(
                    {
                        "type": "input_image",
                        "image_url": (
                            f"data:{source.get('media_type', 'image/png')}"
                            f";base64,{source.get('data', '')}"
                        ),
                    }
                )
            elif block_type == "tool_use":
                items.append(
                    {
                        "type": "function_call",
                        "call_id": block.get("id") or _new_id("call"),
                        "name": block.get("name", ""),
                        "arguments": json.dumps(block.get("input") or {}, ensure_ascii=False),
                    }
                )
            elif block_type == "tool_result":
                items.append(
                    {
                        "type": "function_call_output",
                        "call_id": block.get("tool_use_id") or "",
                        "output": _content_text(block.get("content")),
                    }
                )
        if text_parts or image_parts:
            content: list[dict[str, Any]] = []
            if text_parts:
                content.append({"type": "input_text", "text": "\n".join(text_parts)})
            content.extend(image_parts)
            items.append({"type": "message", "role": role, "content": content})

    payload: dict[str, Any] = {
        "model": upstream_model,
        "input": items,
        "stream": bool(data.get("stream", False)),
    }
    if isinstance(data.get("max_tokens"), int):
        payload["max_output_tokens"] = data["max_tokens"]
    if "temperature" in data:
        payload["temperature"] = data["temperature"]
    if "top_p" in data:
        payload["top_p"] = data["top_p"]
    instructions = _system_text(data)
    if instructions:
        payload["instructions"] = instructions
    tools = _tools_to_openai(data.get("tools"))
    if tools:
        payload["tools"] = [
            {**item["function"], "type": "function"} for item in tools
        ]
    tool_choice = _tool_choice_to_openai(data.get("tool_choice"))
    if tool_choice is not None:
        payload["tool_choice"] = tool_choice
    return payload


def claude_to_gemini(upstream_model: str, data: dict[str, Any]) -> dict[str, Any]:
    """Convert a Claude /v1/messages request into Gemini generateContent format."""
    tool_names: dict[str, str] = {}
    for message in data.get("messages", []):
        content = message.get("content")
        if not isinstance(content, list):
            continue
        for block in content:
            if isinstance(block, dict) and block.get("type") == "tool_use":
                tool_names[block.get("id")] = block.get("name") or "unknown_tool"

    contents: list[dict[str, Any]] = []
    for message in data.get("messages", []):
        role = "model" if message.get("role") == "assistant" else "user"
        content = message.get("content")
        parts: list[dict[str, Any]] = []
        if isinstance(content, str):
            parts.append({"text": content})
        elif isinstance(content, list):
            for block in content:
                if not isinstance(block, dict):
                    continue
                block_type = block.get("type")
                if block_type == "text":
                    parts.append({"text": block.get("text", "")})
                elif block_type == "image":
                    source = block.get("source") or {}
                    parts.append(
                        {
                            "inlineData": {
                                "mimeType": source.get("media_type", "image/png"),
                                "data": source.get("data", ""),
                            }
                        }
                    )
                elif block_type == "tool_use":
                    parts.append(
                        {
                            "functionCall": {
                                "name": block.get("name", ""),
                                "args": block.get("input") or {},
                            }
                        }
                    )
                elif block_type == "tool_result":
                    parts.append(
                        {
                            "functionResponse": {
                                "name": tool_names.get(block.get("tool_use_id"))
                                or "unknown_tool",
                                "response": {
                                    "result": _content_text(block.get("content"))
                                },
                            }
                        }
                    )
        if parts:
            contents.append({"role": role, "parts": parts})

    payload: dict[str, Any] = {"contents": contents}
    system_text = _system_text(data)
    if system_text:
        payload["systemInstruction"] = {"parts": [{"text": system_text}]}
    tools = []
    for tool in data.get("tools") or []:
        if not isinstance(tool, dict) or not tool.get("name"):
            continue
        tools.append(
            {
                "functionDeclarations": [
                    {
                        "name": tool["name"],
                        "description": tool.get("description") or "",
                        "parameters": tool.get("input_schema")
                        or {"type": "object", "properties": {}},
                    }
                ]
            }
        )
    if tools:
        payload["tools"] = tools

    generation_config: dict[str, Any] = {}
    if isinstance(data.get("max_tokens"), int):
        generation_config["maxOutputTokens"] = data["max_tokens"]
    if "temperature" in data:
        generation_config["temperature"] = data["temperature"]
    if "top_p" in data:
        generation_config["topP"] = data["top_p"]
    if "top_k" in data:
        generation_config["topK"] = data["top_k"]
    stop_sequences = data.get("stop_sequences")
    if isinstance(stop_sequences, list) and stop_sequences:
        generation_config["stopSequences"] = stop_sequences
    if generation_config:
        payload["generationConfig"] = generation_config
    thinking = data.get("thinking")
    if isinstance(thinking, dict) and isinstance(thinking.get("budget_tokens"), int):
        payload["thinkingConfig"] = {"thinkingBudget": thinking["budget_tokens"]}
    return payload


def convert_request(
    upstream_protocol: str, upstream_model: str, body: bytes
) -> bytes:
    """Convert a Claude /v1/messages request body for the upstream protocol."""
    try:
        data = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"Invalid Claude request body: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("Claude request body must be a JSON object")
    if upstream_protocol == "claude":
        converted = dict(data)
        converted["model"] = upstream_model
        return json.dumps(converted, ensure_ascii=False).encode("utf-8")
    if upstream_protocol == "openai_compatible":
        payload = claude_to_openai_compatible(upstream_model, data)
    elif upstream_protocol == "openai_responses":
        payload = claude_to_openai_responses(upstream_model, data)
    elif upstream_protocol == "gemini":
        payload = claude_to_gemini(upstream_model, data)
    else:
        raise ValueError(f"Unsupported upstream protocol: {upstream_protocol}")
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


# ---------------------------------------------------------------------------
# upstream -> Claude response conversion (non-streaming)
# ---------------------------------------------------------------------------


_STOP_REASON_OPENAI = {
    "stop": "end_turn",
    "length": "max_tokens",
    "tool_calls": "tool_use",
    "function_call": "tool_use",
    "content_filter": "refusal",
    "null": "end_turn",
}
_STOP_REASON_GEMINI = {
    "STOP": "end_turn",
    "MAX_TOKENS": "max_tokens",
    "SAFETY": "refusal",
    "RECITATION": "refusal",
    "TOOL_CALL": "tool_use",
    "FUNCTION_CALL": "tool_use",
    "MALFORMED_FUNCTION_CALL": "tool_use",
    "FINISH_REASON_UNSPECIFIED": "end_turn",
}


def _claude_usage(
    input_tokens: int | None,
    output_tokens: int | None,
    cache_read: int | None = None,
    cache_write: int | None = None,
) -> dict[str, Any]:
    usage: dict[str, Any] = {
        "input_tokens": input_tokens or 0,
        "output_tokens": output_tokens or 0,
    }
    if cache_read:
        usage["cache_read_input_tokens"] = cache_read
    if cache_write:
        usage["cache_creation_input_tokens"] = cache_write
    return usage


def openai_compatible_to_claude(claude_model: str, data: dict[str, Any]) -> dict[str, Any]:
    choices = data.get("choices") or []
    choice = choices[0] if choices else {}
    message = choice.get("message") or {}
    content: list[dict[str, Any]] = []
    reasoning = message.get("reasoning_content") or message.get("reasoning")
    if reasoning:
        content.append({"type": "thinking", "thinking": reasoning})
    text = _chat_message_text(message.get("content"))
    if text:
        content.append({"type": "text", "text": text})
    for tool_call in message.get("tool_calls") or []:
        function = tool_call.get("function") or {}
        try:
            arguments = json.loads(function.get("arguments") or "{}")
        except (json.JSONDecodeError, TypeError):
            arguments = {}
        content.append(
            {
                "type": "tool_use",
                "id": tool_call.get("id") or _new_id("toolu"),
                "name": function.get("name", ""),
                "input": arguments if isinstance(arguments, dict) else {},
            }
        )
    usage = data.get("usage") or {}
    prompt_details = usage.get("prompt_tokens_details") or usage.get("input_tokens_details") or {}
    cache_read = prompt_details.get("cached_tokens") or prompt_details.get(
        "cache_read_input_tokens"
    ) or prompt_details.get("prompt_cache_hit_tokens")
    cache_write = prompt_details.get(
        "cache_write_tokens", prompt_details.get("cached_write_tokens")
    )
    return {
        "id": data.get("id") or _new_id("msg"),
        "type": "message",
        "role": "assistant",
        "model": claude_model,
        "content": content,
        "stop_reason": _STOP_REASON_OPENAI.get(
            choice.get("finish_reason") or "stop", "end_turn"
        ),
        "stop_sequence": None,
        "usage": _claude_usage(
            usage.get("prompt_tokens", usage.get("input_tokens")),
            usage.get("completion_tokens", usage.get("output_tokens")),
            cache_read,
            cache_write,
        ),
    }


def openai_responses_to_claude(claude_model: str, data: dict[str, Any]) -> dict[str, Any]:
    output = data.get("output") or []
    content: list[dict[str, Any]] = []
    for item in output:
        if not isinstance(item, dict):
            continue
        item_type = item.get("type")
        if item_type in {"message", "reasoning"}:
            for part in item.get("content") or []:
                if not isinstance(part, dict):
                    continue
                if part.get("type") == "output_text" and part.get("text"):
                    content.append({"type": "text", "text": part["text"]})
                elif part.get("type") == "summary_text" and part.get("text"):
                    content.append({"type": "text", "text": part["text"]})
        elif item_type == "function_call":
            try:
                arguments = json.loads(item.get("arguments") or "{}")
            except (json.JSONDecodeError, TypeError):
                arguments = {}
            content.append(
                {
                    "type": "tool_use",
                    "id": item.get("call_id") or _new_id("toolu"),
                    "name": item.get("name", ""),
                    "input": arguments if isinstance(arguments, dict) else {},
                }
            )
    usage = data.get("usage") or {}
    input_details = usage.get("input_tokens_details") or {}
    return {
        "id": data.get("id") or _new_id("msg"),
        "type": "message",
        "role": "assistant",
        "model": claude_model,
        "content": content,
        "stop_reason": "tool_use" if content and content[-1]["type"] == "tool_use" else "end_turn",
        "stop_sequence": None,
        "usage": _claude_usage(
            usage.get("input_tokens"),
            usage.get("output_tokens"),
            input_details.get("cached_tokens"),
            input_details.get("cache_write_tokens"),
        ),
    }


def gemini_to_claude(claude_model: str, data: dict[str, Any]) -> dict[str, Any]:
    candidates = data.get("candidates") or []
    candidate = candidates[0] if candidates else {}
    parts = ((candidate.get("content") or {}).get("parts")) or []
    content: list[dict[str, Any]] = []
    for part in parts:
        if not isinstance(part, dict):
            continue
        if part.get("thought") is not None:
            content.append({"type": "thinking", "thinking": part["thought"]})
        elif "text" in part:
            content.append({"type": "text", "text": part["text"]})
        if isinstance(part.get("functionCall"), dict):
            function_call = part["functionCall"]
            content.append(
                {
                    "type": "tool_use",
                    "id": _new_id("toolu"),
                    "name": function_call.get("name", ""),
                    "input": function_call.get("args") or {},
                }
            )
    usage = data.get("usageMetadata") or {}
    return {
        "id": _new_id("msg"),
        "type": "message",
        "role": "assistant",
        "model": claude_model,
        "content": content,
        "stop_reason": _STOP_REASON_GEMINI.get(
            candidate.get("finishReason") or "STOP", "end_turn"
        ),
        "stop_sequence": None,
        "usage": _claude_usage(
            usage.get("promptTokenCount"),
            usage.get("candidatesTokenCount"),
            usage.get("cachedContentTokenCount"),
            None,
        ),
    }


def convert_response(
    upstream_protocol: str, claude_model: str, body: bytes
) -> bytes:
    """Convert a non-streaming upstream response into Claude /v1/messages format."""
    if upstream_protocol == "claude":
        return body
    try:
        data = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"Invalid upstream response body: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("Upstream response body must be a JSON object")
    if upstream_protocol == "openai_compatible":
        converted = openai_compatible_to_claude(claude_model, data)
    elif upstream_protocol == "openai_responses":
        converted = openai_responses_to_claude(claude_model, data)
    elif upstream_protocol == "gemini":
        converted = gemini_to_claude(claude_model, data)
    else:
        raise ValueError(f"Unsupported upstream protocol: {upstream_protocol}")
    return json.dumps(converted, ensure_ascii=False).encode("utf-8")


# ---------------------------------------------------------------------------
# upstream -> Claude streaming response conversion
# ---------------------------------------------------------------------------


def _sse(event: str, payload: dict[str, Any]) -> bytes:
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n".encode(
        "utf-8"
    )


class ClaudeSSEConverter:
    """Stateful converter turning an upstream SSE stream into Claude SSE events."""

    entry_protocol = "claude"

    def __init__(self, upstream_protocol: str, claude_model: str) -> None:
        self.upstream_protocol = upstream_protocol
        self.claude_model = claude_model
        self.message_id = _new_id("msg")
        self.request_id = _new_id("req")
        self._started = False
        self._finished = False
        self._open_blocks: dict[int, str] = {}
        self._usage: dict[str, Any] = {}

    # -- base helpers ------------------------------------------------------

    def _ensure_start(self) -> bytes:
        if self._started:
            return b""
        self._started = True
        return _sse(
            "message_start",
            {
                "type": "message_start",
                "message": {
                    "id": self.message_id,
                    "type": "message",
                    "role": "assistant",
                    "model": self.claude_model,
                    "content": [],
                    "stop_reason": None,
                    "stop_sequence": None,
                    "usage": _claude_usage(0, 0),
                },
            },
        )

    def _start_block(self, index: int, block: dict[str, Any]) -> bytes:
        if index in self._open_blocks:
            return b""
        self._open_blocks[index] = block.get("type", "text")
        return _sse(
            "content_block_start",
            {"type": "content_block_start", "index": index, "content_block": block},
        )

    def _delta(self, index: int, delta: dict[str, Any]) -> bytes:
        return _sse(
            "content_block_delta",
            {"type": "content_block_delta", "index": index, "delta": delta},
        )

    def _stop_block(self, index: int) -> bytes:
        if index not in self._open_blocks:
            return b""
        del self._open_blocks[index]
        return _sse("content_block_stop", {"type": "content_block_stop", "index": index})

    def _current_block_of_type(self, block_type: str) -> int | None:
        for index, current_type in self._open_blocks.items():
            if current_type == block_type:
                return index
        return None

    def _finish(self, stop_reason: str = "end_turn") -> bytes:
        if self._finished:
            return b""
        self._finished = True
        output = bytearray()
        for index in sorted(self._open_blocks):
            output += self._stop_block(index)
        output += _sse(
            "message_delta",
            {
                "type": "message_delta",
                "delta": {"stop_reason": stop_reason, "stop_sequence": None},
                "usage": _claude_usage(
                    self._usage.get("input_tokens"),
                    self._usage.get("output_tokens"),
                    self._usage.get("cache_read_input_tokens"),
                    self._usage.get("cache_creation_input_tokens"),
                ),
            },
        )
        output += _sse("message_stop", {"type": "message_stop"})
        return bytes(output)

    def error_event(self, message: str) -> bytes:
        if not self._started:
            self._ensure_start()
        self._finished = True
        return _sse(
            "error",
            {
                "type": "error",
                "error": {"type": _GATEWAY_ERROR_TYPE, "message": message},
                "request_id": self.request_id,
            },
        )

    def feed(self, chunk: bytes) -> bytes:
        raise NotImplementedError

    def flush(self) -> bytes:
        return self._finish() if not self._finished else b""

    # -- shared SSE line parsing -------------------------------------------

    def _iter_events(self, chunk: bytes, buffer: bytearray) -> list[Any]:
        buffer.extend(chunk.replace(b"\r\n", b"\n"))
        events: list[Any] = []
        while True:
            marker = buffer.find(b"\n\n")
            if marker == -1:
                break
            block = bytes(buffer[:marker])
            del buffer[: marker + 2]
            data_lines: list[bytes] = []
            for line in block.splitlines():
                line = line.strip()
                if line.startswith(b"data:"):
                    data_lines.append(line[5:].strip())
            if not data_lines:
                continue
            payload = b"\n".join(data_lines).strip()
            if payload == b"[DONE]":
                events.append("__DONE__")
                continue
            try:
                events.append(json.loads(payload))
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
        return events

    def _merge_usage(self, usage: dict[str, Any]) -> None:
        if not isinstance(usage, dict):
            return
        for key in (
            "input_tokens",
            "output_tokens",
            "cache_read_input_tokens",
            "cache_creation_input_tokens",
        ):
            if key in usage and isinstance(usage[key], int):
                self._usage[key] = usage[key]


class OpenAICompatibleStreamToClaude(ClaudeSSEConverter):
    def __init__(self, claude_model: str) -> None:
        super().__init__("openai_compatible", claude_model)
        self._buffer = bytearray()
        self._tool_index: dict[str, int] = {}
        self._tool_by_upstream_index: dict[int, int] = {}
        self._pending_finish_reason: str | None = None
        self._next_index = 0

    def _next_block_index(self) -> int:
        while self._next_index in self._open_blocks:
            self._next_index += 1
        return self._next_index

    def feed(self, chunk: bytes) -> bytes:
        if self._finished:
            return b""
        output = bytearray()
        for event in self._iter_events(chunk, self._buffer):
            if event == "__DONE__":
                output += self._finish(self._pending_finish_reason or "end_turn")
                continue
            if not isinstance(event, dict):
                continue
            output += self._consume(event)
        return bytes(output)

    def flush(self) -> bytes:
        if self._finished:
            return b""
        return self._finish(self._pending_finish_reason or "end_turn")

    def _consume(self, event: dict[str, Any]) -> bytes:
        output = bytearray()
        output += self._ensure_start()
        if isinstance(event.get("usage"), dict):
            self._merge_openai_usage(event["usage"])
        choices = event.get("choices") or []
        if not choices:
            return bytes(output)
        choice = choices[0]
        delta = choice.get("delta") or {}

        reasoning = delta.get("reasoning_content") or delta.get("reasoning")
        if reasoning:
            index = self._next_block_index()
            output += self._start_block(index, {"type": "thinking", "thinking": ""})
            output += self._delta(index, {"type": "thinking_delta", "thinking": reasoning})

        text = _chat_message_text(delta.get("content"))
        if text:
            index = self._current_block_of_type("text")
            if index is None:
                index = self._next_block_index()
                output += self._start_block(index, {"type": "text", "text": ""})
            output += self._delta(index, {"type": "text_delta", "text": text})

        for tool_call in delta.get("tool_calls") or []:
            if not isinstance(tool_call, dict):
                continue
            tool_id = tool_call.get("id")
            upstream_index = tool_call.get("index")
            index = None
            if tool_id and tool_id in self._tool_index:
                index = self._tool_index[tool_id]
            elif isinstance(upstream_index, int) and upstream_index in self._tool_by_upstream_index:
                index = self._tool_by_upstream_index[upstream_index]
            if index is None:
                index = self._next_block_index()
                if tool_id:
                    self._tool_index[tool_id] = index
                if isinstance(upstream_index, int):
                    self._tool_by_upstream_index[upstream_index] = index
                function = tool_call.get("function") or {}
                output += self._start_block(
                    index,
                    {
                        "type": "tool_use",
                        "id": tool_id or _new_id("toolu"),
                        "name": function.get("name") or "",
                        "input": {},
                    },
                )
            arguments_delta = (tool_call.get("function") or {}).get("arguments")
            if arguments_delta:
                output += self._delta(
                    index, {"type": "input_json_delta", "partial_json": arguments_delta}
                )

        finish_reason = choice.get("finish_reason")
        if finish_reason is not None:
            self._pending_finish_reason = _STOP_REASON_OPENAI.get(finish_reason, "end_turn")
        return bytes(output)

    def _merge_openai_usage(self, usage: dict[str, Any]) -> None:
        prompt_details = usage.get("prompt_tokens_details") or {}
        self._usage["input_tokens"] = usage.get("prompt_tokens") or 0
        self._usage["output_tokens"] = usage.get("completion_tokens") or 0
        cache_read = (
            prompt_details.get("cached_tokens")
            or prompt_details.get("prompt_cache_hit_tokens")
            or 0
        )
        cache_write = prompt_details.get("cache_write_tokens", 0)
        if cache_read:
            self._usage["cache_read_input_tokens"] = cache_read
        if cache_write:
            self._usage["cache_creation_input_tokens"] = cache_write


class OpenAIResponsesStreamToClaude(ClaudeSSEConverter):
    def __init__(self, claude_model: str) -> None:
        super().__init__("openai_responses", claude_model)
        self._buffer = bytearray()
        self._item_index: dict[str, int] = {}
        self._next_index = 0

    def _next_block_index(self) -> int:
        while self._next_index in self._open_blocks:
            self._next_index += 1
        return self._next_index

    def feed(self, chunk: bytes) -> bytes:
        if self._finished:
            return b""
        output = bytearray()
        for event in self._iter_events(chunk, self._buffer):
            if event == "__DONE__":
                output += self._finish()
                continue
            if not isinstance(event, dict):
                continue
            output += self._consume(event)
        return bytes(output)

    def _consume(self, event: dict[str, Any]) -> bytes:
        output = bytearray()
        output += self._ensure_start()
        event_type = event.get("type") or ""
        if event_type == "response.output_item.added":
            item = event.get("item") or {}
            if item.get("type") == "function_call":
                index = self._next_block_index()
                call_id = item.get("call_id") or _new_id("toolu")
                self._item_index[item.get("id") or call_id] = index
                self._next_index = index + 1
                output += self._start_block(
                    index,
                    {
                        "type": "tool_use",
                        "id": call_id,
                        "name": item.get("name", ""),
                        "input": {},
                    },
                )
        elif event_type == "response.function_call_arguments.delta":
            item_id = event.get("item_id")
            index = self._item_index.get(item_id)
            if index is not None:
                output += self._delta(
                    index, {"type": "input_json_delta", "partial_json": event.get("delta", "")}
                )
        elif event_type == "response.output_text.delta":
            index = self._current_block_of_type("text")
            if index is None:
                index = self._next_block_index()
                output += self._start_block(index, {"type": "text", "text": ""})
            output += self._delta(index, {"type": "text_delta", "text": event.get("delta", "")})
        elif event_type == "response.output_text.done":
            index = self._current_block_of_type("text")
            if index is not None:
                output += self._stop_block(index)
        elif event_type == "response.function_call_arguments.done":
            index = self._item_index.get(event.get("item_id"))
            if index is not None:
                output += self._stop_block(index)
        elif event_type == "response.completed":
            response = event.get("response") or {}
            self._merge_usage(response.get("usage") or {})
            output += self._finish()
        return bytes(output)


class GeminiStreamToClaude(ClaudeSSEConverter):
    def __init__(self, claude_model: str) -> None:
        super().__init__("gemini", claude_model)
        self._buffer = bytearray()
        self._block_index = 0
        self._finished_finish_reason: str | None = None

    def _iter_events(self, chunk: bytes, buffer: bytearray) -> list[Any]:
        """Gemini streams are newline-separated JSON objects (some proxies wrap
        them in SSE ``data:`` frames), so split on single newlines."""
        buffer.extend(chunk.replace(b"\r\n", b"\n"))
        events: list[Any] = []
        while True:
            marker = buffer.find(b"\n")
            if marker == -1:
                break
            line = bytes(buffer[:marker]).strip()
            del buffer[: marker + 1]
            if not line:
                continue
            if line.startswith(b"data:"):
                line = line[5:].strip()
            if line == b"[DONE]":
                events.append("__DONE__")
                continue
            try:
                events.append(json.loads(line))
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
        return events

    def feed(self, chunk: bytes) -> bytes:
        if self._finished:
            return b""
        output = bytearray()
        for event in self._iter_events(chunk, self._buffer):
            if event == "__DONE__":
                output += self._finish()
                continue
            if not isinstance(event, dict):
                continue
            output += self._consume(event)
        return bytes(output)

    def _consume(self, event: dict[str, Any]) -> bytes:
        output = bytearray()
        output += self._ensure_start()
        candidates = event.get("candidates") or []
        if not candidates:
            self._merge_usage(event.get("usageMetadata") or {})
            return bytes(output)
        candidate = candidates[0]
        parts = ((candidate.get("content") or {}).get("parts")) or []
        for part in parts:
            if not isinstance(part, dict):
                continue
            if part.get("thought") is not None:
                index = self._next_open()
                output += self._start_block(index, {"type": "thinking", "thinking": ""})
                output += self._delta(
                    index, {"type": "thinking_delta", "thinking": part["thought"]}
                )
            elif "text" in part:
                index = self._current_text_index()
                if index is None:
                    index = self._next_open()
                    output += self._start_block(index, {"type": "text", "text": ""})
                output += self._delta(index, {"type": "text_delta", "text": part["text"]})
            if isinstance(part.get("functionCall"), dict):
                function_call = part["functionCall"]
                index = self._next_open()
                output += self._start_block(
                    index,
                    {
                        "type": "tool_use",
                        "id": _new_id("toolu"),
                        "name": function_call.get("name", ""),
                        "input": {},
                    },
                )
                arguments = json.dumps(
                    function_call.get("args") or {}, ensure_ascii=False
                )
                if arguments:
                    output += self._delta(
                        index, {"type": "input_json_delta", "partial_json": arguments}
                    )
                output += self._stop_block(index)
        self._merge_usage(event.get("usageMetadata") or {})
        finish_reason = candidate.get("finishReason")
        if finish_reason:
            output += self._finish(_STOP_REASON_GEMINI.get(finish_reason, "end_turn"))
        return bytes(output)

    def _next_open(self) -> int:
        while self._block_index in self._open_blocks:
            self._block_index += 1
        return self._block_index

    def _current_text_index(self) -> int | None:
        for index, block_type in self._open_blocks.items():
            if block_type == "text":
                return index
        return None

    def _merge_usage(self, usage: dict[str, Any]) -> None:
        if not isinstance(usage, dict):
            return
        if isinstance(usage.get("promptTokenCount"), int):
            self._usage["input_tokens"] = usage["promptTokenCount"]
        if isinstance(usage.get("candidatesTokenCount"), int):
            self._usage["output_tokens"] = usage["candidatesTokenCount"]
        if isinstance(usage.get("cachedContentTokenCount"), int):
            self._usage["cache_read_input_tokens"] = usage["cachedContentTokenCount"]


class PassthroughStreamToClaude(ClaudeSSEConverter):
    def __init__(self, claude_model: str) -> None:
        super().__init__("claude", claude_model)

    def feed(self, chunk: bytes) -> bytes:
        return chunk

    def flush(self) -> bytes:
        return b""

    def error_event(self, message: str) -> bytes:
        return _sse(
            "error",
            {
                "type": "error",
                "error": {"type": _GATEWAY_ERROR_TYPE, "message": message},
                "request_id": self.request_id,
            },
        )


def get_streaming_converter(
    upstream_protocol: str, claude_model: str
) -> ClaudeSSEConverter:
    if upstream_protocol == "claude":
        return PassthroughStreamToClaude(claude_model)
    if upstream_protocol == "openai_compatible":
        return OpenAICompatibleStreamToClaude(claude_model)
    if upstream_protocol == "openai_responses":
        return OpenAIResponsesStreamToClaude(claude_model)
    if upstream_protocol == "gemini":
        return GeminiStreamToClaude(claude_model)
    raise ValueError(f"Unsupported upstream protocol: {upstream_protocol}")


# ---------------------------------------------------------------------------
# upstream error -> Claude error conversion
# ---------------------------------------------------------------------------


def convert_error_response(upstream_protocol: str, body: bytes) -> bytes:
    """Convert a non-2xx upstream error body into Claude /v1/messages error format."""
    if upstream_protocol == "claude":
        return body
    message = "Upstream request failed."
    status = "upstream_error"
    try:
        data = json.loads(body)
        error = data.get("error") if isinstance(data, dict) else None
        if isinstance(error, dict):
            message = error.get("message") or message
            error_type = error.get("type") or error.get("code") or error.get("status")
            if error_type:
                status = str(error_type).lower().replace(" ", "_")
    except (json.JSONDecodeError, UnicodeDecodeError):
        message = body.decode("utf-8", errors="ignore").strip() or message
    payload = {
        "type": "error",
        "error": {"type": status, "message": message},
    }
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


# ===========================================================================
# OpenAI Responses API entry (Codex / codex CLI)
# ===========================================================================
#
# The Responses API /v1/responses entry format mirrors the /v1/messages
# mapping: the codex model id is substituted with the upstream model and the
# payload is converted to the upstream protocol. Responses are converted back
# to the Responses format, both non-streaming and via Responses SSE events.
#
# Reasoning/thinking blocks from upstream models are intentionally dropped in
# the Responses output: codex does not render them and the reasoning SSE event
# sequence (response.reasoning_summary_text.delta) would otherwise have to be
# kept in lockstep with output_item.done for the client accumulator to accept
# the summary.
# ---------------------------------------------------------------------------

# Responses request helpers -------------------------------------------------


def _responses_instructions_text(instructions: Any) -> str | None:
    """Extract the system text from a Responses ``instructions`` field."""
    if instructions is None:
        return None
    if isinstance(instructions, str):
        return instructions
    if isinstance(instructions, list):
        parts = [
            part.get("text", "")
            for part in instructions
            if isinstance(part, dict) and part.get("type") in ("input_text", "text")
        ]
        text = "\n".join(p for p in parts if p)
        return text or None
    return None


def _responses_text(part: Any) -> str | None:
    if isinstance(part, str):
        return part
    if isinstance(part, dict):
        return part.get("text")
    return None


def _responses_tools_to_openai(tools: Any) -> list[dict] | None:
    if not isinstance(tools, list) or not tools:
        return None
    result = []
    for tool in tools:
        if not isinstance(tool, dict) or tool.get("type") != "function":
            continue
        name = tool.get("name")
        if not name:
            continue
        result.append(
            {
                "type": "function",
                "function": {
                    "name": name,
                    "description": tool.get("description") or "",
                    "parameters": tool.get("parameters")
                    or {"type": "object", "properties": {}},
                },
            }
        )
    return result or None


def _responses_tool_choice(tool_choice: Any) -> Any:
    if tool_choice is None:
        return None
    if isinstance(tool_choice, str):
        return tool_choice
    if (
        isinstance(tool_choice, dict)
        and tool_choice.get("type") == "function"
        and tool_choice.get("name")
    ):
        return {"type": "function", "function": {"name": tool_choice["name"]}}
    return None


def _responses_tools_to_claude(tools: Any) -> list[dict] | None:
    if not isinstance(tools, list) or not tools:
        return None
    result = []
    for tool in tools:
        if not isinstance(tool, dict) or tool.get("type") != "function":
            continue
        name = tool.get("name")
        if not name:
            continue
        result.append(
            {
                "name": name,
                "description": tool.get("description") or "",
                "input_schema": tool.get("parameters")
                or {"type": "object", "properties": {}},
            }
        )
    return result or None


def _responses_tools_to_gemini(tools: Any) -> list[dict] | None:
    declarations = []
    for tool in tools or []:
        if not isinstance(tool, dict) or tool.get("type") != "function":
            continue
        name = tool.get("name")
        if not name:
            continue
        declarations.append(
            {
                "name": name,
                "description": tool.get("description") or "",
                "parameters": tool.get("parameters")
                or {"type": "object", "properties": {}},
            }
        )
    return [{"functionDeclarations": declarations}] if declarations else None


def _parse_arguments(arguments: Any) -> dict:
    if isinstance(arguments, dict):
        return arguments
    try:
        parsed = json.loads(arguments or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


def _parse_data_url(url: Any) -> tuple[str, str]:
    """Split a ``data:`` URL into ``(media_type, base64_data)``."""
    if not isinstance(url, str) or not url.startswith("data:"):
        return "image/png", ""
    meta, _, data = url.partition(",")
    media_type = meta[len("data:") :].split(";")[0] or "image/png"
    return media_type, data


def _responses_image_url(part: dict) -> str | None:
    image_url = part.get("image_url")
    if isinstance(image_url, str) and image_url:
        return image_url
    if isinstance(image_url, dict) and image_url.get("url"):
        return image_url["url"]
    return None


# Responses -> upstream request conversion ----------------------------------


def responses_to_openai_compatible(upstream_model: str, data: dict) -> dict:
    messages: list[dict] = []
    instructions = _responses_instructions_text(data.get("instructions"))
    if instructions:
        messages.append({"role": "system", "content": instructions})

    for item in data.get("input") or []:
        if not isinstance(item, dict):
            continue
        item_type = item.get("type")
        if item_type == "message":
            role = item.get("role") or "user"
            if role == "developer":
                role = "system"
            elif role not in ("user", "assistant", "system", "tool"):
                role = "user"
            content = item.get("content")
            if isinstance(content, str):
                messages.append({"role": role, "content": content})
                continue
            if not isinstance(content, list):
                continue
            text_parts: list[str] = []
            image_parts: list[dict] = []
            for part in content:
                if not isinstance(part, dict):
                    continue
                part_type = part.get("type")
                if part_type == "input_text":
                    text_parts.append(part.get("text", ""))
                elif part_type == "input_image":
                    image_url = _responses_image_url(part)
                    if image_url:
                        image_parts.append(
                            {"type": "image_url", "image_url": {"url": image_url}}
                        )
            parts: list[dict] = []
            if text_parts:
                parts.append({"type": "text", "text": "\n".join(text_parts)})
            parts.extend(image_parts)
            if parts:
                messages.append({"role": role, "content": parts})
        elif item_type == "function_call":
            tool_call = {
                "id": item.get("call_id") or _new_id("call"),
                "type": "function",
                "function": {
                    "name": item.get("name", ""),
                    "arguments": item.get("arguments") or "{}",
                },
            }
            if (
                messages
                and messages[-1].get("role") == "assistant"
                and isinstance(messages[-1].get("tool_calls"), list)
            ):
                messages[-1]["tool_calls"].append(tool_call)
                if item.get("reasoning_content"):
                    messages[-1]["reasoning_content"] = item["reasoning_content"]
            else:
                messages.append(
                    {
                        "role": "assistant",
                        "content": None,
                        "reasoning_content": item.get("reasoning_content") or "",
                        "tool_calls": [tool_call],
                    }
                )
        elif item_type == "function_call_output":
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": item.get("call_id") or "",
                    "content": _responses_text(item.get("output")) or "",
                }
            )

    payload: dict[str, Any] = {
        "model": upstream_model,
        "messages": messages,
        "stream": bool(data.get("stream", False)),
    }
    if isinstance(data.get("max_output_tokens"), int):
        payload["max_tokens"] = data["max_output_tokens"]
    for key in ("temperature", "top_p"):
        if key in data:
            payload[key] = data[key]
    tools = _responses_tools_to_openai(data.get("tools"))
    if tools:
        payload["tools"] = tools
    tool_choice = _responses_tool_choice(data.get("tool_choice"))
    if tool_choice is not None:
        payload["tool_choice"] = tool_choice
    return payload


def responses_to_claude(upstream_model: str, data: dict) -> dict:
    system_text = _responses_instructions_text(data.get("instructions"))
    messages: list[dict] = []
    for item in data.get("input") or []:
        if not isinstance(item, dict):
            continue
        item_type = item.get("type")
        if item_type == "message":
            role = item.get("role") or "user"
            if role in ("system", "developer"):
                role = "user"
            if role not in ("user", "assistant"):
                role = "user"
            content = item.get("content")
            if isinstance(content, str):
                messages.append(
                    {"role": role, "content": [{"type": "text", "text": content}]}
                )
                continue
            if not isinstance(content, list):
                continue
            blocks: list[dict] = []
            for part in content:
                if not isinstance(part, dict):
                    continue
                part_type = part.get("type")
                if part_type == "input_text":
                    blocks.append({"type": "text", "text": part.get("text", "")})
                elif part_type == "input_image":
                    media_type, data_b64 = _parse_data_url(_responses_image_url(part))
                    if data_b64:
                        blocks.append(
                            {
                                "type": "image",
                                "source": {
                                    "type": "base64",
                                    "media_type": media_type,
                                    "data": data_b64,
                                },
                            }
                        )
            if blocks:
                messages.append({"role": role, "content": blocks})
        elif item_type == "function_call":
            messages.append(
                {
                    "role": "assistant",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": item.get("call_id") or _new_id("toolu"),
                            "name": item.get("name", ""),
                            "input": _parse_arguments(item.get("arguments")),
                        }
                    ],
                }
            )
        elif item_type == "function_call_output":
            messages.append(
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": item.get("call_id") or "",
                            "content": _responses_text(item.get("output")) or "",
                        }
                    ],
                }
            )

    payload: dict[str, Any] = {
        "model": upstream_model,
        "messages": messages,
        "stream": bool(data.get("stream", False)),
    }
    if isinstance(data.get("max_output_tokens"), int):
        payload["max_tokens"] = data["max_output_tokens"]
    for key in ("temperature", "top_p"):
        if key in data:
            payload[key] = data[key]
    if system_text:
        payload["system"] = system_text
    tools = _responses_tools_to_claude(data.get("tools"))
    if tools:
        payload["tools"] = tools
    return payload


def responses_to_gemini(upstream_model: str, data: dict) -> dict:
    call_names: dict[str, str] = {}
    for item in data.get("input") or []:
        if isinstance(item, dict) and item.get("type") == "function_call":
            call_names[item.get("call_id") or ""] = item.get("name") or "unknown_tool"

    contents: list[dict] = []
    for item in data.get("input") or []:
        if not isinstance(item, dict):
            continue
        item_type = item.get("type")
        if item_type == "message":
            role = "model" if item.get("role") == "assistant" else "user"
            content = item.get("content")
            parts: list[dict] = []
            if isinstance(content, str):
                parts.append({"text": content})
            elif isinstance(content, list):
                for part in content:
                    if not isinstance(part, dict):
                        continue
                    part_type = part.get("type")
                    if part_type == "input_text":
                        parts.append({"text": part.get("text", "")})
                    elif part_type == "input_image":
                        media_type, data_b64 = _parse_data_url(
                            _responses_image_url(part)
                        )
                        if data_b64:
                            parts.append(
                                {
                                    "inlineData": {
                                        "mimeType": media_type,
                                        "data": data_b64,
                                    }
                                }
                            )
            if parts:
                contents.append({"role": role, "parts": parts})
        elif item_type == "function_call":
            contents.append(
                {
                    "role": "model",
                    "parts": [
                        {
                            "functionCall": {
                                "name": item.get("name", ""),
                                "args": _parse_arguments(item.get("arguments")),
                            }
                        }
                    ],
                }
            )
        elif item_type == "function_call_output":
            contents.append(
                {
                    "role": "user",
                    "parts": [
                        {
                            "functionResponse": {
                                "name": call_names.get(item.get("call_id") or "")
                                or "unknown_tool",
                                "response": {
                                    "result": _responses_text(item.get("output"))
                                    or ""
                                },
                            }
                        }
                    ],
                }
            )

    payload: dict[str, Any] = {"contents": contents}
    instructions = _responses_instructions_text(data.get("instructions"))
    if instructions:
        payload["systemInstruction"] = {"parts": [{"text": instructions}]}
    tools = _responses_tools_to_gemini(data.get("tools"))
    if tools:
        payload["tools"] = tools
    generation_config: dict[str, Any] = {}
    if isinstance(data.get("max_output_tokens"), int):
        generation_config["maxOutputTokens"] = data["max_output_tokens"]
    for key in ("temperature", "topP"):
        if key in data:
            generation_config[key] = data[key]
    if generation_config:
        payload["generationConfig"] = generation_config
    return payload


def responses_to_openai_responses(upstream_model: str, data: dict) -> dict:
    converted = dict(data)
    converted["model"] = upstream_model
    return converted


def convert_codex_request(
    upstream_protocol: str, upstream_model: str, body: bytes
) -> bytes:
    """Convert a Responses-format request body to the upstream protocol."""
    try:
        data = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"Invalid Responses request body: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("Responses request body must be a JSON object")
    if upstream_protocol == "openai_responses":
        converted = responses_to_openai_responses(upstream_model, data)
    elif upstream_protocol == "openai_compatible":
        converted = responses_to_openai_compatible(upstream_model, data)
    elif upstream_protocol == "claude":
        converted = responses_to_claude(upstream_model, data)
    elif upstream_protocol == "gemini":
        converted = responses_to_gemini(upstream_model, data)
    else:
        raise ValueError(f"Unsupported upstream protocol: {upstream_protocol}")
    return json.dumps(converted, ensure_ascii=False).encode("utf-8")


# upstream -> Responses non-streaming response conversion --------------------


def _unix_timestamp() -> int:
    return int(time.time())


def _responses_usage(
    input_tokens: Any,
    output_tokens: Any,
    cached_tokens: Any = None,
    reasoning_tokens: Any = None,
) -> dict:
    input_details = {"cached_tokens": int(cached_tokens or 0)}
    output_details = {"reasoning_tokens": int(reasoning_tokens or 0)}
    return {
        "input_tokens": int(input_tokens or 0),
        "output_tokens": int(output_tokens or 0),
        "total_tokens": int((input_tokens or 0) + (output_tokens or 0)),
        "input_tokens_details": input_details,
        "output_tokens_details": output_details,
    }


def _responses_envelope(
    codex_model: str,
    response_id: str,
    created_at: int,
    status: str,
    output: list,
    usage: dict,
    error: dict | None = None,
) -> dict:
    return {
        "id": response_id,
        "object": "response",
        "created_at": created_at,
        "status": status,
        "error": error,
        "incomplete_details": (
            {"reason": "max_output_tokens"} if status == "incomplete" else None
        ),
        "instructions": None,
        "max_output_tokens": None,
        "model": codex_model,
        "output": output,
        "parallel_tool_calls": True,
        "previous_response_id": None,
        "reasoning": None,
        "store": False,
        "temperature": None,
        "text": None,
        "tool_choice": None,
        "tools": [],
        "top_p": None,
        "truncation": None,
        "usage": usage,
        "user": None,
        "metadata": {},
    }

_DSML_BLOCK_NAMES = ("tool_calls", "function_calls")
_DSML_BAR_VARIANTS = ("|", "｜", "||", "｜｜")
_DSML_MARKERS = tuple(
    (
        name,
        f"<{bars}DSML{bars}{name}>",
        f"</{bars}DSML{bars}{name}>",
    )
    for name in _DSML_BLOCK_NAMES
    for bars in _DSML_BAR_VARIANTS
)
_DSML_START_MARKERS = tuple(marker[1] for marker in _DSML_MARKERS)
_DSML_NOISE_MARKERS = tuple(
    f"<{bars}DSML{bars}memory pass:" for bars in _DSML_BAR_VARIANTS
)
_DSML_TAG_BAR = r"(?:\||｜)+"
_DSML_INVOKE_RE = re.compile(
    rf"<{_DSML_TAG_BAR}DSML{_DSML_TAG_BAR}invoke\s+([^>]*)>(.*?)"
    rf"</{_DSML_TAG_BAR}DSML{_DSML_TAG_BAR}invoke>",
    re.DOTALL,
)
_DSML_MESSAGE_RE = re.compile(
    rf"<{_DSML_TAG_BAR}DSML{_DSML_TAG_BAR}message\s+([^>]*)>(.*?)"
    rf"</{_DSML_TAG_BAR}DSML{_DSML_TAG_BAR}invoke>",
    re.DOTALL,
)
_DSML_PARAMETER_RE = re.compile(
    rf"<{_DSML_TAG_BAR}DSML{_DSML_TAG_BAR}parameter\s+([^>]*)>(.*?)"
    rf"</{_DSML_TAG_BAR}DSML{_DSML_TAG_BAR}parameter>",
    re.DOTALL,
)
_DSML_COMMAND_RE = re.compile(
    rf"<{_DSML_TAG_BAR}DSML{_DSML_TAG_BAR}command\s*>(.*?)"
    rf"</{_DSML_TAG_BAR}DSML{_DSML_TAG_BAR}command>",
    re.DOTALL,
)
_DSML_ATTRIBUTE_RE = re.compile(
    r'''([A-Za-z_][\w-]*)=(?:"([^"]*)"|'([^']*)'|([^\s>]+))'''
)


def _dsml_attributes(source: str) -> dict[str, str]:
    attributes: dict[str, str] = {}
    for name, double_quoted, single_quoted, bare in _DSML_ATTRIBUTE_RE.findall(source):
        attributes[name] = html.unescape(double_quoted or single_quoted or bare)
    return attributes


def _dsml_arguments(body: str) -> dict[str, Any]:
    arguments: dict[str, Any] = {}
    for parameter_match in _DSML_PARAMETER_RE.finditer(body):
        parameter_attrs = _dsml_attributes(parameter_match.group(1))
        parameter_name = parameter_attrs.get("name")
        if not parameter_name:
            continue
        raw_value = html.unescape(parameter_match.group(2).strip())
        if parameter_attrs.get("string", "false").lower() == "true":
            value: Any = raw_value
        else:
            try:
                value = json.loads(raw_value)
            except json.JSONDecodeError:
                value = raw_value
        arguments[parameter_name] = value
    command_match = _DSML_COMMAND_RE.search(body)
    if command_match:
        arguments["cmd"] = html.unescape(command_match.group(1).strip())
    return arguments


def _parse_dsml_invocations(block: str) -> list[dict[str, str]]:
    matches: list[tuple[int, str, dict[str, Any]]] = []
    for invoke_match in _DSML_INVOKE_RE.finditer(block):
        invoke_attrs = _dsml_attributes(invoke_match.group(1))
        name = invoke_attrs.get("name")
        if name:
            matches.append(
                (invoke_match.start(), name, _dsml_arguments(invoke_match.group(2)))
            )
    for message_match in _DSML_MESSAGE_RE.finditer(block):
        message_attrs = _dsml_attributes(message_match.group(1))
        name = message_attrs.get("to") or message_attrs.get("name")
        if name:
            matches.append(
                (message_match.start(), name, _dsml_arguments(message_match.group(2)))
            )
    matches.sort(key=lambda item: item[0])
    return [
        {
            "name": name,
            "arguments": json.dumps(arguments, ensure_ascii=False),
        }
        for _start, name, arguments in matches
    ]




def _find_dsml_start(text: str) -> tuple[int, str, str] | None:
    found: list[tuple[int, str, str]] = []
    for _name, marker, end_marker in _DSML_MARKERS:
        index = text.find(marker)
        if index >= 0:
            found.append((index, marker, end_marker))
    return min(found, key=lambda item: item[0]) if found else None

def _find_dsml_noise_start(text: str) -> int | None:
    found = [text.find(marker) for marker in _DSML_NOISE_MARKERS]
    positions = [index for index in found if index >= 0]
    return min(positions) if positions else None


def _dsml_partial_prefix_length(text: str) -> int:
    keep = 0
    for marker in (*_DSML_START_MARKERS, *_DSML_NOISE_MARKERS):
        limit = min(len(text), len(marker) - 1)
        for length in range(1, limit + 1):
            if text.endswith(marker[:length]):
                keep = max(keep, length)
    return keep


def _split_dsml_content(text: str) -> list[tuple[str, Any]]:
    segments: list[tuple[str, Any]] = []
    remaining = text
    while remaining:
        noise_index = _find_dsml_noise_start(remaining)
        start = _find_dsml_start(remaining)
        if noise_index is not None and (start is None or noise_index < start[0]):
            if noise_index:
                segments.append(("text", remaining[:noise_index]))
            line_end = remaining.find("\n", noise_index)
            if line_end < 0:
                break
            remaining = remaining[line_end + 1 :]
            continue
        if start is None:
            segments.append(("text", remaining))
            break
        start_index, start_marker, end_marker = start
        end_index = remaining.find(end_marker, start_index + len(start_marker))
        if end_index < 0:
            segments.append(("text", remaining))
            break
        if start_index:
            segments.append(("text", remaining[:start_index]))
        block_start = start_index + len(start_marker)
        block = remaining[block_start:end_index]
        calls = _parse_dsml_invocations(block)
        if calls:
            segments.extend(("function_call", call) for call in calls)
        else:
            segments.append(
                ("text", remaining[start_index : end_index + len(end_marker)])
            )
        remaining = remaining[end_index + len(end_marker) :]
    return segments


def openai_compatible_to_responses(codex_model: str, data: dict) -> dict:
    choices = data.get("choices") or []
    choice = choices[0] if choices else {}
    message = choice.get("message") or {}
    text = _chat_message_text(message.get("content"))
    output: list[dict] = []
    for segment_type, segment in _split_dsml_content(text):
        if segment_type == "text" and segment:
            output.append(
                {
                    "type": "message",
                    "id": _new_id("msg"),
                    "status": "completed",
                    "role": "assistant",
                    "content": [
                        {"type": "output_text", "text": segment, "annotations": []}
                    ],
                }
            )
        elif segment_type == "function_call":
            output.append(
                {
                    "type": "function_call",
                    "id": _new_id("fc"),
                    "call_id": _new_id("call"),
                    "name": segment["name"],
                    "arguments": segment["arguments"],
                    "status": "completed",
                }
            )
    for tool_call in message.get("tool_calls") or []:
        if not isinstance(tool_call, dict):
            continue
        function = tool_call.get("function") or {}
        output.append(
            {
                "type": "function_call",
                "id": _new_id("fc"),
                "call_id": tool_call.get("id") or _new_id("call"),
                "name": function.get("name", ""),
                "arguments": function.get("arguments") or "{}",
                "status": "completed",
            }
        )
    usage = data.get("usage") or {}
    prompt_details = usage.get("prompt_tokens_details") or {}
    output_details = usage.get("completion_tokens_details") or {}
    status = "incomplete" if choice.get("finish_reason") == "length" else "completed"
    return _responses_envelope(
        codex_model,
        _new_id("resp"),
        _unix_timestamp(),
        status,
        output,
        _responses_usage(
            usage.get("prompt_tokens", usage.get("input_tokens")),
            usage.get("completion_tokens", usage.get("output_tokens")),
            prompt_details.get("cached_tokens")
            or prompt_details.get("cache_read_input_tokens")
            or prompt_details.get("prompt_cache_hit_tokens")
            or usage.get("prompt_cache_hit_tokens"),
            output_details.get("reasoning_tokens")
            or usage.get("reasoning_tokens"),
        ),
    )


def claude_to_responses(codex_model: str, data: dict) -> dict:
    output: list[dict] = []
    for block in data.get("content") or []:
        if not isinstance(block, dict):
            continue
        block_type = block.get("type")
        if block_type == "text":
            output.append(
                {
                    "type": "message",
                    "id": _new_id("msg"),
                    "status": "completed",
                    "role": "assistant",
                    "content": [
                        {
                            "type": "output_text",
                            "text": block.get("text", ""),
                            "annotations": [],
                        }
                    ],
                }
            )
        elif block_type == "tool_use":
            output.append(
                {
                    "type": "function_call",
                    "id": _new_id("fc"),
                    "call_id": block.get("id") or _new_id("call"),
                    "name": block.get("name", ""),
                    "arguments": json.dumps(
                        block.get("input") or {}, ensure_ascii=False
                    ),
                    "status": "completed",
                }
            )
    usage = data.get("usage") or {}
    status = "incomplete" if data.get("stop_reason") == "max_tokens" else "completed"
    return _responses_envelope(
        codex_model,
        _new_id("resp"),
        _unix_timestamp(),
        status,
        output,
        _responses_usage(
            usage.get("input_tokens"),
            usage.get("output_tokens"),
            usage.get("cache_read_input_tokens"),
        ),
    )


def gemini_to_responses(codex_model: str, data: dict) -> dict:
    candidates = data.get("candidates") or []
    candidate = candidates[0] if candidates else {}
    parts = ((candidate.get("content") or {}).get("parts")) or []
    output: list[dict] = []
    for part in parts:
        if not isinstance(part, dict):
            continue
        if "text" in part:
            output.append(
                {
                    "type": "message",
                    "id": _new_id("msg"),
                    "status": "completed",
                    "role": "assistant",
                    "content": [
                        {
                            "type": "output_text",
                            "text": part["text"],
                            "annotations": [],
                        }
                    ],
                }
            )
        if isinstance(part.get("functionCall"), dict):
            function_call = part["functionCall"]
            output.append(
                {
                    "type": "function_call",
                    "id": _new_id("fc"),
                    "call_id": _new_id("call"),
                    "name": function_call.get("name", ""),
                    "arguments": json.dumps(
                        function_call.get("args") or {}, ensure_ascii=False
                    ),
                    "status": "completed",
                }
            )
    usage = data.get("usageMetadata") or {}
    status = (
        "incomplete"
        if (candidate.get("finishReason") or "STOP") == "MAX_TOKENS"
        else "completed"
    )
    return _responses_envelope(
        codex_model,
        _new_id("resp"),
        _unix_timestamp(),
        status,
        output,
        _responses_usage(
            usage.get("promptTokenCount"),
            usage.get("candidatesTokenCount"),
            usage.get("cachedContentTokenCount"),
        ),
    )


def openai_responses_to_responses(codex_model: str, data: dict) -> dict:
    converted = dict(data)
    converted["model"] = codex_model
    return converted


def convert_codex_response(
    upstream_protocol: str, codex_model: str, body: bytes
) -> bytes:
    """Convert a non-streaming upstream response body to Responses format."""
    if upstream_protocol == "openai_responses":
        return body
    try:
        data = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"Invalid upstream response body: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("Upstream response body must be a JSON object")
    if upstream_protocol == "openai_compatible":
        converted = openai_compatible_to_responses(codex_model, data)
    elif upstream_protocol == "claude":
        converted = claude_to_responses(codex_model, data)
    elif upstream_protocol == "gemini":
        converted = gemini_to_responses(codex_model, data)
    else:
        raise ValueError(f"Unsupported upstream protocol: {upstream_protocol}")
    return json.dumps(converted, ensure_ascii=False).encode("utf-8")


# upstream SSE stream -> Responses SSE events --------------------------------


class ResponsesSSEConverter:
    """Stateful converter turning an upstream SSE stream into Responses SSE events.

    Subclasses parse their upstream event stream and drive this base via the
    ``_append_text_delta`` / ``_open_function_item`` helpers; ``flush()``
    closes any open items and emits ``response.completed``.
    """

    def __init__(self, upstream_protocol: str, codex_model: str) -> None:
        self.upstream_protocol = upstream_protocol
        self.codex_model = codex_model
        self.response_id = _new_id("resp")
        self.request_id = _new_id("req")
        self.created_at = _unix_timestamp()
        self.output: list[dict[str, Any]] = []
        self._started = False
        self._finished = False
        self._usage: dict[str, Any] = {}
        self._next_output_index = 0
        self._text_item_id: str | None = None
        self._text_index: int | None = None
        self._text_content: list[str] = []
        self._functions: dict[Any, dict[str, Any]] = {}
        self._next_sequence_number = 0

    def _responses_event(self, event: str, payload: dict[str, Any]) -> bytes:
        enriched = dict(payload)
        enriched["sequence_number"] = self._next_sequence_number
        self._next_sequence_number += 1
        return _sse(event, enriched)

    # -- output item lifecycle ---------------------------------------------

    def _envelope(self, status: str, error: dict | None = None) -> dict:
        return _responses_envelope(
            self.codex_model,
            self.response_id,
            self.created_at,
            status,
            list(self.output),
            self._merged_usage(),
            error,
        )

    def _merged_usage(self) -> dict:
        return _responses_usage(
            self._usage.get("input_tokens"),
            self._usage.get("output_tokens"),
            self._usage.get("cache_read_input_tokens"),
            self._usage.get("reasoning_tokens"),
        )

    def _merge_usage(self, usage: dict) -> None:
        if not isinstance(usage, dict):
            return
        for key in ("input_tokens", "output_tokens", "cache_read_input_tokens", "reasoning_tokens"):
            if key in usage and isinstance(usage[key], int):
                self._usage[key] = usage[key]

    def _ensure_start(self) -> bytes:
        if self._started:
            return b""
        self._started = True
        return self._responses_event(
            "response.created",
            {"type": "response.created", "response": self._envelope("in_progress")},
        )

    def _open_text_item(self) -> bytes:
        if self._text_item_id is not None:
            return b""
        index = self._next_output_index
        self._next_output_index += 1
        item_id = _new_id("msg")
        self._text_item_id = item_id
        self._text_index = index
        self._text_content = []
        item = {
            "type": "message",
            "id": item_id,
            "status": "in_progress",
            "role": "assistant",
            "content": [],
        }
        output = bytearray()
        output += self._responses_event(
            "response.output_item.added",
            {"type": "response.output_item.added", "output_index": index, "item": item},
        )
        output += self._responses_event(
            "response.content_part.added",
            {
                "type": "response.content_part.added",
                "item_id": item_id,
                "output_index": index,
                "content_index": 0,
                "part": {"type": "output_text", "text": "", "annotations": []},
            },
        )
        return bytes(output)

    def _append_text_delta(self, delta_text: str) -> bytes:
        if not delta_text:
            return b""
        output = bytearray()
        output += self._ensure_start()
        if self._text_item_id is None:
            output += self._open_text_item()
        output += self._responses_event(
            "response.output_text.delta",
            {
                "type": "response.output_text.delta",
                "item_id": self._text_item_id,
                "output_index": self._text_index,
                "content_index": 0,
                "delta": delta_text,
            },
        )
        self._text_content.append(delta_text)
        return bytes(output)

    def _close_text_item(self) -> bytes:
        if self._text_item_id is None:
            return b""
        text = "".join(self._text_content)
        item = {
            "type": "message",
            "id": self._text_item_id,
            "status": "completed",
            "role": "assistant",
            "content": [{"type": "output_text", "text": text, "annotations": []}],
        }
        output = bytearray()
        output += self._responses_event(
            "response.output_text.done",
            {
                "type": "response.output_text.done",
                "item_id": self._text_item_id,
                "output_index": self._text_index,
                "content_index": 0,
                "text": text,
            },
        )
        output += self._responses_event(
            "response.content_part.done",
            {
                "type": "response.content_part.done",
                "item_id": self._text_item_id,
                "output_index": self._text_index,
                "content_index": 0,
                "part": {"type": "output_text", "text": text, "annotations": []},
            },
        )
        output += self._responses_event(
            "response.output_item.done",
            {
                "type": "response.output_item.done",
                "output_index": self._text_index,
                "item": item,
            },
        )
        self.output.append(item)
        self._text_item_id = None
        self._text_index = None
        self._text_content = []
        return bytes(output)

    def _open_function_item(self, key: Any, call_id: str, name: str) -> bytes:
        if key in self._functions:
            return b""
        index = self._next_output_index
        self._next_output_index += 1
        item_id = _new_id("fc")
        self._functions[key] = {
            "item_id": item_id,
            "output_index": index,
            "call_id": call_id,
            "name": name,
            "arguments": [],
        }
        item = {
            "type": "function_call",
            "id": item_id,
            "call_id": call_id,
            "name": name,
            "arguments": "",
            "status": "in_progress",
        }
        return self._responses_event(
            "response.output_item.added",
            {"type": "response.output_item.added", "output_index": index, "item": item},
        )

    def _append_function_arguments(self, key: Any, delta: str) -> bytes:
        state = self._functions.get(key)
        if state is None or not delta:
            return b""
        state["arguments"].append(delta)
        return self._responses_event(
            "response.function_call_arguments.delta",
            {
                "type": "response.function_call_arguments.delta",
                "item_id": state["item_id"],
                "output_index": state["output_index"],
                "delta": delta,
            },
        )

    def _close_function_item(self, key: Any) -> bytes:
        state = self._functions.get(key)
        if state is None:
            return b""
        arguments = "".join(state["arguments"])
        item = {
            "type": "function_call",
            "id": state["item_id"],
            "call_id": state["call_id"],
            "name": state["name"],
            "arguments": arguments,
            "status": "completed",
        }
        output = bytearray()
        output += self._responses_event(
            "response.function_call_arguments.done",
            {
                "type": "response.function_call_arguments.done",
                "item_id": state["item_id"],
                "output_index": state["output_index"],
                "arguments": arguments,
            },
        )
        output += self._responses_event(
            "response.output_item.done",
            {
                "type": "response.output_item.done",
                "output_index": state["output_index"],
                "item": item,
            },
        )
        self.output.append(item)
        del self._functions[key]
        return bytes(output)

    def _finish(self, status: str = "completed") -> bytes:
        if self._finished:
            return b""
        self._finished = True
        output = bytearray()
        output += self._ensure_start()
        if self._text_item_id is not None:
            output += self._close_text_item()
        for key in list(self._functions):
            output += self._close_function_item(key)
        output += self._responses_event(
            "response.completed",
            {"type": "response.completed", "response": self._envelope(status)},
        )
        return bytes(output)

    def error_event(self, message: str) -> bytes:
        if self._finished:
            return b""
        self._finished = True
        output = bytearray()
        output += self._ensure_start()
        if self._text_item_id is not None:
            output += self._close_text_item()
        for key in list(self._functions):
            output += self._close_function_item(key)
        output += self._responses_event(
            "response.failed",
            {
                "type": "response.failed",
                "response": self._envelope(
                    "failed", error={"code": "gateway_error", "message": message}
                ),
            },
        )
        return bytes(output)

    def _iter_events(self, chunk: bytes, buffer: bytearray):
        """Parse SSE ``data:`` blocks from an upstream chunk into JSON events."""
        # 兼容 \r\n 行尾的上游（部分代理/网关），否则分隔符永远匹配不到。
        buffer.extend(chunk.replace(b"\r\n", b"\n"))
        events = []
        while True:
            marker = buffer.find(b"\n\n")
            if marker == -1:
                break
            block = bytes(buffer[:marker]).strip()
            del buffer[: marker + 2]
            for line in block.splitlines():
                if not line.startswith(b"data:"):
                    continue
                data = line[5:].strip()
                if data == b"[DONE]":
                    events.append("__DONE__")
                    continue
                try:
                    events.append(json.loads(data))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
        return events

    def feed(self, chunk: bytes) -> bytes:
        raise NotImplementedError

    def flush(self) -> bytes:
        raise NotImplementedError


class PassthroughStreamToResponses(ResponsesSSEConverter):
    """Responses -> Responses: forward the upstream stream unchanged."""

    def __init__(self, codex_model: str) -> None:
        super().__init__("openai_responses", codex_model)

    def feed(self, chunk: bytes) -> bytes:
        return chunk

    def flush(self) -> bytes:
        return b""

    def error_event(self, message: str) -> bytes:
        output = bytearray()
        output += self._ensure_start()
        output += self._responses_event(
            "response.failed",
            {
                "type": "response.failed",
                "response": self._envelope(
                    "failed", error={"code": "gateway_error", "message": message}
                ),
            },
        )
        return bytes(output)


class OpenAICompatibleStreamToResponses(ResponsesSSEConverter):
    def __init__(self, codex_model: str) -> None:
        super().__init__("openai_compatible", codex_model)
        self._buffer = bytearray()
        self._tool_key_index = 0
        self._tool_by_id: dict[str, str] = {}
        self._tool_by_index: dict[int, str] = {}
        self._status = "completed"
        self._dsml_text_buffer = ""
        self._dsml_start_marker: str | None = None
        self._dsml_end_marker: str | None = None
        self._dsml_tool_index = 0

    def feed(self, chunk: bytes) -> bytes:
        if self._finished:
            return b""
        output = bytearray()
        for event in self._iter_events(chunk, self._buffer):
            if event == "__DONE__":
                output += self._drain_dsml_text(final=True)
                output += self._finish(self._status)
                continue
            if isinstance(event, dict):
                output += self._consume(event)
        return bytes(output)

    def flush(self) -> bytes:
        if self._finished:
            return b""
        output = bytearray(self._drain_dsml_text(final=True))
        output += self._finish(self._status)
        return bytes(output)

    def error_event(self, message: str) -> bytes:
        if self._finished:
            return b""
        output = bytearray(self._drain_dsml_text(final=True))
        output += super().error_event(message)
        return bytes(output)

    def _emit_dsml_calls(self, calls: list[dict[str, str]]) -> bytes:
        output = bytearray(self._close_text_item())
        for call in calls:
            key = ("dsml", self._dsml_tool_index)
            self._dsml_tool_index += 1
            output += self._open_function_item(
                key,
                _new_id("call"),
                call["name"],
            )
            output += self._append_function_arguments(key, call["arguments"])
            output += self._close_function_item(key)
        return bytes(output)

    def _drain_dsml_text(self, text: str = "", final: bool = False) -> bytes:
        self._dsml_text_buffer += text
        output = bytearray()
        while self._dsml_text_buffer:
            if self._dsml_end_marker is not None:
                end_index = self._dsml_text_buffer.find(self._dsml_end_marker)
                if end_index < 0:
                    if final:
                        raw = (self._dsml_start_marker or "") + self._dsml_text_buffer
                        output += self._append_text_delta(raw)
                        self._dsml_text_buffer = ""
                        self._dsml_start_marker = None
                        self._dsml_end_marker = None
                    break

                block = self._dsml_text_buffer[:end_index]
                raw = (
                    (self._dsml_start_marker or "")
                    + block
                    + self._dsml_end_marker
                )
                self._dsml_text_buffer = self._dsml_text_buffer[
                    end_index + len(self._dsml_end_marker) :
                ]
                self._dsml_start_marker = None
                self._dsml_end_marker = None
                calls = _parse_dsml_invocations(block)
                if calls:
                    output += self._emit_dsml_calls(calls)
                else:
                    output += self._append_text_delta(raw)
                continue

            noise_index = _find_dsml_noise_start(self._dsml_text_buffer)
            if noise_index is not None:
                if noise_index:
                    output += self._append_text_delta(
                        self._dsml_text_buffer[:noise_index]
                    )
                line_end = self._dsml_text_buffer.find("\n", noise_index)
                if line_end < 0:
                    if final:
                        self._dsml_text_buffer = ""
                    else:
                        self._dsml_text_buffer = self._dsml_text_buffer[noise_index:]
                    break
                self._dsml_text_buffer = self._dsml_text_buffer[line_end + 1 :]
                continue
            start = _find_dsml_start(self._dsml_text_buffer)
            if start is not None:
                start_index, start_marker, end_marker = start
                if start_index:
                    output += self._append_text_delta(
                        self._dsml_text_buffer[:start_index]
                    )
                self._dsml_text_buffer = self._dsml_text_buffer[
                    start_index + len(start_marker) :
                ]
                self._dsml_start_marker = start_marker
                self._dsml_end_marker = end_marker
                continue

            if final:
                output += self._append_text_delta(self._dsml_text_buffer)
                self._dsml_text_buffer = ""
                break

            keep = _dsml_partial_prefix_length(self._dsml_text_buffer)
            safe_length = len(self._dsml_text_buffer) - keep
            if safe_length:
                output += self._append_text_delta(
                    self._dsml_text_buffer[:safe_length]
                )
                self._dsml_text_buffer = self._dsml_text_buffer[safe_length:]
            break
        return bytes(output)

    def _consume(self, event: dict) -> bytes:
        output = bytearray()
        output += self._ensure_start()
        if isinstance(event.get("usage"), dict):
            self._merge_openai_usage(event["usage"])
        choices = event.get("choices") or []
        if not choices:
            return bytes(output)
        choice = choices[0]
        delta = choice.get("delta") or {}

        text = _chat_message_text(delta.get("content"))
        if text:
            output += self._drain_dsml_text(text)

        for tool_call in delta.get("tool_calls") or []:
            if not isinstance(tool_call, dict):
                continue
            function = tool_call.get("function") or {}
            tool_id = tool_call.get("id")
            upstream_index = tool_call.get("index")
            # OpenAI/DeepSeek 流式约定：首个 chunk 携带 id + name，后续 chunk
            # 只有 index + arguments 增量。按 id 与 index 双映射合并到同一个
            # function_call item，否则同一调用会被拆成多个空名 item。
            key = None
            if tool_id and tool_id in self._tool_by_id:
                key = self._tool_by_id[tool_id]
            elif isinstance(upstream_index, int) and upstream_index in self._tool_by_index:
                key = self._tool_by_index[upstream_index]
            if key is None:
                key = "tool_%d" % self._tool_key_index
                self._tool_key_index += 1
                if tool_id:
                    self._tool_by_id[tool_id] = key
                if isinstance(upstream_index, int):
                    self._tool_by_index[upstream_index] = key
                output += self._open_function_item(
                    key,
                    tool_id or _new_id("call"),
                    function.get("name") or "",
                )
            if function.get("arguments"):
                output += self._append_function_arguments(key, function["arguments"])

        finish_reason = choice.get("finish_reason")
        if finish_reason == "length":
            self._status = "incomplete"
        return bytes(output)

    def _merge_openai_usage(self, usage: dict) -> None:
        self._usage["input_tokens"] = usage.get("prompt_tokens") or 0
        self._usage["output_tokens"] = usage.get("completion_tokens") or 0
        prompt_details = usage.get("prompt_tokens_details") or {}
        if isinstance(prompt_details, dict):
            cache_read = (
                prompt_details.get("cached_tokens")
                or prompt_details.get("prompt_cache_hit_tokens")
                or usage.get("prompt_cache_hit_tokens")
                or 0
            )
            if cache_read:
                self._usage["cache_read_input_tokens"] = cache_read
        completion_details = usage.get("completion_tokens_details") or {}
        if isinstance(completion_details, dict):
            reasoning = (
                completion_details.get("reasoning_tokens")
                or usage.get("reasoning_tokens")
                or 0
            )
            if reasoning:
                self._usage["reasoning_tokens"] = reasoning


class ClaudeStreamToResponses(ResponsesSSEConverter):
    def __init__(self, codex_model: str) -> None:
        super().__init__("claude", codex_model)
        self._buffer = bytearray()
        self._open_block_types: dict[int, str] = {}
        self._function_key_by_block: dict[int, Any] = {}
        self._status = "completed"

    def feed(self, chunk: bytes) -> bytes:
        if self._finished:
            return b""
        output = bytearray()
        for event in self._iter_events(chunk, self._buffer):
            if event == "__DONE__":
                output += self._finish(self._status)
                continue
            if isinstance(event, dict):
                output += self._consume(event)
        return bytes(output)

    def flush(self) -> bytes:
        if self._finished:
            return b""
        return self._finish(self._status)

    def _consume(self, event: dict) -> bytes:
        output = bytearray()
        output += self._ensure_start()
        event_type = event.get("type") or ""
        if event_type == "message_start":
            self._merge_usage((event.get("message") or {}).get("usage") or {})
        elif event_type == "content_block_start":
            index = event.get("index") or 0
            block = event.get("content_block") or {}
            block_type = block.get("type")
            self._open_block_types[index] = block_type
            if block_type == "tool_use":
                key = ("block", index)
                self._function_key_by_block[index] = key
                output += self._open_function_item(
                    key,
                    block.get("id") or _new_id("call"),
                    block.get("name") or "",
                )
        elif event_type == "content_block_delta":
            index = event.get("index") or 0
            delta = event.get("delta") or {}
            delta_type = delta.get("type")
            if delta_type == "text_delta":
                output += self._append_text_delta(delta.get("text") or "")
            elif delta_type == "input_json_delta":
                key = self._function_key_by_block.get(index)
                if key is not None:
                    output += self._append_function_arguments(
                        key, delta.get("partial_json") or ""
                    )
        elif event_type == "content_block_stop":
            index = event.get("index") or 0
            block_type = self._open_block_types.get(index)
            if block_type == "text":
                output += self._close_text_item()
            elif block_type == "tool_use":
                key = self._function_key_by_block.get(index)
                if key is not None:
                    output += self._close_function_item(key)
                    self._function_key_by_block.pop(index, None)
            self._open_block_types.pop(index, None)
        elif event_type == "message_delta":
            delta = event.get("delta") or {}
            if delta.get("stop_reason") == "max_tokens":
                self._status = "incomplete"
            self._merge_usage(event.get("usage") or {})
        return bytes(output)


class GeminiStreamToResponses(ResponsesSSEConverter):
    def __init__(self, codex_model: str) -> None:
        super().__init__("gemini", codex_model)
        self._buffer = bytearray()
        self._status = "completed"

    def _iter_events(self, chunk: bytes, buffer: bytearray):
        # Gemini streams are newline-separated JSON objects; some gateways wrap
        # them in SSE ``data:`` frames, so both are accepted here.
        buffer.extend(chunk.replace(b"\r\n", b"\n"))
        events = []
        while True:
            marker = buffer.find(b"\n")
            if marker == -1:
                break
            line = bytes(buffer[:marker]).strip()
            del buffer[: marker + 1]
            if not line:
                continue
            if line.startswith(b"data:"):
                line = line[5:].strip()
            if line == b"[DONE]":
                events.append("__DONE__")
                continue
            try:
                events.append(json.loads(line))
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
        return events

    def feed(self, chunk: bytes) -> bytes:
        if self._finished:
            return b""
        output = bytearray()
        for event in self._iter_events(chunk, self._buffer):
            if event == "__DONE__":
                output += self._finish(self._status)
                continue
            if isinstance(event, dict):
                output += self._consume(event)
        return bytes(output)

    def flush(self) -> bytes:
        if self._finished:
            return b""
        return self._finish(self._status)

    def _consume(self, event: dict) -> bytes:
        output = bytearray()
        output += self._ensure_start()
        candidates = event.get("candidates") or []
        if not candidates:
            self._merge_gemini_usage(event.get("usageMetadata") or {})
            return bytes(output)
        candidate = candidates[0]
        parts = ((candidate.get("content") or {}).get("parts")) or []
        for part in parts:
            if not isinstance(part, dict):
                continue
            if "text" in part:
                output += self._append_text_delta(part["text"])
            if isinstance(part.get("functionCall"), dict):
                function_call = part["functionCall"]
                key = _new_id("fc")
                output += self._open_function_item(
                    key,
                    _new_id("call"),
                    function_call.get("name") or "",
                )
                arguments = json.dumps(
                    function_call.get("args") or {}, ensure_ascii=False
                )
                if arguments:
                    output += self._append_function_arguments(key, arguments)
                output += self._close_function_item(key)
        self._merge_gemini_usage(event.get("usageMetadata") or {})
        if (candidate.get("finishReason") or "") == "MAX_TOKENS":
            self._status = "incomplete"
        return bytes(output)

    def _merge_gemini_usage(self, usage: dict) -> None:
        if not isinstance(usage, dict):
            return
        if isinstance(usage.get("promptTokenCount"), int):
            self._usage["input_tokens"] = usage["promptTokenCount"]
        if isinstance(usage.get("candidatesTokenCount"), int):
            self._usage["output_tokens"] = usage["candidatesTokenCount"]
        if isinstance(usage.get("cachedContentTokenCount"), int):
            self._usage["cache_read_input_tokens"] = usage["cachedContentTokenCount"]


def get_codex_streaming_converter(
    upstream_protocol: str, codex_model: str
) -> ResponsesSSEConverter:
    if upstream_protocol == "openai_responses":
        return PassthroughStreamToResponses(codex_model)
    if upstream_protocol == "openai_compatible":
        return OpenAICompatibleStreamToResponses(codex_model)
    if upstream_protocol == "claude":
        return ClaudeStreamToResponses(codex_model)
    if upstream_protocol == "gemini":
        return GeminiStreamToResponses(codex_model)
    raise ValueError(f"Unsupported upstream protocol: {upstream_protocol}")


# upstream error -> Responses error conversion -------------------------------


def convert_codex_error_response(upstream_protocol: str, body: bytes) -> bytes:
    """Convert a non-2xx upstream error body into Responses error format."""
    if upstream_protocol == "openai_responses":
        return body
    message = "Upstream request failed."
    error_type = "upstream_error"
    try:
        data = json.loads(body)
        error = data.get("error") if isinstance(data, dict) else None
        if isinstance(error, dict):
            message = error.get("message") or message
            code = error.get("type") or error.get("code") or error.get("status")
            if code:
                error_type = str(code).lower().replace(" ", "_")
    except (json.JSONDecodeError, UnicodeDecodeError):
        message = body.decode("utf-8", errors="ignore").strip() or message
    payload = {
        "error": {
            "message": message,
            "type": error_type,
            "code": error_type,
            "param": None,
        }
    }
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


# entry-protocol dispatch ----------------------------------------------------


def convert_mapped_request(
    entry_protocol: str, upstream_protocol: str, upstream_model: str, body: bytes
) -> bytes:
    """Convert a client entry request body to the upstream protocol."""
    if entry_protocol == "openai_responses":
        return convert_codex_request(upstream_protocol, upstream_model, body)
    if entry_protocol == "claude":
        return convert_request(upstream_protocol, upstream_model, body)
    raise ValueError(f"Unsupported entry protocol: {entry_protocol}")


def convert_mapped_response(
    entry_protocol: str, upstream_protocol: str, mapped_model: str, body: bytes
) -> bytes:
    """Convert a non-streaming upstream response body to the entry format."""
    if entry_protocol == "openai_responses":
        return convert_codex_response(upstream_protocol, mapped_model, body)
    if entry_protocol == "claude":
        return convert_response(upstream_protocol, mapped_model, body)
    raise ValueError(f"Unsupported entry protocol: {entry_protocol}")


def get_mapped_streaming_converter(
    entry_protocol: str, upstream_protocol: str, mapped_model: str
):
    """Return the streaming converter for a mapped entry request."""
    if entry_protocol == "openai_responses":
        return get_codex_streaming_converter(upstream_protocol, mapped_model)
    if entry_protocol == "claude":
        return get_streaming_converter(upstream_protocol, mapped_model)
    raise ValueError(f"Unsupported entry protocol: {entry_protocol}")


def convert_mapped_error_response(
    entry_protocol: str, upstream_protocol: str, body: bytes
) -> bytes:
    """Convert a non-2xx upstream error body to the entry error format."""
    if entry_protocol == "openai_responses":
        return convert_codex_error_response(upstream_protocol, body)
    if entry_protocol == "claude":
        return convert_error_response(upstream_protocol, body)
    raise ValueError(f"Unsupported entry protocol: {entry_protocol}")
