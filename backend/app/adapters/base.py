import json
import time
import zlib
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import brotli
import httpx
import ijson
import zstandard


PROTOCOLS = {
    "openai_compatible",
    "openai_responses",
    "claude",
    "gemini",
}
PROTOCOL_ORDER = (
    "openai_compatible",
    "openai_responses",
    "claude",
    "gemini",
)
PROTOCOL_ENDPOINTS = {
    "openai_compatible": (
        "/v1/chat/completions",
    ),
    "openai_responses": ("/v1/responses",),
    "claude": ("/v1/messages",),
    "gemini": (
        "/v1beta/models/{model}:generateContent",
        "/v1beta/models/{model}:streamGenerateContent",
    ),
}

STREAM_ERROR_STATUS_ALIASES = {
    "INVALID_ARGUMENT": 400,
    "BAD_REQUEST": 400,
    "UNAUTHENTICATED": 401,
    "AUTHENTICATION_ERROR": 401,
    "PERMISSION_DENIED": 403,
    "FORBIDDEN": 403,
    "NOT_FOUND": 404,
    "REQUEST_TIMEOUT": 408,
    "TIMEOUT": 408,
    "RATE_LIMITED": 429,
    "RATE_LIMIT_EXCEEDED": 429,
    "RESOURCE_EXHAUSTED": 429,
    "SERVER_ERROR": 503,
    "INTERNAL_ERROR": 503,
    "OVERLOADED_ERROR": 503,
    "UNAVAILABLE": 503,
    "SERVICE_UNAVAILABLE": 503,
    "DEADLINE_EXCEEDED": 504,
}

# These protocols use the same upstream model catalog. A model discovered through
# one member can be reused immediately when another member is enabled.
SHARED_DISCOVERY_PROTOCOL_GROUPS = (
    frozenset({"openai_compatible", "openai_responses"}),
)

HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
}


@dataclass
class Usage:
    input_tokens: int | None = None
    cache_read_tokens: int | None = None
    cache_write_tokens: int | None = None
    cache_miss_input_tokens: int | None = None
    output_tokens: int | None = None
    raw: dict[str, Any] | None = None


class _IdentityDecoder:
    def decode(self, data: bytes) -> bytes:
        return data

    def flush(self) -> bytes:
        return b""


class _ZlibDecoder:
    def __init__(self, wbits: int, allow_raw_fallback: bool = False) -> None:
        self.wbits = wbits
        self.allow_raw_fallback = allow_raw_fallback
        self.first_chunk = True
        self.decompressor = zlib.decompressobj(wbits)

    def decode(self, data: bytes) -> bytes:
        first_chunk = self.first_chunk
        self.first_chunk = False
        try:
            return self.decompressor.decompress(data)
        except zlib.error:
            if not first_chunk or not self.allow_raw_fallback:
                raise
            self.decompressor = zlib.decompressobj(-zlib.MAX_WBITS)
            return self.decompressor.decompress(data)

    def flush(self) -> bytes:
        return self.decompressor.flush()


class _BrotliDecoder:
    def __init__(self) -> None:
        self.decompressor = brotli.Decompressor()

    def decode(self, data: bytes) -> bytes:
        return self.decompressor.process(data) if data else b""

    def flush(self) -> bytes:
        return b""


class _ZstandardDecoder:
    def __init__(self) -> None:
        self.decompressor = zstandard.ZstdDecompressor().decompressobj()

    def decode(self, data: bytes) -> bytes:
        return self.decompressor.decompress(data)

    def flush(self) -> bytes:
        return self.decompressor.flush()


class _MultiDecoder:
    def __init__(self, decoders: list[Any]) -> None:
        self.decoders = list(reversed(decoders))

    def decode(self, data: bytes) -> bytes:
        for decoder in self.decoders:
            data = decoder.decode(data)
        return data

    def flush(self) -> bytes:
        data = b""
        for decoder in self.decoders:
            data = decoder.decode(data) + decoder.flush()
        return data


def _content_decoder(content_encoding: str | None):
    factories = {
        "identity": _IdentityDecoder,
        "gzip": lambda: _ZlibDecoder(zlib.MAX_WBITS | 16),
        "deflate": lambda: _ZlibDecoder(zlib.MAX_WBITS, allow_raw_fallback=True),
        "br": _BrotliDecoder,
        "zstd": _ZstandardDecoder,
    }
    encodings = [item.strip().lower() for item in (content_encoding or "").split(",")]
    encodings = [item for item in encodings if item]
    if not encodings:
        return _IdentityDecoder()
    decoders = [factories[encoding]() for encoding in encodings]
    return decoders[0] if len(decoders) == 1 else _MultiDecoder(decoders)


@dataclass
class StreamObserver:
    started_at: float = field(default_factory=time.perf_counter)
    streaming: bool = False
    first_byte_at: float | None = None
    first_token_at: float | None = None
    finished_at: float | None = None
    stream_error_status_code: int | None = None
    saw_tool_call: bool = False
    saw_completion: bool = False
    response_bytes: int = 0
    usage: Usage = field(default_factory=Usage)
    _usage_candidates: list[dict[str, Any]] = field(default_factory=list)
    _line_buffer: bytes = b""
    _capture: bytearray = field(default_factory=bytearray)
    _decoder: Any = field(default_factory=_IdentityDecoder)
    _decoding_failed: bool = False
    _decoder_flushed: bool = False

    def set_content_encoding(self, content_encoding: str | None) -> None:
        try:
            self._decoder = _content_decoder(content_encoding)
        except (KeyError, ImportError):
            self._decoding_failed = True

    def observe_bytes(self, chunk: bytes) -> bytes:
        if not chunk:
            return b""
        if self.first_byte_at is None:
            self.first_byte_at = time.perf_counter()
        self.response_bytes += len(chunk)
        if self._decoding_failed:
            return b""
        try:
            decoded = self._decoder.decode(chunk)
        except Exception:
            self._decoding_failed = True
            self._capture.clear()
            self._line_buffer = b""
            return b""
        self._capture_decoded(decoded)
        return decoded

    def _capture_decoded(self, data: bytes) -> None:
        if len(self._capture) < 8 * 1024 * 1024:
            remaining = 8 * 1024 * 1024 - len(self._capture)
            self._capture.extend(data[:remaining])

    def flush_decoding(self) -> bytes:
        if self._decoder_flushed or self._decoding_failed:
            return b""
        self._decoder_flushed = True
        try:
            decoded = self._decoder.flush()
        except Exception:
            self._decoding_failed = True
            self._capture.clear()
            self._line_buffer = b""
            return b""
        self._capture_decoded(decoded)
        return decoded

    def feed_objects(self, chunk: bytes) -> list[dict[str, Any]]:
        data = self._line_buffer + chunk
        lines = data.splitlines(keepends=True)
        self._line_buffer = b""
        if lines and not lines[-1].endswith((b"\n", b"\r")):
            self._line_buffer = lines.pop()
        objects: list[dict[str, Any]] = []
        for line in lines:
            payload = line.strip()
            if payload.startswith(b"data:"):
                payload = payload[5:].strip()
            if payload == b"[DONE]":
                self.mark_completion()
                continue
            objects.extend(json_objects_from_chunk(line))
        return objects

    def mark_token(self) -> None:
        if self.streaming and self.first_token_at is None:
            self.first_token_at = time.perf_counter()

    def add_usage(self, value: dict[str, Any] | None) -> None:
        if value:
            self._usage_candidates.append(value)

    def mark_finished(self) -> None:
        if self.finished_at is None:
            self.finished_at = time.perf_counter()

    def mark_completion(self) -> None:
        self.saw_completion = True
        self.mark_finished()

    def mark_stream_error(self, status_code: int | None = None) -> None:
        if self.stream_error_status_code is None:
            self.stream_error_status_code = status_code or 502

    def mark_tool_call(self) -> None:
        self.saw_tool_call = True

    @property
    def elapsed_seconds(self) -> float:
        end = self.finished_at if self.finished_at is not None else time.perf_counter()
        return end - self.started_at

    @property
    def first_byte_ms(self) -> int | None:
        if self.first_byte_at is None:
            return None
        return round((self.first_byte_at - self.started_at) * 1000)

    @property
    def first_token_ms(self) -> int | None:
        if self.first_token_at is None:
            return None
        return round((self.first_token_at - self.started_at) * 1000)

    @property
    def duration_ms(self) -> int:
        return round(self.elapsed_seconds * 1000)

    @property
    def tps(self) -> float | None:
        if self.usage.output_tokens is None:
            return None
        seconds = self.elapsed_seconds
        return round(self.usage.output_tokens / seconds, 3) if seconds > 0 else None


def json_objects_from_chunk(chunk: bytes) -> list[dict[str, Any]]:
    """Extract JSON objects from JSON lines and SSE data lines without changing the stream."""
    text = chunk.decode("utf-8", errors="ignore")
    objects: list[dict[str, Any]] = []
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("data:"):
            line = line[5:].strip()
        if not line or line == "[DONE]":
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            objects.append(value)
    return objects


def _status_code_from_value(value: Any) -> int | None:
    if isinstance(value, int) and 100 <= value <= 599:
        return value
    if isinstance(value, str):
        normalized = value.strip().upper().replace("-", "_").replace(" ", "_")
        if normalized.isdigit():
            status_code = int(normalized)
            return status_code if 100 <= status_code <= 599 else None
        if normalized in STREAM_ERROR_STATUS_ALIASES:
            return STREAM_ERROR_STATUS_ALIASES[normalized]
        if "RATE_LIMIT" in normalized:
            return 429
        if "TIMEOUT" in normalized:
            return 504
        if any(item in normalized for item in ("OVERLOADED", "UNAVAILABLE", "SERVER", "INTERNAL")):
            return 503
        if "AUTH" in normalized:
            return 401
    return None


def _status_code_from_mapping(value: dict[str, Any]) -> int | None:
    for key in (
        "status_code",
        "statusCode",
        "http_status",
        "httpStatus",
        "code",
        "status",
        "type",
    ):
        status_code = _status_code_from_value(value.get(key))
        if status_code is not None:
            return status_code
    return None


def stream_error_status(value: dict[str, Any]) -> int | None:
    error = value.get("error")
    if isinstance(error, dict):
        return _status_code_from_mapping(error) or _status_code_from_mapping(value) or 502
    if error is not None:
        return _status_code_from_mapping(value) or 502

    event_type = value.get("type")
    response = value.get("response")
    if isinstance(response, dict) and (
        event_type == "response.failed" or response.get("status") == "failed"
    ):
        response_error = response.get("error")
        if isinstance(response_error, dict):
            return (
                _status_code_from_mapping(response_error)
                or _status_code_from_mapping(response)
                or 502
            )
        return _status_code_from_mapping(response) or 502

    if isinstance(event_type, str) and (
        event_type == "error" or event_type.endswith(".failed")
    ):
        return _status_code_from_mapping(value) or 502
    return None


def contains_tool_call_signal(value: Any) -> bool:
    if isinstance(value, list):
        return any(contains_tool_call_signal(item) for item in value)
    if not isinstance(value, dict):
        return False
    event_type = value.get("type")
    if isinstance(event_type, str):
        normalized = event_type.lower()
        if "function_call" in normalized or "tool_call" in normalized:
            return True
        if normalized in {
            "function_call",
            "tool_call",
            "mcp_call",
            "computer_call",
            "web_search_call",
            "file_search_call",
            "code_interpreter_call",
            "local_shell_call",
        }:
            return True
    if value.get("tool_calls") or value.get("function_call"):
        return True
    return any(
        contains_tool_call_signal(value.get(key))
        for key in ("item", "output_item", "delta", "content_block", "response")
    )


def contains_completion_signal(value: Any) -> bool:
    """Recognize terminal events without relying on a client keeping the stream open."""
    if not isinstance(value, dict):
        return False
    event_type = value.get("type")
    if isinstance(event_type, str):
        normalized = event_type.lower()
        if normalized == "message_stop" or normalized.endswith(".completed"):
            return True
    response = value.get("response")
    if isinstance(response, dict) and response.get("status") == "completed":
        return True
    choices = value.get("choices")
    if isinstance(choices, list) and any(
        isinstance(choice, dict) and choice.get("finish_reason") is not None
        for choice in choices
    ):
        return True
    candidates = value.get("candidates")
    return isinstance(candidates, list) and any(
        isinstance(candidate, dict)
        and (candidate.get("finishReason") is not None or candidate.get("finish_reason") is not None)
        for candidate in candidates
    )


def clean_headers(
    headers: httpx.Headers | dict[str, str], remove_auth: bool = True
) -> list[tuple[str, str]]:
    blocked = set(HOP_BY_HOP_HEADERS)
    if remove_auth:
        blocked.update({"authorization", "x-api-key", "x-goog-api-key"})
    return [(key, value) for key, value in headers.items() if key.lower() not in blocked]


def join_base_path(base_url: str, suffix: str) -> str:
    base = urlsplit(base_url.rstrip("/"))
    base_path = base.path.rstrip("/")
    suffix = "/" + suffix.lstrip("/")
    if base_path and (suffix == base_path or suffix.startswith(base_path + "/")):
        path = suffix
    else:
        path = f"{base_path}{suffix}" or "/"
    return urlunsplit((base.scheme, base.netloc, path, "", ""))


def normalize_base_url(base_url: str) -> str:
    parsed = urlsplit(base_url.rstrip("/"))
    path = parsed.path.rstrip("/")
    for suffix in ("/v1beta", "/v1"):
        if path.endswith(suffix):
            path = path[: -len(suffix)]
            break
    return urlunsplit((parsed.scheme, parsed.netloc, path, parsed.query, parsed.fragment))


def append_query(url: str, raw_query: str) -> str:
    return f"{url}?{raw_query}" if raw_query else url


def remove_query_key(raw_query: str, key: str) -> str:
    pairs = [(k, v) for k, v in parse_qsl(raw_query, keep_blank_values=True) if k != key]
    return urlencode(pairs, doseq=True)


def set_url_query_key(url: str, key: str, value: str) -> str:
    parsed = urlsplit(url)
    pairs = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k != key]
    pairs.append((key, value))
    return urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, urlencode(pairs, doseq=True), parsed.fragment)
    )


class ProtocolAdapter:
    protocol = ""
    discovery_suffix = "/v1/models"

    def proxy_url(self, base_url: str, path: str, raw_query: str) -> str:
        return append_query(join_base_path(base_url, path), raw_query)

    def discovery_url(self, base_url: str) -> str:
        return join_base_path(base_url, self.discovery_suffix)

    def outbound_headers(self, inbound: httpx.Headers, api_key: str) -> list[tuple[str, str]]:
        headers = clean_headers(inbound)
        headers.append(("authorization", f"Bearer {api_key}"))
        return headers

    def extract_model(self, body: bytes, path: str) -> str | None:
        return None

    def inspect_request(self, body_file, path: str, query: str) -> tuple[str | None, bool | None]:
        model = None
        stream = None
        body_file.seek(0)
        try:
            for prefix, event, value in ijson.parse(body_file):
                if prefix == "model" and event == "string":
                    model = value
                elif prefix == "stream" and event == "boolean":
                    stream = value
                if model is not None and stream is not None:
                    break
        except (ijson.JSONError, UnicodeDecodeError):
            return None, None
        finally:
            body_file.seek(0)
        return model, bool(stream) if stream is not None else False

    def is_streaming(self, body: bytes, query: str = "") -> bool | None:
        try:
            data = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None
        return bool(data.get("stream")) if isinstance(data, dict) and "stream" in data else False

    def discovery_headers(self, api_key: str) -> list[tuple[str, str]]:
        return [("authorization", f"Bearer {api_key}")]

    def parse_models(self, payload: bytes) -> list[dict[str, Any]]:
        data = json.loads(payload)
        items = data.get("data", data) if isinstance(data, dict) else data
        return [item for item in items if isinstance(item, dict) and item.get("id")]

    def next_discovery_url(self, current_url: str, payload: bytes) -> str | None:
        return None

    def health_probe(self, base_url: str, api_key: str, model_id: str) -> httpx.Request:
        raise NotImplementedError

    def observe_chunk(self, observer: StreamObserver, chunk: bytes) -> None:
        decoded = observer.observe_bytes(chunk)
        self.observe_objects(observer, observer.feed_objects(decoded))

    def observe_objects(self, observer: StreamObserver, objects: list[dict[str, Any]]) -> None:
        for value in objects:
            status_code = stream_error_status(value)
            if status_code is not None:
                observer.mark_stream_error(status_code)
            if contains_completion_signal(value):
                observer.mark_completion()
            if contains_tool_call_signal(value):
                observer.mark_tool_call()
            usage = value.get("usage")
            if isinstance(usage, dict):
                observer.add_usage(usage)

    def normalize_usage(self, raw: dict[str, Any] | None) -> Usage:
        return Usage(raw=raw)

    def finish_observer(self, observer: StreamObserver) -> Usage:
        flushed = observer.flush_decoding()
        flushed_objects = observer.feed_objects(flushed) if flushed else []
        tail_objects = (
            json_objects_from_chunk(observer._line_buffer) if observer._line_buffer else []
        )
        captured_objects: list[dict[str, Any]] = []
        if observer._capture:
            try:
                captured = json.loads(observer._capture)
                if isinstance(captured, dict):
                    captured_objects = [captured]
                elif isinstance(captured, list):
                    captured_objects = [item for item in captured if isinstance(item, dict)]
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
        self.observe_objects(observer, flushed_objects + tail_objects + captured_objects)
        raw = None
        if observer._usage_candidates:
            raw = {}
            for candidate in observer._usage_candidates:
                raw.update(candidate)
        observer.usage = self.normalize_usage(raw)
        observer.mark_finished()
        return observer.usage


def _json_request(
    method: str, url: str, headers: list[tuple[str, str]], body: dict[str, Any]
) -> httpx.Request:
    return httpx.Request(method, url, headers=headers, json=body)


def get_adapter(protocol: str) -> ProtocolAdapter:
    if protocol == "openai_compatible":
        return OpenAICompatibleAdapter()
    if protocol == "openai_responses":
        return OpenAIResponsesAdapter()
    if protocol == "claude":
        return ClaudeAdapter()
    if protocol == "gemini":
        return GeminiAdapter()
    raise ValueError(f"Unsupported protocol: {protocol}")


class OpenAICompatibleAdapter(ProtocolAdapter):
    protocol = "openai_compatible"

    def extract_model(self, body: bytes, path: str) -> str | None:
        try:
            value = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None
        return value.get("model") if isinstance(value, dict) else None

    def health_probe(self, base_url: str, api_key: str, model_id: str) -> httpx.Request:
        body = {
            "model": model_id,
            "messages": [{"role": "user", "content": "Reply only OK"}],
            "max_tokens": 2,
        }
        return _json_request(
            "POST",
            join_base_path(base_url, "/v1/chat/completions"),
            self.discovery_headers(api_key),
            body,
        )

    def normalize_usage(self, raw: dict[str, Any] | None) -> Usage:
        if raw is None:
            return Usage()
        details = raw.get("prompt_tokens_details") or raw.get("input_tokens_details") or {}
        total_input = raw.get("prompt_tokens", raw.get("input_tokens"))
        cache_read = details.get("cached_tokens")
        cache_write = details.get("cache_write_tokens", details.get("cached_write_tokens"))
        miss = None
        if isinstance(total_input, int) and isinstance(cache_read, int):
            miss = max(total_input - cache_read - (cache_write or 0), 0)
        return Usage(
            total_input,
            cache_read,
            cache_write,
            miss,
            raw.get("completion_tokens", raw.get("output_tokens")),
            raw,
        )

    def observe_objects(self, observer: StreamObserver, objects: list[dict[str, Any]]) -> None:
        super().observe_objects(observer, objects)
        for value in objects:
            choices = value.get("choices")
            if choices and isinstance(choices, list):
                delta = choices[0].get("delta") or {}
                if delta.get("content") or delta.get("tool_calls") or delta.get("function_call"):
                    if delta.get("tool_calls") or delta.get("function_call"):
                        observer.mark_tool_call()
                    observer.mark_token()


class OpenAIResponsesAdapter(OpenAICompatibleAdapter):
    protocol = "openai_responses"

    def health_probe(self, base_url: str, api_key: str, model_id: str) -> httpx.Request:
        body = {"model": model_id, "input": "Reply only OK", "max_output_tokens": 2}
        return _json_request(
            "POST", join_base_path(base_url, "/v1/responses"), self.discovery_headers(api_key), body
        )

    def observe_objects(self, observer: StreamObserver, objects: list[dict[str, Any]]) -> None:
        ProtocolAdapter.observe_objects(self, observer, objects)
        for value in objects:
            if value.get("type", "").endswith(".delta") or value.get("delta"):
                observer.mark_token()
            if isinstance(value.get("response"), dict):
                usage = value["response"].get("usage")
                if isinstance(usage, dict):
                    observer.add_usage(usage)

    def normalize_usage(self, raw: dict[str, Any] | None) -> Usage:
        if raw is None:
            return Usage()
        details = raw.get("input_tokens_details") or {}
        total_input = raw.get("input_tokens")
        cache_read = details.get("cached_tokens")
        cache_write = details.get("cache_write_tokens", details.get("cached_write_tokens"))
        miss = (
            max(total_input - (cache_read or 0) - (cache_write or 0), 0)
            if isinstance(total_input, int)
            else None
        )
        return Usage(
            total_input, cache_read, cache_write, miss, raw.get("output_tokens"), raw
        )


class ClaudeAdapter(ProtocolAdapter):
    protocol = "claude"
    discovery_suffix = "/v1/models"

    def outbound_headers(self, inbound: httpx.Headers, api_key: str) -> list[tuple[str, str]]:
        headers = clean_headers(inbound)
        headers.append(("x-api-key", api_key))
        if not any(k.lower() == "anthropic-version" for k, _ in headers):
            headers.append(("anthropic-version", "2023-06-01"))
        return headers

    def discovery_headers(self, api_key: str) -> list[tuple[str, str]]:
        return [("x-api-key", api_key), ("anthropic-version", "2023-06-01")]

    def extract_model(self, body: bytes, path: str) -> str | None:
        try:
            value = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None
        return value.get("model") if isinstance(value, dict) else None

    def health_probe(self, base_url: str, api_key: str, model_id: str) -> httpx.Request:
        body = {
            "model": model_id,
            "max_tokens": 2,
            "messages": [{"role": "user", "content": "Reply only OK"}],
        }
        return _json_request(
            "POST", join_base_path(base_url, "/v1/messages"), self.discovery_headers(api_key), body
        )

    def next_discovery_url(self, current_url: str, payload: bytes) -> str | None:
        data = json.loads(payload)
        if not isinstance(data, dict) or not data.get("has_more") or not data.get("last_id"):
            return None
        return set_url_query_key(current_url, "after_id", str(data["last_id"]))

    def normalize_usage(self, raw: dict[str, Any] | None) -> Usage:
        if raw is None:
            return Usage()
        read = raw.get("cache_read_input_tokens")
        write = raw.get("cache_creation_input_tokens")
        miss = raw.get("input_tokens")
        known_inputs = [v for v in (miss, read, write) if isinstance(v, int)]
        total = sum(known_inputs) if known_inputs else None
        return Usage(total, read, write, miss, raw.get("output_tokens"), raw)

    def observe_objects(self, observer: StreamObserver, objects: list[dict[str, Any]]) -> None:
        super().observe_objects(observer, objects)
        for value in objects:
            message_usage = value.get("message", {}).get("usage")
            if isinstance(message_usage, dict):
                observer.add_usage(message_usage)
            if value.get("type") in {"content_block_delta", "message_start", "message_delta"}:
                if value.get("delta", {}).get("text") or value.get("content_block", {}).get("text"):
                    observer.mark_token()


class GeminiAdapter(ProtocolAdapter):
    protocol = "gemini"
    discovery_suffix = "/v1beta/models"

    def proxy_url(self, base_url: str, path: str, raw_query: str) -> str:
        return append_query(join_base_path(base_url, path), remove_query_key(raw_query, "key"))

    def outbound_headers(self, inbound: httpx.Headers, api_key: str) -> list[tuple[str, str]]:
        headers = clean_headers(inbound)
        headers.append(("x-goog-api-key", api_key))
        return headers

    def discovery_headers(self, api_key: str) -> list[tuple[str, str]]:
        return [("x-goog-api-key", api_key)]

    def extract_model(self, body: bytes, path: str) -> str | None:
        segments = path.split("/models/", 1)
        if len(segments) != 2:
            return None
        return segments[1].split(":", 1)[0]

    def inspect_request(self, body_file, path: str, query: str) -> tuple[str | None, bool | None]:
        return self.extract_model(b"", path), ":streamGenerateContent" in path

    def is_streaming(self, body: bytes, query: str = "") -> bool | None:
        return ":streamGenerateContent" in query or False

    def health_probe(self, base_url: str, api_key: str, model_id: str) -> httpx.Request:
        body = {
            "contents": [{"role": "user", "parts": [{"text": "Reply only OK"}]}],
            "generationConfig": {"maxOutputTokens": 2},
        }
        url = join_base_path(base_url, f"/v1beta/models/{model_id}:generateContent")
        return _json_request("POST", url, self.discovery_headers(api_key), body)

    def parse_models(self, payload: bytes) -> list[dict[str, Any]]:
        data = json.loads(payload)
        items = data.get("models", []) if isinstance(data, dict) else []
        result = []
        for item in items:
            if not isinstance(item, dict):
                continue
            name = item.get("name", "")
            model_id = name.rsplit("/", 1)[-1] if name else item.get("modelId")
            if model_id:
                result.append({**item, "id": model_id})
        return result

    def next_discovery_url(self, current_url: str, payload: bytes) -> str | None:
        data = json.loads(payload)
        token = data.get("nextPageToken") if isinstance(data, dict) else None
        return set_url_query_key(current_url, "pageToken", token) if token else None

    def normalize_usage(self, raw: dict[str, Any] | None) -> Usage:
        if raw is None:
            return Usage()
        total = raw.get("promptTokenCount")
        output = raw.get("candidatesTokenCount")
        cache = raw.get("cachedContentTokenCount")
        miss = max(total - (cache or 0), 0) if isinstance(total, int) else None
        return Usage(total, cache, None, miss, output, raw)

    def observe_objects(self, observer: StreamObserver, objects: list[dict[str, Any]]) -> None:
        super().observe_objects(observer, objects)
        for value in objects:
            candidates = value.get("candidates") or []
            if candidates:
                parts = candidates[0].get("content", {}).get("parts", [])
                if any(
                    part.get("text") or part.get("functionCall")
                    for part in parts
                    if isinstance(part, dict)
                ):
                    observer.mark_token()
            if isinstance(value.get("usageMetadata"), dict):
                observer.add_usage(value["usageMetadata"])
