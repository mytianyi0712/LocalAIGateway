import asyncio
import secrets as stdlib_secrets
import time
import uuid
from contextlib import suppress
from datetime import timedelta
from tempfile import SpooledTemporaryFile

import httpx
from fastapi import Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from sqlalchemy import select

from app.adapters import PROTOCOL_ENDPOINTS, get_adapter
from app.adapters.base import HOP_BY_HOP_HEADERS, StreamObserver, Usage
from app.adapters.convert import (
    convert_error_response,
    convert_mapped_error_response,
    convert_mapped_request,
    convert_mapped_response,
    convert_request,
    convert_response,
    get_mapped_streaming_converter,
    get_streaming_converter,
)
from app.core.access import resolve_access_policy
from app.db.models import ClaudeModelMapping, CodexModelMapping, utcnow
from app.services.circuit_breaker import (
    classify_exception,
    classify_http_status,
    record_failure,
)
from app.services.routing import CandidateSnapshot, resolve_candidates
from app.services.catalog import supported_protocols_for_model
from app.services.settings import get_runtime_settings


def _gateway_error(protocol: str, status: int, code: str, message: str, request_id: str):
    if protocol == "claude":
        payload = {
            "type": "error",
            "error": {"type": "gateway_error", "message": message},
            "request_id": request_id,
        }
    elif protocol == "gemini":
        status_name = {
            400: "INVALID_ARGUMENT",
            401: "UNAUTHENTICATED",
            404: "NOT_FOUND",
            413: "RESOURCE_EXHAUSTED",
            503: "UNAVAILABLE",
            504: "DEADLINE_EXCEEDED",
        }.get(status, "UNKNOWN")
        payload = {
            "error": {"code": status, "message": message, "status": status_name},
            "request_id": request_id,
        }
    else:
        payload = {
            "error": {"message": message, "type": "gateway_error", "code": code},
            "request_id": request_id,
        }
    return JSONResponse(payload, status_code=status)


def _unsupported_endpoint_error(
    protocol: str,
    model_id: str,
    supported_protocols: list[str],
    request_id: str,
):
    supported_endpoints = [
        endpoint
        for supported_protocol in supported_protocols
        for endpoint in PROTOCOL_ENDPOINTS[supported_protocol]
    ]
    message = (
        f"Model '{model_id}' does not support the requested '{protocol}' interface. "
        f"Supported interfaces: {', '.join(supported_protocols)}."
    )
    details = {
        "requested_protocol": protocol,
        "supported_protocols": supported_protocols,
        "supported_endpoints": supported_endpoints,
    }
    if protocol == "claude":
        payload = {
            "type": "error",
            "error": {
                "type": "unsupported_model_endpoint",
                "message": message,
                **details,
            },
            "request_id": request_id,
        }
    elif protocol == "gemini":
        payload = {
            "error": {
                "code": 400,
                "message": message,
                "status": "INVALID_ARGUMENT",
                "details": [{"reason": "UNSUPPORTED_MODEL_ENDPOINT", **details}],
            },
            "request_id": request_id,
        }
    else:
        payload = {
            "error": {
                "message": message,
                "type": "invalid_request_error",
                "code": "unsupported_model_endpoint",
                "param": "model",
                **details,
            },
            "request_id": request_id,
        }
    return JSONResponse(payload, status_code=400)


def _supplied_credential(request: Request, protocol: str) -> str | None:
    supplied = request.headers.get("x-local-gateway-key")
    if not supplied and protocol == "catalog":
        auth = request.headers.get("authorization", "")
        supplied = (
            auth[7:]
            if auth.lower().startswith("bearer ")
            else request.headers.get("x-api-key")
            or request.headers.get("x-goog-api-key")
            or request.query_params.get("key")
        )
    elif not supplied and protocol in {"openai_compatible", "openai_responses"}:
        auth = request.headers.get("authorization", "")
        supplied = auth[7:] if auth.lower().startswith("bearer ") else None
    elif not supplied and protocol == "claude":
        supplied = request.headers.get("x-api-key")
    elif not supplied and protocol == "gemini":
        supplied = request.headers.get("x-goog-api-key") or request.query_params.get("key")
    return supplied


def _response_headers(headers: httpx.Headers) -> dict[str, str]:
    blocked = HOP_BY_HOP_HEADERS - {"content-length"}
    return {key: value for key, value in headers.items() if key.lower() not in blocked}


def _attempt_payload(
    request_id: str,
    candidate: CandidateSnapshot,
    attempt_no: int,
    observer: StreamObserver,
    *,
    status_code: int | None,
    outcome: str,
    error_kind: str | None,
    failover_eligible: bool,
    response_started: bool,
    upstream_protocol: str | None = None,
    upstream_model_id: str | None = None,
) -> dict:
    usage = observer.usage
    if not response_started and outcome in {"transport_error", "http_error", "cancelled"}:
        # 尝试未产生任何响应就失败（随后会故障转移到其他渠道）：
        # 其解析到的 usage 只是中断流中的不完整数据，不应计入用量统计。
        usage = Usage(raw=usage.raw)
    finished_at = utcnow()
    payload = {
        "request_id": request_id,
        "channel_id": candidate.channel_id,
        "channel_name": candidate.channel_name,
        "attempt_no": attempt_no,
        "priority_snapshot": candidate.priority,
        "started_at": finished_at - timedelta(milliseconds=observer.duration_ms),
        "finished_at": finished_at,
        "status_code": status_code,
        "outcome": outcome,
        "error_kind": error_kind,
        "failover_eligible": failover_eligible,
        "response_started": response_started,
        "first_byte_ms": observer.first_byte_ms,
        "first_token_ms": observer.first_token_ms,
        "duration_ms": observer.duration_ms,
        "input_tokens": usage.input_tokens,
        "cache_read_tokens": usage.cache_read_tokens,
        "cache_write_tokens": usage.cache_write_tokens,
        "cache_miss_input_tokens": usage.cache_miss_input_tokens,
        "output_tokens": usage.output_tokens,
        "tps": observer.tps,
        "raw_usage_json": usage.raw,
        "response_bytes": observer.response_bytes,
    }
    if upstream_protocol is not None:
        payload["upstream_protocol"] = upstream_protocol
        payload["upstream_model_id"] = upstream_model_id or candidate.model_id
    return payload


def _emit_channel_success(telemetry, channel_id: str) -> None:
    telemetry.emit("channel_success", {"channel_id": channel_id})


def _emit_channel_failure(
    telemetry,
    channel_id: str,
    error_kind: str,
    status_code: int | None,
    countable: bool,
    runtime: dict,
) -> None:
    telemetry.emit(
        "channel_failure",
        {
            "channel_id": channel_id,
            "error_kind": error_kind,
            "status_code": status_code,
            "countable": countable,
            "threshold": int(runtime["failure_threshold"]),
            "open_seconds": int(runtime["circuit_open_seconds"]),
        },
    )


async def _safe_close_response(response: httpx.Response) -> None:
    close_task = asyncio.create_task(response.aclose())
    with suppress(asyncio.CancelledError, Exception):
        await asyncio.shield(close_task)


async def _read_response_prelude(
    adapter,
    response: httpx.Response,
    observer: StreamObserver,
    first_token_timeout_seconds: float,
    max_bytes: int = 1024 * 1024,
):
    chunks: list[bytes] = []
    total = 0
    iterator = response.aiter_raw()
    remaining = max(0.0, first_token_timeout_seconds - observer.elapsed_seconds)
    async with asyncio.timeout(remaining):
        async for chunk in iterator:
            adapter.observe_chunk(observer, chunk)
            chunks.append(chunk)
            total += len(chunk)
            if observer.stream_error_status_code is not None:
                break
            if observer.first_token_at is not None or observer.saw_tool_call or total >= max_bytes:
                break
    return chunks, iterator


async def proxy_request(request: Request, protocol: str):
    app = request.app
    request_id = str(uuid.uuid4())
    started_at = utcnow()
    perf_started = time.perf_counter()
    telemetry = app.state.telemetry
    request_finished = False

    def finish_request(
        *,
        final_status_code: int,
        outcome: str,
        attempt_count: int,
        final_channel_id: str | None = None,
        response_bytes: int = 0,
    ) -> None:
        nonlocal request_finished
        request_finished = True
        payload = {
            "id": request_id,
            "finished_at": utcnow(),
            "total_duration_ms": round((time.perf_counter() - perf_started) * 1000),
            "final_status_code": final_status_code,
            "outcome": outcome,
            "attempt_count": attempt_count,
            "response_bytes": response_bytes,
        }
        if final_channel_id is not None:
            payload["final_channel_id"] = final_channel_id
        telemetry.emit("request_finish", payload)

    access_error = await gateway_access_error(request, protocol, request_id)
    if access_error:
        return access_error

    body_file = SpooledTemporaryFile(max_size=8 * 1024 * 1024, mode="w+b")
    body_size = 0
    async for chunk in request.stream():
        body_size += len(chunk)
        if body_size > app.state.settings.request_body_limit_bytes:
            body_file.close()
            return _gateway_error(
                protocol, 413, "request_too_large", "Request body is too large.", request_id
            )
        body_file.write(chunk)
    body_file.seek(0)

    adapter = get_adapter(protocol)
    model_id, stream = adapter.inspect_request(body_file, request.url.path, request.url.query)
    if not model_id:
        body_file.close()
        return _gateway_error(
            protocol, 400, "model_required", "Unable to determine model from request.", request_id
        )

    telemetry.emit(
        "request_start",
        {
            "id": request_id,
            "protocol": protocol,
            "model_id": model_id,
            "endpoint": request.url.path,
            "stream": stream,
            "started_at": started_at,
            "outcome": "pending",
            "attempt_count": 0,
            "request_bytes": body_size,
        },
    )

    try:
        async with app.state.db.sessions() as session:
            runtime = await get_runtime_settings(session)
            candidates = await resolve_candidates(
                session, protocol, model_id, int(runtime["max_failover_attempts"])
            )
            supported_protocols = (
                await supported_protocols_for_model(session, model_id) if not candidates else []
            )
    except asyncio.CancelledError:
        body_file.close()
        if not request_finished:
            finish_request(
                final_status_code=499,
                outcome="cancelled",
                attempt_count=0,
                response_bytes=0,
            )
        return Response(status_code=204)

    if not candidates:
        body_file.close()
        protocol_mismatch = bool(supported_protocols) and protocol not in supported_protocols
        status_code = 400 if protocol_mismatch else 503
        finish_request(
            final_status_code=status_code,
            outcome="gateway_error",
            attempt_count=0,
            response_bytes=0,
        )
        if protocol_mismatch:
            return _unsupported_endpoint_error(
                protocol, model_id, supported_protocols, request_id
            )
        return _gateway_error(
            protocol, 503, "no_active_channel", "No active channel is available for this model.", request_id
        )

    inbound_headers = httpx.Headers(request.headers.items())
    last_http_error: tuple[int, dict[str, str], bytes, str] | None = None
    last_error_kind = "transport_error"
    attempts = 0

    for attempt_no, candidate in enumerate(candidates, 1):
        attempts = attempt_no
        observer = StreamObserver(streaming=bool(stream))
        api_key = app.state.secrets.decrypt(candidate.api_key_encrypted)
        target_url = adapter.proxy_url(candidate.base_url, request.url.path, request.url.query)
        headers = adapter.outbound_headers(inbound_headers, api_key)
        body_file.seek(0)

        async def request_body_stream():
            while True:
                request_chunk = body_file.read(64 * 1024)
                if not request_chunk:
                    break
                yield request_chunk

        headers.append(("content-length", str(body_size)))
        upstream_request = app.state.http.build_request(
            request.method,
            target_url,
            headers=headers,
            content=request_body_stream(),
        )
        upstream_request.extensions["timeout"] = {
            "connect": float(runtime["connect_timeout_seconds"]),
            "read": float(
                runtime["stream_idle_timeout_seconds"]
                if stream
                else runtime["first_byte_timeout_seconds"]
            ),
            "write": 60.0,
            "pool": 10.0,
        }
        try:
            try:
                upstream = await app.state.http.send(upstream_request, stream=True)
            except asyncio.CancelledError:
                adapter.finish_observer(observer)
                body_file.close()
                telemetry.emit(
                    "attempt",
                    _attempt_payload(
                        request_id,
                        candidate,
                        attempt_no,
                        observer,
                        status_code=None,
                        outcome="cancelled",
                        error_kind=None,
                        failover_eligible=False,
                        response_started=False,
                    ),
                )
                if not request_finished:
                    finish_request(
                        final_status_code=499,
                        outcome="cancelled",
                        attempt_count=attempt_no,
                        final_channel_id=candidate.channel_id,
                        response_bytes=observer.response_bytes,
                    )
                return Response(status_code=204)
            observer.set_content_encoding(upstream.headers.get("content-encoding"))
            error_kind, countable = classify_http_status(upstream.status_code)
            if upstream.is_success:
                try:
                    prelude_chunks, upstream_iterator = await _read_response_prelude(
                        adapter,
                        upstream,
                        observer,
                        float(
                            runtime.get(
                                "first_token_timeout_seconds",
                                runtime["first_byte_timeout_seconds"],
                            )
                        ),
                    )
                except asyncio.CancelledError:
                    adapter.finish_observer(observer)
                    body_file.close()
                    await _safe_close_response(upstream)
                    outcome = (
                        "success"
                        if observer.saw_completion or observer.saw_tool_call
                        else "cancelled"
                    )
                    if outcome == "success":
                        _emit_channel_success(telemetry, candidate.channel_id)
                    telemetry.emit(
                        "attempt",
                        _attempt_payload(
                            request_id,
                            candidate,
                            attempt_no,
                            observer,
                            status_code=upstream.status_code,
                            outcome=outcome,
                            error_kind=None,
                            failover_eligible=False,
                            response_started=observer.response_bytes > 0,
                        ),
                    )
                    finish_request(
                        final_status_code=upstream.status_code,
                        outcome=outcome,
                        attempt_count=attempt_no,
                        final_channel_id=candidate.channel_id,
                        response_bytes=observer.response_bytes,
                    )
                    return Response(status_code=204)
                except Exception as exc:
                    await _safe_close_response(upstream)
                    last_error_kind = classify_exception(exc)
                    async with app.state.db.sessions() as session:
                        opened = await record_failure(
                            session,
                            candidate.channel_id,
                            last_error_kind,
                            None,
                            True,
                            int(runtime["failure_threshold"]),
                            int(runtime["circuit_open_seconds"]),
                        )
                    if opened:
                        app.state.health_supervisor.reschedule()
                    adapter.finish_observer(observer)
                    telemetry.emit(
                        "attempt",
                        _attempt_payload(
                            request_id,
                            candidate,
                            attempt_no,
                            observer,
                            status_code=None,
                            outcome="transport_error",
                            error_kind=last_error_kind,
                            failover_eligible=attempt_no < len(candidates),
                            response_started=False,
                        ),
                    )
                    continue

                if observer.stream_error_status_code is not None:
                    await _safe_close_response(upstream)
                    adapter.finish_observer(observer)
                    status_code = observer.stream_error_status_code
                    stream_error_kind, stream_error_countable = classify_http_status(status_code)
                    raw_error = b"".join(prelude_chunks)
                    last_http_error = (
                        status_code,
                        _response_headers(upstream.headers),
                        raw_error,
                        candidate.channel_id,
                    )
                    last_error_kind = stream_error_kind
                    async with app.state.db.sessions() as session:
                        opened = await record_failure(
                            session,
                            candidate.channel_id,
                            stream_error_kind,
                            status_code,
                            stream_error_countable,
                            int(runtime["failure_threshold"]),
                            int(runtime["circuit_open_seconds"]),
                        )
                    if opened:
                        app.state.health_supervisor.reschedule()
                    telemetry.emit(
                        "attempt",
                        _attempt_payload(
                            request_id,
                            candidate,
                            attempt_no,
                            observer,
                            status_code=status_code,
                            outcome="http_error",
                            error_kind=stream_error_kind,
                            failover_eligible=attempt_no < len(candidates),
                            response_started=False,
                        ),
                    )
                    continue

                async def stream_response(
                    response=upstream,
                    selected=candidate,
                    selected_attempt_no=attempt_no,
                    selected_observer=observer,
                    buffered_chunks=prelude_chunks,
                    raw_iterator=upstream_iterator,
                ):
                    outcome = "success"
                    stream_error = None
                    stream_error_countable = False
                    stream_error_recorded = False
                    final_status_code = response.status_code
                    try:
                        for chunk in buffered_chunks:
                            yield chunk
                        async for chunk in raw_iterator:
                            adapter.observe_chunk(selected_observer, chunk)
                            if selected_observer.stream_error_status_code is not None:
                                final_status_code = selected_observer.stream_error_status_code
                                stream_error, stream_error_countable = classify_http_status(
                                    final_status_code
                                )
                                outcome = "upstream_error"
                                _emit_channel_failure(
                                    telemetry,
                                    selected.channel_id,
                                    stream_error or "upstream_error",
                                    final_status_code,
                                    stream_error_countable,
                                    runtime,
                                )
                                stream_error_recorded = True
                                yield chunk
                                break
                            yield chunk
                    except asyncio.CancelledError:
                        if selected_observer.saw_completion or selected_observer.saw_tool_call:
                            outcome = "success"
                            stream_error = None
                            final_status_code = response.status_code
                        else:
                            outcome = "cancelled"
                            stream_error = None
                            final_status_code = response.status_code
                    except Exception as exc:
                        outcome = "stream_interrupted"
                        stream_error = classify_exception(exc)
                        final_status_code = 504 if "timeout" in stream_error else 502
                        _emit_channel_failure(
                            telemetry,
                            selected.channel_id,
                            stream_error,
                            final_status_code,
                            True,
                            runtime,
                        )
                        raise
                    finally:
                        body_file.close()
                        adapter.finish_observer(selected_observer)
                        if selected_observer.stream_error_status_code is not None:
                            final_status_code = selected_observer.stream_error_status_code
                            stream_error, stream_error_countable = classify_http_status(
                                final_status_code
                            )
                            if outcome == "success":
                                outcome = "upstream_error"
                        else:
                            stream_error_countable = False
                        if outcome == "success":
                            _emit_channel_success(telemetry, selected.channel_id)
                        elif outcome == "upstream_error" and not stream_error_recorded:
                            _emit_channel_failure(
                                telemetry,
                                selected.channel_id,
                                stream_error or "upstream_error",
                                final_status_code,
                                stream_error_countable,
                                runtime,
                            )
                        telemetry.emit(
                            "attempt",
                            _attempt_payload(
                                request_id,
                                selected,
                                selected_attempt_no,
                                selected_observer,
                                status_code=final_status_code,
                                outcome=outcome,
                                error_kind=stream_error,
                                failover_eligible=False,
                                response_started=True,
                            ),
                        )
                        finish_request(
                            final_status_code=final_status_code,
                            outcome=outcome,
                            attempt_count=selected_attempt_no,
                            final_channel_id=selected.channel_id,
                            response_bytes=selected_observer.response_bytes,
                        )
                        await _safe_close_response(response)

                return StreamingResponse(
                    stream_response(),
                    status_code=upstream.status_code,
                    headers=_response_headers(upstream.headers),
                )

            error_body_parts: list[bytes] = []
            try:
                async for chunk in upstream.aiter_raw():
                    observer.observe_bytes(chunk)
                    error_body_parts.append(chunk)
            except asyncio.CancelledError:
                adapter.finish_observer(observer)
                body_file.close()
                await _safe_close_response(upstream)
                telemetry.emit(
                    "attempt",
                    _attempt_payload(
                        request_id,
                        candidate,
                        attempt_no,
                        observer,
                        status_code=upstream.status_code,
                        outcome="cancelled",
                        error_kind=None,
                        failover_eligible=False,
                        response_started=observer.response_bytes > 0,
                    ),
                )
                if not request_finished:
                    finish_request(
                        final_status_code=upstream.status_code,
                        outcome="cancelled",
                        attempt_count=attempt_no,
                        final_channel_id=candidate.channel_id,
                        response_bytes=observer.response_bytes,
                    )
                return Response(status_code=204)
            await _safe_close_response(upstream)
            adapter.finish_observer(observer)
            raw_error = b"".join(error_body_parts)
            last_http_error = (
                upstream.status_code,
                _response_headers(upstream.headers),
                raw_error,
                candidate.channel_id,
            )
            last_error_kind = error_kind
            async with app.state.db.sessions() as session:
                opened = await record_failure(
                    session,
                    candidate.channel_id,
                    error_kind,
                    upstream.status_code,
                    countable,
                    int(runtime["failure_threshold"]),
                    int(runtime["circuit_open_seconds"]),
                )
            if opened:
                app.state.health_supervisor.reschedule()
            telemetry.emit(
                "attempt",
                _attempt_payload(
                    request_id,
                    candidate,
                    attempt_no,
                    observer,
                    status_code=upstream.status_code,
                    outcome="http_error",
                    error_kind=error_kind,
                    failover_eligible=attempt_no < len(candidates),
                    response_started=False,
                ),
            )
        except httpx.HTTPError as exc:
            last_error_kind = classify_exception(exc)
            async with app.state.db.sessions() as session:
                opened = await record_failure(
                    session,
                    candidate.channel_id,
                    last_error_kind,
                    None,
                    True,
                    int(runtime["failure_threshold"]),
                    int(runtime["circuit_open_seconds"]),
                )
            if opened:
                app.state.health_supervisor.reschedule()
            adapter.finish_observer(observer)
            telemetry.emit(
                "attempt",
                _attempt_payload(
                    request_id,
                    candidate,
                    attempt_no,
                    observer,
                    status_code=None,
                    outcome="transport_error",
                    error_kind=last_error_kind,
                    failover_eligible=attempt_no < len(candidates),
                    response_started=False,
                ),
            )

    if last_http_error:
        body_file.close()
        status, headers, raw_body, final_channel_id = last_http_error
        finish_request(
            final_status_code=status,
            outcome="upstream_error",
            attempt_count=attempts,
            final_channel_id=final_channel_id,
            response_bytes=len(raw_body),
        )
        return Response(content=raw_body, status_code=status, headers=headers)

    status = 504 if "timeout" in last_error_kind else 502
    body_file.close()
    finish_request(
        final_status_code=status,
        outcome="gateway_error",
        attempt_count=attempts,
        response_bytes=0,
    )
    return _gateway_error(
        protocol,
        status,
        "upstream_unreachable",
        "All eligible upstream channels failed before a response was available.",
        request_id,
    )


def _mapped_response_headers(headers: httpx.Headers, stream: bool) -> dict[str, str]:
    result = _response_headers(headers)
    result.pop("content-length", None)
    result["content-type"] = "text/event-stream" if stream else "application/json"
    return result


def _gemini_upstream_path(upstream_model: str, stream: bool) -> str:
    from urllib.parse import quote

    action = ":streamGenerateContent" if stream else ":generateContent"
    return f"/v1beta/models/{quote(upstream_model, safe='')}{action}"


def _mapping_class_for_entry(entry_protocol: str):
    if entry_protocol == "claude":
        return ClaudeModelMapping
    if entry_protocol == "openai_responses":
        return CodexModelMapping
    raise ValueError(f"Unsupported entry protocol: {entry_protocol}")


def _mapping_model_id_field(entry_protocol: str) -> str:
    return "claude_model_id" if entry_protocol == "claude" else "codex_model_id"


def _mapped_error_body(entry_protocol: str, message: str) -> bytes:
    if entry_protocol == "claude":
        payload = {
            "type": "error",
            "error": {"type": "gateway_error", "message": message},
        }
    else:
        payload = {
            "error": {"message": message, "type": "gateway_error", "code": "conversion_error"}
        }
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


async def proxy_mapped_entry(request: Request, entry_protocol: str = "claude"):
    """Entry point for a dedicated model-mapping endpoint (e.g. ``/claudecode``,
    ``/codex``).

    Only requests whose model name matches a configured mapping for the entry
    protocol are routed here; the regular gateway endpoints remain untouched.
    """
    app = request.app
    request_id = str(uuid.uuid4())
    started_at = utcnow()
    perf_started = time.perf_counter()
    telemetry = app.state.telemetry
    mapping_cls = _mapping_class_for_entry(entry_protocol)

    access_error = await gateway_access_error(request, entry_protocol, request_id)
    if access_error:
        return access_error

    body_file = SpooledTemporaryFile(max_size=8 * 1024 * 1024, mode="w+b")
    body_size = 0
    async for chunk in request.stream():
        body_size += len(chunk)
        if body_size > app.state.settings.request_body_limit_bytes:
            body_file.close()
            return _gateway_error(
                entry_protocol, 413, "request_too_large", "Request body is too large.", request_id
            )
        body_file.write(chunk)
    body_file.seek(0)

    adapter = get_adapter(entry_protocol)
    model_id, stream = adapter.inspect_request(body_file, request.url.path, request.url.query)
    if not model_id:
        body_file.close()
        return _gateway_error(
            entry_protocol,
            400,
            "model_required",
            "Unable to determine model from request.",
            request_id,
        )

    telemetry.emit(
        "request_start",
        {
            "id": request_id,
            "protocol": entry_protocol,
            "model_id": model_id,
            "endpoint": request.url.path,
            "stream": stream,
            "started_at": started_at,
            "outcome": "pending",
            "attempt_count": 0,
            "request_bytes": body_size,
        },
    )

    try:
        async with app.state.db.sessions() as session:
            mapping = await session.scalar(
                select(mapping_cls).where(
                    getattr(mapping_cls, _mapping_model_id_field(entry_protocol)) == model_id
                )
            )
    except asyncio.CancelledError:
        body_file.close()
        telemetry.emit(
            "request_finish",
            {
                "id": request_id,
                "finished_at": utcnow(),
                "total_duration_ms": round((time.perf_counter() - perf_started) * 1000),
                "final_status_code": 499,
                "outcome": "cancelled",
                "attempt_count": 0,
                "response_bytes": 0,
            },
        )
        return Response(status_code=204)

    if mapping is None or not mapping.enabled:
        body_file.close()
        telemetry.emit(
            "request_finish",
            {
                "id": request_id,
                "finished_at": utcnow(),
                "total_duration_ms": round((time.perf_counter() - perf_started) * 1000),
                "final_status_code": 404,
                "outcome": "gateway_error",
                "attempt_count": 0,
                "response_bytes": 0,
            },
        )
        return _gateway_error(
            entry_protocol,
            404,
            "unknown_mapped_model",
            f"Model '{model_id}' is not a configured model mapping.",
            request_id,
        )

    return await proxy_mapped_request(
        request,
        mapping,
        model_id,
        stream,
        body_file,
        body_size,
        request_id,
        started_at,
        perf_started,
        entry_protocol,
    )


async def proxy_codex_entry(request: Request):
    """Entry point for the dedicated ``/codex`` Responses-API mapping endpoint."""
    return await proxy_mapped_entry(request, "openai_responses")


async def proxy_mapped_request(
    request: Request,
    mapping,
    model_id: str,
    stream: bool,
    body_file: SpooledTemporaryFile,
    body_size: int,
    request_id: str,
    started_at,
    perf_started: float,
    entry_protocol: str = "claude",
):
    """Route a mapped entry request (Claude /v1/messages or Responses API)
    through a model mapping.

    The mapping carries its own upstream protocol and candidate list, so each
    mapped model can be converted to a different protocol. Request bodies are
    converted before forwarding; responses are converted back to the entry
    protocol format (streaming responses event-by-event).
    """
    app = request.app
    telemetry = app.state.telemetry
    request_finished = False
    upstream_protocol = mapping.upstream_protocol
    entry_model_id = (
        mapping.claude_model_id if entry_protocol == "claude" else mapping.codex_model_id
    )
    try:
        upstream_adapter = get_adapter(upstream_protocol)
    except ValueError as exc:
        body_file.close()
        telemetry.emit(
            "request_finish",
            {
                "id": request_id,
                "finished_at": utcnow(),
                "total_duration_ms": round((time.perf_counter() - perf_started) * 1000),
                "final_status_code": 422,
                "outcome": "gateway_error",
                "attempt_count": 0,
                "response_bytes": 0,
            },
        )
        return _gateway_error(
            entry_protocol,
            422,
            "invalid_upstream_protocol",
            str(exc),
            request_id,
        )

    def finish_request(
        *,
        final_status_code: int,
        outcome: str,
        attempt_count: int,
        final_channel_id: str | None = None,
        response_bytes: int = 0,
    ) -> None:
        nonlocal request_finished
        request_finished = True
        payload = {
            "id": request_id,
            "finished_at": utcnow(),
            "total_duration_ms": round((time.perf_counter() - perf_started) * 1000),
            "final_status_code": final_status_code,
            "outcome": outcome,
            "attempt_count": attempt_count,
            "response_bytes": response_bytes,
        }
        if final_channel_id is not None:
            payload["final_channel_id"] = final_channel_id
        telemetry.emit("request_finish", payload)

    try:
        async with app.state.db.sessions() as session:
            runtime = await get_runtime_settings(session)
            candidates = await resolve_candidates(
                session,
                upstream_protocol,
                mapping.upstream_model_id,
                int(runtime["max_failover_attempts"]),
            )
    except asyncio.CancelledError:
        body_file.close()
        if not request_finished:
            finish_request(
                final_status_code=499,
                outcome="cancelled",
                attempt_count=0,
                response_bytes=0,
            )
        return Response(status_code=204)

    if not candidates:
        body_file.close()
        finish_request(
            final_status_code=503,
            outcome="gateway_error",
            attempt_count=0,
            response_bytes=0,
        )
        return _gateway_error(
            entry_protocol,
            503,
            "no_active_channel",
            f"No active channel is available for mapped model '{entry_model_id}'.",
            request_id,
        )

    if upstream_protocol == "gemini":
        upstream_path_template = None  # built per candidate below
    else:
        upstream_path = PROTOCOL_ENDPOINTS[upstream_protocol][0]

    inbound_headers = httpx.Headers(request.headers.items())
    if upstream_protocol != "claude":
        inbound_headers = httpx.Headers(
            [
                (key, value)
                for key, value in inbound_headers.items()
                if key.lower() not in {"anthropic-version", "anthropic-beta"}
            ]
        )

    last_http_error: tuple[int, dict[str, str], bytes, str] | None = None
    last_error_kind = "transport_error"
    attempts = 0

    for attempt_no, candidate in enumerate(candidates, 1):
        attempts = attempt_no
        observer = StreamObserver(streaming=bool(stream))
        api_key = app.state.secrets.decrypt(candidate.api_key_encrypted)
        if upstream_protocol == "gemini":
            upstream_path = _gemini_upstream_path(candidate.model_id, stream)
        target_url = upstream_adapter.proxy_url(
            candidate.base_url, upstream_path, request.url.query
        )
        headers = upstream_adapter.outbound_headers(inbound_headers, api_key)

        body_file.seek(0)
        raw_body = body_file.read()
        try:
            converted_body = convert_mapped_request(
                entry_protocol, upstream_protocol, candidate.model_id, raw_body
            )
        except ValueError as exc:
            body_file.close()
            if not request_finished:
                finish_request(
                    final_status_code=400,
                    outcome="gateway_error",
                    attempt_count=attempt_no,
                    response_bytes=0,
                )
            return _gateway_error(
                entry_protocol,
                400,
                "conversion_error",
                str(exc),
                request_id,
            )

        headers = [(key, value) for key, value in headers if key.lower() != "content-length"]
        headers.append(("content-length", str(len(converted_body))))
        upstream_request = app.state.http.build_request(
            request.method,
            target_url,
            headers=headers,
            content=converted_body,
        )
        upstream_request.extensions["timeout"] = {
            "connect": float(runtime["connect_timeout_seconds"]),
            "read": float(
                runtime["stream_idle_timeout_seconds"]
                if stream
                else runtime["first_byte_timeout_seconds"]
            ),
            "write": 60.0,
            "pool": 10.0,
        }
        try:
            try:
                upstream = await app.state.http.send(upstream_request, stream=True)
            except asyncio.CancelledError:
                upstream_adapter.finish_observer(observer)
                body_file.close()
                telemetry.emit(
                    "attempt",
                    _attempt_payload(
                        request_id,
                        candidate,
                        attempt_no,
                        observer,
                        status_code=None,
                        outcome="cancelled",
                        error_kind=None,
                        failover_eligible=False,
                        response_started=False,
                        upstream_protocol=upstream_protocol,
                        upstream_model_id=candidate.model_id,
                    ),
                )
                if not request_finished:
                    finish_request(
                        final_status_code=499,
                        outcome="cancelled",
                        attempt_count=attempt_no,
                        final_channel_id=candidate.channel_id,
                        response_bytes=observer.response_bytes,
                    )
                return Response(status_code=204)
            observer.set_content_encoding(upstream.headers.get("content-encoding"))
            error_kind, countable = classify_http_status(upstream.status_code)
            if upstream.is_success:
                try:
                    prelude_chunks, upstream_iterator = await _read_response_prelude(
                        upstream_adapter,
                        upstream,
                        observer,
                        float(
                            runtime.get(
                                "first_token_timeout_seconds",
                                runtime["first_byte_timeout_seconds"],
                            )
                        ),
                    )
                except asyncio.CancelledError:
                    upstream_adapter.finish_observer(observer)
                    body_file.close()
                    await _safe_close_response(upstream)
                    outcome = (
                        "success"
                        if observer.saw_completion or observer.saw_tool_call
                        else "cancelled"
                    )
                    if outcome == "success":
                        _emit_channel_success(telemetry, candidate.channel_id)
                    telemetry.emit(
                        "attempt",
                        _attempt_payload(
                            request_id,
                            candidate,
                            attempt_no,
                            observer,
                            status_code=upstream.status_code,
                            outcome=outcome,
                            error_kind=None,
                            failover_eligible=False,
                            response_started=observer.response_bytes > 0,
                            upstream_protocol=upstream_protocol,
                            upstream_model_id=candidate.model_id,
                        ),
                    )
                    finish_request(
                        final_status_code=upstream.status_code,
                        outcome=outcome,
                        attempt_count=attempt_no,
                        final_channel_id=candidate.channel_id,
                        response_bytes=observer.response_bytes,
                    )
                    return Response(status_code=204)
                except Exception as exc:
                    await _safe_close_response(upstream)
                    last_error_kind = classify_exception(exc)
                    async with app.state.db.sessions() as session:
                        opened = await record_failure(
                            session,
                            candidate.channel_id,
                            last_error_kind,
                            None,
                            True,
                            int(runtime["failure_threshold"]),
                            int(runtime["circuit_open_seconds"]),
                        )
                    if opened:
                        app.state.health_supervisor.reschedule()
                    upstream_adapter.finish_observer(observer)
                    telemetry.emit(
                        "attempt",
                        _attempt_payload(
                            request_id,
                            candidate,
                            attempt_no,
                            observer,
                            status_code=None,
                            outcome="transport_error",
                            error_kind=last_error_kind,
                            failover_eligible=attempt_no < len(candidates),
                            response_started=False,
                            upstream_protocol=upstream_protocol,
                            upstream_model_id=candidate.model_id,
                        ),
                    )
                    continue

                if observer.stream_error_status_code is not None:
                    await _safe_close_response(upstream)
                    upstream_adapter.finish_observer(observer)
                    status_code = observer.stream_error_status_code
                    stream_error_kind, stream_error_countable = classify_http_status(status_code)
                    raw_error = b"".join(prelude_chunks)
                    last_http_error = (
                        status_code,
                        _mapped_response_headers(upstream.headers, False),
                        convert_mapped_error_response(entry_protocol, upstream_protocol, raw_error),
                        candidate.channel_id,
                    )
                    last_error_kind = stream_error_kind
                    async with app.state.db.sessions() as session:
                        opened = await record_failure(
                            session,
                            candidate.channel_id,
                            stream_error_kind,
                            status_code,
                            stream_error_countable,
                            int(runtime["failure_threshold"]),
                            int(runtime["circuit_open_seconds"]),
                        )
                    if opened:
                        app.state.health_supervisor.reschedule()
                    telemetry.emit(
                        "attempt",
                        _attempt_payload(
                            request_id,
                            candidate,
                            attempt_no,
                            observer,
                            status_code=status_code,
                            outcome="http_error",
                            error_kind=stream_error_kind,
                            failover_eligible=attempt_no < len(candidates),
                            response_started=False,
                            upstream_protocol=upstream_protocol,
                            upstream_model_id=candidate.model_id,
                        ),
                    )
                    continue

                if not stream:
                    error_body_parts = list(prelude_chunks)
                    try:
                        async for chunk in upstream_iterator:
                            observer.observe_bytes(chunk)
                            error_body_parts.append(chunk)
                    except asyncio.CancelledError:
                        upstream_adapter.finish_observer(observer)
                        body_file.close()
                        await _safe_close_response(upstream)
                        telemetry.emit(
                            "attempt",
                            _attempt_payload(
                                request_id,
                                candidate,
                                attempt_no,
                                observer,
                                status_code=upstream.status_code,
                                outcome="cancelled",
                                error_kind=None,
                                failover_eligible=False,
                                response_started=observer.response_bytes > 0,
                                upstream_protocol=upstream_protocol,
                                upstream_model_id=candidate.model_id,
                            ),
                        )
                        if not request_finished:
                            finish_request(
                                final_status_code=upstream.status_code,
                                outcome="cancelled",
                                attempt_count=attempt_no,
                                final_channel_id=candidate.channel_id,
                                response_bytes=observer.response_bytes,
                            )
                        return Response(status_code=204)
                    await _safe_close_response(upstream)
                    upstream_adapter.finish_observer(observer)
                    raw_success = b"".join(error_body_parts)
                    try:
                        converted = convert_mapped_response(
                            entry_protocol, upstream_protocol, entry_model_id, raw_success
                        )
                    except ValueError as exc:
                        converted = _mapped_error_body(entry_protocol, str(exc))
                        _emit_channel_failure(
                            telemetry,
                            candidate.channel_id,
                            "conversion_error",
                            None,
                            False,
                            runtime,
                        )
                    else:
                        _emit_channel_success(telemetry, candidate.channel_id)
                    telemetry.emit(
                        "attempt",
                        _attempt_payload(
                            request_id,
                            candidate,
                            attempt_no,
                            observer,
                            status_code=200,
                            outcome="success",
                            error_kind=None,
                            failover_eligible=False,
                            response_started=True,
                            upstream_protocol=upstream_protocol,
                            upstream_model_id=candidate.model_id,
                        ),
                    )
                    finish_request(
                        final_status_code=200,
                        outcome="success",
                        attempt_count=attempt_no,
                        final_channel_id=candidate.channel_id,
                        response_bytes=len(converted),
                    )
                    body_file.close()
                    return Response(
                        content=converted,
                        status_code=200,
                        headers=_mapped_response_headers(upstream.headers, False),
                    )

                converter = get_mapped_streaming_converter(
                    entry_protocol, upstream_protocol, entry_model_id
                )

                async def stream_response(
                    response=upstream,
                    selected=candidate,
                    selected_attempt_no=attempt_no,
                    selected_observer=observer,
                    buffered_chunks=prelude_chunks,
                    raw_iterator=upstream_iterator,
                    selected_converter=converter,
                ):
                    outcome = "success"
                    stream_error = None
                    stream_error_recorded = False
                    final_status_code = response.status_code
                    try:
                        for chunk in buffered_chunks:
                            converted = selected_converter.feed(chunk)
                            if converted:
                                yield converted
                        async for chunk in raw_iterator:
                            upstream_adapter.observe_chunk(selected_observer, chunk)
                            if selected_observer.stream_error_status_code is not None:
                                final_status_code = (
                                    selected_observer.stream_error_status_code
                                )
                                stream_error, stream_error_countable = classify_http_status(
                                    final_status_code
                                )
                                outcome = "upstream_error"
                                _emit_channel_failure(
                                    telemetry,
                                    selected.channel_id,
                                    stream_error or "upstream_error",
                                    final_status_code,
                                    stream_error_countable,
                                    runtime,
                                )
                                stream_error_recorded = True
                                error_event = selected_converter.error_event(
                                    stream_error or "upstream_error"
                                )
                                if error_event:
                                    yield error_event
                                break
                            converted = selected_converter.feed(chunk)
                            if converted:
                                yield converted
                        tail = selected_converter.flush()
                        if tail:
                            yield tail
                    except asyncio.CancelledError:
                        if selected_observer.saw_completion or selected_observer.saw_tool_call:
                            outcome = "success"
                            stream_error = None
                            final_status_code = response.status_code
                        else:
                            outcome = "cancelled"
                            stream_error = None
                            final_status_code = response.status_code
                    except Exception as exc:
                        outcome = "stream_interrupted"
                        stream_error = classify_exception(exc)
                        final_status_code = 504 if "timeout" in stream_error else 502
                        _emit_channel_failure(
                            telemetry,
                            selected.channel_id,
                            stream_error,
                            final_status_code,
                            True,
                            runtime,
                        )
                        raise
                    finally:
                        body_file.close()
                        upstream_adapter.finish_observer(selected_observer)
                        if selected_observer.stream_error_status_code is not None:
                            final_status_code = selected_observer.stream_error_status_code
                            stream_error, stream_error_countable = classify_http_status(
                                final_status_code
                            )
                            if outcome == "success":
                                outcome = "upstream_error"
                        else:
                            stream_error_countable = False
                        if outcome == "success":
                            _emit_channel_success(telemetry, selected.channel_id)
                        elif outcome == "upstream_error" and not stream_error_recorded:
                            _emit_channel_failure(
                                telemetry,
                                selected.channel_id,
                                stream_error or "upstream_error",
                                final_status_code,
                                stream_error_countable,
                                runtime,
                            )
                        telemetry.emit(
                            "attempt",
                            _attempt_payload(
                                request_id,
                                selected,
                                selected_attempt_no,
                                selected_observer,
                                status_code=final_status_code,
                                outcome=outcome,
                                error_kind=stream_error,
                                failover_eligible=False,
                                response_started=True,
                                upstream_protocol=upstream_protocol,
                                upstream_model_id=selected.model_id,
                            ),
                        )
                        finish_request(
                            final_status_code=final_status_code,
                            outcome=outcome,
                            attempt_count=selected_attempt_no,
                            final_channel_id=selected.channel_id,
                            response_bytes=selected_observer.response_bytes,
                        )
                        await _safe_close_response(response)

                return StreamingResponse(
                    stream_response(),
                    status_code=upstream.status_code,
                    headers=_mapped_response_headers(upstream.headers, True),
                )

            error_body_parts: list[bytes] = []
            try:
                async for chunk in upstream.aiter_raw():
                    observer.observe_bytes(chunk)
                    error_body_parts.append(chunk)
            except asyncio.CancelledError:
                upstream_adapter.finish_observer(observer)
                body_file.close()
                await _safe_close_response(upstream)
                telemetry.emit(
                    "attempt",
                    _attempt_payload(
                        request_id,
                        candidate,
                        attempt_no,
                        observer,
                        status_code=upstream.status_code,
                        outcome="cancelled",
                        error_kind=None,
                        failover_eligible=False,
                        response_started=observer.response_bytes > 0,
                        upstream_protocol=upstream_protocol,
                        upstream_model_id=candidate.model_id,
                    ),
                )
                if not request_finished:
                    finish_request(
                        final_status_code=upstream.status_code,
                        outcome="cancelled",
                        attempt_count=attempt_no,
                        final_channel_id=candidate.channel_id,
                        response_bytes=observer.response_bytes,
                    )
                return Response(status_code=204)
            await _safe_close_response(upstream)
            upstream_adapter.finish_observer(observer)
            raw_error = b"".join(error_body_parts)
            last_http_error = (
                upstream.status_code,
                _mapped_response_headers(upstream.headers, False),
                convert_mapped_error_response(entry_protocol, upstream_protocol, raw_error),
                candidate.channel_id,
            )
            last_error_kind = error_kind
            async with app.state.db.sessions() as session:
                opened = await record_failure(
                    session,
                    candidate.channel_id,
                    error_kind,
                    upstream.status_code,
                    countable,
                    int(runtime["failure_threshold"]),
                    int(runtime["circuit_open_seconds"]),
                )
            if opened:
                app.state.health_supervisor.reschedule()
            telemetry.emit(
                "attempt",
                _attempt_payload(
                    request_id,
                    candidate,
                    attempt_no,
                    observer,
                    status_code=upstream.status_code,
                    outcome="http_error",
                    error_kind=error_kind,
                    failover_eligible=attempt_no < len(candidates),
                    response_started=False,
                    upstream_protocol=upstream_protocol,
                    upstream_model_id=candidate.model_id,
                ),
            )
        except httpx.HTTPError as exc:
            last_error_kind = classify_exception(exc)
            async with app.state.db.sessions() as session:
                opened = await record_failure(
                    session,
                    candidate.channel_id,
                    last_error_kind,
                    None,
                    True,
                    int(runtime["failure_threshold"]),
                    int(runtime["circuit_open_seconds"]),
                )
            if opened:
                app.state.health_supervisor.reschedule()
            upstream_adapter.finish_observer(observer)
            telemetry.emit(
                "attempt",
                _attempt_payload(
                    request_id,
                    candidate,
                    attempt_no,
                    observer,
                    status_code=None,
                    outcome="transport_error",
                    error_kind=last_error_kind,
                    failover_eligible=attempt_no < len(candidates),
                    response_started=False,
                    upstream_protocol=upstream_protocol,
                    upstream_model_id=candidate.model_id,
                ),
            )

    if last_http_error:
        body_file.close()
        status, headers, raw_body, final_channel_id = last_http_error
        finish_request(
            final_status_code=status,
            outcome="upstream_error",
            attempt_count=attempts,
            final_channel_id=final_channel_id,
            response_bytes=len(raw_body),
        )
        return Response(content=raw_body, status_code=status, headers=headers)

    status = 504 if "timeout" in last_error_kind else 502
    body_file.close()
    finish_request(
        final_status_code=status,
        outcome="gateway_error",
        attempt_count=attempts,
        response_bytes=0,
    )
    return _gateway_error(
        entry_protocol,
        status,
        "upstream_unreachable",
        "All eligible upstream channels failed before a response was available.",
        request_id,
    )


async def gateway_access_error(
    request: Request, protocol: str, request_id: str | None = None
):
    access_policy = await resolve_access_policy(request.app)
    supplied_credential = _supplied_credential(request, protocol)
    if access_policy["trust_local_network"]:
        return None
    if supplied_credential and stdlib_secrets.compare_digest(
        supplied_credential, access_policy["gateway_key"]
    ):
        return None
    return _gateway_error(
        "openai_compatible" if protocol == "catalog" else protocol,
        401,
        "unauthorized",
        "Invalid local gateway key.",
        request_id or str(uuid.uuid4()),
    )
