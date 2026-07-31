import asyncio
import time
from contextlib import suppress
from datetime import timedelta

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.adapters import get_adapter
from app.adapters.base import StreamObserver
from app.db.models import Channel, ChannelHealth, ChannelModel, HealthProbeLog, utcnow
from app.services.circuit_breaker import (
    as_utc,
    classify_exception,
    classify_http_status,
    record_failure,
    record_success,
)
from app.services.settings import get_runtime_settings


async def probe_channel(app, channel_id: str) -> bool:
    perf_started = time.perf_counter()
    async with app.state.db.sessions() as session:
        statement = (
            select(Channel)
            .options(
                selectinload(Channel.provider),
                selectinload(Channel.protocol_bindings),
                selectinload(Channel.models).selectinload(ChannelModel.protocol_bindings),
            )
            .where(Channel.id == channel_id)
        )
        channel = (await session.execute(statement)).scalar_one_or_none()
        if not channel or not channel.manual_enabled:
            return False
        model_id = channel.health_check_model_id
        if not model_id:
            # Match the management API's model-list ordering for the automatic choice.
            model_id = await session.scalar(
                select(ChannelModel.model_id)
                .join(ChannelModel.protocol_bindings)
                .where(
                    ChannelModel.channel_id == channel_id,
                    ChannelModel.available.is_(True),
                    ChannelModel.protocol_bindings.any(protocol=channel.protocol),
                )
                .order_by(ChannelModel.model_id)
                .limit(1)
            )
        if not model_id:
            return False
        runtime = await get_runtime_settings(session)
        adapter = get_adapter(channel.protocol)
        api_key = app.state.secrets.decrypt(channel.api_key_encrypted)
        request = adapter.health_probe(channel.provider.base_url, api_key, model_id)

    status_code = None
    error_kind = None
    success = False
    try:
        response = await app.state.http.send(request, stream=True)
        status_code = response.status_code
        observer = StreamObserver()
        async for chunk in response.aiter_bytes():
            adapter.observe_chunk(observer, chunk)
        adapter.finish_observer(observer)
        success = response.is_success and (observer.saw_completion or observer.first_token_at is not None)
        if not success:
            error_kind, _ = classify_http_status(response.status_code)
    except Exception as exc:
        error_kind = classify_exception(exc)

    async with app.state.db.sessions() as session:
        if success:
            await record_success(session, channel_id)
            next_probe = None
        else:
            await record_failure(
                session,
                channel_id,
                error_kind or "probe_failed",
                status_code,
                True,
                1,
                int(runtime["circuit_open_seconds"]),
            )
            next_probe = utcnow() + timedelta(seconds=int(runtime["circuit_open_seconds"]))
        session.add(
            HealthProbeLog(
                channel_id=channel_id,
                model_id=model_id,
                duration_ms=round((time.perf_counter() - perf_started) * 1000),
                success=success,
                status_code=status_code,
                error_kind=error_kind,
                next_probe_at=next_probe,
            )
        )
        await session.commit()
    app.state.health_supervisor.reschedule()
    return success


class HealthSupervisor:
    def __init__(self, app) -> None:
        self.app = app
        self._task: asyncio.Task | None = None
        self._probing: set[str] = set()
        self._reschedule_event = asyncio.Event()

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="health-supervisor")

    def reschedule(self) -> None:
        """Wake the one-shot scheduler after a circuit state changes."""
        self._reschedule_event.set()

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

    async def _run(self) -> None:
        while True:
            try:
                self._reschedule_event.clear()
                await self._probe_due_channels()
                delay = await self._seconds_until_next_probe()
                if delay is None:
                    await self._reschedule_event.wait()
                else:
                    try:
                        await asyncio.wait_for(self._reschedule_event.wait(), timeout=delay)
                    except TimeoutError:
                        pass
            except Exception:
                # Do not turn a transient database failure into a polling health check.
                await self._reschedule_event.wait()

    async def _seconds_until_next_probe(self) -> float | None:
        now = utcnow()
        async with self.app.state.db.sessions() as session:
            disabled_until = await session.scalar(
                select(ChannelHealth.disabled_until)
                .join(Channel, Channel.id == ChannelHealth.channel_id)
                .where(
                    ChannelHealth.state == "open",
                    Channel.manual_enabled.is_(True),
                    ChannelHealth.disabled_until.is_not(None),
                    ChannelHealth.disabled_until > now,
                )
                .order_by(ChannelHealth.disabled_until)
                .limit(1)
            )
        if not disabled_until:
            return None
        return max(0.0, (as_utc(disabled_until) - now).total_seconds())

    async def _probe_due_channels(self) -> None:
        now = utcnow()
        async with self.app.state.db.sessions() as session:
            rows = (
                await session.execute(
                    select(ChannelHealth.channel_id, ChannelHealth.disabled_until)
                    .join(Channel, Channel.id == ChannelHealth.channel_id)
                    .where(ChannelHealth.state == "open", Channel.manual_enabled.is_(True))
                )
            ).all()
        due = [channel_id for channel_id, until in rows if as_utc(until) and as_utc(until) <= now]
        for channel_id in due:
            if channel_id in self._probing:
                continue
            self._probing.add(channel_id)
            try:
                await probe_channel(self.app, channel_id)
            finally:
                self._probing.discard(channel_id)
