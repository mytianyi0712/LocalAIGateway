import asyncio
from contextlib import suppress
from collections.abc import Callable
from typing import Any

from sqlalchemy import update

from app.db.models import RequestAttempt, RequestLog
from app.services.circuit_breaker import record_failure, record_success


class TelemetryWriter:
    def __init__(
        self,
        session_factory,
        queue_size: int = 1000,
        on_circuit_open: Callable[[], None] | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.on_circuit_open = on_circuit_open
        self.queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue(queue_size)
        self.dropped = 0
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="telemetry-writer")

    async def stop(self) -> None:
        await self.queue.join()
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

    def emit(self, event: str, payload: dict[str, Any]) -> None:
        try:
            self.queue.put_nowait((event, payload))
        except asyncio.QueueFull:
            self.dropped += 1

    async def _run(self) -> None:
        while True:
            event, payload = await self.queue.get()
            try:
                async with self.session_factory() as session:
                    if event == "request_start":
                        session.add(RequestLog(**payload))
                    elif event == "request_finish":
                        request_id = payload.pop("id")
                        await session.execute(
                            update(RequestLog).where(RequestLog.id == request_id).values(**payload)
                        )
                    elif event == "attempt":
                        session.add(RequestAttempt(**payload))
                    elif event == "channel_success":
                        await record_success(session, payload["channel_id"])
                    elif event == "channel_failure":
                        if await record_failure(session, **payload) and self.on_circuit_open:
                            self.on_circuit_open()
                    await session.commit()
            except Exception:
                self.dropped += 1
            finally:
                self.queue.task_done()
