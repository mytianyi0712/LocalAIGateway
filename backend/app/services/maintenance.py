import asyncio
from contextlib import suppress
from datetime import timedelta

from sqlalchemy import delete, func, select
from sqlalchemy.orm import selectinload

from app.db.models import (
    Channel,
    DiscoveryRun,
    HealthProbeLog,
    RequestAttempt,
    RequestLog,
    utcnow,
)
from app.services.circuit_breaker import as_utc
from app.services.discovery import discover_channel_models
from app.services.settings import get_runtime_settings


async def reconcile_completed_stream_cancellations(session_factory) -> int:
    """Repair legacy cancellations that already captured a completed stream's usage."""
    async with session_factory() as session:
        rows = (
            (
                await session.execute(
                    select(RequestLog)
                    .options(selectinload(RequestLog.attempts))
                    .where(
                        RequestLog.outcome == "cancelled",
                        RequestLog.final_status_code == 200,
                        RequestLog.response_bytes > 0,
                    )
                )
            )
            .scalars()
            .all()
        )
        repaired = 0
        for row in rows:
            if not row.attempts:
                continue
            final_attempt = max(row.attempts, key=lambda attempt: attempt.attempt_no)
            if not (
                final_attempt.outcome == "cancelled"
                and final_attempt.status_code == 200
                and final_attempt.response_started
                and final_attempt.raw_usage_json is not None
                and final_attempt.output_tokens is not None
            ):
                continue
            row.outcome = "success"
            final_attempt.outcome = "success"
            repaired += 1
        if repaired:
            await session.commit()
        return repaired


class MaintenanceSupervisor:
    def __init__(self, app, interval_seconds: int = 60) -> None:
        self.app = app
        self.interval_seconds = interval_seconds
        self._task: asyncio.Task | None = None
        self._discovering: set[str] = set()
        self._last_cleanup = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="maintenance-supervisor")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

    async def _run(self) -> None:
        while True:
            try:
                await self._schedule_discovery()
                await self._finalize_stale_pending_requests()
                await self._cleanup_logs()
            except Exception:
                pass
            await asyncio.sleep(self.interval_seconds)

    async def _schedule_discovery(self) -> None:
        now = utcnow()
        async with self.app.state.db.sessions() as session:
            runtime = await get_runtime_settings(session)
            cutoff = now - timedelta(hours=int(runtime["model_discovery_interval_hours"]))
            channels = (
                (await session.execute(select(Channel.id).where(Channel.manual_enabled.is_(True))))
                .scalars()
                .all()
            )
            due: list[str] = []
            for channel_id in channels:
                last_started = await session.scalar(
                    select(func.max(DiscoveryRun.started_at)).where(
                        DiscoveryRun.channel_id == channel_id
                    )
                )
                if last_started is None or as_utc(last_started) <= cutoff:
                    due.append(channel_id)
            runs: list[tuple[str, str]] = []
            for channel_id in due:
                if channel_id in self._discovering:
                    continue
                run = DiscoveryRun(channel_id=channel_id, trigger="scheduled")
                session.add(run)
                await session.flush()
                runs.append((channel_id, run.id))
                self._discovering.add(channel_id)
            await session.commit()
        for channel_id, run_id in runs:
            asyncio.create_task(self._run_discovery(channel_id, run_id))

    async def _run_discovery(self, channel_id: str, run_id: str) -> None:
        try:
            await discover_channel_models(
                self.app.state.db.sessions,
                self.app.state.http,
                self.app.state.secrets,
                channel_id,
                run_id,
            )
        finally:
            self._discovering.discard(channel_id)

    async def _finalize_stale_pending_requests(self) -> None:
        now = utcnow()
        async with self.app.state.db.sessions() as session:
            runtime = await get_runtime_settings(session)
            stale_seconds = max(
                int(runtime["stream_idle_timeout_seconds"]) * 2,
                int(runtime["first_byte_timeout_seconds"]) * 2,
                600,
            )
            cutoff = now - timedelta(seconds=stale_seconds)
            rows = (
                (
                    await session.execute(
                        select(RequestLog).where(
                            RequestLog.outcome == "pending",
                            RequestLog.started_at < cutoff,
                        )
                    )
                )
                .scalars()
                .all()
            )
            for row in rows:
                started_at = as_utc(row.started_at)
                row.finished_at = now
                row.total_duration_ms = (
                    round((now - started_at).total_seconds() * 1000) if started_at else None
                )
                row.outcome = "cancelled"
                row.response_bytes = row.response_bytes or 0
            await session.commit()

    async def _cleanup_logs(self) -> None:
        now = utcnow()
        if self._last_cleanup and now - self._last_cleanup < timedelta(hours=1):
            return
        async with self.app.state.db.sessions() as session:
            runtime = await get_runtime_settings(session)
            cutoff = now - timedelta(days=int(runtime["log_retention_days"]))
            expired_requests = select(RequestLog.id).where(RequestLog.started_at < cutoff)
            await session.execute(
                delete(RequestAttempt).where(RequestAttempt.request_id.in_(expired_requests))
            )
            await session.execute(delete(RequestLog).where(RequestLog.started_at < cutoff))
            await session.execute(delete(HealthProbeLog).where(HealthProbeLog.started_at < cutoff))
            await session.execute(delete(DiscoveryRun).where(DiscoveryRun.started_at < cutoff))
            await session.commit()
        self._last_cleanup = now
