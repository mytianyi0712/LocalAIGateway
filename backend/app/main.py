from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import FileResponse

from app.api.admin import router as admin_router
from app.api.proxy import router as proxy_router
from app.core.config import get_settings
from app.core.security import SecretStore
from app.db.database import Database
from app.services.health import HealthSupervisor
from app.services.maintenance import MaintenanceSupervisor, reconcile_completed_stream_cancellations
from app.services.telemetry import TelemetryWriter


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    settings.ensure_directories()
    app.state.settings = settings
    app.state.secrets = SecretStore(settings.encryption_key_path)
    app.state.db = Database(settings)
    await app.state.db.initialize()
    await reconcile_completed_stream_cancellations(app.state.db.sessions)
    app.state.http = httpx.AsyncClient(
        timeout=httpx.Timeout(connect=10.0, read=300.0, write=60.0, pool=10.0),
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        follow_redirects=False,
    )
    app.state.health_supervisor = HealthSupervisor(app)
    app.state.health_supervisor.start()
    app.state.telemetry = TelemetryWriter(
        app.state.db.sessions,
        settings.log_queue_size,
        on_circuit_open=app.state.health_supervisor.reschedule,
    )
    app.state.telemetry.start()
    app.state.maintenance_supervisor = MaintenanceSupervisor(app)
    app.state.maintenance_supervisor.start()
    yield
    await app.state.maintenance_supervisor.stop()
    await app.state.health_supervisor.stop()
    await app.state.telemetry.stop()
    await app.state.http.aclose()
    await app.state.db.close()


class SPAStaticFiles(StaticFiles):
    async def get_response(self, path: str, scope):
        try:
            response = await super().get_response(path, scope)
        except StarletteHTTPException as exc:
            if exc.status_code == 404 and "." not in Path(path).name:
                return FileResponse(Path(self.directory) / "index.html")
            raise
        if response.status_code == 404 and "." not in Path(path).name:
            return FileResponse(Path(self.directory) / "index.html")
        return response


app = FastAPI(title="Local AI Gateway", version="0.1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=(
        r"^(https?://(localhost|127\.0\.0\.1|\[::1\])(:\d+)?"
        r"|file://.*|onlyoffice://.*|ascdesktop://.*|null)$"
    ),
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)
app.include_router(admin_router)
app.include_router(proxy_router)


@app.get("/api/health")
async def health():
    return {"status": "ok"}


frontend_public = Path(__file__).resolve().parents[2] / "frontend" / "public"
if frontend_public.exists():
    app.mount("/", SPAStaticFiles(directory=frontend_public, html=True), name="frontend")
