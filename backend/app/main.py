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
    def _is_api_request(self, path: str) -> bool:
        # 管理/代理 API 路径必须返回真实错误，不能被 SPA 回退吞掉：
        # 否则旧后端进程（缺路由）会返回 200 + index.html，前端把 HTML
        # 当 JSON 解析后报出 “Cannot read properties of null” 这类误导性错误。
        return (
            path == "api"
            or path.startswith("api/")
            or path == "v1"
            or path.startswith("v1/")
            or path == "v1beta"
            or path.startswith("v1beta/")
            or path == "claudecode"
            or path.startswith("claudecode/")
            or path == "codex"
            or path.startswith("codex/")
        )

    async def get_response(self, path: str, scope):
        try:
            response = await super().get_response(path, scope)
        except StarletteHTTPException as exc:
            if (
                exc.status_code == 404
                and not self._is_api_request(path)
                and "." not in Path(path).name
            ):
                return FileResponse(Path(self.directory) / "index.html")
            raise
        if (
            response.status_code == 404
            and not self._is_api_request(path)
            and "." not in Path(path).name
        ):
            return FileResponse(Path(self.directory) / "index.html")
        # 前端为原生静态文件且经常更新：禁止缓存，确保浏览器总是拿到最新版本。
        if path.startswith("assets/") or path == "assets/app.js":
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        return response


app = FastAPI(title="Local AI Gateway", version="0.2.0-fix1", lifespan=lifespan)
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
