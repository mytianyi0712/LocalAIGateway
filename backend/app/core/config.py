from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="AI_GATEWAY_", env_file=".env")

    app_name: str = "Local AI Gateway"
    host: str = "0.0.0.0"
    port: int = 3000
    data_dir: Path = Path("data")
    database_url: str | None = None
    admin_token: str = ""
    gateway_key: str = ""
    log_queue_size: int = 1000
    request_body_limit_bytes: int = 256 * 1024 * 1024

    @property
    def resolved_database_url(self) -> str:
        if self.database_url:
            return self.database_url
        db_path = (self.data_dir / "gateway.db").resolve()
        return f"sqlite+aiosqlite:///{db_path}"

    @property
    def encryption_key_path(self) -> Path:
        return self.data_dir / "master.key"

    def ensure_directories(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)


@lru_cache
def get_settings() -> Settings:
    return Settings()
