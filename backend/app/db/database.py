from collections.abc import AsyncIterator

from sqlalchemy import event, inspect, select, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import Settings
from app.adapters.base import SHARED_DISCOVERY_PROTOCOL_GROUPS, normalize_base_url
from app.db.models import Base, Channel, ChannelModel, ChannelModelProtocol, ChannelProtocol, Provider


class Database:
    def __init__(self, settings: Settings) -> None:
        self.engine: AsyncEngine = create_async_engine(
            settings.resolved_database_url,
            pool_pre_ping=True,
        )
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)

        if settings.resolved_database_url.startswith("sqlite"):
            event.listen(self.engine.sync_engine, "connect", self._configure_sqlite)

    @staticmethod
    def _configure_sqlite(dbapi_connection, _connection_record) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA busy_timeout=5000")
        cursor.close()

    async def initialize(self) -> None:
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        await self._ensure_schema_columns()
        await self._backfill_protocol_bindings()

    async def _ensure_schema_columns(self) -> None:
        # create_all 不会给已存在的表补列；对旧库做幂等 ALTER。
        async with self.engine.begin() as connection:
            columns = await connection.run_sync(
                lambda sync_conn: {
                    column["name"] for column in inspect(sync_conn).get_columns("model_caps")
                }
            )
            if "profile_id" not in columns:
                await connection.execute(
                    text(
                        "ALTER TABLE model_caps ADD COLUMN profile_id VARCHAR "
                        "REFERENCES capability_profiles(id) ON DELETE SET NULL"
                    )
                )

    async def _backfill_protocol_bindings(self) -> None:
        async with self.sessions() as session:
            channels = (await session.execute(select(Channel))).scalars().all()
            channel_protocols = {
                (row.channel_id, row.protocol)
                for row in (await session.execute(select(ChannelProtocol))).scalars()
            }
            model_protocols = {
                (row.channel_model_id, row.protocol)
                for row in (await session.execute(select(ChannelModelProtocol))).scalars()
            }
            for channel in channels:
                key = (channel.id, channel.protocol)
                if key not in channel_protocols:
                    session.add(ChannelProtocol(channel_id=channel.id, protocol=channel.protocol))
                    channel_protocols.add(key)
                enabled_protocols = {
                    protocol
                    for channel_id, protocol in channel_protocols
                    if channel_id == channel.id
                }
                model_ids = (
                    await session.execute(
                        select(ChannelModel.id).where(ChannelModel.channel_id == channel.id)
                    )
                ).scalars()
                for model_id in model_ids:
                    bound_protocols = {
                        protocol
                        for channel_model_id, protocol in model_protocols
                        if channel_model_id == model_id
                    }
                    if not bound_protocols:
                        session.add(
                            ChannelModelProtocol(
                                channel_model_id=model_id, protocol=channel.protocol
                            )
                        )
                        model_protocols.add((model_id, channel.protocol))
                        bound_protocols.add(channel.protocol)
                    for group in SHARED_DISCOVERY_PROTOCOL_GROUPS:
                        if not bound_protocols & group:
                            continue
                        for protocol in (enabled_protocols & group) - bound_protocols:
                            session.add(
                                ChannelModelProtocol(
                                    channel_model_id=model_id,
                                    protocol=protocol,
                                )
                            )
                            model_protocols.add((model_id, protocol))
            providers = (await session.execute(select(Provider))).scalars().all()
            for provider in providers:
                provider.base_url = normalize_base_url(provider.base_url)
            await session.commit()

    async def close(self) -> None:
        await self.engine.dispose()

    async def session(self) -> AsyncIterator[AsyncSession]:
        async with self.sessions() as session:
            yield session
