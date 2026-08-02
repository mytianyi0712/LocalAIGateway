import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    LargeBinary,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def uuid4() -> str:
    return str(uuid.uuid4())


class Base(DeclarativeBase):
    pass


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class Provider(Base, TimestampMixin):
    __tablename__ = "providers"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    base_url: Mapped[str] = mapped_column(String, nullable=False)
    channels: Mapped[list["Channel"]] = relationship(back_populates="provider")


class Channel(Base, TimestampMixin):
    __tablename__ = "channels"
    __table_args__ = (UniqueConstraint("provider_id", "name"),)

    id: Mapped[str] = mapped_column(String, primary_key=True, default=uuid4)
    provider_id: Mapped[str] = mapped_column(ForeignKey("providers.id"), nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    protocol: Mapped[str] = mapped_column(String, nullable=False)
    api_key_encrypted: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    api_key_hint: Mapped[str] = mapped_column(String, nullable=False)
    manual_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    health_check_model_id: Mapped[str | None] = mapped_column(String)

    provider: Mapped[Provider] = relationship(back_populates="channels")
    health: Mapped["ChannelHealth"] = relationship(
        back_populates="channel", cascade="all, delete-orphan", uselist=False
    )
    models: Mapped[list["ChannelModel"]] = relationship(
        back_populates="channel", cascade="all, delete-orphan"
    )
    protocol_bindings: Mapped[list["ChannelProtocol"]] = relationship(
        back_populates="channel", cascade="all, delete-orphan"
    )


class ChannelProtocol(Base):
    __tablename__ = "channel_protocols"

    channel_id: Mapped[str] = mapped_column(
        ForeignKey("channels.id", ondelete="CASCADE"), primary_key=True
    )
    protocol: Mapped[str] = mapped_column(String, primary_key=True)

    channel: Mapped[Channel] = relationship(back_populates="protocol_bindings")


class ChannelHealth(Base):
    __tablename__ = "channel_health"

    channel_id: Mapped[str] = mapped_column(
        ForeignKey("channels.id", ondelete="CASCADE"), primary_key=True
    )
    state: Mapped[str] = mapped_column(String, default="active", nullable=False)
    consecutive_failures: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    disabled_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_failure_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error_kind: Mapped[str | None] = mapped_column(String)
    last_status_code: Mapped[int | None] = mapped_column(Integer)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )

    channel: Mapped[Channel] = relationship(back_populates="health")


class ChannelModel(Base, TimestampMixin):
    __tablename__ = "channel_models"
    __table_args__ = (UniqueConstraint("channel_id", "model_id"),)

    id: Mapped[str] = mapped_column(String, primary_key=True, default=uuid4)
    channel_id: Mapped[str] = mapped_column(
        ForeignKey("channels.id", ondelete="CASCADE"), nullable=False
    )
    model_id: Mapped[str] = mapped_column(String, nullable=False)
    display_name: Mapped[str | None] = mapped_column(String)
    source: Mapped[str] = mapped_column(String, default="discovered", nullable=False)
    available: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    metadata_json: Mapped[dict | None] = mapped_column(JSON)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=utcnow)

    channel: Mapped[Channel] = relationship(back_populates="models")
    candidates: Mapped[list["RouteCandidate"]] = relationship(back_populates="channel_model")
    protocol_bindings: Mapped[list["ChannelModelProtocol"]] = relationship(
        back_populates="channel_model", cascade="all, delete-orphan"
    )


class ChannelModelProtocol(Base):
    __tablename__ = "channel_model_protocols"

    channel_model_id: Mapped[str] = mapped_column(
        ForeignKey("channel_models.id", ondelete="CASCADE"), primary_key=True
    )
    protocol: Mapped[str] = mapped_column(String, primary_key=True)

    channel_model: Mapped[ChannelModel] = relationship(back_populates="protocol_bindings")


class ModelRoute(Base, TimestampMixin):
    __tablename__ = "model_routes"
    __table_args__ = (UniqueConstraint("protocol", "requested_model_id"),)

    id: Mapped[str] = mapped_column(String, primary_key=True, default=uuid4)
    protocol: Mapped[str] = mapped_column(String, nullable=False)
    requested_model_id: Mapped[str] = mapped_column(String, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    candidates: Mapped[list["RouteCandidate"]] = relationship(
        back_populates="route", cascade="all, delete-orphan"
    )


class ClaudeModelMapping(Base, TimestampMixin):
    """A Claude-standard model name exposed to clients, routed to an existing
    system model with optional protocol conversion (see app/adapters/convert.py).

    The mapping inherits candidates from the *existing* route of
    ``upstream_model_id`` in ``upstream_protocol``, so no separate candidate
    configuration is needed.
    """

    __tablename__ = "claude_model_mappings"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=uuid4)
    claude_model_id: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    display_name: Mapped[str | None] = mapped_column(String)
    upstream_protocol: Mapped[str] = mapped_column(String, nullable=False)
    upstream_model_id: Mapped[str] = mapped_column(String, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class CodexModelMapping(Base, TimestampMixin):
    """A Codex-standard model name (OpenAI Responses API) exposed to clients,
    routed to an existing system model with optional protocol conversion
    (see app/adapters/convert.py).

    The mapping inherits candidates from the *existing* route of
    ``upstream_model_id`` in ``upstream_protocol``, so no separate candidate
    configuration is needed.
    """

    __tablename__ = "codex_model_mappings"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=uuid4)
    codex_model_id: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    display_name: Mapped[str | None] = mapped_column(String)
    upstream_protocol: Mapped[str] = mapped_column(String, nullable=False)
    upstream_model_id: Mapped[str] = mapped_column(String, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class CapabilityProfile(Base, TimestampMixin):
    """A reusable, named set of model capabilities.

    Multiple models (``model_caps`` rows) can reference the same profile via
    ``profile_id``, so e.g. GPT-5.6 Sol and GPT-5.6 Terra share one capability
    set. Editing a profile propagates the capability fields to every model
    that references it; costs stay per-model because they are pricing, not
    capability.
    """

    __tablename__ = "capability_profiles"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    description: Mapped[str | None] = mapped_column(String)
    context_window: Mapped[int | None] = mapped_column(Integer)
    max_tokens: Mapped[int | None] = mapped_column(Integer)
    supports_image_input: Mapped[bool | None] = mapped_column(Boolean)
    reasoning: Mapped[bool | None] = mapped_column(Boolean)
    thinking_level_map: Mapped[dict | None] = mapped_column(JSON)


class ModelCaps(Base, TimestampMixin):
    __tablename__ = "model_caps"

    requested_model_id: Mapped[str] = mapped_column(String, primary_key=True)
    context_window: Mapped[int | None] = mapped_column(Integer)
    max_tokens: Mapped[int | None] = mapped_column(Integer)
    supports_image_input: Mapped[bool | None] = mapped_column(Boolean)
    reasoning: Mapped[bool | None] = mapped_column(Boolean)
    thinking_level_map: Mapped[dict | None] = mapped_column(JSON)
    cost_input: Mapped[float | None] = mapped_column(Float)
    cost_output: Mapped[float | None] = mapped_column(Float)
    cost_cache_read: Mapped[float | None] = mapped_column(Float)
    cost_cache_write: Mapped[float | None] = mapped_column(Float)
    source: Mapped[str] = mapped_column(String, default="auto", nullable=False)
    profile_id: Mapped[str | None] = mapped_column(
        ForeignKey("capability_profiles.id", ondelete="SET NULL")
    )


class RouteCandidate(Base, TimestampMixin):
    __tablename__ = "route_candidates"
    __table_args__ = (
        UniqueConstraint("route_id", "channel_model_id"),
        UniqueConstraint("route_id", "priority"),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True, default=uuid4)
    route_id: Mapped[str] = mapped_column(
        ForeignKey("model_routes.id", ondelete="CASCADE"), nullable=False
    )
    channel_model_id: Mapped[str] = mapped_column(
        ForeignKey("channel_models.id", ondelete="RESTRICT"), nullable=False
    )
    priority: Mapped[int] = mapped_column(Integer, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    route: Mapped[ModelRoute] = relationship(back_populates="candidates")
    channel_model: Mapped[ChannelModel] = relationship(back_populates="candidates")


class RequestLog(Base):
    __tablename__ = "request_logs"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=uuid4)
    protocol: Mapped[str] = mapped_column(String, nullable=False)
    model_id: Mapped[str | None] = mapped_column(String)
    endpoint: Mapped[str] = mapped_column(String, nullable=False)
    stream: Mapped[bool | None] = mapped_column(Boolean)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    total_duration_ms: Mapped[int | None] = mapped_column(Integer)
    final_status_code: Mapped[int | None] = mapped_column(Integer)
    outcome: Mapped[str] = mapped_column(String, default="pending", nullable=False)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    final_channel_id: Mapped[str | None] = mapped_column(
        ForeignKey("channels.id", ondelete="SET NULL")
    )
    request_bytes: Mapped[int | None] = mapped_column(Integer)
    response_bytes: Mapped[int | None] = mapped_column(Integer)
    attempts: Mapped[list["RequestAttempt"]] = relationship(
        back_populates="request", cascade="all, delete-orphan"
    )


class RequestAttempt(Base):
    __tablename__ = "request_attempts"
    __table_args__ = (UniqueConstraint("request_id", "attempt_no"),)

    id: Mapped[str] = mapped_column(String, primary_key=True, default=uuid4)
    request_id: Mapped[str] = mapped_column(
        ForeignKey("request_logs.id", ondelete="CASCADE"), nullable=False
    )
    channel_id: Mapped[str | None] = mapped_column(ForeignKey("channels.id", ondelete="SET NULL"))
    channel_name: Mapped[str] = mapped_column(String, nullable=False)
    attempt_no: Mapped[int] = mapped_column(Integer, nullable=False)
    priority_snapshot: Mapped[int] = mapped_column(Integer, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status_code: Mapped[int | None] = mapped_column(Integer)
    outcome: Mapped[str] = mapped_column(String, nullable=False)
    error_kind: Mapped[str | None] = mapped_column(String)
    failover_eligible: Mapped[bool] = mapped_column(Boolean, default=False)
    response_started: Mapped[bool] = mapped_column(Boolean, default=False)
    first_byte_ms: Mapped[int | None] = mapped_column(Integer)
    first_token_ms: Mapped[int | None] = mapped_column(Integer)
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    input_tokens: Mapped[int | None] = mapped_column(Integer)
    cache_read_tokens: Mapped[int | None] = mapped_column(Integer)
    cache_write_tokens: Mapped[int | None] = mapped_column(Integer)
    cache_miss_input_tokens: Mapped[int | None] = mapped_column(Integer)
    output_tokens: Mapped[int | None] = mapped_column(Integer)
    tps: Mapped[float | None] = mapped_column(Float)
    raw_usage_json: Mapped[dict | None] = mapped_column(JSON)
    response_bytes: Mapped[int | None] = mapped_column(Integer)
    upstream_protocol: Mapped[str | None] = mapped_column(String)
    upstream_model_id: Mapped[str | None] = mapped_column(String)

    request: Mapped[RequestLog] = relationship(back_populates="attempts")


class AppSetting(Base):
    __tablename__ = "settings"

    key: Mapped[str] = mapped_column(String, primary_key=True)
    value_json: Mapped[dict | int | float | str | bool] = mapped_column(JSON, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class DiscoveryRun(Base):
    __tablename__ = "discovery_runs"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=uuid4)
    channel_id: Mapped[str] = mapped_column(
        ForeignKey("channels.id", ondelete="CASCADE"), nullable=False
    )
    trigger: Mapped[str] = mapped_column(String, default="manual", nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    success: Mapped[bool | None] = mapped_column(Boolean)
    model_count: Mapped[int | None] = mapped_column(Integer)
    status_code: Mapped[int | None] = mapped_column(Integer)
    error_kind: Mapped[str | None] = mapped_column(String)


class HealthProbeLog(Base):
    __tablename__ = "health_probe_logs"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=uuid4)
    channel_id: Mapped[str] = mapped_column(
        ForeignKey("channels.id", ondelete="CASCADE"), nullable=False
    )
    model_id: Mapped[str] = mapped_column(String, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    success: Mapped[bool] = mapped_column(Boolean, nullable=False)
    status_code: Mapped[int | None] = mapped_column(Integer)
    error_kind: Mapped[str | None] = mapped_column(String)
    next_probe_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
