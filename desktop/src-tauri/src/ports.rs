//! Application ports (P2-1): the seams between business logic and
//! infrastructure.
//!
//! Handlers and services depend on these traits; the SQLx / reqwest /
//! telemetry implementations live in `infrastructure` (and their historical
//! modules) and are injected through [`crate::application::Context`]. A
//! business module must not import `reqwest` or reach into a concrete
//! repository — the gate in `scripts/check-layers.sh` enforces the
//! direction for the layered modules.

use std::pin::Pin;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::{Stream, StreamExt};
use http::{HeaderMap, StatusCode};
use url::Url;

use crate::telemetry::Event;

pub use crate::routing::{Candidate, MappingTarget, RoutableModel};

/// One upstream HTTP exchange, abstracted so business logic never sees
/// reqwest.
pub struct UpstreamRequest {
    pub url: Url,
    pub headers: HeaderMap,
    /// `Some` sends POST, `None` sends GET.
    pub body: Option<Bytes>,
    /// Client connect timeout for this exchange.
    pub connect_timeout: Duration,
    /// Absolute deadline for the send phase (until response headers).
    pub deadline: Duration,
}

/// Why an upstream exchange failed before any status was usable.
#[derive(Debug, Clone)]
pub enum UpstreamError {
    /// The send phase exceeded [`UpstreamRequest::deadline`].
    Deadline,
    /// The underlying client classified the failure as a connect timeout.
    ConnectTimeout,
    /// Other transport failure (connect refused, DNS, reset).
    Transport(String),
}

/// The upstream response after headers arrived. The body is a byte stream:
/// bounded reads are the caller's responsibility.
pub struct UpstreamResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: UpstreamBody,
}

/// Streaming upstream body. Each item is one network chunk or a transport
/// failure mid-body.
pub struct UpstreamBody {
    inner: Pin<Box<dyn Stream<Item = Result<Bytes, UpstreamError>> + Send>>,
}

impl UpstreamBody {
    pub fn new(inner: Pin<Box<dyn Stream<Item = Result<Bytes, UpstreamError>> + Send>>) -> Self {
        Self { inner }
    }

    pub fn into_stream(self) -> Pin<Box<dyn Stream<Item = Result<Bytes, UpstreamError>> + Send>> {
        self.inner
    }

    /// Reads the whole body, stopping at `cap` bytes (the tail is dropped
    /// with the stream). Returns `(bytes, truncated)`.
    pub async fn read_capped(mut self, cap: usize) -> (Vec<u8>, bool) {
        let mut buf = Vec::with_capacity(cap.min(64 * 1024));
        let mut truncated = false;
        while let Some(item) = self.inner.next().await {
            match item {
                Ok(chunk) => {
                    let remaining = cap - buf.len();
                    if chunk.len() > remaining {
                        buf.extend_from_slice(&chunk[..remaining]);
                        truncated = true;
                        break;
                    }
                    buf.extend_from_slice(&chunk);
                }
                Err(_) => break,
            }
        }
        (buf, truncated)
    }
}

/// The upstream HTTP port (P2-1): proxy, health and discovery talk to
/// upstreams exclusively through this.
pub trait UpstreamClient: Send + Sync {
    fn send(
        &self,
        request: UpstreamRequest,
    ) -> BoxFuture<'static, Result<UpstreamResponse, UpstreamError>>;
}

/// Telemetry/event port (P2-1): fire-and-forget domain events. The queueing
/// and persistence implementation is `telemetry::Telemetry`.
pub trait EventSink: Send + Sync {
    fn emit(&self, event: Event);
}

/// Time port (P2-1): lets services timestamp without a hard chrono
/// dependency; tests can substitute a fixed clock.
pub trait Clock: Send + Sync {
    fn now_utc(&self) -> chrono::DateTime<chrono::Utc>;
}

/// Model-route persistence port (P2-1): candidate resolution and routable
/// endpoint listing. The SQLite implementation is `infrastructure`.
pub trait RouteRepository: Send + Sync {
    /// Ordered eligible candidates for (protocol, model), capped at
    /// `max_attempts` (the proxy's failover budget).
    fn resolve_candidates(
        &self,
        protocol: &str,
        model: &str,
        max_attempts: i64,
    ) -> BoxFuture<'static, Result<Vec<Candidate>>>;

    /// Models that are actually callable right now (routed, enabled,
    /// healthy), for the model-catalog endpoints.
    fn list_routable_models(
        &self,
        protocol: Option<&str>,
    ) -> BoxFuture<'static, Result<Vec<RoutableModel>>>;

    /// Mapping target for a mapped entry model (claudecode/codex), if any.
    fn resolve_mapping(
        &self,
        entry: &str,
        model: &str,
    ) -> BoxFuture<'static, Result<Option<MappingTarget>>>;

    /// Primary entrypoints a model can currently be routed through.
    fn routable_endpoints_for_model(
        &self,
        model_id: &str,
    ) -> BoxFuture<'static, Result<Vec<String>>>;

    /// Enabled mapping models (claudecode/codex catalog rows).
    fn list_mapping_models(&self, kind: &str) -> BoxFuture<'static, Result<Vec<RoutableModel>>>;
}

/// Channel row snapshot needed by the background services (health probes,
/// discovery): credentials stay encrypted, decryption happens in the
/// service.
#[derive(Debug, Clone)]
pub struct ChannelRow {
    pub id: String,
    pub name: String,
    pub protocol: String,
    pub api_key_encrypted: Vec<u8>,
    pub base_url: String,
    pub manual_enabled: bool,
    pub health_check_model_id: Option<String>,
}

/// Channel persistence port (P2-1): the row loads behind probes and
/// discovery. The SQLite implementation is `infrastructure`.
pub trait ChannelRepository: Send + Sync {
    fn load_channel(&self, channel_id: &str) -> BoxFuture<'static, Result<Option<ChannelRow>>>;
}

/// One failover event: a request attempt on `failed_channel_name` was handed
/// over to `next_channel_name`. `error_kind` is a short stable label
/// ("connect_timeout", "transport_error", "HTTP 500", ...) or None when the
/// failure carried no classification.
#[derive(Debug, Clone)]
pub struct FailoverNotice {
    pub model_id: String,
    pub failed_channel_name: String,
    pub next_channel_name: String,
    pub error_kind: Option<String>,
}

/// Desktop-notification port (P2-1): fire-and-forget user-facing alerts.
/// Implementations must never block the caller: delivery is asynchronous
/// (queued to a background worker), and environments without a notification
/// daemon degrade silently.
pub trait Notifier: Send + Sync {
    fn notify_failover(&self, notice: FailoverNotice);
}
