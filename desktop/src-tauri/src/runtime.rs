//! Internal safety bounds and scheduling intervals (P2-10).
//!
//! These constants decide resource, shutdown, and external-I/O behavior.
//! They used to be scattered across modules (server reaper 500 ms, health
//! 20 s/5 s, discovery 120 s/50 pages, proxy 1 MiB, controller/Tauri 30 s/
//! 10 s shutdown) with mutually conflicting shutdown deadlines. One
//! immutable [`RuntimeLimits`] snapshot is built at startup and shared by
//! every supervisor, so a limit change happens in exactly one place.
//!
//! User-configurable upstream timeouts live in
//! [`crate::settings::RuntimeSettings`]; this module is the fixed
//! operational layer. Tests inject short values instead of relying on
//! production constants.

use std::time::Duration;

/// Immutable operational limits, constructed once per runtime.
#[derive(Debug, Clone, Copy)]
pub struct RuntimeLimits {
    /// One-shot task reaper poll interval (was 500 ms in `reap_loop`).
    pub reaper_interval: Duration,
    /// Hard timeout for a single health probe (was 20 s).
    pub probe_timeout: Duration,
    /// Health supervisor poll interval (was 5 s).
    pub probe_interval: Duration,
    /// Hard timeout for one discovery run (was 120 s).
    pub discovery_timeout: Duration,
    /// Discovery pagination cap (was 50 pages).
    pub discovery_max_pages: usize,
    /// Maintenance supervisor cadence (was 60 s).
    pub maintenance_interval: Duration,
    /// Log cleanup cadence inside the maintenance loop (was 3600 s).
    pub cleanup_interval: Duration,
    /// Absolute timeout for one channel balance query (upstream exchange
    /// plus body read); balance queries are a sidecar and must never hold a
    /// connection for long.
    pub balance_timeout: Duration,
    /// Background cadence for refreshing channels with balance queries
    /// enabled (was 3600 s, i.e. hourly). Tests inject short values.
    pub balance_interval: Duration,
    /// Non-2xx upstream error body replay cap (was 1 MiB).
    pub error_body_max: usize,
    /// Absolute shutdown deadline: after cancel, this long to drain every
    /// task; anything still running is aborted and joined (was the
    /// conflicting controller 30 s / Tauri 10 s pair).
    pub shutdown_deadline: Duration,
}

impl Default for RuntimeLimits {
    fn default() -> Self {
        Self {
            reaper_interval: Duration::from_millis(500),
            probe_timeout: Duration::from_secs(20),
            probe_interval: Duration::from_secs(5),
            discovery_timeout: Duration::from_secs(120),
            discovery_max_pages: 50,
            maintenance_interval: Duration::from_secs(60),
            cleanup_interval: Duration::from_secs(3600),
            balance_timeout: Duration::from_secs(15),
            balance_interval: Duration::from_secs(3600),
            error_body_max: 1024 * 1024,
            shutdown_deadline: Duration::from_secs(30),
        }
    }
}
