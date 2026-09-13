//! Channel balance / usage query sidecar.
//!
//! Balance is an opt-in, per-channel sidecar: a channel without a row in
//! `channel_balance_configs` never produces an upstream balance request.
//! Users pick one of five adapters and enable the switch; after that the
//! maintenance supervisor refreshes enabled channels hourly and the admin
//! API can query one channel manually.
//!
//! Hard boundaries (mirrors health probing):
//! 1. A balance query is a sidecar — any failure must never touch the proxy
//!    path;
//! 2. failures only write a snapshot (`status=error` + stable `error_kind`)
//!    and never escape the admin API as a 5xx for upstream problems;
//! 3. snapshots never store the raw response body or any token; tokens only
//!    live encrypted;
//! 4. no network call happens inside a transaction: prepare, exchange, then
//!    one write.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context as _, anyhow};
use bytes::Bytes;
use futures_util::StreamExt;
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use url::Url;

use crate::{
    api_error::ApiError,
    application::Context,
    crypto::SecretStore,
    db::Database,
    ports::{ChannelRepository, ChannelRow, Clock, UpstreamClient, UpstreamError, UpstreamRequest},
    protocol,
    runtime::RuntimeLimits,
};

/// New API stores quota at 500,000 units per USD.
pub const NEW_API_QUOTA_PER_USD: f64 = 500_000.0;

/// New API account-balance endpoint, reachable with a dashboard Personal
/// Access Token (the channel's sk- key only exposes token-scoped usage).
const NEW_API_ACCOUNT_PATH: &str = "/api/user/self";

/// Balance payloads are tiny; cap the body like the other sidecars do.
const BALANCE_BODY_MAX: usize = 256 * 1024;
/// Custom request template bounds (validated on save).
const MAX_CUSTOM_HEADERS: usize = 8;
const MAX_CUSTOM_BODY_BYTES: usize = 16 * 1024;
const MAX_CUSTOM_PATH_CHARS: usize = 1024;
const MAX_MAPPING_CHARS: usize = 256;
/// Background and batch refresh concurrency cap.
const REFRESH_CONCURRENCY: usize = 4;

/// Stable upstream failure classifications (stored in
/// `channel_balance_snapshots.error_kind`).
pub mod error_kind {
    pub const HTTP_401: &str = "http_401";
    pub const HTTP_403: &str = "http_403";
    pub const HTTP_5XX: &str = "http_5xx";
    pub const HTTP_ERROR: &str = "http_error";
    pub const TRANSPORT_ERROR: &str = "transport_error";
    pub const TIMEOUT: &str = "timeout";
    pub const INVALID_PAYLOAD: &str = "invalid_payload";
    pub const CUSTOM_PATH_MISSING: &str = "custom_path_missing";
    /// Command Code integration is globally disabled (`command_code_enabled`).
    pub const DISABLED: &str = "disabled";
}

/// Stable service-level failures for admin handlers.
#[derive(Debug)]
pub enum BalanceError {
    /// Manual query on a channel without a saved adapter (default-off).
    NotConfigured,
    ChannelNotFound,
    /// Invalid user input (422).
    Invalid(String),
    Internal(anyhow::Error),
}

impl std::fmt::Display for BalanceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BalanceError::NotConfigured => write!(formatter, "balance_not_configured"),
            BalanceError::ChannelNotFound => write!(formatter, "Channel not found"),
            BalanceError::Invalid(message) => write!(formatter, "{message}"),
            BalanceError::Internal(error) => write!(formatter, "{error:#}"),
        }
    }
}

impl std::error::Error for BalanceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BalanceError::Internal(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

impl From<anyhow::Error> for BalanceError {
    fn from(error: anyhow::Error) -> Self {
        BalanceError::Internal(error)
    }
}

impl From<sqlx::Error> for BalanceError {
    fn from(error: sqlx::Error) -> Self {
        BalanceError::Internal(error.into())
    }
}

impl From<serde_json::Error> for BalanceError {
    fn from(error: serde_json::Error) -> Self {
        BalanceError::Internal(error.into())
    }
}

impl From<BalanceError> for ApiError {
    fn from(error: BalanceError) -> Self {
        match error {
            BalanceError::NotConfigured => ApiError::balance_not_configured(),
            BalanceError::ChannelNotFound => ApiError::not_found("Channel not found"),
            BalanceError::Invalid(message) => ApiError::validation(message),
            BalanceError::Internal(error) => ApiError::internal(error),
        }
    }
}

/// The five supported adapters. Serialized names are the persisted values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BalanceAdapter {
    #[serde(rename = "newapi")]
    NewApi,
    #[serde(rename = "sub2api")]
    Sub2Api,
    #[serde(rename = "opencode_go")]
    OpencodeGo,
    #[serde(rename = "deepseek")]
    DeepSeek,
    /// Command Code Go: `whoami` → `billing/credits` → `usage/summary`
    /// (three read-only GETs, no generation).
    #[serde(rename = "command_code")]
    CommandCode,
    #[serde(rename = "custom")]
    Custom,
}

impl BalanceAdapter {
    pub fn parse(value: &str) -> Result<Self, BalanceError> {
        match value.trim() {
            "newapi" => Ok(BalanceAdapter::NewApi),
            "sub2api" => Ok(BalanceAdapter::Sub2Api),
            "opencode_go" => Ok(BalanceAdapter::OpencodeGo),
            "deepseek" => Ok(BalanceAdapter::DeepSeek),
            "command_code" => Ok(BalanceAdapter::CommandCode),
            "custom" => Ok(BalanceAdapter::Custom),
            other => Err(BalanceError::Invalid(format!(
                "不支持的余额适配器：{other}"
            ))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            BalanceAdapter::NewApi => "newapi",
            BalanceAdapter::Sub2Api => "sub2api",
            BalanceAdapter::OpencodeGo => "opencode_go",
            BalanceAdapter::DeepSeek => "deepseek",
            BalanceAdapter::CommandCode => "command_code",
            BalanceAdapter::Custom => "custom",
        }
    }

    /// Preset endpoint stored for every built-in adapter (`custom` has none).
    pub fn default_path(self) -> Option<&'static str> {
        match self {
            BalanceAdapter::NewApi => Some("/api/usage/token/"),
            BalanceAdapter::Sub2Api => Some("/v1/usage"),
            BalanceAdapter::OpencodeGo => Some("v1/usage"),
            BalanceAdapter::DeepSeek => Some("/user/balance"),
            // Multi-step (whoami → credits → summary); the stored path is
            // unused for this adapter.
            BalanceAdapter::CommandCode => None,
            BalanceAdapter::Custom => None,
        }
    }
}

/// Authentication mode for the balance request. Built-in adapters always
/// use `bearer`; `none` exists for custom endpoints that authenticate by
/// header template only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BalanceAuth {
    Bearer,
    None,
}

impl BalanceAuth {
    pub fn parse(value: &str) -> Result<Self, BalanceError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "bearer" => Ok(BalanceAuth::Bearer),
            "none" => Ok(BalanceAuth::None),
            other => Err(BalanceError::Invalid(format!("不支持的鉴权方式：{other}"))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            BalanceAuth::Bearer => "bearer",
            BalanceAuth::None => "none",
        }
    }
}

/// One usage window (OpenCode Go rolling / weekly / monthly).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuotaWindow {
    pub label: String,
    pub used_percent: f64,
    pub remaining_percent: f64,
    pub resets_at: Option<String>,
}

/// Normalized result of one adapter parser.
#[derive(Debug, Clone, Default)]
pub struct BalanceReading {
    pub remaining: Option<f64>,
    pub currency: Option<String>,
    pub used: Option<f64>,
    pub total: Option<f64>,
    pub unlimited: bool,
    pub label: Option<String>,
    pub windows: Vec<QuotaWindow>,
    pub detail: Value,
}

/// User's balance mapping for the custom adapter.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BalanceMapping {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remaining: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub used: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

/// Stored configuration. `adapter=None` means the channel has no config row
/// and must not produce any balance request.
#[derive(Debug, Clone)]
pub struct BalanceConfig {
    pub adapter: Option<BalanceAdapter>,
    pub enabled: bool,
    pub method: String,
    pub path: Option<String>,
    pub auth: BalanceAuth,
    pub headers: Vec<(String, String)>,
    pub body: Option<String>,
    pub mapping: BalanceMapping,
    pub has_token: bool,
    pub token_hint: Option<String>,
    /// Ciphertext is only used by `prepare` and never serialized.
    token_encrypted: Option<Vec<u8>>,
}

impl Default for BalanceConfig {
    fn default() -> Self {
        Self {
            adapter: None,
            enabled: false,
            method: "GET".into(),
            path: None,
            auth: BalanceAuth::Bearer,
            headers: Vec::new(),
            body: None,
            mapping: BalanceMapping::default(),
            has_token: false,
            token_hint: None,
            token_encrypted: None,
        }
    }
}

impl BalanceConfig {
    /// Admin JSON view: everything except the token ciphertext.
    pub fn to_json(&self) -> Value {
        let headers: BTreeMap<&str, &str> = self
            .headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        json!({
            "configured": self.adapter.is_some(),
            "adapter": self.adapter.map(BalanceAdapter::as_str),
            "enabled": self.enabled,
            "method": self.method,
            "path": self.path,
            "auth": self.auth.as_str(),
            "headers": headers,
            "body": self.body,
            "mapping": self.mapping,
            "has_token": self.has_token,
            "token_hint": self.token_hint,
        })
    }
}

/// JSON path mapping inside [`BalanceConfigInput`].
#[derive(Debug, Clone, Default, Deserialize)]
pub struct BalanceMappingInput {
    #[serde(default)]
    pub remaining: Option<String>,
    #[serde(default)]
    pub currency: Option<String>,
    #[serde(default)]
    pub used: Option<String>,
    #[serde(default)]
    pub total: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
}

/// Request body for `PUT /channels/{id}/balance-config`.
#[derive(Debug, Clone, Deserialize)]
pub struct BalanceConfigInput {
    pub adapter: String,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub auth: Option<String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub body: Option<String>,
    #[serde(default)]
    pub mapping: BalanceMappingInput,
    /// New dedicated token. An empty/absent string keeps the stored token;
    /// deletion requires `clear_token: true`.
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default)]
    pub clear_token: bool,
}

/// Latest snapshot of one channel.
#[derive(Debug, Clone, Serialize)]
pub struct BalanceSnapshot {
    pub channel_id: String,
    pub adapter: String,
    pub status: &'static str,
    pub remaining: Option<f64>,
    pub currency: Option<String>,
    pub used: Option<f64>,
    pub total: Option<f64>,
    pub unlimited: bool,
    pub label: Option<String>,
    pub windows: Vec<QuotaWindow>,
    pub detail: Value,
    pub error_kind: Option<String>,
    pub status_code: Option<i64>,
    pub duration_ms: i64,
    pub checked_at: String,
}

/// Normalized config handed to persistence after validation.
struct NormalizedConfig {
    adapter: BalanceAdapter,
    enabled: bool,
    method: String,
    path: Option<String>,
    auth: BalanceAuth,
    headers: Vec<(String, String)>,
    body: Option<String>,
    mapping: BalanceMapping,
}

/// Token mutation requested by one config save.
enum TokenUpdate {
    Keep,
    Set(String),
    Clear,
}

/// Service dependencies injected from the composition root.
pub struct BalanceService {
    db: Database,
    secrets: SecretStore,
    http: Arc<dyn UpstreamClient>,
    channels: Arc<dyn ChannelRepository>,
    clock: Arc<dyn Clock>,
    limits: Arc<RuntimeLimits>,
}

impl BalanceService {
    pub fn new(
        db: Database,
        secrets: SecretStore,
        http: Arc<dyn UpstreamClient>,
        channels: Arc<dyn ChannelRepository>,
        clock: Arc<dyn Clock>,
        limits: Arc<RuntimeLimits>,
    ) -> Arc<Self> {
        Arc::new(Self {
            db,
            secrets,
            http,
            channels,
            clock,
            limits,
        })
    }

    /// Channel existence guard for every public entry point.
    async fn ensure_channel(&self, channel_id: &str) -> Result<(), BalanceError> {
        let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM channels WHERE id=?")
            .bind(channel_id)
            .fetch_one(self.db.pool())
            .await?;
        if exists == 0 {
            return Err(BalanceError::ChannelNotFound);
        }
        Ok(())
    }

    /// Load the stored config; a missing row yields `adapter=None`
    /// (default-off, no requests).
    pub async fn config(&self, channel_id: &str) -> Result<BalanceConfig, BalanceError> {
        self.ensure_channel(channel_id).await?;
        Ok(self.load_config(channel_id).await?.unwrap_or_default())
    }

    async fn load_config(&self, channel_id: &str) -> Result<Option<BalanceConfig>, BalanceError> {
        let row = sqlx::query_as::<_, ConfigRow>(
            "SELECT adapter, enabled, method, path, auth, headers_json, body_json, \
                    mapping_json, balance_token_encrypted, balance_token_hint \
             FROM channel_balance_configs WHERE channel_id=?",
        )
        .bind(channel_id)
        .fetch_optional(self.db.pool())
        .await?;
        row.map(ConfigRow::into_config).transpose()
    }

    /// Read the latest snapshot (if any) for the admin view.
    pub async fn snapshot(
        &self,
        channel_id: &str,
    ) -> Result<Option<BalanceSnapshot>, BalanceError> {
        self.ensure_channel(channel_id).await?;
        Ok(load_snapshot(&self.db, channel_id).await?)
    }

    /// Save (upsert) the channel's balance configuration.
    pub async fn save_config(
        &self,
        channel_id: &str,
        input: BalanceConfigInput,
    ) -> Result<BalanceConfig, BalanceError> {
        self.ensure_channel(channel_id).await?;
        let (normalized, token_update) = normalize_config(&input)?;
        let now = self.clock.now_utc().to_rfc3339();
        let headers_json = serde_json::to_string(&normalized.headers)?;
        let mapping_json = serde_json::to_string(&normalized.mapping)?;
        let mut tx = self.db.pool().begin().await?;
        sqlx::query(
            "INSERT INTO channel_balance_configs \
             (channel_id, adapter, enabled, method, path, auth, headers_json, body_json, \
              mapping_json, balance_token_encrypted, balance_token_hint, created_at, updated_at) \
             VALUES (?,?,?,?,?,?,?,?,?,NULL,NULL,?,?) \
             ON CONFLICT(channel_id) DO UPDATE SET \
               adapter=excluded.adapter, enabled=excluded.enabled, method=excluded.method, \
               path=excluded.path, auth=excluded.auth, headers_json=excluded.headers_json, \
               body_json=excluded.body_json, mapping_json=excluded.mapping_json, \
               updated_at=excluded.updated_at",
        )
        .bind(channel_id)
        .bind(normalized.adapter.as_str())
        .bind(normalized.enabled)
        .bind(&normalized.method)
        .bind(normalized.path.as_deref())
        .bind(normalized.auth.as_str())
        .bind(&headers_json)
        .bind(normalized.body.as_deref())
        .bind(&mapping_json)
        .bind(&now)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        match token_update {
            TokenUpdate::Keep => {}
            TokenUpdate::Set(token) => {
                let encrypted = self.secrets.encrypt(&token);
                let hint = SecretStore::hint(&token);
                sqlx::query(
                    "UPDATE channel_balance_configs \
                     SET balance_token_encrypted=?, balance_token_hint=?, updated_at=? \
                     WHERE channel_id=?",
                )
                .bind(encrypted)
                .bind(hint)
                .bind(&now)
                .bind(channel_id)
                .execute(&mut *tx)
                .await?;
            }
            TokenUpdate::Clear => {
                sqlx::query(
                    "UPDATE channel_balance_configs \
                     SET balance_token_encrypted=NULL, balance_token_hint=NULL, updated_at=? \
                     WHERE channel_id=?",
                )
                .bind(&now)
                .bind(channel_id)
                .execute(&mut *tx)
                .await?;
            }
        }
        tx.commit().await?;
        self.config(channel_id).await
    }

    /// Delete config plus snapshot — the channel returns to default-off.
    pub async fn delete_config(&self, channel_id: &str) -> Result<(), BalanceError> {
        self.ensure_channel(channel_id).await?;
        let mut tx = self.db.pool().begin().await?;
        sqlx::query("DELETE FROM channel_balance_snapshots WHERE channel_id=?")
            .bind(channel_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM channel_balance_configs WHERE channel_id=?")
            .bind(channel_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Query one channel now.
    ///
    /// A saved-but-disabled config is still queried here: the user clicking
    /// "query now" is explicit intent. Only background/batch refresh filters
    /// on `enabled=1`. A channel without any config returns
    /// [`BalanceError::NotConfigured`].
    pub async fn query(
        &self,
        state: &Context,
        channel_id: &str,
    ) -> Result<BalanceSnapshot, BalanceError> {
        let channel: ChannelRow = self
            .channels
            .load_channel(channel_id)
            .await?
            .ok_or(BalanceError::ChannelNotFound)?;
        let config = self.config(channel_id).await?;
        let Some(adapter) = config.adapter else {
            return Err(BalanceError::NotConfigured);
        };
        let checked_at = self.clock.now_utc().to_rfc3339();
        let started = Instant::now();
        let outcome = if adapter == BalanceAdapter::CommandCode {
            self.command_code_reading(state, &channel, &config).await
        } else {
            match self.prepare(state, &channel, &config, adapter).await {
                Ok(prepared) => self.exchange(prepared).await,
                Err(failure) => Err(failure),
            }
        };
        let duration_ms = started.elapsed().as_millis().min(i64::MAX as u128) as i64;
        let snapshot = match outcome {
            Ok((status_code, reading)) => snapshot_ok(
                channel_id,
                adapter,
                reading,
                status_code,
                duration_ms,
                checked_at,
            ),
            Err(BalanceFailure::Classified { kind, status_code }) => snapshot_error(
                channel_id,
                adapter,
                kind,
                status_code,
                duration_ms,
                checked_at,
            ),
            Err(BalanceFailure::Fatal(error)) => return Err(BalanceError::Internal(error)),
        };
        self.store_snapshot(&snapshot).await?;
        Ok(snapshot)
    }

    /// Manual batch refresh and the hourly maintenance task share this:
    /// only channels with `enabled=1` are queried, with a concurrency cap.
    /// Individual failures are silent (their snapshots are still written).
    pub async fn refresh_enabled(
        &self,
        state: &Context,
    ) -> Result<Vec<BalanceSnapshot>, BalanceError> {
        let channel_ids: Vec<String> = sqlx::query_scalar(
            "SELECT channel_id FROM channel_balance_configs WHERE enabled=1 ORDER BY channel_id",
        )
        .fetch_all(self.db.pool())
        .await?;
        if channel_ids.is_empty() {
            return Ok(Vec::new());
        }
        let results: Vec<Result<BalanceSnapshot, BalanceError>> = futures_util::stream::iter(
            channel_ids
                .into_iter()
                .map(|channel_id| async move { self.query(state, &channel_id).await }),
        )
        .buffer_unordered(REFRESH_CONCURRENCY)
        .collect()
        .await;
        let mut snapshots = Vec::new();
        for result in results {
            match result {
                Ok(snapshot) => snapshots.push(snapshot),
                Err(error) => {
                    // Deleted/not-configured between listing and query, or a
                    // local storage failure: never disturb the maintenance
                    // loop or the batch response with a single bad channel.
                    tracing::debug!(%error, "channel balance refresh skipped");
                }
            }
        }
        Ok(snapshots)
    }

    /// Admin view: config (if any) plus the latest snapshot.
    pub async fn balance_json(&self, channel_id: &str) -> Result<Value, BalanceError> {
        self.ensure_channel(channel_id).await?;
        let config = self.load_config(channel_id).await?.unwrap_or_default();
        let snapshot = load_snapshot(&self.db, channel_id).await?;
        let mut value = config.to_json();
        if let Some(object) = value.as_object_mut() {
            object.insert(
                "snapshot".into(),
                serde_json::to_value(&snapshot).unwrap_or(Value::Null),
            );
        }
        Ok(value)
    }

    /// `POST /channels/{id}/balance` response body.
    pub async fn query_json(
        &self,
        state: &Context,
        channel_id: &str,
    ) -> Result<Value, BalanceError> {
        let snapshot = self.query(state, channel_id).await?;
        Ok(serde_json::to_value(&snapshot)?)
    }

    /// `POST /balances/refresh` response body.
    pub async fn refresh_json(&self, state: &Context) -> Result<Value, BalanceError> {
        let snapshots = self.refresh_enabled(state).await?;
        let ok = snapshots.iter().filter(|item| item.status == "ok").count();
        let failed = snapshots.len() - ok;
        Ok(json!({
            "items": snapshots,
            "total": ok + failed,
            "ok": ok,
            "failed": failed,
        }))
    }

    /// Build the request. Failures here are already classified so they can
    /// be written as an error snapshot.
    async fn prepare(
        &self,
        state: &Context,
        channel: &ChannelRow,
        config: &BalanceConfig,
        adapter: BalanceAdapter,
    ) -> Result<PreparedRequest, BalanceFailure> {
        let api_key = self
            .secrets
            .decrypt(&channel.api_key_encrypted)
            .map_err(BalanceFailure::Fatal)?;
        let token = match config.token_encrypted.as_deref() {
            Some(ciphertext) if !ciphertext.is_empty() => self
                .secrets
                .decrypt(ciphertext)
                .map_err(BalanceFailure::Fatal)?,
            _ => api_key.clone(),
        };
        // New API: a dedicated dashboard token (PAT) unlocks the account
        // balance endpoint; without it only the token-scoped usage endpoint
        // is reachable with an sk- key.
        let account_mode = adapter == BalanceAdapter::NewApi && config.has_token;
        let raw_path = if account_mode {
            NEW_API_ACCOUNT_PATH
        } else {
            config
                .path
                .as_deref()
                .map(str::trim)
                .filter(|path| !path.is_empty())
                .or_else(|| adapter.default_path())
                .ok_or(BalanceFailure::Classified {
                    kind: error_kind::CUSTOM_PATH_MISSING,
                    status_code: None,
                })?
        };
        let path = render_template(raw_path, &api_key, &token);
        let url =
            resolve_url(&channel.base_url, &path).map_err(|_| BalanceFailure::Classified {
                kind: error_kind::INVALID_PAYLOAD,
                status_code: None,
            })?;
        let method = match adapter {
            BalanceAdapter::Custom => parse_method(&config.method).unwrap_or(Method::GET),
            _ => Method::GET,
        };
        let rendered_body = config
            .body
            .as_deref()
            .filter(|body| !body.is_empty())
            .map(|body| render_template(body, &api_key, &token));
        let body = if method == Method::GET {
            None
        } else {
            rendered_body.map(Bytes::from)
        };
        let mut headers = HeaderMap::new();
        if adapter == BalanceAdapter::Custom {
            for (name, value) in &config.headers {
                let rendered = render_template(value, &api_key, &token);
                let name = HeaderName::from_bytes(name.as_bytes()).map_err(|_| {
                    BalanceFailure::Classified {
                        kind: error_kind::INVALID_PAYLOAD,
                        status_code: None,
                    }
                })?;
                let value =
                    HeaderValue::from_str(&rendered).map_err(|_| BalanceFailure::Classified {
                        kind: error_kind::INVALID_PAYLOAD,
                        status_code: None,
                    })?;
                headers.insert(name, value);
            }
        }
        if config.auth == BalanceAuth::Bearer {
            let value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| {
                BalanceFailure::Classified {
                    kind: error_kind::INVALID_PAYLOAD,
                    status_code: None,
                }
            })?;
            headers.insert(axum::http::header::AUTHORIZATION, value);
        }
        if body.is_some() && !headers.contains_key(axum::http::header::CONTENT_TYPE) {
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
        }
        if protocol::requires_opencode_session(&channel.base_url) {
            let session_id = crate::settings::opencode_session_id(&self.db).await;
            let _ = protocol::apply_opencode_session(&mut headers, &channel.base_url, &session_id);
        }
        let connect_timeout = match crate::settings::runtime_settings(state).await {
            Ok(runtime) => Duration::from_secs(runtime.connect_timeout_seconds.max(1) as u64),
            Err(_) => Duration::from_secs(10),
        };
        Ok(PreparedRequest {
            adapter,
            account_mode,
            mapping: config.mapping.clone(),
            url,
            method,
            headers,
            body,
            connect_timeout,
        })
    }

    /// Perform the exchange under one overall deadline and classify the
    /// result. No response body is ever returned to the caller or persisted.
    async fn exchange(
        &self,
        prepared: PreparedRequest,
    ) -> Result<(i64, BalanceReading), BalanceFailure> {
        let PreparedRequest {
            adapter,
            account_mode,
            mapping,
            url,
            method,
            headers,
            body,
            connect_timeout,
        } = prepared;
        let request = UpstreamRequest {
            url,
            headers,
            method,
            body,
            connect_timeout,
            deadline: self.limits.balance_timeout,
        };
        let exchange = async {
            let response = self.http.send(request).await?;
            let status_code = response.status.as_u16() as i64;
            let (body, _truncated) = response.body.read_capped(BALANCE_BODY_MAX).await;
            Ok::<(i64, StatusCode, Vec<u8>), UpstreamError>((status_code, response.status, body))
        };
        let (status_code, status, body) =
            match tokio::time::timeout(self.limits.balance_timeout, exchange).await {
                Err(_) => {
                    return Err(BalanceFailure::Classified {
                        kind: error_kind::TIMEOUT,
                        status_code: None,
                    });
                }
                Ok(Err(UpstreamError::Deadline)) | Ok(Err(UpstreamError::ConnectTimeout)) => {
                    return Err(BalanceFailure::Classified {
                        kind: error_kind::TIMEOUT,
                        status_code: None,
                    });
                }
                Ok(Err(UpstreamError::Transport(_))) => {
                    return Err(BalanceFailure::Classified {
                        kind: error_kind::TRANSPORT_ERROR,
                        status_code: None,
                    });
                }
                Ok(Ok(value)) => value,
            };
        if !status.is_success() {
            return Err(BalanceFailure::Classified {
                kind: status_error_kind(status.as_u16()),
                status_code: Some(status_code),
            });
        }
        let reading = if account_mode {
            parse_newapi_user(&body)
        } else {
            match adapter {
                BalanceAdapter::Custom => parse_custom(&body, &mapping),
                _ => parse_reading(adapter, &body),
            }
        };
        reading
            .map(|reading| (status_code, reading))
            .map_err(|_| BalanceFailure::Classified {
                kind: error_kind::INVALID_PAYLOAD,
                status_code: Some(status_code),
            })
    }

    /// Command Code quota: `whoami` → `billing/credits` → `usage/summary`.
    /// All three are read-only GETs (no generation, no token spend) and
    /// gated by the global `command_code_enabled` switch.
    async fn command_code_reading(
        &self,
        state: &Context,
        channel: &ChannelRow,
        _config: &BalanceConfig,
    ) -> Result<(i64, BalanceReading), BalanceFailure> {
        let runtime = crate::settings::runtime_settings(state)
            .await
            .unwrap_or_default();
        if !runtime.command_code_enabled {
            return Err(BalanceFailure::Classified {
                kind: error_kind::DISABLED,
                status_code: None,
            });
        }
        let api_key = self
            .secrets
            .decrypt(&channel.api_key_encrypted)
            .map_err(BalanceFailure::Fatal)?;
        let deadline = self.limits.balance_timeout;
        let (_, whoami) = self
            .command_code_get(&channel.base_url, "/alpha/whoami", &api_key, deadline)
            .await?;
        // 账号可能没有 org（whoami 的 `org` 为 null，个人 key）。此时官方
        // 语义是**省略 orgId 参数**（拼成 `?orgId=` 会得到 400 Invalid UUID）；
        // patlux 的 buildUrl 同样会丢掉 undefined 参数。
        let org_id = command_code_org_id(&whoami);
        let with_org = |path: &str| match &org_id {
            Some(org_id) => format!("{path}?orgId={org_id}"),
            None => path.to_owned(),
        };
        let credits_path = with_org("/alpha/billing/credits");
        let (credits_status, credits) = self
            .command_code_get(&channel.base_url, &credits_path, &api_key, deadline)
            .await?;
        // Usage summary is optional: a failure there still yields the
        // credits/window reading rather than no balance at all.
        let summary_path = with_org("/alpha/usage/summary");
        let summary = match self
            .command_code_get(&channel.base_url, &summary_path, &api_key, deadline)
            .await
        {
            Ok((_, body)) => Some(body),
            Err(_error) => {
                tracing::debug!("command code usage summary unavailable");
                None
            }
        };
        // 订阅信息用于展示 planId/状态，并作为 windowLimits 之外的计划来源；
        // 查询失败不影响额度读数。
        let subscriptions_path = with_org("/alpha/billing/subscriptions");
        let subscription = match self
            .command_code_get(&channel.base_url, &subscriptions_path, &api_key, deadline)
            .await
        {
            Ok((_, body)) => Some(body),
            Err(_error) => {
                tracing::debug!("command code subscriptions unavailable");
                None
            }
        };
        let reading = parse_command_code(
            &whoami,
            &credits,
            summary.as_deref(),
            subscription.as_deref(),
            org_id.as_deref().unwrap_or_default(),
        )
        .map_err(|_| BalanceFailure::Classified {
            kind: error_kind::INVALID_PAYLOAD,
            status_code: Some(credits_status),
        })?;
        Ok((credits_status, reading))
    }

    async fn command_code_get(
        &self,
        base_url: &str,
        path: &str,
        api_key: &str,
        deadline: Duration,
    ) -> Result<(i64, Vec<u8>), BalanceFailure> {
        let url = crate::protocol::upstream_url(base_url, path, None, "command_code")
            .map_err(|_| BalanceFailure::Classified {
                kind: error_kind::INVALID_PAYLOAD,
                status_code: None,
            })?;
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {api_key}")).map_err(|_| {
                BalanceFailure::Classified {
                    kind: error_kind::INVALID_PAYLOAD,
                    status_code: None,
                }
            })?,
        );
        headers.insert(
            axum::http::header::ACCEPT,
            HeaderValue::from_static("application/json"),
        );
        let response = self
            .http
            .send(UpstreamRequest {
                url,
                headers,
                method: Method::GET,
                body: None,
                connect_timeout: Duration::from_secs(10),
                deadline,
            })
            .await
            .map_err(|_| BalanceFailure::Classified {
                kind: error_kind::TRANSPORT_ERROR,
                status_code: None,
            })?;
        let status_code = response.status.as_u16() as i64;
        let (body, _truncated) = response.body.read_capped(BALANCE_BODY_MAX).await;
        if !response.status.is_success() {
            return Err(BalanceFailure::Classified {
                kind: status_error_kind(response.status.as_u16()),
                status_code: Some(status_code),
            });
        }
        Ok((status_code, body))
    }

    async fn store_snapshot(&self, snapshot: &BalanceSnapshot) -> Result<(), BalanceError> {
        let windows = serde_json::to_string(&snapshot.windows)?;
        let detail = serde_json::to_string(&snapshot.detail)?;
        sqlx::query(
            "INSERT INTO channel_balance_snapshots \
             (channel_id, adapter, status, remaining, currency, used, total, unlimited, label, \
              windows_json, detail_json, error_kind, status_code, duration_ms, checked_at) \
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) \
             ON CONFLICT(channel_id) DO UPDATE SET \
               adapter=excluded.adapter, status=excluded.status, remaining=excluded.remaining, \
               currency=excluded.currency, used=excluded.used, total=excluded.total, \
               unlimited=excluded.unlimited, label=excluded.label, \
               windows_json=excluded.windows_json, detail_json=excluded.detail_json, \
               error_kind=excluded.error_kind, status_code=excluded.status_code, \
               duration_ms=excluded.duration_ms, checked_at=excluded.checked_at",
        )
        .bind(&snapshot.channel_id)
        .bind(&snapshot.adapter)
        .bind(snapshot.status)
        .bind(snapshot.remaining)
        .bind(snapshot.currency.as_deref())
        .bind(snapshot.used)
        .bind(snapshot.total)
        .bind(snapshot.unlimited)
        .bind(snapshot.label.as_deref())
        .bind(&windows)
        .bind(&detail)
        .bind(snapshot.error_kind.as_deref())
        .bind(snapshot.status_code)
        .bind(snapshot.duration_ms)
        .bind(&snapshot.checked_at)
        .execute(self.db.pool())
        .await?;
        Ok(())
    }
}

/// Request built from a config, ready for the network phase.
struct PreparedRequest {
    adapter: BalanceAdapter,
    /// New API account-balance mode (`/api/user/self` + dedicated PAT).
    account_mode: bool,
    mapping: BalanceMapping,
    url: Url,
    method: Method,
    headers: HeaderMap,
    body: Option<Bytes>,
    connect_timeout: Duration,
}

/// Classified balance failure: `Classified` becomes a snapshot, `Fatal`
/// stays a service error (local corruption, never an upstream answer).
enum BalanceFailure {
    Classified {
        kind: &'static str,
        status_code: Option<i64>,
    },
    Fatal(anyhow::Error),
}

fn snapshot_ok(
    channel_id: &str,
    adapter: BalanceAdapter,
    reading: BalanceReading,
    status_code: i64,
    duration_ms: i64,
    checked_at: String,
) -> BalanceSnapshot {
    BalanceSnapshot {
        channel_id: channel_id.to_owned(),
        adapter: adapter.as_str().into(),
        status: "ok",
        remaining: reading.remaining,
        currency: reading.currency,
        used: reading.used,
        total: reading.total,
        unlimited: reading.unlimited,
        label: reading.label,
        windows: reading.windows,
        detail: reading.detail,
        error_kind: None,
        status_code: Some(status_code),
        duration_ms,
        checked_at,
    }
}

fn snapshot_error(
    channel_id: &str,
    adapter: BalanceAdapter,
    error_kind: &'static str,
    status_code: Option<i64>,
    duration_ms: i64,
    checked_at: String,
) -> BalanceSnapshot {
    BalanceSnapshot {
        channel_id: channel_id.to_owned(),
        adapter: adapter.as_str().into(),
        status: "error",
        remaining: None,
        currency: None,
        used: None,
        total: None,
        unlimited: false,
        label: None,
        windows: Vec::new(),
        detail: Value::Null,
        error_kind: Some(error_kind.to_owned()),
        status_code,
        duration_ms,
        checked_at,
    }
}

/// Persisted config row.
#[derive(sqlx::FromRow)]
struct ConfigRow {
    adapter: String,
    enabled: bool,
    method: String,
    path: Option<String>,
    auth: String,
    headers_json: Option<String>,
    body_json: Option<String>,
    mapping_json: Option<String>,
    balance_token_encrypted: Option<Vec<u8>>,
    balance_token_hint: Option<String>,
}

impl ConfigRow {
    fn into_config(self) -> Result<BalanceConfig, BalanceError> {
        let adapter = BalanceAdapter::parse(&self.adapter)?;
        let auth = BalanceAuth::parse(&self.auth).unwrap_or(BalanceAuth::Bearer);
        let headers: Vec<(String, String)> = self
            .headers_json
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok())
            .unwrap_or_default();
        let mapping: BalanceMapping = self
            .mapping_json
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok())
            .unwrap_or_default();
        let token_encrypted = self
            .balance_token_encrypted
            .filter(|ciphertext| !ciphertext.is_empty());
        Ok(BalanceConfig {
            adapter: Some(adapter),
            enabled: self.enabled,
            method: if self.method.trim().is_empty() {
                "GET".into()
            } else {
                self.method
            },
            path: self.path,
            auth,
            headers,
            body: self.body_json,
            mapping,
            has_token: token_encrypted.is_some(),
            token_hint: self.balance_token_hint,
            token_encrypted,
        })
    }
}

/// Persisted snapshot row.
#[derive(sqlx::FromRow)]
struct SnapshotRow {
    adapter: String,
    status: String,
    remaining: Option<f64>,
    currency: Option<String>,
    used: Option<f64>,
    total: Option<f64>,
    unlimited: bool,
    label: Option<String>,
    windows_json: Option<String>,
    detail_json: Option<String>,
    error_kind: Option<String>,
    status_code: Option<i64>,
    duration_ms: Option<i64>,
    checked_at: String,
}

impl SnapshotRow {
    fn into_snapshot(self, channel_id: &str) -> BalanceSnapshot {
        BalanceSnapshot {
            channel_id: channel_id.to_owned(),
            adapter: self.adapter,
            status: if self.status == "ok" { "ok" } else { "error" },
            remaining: self.remaining,
            currency: self.currency,
            used: self.used,
            total: self.total,
            unlimited: self.unlimited,
            label: self.label,
            windows: self
                .windows_json
                .as_deref()
                .and_then(|raw| serde_json::from_str(raw).ok())
                .unwrap_or_default(),
            detail: self
                .detail_json
                .as_deref()
                .and_then(|raw| serde_json::from_str(raw).ok())
                .unwrap_or(Value::Null),
            error_kind: self.error_kind,
            status_code: self.status_code,
            duration_ms: self.duration_ms.unwrap_or(0),
            checked_at: self.checked_at,
        }
    }
}

async fn load_snapshot(
    db: &Database,
    channel_id: &str,
) -> Result<Option<BalanceSnapshot>, sqlx::Error> {
    let row = sqlx::query_as::<_, SnapshotRow>(
        "SELECT adapter, status, remaining, currency, used, total, unlimited, label, \
                windows_json, detail_json, error_kind, status_code, duration_ms, checked_at \
         FROM channel_balance_snapshots WHERE channel_id=?",
    )
    .bind(channel_id)
    .fetch_optional(db.pool())
    .await?;
    Ok(row.map(|row| row.into_snapshot(channel_id)))
}

/// Read-only balance summary embedded into every channel JSON (admin list /
/// get). Never reports `configured=true` for a channel without a config row.
pub async fn channel_balance_json(db: &Database, channel_id: &str) -> Result<Value, sqlx::Error> {
    let config: Option<(String, bool)> =
        sqlx::query_as("SELECT adapter, enabled FROM channel_balance_configs WHERE channel_id=?")
            .bind(channel_id)
            .fetch_optional(db.pool())
            .await?;
    let snapshot = load_snapshot(db, channel_id).await?;
    Ok(json!({
        "configured": config.is_some(),
        "enabled": config.as_ref().map(|(_, enabled)| *enabled).unwrap_or(false),
        "adapter": config.as_ref().map(|(adapter, _)| adapter.clone()),
        "snapshot": snapshot
            .map(|snapshot| serde_json::to_value(snapshot).unwrap_or(Value::Null))
            .unwrap_or(Value::Null),
    }))
}

/// Validate and normalize one `PUT balance-config` body.
fn normalize_config(
    input: &BalanceConfigInput,
) -> Result<(NormalizedConfig, TokenUpdate), BalanceError> {
    let adapter = BalanceAdapter::parse(&input.adapter)?;
    let method = match adapter {
        BalanceAdapter::Custom => {
            let value = input
                .method
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("GET")
                .to_ascii_uppercase();
            if !matches!(value.as_str(), "GET" | "POST" | "PUT") {
                return Err(BalanceError::Invalid(
                    "自定义余额查询只支持 GET / POST / PUT".into(),
                ));
            }
            value
        }
        _ => "GET".into(),
    };
    let path = match adapter {
        BalanceAdapter::Custom => {
            let value = input
                .path
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| BalanceError::Invalid("自定义余额查询必须填写路径".into()))?;
            if value.chars().count() > MAX_CUSTOM_PATH_CHARS {
                return Err(BalanceError::Invalid(format!(
                    "余额查询路径不能超过 {MAX_CUSTOM_PATH_CHARS} 个字符"
                )));
            }
            Some(value.to_owned())
        }
        built_in => Some(built_in.default_path().unwrap_or_default().to_owned()),
    };
    let auth = match adapter {
        BalanceAdapter::Custom => BalanceAuth::parse(input.auth.as_deref().unwrap_or("bearer"))?,
        _ => BalanceAuth::Bearer,
    };
    if input.headers.len() > MAX_CUSTOM_HEADERS {
        return Err(BalanceError::Invalid(format!(
            "余额查询请求头不能超过 {MAX_CUSTOM_HEADERS} 条"
        )));
    }
    let mut headers = Vec::with_capacity(input.headers.len());
    for (name, value) in &input.headers {
        let name = name.trim();
        if name.is_empty()
            || HeaderName::from_bytes(name.as_bytes()).is_err()
            || HeaderValue::from_str(value).is_err()
        {
            return Err(BalanceError::Invalid(format!("余额查询请求头无效：{name}")));
        }
        headers.push((name.to_owned(), value.clone()));
    }
    let body = match adapter {
        BalanceAdapter::Custom => match input.body.as_deref().map(str::trim) {
            Some(body) if !body.is_empty() => {
                if body.len() > MAX_CUSTOM_BODY_BYTES {
                    return Err(BalanceError::Invalid(format!(
                        "余额查询请求体不能超过 {} KiB",
                        MAX_CUSTOM_BODY_BYTES / 1024
                    )));
                }
                Some(body.to_owned())
            }
            _ => None,
        },
        _ => None,
    };
    let mapping = match adapter {
        BalanceAdapter::Custom => BalanceMapping {
            remaining: mapping_path(&input.mapping.remaining)?,
            currency: mapping_path(&input.mapping.currency)?,
            used: mapping_path(&input.mapping.used)?,
            total: mapping_path(&input.mapping.total)?,
            label: mapping_path(&input.mapping.label)?,
        },
        _ => BalanceMapping::default(),
    };
    let token_update = if input.clear_token {
        TokenUpdate::Clear
    } else {
        match input.token.as_deref().map(str::trim) {
            Some(token) if !token.is_empty() => {
                if token.chars().count() > 65535 {
                    return Err(BalanceError::Invalid("余额令牌长度无效".into()));
                }
                TokenUpdate::Set(token.to_owned())
            }
            _ => TokenUpdate::Keep,
        }
    };
    Ok((
        NormalizedConfig {
            adapter,
            enabled: input.enabled,
            method,
            path,
            auth,
            headers,
            body,
            mapping,
        },
        token_update,
    ))
}

fn mapping_path(value: &Option<String>) -> Result<Option<String>, BalanceError> {
    match value.as_deref().map(str::trim) {
        Some(path) if !path.is_empty() => {
            if path.chars().count() > MAX_MAPPING_CHARS {
                return Err(BalanceError::Invalid(format!(
                    "映射路径不能超过 {MAX_MAPPING_CHARS} 个字符"
                )));
            }
            if !path.starts_with('$') {
                return Err(BalanceError::Invalid(format!(
                    "映射路径必须以 $ 开头：{path}"
                )));
            }
            Ok(Some(path.to_owned()))
        }
        _ => Ok(None),
    }
}

fn parse_method(value: &str) -> Option<Method> {
    match value.trim().to_ascii_uppercase().as_str() {
        "GET" => Some(Method::GET),
        "POST" => Some(Method::POST),
        "PUT" => Some(Method::PUT),
        _ => None,
    }
}

fn status_error_kind(status: u16) -> &'static str {
    match status {
        401 => error_kind::HTTP_401,
        403 => error_kind::HTTP_403,
        value if value >= 500 => error_kind::HTTP_5XX,
        _ => error_kind::HTTP_ERROR,
    }
}

/// Resolve the configured path against the channel base URL.
///
/// * `https://…` absolute;
/// * `/xxx` is site-root relative;
/// * anything else is appended to the base URL path;
/// * query strings survive all three forms.
pub fn resolve_url(base_url: &str, path: &str) -> anyhow::Result<Url> {
    let path = path.trim();
    if path.is_empty() {
        return Err(anyhow!("empty balance path"));
    }
    if let Ok(url) = Url::parse(path)
        && url.host_str().is_some()
    {
        if !matches!(url.scheme(), "http" | "https") {
            return Err(anyhow!("balance path must be an HTTP(S) URL"));
        }
        return Ok(url);
    }
    let base = Url::parse(base_url).context("channel base_url is not a valid URL")?;
    if !matches!(base.scheme(), "http" | "https") || base.host_str().is_none() {
        return Err(anyhow!("channel base_url is not an absolute HTTP(S) URL"));
    }
    if let Some(rest) = path.strip_prefix('/') {
        let (path_part, query) = split_query(rest);
        let mut url = base;
        url.set_path(&format!("/{path_part}"));
        url.set_query(query);
        url.set_fragment(None);
        Ok(url)
    } else {
        let (path_part, query) = split_query(path);
        protocol::upstream_url(base_url, path_part, query, "openai_compatible")
    }
}

fn split_query(path: &str) -> (&str, Option<&str>) {
    match path.split_once('?') {
        Some((path, query)) if !query.is_empty() => (path, Some(query)),
        Some((path, _)) => (path, None),
        None => (path, None),
    }
}

/// `${api_key}` / `${token}` substitution for custom headers and bodies.
/// The rendered value is only ever used on the wire — never persisted.
pub fn render_template(template: &str, api_key: &str, token: &str) -> String {
    template
        .replace("${api_key}", api_key)
        .replace("${token}", token)
}

/// Minimal JSON path subset: `$`, `.field` and `[index]`, e.g.
/// `$.data.items[0].balance`.
pub fn json_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut rest = path.trim().strip_prefix('$')?;
    let mut current = value;
    while !rest.is_empty() {
        if let Some(tail) = rest.strip_prefix('.') {
            let end = tail.find(['.', '[']).unwrap_or(tail.len());
            let key = &tail[..end];
            if key.is_empty() {
                return None;
            }
            current = current.get(key)?;
            rest = &tail[end..];
        } else {
            let tail = rest.strip_prefix('[')?;
            let end = tail.find(']')?;
            let index: usize = tail[..end].trim().parse().ok()?;
            current = current.get(index)?;
            rest = &tail[end + 1..];
        }
    }
    Some(current)
}

/// Number or numeric string to `f64` (upstreams disagree on JSON types).
pub fn number(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64().filter(|value| value.is_finite()),
        Value::String(value) => value
            .trim()
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite()),
        _ => None,
    }
}

/// Dispatch to the adapter parser.
fn parse_reading(adapter: BalanceAdapter, body: &[u8]) -> Result<BalanceReading, BalanceError> {
    match adapter {
        BalanceAdapter::NewApi => parse_newapi(body),
        BalanceAdapter::Sub2Api => parse_sub2api(body),
        BalanceAdapter::OpencodeGo => parse_opencode_go(body),
        BalanceAdapter::DeepSeek => parse_deepseek(body),
        BalanceAdapter::CommandCode => Err(BalanceError::Invalid(
            "command_code uses a multi-step query".into(),
        )),
        BalanceAdapter::Custom => Err(BalanceError::Invalid("custom needs a mapping".into())),
    }
}

fn invalid_payload() -> BalanceError {
    BalanceError::Invalid("invalid_payload".into())
}

/// New API / one-api forks: quota values at 500,000 per USD.
pub fn parse_newapi(body: &[u8]) -> Result<BalanceReading, BalanceError> {
    let value: Value = serde_json::from_slice(body).map_err(|_| invalid_payload())?;
    let data = value
        .get("data")
        .filter(|data| data.is_object())
        .ok_or_else(invalid_payload)?;
    let granted = data.get("total_granted").and_then(number);
    let used = data.get("total_used").and_then(number);
    let available = data
        .get("total_available")
        .and_then(number)
        .or(match (granted, used) {
            (Some(granted), Some(used)) => Some(granted - used),
            _ => None,
        });
    if granted.is_none() && used.is_none() && available.is_none() {
        return Err(invalid_payload());
    }
    let unlimited_flag = data
        .get("unlimited_quota")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    // A concrete quota always wins over the unlimited flag: several forks
    // set `unlimited_quota` while still returning real granted/available
    // values (CCTQ returns granted > 0 and a negative available when the
    // plan is overdrawn). Only the explicit `total_granted < 0` sentinel
    // without any usable number keeps the unlimited view.
    let numeric_quota = granted.is_some_and(|granted| granted >= 0.0)
        || available.is_some_and(|available| available >= 0.0);
    let unlimited =
        (unlimited_flag || granted.is_some_and(|granted| granted < 0.0)) && !numeric_quota;
    let scale = |value: Option<f64>| value.map(|value| value / NEW_API_QUOTA_PER_USD);
    let mut detail = serde_json::Map::new();
    if let Some(expires_at) = data.get("expires_at").and_then(number) {
        detail.insert("expires_at".into(), json!(expires_at));
    }
    Ok(BalanceReading {
        remaining: if unlimited { None } else { scale(available) },
        currency: Some("USD".into()),
        used: scale(used),
        total: scale(granted),
        unlimited,
        label: data
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .filter(|name| !name.is_empty()),
        windows: Vec::new(),
        detail: Value::Object(detail),
    })
}

/// New API account balance (`GET /api/user/self`) using a dashboard PAT.
/// `quota` is the account's remaining quota, `used_quota` the consumed one,
/// both in 500,000-per-USD quota units.
pub fn parse_newapi_user(body: &[u8]) -> Result<BalanceReading, BalanceError> {
    let value: Value = serde_json::from_slice(body).map_err(|_| invalid_payload())?;
    let data = value
        .get("data")
        .filter(|data| data.is_object())
        .ok_or_else(invalid_payload)?;
    let quota = data.get("quota").and_then(number);
    let used = data.get("used_quota").and_then(number);
    if quota.is_none() && used.is_none() {
        return Err(invalid_payload());
    }
    let scale = |value: Option<f64>| value.map(|value| value / NEW_API_QUOTA_PER_USD);
    let total = match (quota, used) {
        (Some(quota), Some(used)) => scale(Some(quota + used)),
        _ => scale(quota),
    };
    let mut detail = serde_json::Map::new();
    if let Some(request_count) = data.get("request_count").and_then(number) {
        detail.insert("request_count".into(), json!(request_count));
    }
    if let Some(group) = data.get("group").and_then(Value::as_str) {
        detail.insert("group".into(), json!(group));
    }
    Ok(BalanceReading {
        remaining: scale(quota),
        currency: Some("USD".into()),
        used: scale(used),
        total,
        unlimited: false,
        label: data
            .get("display_name")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
            .or_else(|| data.get("username").and_then(Value::as_str))
            .map(str::to_owned),
        windows: Vec::new(),
        detail: Value::Object(detail),
    })
}

/// Sub2API `/v1/usage` (quota_limited / unrestricted modes).
pub fn parse_sub2api(body: &[u8]) -> Result<BalanceReading, BalanceError> {
    let value: Value = serde_json::from_slice(body).map_err(|_| invalid_payload())?;
    let mode = value
        .get("mode")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let quota = value.get("quota").filter(|quota| quota.is_object());
    let remaining = quota
        .and_then(|quota| quota.get("remaining"))
        .and_then(number)
        .or_else(|| value.get("remaining").and_then(number));
    let total = quota
        .and_then(|quota| quota.get("limit"))
        .and_then(number)
        .or_else(|| value.get("total").and_then(number));
    let used = quota
        .and_then(|quota| quota.get("used"))
        .and_then(number)
        .or_else(|| value.get("used").and_then(number));
    if remaining.is_none() && total.is_none() && used.is_none() {
        // Only a response without any concrete number can be "unrestricted";
        // when the upstream returns quota values they are displayed even if
        // the mode says unrestricted.
        if mode == "unrestricted" {
            return Ok(BalanceReading {
                unlimited: true,
                label: value
                    .get("status")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                detail: sub2api_detail(&value),
                ..BalanceReading::default()
            });
        }
        return Err(invalid_payload());
    }
    let currency = quota
        .and_then(|quota| quota.get("unit"))
        .and_then(Value::as_str)
        .or_else(|| value.get("unit").and_then(Value::as_str))
        .map(str::to_owned);
    Ok(BalanceReading {
        remaining,
        currency,
        used,
        total,
        unlimited: false,
        label: value
            .get("status")
            .and_then(Value::as_str)
            .map(str::to_owned),
        windows: Vec::new(),
        detail: sub2api_detail(&value),
    })
}

fn sub2api_detail(value: &Value) -> Value {
    let mut detail = serde_json::Map::new();
    if let Some(mode) = value.get("mode") {
        detail.insert("mode".into(), mode.clone());
    }
    if let Some(valid) = value.get("isValid") {
        detail.insert("is_valid".into(), valid.clone());
    }
    if let Some(usage) = value.get("usage") {
        detail.insert("usage".into(), usage.clone());
    }
    Value::Object(detail)
}

/// OpenCode Zen / Go `/v1/usage`: three rolling windows, percent used only.
pub fn parse_opencode_go(body: &[u8]) -> Result<BalanceReading, BalanceError> {
    let value: Value = serde_json::from_slice(body).map_err(|_| invalid_payload())?;
    let usage = value
        .get("usage")
        .filter(|usage| usage.is_object())
        .ok_or_else(invalid_payload)?;
    let mut windows = Vec::new();
    for (key, label) in [("rolling", "5h"), ("weekly", "周"), ("monthly", "月")] {
        let Some(window) = usage.get(key) else {
            continue;
        };
        let status_ok = window
            .get("status")
            .and_then(Value::as_str)
            .is_some_and(|status| status == "ok");
        let Some(used_percent) = window.get("percent").and_then(number) else {
            continue;
        };
        if !status_ok || !(0.0..=100.0).contains(&used_percent) {
            continue;
        }
        windows.push(QuotaWindow {
            label: label.into(),
            used_percent,
            remaining_percent: 100.0 - used_percent,
            resets_at: window
                .get("resetsAt")
                .and_then(Value::as_str)
                .map(str::to_owned),
        });
    }
    if windows.is_empty() {
        return Err(invalid_payload());
    }
    Ok(BalanceReading {
        windows,
        ..BalanceReading::default()
    })
}

/// Command Code `whoami`: `org.id`, `orgId` or `org.orgId`.
pub fn command_code_org_id(whoami: &[u8]) -> Option<String> {
    let value: Value = serde_json::from_slice(whoami).ok()?;
    // 官方 CLI 读取 `data.org.id`（`/alpha/whoami` 的 usage 包装），社区实现
    // 读取顶层 `org.id`；两种形状都必须兼容。
    [
        value.pointer("/data/org/id"),
        value.pointer("/data/orgId"),
        value.pointer("/data/org_id"),
        value.pointer("/org/id"),
        value.get("orgId"),
        value.pointer("/org/orgId"),
        value.get("org_id"),
    ]
    .into_iter()
    .flatten()
    .find_map(|value| value.as_str().map(str::to_owned).filter(|id| !id.is_empty()))
}

/// Command Code 账号名（whoami 顶层 `user`/`data.user` 两种形状）。
pub fn command_code_account_name(whoami: &[u8]) -> Option<String> {
    let value: Value = serde_json::from_slice(whoami).ok()?;
    [
        value.pointer("/user/userName"),
        value.pointer("/user/name"),
        value.pointer("/data/user/userName"),
        value.pointer("/data/user/name"),
        value.pointer("/org/login"),
        value.pointer("/data/org/login"),
    ]
    .into_iter()
    .flatten()
    .find_map(|value| value.as_str().map(str::to_owned).filter(|name| !name.is_empty()))
}

/// 把窗口重置时间统一成 RFC3339（秒/毫秒时间戳或 ISO 字符串）。
fn quota_reset_at(value: &Value) -> Option<String> {
    // `resetAt: 0`（以及负数）表示“窗口尚未消耗、没有待重置时间”，不能
    // 当成 1970 年展示。
    let normalize = |epoch: i64| {
        if epoch <= 0 {
            return None;
        }
        let (seconds, nanos) = if epoch > 1_000_000_000_000 {
            (epoch / 1000, ((epoch % 1000) * 1_000_000) as u32)
        } else {
            (epoch, 0)
        };
        chrono::DateTime::from_timestamp(seconds, nanos).map(|value| value.to_rfc3339())
    };
    match value {
        Value::String(text) => {
            let text = text.trim();
            if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(text) {
                let parsed = parsed.with_timezone(&chrono::Utc);
                return (parsed.timestamp() > 0).then(|| parsed.to_rfc3339());
            }
            let epoch = text.parse::<i64>().ok()?;
            normalize(epoch)
        }
        Value::Number(number) => normalize(number.as_i64()?),
        _ => None,
    }
}

/// Command Code 额度：credits + 5h/周窗口 + 可选用量汇总/订阅。
///
/// Go 套餐的 `monthlyCredits/purchasedCredits/freeCredits` 可能全为 0，额度体现
/// 在 `windowLimits.fiveHour/weekly`；因此**零 credits 不是无效负载**。
///
/// - `remaining = monthlyCredits + purchasedCredits + freeCredits`（>0 才展示）
/// - `used = summary.totalCost`
/// - `total = remaining + used`（credits 未知时为 None，由窗口展示）
/// - `fiveHour` / `weekly` → `QuotaWindow{label:"5h"/"周"}`，
///   `used_percent = used/cap * 100`，`resets_at` 统一为 RFC3339。
pub fn parse_command_code(
    whoami: &[u8],
    credits: &[u8],
    summary: Option<&[u8]>,
    subscription: Option<&[u8]>,
    org_id: &str,
) -> Result<BalanceReading, BalanceError> {
    let value: Value = serde_json::from_slice(credits).map_err(|_| invalid_payload())?;
    let credits_object = value.get("credits").filter(|value| value.is_object());
    let monthly = credits_object
        .and_then(|value| value.get("monthlyCredits"))
        .and_then(number)
        .unwrap_or(0.0);
    let purchased = credits_object
        .and_then(|value| value.get("purchasedCredits"))
        .and_then(number)
        .unwrap_or(0.0);
    let free = credits_object
        .and_then(|value| value.get("freeCredits"))
        .and_then(number)
        .unwrap_or(0.0);
    // 兼容 windowLimits 嵌套与顶层两种形状。
    let limits = value
        .get("windowLimits")
        .filter(|value| value.is_object())
        .or_else(|| Some(&value));
    let mut windows: Vec<QuotaWindow> = Vec::new();
    if let Some(limits) = limits {
        for (key, label) in [("fiveHour", "5h"), ("weekly", "周")] {
            let Some(window) = limits.get(key).filter(|value| value.is_object()) else {
                continue;
            };
            let used = window.get("used").and_then(number).unwrap_or(0.0);
            let cap = window.get("cap").and_then(number).unwrap_or(0.0);
            if cap <= 0.0 {
                continue;
            }
            let used_percent = (used / cap * 100.0).clamp(0.0, 100.0);
            windows.push(QuotaWindow {
                label: label.to_owned(),
                used_percent,
                remaining_percent: 100.0 - used_percent,
                resets_at: window.get("resetAt").and_then(quota_reset_at),
            });
        }
    }
    let summary_value: Option<Value> =
        summary.and_then(|body| serde_json::from_slice::<Value>(body).ok());
    let subscription_value: Option<Value> =
        subscription.and_then(|body| serde_json::from_slice::<Value>(body).ok());
    let used = summary_value
        .as_ref()
        .and_then(|value| value.get("totalCost"))
        .and_then(number);
    let plan_id = value
        .get("planId")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            subscription_value
                .as_ref()
                .and_then(|value| value.pointer("/data/planId"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        });
    let plan_status = subscription_value
        .as_ref()
        .and_then(|value| value.pointer("/data/status"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    // 只有“既没有 credits 结构、也没有窗口、没有汇总/订阅”才视为无效负载：
    // 单个账号 credits 全为 0 是完全合法的 Go 套餐。
    if credits_object.is_none() && windows.is_empty() && summary_value.is_none() {
        return Err(invalid_payload());
    }
    let credits_total = monthly + purchased + free;
    let remaining = (credits_total > 0.0).then_some(credits_total);
    Ok(BalanceReading {
        remaining,
        currency: None,
        used,
        total: remaining.map(|value| value + used.unwrap_or(0.0)),
        unlimited: false,
        label: remaining.map(|_| "Credits".to_owned()),
        windows,
        detail: json!({
            "orgId": org_id,
            "account": command_code_account_name(whoami),
            "planId": plan_id,
            "planStatus": plan_status,
            "monthlyCredits": monthly,
            "purchasedCredits": purchased,
            "freeCredits": free,
            "totalCost": used,
            "totalCount": summary_value
                .as_ref()
                .and_then(|value| value.get("totalCount"))
                .and_then(number),
        }),
    })
}

/// DeepSeek `/user/balance`: string amounts, one entry per currency; the
/// first entry is the primary display balance.
pub fn parse_deepseek(body: &[u8]) -> Result<BalanceReading, BalanceError> {
    let value: Value = serde_json::from_slice(body).map_err(|_| invalid_payload())?;
    let infos = value
        .get("balance_infos")
        .filter(|infos| infos.is_array())
        .ok_or_else(invalid_payload)?;
    let balances: Vec<Value> = infos.as_array().cloned().unwrap_or_default();
    let first = balances.first();
    let remaining = first
        .and_then(|entry| entry.get("total_balance"))
        .and_then(number);
    let currency = first
        .and_then(|entry| entry.get("currency"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let detail = json!({
        "is_available": value.get("is_available").cloned().unwrap_or(Value::Null),
        "balances": balances,
    });
    if first.is_none() && value.get("is_available").and_then(Value::as_bool) == Some(true) {
        return Err(invalid_payload());
    }
    Ok(BalanceReading {
        remaining,
        currency,
        unlimited: false,
        detail,
        ..BalanceReading::default()
    })
}

/// Custom adapter: extract fields through the configured JSON paths.
pub fn parse_custom(body: &[u8], mapping: &BalanceMapping) -> Result<BalanceReading, BalanceError> {
    let value: Value = serde_json::from_slice(body).map_err(|_| invalid_payload())?;
    let read = |path: &Option<String>| -> Option<Value> {
        path.as_deref()
            .and_then(|path| json_path(&value, path))
            .cloned()
    };
    let remaining = read(&mapping.remaining).as_ref().and_then(number);
    let used = read(&mapping.used).as_ref().and_then(number);
    let total = read(&mapping.total).as_ref().and_then(number);
    if remaining.is_none() && used.is_none() && total.is_none() {
        return Err(invalid_payload());
    }
    Ok(BalanceReading {
        remaining,
        currency: read(&mapping.currency).and_then(|value| value.as_str().map(str::to_owned)),
        used,
        total,
        unlimited: false,
        label: read(&mapping.label).and_then(|value| match value {
            Value::String(label) => Some(label),
            Value::Number(label) => Some(label.to_string()),
            _ => None,
        }),
        windows: Vec::new(),
        detail: Value::Null,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::Context;
    use crate::config::AppConfig;
    use crate::db::Database;
    use crate::ports::{UpstreamBody, UpstreamResponse};
    use crate::runtime::RuntimeLimits;
    use futures_util::future::BoxFuture;
    use parking_lot::Mutex;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio_util::sync::CancellationToken;

    // ---------------------------------------------------------------- pure

    #[test]
    fn resolve_url_handles_absolute_root_relative_and_query() {
        let base = "https://relay.example.com/zen/go";
        assert_eq!(
            resolve_url(base, "https://absolute.example.com/api?x=1")
                .unwrap()
                .as_str(),
            "https://absolute.example.com/api?x=1"
        );
        assert_eq!(
            resolve_url(base, "/api/usage/token/").unwrap().as_str(),
            "https://relay.example.com/api/usage/token/"
        );
        assert_eq!(
            resolve_url(base, "v1/usage").unwrap().as_str(),
            "https://relay.example.com/zen/go/v1/usage"
        );
        assert_eq!(
            resolve_url(base, "/user/balance?currency=USD")
                .unwrap()
                .as_str(),
            "https://relay.example.com/user/balance?currency=USD"
        );
        assert_eq!(
            resolve_url(base, "v1/usage?window=month").unwrap().as_str(),
            "https://relay.example.com/zen/go/v1/usage?window=month"
        );
        assert!(resolve_url(base, "  ").is_err());
        assert!(resolve_url(base, "ftp://host/x").is_err());
    }

    const NEWAPI_BODY: &str = r#"{"code":true,"message":"ok","data":{"object":"token_usage","name":"默认令牌","total_granted":5000000,"total_used":1250000,"total_available":3750000,"unlimited_quota":false,"expires_at":0}}"#;

    #[test]
    fn newapi_converts_quota_and_handles_unlimited() {
        let reading = parse_newapi(NEWAPI_BODY.as_bytes()).unwrap();
        assert_eq!(reading.currency.as_deref(), Some("USD"));
        assert_eq!(reading.remaining, Some(7.5));
        assert_eq!(reading.used, Some(2.5));
        assert_eq!(reading.total, Some(10.0));
        assert!(!reading.unlimited);
        assert_eq!(reading.label.as_deref(), Some("默认令牌"));

        let body = r#"{"data":{"total_granted":-1,"total_used":10,"total_available":-11,"unlimited_quota":true}}"#;
        let reading = parse_newapi(body.as_bytes()).unwrap();
        assert!(reading.unlimited, "unlimited_quota must win");
        assert_eq!(reading.remaining, None);

        // total_granted < 0 alone is also unlimited.
        let body = r#"{"data":{"total_granted":-1,"total_used":10}}"#;
        let reading = parse_newapi(body.as_bytes()).unwrap();
        assert!(reading.unlimited);
        assert_eq!(reading.remaining, None);

        // A concrete non-negative balance wins even when the upstream set the
        // unlimited flag (fork data: flag true while the quota is numeric).
        let body =
            r#"{"data":{"total_granted":5000000,"total_used":1250000,"unlimited_quota":true}}"#;
        let reading = parse_newapi(body.as_bytes()).unwrap();
        assert!(!reading.unlimited);
        assert_eq!(reading.remaining, Some(7.5));

        // ... including when total_granted used the -1 sentinel but a real
        // available amount is present.
        let body =
            r#"{"data":{"total_granted":-1,"total_available":3750000,"unlimited_quota":true}}"#;
        let reading = parse_newapi(body.as_bytes()).unwrap();
        assert!(!reading.unlimited);
        assert_eq!(reading.remaining, Some(7.5));

        // Real CCTQ shape: the plan is overdrawn (available < 0), yet
        // total_granted > 0 proves the quota is numeric. `unlimited_quota`
        // must NOT hide the concrete (negative) available amount.
        let body = r#"{"code":true,"data":{"name":"codex","total_granted":3276396,"total_used":4640192,"total_available":-1363796,"unlimited_quota":true},"message":"ok"}"#;
        let reading = parse_newapi(body.as_bytes()).unwrap();
        assert!(!reading.unlimited);
        let remaining = reading.remaining.expect("negative available must be kept");
        assert!((remaining + 2.727592).abs() < 1e-9, "remaining={remaining}");
        assert!((reading.total.unwrap() - 6.552792).abs() < 1e-9);
        assert!((reading.used.unwrap() - 9.280384).abs() < 1e-9);

        assert!(parse_newapi(b"{}").is_err());
        assert!(parse_newapi(b"not json").is_err());
    }

    #[test]
    fn newapi_user_reads_account_balance() {
        let body = r#"{"success":true,"message":"","data":{"username":"alice","display_name":"Alice","quota":12925000,"used_quota":4640192,"request_count":321,"group":"default"}}"#;
        let reading = parse_newapi_user(body.as_bytes()).unwrap();
        assert_eq!(reading.remaining, Some(25.85));
        assert_eq!(reading.used, Some(9.280384));
        let total = reading.total.unwrap();
        assert!((total - 35.130384).abs() < 1e-9, "total={total}");
        assert_eq!(reading.currency.as_deref(), Some("USD"));
        assert_eq!(reading.label.as_deref(), Some("Alice"));
        assert_eq!(reading.detail["request_count"].as_f64(), Some(321.0));

        let body = r#"{"success":true,"data":{"username":"bob","quota":500000,"used_quota":0}}"#;
        let reading = parse_newapi_user(body.as_bytes()).unwrap();
        assert_eq!(reading.remaining, Some(1.0));
        assert_eq!(reading.label.as_deref(), Some("bob"));

        assert!(parse_newapi_user(b"{}").is_err());
        assert!(parse_newapi_user(b"not json").is_err());
    }

    #[test]
    fn sub2api_reads_limited_and_unrestricted_modes() {
        let body = r#"{"mode":"quota_limited","isValid":true,"status":"active","quota":{"limit":10,"used":1.2,"remaining":8.8,"unit":"USD"},"remaining":8.8,"unit":"USD","usage":{"today":{"requests":12,"cost":0.3,"tokens":15234},"total":{"requests":300,"cost":1.2,"tokens":402133}}}"#;
        let reading = parse_sub2api(body.as_bytes()).unwrap();
        assert_eq!(reading.remaining, Some(8.8));
        assert_eq!(reading.used, Some(1.2));
        assert_eq!(reading.total, Some(10.0));
        assert_eq!(reading.currency.as_deref(), Some("USD"));
        assert_eq!(reading.label.as_deref(), Some("active"));
        assert_eq!(reading.detail["usage"]["total"]["cost"], 1.2);

        let body = r#"{"mode":"unrestricted","isValid":true,"status":"active"}"#;
        let reading = parse_sub2api(body.as_bytes()).unwrap();
        assert!(reading.unlimited);
        assert_eq!(reading.remaining, None);

        // Unrestricted mode with concrete numbers still displays them.
        let body = r#"{"mode":"unrestricted","isValid":true,"status":"active","remaining":12.5,"unit":"USD"}"#;
        let reading = parse_sub2api(body.as_bytes()).unwrap();
        assert!(!reading.unlimited);
        assert_eq!(reading.remaining, Some(12.5));
        assert_eq!(reading.currency.as_deref(), Some("USD"));

        assert!(parse_sub2api(b"{\"mode\":\"quota_limited\"}").is_err());
    }

    #[test]
    fn deepseek_reads_string_amounts_and_lists_currencies() {
        let body = r#"{"is_available":true,"balance_infos":[{"currency":"CNY","total_balance":"110.00","granted_balance":"10.00","topped_up_balance":"100.00"}]}"#;
        let reading = parse_deepseek(body.as_bytes()).unwrap();
        assert_eq!(reading.remaining, Some(110.0));
        assert_eq!(reading.currency.as_deref(), Some("CNY"));
        assert_eq!(reading.detail["balances"].as_array().unwrap().len(), 1);
        assert_eq!(reading.detail["balances"][0]["granted_balance"], "10.00");

        let body = r#"{"is_available":false,"balance_infos":[]}"#;
        let reading = parse_deepseek(body.as_bytes()).unwrap();
        assert_eq!(reading.remaining, None);
        assert_eq!(reading.detail["is_available"], false);

        assert!(parse_deepseek(b"{}").is_err());
    }

    const OPENCODE_BODY: &str = r#"{"usage":{"rolling":{"status":"ok","percent":37.5,"resetsAt":"2026-09-11T20:00:00Z"},"weekly":{"status":"error","percent":12.0,"resetsAt":null},"monthly":{"status":"ok","percent":4.5,"resetsAt":"2026-10-01T00:00:00Z"}}}"#;

    #[test]
    fn opencode_windows_convert_percent_and_keep_stable_order() {
        let reading = parse_opencode_go(OPENCODE_BODY.as_bytes()).unwrap();
        let labels: Vec<&str> = reading
            .windows
            .iter()
            .map(|window| window.label.as_str())
            .collect();
        assert_eq!(labels, vec!["5h", "月"], "non-ok windows are skipped");
        assert_eq!(reading.windows[0].used_percent, 37.5);
        assert_eq!(reading.windows[0].remaining_percent, 62.5);
        assert_eq!(reading.windows[1].remaining_percent, 95.5);
        assert_eq!(
            reading.windows[0].resets_at.as_deref(),
            Some("2026-09-11T20:00:00Z")
        );

        let body = r#"{"usage":{"rolling":{"status":"ok","percent":120},"weekly":{"status":"ok","percent":-1}}}"#;
        assert!(parse_opencode_go(body.as_bytes()).is_err());
        assert!(parse_opencode_go(b"{}").is_err());
    }

    #[test]
    fn json_path_supports_documented_subset() {
        let value = serde_json::json!({"data":{"items":[{"balance":"12.34"}],"flag":true}});
        assert_eq!(
            json_path(&value, "$.data.items[0].balance").unwrap(),
            &serde_json::json!("12.34")
        );
        assert_eq!(json_path(&value, "$").unwrap(), &value);
        assert!(json_path(&value, "$.data.items[1]").is_none());
        assert!(json_path(&value, "data.items").is_none());
        assert!(json_path(&value, "$.data.items[x]").is_none());
    }

    #[test]
    fn numbers_accept_strings_and_reject_non_finite() {
        assert_eq!(number(&serde_json::json!(12.5)), Some(12.5));
        assert_eq!(number(&serde_json::json!(" 12.50 ")), Some(12.5));
        assert_eq!(number(&serde_json::json!("abc")), None);
        assert_eq!(number(&serde_json::json!(null)), None);
    }

    #[test]
    fn render_template_replaces_both_placeholders() {
        let rendered = render_template(
            "Bearer ${token}; key=${api_key}",
            "sk-channel",
            "panel-token",
        );
        assert_eq!(rendered, "Bearer panel-token; key=sk-channel");
        assert_eq!(render_template("plain", "a", "b"), "plain");
    }

    #[test]
    fn custom_parser_maps_optional_fields() {
        let body = br#"{"result":{"left":"8.80","unit":"USD","spent":1.2}}"#;
        let mapping = BalanceMapping {
            remaining: Some("$.result.left".into()),
            currency: Some("$.result.unit".into()),
            used: Some("$.result.spent".into()),
            ..BalanceMapping::default()
        };
        let reading = parse_custom(body, &mapping).unwrap();
        assert_eq!(reading.remaining, Some(8.8));
        assert_eq!(reading.currency.as_deref(), Some("USD"));
        assert_eq!(reading.used, Some(1.2));
        assert_eq!(reading.total, None);

        let empty = BalanceMapping::default();
        assert!(parse_custom(body, &empty).is_err());
        assert!(parse_custom(b"nope", &mapping).is_err());
    }

    // ------------------------------------------------------- mock upstream

    #[derive(Clone)]
    struct RecordedRequest {
        method: Method,
        url: String,
        body: Option<Vec<u8>>,
        headers: HeaderMap,
    }

    enum MockReply {
        Json(u16, String),
        Error(UpstreamError),
        Pending,
    }

    struct MockUpstream {
        calls: AtomicUsize,
        requests: Mutex<Vec<RecordedRequest>>,
        handler: Box<dyn Fn(&RecordedRequest) -> MockReply + Send + Sync>,
    }

    impl MockUpstream {
        fn new(
            handler: impl Fn(&RecordedRequest) -> MockReply + Send + Sync + 'static,
        ) -> Arc<Self> {
            Arc::new(Self {
                calls: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
                handler: Box::new(handler),
            })
        }

        fn always(status: u16, body: &'static str) -> Arc<Self> {
            Self::new(move |_| MockReply::Json(status, body.to_owned()))
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        fn requests(&self) -> Vec<RecordedRequest> {
            self.requests.lock().clone()
        }
    }

    impl UpstreamClient for MockUpstream {
        fn send(
            &self,
            request: UpstreamRequest,
        ) -> BoxFuture<'static, Result<UpstreamResponse, UpstreamError>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let recorded = RecordedRequest {
                method: request.method.clone(),
                url: request.url.to_string(),
                body: request.body.as_ref().map(|body| body.to_vec()),
                headers: request.headers.clone(),
            };
            self.requests.lock().push(recorded.clone());
            let reply = (self.handler)(&recorded);
            Box::pin(async move {
                match reply {
                    MockReply::Json(status, body) => Ok(UpstreamResponse {
                        status: StatusCode::from_u16(status).expect("valid test status"),
                        headers: HeaderMap::new(),
                        body: UpstreamBody::new(Box::pin(futures_util::stream::iter(vec![Ok(
                            Bytes::from(body),
                        )]))),
                    }),
                    MockReply::Error(error) => Err(error),
                    MockReply::Pending => {
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        Err(UpstreamError::Transport("pending test upstream".into()))
                    }
                }
            })
        }
    }

    async fn test_state(
        mock: Arc<dyn UpstreamClient>,
        limits: RuntimeLimits,
    ) -> (Context, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("lagw-balance-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = Database::open(&dir.join("test.db")).await.unwrap();
        let secrets = crate::crypto::SecretStore::load(&dir.join("master.key"))
            .await
            .unwrap();
        let (telemetry, _rx) = crate::telemetry::Telemetry::new(1000);
        let routes: Arc<dyn crate::ports::RouteRepository> =
            crate::infrastructure::SqliteRouteRepository::new(db.clone());
        let channels: Arc<dyn crate::ports::ChannelRepository> =
            crate::infrastructure::SqliteChannelRepository::new(db.clone());
        let clock: Arc<dyn crate::ports::Clock> = Arc::new(crate::infrastructure::SystemClock);
        let background = crate::infrastructure::RuntimeSupervisor::new(CancellationToken::new());
        let limits = Arc::new(limits);
        let discovery = crate::discovery::DiscoveryService::new(
            db.clone(),
            secrets.clone(),
            Arc::clone(&mock),
            Arc::clone(&channels),
            Arc::clone(&clock),
            Arc::clone(&background),
            Arc::clone(&limits),
        );
        let notifier: Arc<dyn crate::ports::Notifier> =
            crate::notification::DesktopNotifier::new(Duration::from_millis(50));
        let proxy = crate::proxy::ProxyService::new(
            db.clone(),
            secrets.clone(),
            Arc::clone(&mock),
            routes.clone(),
            telemetry.clone(),
            Arc::clone(&clock),
            Arc::clone(&limits),
            crate::notification::DesktopNotifier::new(Duration::from_millis(50)),
        );
        let balance = BalanceService::new(
            db.clone(),
            secrets.clone(),
            Arc::clone(&mock),
            Arc::clone(&channels),
            Arc::clone(&clock),
            Arc::clone(&limits),
        );
        let admin = crate::admin::AdminService::new(db.clone(), secrets.clone());
        let command_code_login = crate::commandcode_login::CommandCodeLogin::new(std::sync::Arc::clone(&mock));
        let state = Context {
            config: Arc::new(AppConfig::default()),
            db: db.clone(),
            secrets,
            http: mock,
            routes,
            channels,
            clock,
            notifier,
            discovery,
            proxy,
            telemetry,
            background,
            limits,
            admin,
            balance,
            command_code_login,
            recovery: crate::auth::RecoverySession::new(),
        };
        (state, dir)
    }

    async fn seed_channel(state: &Context, base_url: &str) {
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query(
            "INSERT INTO providers(id,name,base_url,created_at,updated_at) VALUES('prov-1','mock',?,?,?)",
        )
        .bind(base_url)
        .bind(time)
        .bind(time)
        .execute(state.db.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO channels(id,provider_id,name,protocol,api_key_encrypted,api_key_hint,manual_enabled,created_at,updated_at) VALUES('ch-1','prov-1','chan','openai_compatible',?,?,0,?,?)",
        )
        .bind(state.secrets.encrypt("sk-channel-secret"))
        .bind("sk-...cret")
        .bind(time)
        .bind(time)
        .execute(state.db.pool())
        .await
        .unwrap();
    }

    fn base_input(adapter: &str, enabled: bool) -> BalanceConfigInput {
        BalanceConfigInput {
            adapter: adapter.into(),
            enabled,
            method: None,
            path: None,
            auth: None,
            headers: BTreeMap::new(),
            body: None,
            mapping: BalanceMappingInput::default(),
            token: None,
            clear_token: false,
        }
    }

    async fn save_balance(state: &Context, adapter: &str, enabled: bool) -> BalanceConfig {
        state
            .balance
            .save_config("ch-1", base_input(adapter, enabled))
            .await
            .unwrap()
    }

    // ------------------------------------------------------ service paths

    #[tokio::test]
    async fn default_off_sends_no_request_at_all() {
        let mock = MockUpstream::always(200, NEWAPI_BODY);
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://relay.example.com").await;

        let refreshed = state.balance.refresh_enabled(&state).await.unwrap();
        assert!(refreshed.is_empty(), "no config row -> nothing to refresh");
        assert_eq!(mock.calls(), 0, "default-off must not touch the upstream");

        let error = state.balance.query(&state, "ch-1").await.unwrap_err();
        assert!(matches!(error, BalanceError::NotConfigured));
        assert_eq!(mock.calls(), 0, "manual query without config sends nothing");
    }

    #[tokio::test]
    async fn refresh_skips_disabled_but_manual_query_still_runs() {
        let mock = MockUpstream::always(200, NEWAPI_BODY);
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://relay.example.com").await;
        save_balance(&state, "newapi", false).await;

        let refreshed = state.balance.refresh_enabled(&state).await.unwrap();
        assert!(refreshed.is_empty());
        assert_eq!(mock.calls(), 0, "enabled=0 is the background switch");

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "ok");
        assert_eq!(mock.calls(), 1, "manual query is explicit intent");

        save_balance(&state, "newapi", true).await;
        let refreshed = state.balance.refresh_enabled(&state).await.unwrap();
        assert_eq!(refreshed.len(), 1);
        assert_eq!(mock.calls(), 2);
    }

    #[tokio::test]
    async fn refresh_enabled_covers_multiple_channels_in_parallel() {
        let mock = MockUpstream::always(200, NEWAPI_BODY);
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        let time = "2026-08-04T01:00:00+00:00";
        for index in 1..=3 {
            let provider_id = format!("prov-{index}");
            let channel_id = format!("ch-{index}");
            let base_url = format!("https://relay{index}.example.com");
            sqlx::query(
                "INSERT INTO providers(id,name,base_url,created_at,updated_at) VALUES(?,?,?,?,?)",
            )
            .bind(&provider_id)
            .bind(&provider_id)
            .bind(&base_url)
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
            sqlx::query("INSERT INTO channels(id,provider_id,name,protocol,api_key_encrypted,api_key_hint,manual_enabled,created_at,updated_at) VALUES(?,?,'chan','openai_compatible',?,?,0,?,?)")
                .bind(&channel_id)
                .bind(&provider_id)
                .bind(state.secrets.encrypt("sk-channel-secret"))
                .bind("sk-...cret")
                .bind(time)
                .bind(time)
                .execute(state.db.pool())
                .await
                .unwrap();
            sqlx::query("INSERT INTO channel_balance_configs(channel_id,adapter,enabled,method,path,auth,headers_json,body_json,mapping_json,created_at,updated_at) VALUES(?,'newapi',1,'GET','/api/usage/token/','bearer',NULL,NULL,NULL,?,?)")
                .bind(&channel_id)
                .bind(time)
                .bind(time)
                .execute(state.db.pool())
                .await
                .unwrap();
        }

        let refreshed = state.balance.refresh_enabled(&state).await.unwrap();
        assert_eq!(refreshed.len(), 3, "all enabled channels are covered");
        assert_eq!(mock.calls(), 3);
        for index in 1..=3 {
            let stored = state
                .balance
                .snapshot(&format!("ch-{index}"))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(stored.status, "ok");
            assert_eq!(stored.remaining, Some(7.5));
        }
    }

    #[tokio::test]
    async fn query_newapi_persists_normalized_snapshot() {
        let mock = MockUpstream::always(200, NEWAPI_BODY);
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://relay.example.com").await;
        save_balance(&state, "newapi", true).await;

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "ok");
        assert_eq!(snapshot.remaining, Some(7.5));
        assert_eq!(snapshot.used, Some(2.5));
        assert_eq!(snapshot.total, Some(10.0));
        assert_eq!(snapshot.currency.as_deref(), Some("USD"));
        assert_eq!(snapshot.error_kind, None);
        assert_eq!(snapshot.status_code, Some(200));
        assert!(!snapshot.checked_at.is_empty());

        assert_eq!(
            mock.requests()[0].url,
            "https://relay.example.com/api/usage/token/"
        );
        let stored = state.balance.snapshot("ch-1").await.unwrap().unwrap();
        assert_eq!(stored.remaining, Some(7.5));
        assert_eq!(stored.status, "ok");

        let embedded = crate::balance::channel_balance_json(&state.db, "ch-1")
            .await
            .unwrap();
        assert_eq!(embedded["configured"], true);
        assert_eq!(embedded["enabled"], true);
        assert_eq!(embedded["snapshot"]["remaining"], 7.5);
    }

    #[tokio::test]
    async fn newapi_with_dedicated_token_queries_account_balance() {
        const USER_SELF_BODY: &str = r#"{"success":true,"message":"","data":{"username":"alice","display_name":"Alice","quota":12925000,"used_quota":4640192,"request_count":321,"group":"default"}}"#;
        let mock = MockUpstream::new(|request| {
            if request.url.ends_with("/api/user/self") {
                MockReply::Json(200, USER_SELF_BODY.to_owned())
            } else {
                MockReply::Json(200, NEWAPI_BODY.to_owned())
            }
        });
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://relay.example.com").await;
        let mut input = base_input("newapi", true);
        input.token = Some("cctq-pat-token".into());
        state.balance.save_config("ch-1", input).await.unwrap();

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "ok");
        assert_eq!(snapshot.remaining, Some(25.85));
        assert_eq!(snapshot.used, Some(9.280384));
        assert_eq!(snapshot.status_code, Some(200));
        let recorded = mock.requests();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].url, "https://relay.example.com/api/user/self");
        assert_eq!(
            recorded[0]
                .headers
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap(),
            "Bearer cctq-pat-token",
            "account balance must use the dedicated PAT, not the sk- channel key"
        );
    }

    #[tokio::test]
    async fn query_classifies_http_errors_without_failing_the_api() {
        let mock = MockUpstream::always(401, r#"{"message":"bad key"}"#);
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://relay.example.com").await;
        save_balance(&state, "newapi", true).await;

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "error");
        assert_eq!(snapshot.error_kind.as_deref(), Some("http_401"));
        assert_eq!(snapshot.status_code, Some(401));
        assert_eq!(snapshot.remaining, None);
        assert!(state.balance.snapshot("ch-1").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn query_classifies_invalid_json() {
        let mock = MockUpstream::always(200, "definitely not json");
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://relay.example.com").await;
        save_balance(&state, "deepseek", true).await;

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "error");
        assert_eq!(snapshot.error_kind.as_deref(), Some("invalid_payload"));
        assert_eq!(snapshot.status_code, Some(200));
    }

    #[tokio::test]
    async fn query_classifies_transport_error() {
        let mock =
            MockUpstream::new(|_| MockReply::Error(UpstreamError::Transport("reset".into())));
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://relay.example.com").await;
        save_balance(&state, "newapi", true).await;

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "error");
        assert_eq!(snapshot.error_kind.as_deref(), Some("transport_error"));
        assert_eq!(snapshot.status_code, None);
        assert_eq!(mock.calls(), 1);
    }

    #[tokio::test]
    async fn query_classifies_timeout() {
        let mock = MockUpstream::new(|_| MockReply::Pending);
        let limits = RuntimeLimits {
            balance_timeout: Duration::from_millis(60),
            ..RuntimeLimits::default()
        };
        let (state, _dir) = test_state(mock.clone(), limits).await;
        seed_channel(&state, "https://relay.example.com").await;
        save_balance(&state, "newapi", true).await;

        let started = std::time::Instant::now();
        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "error");
        assert_eq!(snapshot.error_kind.as_deref(), Some("timeout"));
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn query_opencode_returns_three_window_snapshot() {
        let body = r#"{"usage":{"rolling":{"status":"ok","percent":37.5,"resetsAt":"2026-09-11T20:00:00Z"},"weekly":{"status":"ok","percent":12.0,"resetsAt":null},"monthly":{"status":"ok","percent":4.5,"resetsAt":"2026-10-01T00:00:00Z"}}}"#;
        let mock = MockUpstream::always(200, body);
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://opencode.ai/zen/go").await;
        save_balance(&state, "opencode_go", true).await;

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "ok");
        let labels: Vec<&str> = snapshot
            .windows
            .iter()
            .map(|window| window.label.as_str())
            .collect();
        assert_eq!(labels, vec!["5h", "周", "月"]);
        assert_eq!(snapshot.windows[0].remaining_percent, 62.5);
        assert_eq!(snapshot.windows[2].remaining_percent, 95.5);
        assert_eq!(
            mock.requests()[0].url,
            "https://opencode.ai/zen/go/v1/usage"
        );
        assert!(
            mock.requests()[0]
                .headers
                .contains_key("x-opencode-session"),
            "OpenCode upstreams need the session header"
        );
    }

    #[tokio::test]
    async fn delete_config_returns_to_default_off() {
        let mock = MockUpstream::always(200, NEWAPI_BODY);
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://relay.example.com").await;
        save_balance(&state, "newapi", true).await;
        state.balance.query(&state, "ch-1").await.unwrap();
        state.balance.delete_config("ch-1").await.unwrap();

        assert!(state.balance.snapshot("ch-1").await.unwrap().is_none());
        assert!(matches!(
            state.balance.query(&state, "ch-1").await.unwrap_err(),
            BalanceError::NotConfigured
        ));
        assert_eq!(mock.calls(), 1, "delete must not produce requests");
    }

    // ------------------------------------------------- custom and secrets

    #[tokio::test]
    async fn custom_adapter_renders_templates_and_never_persists_secrets() {
        let mock = MockUpstream::always(200, r#"{"data":{"balance":"12.34","spent":"1.66"}}"#);
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://relay.example.com/prefix").await;
        let mut headers = BTreeMap::new();
        headers.insert("X-Api-Key".into(), "${token}".into());
        let input = BalanceConfigInput {
            adapter: "custom".into(),
            enabled: true,
            method: Some("PUT".into()),
            path: Some("balance/current?source=gw".into()),
            auth: Some("none".into()),
            headers,
            body: Some(r#"{"key":"${api_key}","label":"${token}"}"#.into()),
            mapping: BalanceMappingInput {
                remaining: Some("$.data.balance".into()),
                used: Some("$.data.spent".into()),
                ..BalanceMappingInput::default()
            },
            token: None,
            clear_token: false,
        };
        let saved = state.balance.save_config("ch-1", input).await.unwrap();
        assert_eq!(saved.adapter, Some(BalanceAdapter::Custom));
        assert_eq!(saved.method, "PUT");
        assert_eq!(saved.auth, BalanceAuth::None);

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "ok");
        assert_eq!(snapshot.remaining, Some(12.34));
        assert_eq!(snapshot.used, Some(1.66));
        assert_eq!(snapshot.currency, None);

        let recorded = mock.requests()[0].clone();
        assert_eq!(recorded.method, Method::PUT);
        assert_eq!(
            recorded.url,
            "https://relay.example.com/prefix/balance/current?source=gw"
        );
        assert_eq!(
            recorded.headers.get("x-api-key").unwrap().to_str().unwrap(),
            "sk-channel-secret"
        );
        assert_eq!(
            recorded.body.as_deref(),
            Some(br#"{"key":"sk-channel-secret","label":"sk-channel-secret"}"#.as_slice())
        );

        // The rendered request (and therefore the channel key) must never be
        // written into the snapshot tables.
        let stored: (String,) = sqlx::query_as(
            "SELECT COALESCE(windows_json,'') || COALESCE(detail_json,'') || \
                    COALESCE(label,'') || COALESCE(currency,'') || COALESCE(error_kind,'') \
             FROM channel_balance_snapshots WHERE channel_id='ch-1'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert!(!stored.0.contains("sk-channel-secret"));
        let config_dump: (String,) = sqlx::query_as(
            "SELECT COALESCE(headers_json,'') || COALESCE(body_json,'') || \
                    COALESCE(mapping_json,'') || COALESCE(balance_token_hint,'') \
             FROM channel_balance_configs WHERE channel_id='ch-1'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert!(!config_dump.0.contains("sk-channel-secret"));
    }

    /// Minimal one-shot HTTP upstream that captures the raw request text and
    /// answers with `body`.
    async fn spawn_http_upstream(
        body: &'static str,
    ) -> (u16, tokio::sync::mpsc::UnboundedReceiver<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = Vec::new();
            let mut header_end: Option<usize> = None;
            loop {
                let mut chunk = [0u8; 4096];
                let read = match stream.read(&mut chunk).await {
                    Ok(0) | Err(_) => break,
                    Ok(read) => read,
                };
                buf.extend_from_slice(&chunk[..read]);
                if header_end.is_none() {
                    header_end = buf
                        .windows(4)
                        .position(|window| window == b"\r\n\r\n")
                        .map(|index| index + 4);
                }
                if let Some(end) = header_end {
                    let headers = String::from_utf8_lossy(&buf[..end]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .unwrap_or(0);
                    if buf.len() >= end + content_length {
                        break;
                    }
                }
            }
            let _ = tx.send(String::from_utf8_lossy(&buf).to_string());
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes()).await;
        });
        (port, rx)
    }

    /// The custom adapter's PUT (and its request body) must reach a real
    /// upstream through the shared HTTP client port.
    #[tokio::test]
    async fn custom_put_reaches_a_real_http_upstream_with_body() {
        let (port, mut requests) = spawn_http_upstream(r#"{"data":{"remaining":3.5}}"#).await;
        let http: Arc<dyn UpstreamClient> =
            Arc::new(crate::infrastructure::HttpClientPool::default());
        let (state, _dir) = test_state(http, RuntimeLimits::default()).await;
        seed_channel(&state, &format!("http://127.0.0.1:{port}/prefix")).await;
        let mut input = base_input("custom", true);
        input.method = Some("PUT".into());
        input.path = Some("balance".into());
        input.auth = Some("none".into());
        input.body = Some(r#"{"probe":"yes"}"#.into());
        input.mapping.remaining = Some("$.data.remaining".into());
        state.balance.save_config("ch-1", input).await.unwrap();

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "ok");
        assert_eq!(snapshot.remaining, Some(3.5));
        let request = requests
            .recv()
            .await
            .expect("upstream must see the request");
        assert!(
            request.starts_with("PUT /prefix/balance HTTP/1.1"),
            "unexpected request line: {request}"
        );
        assert!(
            request.contains(r#"{"probe":"yes"}"#),
            "PUT body missing: {request}"
        );
    }

    #[tokio::test]
    async fn dedicated_token_is_stored_encrypted_and_clearable() {
        let mock = MockUpstream::always(200, NEWAPI_BODY);
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://relay.example.com").await;
        let mut input = base_input("newapi", true);
        input.token = Some("panel-token-secret".into());
        let saved = state.balance.save_config("ch-1", input).await.unwrap();
        assert!(saved.has_token);
        assert!(saved.token_hint.is_some());

        let ciphertext: Vec<u8> = sqlx::query_scalar(
            "SELECT balance_token_encrypted FROM channel_balance_configs WHERE channel_id='ch-1'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_ne!(ciphertext, b"panel-token-secret");
        assert_eq!(
            state.secrets.decrypt(&ciphertext).unwrap(),
            "panel-token-secret"
        );

        // An empty token string keeps the stored one; clear_token removes it.
        let mut keep = base_input("newapi", true);
        keep.token = Some(String::new());
        state.balance.save_config("ch-1", keep).await.unwrap();
        assert!(state.balance.config("ch-1").await.unwrap().has_token);

        let mut clear = base_input("newapi", true);
        clear.clear_token = true;
        let cleared = state.balance.save_config("ch-1", clear).await.unwrap();
        assert!(!cleared.has_token);
        assert!(cleared.token_hint.is_none());
    }

    #[tokio::test]
    async fn save_config_rejects_invalid_custom_inputs() {
        let mock = MockUpstream::always(200, NEWAPI_BODY);
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://relay.example.com").await;

        // custom without a path
        let error = state
            .balance
            .save_config("ch-1", base_input("custom", true))
            .await
            .unwrap_err();
        assert!(matches!(error, BalanceError::Invalid(_)));

        // unknown adapter
        let error = state
            .balance
            .save_config("ch-1", base_input("auto", true))
            .await
            .unwrap_err();
        assert!(matches!(error, BalanceError::Invalid(_)));

        // too many headers
        let mut too_many = BTreeMap::new();
        for index in 0..=MAX_CUSTOM_HEADERS {
            too_many.insert(format!("X-Test-{index}"), "value".into());
        }
        let mut input = base_input("custom", true);
        input.path = Some("/balance".into());
        input.headers = too_many;
        let error = state.balance.save_config("ch-1", input).await.unwrap_err();
        assert!(matches!(error, BalanceError::Invalid(_)));

        // oversized body
        let mut input = base_input("custom", true);
        input.path = Some("/balance".into());
        input.body = Some("x".repeat(MAX_CUSTOM_BODY_BYTES + 1));
        let error = state.balance.save_config("ch-1", input).await.unwrap_err();
        assert!(matches!(error, BalanceError::Invalid(_)));

        // mapping path must be a JSON path
        let mut input = base_input("custom", true);
        input.path = Some("/balance".into());
        input.mapping.remaining = Some("data.balance".into());
        let error = state.balance.save_config("ch-1", input).await.unwrap_err();
        assert!(matches!(error, BalanceError::Invalid(_)));

        // unsupported method
        let mut input = base_input("custom", true);
        input.path = Some("/balance".into());
        input.method = Some("DELETE".into());
        let error = state.balance.save_config("ch-1", input).await.unwrap_err();
        assert!(matches!(error, BalanceError::Invalid(_)));
        assert_eq!(mock.calls(), 0, "validation must never hit the network");
    }

    // ------------------------------------------------------ maintenance

    #[tokio::test]
    async fn maintenance_refreshes_enabled_channels_on_its_interval() {
        let mock = MockUpstream::always(200, NEWAPI_BODY);
        let limits = RuntimeLimits {
            maintenance_interval: Duration::from_millis(20),
            balance_interval: Duration::from_millis(40),
            balance_timeout: Duration::from_secs(2),
            ..RuntimeLimits::default()
        };
        let (state, _dir) = test_state(mock.clone(), limits).await;
        seed_channel(&state, "https://relay.example.com").await;
        save_balance(&state, "newapi", true).await;

        let cancel = CancellationToken::new();
        let handle = tokio::spawn(crate::maintenance::run_supervisor(
            state.clone(),
            cancel.clone(),
        ));
        tokio::time::sleep(Duration::from_millis(260)).await;
        cancel.cancel();
        let _ = handle.await;

        assert!(
            mock.calls() >= 2,
            "maintenance must run the balance refresh repeatedly, calls={}",
            mock.calls()
        );
        let stored = state.balance.snapshot("ch-1").await.unwrap().unwrap();
        assert_eq!(stored.status, "ok");
    }
    #[test]
    fn command_code_quota_parses_credits_windows_and_summary() {
        let whoami = br#"{"org":{"id":"org_1","login":"me"}}"#;
        let credits = br#"{"credits":{"monthlyCredits":10,"purchasedCredits":5,"freeCredits":1},
            "windowLimits":{"fiveHour":{"used":3,"cap":6,"resetAt":"2026-09-12T10:00:00Z"},
                            "weekly":{"used":40,"cap":100,"resetAt":1789207200}}}"#;
        let summary = br#"{"totalCost":2.5,"totalCount":3,"totalTokens":100}"#;
        let reading =
            parse_command_code(whoami, credits, Some(summary), None, "org_1").unwrap();
        assert_eq!(reading.remaining, Some(16.0));
        assert_eq!(reading.used, Some(2.5));
        assert_eq!(reading.total, Some(18.5));
        assert_eq!(reading.label.as_deref(), Some("Credits"));
        assert_eq!(reading.windows.len(), 2);
        assert_eq!(reading.windows[0].label, "5h");
        assert!((reading.windows[0].used_percent - 50.0).abs() < 1e-9);
        assert_eq!(
            reading.windows[0].resets_at.as_deref(),
            Some("2026-09-12T10:00:00+00:00")
        );
        assert_eq!(reading.windows[1].label, "周");
        assert_eq!(reading.windows[1].used_percent, 40.0);
        assert_eq!(reading.windows[1].remaining_percent, 60.0);
        // 秒级时间戳统一成 RFC3339，前端 formatTime 才能正确显示。
        assert_eq!(
            reading.windows[1].resets_at.as_deref(),
            Some("2026-09-12T10:00:00+00:00")
        );
        assert_eq!(command_code_org_id(whoami).as_deref(), Some("org_1"));
        assert_eq!(
            command_code_org_id(br#"{"orgId":"org_2"}"#).as_deref(),
            Some("org_2")
        );
        // 官方 CLI 的 usage 包装形状：data.org.id。
        assert_eq!(
            command_code_org_id(br#"{"data":{"org":{"id":"org_3"}}}"#).as_deref(),
            Some("org_3")
        );
        assert!(command_code_org_id(br#"{"user":{"userName":"x"}}"#).is_none());
        // The usage summary is optional: credits/windows still parse.
        let fallback = parse_command_code(whoami, credits, None, None, "org_1").unwrap();
        assert_eq!(fallback.used, None);
        assert_eq!(fallback.total, Some(16.0));

        // Go 套餐：credits 全为 0，额度只在 5h/周窗口里 —— 必须解析成功而不是
        // invalid_payload（历史 bug）。
        let go_credits = br#"{"credits":{"monthlyCredits":0,"purchasedCredits":0,"freeCredits":0},
            "windowLimits":{"fiveHour":{"used":10,"cap":60,"resetAt":1789210700},
                            "weekly":{"used":20,"cap":200,"resetAt":"2026-09-19T10:00:00Z"}},
            "planId":"go"}"#;
        let go = parse_command_code(
            br#"{"user":{"userName":"alice"},"org":{"id":"org_1"}}"#,
            go_credits,
            Some(summary),
            Some(br#"{"data":{"planId":"go","status":"active"}}"#),
            "org_1",
        )
        .unwrap();
        assert_eq!(go.remaining, None, "zero credits must not be shown as 0.00");
        assert_eq!(go.used, Some(2.5));
        assert_eq!(go.total, None);
        assert_eq!(go.windows.len(), 2);
        assert!((go.windows[0].used_percent - (10.0 / 60.0 * 100.0)).abs() < 1e-9);
        assert_eq!(go.detail["planId"], "go");
        assert_eq!(go.detail["planStatus"], "active");
        assert_eq!(go.detail["account"], "alice");

        // 既没有 credits 结构、也没有窗口/汇总才是无效负载。
        assert!(parse_command_code(whoami, b"{}", None, None, "org_1").is_err());
    }

    /// Command Code quota is a three-step read-only flow: whoami → credits →
    /// usage summary, all with the channel API key.
    #[tokio::test]
    async fn command_code_balance_queries_whoami_credits_and_summary() {
        let mock = MockUpstream::new(|request| {
            if request.url.contains("/alpha/whoami") {
                MockReply::Json(200, r#"{"org":{"id":"org_9","login":"me"}}"#.into())
            } else if request.url.contains("/alpha/billing/credits") {
                MockReply::Json(
                    200,
                    r#"{"credits":{"monthlyCredits":10,"purchasedCredits":5,"freeCredits":1},
                        "windowLimits":{"fiveHour":{"used":3,"cap":6,"resetAt":"2096-09-12T10:00:00Z"},
                                        "weekly":{"used":40,"cap":100,"resetAt":1789207200}}}"#
                        .into(),
                )
            } else if request.url.contains("/alpha/usage/summary") {
                MockReply::Json(200, r#"{"totalCost":2.5,"totalCount":3,"totalTokens":100}"#.into())
            } else if request.url.contains("/alpha/billing/subscriptions") {
                MockReply::Json(200, r#"{"data":{"planId":"go","status":"active"}}"#.into())
            } else {
                MockReply::Json(404, "{}".into())
            }
        });
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        sqlx::query(
            "INSERT INTO settings(key,value_json,updated_at) VALUES('command_code_enabled','true',?)",
        )
        .bind("2026-08-04T01:00:00+00:00")
        .execute(state.db.pool())
        .await
        .unwrap();
        seed_channel(&state, "https://api.commandcode.ai").await;
        save_balance(&state, "command_code", true).await;

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "ok");
        assert_eq!(snapshot.remaining, Some(16.0));
        assert_eq!(snapshot.used, Some(2.5));
        assert_eq!(snapshot.total, Some(18.5));
        assert_eq!(snapshot.label.as_deref(), Some("Credits"));
        assert_eq!(snapshot.windows.len(), 2);
        assert_eq!(snapshot.windows[0].label, "5h");
        assert!((snapshot.windows[0].used_percent - 50.0).abs() < 1e-9);
        assert_eq!(
            snapshot.windows[0].resets_at.as_deref(),
            Some("2096-09-12T10:00:00+00:00")
        );
        assert_eq!(snapshot.windows[1].label, "周");
        assert_eq!(snapshot.windows[1].used_percent, 40.0);

        assert_eq!(snapshot.detail["planId"], "go");
        assert_eq!(snapshot.detail["planStatus"], "active");
        let requests = mock.requests();
        assert_eq!(requests.len(), 4, "whoami + credits + summary + subscriptions");
        assert!(requests[0].url.ends_with("/alpha/whoami"));
        assert!(
            requests[1]
                .url
                .contains("/alpha/billing/credits?orgId=org_9"),
            "query must be a real query string, got {}",
            requests[1].url
        );
        assert!(
            requests[2]
                .url
                .contains("/alpha/usage/summary?orgId=org_9"),
            "query must be a real query string, got {}",
            requests[2].url
        );
        assert!(
            requests[3]
                .url
                .contains("/alpha/billing/subscriptions?orgId=org_9"),
            "query must be a real query string, got {}",
            requests[3].url
        );
        for request in &requests {
            assert_eq!(request.method, Method::GET);
            let authorization = request
                .headers
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default();
            assert_eq!(authorization, "Bearer sk-channel-secret");
        }
    }


    /// 无 org 的个人账号（whoami `org:null`，真实 Go 套餐形状）：billing 系列
    /// 必须**省略 orgId**（不能拼 `?orgId=`），credits 的 5h/周窗口与
    /// `resetAt:0`（无待重置）要正确呈现。
    #[tokio::test]
    async fn command_code_balance_without_org_uses_paramless_billing_requests() {
        let mock = MockUpstream::new(|request| {
            if request.url.contains("/alpha/whoami") {
                MockReply::Json(
                    200,
                    r#"{"success":true,"user":{"id":"u-1","userName":"alice"},"org":null}"#.into(),
                )
            } else if request.url.contains("/alpha/billing/credits") {
                MockReply::Json(
                    200,
                    r#"{"credits":{"monthlyCredits":10,"purchasedCredits":0,"freeCredits":0},
                        "windowLimits":{"limited":true,"fiveHour":{"used":0,"cap":3,"resetAt":0},
                                        "weekly":{"used":0,"cap":6,"resetAt":0}}}"#
                        .into(),
                )
            } else if request.url.contains("/alpha/usage/summary") {
                MockReply::Json(
                    200,
                    r#"{"totalCost":0,"totalCount":0,"totalTokens":0}"#.into(),
                )
            } else {
                MockReply::Json(404, "{}".into())
            }
        });
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        sqlx::query(
            "INSERT INTO settings(key,value_json,updated_at) VALUES('command_code_enabled','true',?)",
        )
        .bind("2026-08-04T01:00:00+00:00")
        .execute(state.db.pool())
        .await
        .unwrap();
        seed_channel(&state, "https://api.commandcode.ai").await;
        save_balance(&state, "command_code", true).await;

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "ok", "{snapshot:?}");
        assert_eq!(snapshot.remaining, Some(10.0));
        assert_eq!(snapshot.label.as_deref(), Some("Credits"));
        assert_eq!(snapshot.windows.len(), 2);
        assert_eq!(snapshot.windows[0].label, "5h");
        assert_eq!(snapshot.windows[0].used_percent, 0.0);
        assert_eq!(snapshot.windows[0].remaining_percent, 100.0);
        assert_eq!(
            snapshot.windows[0].resets_at, None,
            "resetAt:0 means no scheduled reset"
        );
        assert_eq!(snapshot.windows[1].label, "周");
        assert_eq!(snapshot.detail["account"], "alice");
        assert_eq!(snapshot.detail["orgId"], "");

        let requests = mock.requests();
        for request in &requests {
            if request.url.contains("/alpha/billing/") || request.url.contains("/alpha/usage/") {
                assert!(
                    !request.url.contains("orgId"),
                    "org-less accounts must omit the parameter, got {}",
                    request.url
                );
            }
        }
        assert!(requests.iter().any(|r| r.url.ends_with("/alpha/billing/credits")));
    }

    /// The global Command Code switch gates the balance sidecar too: while
    /// disabled the query is classified and performs zero upstream requests.
    #[tokio::test]
    async fn command_code_balance_is_blocked_while_disabled() {
        let mock = MockUpstream::always(200, "{}");
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://api.commandcode.ai").await;
        save_balance(&state, "command_code", true).await;

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "error");
        assert_eq!(snapshot.error_kind.as_deref(), Some("disabled"));
        assert_eq!(
            mock.calls(),
            0,
            "disabled integration must not touch the upstream"
        );
    }

}
