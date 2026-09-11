use std::collections::HashMap;

use anyhow::{Result, bail};
use axum::http::{HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::Row;

use crate::{application::Context, crypto::SecretStore, db::Database};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RuntimeSettings {
    pub trust_local_network: bool,
    pub failure_threshold: i64,
    pub circuit_open_seconds: i64,
    pub max_failover_attempts: i64,
    pub connect_timeout_seconds: i64,
    pub first_byte_timeout_seconds: i64,
    pub first_token_timeout_seconds: i64,
    pub stream_idle_timeout_seconds: i64,
    pub non_stream_total_timeout_seconds: i64,
    pub max_request_body_mb: i64,
    /// Hard cap on the buffered plaintext/raw upstream response body for
    /// paths that must convert (mapped non-stream). Streaming paths are
    /// never buffered beyond this. (P1-1)
    pub max_buffered_upstream_body_mb: i64,
    pub model_discovery_interval_hours: i64,
    pub log_retention_days: i64,
}

impl Default for RuntimeSettings {
    fn default() -> Self {
        Self {
            trust_local_network: true,
            failure_threshold: 3,
            circuit_open_seconds: 900,
            max_failover_attempts: 3,
            connect_timeout_seconds: 10,
            first_byte_timeout_seconds: 60,
            first_token_timeout_seconds: 60,
            stream_idle_timeout_seconds: 300,
            non_stream_total_timeout_seconds: 600,
            max_request_body_mb: 256,
            max_buffered_upstream_body_mb: 64,
            model_discovery_interval_hours: 24,
            log_retention_days: 30,
        }
    }
}

impl RuntimeSettings {
    pub fn as_value(&self) -> Value {
        serde_json::to_value(self).expect("settings serialize")
    }
}

/// Keys that belong to [`RuntimeSettings`]. Only these rows are read into
/// the runtime settings object — preset caches and other non-runtime keys
/// never leak in (P1-3).
const RUNTIME_SETTING_KEYS: &[&str] = &[
    "trust_local_network",
    "failure_threshold",
    "circuit_open_seconds",
    "max_failover_attempts",
    "connect_timeout_seconds",
    "first_byte_timeout_seconds",
    "first_token_timeout_seconds",
    "stream_idle_timeout_seconds",
    "non_stream_total_timeout_seconds",
    "max_request_body_mb",
    "max_buffered_upstream_body_mb",
    "model_discovery_interval_hours",
    "log_retention_days",
];

/// Raised when a stored runtime setting row cannot be decoded exactly (bad
/// JSON, wrong type, or out of range). Reading must fail closed — a corrupt
/// `trust_local_network` must never silently become `true` (P1-3).
#[derive(Debug)]
pub struct ConfigCorrupted(pub String);

impl std::fmt::Display for ConfigCorrupted {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "runtime setting {} is corrupt", self.0)
    }
}
impl std::error::Error for ConfigCorrupted {}

/// Integer value ranges per runtime setting; shared by the write-path
/// validator and the strict read-path decode.
fn setting_ranges() -> HashMap<&'static str, (i64, i64)> {
    HashMap::from([
        ("failure_threshold", (1, 20)),
        ("circuit_open_seconds", (0, 86400)),
        ("max_failover_attempts", (1, 20)),
        ("connect_timeout_seconds", (1, 120)),
        ("first_byte_timeout_seconds", (1, 600)),
        ("first_token_timeout_seconds", (1, 600)),
        ("stream_idle_timeout_seconds", (10, 3600)),
        ("non_stream_total_timeout_seconds", (10, 3600)),
        ("max_request_body_mb", (1, 1024)),
        ("max_buffered_upstream_body_mb", (1, 1024)),
        ("model_discovery_interval_hours", (1, 168)),
        ("log_retention_days", (1, 365)),
    ])
}

/// Strict type + JSON check for ONE stored runtime setting row. The write
/// path validates ranges via [`validate_updates`]; the read path rejects
/// only STRUCTURAL corruption (bad JSON, wrong type) — out-of-range but
/// well-typed values stay usable (consumers clamp defensively, e.g.
/// `max(1)`), so a legitimate legacy configuration can never brick the
/// gateway at startup. `trust_local_network` in particular can never
/// silently degrade to `true` (P1-3).
fn strict_setting_check(key: &str, parsed: &Value) -> Result<(), ConfigCorrupted> {
    if key == "trust_local_network" {
        if !parsed.is_boolean() {
            return Err(ConfigCorrupted(format!("{key}: expected boolean")));
        }
        return Ok(());
    }
    if parsed.as_i64().is_none() {
        return Err(ConfigCorrupted(format!("{key}: expected integer")));
    }
    Ok(())
}

async fn runtime_setting_rows(db: &Database) -> Result<Vec<(String, String)>> {
    let placeholders = RUNTIME_SETTING_KEYS
        .iter()
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(",");
    let statement = format!(
        "SELECT key, CAST(value_json AS TEXT) value_json FROM settings WHERE key IN ({placeholders})"
    );
    let mut query = sqlx::query(&statement);
    for key in RUNTIME_SETTING_KEYS {
        query = query.bind(key);
    }
    let rows = query.fetch_all(db.pool()).await?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push((row.try_get("key")?, row.try_get("value_json")?));
    }
    Ok(out)
}

/// Settings load from a raw pool handle (used by services that do not hold
/// the full context, P2-1). Fail-closed (P1-3): any stored runtime row that
/// is not exactly valid JSON of the right type and range is reported as
/// [`ConfigCorrupted`] — the default is only used for keys absent from the
/// database, never for corrupt rows.
pub async fn runtime_settings_from(db: &Database) -> Result<RuntimeSettings> {
    let mut value = RuntimeSettings::default().as_value();
    for (key, raw) in runtime_setting_rows(db).await? {
        let parsed: Value = serde_json::from_str(&raw)
            .map_err(|error| ConfigCorrupted(format!("{key}: {error}")))?;
        strict_setting_check(&key, &parsed)?;
        value[&key] = parsed;
    }
    Ok(serde_json::from_value(value)?)
}

/// Best-effort runtime settings for the settings page only: corrupt rows
/// are skipped and reported by key so the page can show a repair hint
/// instead of dying. Auth, proxying and maintenance NEVER use this — they
/// fail closed via [`runtime_settings_from`].
pub async fn runtime_settings_ui(db: &Database) -> (RuntimeSettings, Vec<String>) {
    let mut value = RuntimeSettings::default().as_value();
    let mut corrupt = Vec::new();
    let rows = match runtime_setting_rows(db).await {
        Ok(rows) => rows,
        Err(error) => {
            tracing::warn!(%error, "settings row query failed; page shows defaults");
            return (RuntimeSettings::default(), corrupt);
        }
    };
    for (key, raw) in rows {
        let checked = serde_json::from_str::<Value>(&raw)
            .map_err(|error| ConfigCorrupted(format!("{key}: {error}")))
            .and_then(|parsed| {
                strict_setting_check(&key, &parsed)?;
                Ok(parsed)
            });
        match checked {
            Ok(parsed) => value[&key] = parsed,
            Err(error) => {
                tracing::warn!(%error, "corrupt runtime setting row; page requires re-entry");
                corrupt.push(key);
            }
        }
    }
    match serde_json::from_value(value) {
        Ok(settings) => (settings, corrupt),
        Err(error) => {
            tracing::warn!(%error, "validated settings still failed to decode");
            (RuntimeSettings::default(), corrupt)
        }
    }
}

pub async fn runtime_settings(state: &Context) -> Result<RuntimeSettings> {
    runtime_settings_from(&state.db).await
}

pub struct AccessPolicy {
    pub trust_local_network: bool,
    pub admin_key: String,
    pub gateway_key: String,
}

/// Raised when a stored access key cannot be parsed or decrypted.
#[derive(Debug)]
pub struct CorruptKey(pub String);

impl std::fmt::Display for CorruptKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "access key {} is corrupt", self.0)
    }
}
impl std::error::Error for CorruptKey {}

pub async fn access_policy_from(db: &Database, secrets: &SecretStore) -> Result<AccessPolicy> {
    let settings = runtime_settings_from(db).await?;
    let rows = sqlx::query("SELECT key, CAST(value_json AS TEXT) value_json FROM settings WHERE key IN ('admin_access_key', 'gateway_access_key')")
        .fetch_all(db.pool()).await?;
    let mut keys = HashMap::new();
    for row in rows {
        let key: String = row.try_get("key")?;
        let raw: String = row.try_get("value_json")?;
        let token = serde_json::from_str::<String>(&raw)
            .map_err(|error| CorruptKey(format!("{key}: {error}")))?;
        let value = secrets
            .decrypt(token.as_bytes())
            .map_err(|error| CorruptKey(format!("{key}: {error}")))?;
        keys.insert(key, value);
    }
    Ok(AccessPolicy {
        trust_local_network: settings.trust_local_network,
        admin_key: keys.remove("admin_access_key").unwrap_or_default(),
        gateway_key: keys.remove("gateway_access_key").unwrap_or_default(),
    })
}

pub async fn access_policy(state: &Context) -> Result<AccessPolicy> {
    access_policy_from(&state.db, &state.secrets).await
}

/// Read-only corruption status of the two access keys, for admin UI hints.
/// Never propagates errors: a corrupt store reports `corrupt` instead.
pub async fn key_statuses(state: &Context) -> Result<(bool, bool)> {
    let rows = sqlx::query("SELECT key, CAST(value_json AS TEXT) value_json FROM settings WHERE key IN ('admin_access_key', 'gateway_access_key')")
        .fetch_all(state.db.pool()).await?;
    let mut corrupt = (false, false);
    for row in rows {
        let key: String = row.try_get("key")?;
        let raw: String = row.try_get("value_json")?;
        let broken = serde_json::from_str::<String>(&raw)
            .map_err(|error| CorruptKey(format!("{key}: {error}")))
            .and_then(|token| {
                state
                    .secrets
                    .decrypt(token.as_bytes())
                    .map_err(|error| CorruptKey(format!("{key}: {error}")))
            })
            .is_err();
        match key.as_str() {
            "admin_access_key" => corrupt.0 = broken,
            "gateway_access_key" => corrupt.1 = broken,
            _ => {}
        }
    }
    Ok(corrupt)
}

pub async fn authorize_admin(state: &Context, headers: &HeaderMap) -> Result<bool> {
    let policy = access_policy(state).await?;
    if policy.trust_local_network {
        return Ok(true);
    }
    Ok(bearer(headers)
        .is_some_and(|value| constant_time_eq(value.as_bytes(), policy.admin_key.as_bytes())))
}

pub async fn authorize_gateway_from(
    db: &Database,
    secrets: &SecretStore,
    headers: &HeaderMap,
    query: Option<&str>,
    protocol: &str,
) -> Result<bool> {
    let policy = access_policy_from(db, secrets).await?;
    if policy.trust_local_network {
        return Ok(true);
    }
    let supplied = headers
        .get("x-local-gateway-key")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .or_else(|| match protocol {
            "openai_compatible" | "openai_responses" | "catalog" => {
                bearer(headers).map(str::to_owned)
            }
            "claude" => headers
                .get("x-api-key")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned),
            "gemini" => headers
                .get("x-goog-api-key")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned)
                .or_else(|| {
                    query.and_then(|q| {
                        url::form_urlencoded::parse(q.as_bytes())
                            .find(|(k, _)| k == "key")
                            .map(|(_, v)| v.into_owned())
                    })
                }),
            _ => None,
        });
    Ok(supplied
        .is_some_and(|value| constant_time_eq(value.as_bytes(), policy.gateway_key.as_bytes())))
}

pub async fn authorize_gateway(
    state: &Context,
    headers: &HeaderMap,
    query: Option<&str>,
    protocol: &str,
) -> Result<bool> {
    authorize_gateway_from(&state.db, &state.secrets, headers, query, protocol).await
}

pub fn validate_updates(value: &Value) -> Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("设置必须是 JSON 对象"))?;
    let ranges = setting_ranges();
    for (key, value) in object {
        if matches!(key.as_str(), "admin_access_key" | "gateway_access_key") {
            continue;
        }
        if key == "trust_local_network" {
            if !value.is_boolean() {
                bail!("trust_local_network 必须是布尔值");
            }
            continue;
        }
        let Some((min, max)) = ranges.get(key.as_str()) else {
            bail!("未知设置：{key}");
        };
        let number = value
            .as_i64()
            .ok_or_else(|| anyhow::anyhow!("{key} 必须是整数"))?;
        if number < *min || number > *max {
            bail!("{key} 必须在 {min}–{max} 之间");
        }
    }
    Ok(())
}

pub async fn save_setting_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    key: &str,
    value: &Value,
) -> Result<()> {
    sqlx::query("INSERT INTO settings(key, value_json, updated_at) VALUES(?, ?, ?) \
                 ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at")
        .bind(key).bind(serde_json::to_string(value)?).bind(chrono::Utc::now().to_rfc3339())
        .execute(&mut **tx).await?;
    Ok(())
}

pub fn settings_with_hints(
    settings: &RuntimeSettings,
    policy: &AccessPolicy,
    admin_corrupt: bool,
    gateway_corrupt: bool,
) -> Value {
    let mut value = settings.as_value();
    let status = |corrupt: bool, key: &str| {
        if corrupt {
            "corrupt".to_owned()
        } else if key.is_empty() {
            "missing".to_owned()
        } else {
            "ok".to_owned()
        }
    };
    value["admin_key_status"] = json!(status(admin_corrupt, &policy.admin_key));
    value["gateway_key_status"] = json!(status(gateway_corrupt, &policy.gateway_key));
    value["admin_key_hint"] = json!(if policy.admin_key.is_empty() {
        String::new()
    } else {
        crate::crypto::SecretStore::hint(&policy.admin_key)
    });
    value["gateway_key_hint"] = json!(if policy.gateway_key.is_empty() {
        String::new()
    } else {
        crate::crypto::SecretStore::hint(&policy.gateway_key)
    });
    value["access_keys_configured"] =
        json!(!policy.admin_key.is_empty() && !policy.gateway_key.is_empty());
    value
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |diff, (a, b)| diff | (a ^ b))
        == 0
}

/// Stable per-install id used as the fallback `x-opencode-session` value
/// for OpenCode Zen/Go upstreams (see `protocol::apply_opencode_session`).
/// Persisted once so the upstream keeps the same session across restarts;
/// when the row cannot be read or written an ephemeral id keeps proxying
/// alive, because the header only affects upstream routing/caching.
pub async fn opencode_session_id(db: &Database) -> String {
    match load_or_create_opencode_session_id(db).await {
        Ok(value) => value,
        Err(error) => {
            tracing::warn!(%error, "opencode session id unavailable; using an ephemeral id");
            uuid::Uuid::new_v4().to_string()
        }
    }
}

async fn load_or_create_opencode_session_id(db: &Database) -> Result<String> {
    let existing: Option<String> = sqlx::query_scalar::<_, String>(
        "SELECT CAST(value_json AS TEXT) FROM settings WHERE key='opencode_session_id'",
    )
    .fetch_optional(db.pool())
    .await?
    .and_then(|raw| serde_json::from_str::<String>(&raw).ok())
    .filter(|value| HeaderValue::from_str(value).is_ok() && !value.is_empty());
    if let Some(value) = existing {
        return Ok(value);
    }
    let value = uuid::Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO settings(key, value_json, updated_at) VALUES('opencode_session_id', ?, ?) \
         ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at",
    )
    .bind(serde_json::to_string(&value)?)
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(db.pool())
    .await?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{application::Context, config::AppConfig, db::Database};
    use std::sync::Arc;

    async fn test_state() -> (Context, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("lagw-settings-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = Database::open(&dir.join("test.db")).await.unwrap();
        let secrets = crate::crypto::SecretStore::load(&dir.join("master.key"))
            .await
            .unwrap();
        let (telemetry, _rx) = crate::telemetry::Telemetry::new(1000);
        let http: Arc<dyn crate::ports::UpstreamClient> =
            Arc::new(crate::infrastructure::HttpClientPool::default());
        let routes: Arc<dyn crate::ports::RouteRepository> =
            crate::infrastructure::SqliteRouteRepository::new(db.clone());
        let channels: Arc<dyn crate::ports::ChannelRepository> =
            crate::infrastructure::SqliteChannelRepository::new(db.clone());
        let clock: Arc<dyn crate::ports::Clock> = Arc::new(crate::infrastructure::SystemClock);
        let background = crate::infrastructure::RuntimeSupervisor::new(
            tokio_util::sync::CancellationToken::new(),
        );
        let limits = Arc::new(crate::runtime::RuntimeLimits::default());
        let discovery = crate::discovery::DiscoveryService::new(
            db.clone(),
            secrets.clone(),
            Arc::clone(&http),
            Arc::clone(&channels),
            Arc::clone(&clock),
            Arc::clone(&background),
            Arc::clone(&limits),
        );
        let notifier: std::sync::Arc<dyn crate::ports::Notifier> =
            crate::notification::DesktopNotifier::new(std::time::Duration::from_millis(50));
        let proxy = crate::proxy::ProxyService::new(
            db.clone(),
            secrets.clone(),
            Arc::clone(&http),
            routes.clone(),
            telemetry.clone(),
            Arc::clone(&clock),
            Arc::clone(&limits),
            crate::notification::DesktopNotifier::new(std::time::Duration::from_millis(50)),
        );
        let state = crate::application::Context {
            config: Arc::new(AppConfig::default()),
            db: db.clone(),
            secrets: secrets.clone(),
            http,
            routes,
            channels,
            clock,
            notifier,
            discovery,
            proxy,
            telemetry,
            background,
            limits,
            admin: crate::admin::AdminService::new(db.clone(), secrets.clone()),
            recovery: crate::auth::RecoverySession::new(),
        };
        (state, dir)
    }

    /// P2-8: an undecryptable stored key surfaces as a typed CorruptKey
    /// (for the admin API) and as a `corrupt` status (for the settings page)
    /// instead of being silently treated as missing.
    #[tokio::test]
    async fn corrupt_access_key_surfaces_as_corrupt() {
        let (state, dir) = test_state().await;
        let time = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO settings(key, value_json, updated_at) VALUES('admin_access_key', ?, ?)",
        )
        .bind("gAAAAABnot-a-valid-fernet-token")
        .bind(&time)
        .execute(state.db.pool())
        .await
        .unwrap();
        let error = match access_policy(&state).await {
            Err(error) => error,
            Ok(_) => panic!("access_policy must fail on a corrupt key"),
        };
        assert!(
            error.downcast_ref::<CorruptKey>().is_some(),
            "corrupt key must raise CorruptKey, not be swallowed"
        );
        let (admin_corrupt, gateway_corrupt) = key_statuses(&state).await.unwrap();
        assert!(admin_corrupt, "admin key must report corrupt");
        assert!(!gateway_corrupt, "gateway key is untouched");
        let _ = dir;
    }

    /// P1-3: a corrupt `trust_local_network` row must fail the read (never
    /// silently fall back to the default `true`), and auth must reject —
    /// the gateway can never become "trust local network" from corruption.
    #[tokio::test]
    async fn corrupt_trust_local_network_fails_closed() {
        let (state, dir) = test_state().await;
        let time = chrono::Utc::now().to_rfc3339();
        sqlx::query("INSERT INTO settings(key, value_json, updated_at) VALUES('trust_local_network', ?, ?)")
            .bind(json!("yes").to_string())
            .bind(&time)
            .execute(state.db.pool())
            .await
            .unwrap();
        let error = runtime_settings(&state)
            .await
            .expect_err("corrupt trust_local_network must fail closed");
        assert!(
            error.downcast_ref::<ConfigCorrupted>().is_some(),
            "must raise ConfigCorrupted with the key name"
        );
        assert!(
            error.to_string().contains("trust_local_network"),
            "the corrupt key must be named in the error: {error}"
        );
        // Auth reads fail closed too: no request may be admitted because a
        // stored row is broken.
        let headers = axum::http::HeaderMap::new();
        assert!(
            authorize_admin(&state, &headers).await.is_err(),
            "admin auth must reject when trust setting is corrupt"
        );
        assert!(
            authorize_gateway(&state, &headers, None, "openai_compatible")
                .await
                .is_err(),
            "gateway auth must reject when trust setting is corrupt"
        );
        // The settings page still loads and names the corrupt key for repair.
        let (_values, corrupt) = runtime_settings_ui(&state.db).await;
        assert!(
            corrupt.iter().any(|key| key == "trust_local_network"),
            "settings page must report the corrupt key: {corrupt:?}"
        );
        // Repair: overwriting the row with a valid value restores reads.
        let mut tx = state.db.pool().begin().await.unwrap();
        save_setting_tx(&mut tx, "trust_local_network", &json!(false))
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let settings = runtime_settings(&state).await.unwrap();
        assert!(!settings.trust_local_network, "repaired value must be read");
        let _ = dir;
    }

    /// P1-3: corrupt integer rows (bad JSON, wrong type) fail closed with
    /// the key named; absent keys still use defaults. (Range violations are
    /// a write-path concern — `validate_updates` rejects them; well-typed
    /// out-of-range values remain readable so legacy configs keep working.)
    #[tokio::test]
    async fn corrupt_integer_settings_fail_closed_with_key_name() {
        let (state, dir) = test_state().await;
        let time = chrono::Utc::now().to_rfc3339();
        for (key, raw) in [
            ("failure_threshold", r#""abc""#),
            ("connect_timeout_seconds", r#"{"seconds":10}"#),
            ("circuit_open_seconds", "[1,2,3]"),
        ] {
            sqlx::query("INSERT INTO settings(key, value_json, updated_at) VALUES(?,?,?)")
                .bind(key)
                .bind(raw)
                .bind(&time)
                .execute(state.db.pool())
                .await
                .unwrap();
            let error = runtime_settings(&state)
                .await
                .expect_err("corrupt row must fail closed");
            assert!(
                error.downcast_ref::<ConfigCorrupted>().is_some()
                    && error.to_string().contains(key),
                "{key}: expected ConfigCorrupted naming the key, got {error}"
            );
            sqlx::query("DELETE FROM settings WHERE key=?")
                .bind(key)
                .execute(state.db.pool())
                .await
                .unwrap();
        }
        // Absent keys are not corruption: defaults apply.
        let settings = runtime_settings(&state).await.unwrap();
        assert_eq!(settings.failure_threshold, 3);
        assert_eq!(settings.max_buffered_upstream_body_mb, 64);
        let _ = dir;
    }

    /// P1-3: non-runtime settings rows (e.g. preset caches) must never leak
    /// into the runtime settings object, even with garbage values.
    #[tokio::test]
    async fn non_runtime_keys_are_ignored_by_runtime_settings() {
        let (state, dir) = test_state().await;
        let time = chrono::Utc::now().to_rfc3339();
        sqlx::query("INSERT INTO settings(key, value_json, updated_at) VALUES('claude_preset_cache', ?, ?)")
            .bind(json!({"broken": true}).to_string())
            .bind(&time)
            .execute(state.db.pool())
            .await
            .unwrap();
        let settings = runtime_settings(&state).await.unwrap();
        assert_eq!(settings.failure_threshold, 3, "preset cache must not leak in");
        let _ = dir;
    }

    /// The OpenCode session id must be stable across calls and persisted,
    /// so upstream sessions survive gateway restarts.
    #[tokio::test]
    async fn opencode_session_id_is_stable_and_persisted() {
        let (state, dir) = test_state().await;
        let first = opencode_session_id(&state.db).await;
        assert!(!first.trim().is_empty());
        let second = opencode_session_id(&state.db).await;
        assert_eq!(first, second, "session id must be stable across calls");
        let stored: String = sqlx::query_scalar(
            "SELECT CAST(value_json AS TEXT) FROM settings WHERE key='opencode_session_id'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(stored, format!("\"{first}\""), "row must hold the id");
        let _ = dir;
    }

    /// A malformed stored row is replaced instead of being forwarded as an
    /// invalid header value.
    #[tokio::test]
    async fn opencode_session_id_replaces_invalid_row() {
        let (state, dir) = test_state().await;
        let time = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO settings(key, value_json, updated_at) VALUES('opencode_session_id', ?, ?)",
        )
        .bind(json!(123).to_string())
        .bind(&time)
        .execute(state.db.pool())
        .await
        .unwrap();
        let value = opencode_session_id(&state.db).await;
        assert!(!value.is_empty());
        let stored: String = sqlx::query_scalar(
            "SELECT CAST(value_json AS TEXT) FROM settings WHERE key='opencode_session_id'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(
            stored,
            format!("\"{value}\""),
            "invalid row must be replaced"
        );
        let _ = dir;
    }
}
