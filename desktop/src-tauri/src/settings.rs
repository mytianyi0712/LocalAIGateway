//! 运行时设置的读取、校验与访问控制。
//!
//! 职责：从 `settings` 表读取运行时设置行并解码为 [`RuntimeSettings`]；校验写入
//! 设置必须落在允许范围内；解密访问密钥并完成请求鉴权与密钥损坏状态查询。
//! 边界：本模块只管设置/密钥的读写与鉴权，不负责代理转发，也不负责后台维护调度。
//! 关键不变量：读取路径 fail-closed —— 任一行无法解码即报 [`ConfigCorrupted`]，
//! 只有数据库中不存在的键才回退默认值；损坏的行绝不静默降级为默认值。

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
    /// 必须转换的路径（非流式的映射路径）所缓冲的上游明文/原始响应体的硬上限。
    /// 流式路径的缓冲绝不会超过该值。
    pub max_buffered_upstream_body_mb: i64,
    pub model_discovery_interval_hours: i64,
    pub log_retention_days: i64,
    /// Command Code 集成总开关（默认关闭：关闭期间不向上游发起任何 Command Code
    /// 请求）。
    pub command_code_enabled: bool,
    /// Command Code NDJSON 流的空闲超时（包含静默的 `tool-input-*` 窗口）。
    pub command_code_idle_timeout_seconds: i64,
    /// 指纹/生命周期重新初始化的节流间隔（社区基线：8 小时）。
    pub command_code_init_interval_hours: i64,
    /// npm `latest` 版本漂移检查的周期。
    pub command_code_version_check_interval_hours: i64,
    /// Command Code 配额（余额）刷新的周期。
    pub command_code_quota_interval_minutes: i64,
    /// 每个渠道（一个账号）在途 `/alpha/generate` 请求数的上限。
    pub command_code_max_concurrency: i64,
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
            command_code_enabled: false,
            command_code_idle_timeout_seconds: 120,
            command_code_init_interval_hours: 8,
            command_code_version_check_interval_hours: 24,
            command_code_quota_interval_minutes: 15,
            command_code_max_concurrency: 2,
        }
    }
}

impl RuntimeSettings {
    /// 序列化成设置页/接口用的 JSON。失败返回错误而不是 panic（release 构建
    /// `panic=abort`，设置读取路径不该因为一次序列化失败杀死进程）。
    pub fn as_value(&self) -> Result<Value, serde_json::Error> {
        serde_json::to_value(self)
    }
}

/// 属于 [`RuntimeSettings`] 的键。只有这些行会被读入运行时设置对象 —— 预设缓存
/// 等非运行时键绝不会混入。
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
    "command_code_enabled",
    "command_code_idle_timeout_seconds",
    "command_code_init_interval_hours",
    "command_code_version_check_interval_hours",
    "command_code_quota_interval_minutes",
    "command_code_max_concurrency",
];

/// 当存储的运行时设置行无法被严格解码（JSON 非法或类型不符）时抛出。读取必须
/// fail-closed —— 损坏的 `trust_local_network` 绝不能静默变成 `true`。
#[derive(Debug)]
pub struct ConfigCorrupted(pub String);

impl std::fmt::Display for ConfigCorrupted {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "runtime setting {} is corrupt", self.0)
    }
}
impl std::error::Error for ConfigCorrupted {}

/// 每个运行时设置的整数值范围；由写入路径的校验器与读取路径的严格解码共用。
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
        ("command_code_idle_timeout_seconds", (10, 3600)),
        ("command_code_init_interval_hours", (1, 168)),
        ("command_code_version_check_interval_hours", (1, 720)),
        ("command_code_quota_interval_minutes", (1, 1440)),
        ("command_code_max_concurrency", (1, 32)),
    ])
}

/// 对单条存储的运行时设置行做严格的类型 + JSON 校验。写入路径通过
/// [`validate_updates`] 校验范围；读取路径只拒绝结构损坏（JSON 非法、类型不
/// 符）—— 类型正确但越界的值仍可使用（消费方会防御性钳制，例如 `max(1)`），
/// 因此合法的历史配置绝不会让网关在启动时瘫痪。尤其是 `trust_local_network`
/// 绝不会静默降级为 `true`。
fn strict_setting_check(key: &str, parsed: &Value) -> Result<(), ConfigCorrupted> {
    if matches!(key, "trust_local_network" | "command_code_enabled") {
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

/// 从裸连接池句柄加载设置（供不持有完整 context 的服务使用）。fail-closed：任何
/// 存储的运行时行若不是类型正确且合法的 JSON，都会以 [`ConfigCorrupted`] 报错 ——
/// 默认值只用于数据库中不存在的键，绝不用于损坏的行。
pub async fn runtime_settings_from(db: &Database) -> Result<RuntimeSettings> {
    let mut value = RuntimeSettings::default().as_value()?;
    for (key, raw) in runtime_setting_rows(db).await? {
        let parsed: Value = serde_json::from_str(&raw)
            .map_err(|error| ConfigCorrupted(format!("{key}: {error}")))?;
        strict_setting_check(&key, &parsed)?;
        value[&key] = parsed;
    }
    Ok(serde_json::from_value(value)?)
}

/// 仅供设置页使用的最佳努力运行时设置：损坏的行会被跳过并按键上报，以便页面
/// 显示修复提示而不是直接失败。auth 与代理转发的读取走 [`runtime_settings_from`]，
/// fail-closed；而维护调度中的 Command Code 路径当前实现为 fail-open（读取失败按
/// 默认值处理，见 `maintenance.rs` 中的该调度分支）。
pub async fn runtime_settings_ui(db: &Database) -> (RuntimeSettings, Vec<String>) {
    let mut value = match RuntimeSettings::default().as_value() {
        Ok(value) => value,
        Err(error) => {
            tracing::error!(error = ?error, "runtime settings defaults are not serializable");
            return (RuntimeSettings::default(), Vec::new());
        }
    };
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

/// 当存储的访问密钥无法解析或解密时抛出。
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

/// 两个访问密钥的只读损坏状态，用于管理端 UI 提示。绝不向上传播错误：存储损坏
/// 时改为上报 `corrupt`。
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
        if matches!(key.as_str(), "trust_local_network" | "command_code_enabled") {
            if !value.is_boolean() {
                bail!("{key} 必须是布尔值");
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

/// 读取 `settings` 表中的原始字符串值（键不存在返回 `None`）。
///
/// 返回的是存储文本本身（即 JSON 文本，字符串值带引号）；调用方按需解析。
/// 单键读写只有这一处实现：`commandcode` 的 JSON 助手、opencode 会话 id 与
/// 管理端状态页都经它访问 KV 表。
pub async fn read_setting(db: &Database, key: &str) -> Result<Option<String>> {
    Ok(
        sqlx::query_scalar::<_, String>("SELECT CAST(value_json AS TEXT) FROM settings WHERE key=?")
            .bind(key)
            .fetch_optional(db.pool())
            .await?,
    )
}

/// 写入 `settings` 表的字符串值（upsert；`value` 必须是合法 JSON 文本）。
///
/// 需要在调用方事务内写入时用 [`save_setting_tx`]，本函数走连接池。
pub async fn write_setting(db: &Database, key: &str, value: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO settings(key, value_json, updated_at) VALUES(?, ?, ?) \
         ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at",
    )
    .bind(key)
    .bind(value)
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(db.pool())
    .await?;
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
    let mut value = match settings.as_value() {
        Ok(value) => value,
        Err(error) => {
            tracing::error!(error = ?error, "settings serialization failed");
            Value::Null
        }
    };
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

/// 稳定的一次性安装 id，用作 OpenCode Zen/Go 上游的兜底 `x-opencode-session`
/// 值（见 `protocol::apply_opencode_session`）。持久化一次，使上游在重启后保持
/// 同一会话；当该行无法读写时用一个临时 id 维持代理可用，因为该请求头只影响
/// 上游的路由/缓存。
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
    let existing = read_setting(db, "opencode_session_id")
        .await?
        .and_then(|raw| serde_json::from_str::<String>(&raw).ok())
        .filter(|value| HeaderValue::from_str(value).is_ok() && !value.is_empty());
    if let Some(value) = existing {
        return Ok(value);
    }
    let value = uuid::Uuid::new_v4().to_string();
    write_setting(db, "opencode_session_id", &serde_json::to_string(&value)?).await?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::AppState;
    use crate::test_support::TempDir;

    async fn test_state() -> (AppState, TempDir) {
        // 统一夹具：临时目录 + 整套 Context（见 `crate::test_support`）。
        let crate::test_support::TestEnv {
            dir,
            context: state,
        } = crate::test_support::context("settings").await;
        (state, dir)
    }

    /// 无法解密的存储密钥会以类型化的 CorruptKey（面向管理端 API）以及
    /// `corrupt` 状态（面向设置页）暴露，而不是被静默当作缺失处理。
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

    /// 损坏的 `trust_local_network` 行必须让读取失败（绝不静默回退到默认的
    /// `true`），且鉴权必须拒绝 —— 损坏绝不能让网关变成“信任本地网络”。
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
        // 鉴权读取同样 fail-closed：不能因为某行存储损坏就放行任何请求。
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
        // 设置页仍能加载并点名损坏的键，供修复使用。
        let (_values, corrupt) = runtime_settings_ui(&state.db).await;
        assert!(
            corrupt.iter().any(|key| key == "trust_local_network"),
            "settings page must report the corrupt key: {corrupt:?}"
        );
        // 修复：用合法值覆盖该行即可恢复读取。
        let mut tx = state.db.pool().begin().await.unwrap();
        save_setting_tx(&mut tx, "trust_local_network", &json!(false))
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let settings = runtime_settings(&state).await.unwrap();
        assert!(!settings.trust_local_network, "repaired value must be read");
        let _ = dir;
    }

    /// 损坏的整数行（JSON 非法、类型不符）会点名键并 fail-closed；缺失的键仍使用
    /// 默认值。（范围违规属于写入路径的关注点 —— `validate_updates` 会拒绝它们；
    /// 类型正确但越界的值仍可读取，以便历史配置继续工作。）
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
        // 缺失的键不算损坏：应用默认值。
        let settings = runtime_settings(&state).await.unwrap();
        assert_eq!(settings.failure_threshold, 3);
        assert_eq!(settings.max_buffered_upstream_body_mb, 64);
        let _ = dir;
    }

    /// 非运行时设置行（例如预设缓存）绝不能混入运行时设置对象，即使其值是垃圾
    /// 数据也一样。
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

    /// OpenCode 会话 id 必须在多次调用间稳定并被持久化，使上游会话能跨网关重启
    /// 存活。
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

    /// 畸形的存储行会被替换，而不是作为非法的请求头值被转发出去。
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
