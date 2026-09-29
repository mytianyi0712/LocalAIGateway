//! admin API 域模块：设置、访问密钥与系统状态（settings/system 域）。
//!
//! 运行时设置行由 `crate::settings` 负责解析与校验，这里只做：请求体清洗
//! （剥离访问密钥字段）、fail-closed 判断（关闭局域网信任前必须有管理密钥）、
//! 以及事务内落库（见 [`AdminService::save_settings`]）。
//! 恢复模式端点（`RecoveryAuth` / `RecoveryGenerateAuth`）走一次性 nonce：
//! 密钥损坏时用它们重建访问密钥，普通管理端点保持关闭。

use super::{AdminService, ApiResult, ok};
use crate::api_error::ApiError;
use crate::auth::{AdminAuth, RecoveryAuth, RecoveryGenerateAuth};
use crate::protocol::PROTOCOL_ORDER;
use crate::state::AppState;
use axum::{Json, extract::State};
use serde_json::{Map, Value, json};

pub(super) async fn get_settings(_: AdminAuth, State(state): State<AppState>) -> ApiResult {
    // 即使运行时设置损坏，本页也必须能加载，用户才能重新填写以修复。鉴权与代理
    // 依然 fail-closed，只有这一次读取降级，并显式列出损坏的键。
    let (values, corrupt_keys) = crate::settings::runtime_settings_ui(&state.db).await;
    let policy = match crate::settings::access_policy(&state).await {
        Ok(policy) => Some(policy),
        Err(error) if error.downcast_ref::<crate::settings::ConfigCorrupted>().is_some() => None,
        Err(error) => return Err(ApiError::internal(error)),
    };
    let (admin_corrupt, gateway_corrupt) = crate::settings::key_statuses(&state).await?;
    let fallback = crate::settings::AccessPolicy {
        trust_local_network: values.trust_local_network,
        admin_key: String::new(),
        gateway_key: String::new(),
    };
    let mut result = crate::settings::settings_with_hints(
        &values,
        policy.as_ref().unwrap_or(&fallback),
        admin_corrupt,
        gateway_corrupt,
    );
    result["config_corrupted_keys"] = json!(corrupt_keys);
    Ok(ok(result))
}
pub(super) async fn patch_settings(
    _: AdminAuth,
    State(state): State<AppState>,
    Json(mut value): Json<Value>,
) -> ApiResult {
    let submitted_admin = value
        .get("admin_access_key")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
        .map(str::to_owned);
    let submitted_gateway = value
        .get("gateway_access_key")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
        .map(str::to_owned);
    if let Some(object) = value.as_object_mut() {
        object.remove("admin_access_key");
        object.remove("gateway_access_key");
    }
    crate::settings::validate_updates(&value)?;
    // 运行时设置损坏时读不到已存的管理密钥，因此关闭局域网信任的那次 PATCH 必须
    // 显式带上新密钥，修复路径绝不会把鉴权打开。
    let current = match crate::settings::access_policy(&state).await {
        Ok(policy) => policy,
        Err(error) if error.downcast_ref::<crate::settings::ConfigCorrupted>().is_some() => {
            crate::settings::AccessPolicy {
                trust_local_network: true,
                admin_key: String::new(),
                gateway_key: String::new(),
            }
        }
        Err(error) => return Err(ApiError::internal(error)),
    };
    if value.get("trust_local_network").and_then(Value::as_bool) == Some(false)
        && submitted_admin
            .as_deref()
            .unwrap_or(&current.admin_key)
            .is_empty()
    {
        return Err(ApiError::validation("关闭局域网信任前必须设置管理密钥"));
    }
    // 访问密钥与运行时设置同一事务写入：任一失败都不会留下半套配置。
    let mut access_keys = Vec::new();
    if let Some(value) = submitted_admin {
        access_keys.push(("admin_access_key", value));
    }
    if let Some(value) = submitted_gateway {
        access_keys.push(("gateway_access_key", value));
    }
    state
        .admin
        .save_settings(access_keys, value.as_object().cloned().unwrap_or_default())
        .await?;
    // 写入已提交；响应与 GET 一样宽容：本次 PATCH 未修复的损坏行按 key 报出，
    // 而不是在提交之后让请求失败。
    let (values, corrupt_keys) = crate::settings::runtime_settings_ui(&state.db).await;
    let policy = match crate::settings::access_policy(&state).await {
        Ok(policy) => Some(policy),
        Err(error) if error.downcast_ref::<crate::settings::ConfigCorrupted>().is_some() => None,
        Err(error) => return Err(ApiError::internal(error)),
    };
    let (admin_corrupt, gateway_corrupt) = crate::settings::key_statuses(&state).await?;
    let fallback = crate::settings::AccessPolicy {
        trust_local_network: values.trust_local_network,
        admin_key: String::new(),
        gateway_key: String::new(),
    };
    let mut result = crate::settings::settings_with_hints(
        &values,
        policy.as_ref().unwrap_or(&fallback),
        admin_corrupt,
        gateway_corrupt,
    );
    result["config_corrupted_keys"] = json!(corrupt_keys);
    Ok(ok(result))
}
pub(super) async fn recovery_key_status(
    _: RecoveryAuth,
    State(state): State<AppState>,
) -> ApiResult {
    let (admin_corrupt, gateway_corrupt) = crate::settings::key_statuses(&state).await?;
    // 访问策略需要解密密钥；键损坏时这次读取本就会失败，而损坏标记（来自原始行）
    // 已足以分类，因此容忍失败，缺行时退化为 `missing`。
    let policy = crate::settings::access_policy(&state).await.ok();
    let (_settings, corrupt_keys) = crate::settings::runtime_settings_ui(&state.db).await;
    let status = |corrupt: bool, key: Option<&str>| {
        if corrupt {
            "corrupt".to_owned()
        } else if key.is_none_or(str::is_empty) {
            "missing".to_owned()
        } else {
            "ok".to_owned()
        }
    };
    let nonce = state.recovery.issue().await;
    Ok(ok(json!({
        "admin_key_status": status(admin_corrupt, policy.as_ref().map(|p| p.admin_key.as_str())),
        "gateway_key_status": status(gateway_corrupt, policy.as_ref().map(|p| p.gateway_key.as_str())),
        "recovery_nonce": nonce,
        // 运行时设置损坏也在此上报，键本身健康时设置页才能给出修复表单
        // （回环 + nonce）。
        "settings_corrupt": !corrupt_keys.is_empty(),
        "config_corrupted_keys": corrupt_keys,
    })))
}

pub(super) async fn generate_access_keys(
    _: RecoveryGenerateAuth,
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> ApiResult {
    // 携带的 nonce 必须校验通过（一次性，每次尝试都会作废）；未携带时恢复模式
    // 调用一律关闭。
    let nonce = headers
        .get("x-recovery-nonce")
        .and_then(|value| value.to_str().ok());
    match nonce {
        Some(nonce) if state.recovery.consume(nonce).await => {}
        Some(_) => return Err(ApiError::unauthorized()),
        None => {
            let (admin_corrupt, gateway_corrupt) = crate::settings::key_statuses(&state).await?;
            if admin_corrupt || gateway_corrupt {
                return Err(ApiError::unauthorized());
            }
        }
    }
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    let mut bytes = [0u8; 32];
    rand::fill(&mut bytes);
    let admin = URL_SAFE_NO_PAD.encode(bytes);
    rand::fill(&mut bytes);
    let gateway = URL_SAFE_NO_PAD.encode(bytes);
    state
        .admin
        .save_settings(
            vec![
                ("admin_access_key", admin.clone()),
                ("gateway_access_key", gateway.clone()),
            ],
            Map::new(),
        )
        .await?;
    Ok(ok(
        json!({"admin_access_key":admin,"gateway_access_key":gateway,"warning":"这些密钥只在本次响应中返回，请立即保存。"}),
    ))
}
pub(super) async fn recovery_repair_settings(
    _: RecoveryGenerateAuth,
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(value): Json<Value>,
) -> ApiResult {
    // 运行时设置损坏会把管理面锁死（fail-closed）。本端点仅限回环 + nonce 门槛，
    // 是修复入口：它只写运行时设置行（绝不写访问密钥），可恢复损坏的
    // `trust_local_network` 等；nonce 由恢复状态端点签发，并且与密钥重建流程
    // 一样每次尝试都作废。
    let nonce = headers
        .get("x-recovery-nonce")
        .and_then(|value| value.to_str().ok());
    match nonce {
        Some(nonce) if state.recovery.consume(nonce).await => {}
        Some(_) => return Err(ApiError::unauthorized()),
        None => {
            let (admin_corrupt, gateway_corrupt) = crate::settings::key_statuses(&state).await?;
            if admin_corrupt || gateway_corrupt {
                return Err(ApiError::unauthorized());
            }
        }
    }
    let mut value = value;
    if let Some(object) = value.as_object_mut() {
        object.remove("admin_access_key");
        object.remove("gateway_access_key");
    }
    crate::settings::validate_updates(&value)?;
    // 修复路径只写运行时设置，绝不写访问密钥。
    state
        .admin
        .save_settings(
            Vec::new(),
            value.as_object().cloned().unwrap_or_default(),
        )
        .await?;
    let (values, corrupt_keys) = crate::settings::runtime_settings_ui(&state.db).await;
    let policy = match crate::settings::access_policy(&state).await {
        Ok(policy) => Some(policy),
        Err(error) if error.downcast_ref::<crate::settings::ConfigCorrupted>().is_some() => None,
        Err(error) => return Err(ApiError::internal(error)),
    };
    let (admin_corrupt, gateway_corrupt) = crate::settings::key_statuses(&state).await?;
    let fallback = crate::settings::AccessPolicy {
        trust_local_network: values.trust_local_network,
        admin_key: String::new(),
        gateway_key: String::new(),
    };
    let mut result = crate::settings::settings_with_hints(
        &values,
        policy.as_ref().unwrap_or(&fallback),
        admin_corrupt,
        gateway_corrupt,
    );
    result["config_corrupted_keys"] = json!(corrupt_keys);
    Ok(ok(result))
}

pub(super) async fn system_status(_: AdminAuth, State(state): State<AppState>) -> ApiResult {
    let policy = crate::settings::access_policy(&state).await?;
    Ok(ok(
        json!({"status":"ok","database":"ok","host":state.config.host,"port":state.config.port,"telemetry_queue_size":state.telemetry.queue_size(),"telemetry_dropped":state.telemetry.dropped(),"protocols":PROTOCOL_ORDER,"trust_local_network":policy.trust_local_network}),
    ))
}
pub(super) async fn system_protocols(_: AdminAuth) -> ApiResult {
    let items = crate::protocol::ProtocolId::ALL
        .iter()
        .map(|protocol| {
            json!({"id":protocol.as_str(),"endpoints":protocol.endpoints(),"model_discovery_endpoint":protocol.discovery_path()})
        })
        .collect::<Vec<_>>();
    Ok(ok(json!({"items":items})))
}

/// Command Code 集成状态：全局开关、CLI 版本相对已核实基线的漂移，
/// 以及传输策略。
pub(super) async fn command_code_status(
    _: AdminAuth,
    State(state): State<AppState>,
) -> ApiResult {
    let runtime = crate::settings::runtime_settings(&state).await?;
    let cli_version = crate::commandcode::cli_version(&state.db).await;
    let (version_checked_at, channel_count) = state.admin.command_code_db_summary().await?;
    Ok(ok(json!({
        "enabled": runtime.command_code_enabled,
        "cli_version": cli_version,
        "verified_baseline": crate::commandcode::DEFAULT_CLI_VERSION,
        "drift": crate::commandcode::version_drift(&cli_version),
        "version_checked_at": version_checked_at,
        "channel_count": channel_count,
        "transport_policy": "provider_first_then_generate_on_403_upgrade_required",
        "warning": "Go 套餐没有官方 API 访问：服务端以 403 upgrade_required 拒绝后，网关仅对该账号降级到 CLI 兼容路径，并使用 CLI 身份头。这是对服务端明确拒绝路径的绕过，可能违反服务条款并导致账号封禁；开启即表示已知晓并自行承担全部风险。",
    })))
}

impl AdminService {
    /// 单个事务写入访问密钥（加密后存为 JSON 字符串）与其它运行时设置；
    /// `access_keys` 只含请求里实际提供的新密钥，`runtime` 为其余字段。
    pub(super) async fn save_settings(
        &self,
        access_keys: Vec<(&'static str, String)>,
        runtime: Map<String, Value>,
    ) -> Result<(), ApiError> {
        let mut tx = self.db.pool().begin().await?;
        for (key, value) in access_keys {
            crate::settings::save_setting_tx(
                &mut tx,
                key,
                &json!(String::from_utf8(self.secrets.encrypt(&value)).unwrap_or_default()),
            )
            .await?;
        }
        for (key, item) in runtime {
            crate::settings::save_setting_tx(&mut tx, &key, &item).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Command Code 状态页所需的库内数据：CLI 版本校验时间与 Command Code 渠道数。
    pub(super) async fn command_code_db_summary(&self) -> Result<(Option<String>, i64), ApiError> {
        let checked_at = crate::settings::read_setting(
            &self.db,
            "command_code_cli_version_checked_at",
        )
        .await?
        .and_then(|raw| serde_json::from_str::<String>(&raw).ok());
        let channels: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM channels c JOIN providers p ON p.id=c.provider_id \
             WHERE p.kind='command_code' OR c.protocol='command_code'",
        )
        .fetch_one(self.db.pool())
        .await?;
        Ok((checked_at, channels))
    }
}
