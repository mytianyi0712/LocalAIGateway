//! admin API 域模块：设置、访问密钥与系统状态（settings/system 域）
//! 每个域文件包含该资源的 handler（薄壳）与输入/输出类型；
//! 直写 SQL 的域逻辑正逐步收敛到 super::AdminService。

use super::*;
use axum::{Json, extract::State};
use serde_json::{Value, json};

pub(super) async fn get_settings(_: AdminAuth, State(state): State<Context>) -> ApiResult {
    // The page must load even with a corrupt runtime setting so the user
    // can re-enter the value (repair). Auth and proxying fail closed; only
    // this page read degrades, and it reports the corrupt keys explicitly
    // (P1-3).
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
    State(state): State<Context>,
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
    // When runtime settings are corrupt we cannot read the stored admin
    // key; require an explicit key in the same patch that disables
    // local-network trust, so the repair path never opens auth (P1-3).
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
    let mut tx = state.db.pool().begin().await?;
    for (key, raw) in [
        ("admin_access_key", submitted_admin),
        ("gateway_access_key", submitted_gateway),
    ] {
        if let Some(value) = raw {
            crate::settings::save_setting_tx(
                &mut tx,
                key,
                &json!(String::from_utf8(state.secrets.encrypt(&value)).unwrap_or_default()),
            )
            .await?;
        }
    }
    for (key, item) in value.as_object().cloned().unwrap_or_default() {
        crate::settings::save_setting_tx(&mut tx, &key, &item).await?;
    }
    tx.commit().await?;
    // The write is committed; the response is the same tolerant view as
    // GET — a corrupt row the patch did not repair is reported by key
    // instead of failing the request after the commit (P1-3).
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
    _: super::super::auth::RecoveryAuth,
    State(state): State<Context>,
) -> ApiResult {
    let (admin_corrupt, gateway_corrupt) = crate::settings::key_statuses(&state).await?;
    // Access policy decrypts the keys; while a key is corrupt that read fails
    // by design, and the corrupt flag (from the raw rows) already classifies
    // it — so tolerate the failure and fall back to "missing" for absent rows.
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
        // P1-3: runtime-setting corruption is reported here too, so the
        // settings page can offer the repair form (loopback + nonce) when
        // the keys themselves are healthy.
        "settings_corrupt": !corrupt_keys.is_empty(),
        "config_corrupted_keys": corrupt_keys,
    })))
}

pub(super) async fn generate_access_keys(
    _: super::super::auth::RecoveryGenerateAuth,
    State(state): State<Context>,
    headers: axum::http::HeaderMap,
) -> ApiResult {
    // P1-4: a presented nonce must always validate (single-use, burned on
    // every attempt); without one, recovery-mode calls are closed outright.
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
    let mut tx = state.db.pool().begin().await?;
    for (key, value) in [
        ("admin_access_key", &admin),
        ("gateway_access_key", &gateway),
    ] {
        crate::settings::save_setting_tx(
            &mut tx,
            key,
            &json!(String::from_utf8(state.secrets.encrypt(value)).unwrap_or_default()),
        )
        .await?;
    }
    tx.commit().await?;
    Ok(ok(
        json!({"admin_access_key":admin,"gateway_access_key":gateway,"warning":"这些密钥只在本次响应中返回，请立即保存。"}),
    ))
}
pub(super) async fn recovery_repair_settings(
    _: super::super::auth::RecoveryGenerateAuth,
    State(state): State<Context>,
    headers: axum::http::HeaderMap,
    Json(value): Json<Value>,
) -> ApiResult {
    // P1-3: corrupt runtime settings lock the admin surface down (fail
    // closed). This loopback-only, nonce-gated endpoint is the repair
    // entry: it writes runtime settings rows (never access keys) and can
    // restore a corrupt `trust_local_network` etc. The nonce is issued by
    // the recovery status endpoint and burned on every attempt, like the
    // key regeneration flow.
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
    let mut tx = state.db.pool().begin().await?;
    for (key, item) in value.as_object().cloned().unwrap_or_default() {
        crate::settings::save_setting_tx(&mut tx, &key, &item).await?;
    }
    tx.commit().await?;
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

pub(super) async fn system_status(_: AdminAuth, State(state): State<Context>) -> ApiResult {
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
