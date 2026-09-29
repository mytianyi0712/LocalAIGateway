//! 管理端鉴权提取器与密钥恢复挑战：`AdminAuth` 校验正常管理请求，
//! `RecoveryAuth` / `RecoveryGenerateAuth` 在密钥或运行时设置损坏时
//! 只允许回环来源访问恢复端点，`RecoverySession` 提供一次性 nonce 挑战。
//!
//! 边界：本模块只做鉴权与来源校验，不做密钥的实际轮换（由 settings 完成）。
//! 关键不变量：密钥损坏时管理面锁定，恢复端点要求回环 peer + 回环 Host，
//! 写端点额外要求回环 Origin；nonce 每次尝试后即失效。
use std::sync::Arc;

use axum::{
    extract::{ConnectInfo, FromRequestParts},
    http::request::Parts,
};
use tokio::sync::Mutex;

use crate::{
    api_error::ApiError,
    application::Context,
    settings::{self, CorruptKey},
};

/// 密钥损坏端点使用的一次性恢复挑战。
///
/// 访问密钥损坏时管理面会锁定，因此恢复端点不能依赖 key。改为由 status 端点
/// 签发一个短时、单次使用的 256 位 nonce，只有同源 UI 能读到它；
/// regenerate 端点要求该 nonce（外加回环 peer、Host 与 Origin），并在每次尝试
/// ——无论成功、失败还是过期——后使其失效。跨站表单无法设置自定义的
/// `x-recovery-nonce` 头，也无法读取该 nonce，因此永远无法轮换密钥。
pub struct RecoverySession {
    nonce: Mutex<Option<RecoveryNonce>>,
    ttl: chrono::Duration,
}

struct RecoveryNonce {
    value: [u8; 32],
    expires_at: chrono::DateTime<chrono::Utc>,
}

impl RecoverySession {
    pub fn new() -> Arc<Self> {
        Self::with_ttl(chrono::Duration::seconds(60))
    }

    /// 测试缝：用短 TTL 验证过期处理，无需等待 60 秒。
    pub fn with_ttl(ttl: chrono::Duration) -> Arc<Self> {
        Arc::new(Self {
            nonce: Mutex::new(None),
            ttl,
        })
    }

    /// 返回当前有效的 nonce，若不存在或上一个已过期则签发新的。
    /// 仍有效的 nonce 原样返回，这样零散的 status 轮询不会烧掉 UI 的挑战。
    pub async fn issue(&self) -> String {
        let mut guard = self.nonce.lock().await;
        let expired = guard
            .as_ref()
            .is_none_or(|existing| existing.expires_at <= chrono::Utc::now());
        if expired {
            let mut bytes = [0u8; 32];
            rand::fill(&mut bytes);
            *guard = Some(RecoveryNonce {
                value: bytes,
                expires_at: chrono::Utc::now() + self.ttl,
            });
        }
        use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
        URL_SAFE_NO_PAD.encode(guard.as_ref().expect("just issued").value)
    }

    /// 消费该挑战：仅当 nonce 匹配且未过期时返回 `true`。
    /// 每次尝试都会让当前 nonce 失效，因此重放不可能，
    /// 暴力破解也只有一次机会去猜 256 位值。
    pub async fn consume(&self, supplied: &str) -> bool {
        let mut guard = self.nonce.lock().await;
        use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
        let valid = guard.as_ref().is_some_and(|challenge| {
            challenge.expires_at > chrono::Utc::now()
                && URL_SAFE_NO_PAD.encode(challenge.value) == supplied
        });
        *guard = None;
        valid
    }
}

/// 恢复端点在 `Host`/`Origin` 头里接受的主机名：DNS 重绑定攻击者连接到
/// 127.0.0.1，但浏览器仍会发送自己的（攻击者可控的）主机名，
/// 因此 authority 必须是回环名。
fn is_loopback_authority(authority: &str) -> bool {
    let host = authority
        .rsplit_once(':')
        .filter(|(_, port)| port.chars().all(|c| c.is_ascii_digit()))
        .map(|(host, _)| host)
        .unwrap_or(authority);
    let host = host.trim_start_matches('[').trim_end_matches(']');
    matches!(host, "127.0.0.1" | "localhost" | "::1")
}

pub struct AdminAuth;

/// 提取器对任何「能解引用到 `Context`」的状态都成立：生产路由的状态是
/// `state::AppState`（它 `Deref` 到 `Context`），测试夹具也传 `AppState`，
/// 因此 handler 既能取到端口字段，也能经 `Deref` 调用接收 `&Context` 的服务方法。
impl<S> FromRequestParts<S> for AdminAuth
where
    S: std::ops::Deref<Target = Context> + Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        let state: &Context = state;
        match settings::authorize_admin(state, &parts.headers).await {
            Ok(true) => Ok(Self),
            Ok(false) => Err(ApiError::unauthorized()),
            Err(error) => {
                if error.downcast_ref::<CorruptKey>().is_some() {
                    Err(ApiError::config_corrupted())
                } else if error.downcast_ref::<settings::ConfigCorrupted>().is_some() {
                    // 运行时设置损坏（例如坏掉的 `trust_local_network`）会以
                    // 独立错误 fail closed。当密钥本身健康时，消息会指向设置修复
                    // 流程，而不是密钥恢复流程。
                    let (admin_corrupt, gateway_corrupt) = settings::key_statuses(state)
                        .await
                        .map_err(ApiError::internal)?;
                    if admin_corrupt || gateway_corrupt {
                        Err(ApiError::config_corrupted())
                    } else {
                        Err(ApiError::config_corrupted_with(
                            "运行时设置数据损坏，请在设置页修复",
                        ))
                    }
                } else {
                    Err(ApiError::internal(error))
                }
            }
        }
    }
}

/// 密钥恢复端点的鉴权。访问密钥健康时它等同于 `AdminAuth`。
/// 一旦有 key 损坏，管理面即锁定（损坏的 `admin_access_key` 无法认证任何人），
/// 此时只有回环 peer 能抵达恢复端点——那是让密钥重新有效的原子再生成路径。
pub struct RecoveryAuth;

impl<S> FromRequestParts<S> for RecoveryAuth
where
    S: std::ops::Deref<Target = Context> + Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        let ctx: &Context = state;
        let (admin_corrupt, gateway_corrupt) = settings::key_statuses(ctx)
            .await
            .map_err(ApiError::internal)?;
        // 运行时设置损坏同样锁定管理面；恢复端点必须保持可达（以回环为门禁），
        // 以便设置修复流程能拿到 nonce。只有密钥与设置**双双**健康时，
        // 才降级为普通管理端鉴权。
        let (_, settings_corrupt_keys) = settings::runtime_settings_ui(&ctx.db).await;
        if !admin_corrupt && !gateway_corrupt && settings_corrupt_keys.is_empty() {
            // 密钥健康：降级为普通管理端鉴权。
            return AdminAuth::from_request_parts(parts, state)
                .await
                .map(|_| Self);
        }
        // 密钥或设置损坏：只有回环能抵达恢复端点，且请求不得来自 DNS 重绑定
        // 页面——Host authority 必须是回环名。status GET 另外不要求 Origin：
        // 浏览器并不保证在同源 GET 上发送 Origin，而跨站 GET 读取 nonce
        // 无论如何都会被 CORS 拦住。
        check_loopback_request(parts)?;
        Ok(Self)
    }
}

/// 恢复**写**端点的回环 + Host + Origin 校验。跨站表单或 DNS 重绑定页面
/// 会发送自己的 origin/host，因此即便 TCP peer 是 127.0.0.1，两项检查也会失败。
pub struct RecoveryGenerateAuth;

impl<S> FromRequestParts<S> for RecoveryGenerateAuth
where
    S: std::ops::Deref<Target = Context> + Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        let ctx: &Context = state;
        let (admin_corrupt, gateway_corrupt) = settings::key_statuses(ctx)
            .await
            .map_err(ApiError::internal)?;
        // 与 RecoveryAuth 一样，运行时设置损坏时**不得**强制此提取器走
        // AdminAuth——设置修复流程在设置损坏时正需要它。
        let (_, settings_corrupt_keys) = settings::runtime_settings_ui(&ctx.db).await;
        if !admin_corrupt && !gateway_corrupt && settings_corrupt_keys.is_empty() {
            // 密钥健康：降级为普通管理端鉴权。
            return AdminAuth::from_request_parts(parts, state)
                .await
                .map(|_| Self);
        }
        check_loopback_request(parts)?;
        let headers = &parts.headers;
        let origin_ok = headers
            .get("origin")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|origin| {
                origin
                    .strip_prefix("http://")
                    .or_else(|| origin.strip_prefix("https://"))
                    .is_some_and(is_loopback_authority)
            });
        if !origin_ok {
            return Err(ApiError::unauthorized());
        }
        Ok(Self)
    }
}

fn check_loopback_request(parts: &Parts) -> Result<(), ApiError> {
    let peer = parts
        .extensions
        .get::<ConnectInfo<std::net::SocketAddr>>()
        .ok_or_else(ApiError::unauthorized)?
        .0;
    if !peer.ip().is_loopback() {
        return Err(ApiError::unauthorized());
    }
    let host_ok = parts
        .headers
        .get("host")
        .and_then(|value| value.to_str().ok())
        .is_some_and(is_loopback_authority);
    if !host_ok {
        return Err(ApiError::unauthorized());
    }
    Ok(())
}
