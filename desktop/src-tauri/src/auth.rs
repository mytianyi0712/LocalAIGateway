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

/// One-time recovery challenge for the corrupt-key endpoints (P1-4).
///
/// When the access keys are corrupt the admin surface is locked down, so
/// the recovery endpoints cannot rely on a key. Instead the status endpoint
/// issues a short-lived, single-use 256-bit nonce that only the same-origin
/// UI can read; the regenerate endpoint requires that nonce (plus loopback
/// peer, Host and Origin) and invalidates it on every attempt — success,
/// failure, or expiry. A cross-site form cannot set the custom
/// `x-recovery-nonce` header or read the nonce, so it can never rotate the
/// keys.
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

    /// Test seam: short TTLs prove expiry handling without waiting 60 s.
    pub fn with_ttl(ttl: chrono::Duration) -> Arc<Self> {
        Arc::new(Self {
            nonce: Mutex::new(None),
            ttl,
        })
    }

    /// Returns the current valid nonce, issuing a fresh one when none
    /// exists or the previous one expired. A still-valid nonce is returned
    /// unchanged so stray status polls cannot burn the UI's challenge.
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

    /// Consumes the challenge: `true` only for a matching, unexpired nonce.
    /// Every attempt invalidates the current nonce, so a replay is
    /// impossible and a brute force has one shot at a 256-bit value.
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

/// Hostnames the recovery endpoints accept in the `Host`/`Origin` headers:
/// a DNS-rebinding attacker connects to 127.0.0.1 but the browser still
/// sends its own (attacker-controlled) hostname, so the authority must be a
/// loopback name.
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

impl FromRequestParts<Context> for AdminAuth {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Context,
    ) -> Result<Self, Self::Rejection> {
        match settings::authorize_admin(state, &parts.headers).await {
            Ok(true) => Ok(Self),
            Ok(false) => Err(ApiError::unauthorized()),
            Err(error) => {
                if error.downcast_ref::<CorruptKey>().is_some() {
                    Err(ApiError::config_corrupted())
                } else if error.downcast_ref::<settings::ConfigCorrupted>().is_some() {
                    // P1-3: corrupt runtime settings (e.g. a broken
                    // `trust_local_network`) fail closed with a distinct
                    // error. When the keys themselves are healthy, the
                    // message points at the settings repair flow instead of
                    // the key-recovery flow.
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

/// Auth for the key-recovery endpoints (P1-7). While the access keys are
/// healthy this is exactly `AdminAuth`. When a key is corrupt the admin
/// surface is locked down (a corrupt `admin_access_key` cannot authenticate
/// anyone), so only loopback peers may reach the recovery endpoints — the
/// atomic regeneration path that makes the keys valid again.
pub struct RecoveryAuth;

impl FromRequestParts<Context> for RecoveryAuth {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Context,
    ) -> Result<Self, Self::Rejection> {
        let (admin_corrupt, gateway_corrupt) = settings::key_statuses(state)
            .await
            .map_err(ApiError::internal)?;
        // P1-3: corrupt runtime settings also lock the admin surface; the
        // recovery endpoints must stay reachable (loopback-gated) so the
        // settings repair flow can get its nonce. Only when BOTH keys and
        // settings are healthy does this degrade to normal admin auth.
        let (_, settings_corrupt_keys) = settings::runtime_settings_ui(&state.db).await;
        if !admin_corrupt && !gateway_corrupt && settings_corrupt_keys.is_empty() {
            // Keys healthy: degrade to normal admin auth.
            return AdminAuth::from_request_parts(parts, state)
                .await
                .map(|_| Self);
        }
        // Keys or settings corrupt: only loopback may reach the recovery
        // endpoints, and the request must not come from a DNS-rebinding
        // page — the Host authority has to be a loopback name (P1-4). The
        // status GET additionally carries no Origin requirement: browsers
        // are not guaranteed to send Origin on same-origin GETs, and
        // reading the nonce from a cross-site GET is blocked by CORS
        // anyway.
        check_loopback_request(parts)?;
        Ok(Self)
    }
}

/// Loopback + Host + Origin verification for the recovery WRITE endpoint
/// (P1-4). A cross-site form or a DNS-rebinding page sends its own
/// origin/host, so both checks fail even though the TCP peer is 127.0.0.1.
pub struct RecoveryGenerateAuth;

impl FromRequestParts<Context> for RecoveryGenerateAuth {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Context,
    ) -> Result<Self, Self::Rejection> {
        let (admin_corrupt, gateway_corrupt) = settings::key_statuses(state)
            .await
            .map_err(ApiError::internal)?;
        // P1-3: like RecoveryAuth, corrupt runtime settings must NOT force
        // this extractor through AdminAuth — the settings-repair flow needs
        // it while settings are broken.
        let (_, settings_corrupt_keys) = settings::runtime_settings_ui(&state.db).await;
        if !admin_corrupt && !gateway_corrupt && settings_corrupt_keys.is_empty() {
            // Keys healthy: degrade to normal admin auth.
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
