use crate::{
    api_error::{ApiError, correlation_middleware, json_response},
    application::Context,
    auth::AdminAuth,
    crypto::SecretStore,
    db::Database,
    protocol::{PROTOCOL_ORDER, normalize_base_url, valid_protocol},
};
use axum::{
    Json, Router,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, patch, post, put},
};
use chrono::Utc;
use providers::{ProviderInput, ProviderPatch, ProviderRow, load_provider, provider_json};
use serde_json::{Map, Value, json};
use sqlx::{FromRow, QueryBuilder, Row};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use uuid::Uuid;
mod channels;
mod discovery;
mod logs;
mod mappings;
mod models;
mod profiles;
mod providers;
mod routes;
mod settings;
mod stats;
use channels::{
    create_channel, delete_channel, get_channel, list_channels, patch_channel, replace_api_key,
    reset_health,
};
use discovery::{discover_models, get_discovery_run, list_discovery_runs, manual_probe};
use logs::{clear_logs, get_request, list_health_probes, list_requests};
use mappings::{
    claude_presets, codex_presets, create_claude_mapping, create_codex_mapping,
    delete_claude_mapping, delete_codex_mapping, list_claude_mappings, list_codex_mappings,
    patch_claude_mapping, patch_codex_mapping, refresh_claude_presets, refresh_codex_presets,
};
use models::{create_manual_model, delete_channel_model, list_channel_models, patch_channel_model};
use profiles::{
    create_profile, delete_profile, detect_capabilities, get_capabilities, get_profile,
    list_profiles, put_capabilities, update_profile,
};
use providers::{create_provider, delete_provider, get_provider, list_providers, patch_provider};
use routes::{create_route, delete_route, list_routes, patch_route, replace_candidates};
use settings::{
    generate_access_keys, get_settings, patch_settings, recovery_key_status,
    recovery_repair_settings, system_protocols, system_status,
};
use stats::{stats_cache, stats_channels, stats_models, stats_summary, stats_timeseries};

type ApiResult = Result<Response, ApiError>;
pub fn router() -> Router<Context> {
    Router::new()
        .route(
            "/api/admin/v1/providers",
            get(list_providers).post(create_provider),
        )
        .route(
            "/api/admin/v1/providers/{id}",
            get(get_provider)
                .patch(patch_provider)
                .delete(delete_provider),
        )
        .route(
            "/api/admin/v1/channels",
            get(list_channels).post(create_channel),
        )
        .route(
            "/api/admin/v1/channels/{id}",
            get(get_channel).patch(patch_channel).delete(delete_channel),
        )
        .route("/api/admin/v1/channels/{id}/api-key", put(replace_api_key))
        .route(
            "/api/admin/v1/channels/{id}/reset-health",
            post(reset_health),
        )
        .route("/api/admin/v1/channels/{id}/probe", post(manual_probe))
        .route(
            "/api/admin/v1/channels/{id}/discover-models",
            post(discover_models),
        )
        .route(
            "/api/admin/v1/channels/{id}/discovery-runs",
            get(list_discovery_runs),
        )
        .route("/api/admin/v1/discovery-runs/{id}", get(get_discovery_run))
        .route("/api/admin/v1/channel-models", get(list_channel_models))
        .route(
            "/api/admin/v1/channels/{id}/models",
            post(create_manual_model),
        )
        .route(
            "/api/admin/v1/channel-models/{id}",
            patch(patch_channel_model).delete(delete_channel_model),
        )
        .route("/api/admin/v1/routes", get(list_routes).post(create_route))
        .route(
            "/api/admin/v1/routes/{id}",
            patch(patch_route).delete(delete_route),
        )
        .route(
            "/api/admin/v1/routes/{id}/candidates",
            put(replace_candidates),
        )
        .route(
            "/api/admin/v1/capability-profiles",
            get(list_profiles).post(create_profile),
        )
        .route(
            "/api/admin/v1/capability-profiles/{id}",
            get(get_profile).put(update_profile).delete(delete_profile),
        )
        .route(
            "/api/admin/v1/model-capabilities/detect/{*model_id}",
            post(detect_capabilities),
        )
        .route(
            "/api/admin/v1/model-capabilities/{*model_id}",
            get(get_capabilities).put(put_capabilities),
        )
        .route("/api/admin/v1/claude-presets", get(claude_presets))
        .route(
            "/api/admin/v1/claude-presets/refresh",
            post(refresh_claude_presets),
        )
        .route(
            "/api/admin/v1/claude-mappings",
            get(list_claude_mappings).post(create_claude_mapping),
        )
        .route(
            "/api/admin/v1/claude-mappings/{id}",
            patch(patch_claude_mapping).delete(delete_claude_mapping),
        )
        .route("/api/admin/v1/codex-presets", get(codex_presets))
        .route(
            "/api/admin/v1/codex-presets/refresh",
            post(refresh_codex_presets),
        )
        .route(
            "/api/admin/v1/codex-mappings",
            get(list_codex_mappings).post(create_codex_mapping),
        )
        .route(
            "/api/admin/v1/codex-mappings/{id}",
            patch(patch_codex_mapping).delete(delete_codex_mapping),
        )
        .route("/api/admin/v1/requests", get(list_requests))
        .route("/api/admin/v1/requests/{id}", get(get_request))
        .route("/api/admin/v1/health-probes", get(list_health_probes))
        .route("/api/admin/v1/logs", delete(clear_logs))
        .route("/api/admin/v1/stats/summary", get(stats_summary))
        .route("/api/admin/v1/stats/cache", get(stats_cache))
        .route("/api/admin/v1/stats/models", get(stats_models))
        .route("/api/admin/v1/stats/channels", get(stats_channels))
        .route("/api/admin/v1/stats/timeseries", get(stats_timeseries))
        .route(
            "/api/admin/v1/settings",
            get(get_settings).patch(patch_settings),
        )
        .route(
            "/api/admin/v1/settings/access-keys/status",
            get(recovery_key_status),
        )
        .route(
            "/api/admin/v1/settings/access-keys/generate",
            post(generate_access_keys),
        )
        .route(
            "/api/admin/v1/settings/recovery-repair",
            patch(recovery_repair_settings),
        )
        .route("/api/admin/v1/system/status", get(system_status))
        .route("/api/admin/v1/system/protocols", get(system_protocols))
        .layer(axum::middleware::from_fn(correlation_middleware))
}
fn now() -> String {
    Utc::now().to_rfc3339()
}
fn id() -> String {
    Uuid::new_v4().to_string()
}
fn ok(value: Value) -> Response {
    Json(value).into_response()
}
fn no_content() -> Response {
    StatusCode::NO_CONTENT.into_response()
}
fn validate_text(value: &str, field: &str, max: usize) -> Result<(), ApiError> {
    if value.trim().is_empty() || value.chars().count() > max {
        return Err(ApiError::validation(format!("{field} 长度无效")));
    }
    Ok(())
}
fn integrity(error: sqlx::Error, message: &str) -> ApiError {
    if matches!(error, sqlx::Error::Database(_)) {
        ApiError::conflict(message)
    } else {
        ApiError::internal(error)
    }
}
#[derive(FromRow)]
/// Admin service (P2-1): provider/channel domain operations. Handlers are
/// thin shells delegating here; the service owns the SQL and the response
/// assembly, depending only on storage and the secret store. Other admin
/// subdomains (routes, capabilities, mappings, logs, stats, settings) are
/// migrated progressively.
pub struct AdminService {
    db: Database,
    secrets: SecretStore,
}
impl AdminService {
    pub async fn list_providers(&self) -> ApiResult {
        let rows = sqlx::query_as::<_, ProviderRow>("SELECT p.id,p.name,p.base_url,p.created_at,p.updated_at,COUNT(c.id) channel_count FROM providers p LEFT JOIN channels c ON c.provider_id=p.id GROUP BY p.id ORDER BY p.name")
            .fetch_all(self.db.pool()).await?;
        let items: Vec<_> = rows.into_iter().map(provider_json).collect();
        Ok(ok(
            json!({"items":items,"total":items.len(),"page":1,"page_size":items.len()}),
        ))
    }
    pub async fn create_provider(&self, input: ProviderInput) -> ApiResult {
        validate_text(&input.name, "name", 120)?;
        let base_url = normalize_base_url(&input.base_url)
            .map_err(|error| ApiError::validation(error.to_string()))?;
        let row_id = id();
        let time = now();
        sqlx::query(
            "INSERT INTO providers(id,name,base_url,created_at,updated_at) VALUES(?,?,?,?,?)",
        )
        .bind(&row_id)
        .bind(input.name.trim())
        .bind(base_url)
        .bind(&time)
        .bind(&time)
        .execute(self.db.pool())
        .await
        .map_err(|e| integrity(e, "Provider already exists"))?;
        let row = load_provider(&self.db, &row_id).await?;
        Ok(json_response(StatusCode::CREATED, provider_json(row)))
    }
    pub async fn get_provider(&self, row_id: &str) -> ApiResult {
        Ok(ok(provider_json(load_provider(&self.db, row_id).await?)))
    }
    pub async fn patch_provider(&self, row_id: &str, input: ProviderPatch) -> ApiResult {
        // P1-4: validate EVERYTHING before touching the row, then commit
        // name and base_url in ONE atomic UPDATE — a bad URL or a failed
        // statement can no longer leave the name half-updated.
        let name = match input.name {
            Some(name) => {
                validate_text(&name, "name", 120)?;
                Some(name.trim().to_owned())
            }
            None => None,
        };
        let base_url = match input.base_url {
            Some(url) => Some(
                normalize_base_url(&url)
                    .map_err(|error| ApiError::validation(error.to_string()))?,
            ),
            None => None,
        };
        if name.is_none() && base_url.is_none() {
            return self.get_provider(row_id).await;
        }
        let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM providers WHERE id=?")
            .bind(row_id)
            .fetch_one(self.db.pool())
            .await?;
        if exists == 0 {
            return Err(ApiError::not_found("Provider not found"));
        }
        let mut builder: sqlx::QueryBuilder<sqlx::Sqlite> =
            sqlx::QueryBuilder::new("UPDATE providers SET");
        let mut comma = false;
        if let Some(name) = &name {
            builder.push(" name=").push_bind(name);
            comma = true;
        }
        if let Some(url) = &base_url {
            if comma {
                builder.push(",");
            }
            builder.push(" base_url=").push_bind(url);
            comma = true;
        }
        if comma {
            builder.push(",");
        }
        builder.push(" updated_at=").push_bind(now());
        builder.push(" WHERE id=").push_bind(row_id);
        builder
            .build()
            .execute(self.db.pool())
            .await
            .map_err(|e| integrity(e, "Provider already exists"))?;
        self.get_provider(row_id).await
    }

    /// P1-4: delete is already one transaction; keep the cascades explicit
    /// (route_candidates → channels → provider).
    pub async fn delete_provider(&self, row_id: &str) -> ApiResult {
        let mut tx = self.db.pool().begin().await?;
        sqlx::query("DELETE FROM route_candidates WHERE channel_model_id IN (SELECT cm.id FROM channel_models cm JOIN channels c ON c.id=cm.channel_id WHERE c.provider_id=?)").bind(row_id).execute(&mut *tx).await?;
        sqlx::query("DELETE FROM channels WHERE provider_id=?")
            .bind(row_id)
            .execute(&mut *tx)
            .await?;
        let result = sqlx::query("DELETE FROM providers WHERE id=?")
            .bind(row_id)
            .execute(&mut *tx)
            .await?;
        if result.rows_affected() == 0 {
            return Err(ApiError::not_found("Provider not found"));
        }
        tx.commit().await?;
        Ok(no_content())
    }

    pub fn new(db: Database, secrets: SecretStore) -> Arc<Self> {
        Arc::new(Self { db, secrets })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use super::{
        channels::{ChannelInput, ChannelPatch},
        profiles::ProfileInput,
        routes::{CandidateInput, CandidateList},
        stats::{SummaryQuery, format_utc_millis, parse_utc_rfc3339, resolve_token_window},
    };
    use crate::{
        api_error::ErrorCode,
        auth::{RecoveryAuth, RecoveryGenerateAuth},
        config::AppConfig,
        db::Database,
        settings as core_settings,
    };
    use axum::extract::{FromRequestParts, Path, State};

    use std::sync::Arc;

    fn window(from: &str, to: &str) -> SummaryQuery {
        SummaryQuery {
            from: Some(from.to_owned()),
            to: Some(to.to_owned()),
        }
    }

    #[test]
    fn utc_plus_8_local_midnight_maps_to_previous_day_16z() {
        let from = parse_utc_rfc3339("2026-08-04T00:00:00+08:00").unwrap();
        assert_eq!(format_utc_millis(from), "2026-08-03T16:00:00.000Z");
    }

    #[test]
    fn arbitrary_offsets_and_fractional_seconds_normalize_to_utc() {
        let from = parse_utc_rfc3339("2026-08-04T08:30:00Z").unwrap();
        assert_eq!(format_utc_millis(from), "2026-08-04T08:30:00.000Z");
        let from = parse_utc_rfc3339("2026-08-04T08:30:00.125+05:30").unwrap();
        assert_eq!(format_utc_millis(from), "2026-08-04T03:00:00.125Z");
        let from = parse_utc_rfc3339("2026-08-04T23:59:59.999-07:00").unwrap();
        assert_eq!(format_utc_millis(from), "2026-08-05T06:59:59.999Z");
    }

    #[test]
    fn naive_timestamp_without_timezone_is_rejected() {
        assert!(parse_utc_rfc3339("2026-08-04T08:30:00").is_err());
        assert!(parse_utc_rfc3339("2026-08-04").is_err());
    }

    #[test]
    fn canonical_occurred_at_format_is_lexicographically_ordered() {
        let values = [
            "2026-08-03T15:59:59.999Z",
            "2026-08-03T16:00:00.000Z",
            "2026-08-03T16:00:00.001Z",
            "2026-08-03T16:00:01.000Z",
        ];
        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        assert_eq!(
            sorted, values,
            "fixed-millis Z format must sort chronologically"
        );
        // The half-open window start must compare equal to a stored value at
        // the same instant.
        let window = resolve_token_window(&window(
            "2026-08-04T00:00:00+08:00",
            "2026-08-05T00:00:00+08:00",
        ))
        .unwrap()
        .unwrap();
        assert_eq!(format_utc_millis(window.from), "2026-08-03T16:00:00.000Z");
        assert_eq!(format_utc_millis(window.to), "2026-08-04T16:00:00.000Z");
    }

    #[test]
    fn omitted_range_means_all_history() {
        assert!(
            resolve_token_window(&SummaryQuery::default())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn partial_range_is_rejected() {
        let both = resolve_token_window(&window(
            "2026-08-04T00:00:00+08:00",
            "2026-08-05T00:00:00+08:00",
        ));
        assert!(both.is_ok());
        assert!(
            resolve_token_window(&SummaryQuery {
                from: Some("2026-08-04T00:00:00Z".into()),
                to: None
            })
            .is_err()
        );
        assert!(
            resolve_token_window(&SummaryQuery {
                from: None,
                to: Some("2026-08-05T00:00:00Z".into())
            })
            .is_err()
        );
    }

    #[test]
    fn empty_or_reversed_range_is_rejected() {
        let equal = resolve_token_window(&window("2026-08-04T00:00:00Z", "2026-08-04T00:00:00Z"));
        assert!(equal.is_err());
        let reversed = resolve_token_window(&window(
            "2026-08-05T00:00:00+08:00",
            "2026-08-04T00:00:00+08:00",
        ));
        assert!(reversed.is_err());
    }

    #[test]
    fn boundary_values_keep_half_open_semantics() {
        // A stored occurred_at exactly at `from` must compare >= the bound,
        // and one exactly at `to` must compare < the bound.
        let window = resolve_token_window(&window(
            "2026-08-03T16:00:00.000Z",
            "2026-08-04T16:00:00.000Z",
        ))
        .unwrap()
        .unwrap();
        let from = format_utc_millis(window.from);
        let to = format_utc_millis(window.to);
        assert!("2026-08-03T16:00:00.000Z" >= from.as_str());
        assert!("2026-08-04T15:59:59.999Z" < to.as_str());
        assert!(
            "2026-08-04T16:00:00.000Z" >= to.as_str(),
            "end is exclusive"
        );
        assert!(
            "2026-08-03T15:59:59.999Z" < from.as_str(),
            "start is inclusive"
        );
    }

    async fn test_state() -> (Context, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("lagw-admin-test-{}", uuid::Uuid::new_v4()));
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

    /// P1-7: a settings patch writes the keys and the runtime settings in one
    /// transaction; both must be readable afterwards.
    #[tokio::test]
    async fn patch_settings_writes_keys_and_settings_atomically() {
        let (state, _dir) = test_state().await;
        let response = patch_settings(
            AdminAuth,
            State(state.clone()),
            Json(json!({
                "admin_access_key": "new-admin-key",
                "gateway_access_key": "new-gateway-key",
                "max_request_body_mb": 128,
            })),
        )
        .await
        .expect("patch must succeed");
        assert_eq!(response.status(), StatusCode::OK);
        let rows = sqlx::query("SELECT key, CAST(value_json AS TEXT) value_json FROM settings")
            .fetch_all(state.db.pool())
            .await
            .unwrap();
        let mut by_key = HashMap::new();
        for row in rows {
            by_key.insert(
                row.get::<String, _>("key"),
                row.get::<String, _>("value_json"),
            );
        }
        let admin_raw = by_key.get("admin_access_key").expect("admin key row");
        let admin_plain: String = serde_json::from_str(admin_raw).unwrap();
        assert_eq!(
            state.secrets.decrypt(admin_plain.as_bytes()).unwrap(),
            "new-admin-key"
        );
        let gateway_raw = by_key.get("gateway_access_key").expect("gateway key row");
        let gateway_plain: String = serde_json::from_str(gateway_raw).unwrap();
        assert_eq!(
            state.secrets.decrypt(gateway_plain.as_bytes()).unwrap(),
            "new-gateway-key"
        );
        assert_eq!(
            by_key.get("max_request_body_mb").map(String::as_str),
            Some("128")
        );
    }

    /// P1-7: updating a capability profile propagates to model_caps in one
    /// transaction; both sides are consistent afterwards.
    #[tokio::test]
    async fn update_profile_propagates_to_model_caps_in_one_transaction() {
        let (state, _dir) = test_state().await;
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("INSERT INTO capability_profiles(id,name,description,context_window,max_tokens,supports_image_input,reasoning,thinking_level_map,created_at,updated_at) VALUES('profile-1','P',NULL,1000,2000,0,0,NULL,?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO model_caps(requested_model_id,context_window,max_tokens,source,profile_id,created_at,updated_at) VALUES('m-1',1000,2000,'manual','profile-1',?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        let response = update_profile(
            AdminAuth,
            State(state.clone()),
            Path("profile-1".to_owned()),
            Json(ProfileInput {
                name: "Updated".into(),
                description: Some("desc".into()),
                context_window: Some(64000),
                max_tokens: None,
                supports_image_input: None,
                reasoning: None,
                thinking_level_map: None,
            }),
        )
        .await
        .expect("update must succeed");
        assert_eq!(response.status(), StatusCode::OK);
        let caps: (i64, String) = sqlx::query_as(
            "SELECT context_window, profile_id FROM model_caps WHERE requested_model_id='m-1'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(caps.0, 64000, "model_caps must follow the profile update");
        assert_eq!(caps.1, "profile-1");
        let profile_name: String =
            sqlx::query_scalar("SELECT name FROM capability_profiles WHERE id='profile-1'")
                .fetch_one(state.db.pool())
                .await
                .unwrap();
        assert_eq!(profile_name, "Updated");
    }

    /// P1-4: a failed provider PATCH must leave BOTH fields unchanged — the
    /// name and base_url commit in a single atomic UPDATE, so a trigger
    /// failure cannot produce a half-updated row.
    #[tokio::test]
    async fn provider_patch_failure_rolls_back_both_fields() {
        let (state, _dir) = test_state().await;
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("INSERT INTO providers(id,name,base_url,created_at,updated_at) VALUES('prov-1','original','http://127.0.0.1:1',?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        // Inject a failure on ANY providers UPDATE — the second statement of
        // the old two-statement implementation would have left the name
        // changed; the single UPDATE must fail as one unit.
        sqlx::query("CREATE TRIGGER fail_provider_update BEFORE UPDATE ON providers BEGIN SELECT RAISE(ABORT, 'injected provider failure'); END")
            .execute(state.db.pool())
            .await
            .unwrap();
        let error = patch_provider(
            AdminAuth,
            State(state.clone()),
            Path("prov-1".to_owned()),
            Json(ProviderPatch {
                name: Some("renamed".into()),
                base_url: Some("http://127.0.0.1:2".into()),
            }),
        )
        .await
        .expect_err("injected failure must surface as an error");
        assert!(
            error.status.is_client_error() || error.status.is_server_error(),
            "injected failure must surface as an error status"
        );
        let (name, base_url): (String, String) = sqlx::query_as(
            "SELECT name, base_url FROM providers WHERE id='prov-1'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(name, "original", "name must roll back with the failure");
        assert_eq!(
            base_url, "http://127.0.0.1:1",
            "base_url must roll back with the failure"
        );
    }

    /// P1-4: a successful provider PATCH writes name + base_url in one
    /// statement, so the row carries a single consistent updated_at.
    #[tokio::test]
    async fn provider_patch_success_writes_single_consistent_snapshot() {
        let (state, _dir) = test_state().await;
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("INSERT INTO providers(id,name,base_url,created_at,updated_at) VALUES('prov-1','original','http://127.0.0.1:1',?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        let response = patch_provider(
            AdminAuth,
            State(state.clone()),
            Path("prov-1".to_owned()),
            Json(ProviderPatch {
                name: Some("renamed".into()),
                base_url: Some("http://127.0.0.1:2".into()),
            }),
        )
        .await
        .expect("patch must succeed");
        assert_eq!(response.status(), StatusCode::OK);
        let (name, base_url, updated_at): (String, String, String) = sqlx::query_as(
            "SELECT name, base_url, updated_at FROM providers WHERE id='prov-1'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(name, "renamed");
        assert_eq!(base_url, "http://127.0.0.1:2");
        assert_ne!(updated_at, time, "updated_at must move on");
        // A bad base_url is rejected BEFORE any write (validation order).
        let error = patch_provider(
            AdminAuth,
            State(state.clone()),
            Path("prov-1".to_owned()),
            Json(ProviderPatch {
                name: Some("renamed-again".into()),
                base_url: Some("not a url".into()),
            }),
        )
        .await
        .expect_err("pre-validation must reject a bad base_url");
        assert_eq!(error.status, StatusCode::UNPROCESSABLE_ENTITY);
        let name: String =
            sqlx::query_scalar("SELECT name FROM providers WHERE id='prov-1'")
                .fetch_one(state.db.pool())
                .await
                .unwrap();
        assert_eq!(name, "renamed", "pre-validation must not write the name");
    }

    /// P1-4: a failure while propagating a profile update to model_caps
    /// rolls the WHOLE update back — the profile row must not change when
    /// the caps propagation fails.
    #[tokio::test]
    async fn profile_update_rolls_back_with_caps_propagation_failure() {
        let (state, _dir) = test_state().await;
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("INSERT INTO capability_profiles(id,name,description,context_window,max_tokens,supports_image_input,reasoning,thinking_level_map,created_at,updated_at) VALUES('profile-1','P',NULL,1000,2000,0,0,NULL,?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO model_caps(requested_model_id,context_window,max_tokens,source,profile_id,created_at,updated_at) VALUES('m-1',1000,2000,'manual','profile-1',?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        // Fail the SECOND statement (the model_caps propagation) — with the
        // old non-transactional code the profile row would already be
        // committed and stay changed.
        sqlx::query("CREATE TRIGGER fail_model_caps_update BEFORE UPDATE ON model_caps BEGIN SELECT RAISE(ABORT, 'injected caps failure'); END")
            .execute(state.db.pool())
            .await
            .unwrap();
        let error = update_profile(
            AdminAuth,
            State(state.clone()),
            Path("profile-1".to_owned()),
            Json(ProfileInput {
                name: "Updated".into(),
                description: Some("desc".into()),
                context_window: Some(64000),
                max_tokens: None,
                supports_image_input: None,
                reasoning: None,
                thinking_level_map: None,
            }),
        )
        .await
        .expect_err("injected failure must surface as an error");
        assert!(
            error.status.is_client_error() || error.status.is_server_error(),
            "injected failure must surface as an error status"
        );
        let (name, context_window): (String, Option<i64>) = sqlx::query_as(
            "SELECT name, context_window FROM capability_profiles WHERE id='profile-1'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(name, "P", "profile row must roll back with the failure");
        assert_eq!(context_window, Some(1000));
        let caps: Option<i64> = sqlx::query_scalar(
            "SELECT context_window FROM model_caps WHERE requested_model_id='m-1'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(caps, Some(1000), "model_caps must be untouched");
    }

    /// P2-6: saving candidates whose channel model supports none of the
    /// route's protocols is rejected with 422 and changes nothing.
    #[tokio::test]
    async fn replace_candidates_rejects_protocol_unsupported_items() {
        let (state, _dir) = test_state().await;
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("INSERT INTO providers(id,name,base_url,created_at,updated_at) VALUES('prov-1','mock','http://127.0.0.1:1',?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channels(id,provider_id,name,protocol,api_key_encrypted,api_key_hint,manual_enabled,created_at,updated_at) VALUES('ch-1','prov-1','chan','openai_compatible',x'00','',1,?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_models(id,channel_id,model_id,display_name,source,available,first_seen_at,created_at,updated_at) VALUES('cm-1','ch-1','test-model','Test','discovered',1,?,?,?)")
            .bind(time)
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        // The channel model only supports claude; the route is openai.
        sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES('cm-1','claude')")
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO model_routes(id,protocol,requested_model_id,enabled,created_at,updated_at) VALUES('route-1','openai_compatible','test-model',1,?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        // A pre-existing valid candidate (openai support) so the rejection
        // must leave existing rows untouched (P2-1). It lives on a second
        // channel: channel_models is unique per (channel_id, model_id).
        sqlx::query("INSERT INTO channels(id,provider_id,name,protocol,api_key_encrypted,api_key_hint,manual_enabled,created_at,updated_at) VALUES('ch-2','prov-1','chan2','openai_compatible',x'00','',1,?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_models(id,channel_id,model_id,display_name,source,available,first_seen_at,created_at,updated_at) VALUES('cm-2','ch-2','test-model','Test','discovered',1,?,?,?)")
            .bind(time)
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES('cm-2','openai_compatible')")
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO route_candidates(id,route_id,channel_model_id,priority,enabled,created_at,updated_at) VALUES('rc-2','route-1','cm-2',1,1,?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        let result = replace_candidates(
            AdminAuth,
            State(state.clone()),
            Path("route-1".to_owned()),
            Json(CandidateList {
                candidates: vec![CandidateInput {
                    channel_model_id: "cm-1".into(),
                    priority: 1,
                    enabled: true,
                }],
            }),
        )
        .await;
        let error = result.expect_err("unsupported candidate must be rejected");
        assert_eq!(error.status, StatusCode::UNPROCESSABLE_ENTITY);
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM route_candidates")
            .fetch_one(state.db.pool())
            .await
            .unwrap();
        assert_eq!(
            count, 1,
            "the pre-existing candidate must survive the rejection"
        );
        let surviving: String = sqlx::query_scalar("SELECT channel_model_id FROM route_candidates")
            .fetch_one(state.db.pool())
            .await
            .unwrap();
        assert_eq!(surviving, "cm-2", "nothing may be saved on rejection");
    }

    /// P2-1: candidates are inserted per (route, protocol) pair — a claude
    /// candidate lands only under the claude sibling route, an openai
    /// candidate only under the openai sibling.
    #[tokio::test]
    async fn replace_candidates_mixed_protocols_inserts_matching_pairs() {
        let (state, _dir) = test_state().await;
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("INSERT INTO providers(id,name,base_url,created_at,updated_at) VALUES('prov-1','mock','http://127.0.0.1:1',?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channels(id,provider_id,name,protocol,api_key_encrypted,api_key_hint,manual_enabled,created_at,updated_at) VALUES('ch-openai','prov-1','chan-oai','openai_compatible',x'00','',1,?,?),('ch-claude','prov-1','chan-claude','claude',x'00','',1,?,?)")
            .bind(time)
            .bind(time)
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_models(id,channel_id,model_id,display_name,source,available,first_seen_at,created_at,updated_at) VALUES('cm-openai','ch-openai','test-model','OpenAI','discovered',1,?,?,?),('cm-claude','ch-claude','test-model','Claude','discovered',1,?,?,?)")
            .bind(time)
            .bind(time)
            .bind(time)
            .bind(time)
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES('cm-openai','openai_compatible'),('cm-claude','claude')")
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO model_routes(id,protocol,requested_model_id,enabled,created_at,updated_at) VALUES('route-openai','openai_compatible','test-model',1,?,?),('route-claude','claude','test-model',1,?,?)")
            .bind(time)
            .bind(time)
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        let response = replace_candidates(
            AdminAuth,
            State(state.clone()),
            Path("route-openai".to_owned()),
            Json(CandidateList {
                candidates: vec![
                    CandidateInput {
                        channel_model_id: "cm-openai".into(),
                        priority: 1,
                        enabled: true,
                    },
                    CandidateInput {
                        channel_model_id: "cm-claude".into(),
                        priority: 2,
                        enabled: true,
                    },
                ],
            }),
        )
        .await
        .expect("mixed-protocol candidates must be accepted");
        assert_eq!(response.status(), StatusCode::OK);
        let rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT rc.route_id, rc.channel_model_id, mr.protocol FROM route_candidates rc \
             JOIN model_routes mr ON mr.id = rc.route_id ORDER BY rc.route_id",
        )
        .fetch_all(state.db.pool())
        .await
        .unwrap();
        assert_eq!(rows.len(), 2, "one candidate per matching sibling route");
        assert!(rows.contains(&(
            "route-openai".into(),
            "cm-openai".into(),
            "openai_compatible".into()
        )));
        assert!(rows.contains(&("route-claude".into(), "cm-claude".into(), "claude".into())));
    }

    /// P1-7: with a corrupt admin key, the recovery path (healthy keys ->
    /// AdminAuth, corrupt keys -> loopback only) must still regenerate both
    /// access keys atomically, and a `RecoveryAuth` extraction without the
    /// `ConnectInfo` extension must be rejected.
    #[tokio::test]
    async fn corrupt_key_recovery_generates_fresh_keys() {
        let (state, _dir) = test_state().await;
        let time = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO settings(key, value_json, updated_at) VALUES('admin_access_key', ?, ?)",
        )
        .bind("gAAAAABnot-a-valid-fernet-token")
        .bind(&time)
        .execute(state.db.pool())
        .await
        .unwrap();
        // While the key is corrupt, an extraction without the ConnectInfo
        // extension (no real HTTP peer) must be rejected.
        let parts = axum::extract::Request::builder()
            .uri("/")
            .body(())
            .unwrap()
            .into_parts()
            .0;
        let mut parts = parts;
        let rejected = RecoveryAuth::from_request_parts(&mut parts, &state).await;
        assert!(
            matches!(rejected, Err(error) if error.status == StatusCode::UNAUTHORIZED),
            "missing ConnectInfo must be rejected while keys are corrupt"
        );
        // P1-4: without the one-time nonce the generate endpoint is closed.
        let rejected = generate_access_keys(
            RecoveryGenerateAuth,
            State(state.clone()),
            axum::http::HeaderMap::new(),
        )
        .await
        .expect_err("missing nonce must be rejected");
        assert_eq!(rejected.status, StatusCode::UNAUTHORIZED);
        // A valid nonce (as issued by the recovery status endpoint) allows
        // one regeneration.
        let nonce = state.recovery.issue().await;
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "x-recovery-nonce",
            axum::http::HeaderValue::from_str(&nonce).unwrap(),
        );
        let response = generate_access_keys(RecoveryGenerateAuth, State(state.clone()), headers)
            .await
            .expect("loopback recovery with a valid nonce must regenerate the keys");
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        let admin = value
            .get("admin_access_key")
            .and_then(Value::as_str)
            .unwrap();
        let gateway = value
            .get("gateway_access_key")
            .and_then(Value::as_str)
            .unwrap();
        assert!(!admin.is_empty() && !gateway.is_empty());
        // The freshly written keys must be decryptable again.
        let policy = core_settings::access_policy(&state)
            .await
            .expect("keys must be valid again");
        assert_eq!(policy.admin_key, admin);
        assert_eq!(policy.gateway_key, gateway);
        // The nonce is single-use: replaying it must be rejected.
        let mut replay = axum::http::HeaderMap::new();
        replay.insert(
            "x-recovery-nonce",
            axum::http::HeaderValue::from_str(&nonce).unwrap(),
        );
        assert!(
            generate_access_keys(RecoveryGenerateAuth, State(state.clone()), replay)
                .await
                .is_err(),
            "nonce replay must be rejected"
        );
    }

    /// P1-3: corrupt runtime settings fail closed (admin surface locked with
    /// `config_corrupted`) but stay repairable through the loopback + nonce
    /// recovery-repair endpoint; the corrupt rows are overwritten and reads
    /// recover.
    #[tokio::test]
    async fn corrupt_settings_are_repairable_via_recovery_endpoint() {
        let (state, _dir) = test_state().await;
        let time = chrono::Utc::now().to_rfc3339();
        sqlx::query("INSERT INTO settings(key, value_json, updated_at) VALUES('trust_local_network', ?, ?)")
            .bind(json!("yes").to_string())
            .bind(&time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO settings(key, value_json, updated_at) VALUES('failure_threshold', ?, ?)")
            .bind(r#"{"broken":true}"#.to_string())
            .bind(&time)
            .execute(state.db.pool())
            .await
            .unwrap();
        // 1. Admin auth fails closed with the config_corrupted signal.
        let parts = axum::extract::Request::builder()
            .uri("/")
            .body(())
            .unwrap()
            .into_parts()
            .0;
        let mut parts = parts;
        let rejected = AdminAuth::from_request_parts(&mut parts, &state).await;
        assert!(
            matches!(rejected, Err(error) if error.code == ErrorCode::ConfigCorrupted),
            "corrupt settings must fail closed with config_corrupted"
        );
        // 2. The recovery status endpoint reports the corrupt keys.
        let response = recovery_key_status(RecoveryAuth, State(state.clone()))
            .await
            .expect("recovery status must load");
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let status: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(status["settings_corrupt"], json!(true));
        let corrupt_keys = status["config_corrupted_keys"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>();
        assert!(
            corrupt_keys.contains(&"trust_local_network"),
            "the corrupt trust row must be named: {corrupt_keys:?}"
        );
        assert!(corrupt_keys.contains(&"failure_threshold"));
        let nonce = status["recovery_nonce"].as_str().unwrap().to_owned();
        // 3. Repair with the nonce: valid rows overwrite the corrupt ones.
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "x-recovery-nonce",
            axum::http::HeaderValue::from_str(&nonce).unwrap(),
        );
        let response = recovery_repair_settings(
            RecoveryGenerateAuth,
            State(state.clone()),
            headers,
            Json(json!({
                "trust_local_network": true,
                "failure_threshold": 5,
            })),
        )
        .await
        .expect("loopback repair with a valid nonce must succeed");
        assert_eq!(response.status(), StatusCode::OK);
        let settings = core_settings::runtime_settings(&state)
            .await
            .expect("settings must be readable again after repair");
        assert!(settings.trust_local_network, "repaired value must be read");
        assert_eq!(settings.failure_threshold, 5);
        // 4. Admin auth works again.
        let parts = axum::extract::Request::builder()
            .uri("/")
            .body(())
            .unwrap()
            .into_parts()
            .0;
        let mut parts = parts;
        AdminAuth::from_request_parts(&mut parts, &state)
            .await
            .expect("admin auth must recover after the repair");
        // 5. The nonce is single-use.
        let mut replay = axum::http::HeaderMap::new();
        replay.insert(
            "x-recovery-nonce",
            axum::http::HeaderValue::from_str(&nonce).unwrap(),
        );
        let rejected = recovery_repair_settings(
            RecoveryGenerateAuth,
            State(state.clone()),
            replay,
            Json(json!({"trust_local_network": false})),
        )
        .await
        .expect_err("the consumed nonce must not be replayable");
        assert_eq!(rejected.status, StatusCode::UNAUTHORIZED);
        // 6. Access keys are never accepted through the repair endpoint.
        let nonce = state.recovery.issue().await;
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "x-recovery-nonce",
            axum::http::HeaderValue::from_str(&nonce).unwrap(),
        );
        let _ = recovery_repair_settings(
            RecoveryGenerateAuth,
            State(state.clone()),
            headers,
            Json(json!({"admin_access_key": "should-be-ignored"})),
        )
        .await
        .expect("repair must ignore key fields");
        let key_rows: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM settings WHERE key='admin_access_key'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(key_rows, 0, "repair must never write access keys");
    }

    /// P1-4: the generate endpoint's extractor rejects cross-site origins,
    /// missing origins, DNS-rebinding hosts, and non-loopback peers.
    #[tokio::test]
    async fn recovery_generate_auth_rejects_cross_site_requests() {
        use axum::extract::{ConnectInfo, FromRequestParts};
        let (state, _dir) = test_state().await;
        let time = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO settings(key, value_json, updated_at) VALUES('admin_access_key', ?, ?)",
        )
        .bind("gAAAAABnot-a-valid-fernet-token")
        .bind(&time)
        .execute(state.db.pool())
        .await
        .unwrap();
        let build_parts = |host: &str, origin: Option<&str>, peer: std::net::IpAddr| {
            let mut builder = axum::extract::Request::builder()
                .uri("/")
                .header("host", host);
            if let Some(origin) = origin {
                builder = builder.header("origin", origin);
            }
            let mut parts = builder.body(()).unwrap().into_parts().0;
            parts
                .extensions
                .insert(ConnectInfo(std::net::SocketAddr::new(peer, 54321)));
            parts
        };
        let loopback = std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
        // The legitimate same-origin call passes.
        let mut parts = build_parts("127.0.0.1:3000", Some("http://127.0.0.1:3000"), loopback);
        assert!(
            RecoveryGenerateAuth::from_request_parts(&mut parts, &state)
                .await
                .is_ok(),
            "same-origin loopback must pass"
        );
        // Cross-site form: loopback peer but the attacker's origin.
        let mut parts = build_parts("127.0.0.1:3000", Some("http://evil.example"), loopback);
        assert!(matches!(
            RecoveryGenerateAuth::from_request_parts(&mut parts, &state).await,
            Err(error) if error.status == StatusCode::UNAUTHORIZED
        ));
        // No Origin header at all (e.g. a raw socket form).
        let mut parts = build_parts("127.0.0.1:3000", None, loopback);
        assert!(matches!(
            RecoveryGenerateAuth::from_request_parts(&mut parts, &state).await,
            Err(error) if error.status == StatusCode::UNAUTHORIZED
        ));
        // DNS rebinding: loopback peer, attacker-controlled Host.
        let mut parts = build_parts("evil.example:3000", Some("http://evil.example"), loopback);
        assert!(matches!(
            RecoveryGenerateAuth::from_request_parts(&mut parts, &state).await,
            Err(error) if error.status == StatusCode::UNAUTHORIZED
        ));
        // Non-loopback peer.
        let mut parts = build_parts(
            "127.0.0.1:3000",
            Some("http://127.0.0.1:3000"),
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 1)),
        );
        assert!(matches!(
            RecoveryGenerateAuth::from_request_parts(&mut parts, &state).await,
            Err(error) if error.status == StatusCode::UNAUTHORIZED
        ));
    }

    /// P1-4: an expired recovery nonce is rejected — the challenge is
    /// short-lived even if it was never used.
    #[tokio::test]
    async fn recovery_nonce_expires() {
        let session = crate::auth::RecoverySession::with_ttl(chrono::Duration::milliseconds(50));
        let nonce = session.issue().await;
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        assert!(
            !session.consume(&nonce).await,
            "an expired nonce must be rejected"
        );
    }

    /// P2-3: an Axum rejection (malformed JSON body) must come back as the
    /// stable JSON envelope — `code` / `message` / `request_id`, with the
    /// `x-request-id` header matching the body — instead of raw extractor
    /// text.
    #[tokio::test]
    async fn malformed_json_rejection_has_stable_shape() {
        use tower::ServiceExt;
        let (state, _dir) = test_state().await;
        let app = router().with_state(state.clone());
        let response = app
            .oneshot(
                axum::extract::Request::builder()
                    .method("POST")
                    .uri("/api/admin/v1/providers")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from("not json"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let request_id = response
            .headers()
            .get("x-request-id")
            .and_then(|value| value.to_str().ok())
            .expect("x-request-id header")
            .to_owned();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            value.get("code").and_then(Value::as_str),
            Some("bad_request")
        );
        assert_eq!(
            value.get("message").and_then(Value::as_str),
            Some("Bad Request"),
            "canonical HTTP reason, never extractor internals"
        );
        assert_eq!(
            value.get("request_id").and_then(Value::as_str),
            Some(request_id.as_str()),
            "body request_id must match the response header"
        );
    }

    /// P2-3: the same stable envelope applies to query-string rejections.
    #[tokio::test]
    async fn query_rejection_normalized() {
        use tower::ServiceExt;
        let (state, _dir) = test_state().await;
        let app = router().with_state(state.clone());
        let response = app
            .oneshot(
                axum::extract::Request::builder()
                    .method("GET")
                    .uri("/api/admin/v1/requests?page=abc")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            value.get("code").and_then(Value::as_str),
            Some("bad_request")
        );
        assert_eq!(
            value.get("message").and_then(Value::as_str),
            Some("Bad Request")
        );
        assert!(value.get("request_id").and_then(Value::as_str).is_some());
    }

    /// P2-4: profile capability values are validated on write — a zero
    /// context window and a non-object thinking map are rejected with 422
    /// (pre-fix: `update_profile` accepted anything).
    #[tokio::test]
    async fn profile_validation_rejects_invalid_values() {
        let (state, _dir) = test_state().await;
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("INSERT INTO capability_profiles(id,name,description,context_window,max_tokens,supports_image_input,reasoning,thinking_level_map,created_at,updated_at) VALUES('profile-1','P',NULL,1000,2000,0,0,NULL,?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        let bad_window = update_profile(
            AdminAuth,
            State(state.clone()),
            Path("profile-1".to_owned()),
            Json(ProfileInput {
                name: "P".into(),
                description: None,
                context_window: Some(0),
                max_tokens: None,
                supports_image_input: None,
                reasoning: None,
                thinking_level_map: None,
            }),
        )
        .await
        .expect_err("context_window < 1 must be rejected");
        assert_eq!(bad_window.status, StatusCode::UNPROCESSABLE_ENTITY);
        let bad_map = update_profile(
            AdminAuth,
            State(state.clone()),
            Path("profile-1".to_owned()),
            Json(ProfileInput {
                name: "P".into(),
                description: None,
                context_window: None,
                max_tokens: None,
                supports_image_input: None,
                reasoning: None,
                thinking_level_map: Some(json!("not-an-object")),
            }),
        )
        .await
        .expect_err("a non-object thinking map must be rejected");
        assert_eq!(bad_map.status, StatusCode::UNPROCESSABLE_ENTITY);
        // Neither rejection may have mutated the stored profile.
        let window: Option<i64> = sqlx::query_scalar(
            "SELECT context_window FROM capability_profiles WHERE id='profile-1'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(window, Some(1000));
    }

    /// P2-4: a failing profile delete rolls the whole transaction back — the
    /// model_caps profile_id reference must survive the injected failure.
    #[tokio::test]
    async fn delete_profile_rolls_back_on_failure() {
        let (state, _dir) = test_state().await;
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("INSERT INTO capability_profiles(id,name,description,context_window,max_tokens,supports_image_input,reasoning,thinking_level_map,created_at,updated_at) VALUES('profile-1','P',NULL,1000,2000,0,0,NULL,?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO model_caps(requested_model_id,source,profile_id,created_at,updated_at) VALUES('test-model','manual','profile-1',?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("CREATE TRIGGER fail_del BEFORE DELETE ON capability_profiles BEGIN SELECT RAISE(ABORT, 'injected'); END")
            .execute(state.db.pool())
            .await
            .unwrap();
        let result = delete_profile(
            AdminAuth,
            State(state.clone()),
            Path("profile-1".to_owned()),
        )
        .await;
        let error = result.expect_err("the injected trigger must fail the delete");
        assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
        let profile_id: Option<String> = sqlx::query_scalar(
            "SELECT profile_id FROM model_caps WHERE requested_model_id='test-model'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(
            profile_id.as_deref(),
            Some("profile-1"),
            "the UPDATE must roll back together with the failed DELETE"
        );
        sqlx::query("DROP TRIGGER fail_del")
            .execute(state.db.pool())
            .await
            .unwrap();
    }

    /// The health-check model is advisory: a provider whose protocols are
    /// messy (e.g. one catalog serves some protocols and not others) must
    /// still be able to pin a health model that covers only a subset. The
    /// runtime probe falls back per protocol (health.rs), so saving with
    /// partial or no coverage is accepted.
    #[tokio::test]
    async fn channel_save_accepts_partial_health_model_coverage() {
        let (state, _dir) = test_state().await;
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("INSERT INTO providers(id,name,base_url,created_at,updated_at) VALUES('prov-1','mock','http://127.0.0.1:1',?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        // Two protocols, no channel model covers either of them — the save
        // must not be rejected.
        let response = create_channel(
            AdminAuth,
            State(state.clone()),
            Json(ChannelInput {
                provider_id: "prov-1".into(),
                name: "chan".into(),
                protocol: None,
                protocols: vec!["openai_compatible".into(), "claude".into()],
                api_key: "k".into(),
                manual_enabled: true,
                health_check_model_id: Some("A".into()),
            }),
        )
        .await;
        let response = response.expect("partial coverage must be accepted");
        assert_eq!(response.status(), StatusCode::CREATED);
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM channels")
            .fetch_one(state.db.pool())
            .await
            .unwrap();
        assert_eq!(count, 1, "the channel is created with its health model");
        let stored: Option<String> = sqlx::query_scalar("SELECT health_check_model_id FROM channels")
            .fetch_one(state.db.pool())
            .await
            .unwrap();
        assert_eq!(stored.as_deref(), Some("A"));
    }

    /// P1-6: `health_check_model_id: null` clears the stored model back to
    /// automatic — a PATCH that omits the field leaves it untouched, and a
    /// value not owned by the channel is rejected.
    #[tokio::test]
    async fn health_check_model_can_be_cleared_with_null() {
        let (state, _dir) = test_state().await;
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("INSERT INTO providers(id,name,base_url,created_at,updated_at) VALUES('prov-1','mock','http://127.0.0.1:1',?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channels(id,provider_id,name,protocol,api_key_encrypted,api_key_hint,manual_enabled,health_check_model_id,created_at,updated_at) VALUES('ch-1','prov-1','chan','openai_compatible',X'00','',1,'A',?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_models(id,channel_id,model_id,source,available,first_seen_at,created_at,updated_at) VALUES('cm-1','ch-1','A','discovered',1,?,?,?)")
            .bind(time)
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        // 1. PATCH with an explicit null clears the value.
        let cleared = patch_channel(
            AdminAuth,
            State(state.clone()),
            Path("ch-1".to_owned()),
            Json(
                serde_json::from_value::<ChannelPatch>(serde_json::json!({
                    "health_check_model_id": null
                }))
                .unwrap(),
            ),
        )
        .await
        .expect("explicit null must be accepted");
        assert_eq!(cleared.status(), StatusCode::OK);
        let stored: Option<String> =
            sqlx::query_scalar("SELECT health_check_model_id FROM channels WHERE id='ch-1'")
                .fetch_one(state.db.pool())
                .await
                .unwrap();
        assert_eq!(stored, None, "explicit null must clear the model");
        // 2. A PATCH omitting the field leaves the (cleared) value alone.
        let _ = patch_channel(
            AdminAuth,
            State(state.clone()),
            Path("ch-1".to_owned()),
            Json(
                serde_json::from_value::<ChannelPatch>(serde_json::json!({"name": "chan2"}))
                    .unwrap(),
            ),
        )
        .await
        .expect("omitted field must be accepted");
        let stored: Option<String> =
            sqlx::query_scalar("SELECT health_check_model_id FROM channels WHERE id='ch-1'")
                .fetch_one(state.db.pool())
                .await
                .unwrap();
        assert_eq!(stored, None, "omitted field must not change the value");
        // 3. A model that does not belong to the channel is rejected.
        let error = patch_channel(
            AdminAuth,
            State(state.clone()),
            Path("ch-1".to_owned()),
            Json(
                serde_json::from_value::<ChannelPatch>(serde_json::json!({
                    "health_check_model_id": "foreign-model"
                }))
                .unwrap(),
            ),
        )
        .await
        .expect_err("a foreign model must be rejected");
        assert_eq!(error.status, StatusCode::UNPROCESSABLE_ENTITY);
        // 4. Setting an owned model works.
        let _ = patch_channel(
            AdminAuth,
            State(state.clone()),
            Path("ch-1".to_owned()),
            Json(
                serde_json::from_value::<ChannelPatch>(serde_json::json!({
                    "health_check_model_id": "A"
                }))
                .unwrap(),
            ),
        )
        .await
        .expect("an owned model must be accepted");
        let stored: Option<String> =
            sqlx::query_scalar("SELECT health_check_model_id FROM channels WHERE id='ch-1'")
                .fetch_one(state.db.pool())
                .await
                .unwrap();
        assert_eq!(stored.as_deref(), Some("A"));
    }

    /// P2-3: the channel form's API key commits in the SAME transaction as
    /// the other fields — a rejected key rolls the whole patch back.
    #[tokio::test]
    async fn channel_patch_with_api_key_is_atomic() {
        let (state, _dir) = test_state().await;
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("INSERT INTO providers(id,name,base_url,created_at,updated_at) VALUES('prov-1','mock','http://127.0.0.1:1',?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channels(id,provider_id,name,protocol,api_key_encrypted,api_key_hint,manual_enabled,created_at,updated_at) VALUES('ch-1','prov-1','chan','openai_compatible',X'00','old-hint',1,?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        // 1. name + api_key in one PATCH: both commit together.
        let response = patch_channel(
            AdminAuth,
            State(state.clone()),
            Path("ch-1".to_owned()),
            Json(
                serde_json::from_value::<ChannelPatch>(serde_json::json!({
                    "name": "renamed",
                    "api_key": "new-key-123",
                }))
                .unwrap(),
            ),
        )
        .await
        .expect("atomic patch must succeed");
        assert_eq!(response.status(), StatusCode::OK);
        let (name, hint): (String, String) = sqlx::query_as(
            "SELECT name, api_key_hint FROM channels WHERE id='ch-1'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(name, "renamed");
        assert_ne!(hint, "old-hint", "the key must rotate with the patch");
        let encrypted: Vec<u8> =
            sqlx::query_scalar("SELECT api_key_encrypted FROM channels WHERE id='ch-1'")
                .fetch_one(state.db.pool())
                .await
                .unwrap();
        assert_eq!(
            state.secrets.decrypt(&encrypted).unwrap(),
            "new-key-123",
            "the stored key must decrypt to the submitted one"
        );
        // 2. A rejected key (empty) rolls the WHOLE patch back.
        let error = patch_channel(
            AdminAuth,
            State(state.clone()),
            Path("ch-1".to_owned()),
            Json(
                serde_json::from_value::<ChannelPatch>(serde_json::json!({
                    "name": "half-updated",
                    "api_key": "   ",
                }))
                .unwrap(),
            ),
        )
        .await
        .expect_err("an empty api_key must be rejected");
        assert_eq!(error.status, StatusCode::UNPROCESSABLE_ENTITY);
        let name: String = sqlx::query_scalar("SELECT name FROM channels WHERE id='ch-1'")
            .fetch_one(state.db.pool())
            .await
            .unwrap();
        assert_eq!(
            name, "renamed",
            "the rejected key must roll back the name change too"
        );
    }

    /// P1-5: preset refresh queues REAL discovery runs for eligible enabled
    /// channels and returns their IDs; with no eligible channel it fails
    /// clearly instead of faking "queued".
    #[tokio::test]
    async fn preset_refresh_queues_real_discovery_runs() {
        let (state, _dir) = test_state().await;
        // No channels yet: a clear conflict, never a fake 202.
        let error = refresh_claude_presets(AdminAuth, State(state.clone()))
            .await
            .expect_err("no eligible channel must fail clearly");
        assert_eq!(error.status, StatusCode::CONFLICT);

        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("INSERT INTO providers(id,name,base_url,created_at,updated_at) VALUES('prov-1','mock','http://127.0.0.1:1',?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channels(id,provider_id,name,protocol,api_key_encrypted,api_key_hint,manual_enabled,created_at,updated_at) VALUES('ch-1','prov-1','chan','claude',X'00','',1,?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_protocols(channel_id,protocol) VALUES('ch-1','claude')")
            .execute(state.db.pool())
            .await
            .unwrap();
        let response = refresh_claude_presets(AdminAuth, State(state.clone()))
            .await
            .expect("refresh must succeed with an eligible channel");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        let run_ids = value["run_ids"].as_array().expect("run_ids array");
        assert_eq!(run_ids.len(), 1, "exactly one channel was queued");
        let runs: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM discovery_runs WHERE channel_id='ch-1'")
                .fetch_one(state.db.pool())
                .await
                .unwrap();
        assert_eq!(runs, 1, "a real discovery run row must exist");
        // Codex refresh must NOT queue the claude-only channel.
        let error = refresh_codex_presets(AdminAuth, State(state.clone()))
            .await
            .expect_err("a claude-only channel cannot feed Codex presets");
        assert_eq!(error.status, StatusCode::CONFLICT);
        // The queued discovery task eventually fails against port 1; wait
        // for it to finish so it cannot outlive the test supervisor.
        let run_id = run_ids[0].as_str().unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let finished: Option<String> = sqlx::query_scalar(
                "SELECT finished_at FROM discovery_runs WHERE id=?",
            )
            .bind(run_id)
            .fetch_one(state.db.pool())
            .await
            .unwrap();
            if finished.is_some() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the queued run must reach a terminal state"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    /// P1-5: GET presets aggregates live channel models with the built-in
    /// defaults, deduplicates, and keeps protocol families apart.
    #[tokio::test]
    async fn presets_aggregate_live_channel_models_with_defaults() {
        let (state, _dir) = test_state().await;
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("INSERT INTO providers(id,name,base_url,created_at,updated_at) VALUES('prov-1','mock','http://127.0.0.1:1',?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channels(id,provider_id,name,protocol,api_key_encrypted,api_key_hint,manual_enabled,created_at,updated_at) VALUES('ch-1','prov-1','chan','claude',X'00','',1,?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_models(id,channel_id,model_id,display_name,source,available,first_seen_at,created_at,updated_at) VALUES('cm-1','ch-1','channel-model-x','Channel X',1,1,?,?,?)")
            .bind(time)
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_models(id,channel_id,model_id,display_name,source,available,first_seen_at,created_at,updated_at) VALUES('cm-2','ch-1','codex-only-model','Codex Only',1,1,?,?,?)")
            .bind(time)
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        // The same model twice through two rows: dedup must yield one entry.
        sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES('cm-1','claude')")
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES('cm-1','openai_responses')")
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES('cm-2','openai_compatible')")
            .execute(state.db.pool())
            .await
            .unwrap();
        // An unavailable model is not a preset.
        sqlx::query("INSERT INTO channel_models(id,channel_id,model_id,display_name,source,available,first_seen_at,created_at,updated_at) VALUES('cm-3','ch-1','unavailable-model',NULL,1,0,?,?,?)")
            .bind(time)
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES('cm-3','claude')")
            .execute(state.db.pool())
            .await
            .unwrap();

        let claude = claude_presets(AdminAuth, State(state.clone()))
            .await
            .expect("claude presets must load");
        let body = axum::body::to_bytes(claude.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        let ids: Vec<String> = value["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|item| item["id"].as_str().map(str::to_owned))
            .collect();
        assert!(ids.contains(&"claude-opus-5".into()), "defaults survive");
        assert!(
            ids.contains(&"channel-model-x".into()),
            "live channel models are aggregated"
        );
        assert!(
            !ids.contains(&"codex-only-model".into()),
            "codex-only models must not leak into claude presets"
        );
        assert!(
            !ids.contains(&"unavailable-model".into()),
            "unavailable models are not presets"
        );
        let count = ids
            .iter()
            .filter(|id| id.as_str() == "channel-model-x")
            .count();
        assert_eq!(count, 1, "duplicate protocols must not duplicate presets");

        let codex = codex_presets(AdminAuth, State(state.clone()))
            .await
            .expect("codex presets must load");
        let body = axum::body::to_bytes(codex.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        let ids: Vec<String> = value["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|item| item["id"].as_str().map(str::to_owned))
            .collect();
        assert!(ids.contains(&"codex-only-model".into()));
        assert!(ids.contains(&"channel-model-x".into()), "responses-capable models feed codex presets");
        assert!(!ids.contains(&"claude-opus-5".into()), "claude defaults do not leak into codex presets");
    }
}
