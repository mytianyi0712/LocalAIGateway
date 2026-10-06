//! 管理端 HTTP 层：装配 `/api/admin/v1/*` 路由，并定义 `AdminService`、
//! 共享响应助手（`ok`/`no_content`/`validate_text`/`integrity`）与 `ApiResult` 别名。
//! 边界：13 个 admin 子模块的 SQL 都收口到 `AdminService`（多数在各自同名
//! 子模块内，provider 域的在 `admin/mod.rs`），handler 只做参数提取与透传。
//! 关键不变量：多步写路径包在单个事务内，校验失败即回滚，不留半更新状态。
use crate::{
    api_error::{ApiError, correlation_middleware, json_response},
    crypto::SecretStore,
    db::Database,
    protocol::normalize_base_url,
    state::AppState,
};
use axum::{
    Json, Router,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, patch, post, put},
};
use chrono::Utc;
use providers::{
    ProviderInput, ProviderPatch, ProviderRow, load_provider, provider_json, validate_provider_kind,
};
use serde_json::{Value, json};
use std::sync::Arc;
use uuid::Uuid;
mod balances;
mod channels;
mod commandcode;
mod discovery;
mod logs;
mod models;
mod presets;
mod profiles;
mod providers;
mod routes;
mod settings;
mod stats;
use balances::{
    delete_balance_config, get_balance, put_balance_config, query_balance, refresh_balances,
};
use channels::{
    create_channel, delete_channel, get_channel, list_channels, patch_channel, replace_api_key,
    reset_health,
};
use commandcode::{
    command_code_login_begin, command_code_login_cancel, command_code_login_import_cli,
    command_code_login_status,
};
use discovery::{discover_models, get_discovery_run, list_discovery_runs, manual_probe};
use logs::{clear_logs, get_request, list_health_probes, list_requests};
use models::{create_manual_model, delete_channel_model, list_channel_models, patch_channel_model};
use presets::list_provider_presets;
use profiles::{
    create_profile, delete_profile, detect_capabilities, get_capabilities, get_profile,
    list_profiles, put_capabilities, update_profile,
};
use providers::{create_provider, delete_provider, get_provider, list_providers, patch_provider};
use routes::{create_route, delete_route, list_routes, patch_route, replace_candidates};
use settings::{
    command_code_status, generate_access_keys, get_settings, patch_settings, recovery_key_status,
    recovery_repair_settings, system_protocols, system_status,
};
use stats::{stats_cache, stats_channels, stats_models, stats_summary, stats_timeseries};

type ApiResult = Result<Response, ApiError>;
pub fn router() -> Router<AppState> {
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
            "/api/admin/v1/provider-presets",
            get(list_provider_presets),
        )
        .route(
            "/api/admin/v1/command-code/status",
            get(command_code_status),
        )
        .route(
            "/api/admin/v1/command-code/login",
            get(command_code_login_status)
                .post(command_code_login_begin)
                .delete(command_code_login_cancel),
        )
        .route(
            "/api/admin/v1/command-code/import-cli",
            post(command_code_login_import_cli),
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
            "/api/admin/v1/channels/{id}/balance",
            get(get_balance).post(query_balance),
        )
        .route(
            "/api/admin/v1/channels/{id}/balance-config",
            put(put_balance_config).delete(delete_balance_config),
        )
        .route(
            "/api/admin/v1/balances/refresh",
            post(refresh_balances),
        )
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
/// 管理端服务：所有 admin 子域的 SQL 与响应组装的归属地。
/// handler 只做参数提取与透传；本服务持有数据库、密钥库与渠道端口
/// （不持有 HTTP 客户端）。
/// 需要能力检测 / discovery 等其它域服务的方法，按仓库既有约定临时接收
/// `&Context`（仅作参数，不保存）。
pub struct AdminService {
    db: Database,
    secrets: SecretStore,
    /// 渠道端口：存在性、`providers.kind` 与协议清单查询的唯一来源
    /// （渠道行的 SQL 归 `infrastructure`，admin 侧不再重复）。
    channels: Arc<dyn crate::ports::ChannelRepository>,
}
impl AdminService {

    pub async fn list_providers(&self) -> ApiResult {
        let rows = sqlx::query_as::<_, ProviderRow>("SELECT p.id,p.name,p.base_url,p.kind,p.created_at,p.updated_at,COUNT(c.id) channel_count FROM providers p LEFT JOIN channels c ON c.provider_id=p.id GROUP BY p.id ORDER BY p.name")
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
        let kind = validate_provider_kind(input.kind.as_deref())?;
        let row_id = id();
        let time = now();
        sqlx::query(
            "INSERT INTO providers(id,name,base_url,kind,created_at,updated_at) VALUES(?,?,?,?,?,?)",
        )
        .bind(&row_id)
        .bind(input.name.trim())
        .bind(base_url)
        .bind(kind.as_deref())
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
        // 先校验全部输入再触碰数据行，随后用单条原子 UPDATE 提交
        // name、base_url（以及有值时的 kind）—— 非法 URL 或语句失败
        // 都不会再留下半更新的名称。
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
        let kind = match &input.kind {
            Some(value) => validate_provider_kind(Some(value))?,
            None => None,
        };
        // 续上：`kind` 与 name/base_url 共用同一套全有或全无的更新，
        // 有值即写、无值即跳过。
        let kind_present = input.kind.is_some();
        if name.is_none() && base_url.is_none() && !kind_present {
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
        if kind_present {
            if comma {
                builder.push(",");
            }
            builder.push(" kind=").push_bind(kind);
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

    /// 删除本就包在单个事务内；级联顺序保持显式：
    /// 即 route_candidates → channels → provider。
    pub async fn delete_provider(&self, row_id: &str) -> ApiResult {
        let mut tx = self.db.pool().begin().await?;
        self.delete_candidates_for_provider(&mut tx, row_id).await?;
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

    pub fn new(
        db: Database,
        secrets: SecretStore,
        channels: Arc<dyn crate::ports::ChannelRepository>,
    ) -> Arc<Self> {
        Arc::new(Self {
            db,
            secrets,
            channels,
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use super::{
        channels::{ChannelFilter, ChannelInput, ChannelPatch},
        models::{ManualModelInput, ModelPatch},
        profiles::ProfileInput,
        routes::{CandidateInput, CandidateList, RouteInput},
        stats::{SummaryQuery, format_utc_millis, parse_utc_rfc3339, resolve_token_window},
    };
    use crate::{
        api_error::ErrorCode,
        application::Context,
        auth::{AdminAuth, RecoveryAuth, RecoveryGenerateAuth},
        settings as core_settings,
    };
    use axum::extract::{FromRequestParts, Path, Query, State};
    use sqlx::Row;

    use crate::test_support::TempDir;
    use std::collections::HashMap;

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
        // 半开区间的起点与同一时刻的已存值比较必须相等。
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
        // occurred_at 恰好等于 `from` 的已存值必须 >= 该边界，
        // 恰好等于 `to` 的必须 < 该边界。
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

    async fn test_state() -> (AppState, TempDir) {
        // 统一夹具：临时目录 + 整套 Context（见 `crate::test_support`）。
        let crate::test_support::TestEnv {
            dir,
            context: state,
        } = crate::test_support::context("admin").await;
        (state, dir)
    }

    /// 设置补丁在同一个事务里写入密钥与运行时设置；
    /// 两者事后都必须可读。
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

    /// 更新能力档案会在同一个事务里把改动传播到 model_caps；
    /// 事后两侧保持一致。
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

    /// provider PATCH 失败时两个字段都必须保持不变 —— name 与 base_url
    /// 在单条原子 UPDATE 中提交，因此触发器失败不会产生半更新的数据行。
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
        // 对任何 providers UPDATE 注入失败：历史上实现为非原子的两条语句时，
        // name 会先被写入；单条 UPDATE 必须整体失败。
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
                kind: None,
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

    /// `providers.kind`（migration 0005）显式且经过校验：只接受
    /// `command_code`，未知值在写入前就被拒绝，空字符串则清除标记。
    #[tokio::test]
    async fn provider_kind_round_trips_and_rejects_unknown_values() {
        let (state, _dir) = test_state().await;
        let response = create_provider(
            AdminAuth,
            State(state.clone()),
            Json(ProviderInput {
                name: "cc".into(),
                base_url: "https://api.commandcode.ai".into(),
                kind: Some("command_code".into()),
            }),
        )
        .await
        .expect("create must succeed");
        assert_eq!(response.status(), StatusCode::CREATED);
        let (id, kind): (String, Option<String>) =
            sqlx::query_as("SELECT id, kind FROM providers WHERE name='cc'")
                .fetch_one(state.db.pool())
                .await
                .unwrap();
        assert_eq!(kind.as_deref(), Some("command_code"));

        let error = create_provider(
            AdminAuth,
            State(state.clone()),
            Json(ProviderInput {
                name: "bad".into(),
                base_url: "https://example.com".into(),
                kind: Some("bogus".into()),
            }),
        )
        .await
        .expect_err("unknown kind must be rejected");
        assert_eq!(error.status, StatusCode::UNPROCESSABLE_ENTITY);
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM providers WHERE name='bad'")
            .fetch_one(state.db.pool())
            .await
            .unwrap();
        assert_eq!(count, 0, "rejected input must not be persisted");

        let response = patch_provider(
            AdminAuth,
            State(state.clone()),
            Path(id.clone()),
            Json(ProviderPatch {
                name: None,
                base_url: None,
                kind: Some(String::new()),
            }),
        )
        .await
        .expect("clearing kind must succeed");
        assert_eq!(response.status(), StatusCode::OK);
        let kind: Option<String> = sqlx::query_scalar("SELECT kind FROM providers WHERE id=?")
            .bind(&id)
            .fetch_one(state.db.pool())
            .await
            .unwrap();
        assert!(kind.is_none());
    }

    /// provider PATCH 成功时用单条语句写入 name + base_url，
    /// 因此数据行只带一个一致的 updated_at。
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
                kind: None,
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
        // 非法 base_url 在任何写入之前就被拒绝（校验顺序）。
        let error = patch_provider(
            AdminAuth,
            State(state.clone()),
            Path("prov-1".to_owned()),
            Json(ProviderPatch {
                name: Some("renamed-again".into()),
                base_url: Some("not a url".into()),
                kind: None,
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

    /// 余额管理流程：PUT config -> POST query -> GET -> POST refresh
    /// -> DELETE。上游被刻意设为不可达：查询仍须返回 200 加错误快照，
    /// 绝不破坏管理端 API。
    #[tokio::test]
    async fn balance_admin_flow_round_trips_and_survives_upstream_failure() {
        let (state, _dir) = test_state().await;
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("INSERT INTO providers(id,name,base_url,created_at,updated_at) VALUES('prov-1','mock','http://127.0.0.1:1',?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channels(id,provider_id,name,protocol,api_key_encrypted,api_key_hint,manual_enabled,created_at,updated_at) VALUES('ch-1','prov-1','chan','openai_compatible',?,?,1,?,?)")
            .bind(state.secrets.encrypt("sk-test"))
            .bind("sk-...test")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();

        let saved = put_balance_config(
            AdminAuth,
            State(state.clone()),
            Path("ch-1".to_owned()),
            Json(crate::balance::BalanceConfigInput {
                adapter: "newapi".into(),
                enabled: true,
                method: None,
                path: None,
                auth: None,
                headers: std::collections::BTreeMap::new(),
                body: None,
                mapping: crate::balance::BalanceMappingInput::default(),
                token: None,
                clear_token: false,
            }),
        )
        .await
        .expect("save must succeed");
        assert_eq!(saved.status(), StatusCode::OK);

        let queried = query_balance(AdminAuth, State(state.clone()), Path("ch-1".to_owned()))
            .await
            .expect("upstream failure must still return 200");
        assert_eq!(queried.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(queried.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let snapshot: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(snapshot["status"], "error");
        assert_eq!(snapshot["error_kind"], "transport_error");

        let fetched = get_balance(AdminAuth, State(state.clone()), Path("ch-1".to_owned()))
            .await
            .expect("get must succeed");
        let bytes = axum::body::to_bytes(fetched.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let config: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(config["configured"], true);
        assert_eq!(config["adapter"], "newapi");
        assert_eq!(config["snapshot"]["status"], "error");

        let refreshed = refresh_balances(AdminAuth, State(state.clone()))
            .await
            .expect("refresh must succeed");
        let bytes = axum::body::to_bytes(refreshed.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let result: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(result["total"], 1);
        assert_eq!(result["failed"], 1);

        let deleted =
            delete_balance_config(AdminAuth, State(state.clone()), Path("ch-1".to_owned()))
                .await
                .expect("delete must succeed");
        assert_eq!(deleted.status(), StatusCode::NO_CONTENT);

        // 恢复默认关闭；此时手动查询得到稳定的 422。
        let error = query_balance(AdminAuth, State(state.clone()), Path("ch-1".to_owned()))
            .await
            .expect_err("unconfigured manual query must fail");
        assert_eq!(error.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(error.code, ErrorCode::BalanceNotConfigured);
    }

    /// 路由接线冒烟测试：余额端点必须能经由真正的 admin 路由访问
    /// （extractor + correlation 中间件）。
    #[tokio::test]
    async fn balance_routes_are_wired_through_the_admin_router() {
        use tower::ServiceExt;
        let (state, _dir) = test_state().await;
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("INSERT INTO providers(id,name,base_url,created_at,updated_at) VALUES('prov-1','mock','http://127.0.0.1:1',?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channels(id,provider_id,name,protocol,api_key_encrypted,api_key_hint,manual_enabled,created_at,updated_at) VALUES('ch-1','prov-1','chan','openai_compatible',?,?,1,?,?)")
            .bind(state.secrets.encrypt("sk-test"))
            .bind("sk-...test")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();

        let app = router().with_state(state.clone());
        let put = axum::http::Request::builder()
            .method("PUT")
            .uri("/api/admin/v1/channels/ch-1/balance-config")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(
                json!({"adapter": "newapi", "enabled": true}).to_string(),
            ))
            .unwrap();
        let response = app.clone().oneshot(put).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let config: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(config["configured"], true);
        assert_eq!(config["adapter"], "newapi");
        assert_eq!(config["enabled"], true);

        let get = axum::http::Request::builder()
            .method("GET")
            .uri("/api/admin/v1/channels/ch-1/balance")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(get).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let refresh = axum::http::Request::builder()
            .method("POST")
            .uri("/api/admin/v1/balances/refresh")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(refresh).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let result: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(result["total"], 1);
        assert_eq!(result["failed"], 1, "unreachable upstream -> error snapshot");

        let delete = axum::http::Request::builder()
            .method("DELETE")
            .uri("/api/admin/v1/channels/ch-1/balance-config")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(delete).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let missing = axum::http::Request::builder()
            .method("GET")
            .uri("/api/admin/v1/channels/missing/balance")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(missing).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    /// 把档案更新传播到 model_caps 的过程中失败时，整个更新都会回滚 ——
    /// 传播失败时档案行不得改变。
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
        // 让第二条语句（model_caps 传播）失败：历史上传播不在同一事务内时，
        // 档案行会先被提交并残留改动。
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

    /// 保存的候选中渠道模型不支持该路由的任一协议时，返回 422 且不做任何改动。
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
        // 该渠道模型只支持 claude；而路由是 openai。
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
        // 预先放一个有效候选（支持 openai），以确保拒绝时既有行不被触碰。
        // 它位于第二个渠道：channel_models 按 (channel_id, model_id) 唯一。
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

    /// 候选按 (route, protocol) 对插入 —— claude 候选只落在 claude 兄弟路由下，
    /// openai 候选只落在 openai 兄弟路由下。
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

    /// 自定义模型：任意路由名都可用显式协议创建，其候选可以携带不同于路由的
    /// 上游模型 id（网关按候选重写出站的 `model`）。
    #[tokio::test]
    async fn create_custom_route_accepts_differing_model_candidates() {
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
        sqlx::query("INSERT INTO channel_models(id,channel_id,model_id,display_name,source,available,first_seen_at,created_at,updated_at) VALUES('cm-1','ch-1','gpt-4o','GPT-4o','discovered',1,?,?,?)")
            .bind(time)
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES('cm-1','openai_compatible')")
            .execute(state.db.pool())
            .await
            .unwrap();

        let created = create_route(
            AdminAuth,
            State(state.clone()),
            Json(RouteInput {
                protocol: None,
                protocols: Some(vec!["openai_compatible".into()]),
                requested_model_id: "my-gpt".into(),
                enabled: true,
            }),
        )
        .await
        .expect("a custom model id with explicit protocols must be creatable");
        assert_eq!(created.status(), StatusCode::CREATED);
        let route_id: String =
            sqlx::query_scalar("SELECT id FROM model_routes WHERE requested_model_id='my-gpt'")
                .fetch_one(state.db.pool())
                .await
                .unwrap();

        let response = replace_candidates(
            AdminAuth,
            State(state.clone()),
            Path(route_id),
            Json(CandidateList {
                candidates: vec![CandidateInput {
                    channel_model_id: "cm-1".into(),
                    priority: 0,
                    enabled: true,
                }],
            }),
        )
        .await
        .expect("a candidate with a different upstream model id must be accepted");
        assert_eq!(response.status(), StatusCode::OK);
        let bound: String = sqlx::query_scalar(
            "SELECT cm.model_id FROM route_candidates rc JOIN channel_models cm ON cm.id=rc.channel_model_id LIMIT 1",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(bound, "gpt-4o");
    }

    /// 自定义路由可以不带任何候选创建；只有在省略协议时，
    /// 没有匹配渠道模型的路由才会被拒绝。
    #[tokio::test]
    async fn create_route_without_protocols_still_requires_a_channel_model() {
        let (state, _dir) = test_state().await;
        let result = create_route(
            AdminAuth,
            State(state.clone()),
            Json(RouteInput {
                protocol: None,
                protocols: None,
                requested_model_id: "ghost-model".into(),
                enabled: true,
            }),
        )
        .await;
        let error = result.expect_err("deriving protocols must fail without a channel model");
        assert_eq!(error.status, StatusCode::UNPROCESSABLE_ENTITY);
    }

    /// admin 密钥损坏时，恢复路径（密钥健康 -> AdminAuth，损坏 -> 仅回环）
    /// 仍须原子地重新生成两把访问密钥；且缺少 `ConnectInfo` 扩展的
    /// `RecoveryAuth` 提取必须被拒绝。
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
        // 密钥损坏期间，缺少 ConnectInfo 扩展（没有真实 HTTP 对端）的提取
        // 必须被拒绝。
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
        // 没有一次性 nonce 时，generate 端点对外关闭。
        let rejected = generate_access_keys(
            RecoveryGenerateAuth,
            State(state.clone()),
            axum::http::HeaderMap::new(),
        )
        .await
        .expect_err("missing nonce must be rejected");
        assert_eq!(rejected.status, StatusCode::UNAUTHORIZED);
        // 有效 nonce（由 recovery status 端点签发）允许重新生成一次。
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
        // 刚写入的密钥必须能再次解密。
        let policy = core_settings::access_policy(&state)
            .await
            .expect("keys must be valid again");
        assert_eq!(policy.admin_key, admin);
        assert_eq!(policy.gateway_key, gateway);
        // nonce 单次有效：重放必须被拒绝。
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

    /// 运行时设置损坏时故障关闭（管理界面以 `config_corrupted` 锁定），
    /// 但仍可通过回环 + nonce 的 recovery-repair 端点修复；
    /// 损坏的行被覆盖写，读取随之恢复。
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
        // 1. admin 鉴权以 config_corrupted 信号故障关闭。
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
        // 2. recovery status 端点报告损坏的键。
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
        // 3. 带 nonce 修复：有效的行覆盖损坏的行。
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
        // 4. admin 鉴权恢复可用。
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
        // 5. nonce 单次有效。
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
        // 6. 修复端点绝不接受访问密钥。
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

    /// generate 端点的 extractor 拒绝跨站来源、缺失来源、
    /// DNS 重绑定主机以及非回环对端。
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
        // 合法的同源调用通过。
        let mut parts = build_parts("127.0.0.1:3000", Some("http://127.0.0.1:3000"), loopback);
        assert!(
            RecoveryGenerateAuth::from_request_parts(&mut parts, &state)
                .await
                .is_ok(),
            "same-origin loopback must pass"
        );
        // 跨站形式：回环对端，但来源是攻击者的。
        let mut parts = build_parts("127.0.0.1:3000", Some("http://evil.example"), loopback);
        assert!(matches!(
            RecoveryGenerateAuth::from_request_parts(&mut parts, &state).await,
            Err(error) if error.status == StatusCode::UNAUTHORIZED
        ));
        // 完全没有 Origin 头（例如原始 socket 形式）。
        let mut parts = build_parts("127.0.0.1:3000", None, loopback);
        assert!(matches!(
            RecoveryGenerateAuth::from_request_parts(&mut parts, &state).await,
            Err(error) if error.status == StatusCode::UNAUTHORIZED
        ));
        // DNS 重绑定：回环对端，Host 由攻击者控制。
        let mut parts = build_parts("evil.example:3000", Some("http://evil.example"), loopback);
        assert!(matches!(
            RecoveryGenerateAuth::from_request_parts(&mut parts, &state).await,
            Err(error) if error.status == StatusCode::UNAUTHORIZED
        ));
        // 非回环对端。
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

    /// 过期的恢复 nonce 会被拒绝 —— 即使从未使用过，
    /// 该挑战也是短时效的。
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

    /// Axum 的拒绝（JSON 体格式错误）必须以稳定的 JSON 信封返回 ——
    /// `code` / `message` / `request_id`，且 `x-request-id` 头与响应体一致 ——
    /// 而不是原始的 extractor 文本。
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

    /// 同样的稳定信封适用于查询串拒绝。
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

    /// 档案能力值在写入时校验 —— 零 context window 和非对象 thinking map
    /// 都返回 422（历史上 `update_profile` 不做校验，现已拒绝）。
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
        // 两次拒绝都不得改动已存档案。
        let window: Option<i64> = sqlx::query_scalar(
            "SELECT context_window FROM capability_profiles WHERE id='profile-1'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(window, Some(1000));
    }

    /// 档案删除失败时整个事务回滚 —— model_caps 的 profile_id 引用
    /// 必须在注入的失败后仍然存在。
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

    /// 建渠道必须落下一行 `active` 初始健康行（否则该渠道永远无法被路由选中，
    /// 熔断/恢复状态机也无行可改）；重复初始化必须幂等且不改状态。
    #[tokio::test]
    async fn channel_creation_seeds_active_health_row() {
        let (state, _dir) = test_state().await;
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("INSERT INTO providers(id,name,base_url,created_at,updated_at) VALUES('prov-1','mock','http://127.0.0.1:1',?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        let response = create_channel(
            AdminAuth,
            State(state.clone()),
            Json(ChannelInput {
                provider_id: "prov-1".into(),
                name: "chan".into(),
                protocol: None,
                protocols: vec!["openai_compatible".into()],
                api_key: "k".into(),
                login_id: None,
                manual_enabled: true,
                health_check_model_id: None,
            }),
        )
        .await
        .expect("channel creation must succeed");
        assert_eq!(response.status(), StatusCode::CREATED);
        let channel_id: String = sqlx::query_scalar("SELECT id FROM channels LIMIT 1")
            .fetch_one(state.db.pool())
            .await
            .unwrap();
        let (health_state, failures): (String, i64) = sqlx::query_as(
            "SELECT state, consecutive_failures FROM channel_health WHERE channel_id=?",
        )
        .bind(&channel_id)
        .fetch_one(state.db.pool())
        .await
        .expect("creation must seed a channel_health row");
        assert_eq!(health_state, "active");
        assert_eq!(failures, 0);

        // 幂等：已开断的行不会被再次初始化覆盖。
        sqlx::query("UPDATE channel_health SET state='open' WHERE channel_id=?")
            .bind(&channel_id)
            .execute(state.db.pool())
            .await
            .unwrap();
        crate::health::apply_health_event(
            state.db.pool(),
            crate::health::HealthEvent::Initialize {
                channel_id: &channel_id,
                at: time,
            },
        )
        .await
        .expect("re-initialization must not fail");
        let after: String = sqlx::query_scalar("SELECT state FROM channel_health WHERE channel_id=?")
            .bind(&channel_id)
            .fetch_one(state.db.pool())
            .await
            .unwrap();
        assert_eq!(after, "open", "initialize must not clobber an existing row");
    }

    /// 健康检查模型是建议性的：协议混杂的 provider（例如同一目录只覆盖
    /// 部分协议）仍应能指定一个只覆盖子集的健康模型。运行时探针按协议
    /// 回退（health.rs），因此部分覆盖甚至不覆盖都允许保存。
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
        // 两个协议，没有任何渠道模型覆盖其中之一 —— 保存不得被拒绝。
        let response = create_channel(
            AdminAuth,
            State(state.clone()),
            Json(ChannelInput {
                provider_id: "prov-1".into(),
                name: "chan".into(),
                protocol: None,
                protocols: vec!["openai_compatible".into(), "claude".into()],
                api_key: "k".into(),
                login_id: None,
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

    /// providers 页面按 provider 分组渲染账号；因此渠道列表必须按
    /// provider 名（再按渠道名）排序，绝不只按渠道名。此处渠道名的排序
    /// 与所属 provider 的排序相反，所以按渠道名排序的断言会失败。
    #[tokio::test]
    async fn channels_list_is_ordered_by_provider_name() {
        let (state, _dir) = test_state().await;
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("INSERT INTO providers(id,name,base_url,created_at,updated_at) VALUES('prov-z','zebra-provider','http://127.0.0.1:1',?,?),('prov-a','alpha-provider','http://127.0.0.1:2',?,?)")
            .bind(time)
            .bind(time)
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channels(id,provider_id,name,protocol,api_key_encrypted,api_key_hint,manual_enabled,health_check_model_id,created_at,updated_at) VALUES('ch-1','prov-z','aaa-channel','openai_compatible',X'00','',1,NULL,?,?),('ch-2','prov-a','zzz-channel','openai_compatible',X'00','',1,NULL,?,?)")
            .bind(time)
            .bind(time)
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        let response = list_channels(
            AdminAuth,
            State(state.clone()),
            Query(ChannelFilter::default()),
        )
        .await
        .expect("channel list must load");
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        let providers: Vec<&str> = value["items"]
            .as_array()
            .expect("items must be an array")
            .iter()
            .map(|item| item["provider_name"].as_str().expect("provider_name missing"))
            .collect();
        assert_eq!(
            providers,
            vec!["alpha-provider", "zebra-provider"],
            "channels must be ordered by provider name, not channel name"
        );
    }

    /// `health_check_model_id: null` 把已存模型清回自动 —— 省略该字段的
    /// PATCH 保持原值不动，不属于该渠道的值则被拒绝。
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
        // 1. 显式 null 的 PATCH 清除该值。
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
        // 2. 省略该字段的 PATCH 不动（已清空的）值。
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
        // 3. 不属于该渠道的模型会被拒绝。
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
        // 4. 设置一个归属该渠道的模型可行。
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

    /// Command Code 渠道在渠道/模型编辑后保留每个可转换条目的绑定，
    /// 手动创建的模型也继承它们 —— 守卫不得拒绝候选为 CC 目录行的
    /// claude/openai 路由。
    #[tokio::test]
    async fn command_code_bindings_survive_channel_and_model_edits() {
        let (state, _dir) = test_state().await;
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("INSERT INTO providers(id,name,base_url,kind,created_at,updated_at) VALUES('prov-cc','cc','https://api.commandcode.ai','command_code',?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channels(id,provider_id,name,protocol,api_key_encrypted,api_key_hint,manual_enabled,created_at,updated_at) VALUES('ch-1','prov-cc','cc','command_code',X'00','hint',1,?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_protocols(channel_id,protocol) VALUES('ch-1','command_code')")
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_models(id,channel_id,model_id,display_name,source,available,first_seen_at,created_at,updated_at) VALUES('cm-1','ch-1','deepseek-v4','DeepSeek','discovered',1,?,?,?)")
            .bind(time)
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES('cm-1','command_code')")
            .execute(state.db.pool())
            .await
            .unwrap();
        // 在 `claude` 上有一个活跃候选 —— 该协议是 CC 渠道通过转换提供的，
        // 而非其自身目录所含。
        sqlx::query("INSERT INTO model_routes(id,protocol,requested_model_id,enabled,created_at,updated_at) VALUES('route-1','claude','deepseek-v4',1,?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO route_candidates(id,route_id,channel_model_id,priority,enabled,created_at,updated_at) VALUES('rc-1','route-1','cm-1',1,1,?,?)")
            .bind(time)
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();

        async fn bindings(state: &Context) -> Vec<String> {
            let mut rows: Vec<String> = sqlx::query_scalar(
                "SELECT protocol FROM channel_model_protocols WHERE channel_model_id='cm-1'",
            )
            .fetch_all(state.db.pool())
            .await
            .unwrap();
            rows.sort();
            rows
        }

        // 1. 编辑渠道协议时保留可转换条目的绑定
        //    （守卫与 DELETE 都使用展开后的集合）。
        patch_channel(
            AdminAuth,
            State(state.clone()),
            Path("ch-1".to_owned()),
            Json(
                serde_json::from_value::<ChannelPatch>(
                    serde_json::json!({"protocols": ["command_code"]}),
                )
                .unwrap(),
            ),
        )
        .await
        .expect("the claude route must not block the channel edit");
        assert_eq!(
            bindings(&state).await,
            [
                "claude",
                "command_code",
                "openai_compatible",
                "openai_responses"
            ]
        );
        let channel_protocols: Vec<String> =
            sqlx::query_scalar("SELECT protocol FROM channel_protocols WHERE channel_id='ch-1'")
                .fetch_all(state.db.pool())
                .await
                .unwrap();
        assert_eq!(
            channel_protocols,
            ["command_code"],
            "probe-level protocols stay as configured"
        );

        // 2. 未指定协议的手动模型会继承它们。
        let response = create_manual_model(
            AdminAuth,
            State(state.clone()),
            Path("ch-1".to_owned()),
            Json(
                serde_json::from_value::<ManualModelInput>(
                    serde_json::json!({"model_id": "manual-1"}),
                )
                .unwrap(),
            ),
        )
        .await
        .expect("manual model creation must succeed");
        assert_eq!(response.status(), StatusCode::CREATED);
        let mut manual: Vec<String> = sqlx::query_scalar(
            "SELECT cmp.protocol FROM channel_model_protocols cmp \
             JOIN channel_models cm ON cm.id = cmp.channel_model_id WHERE cm.model_id='manual-1'",
        )
        .fetch_all(state.db.pool())
        .await
        .unwrap();
        manual.sort();
        assert_eq!(
            manual,
            [
                "claude",
                "command_code",
                "openai_compatible",
                "openai_responses"
            ]
        );

        // 3. 把模型协议收窄为 `command_code` 时，保留活跃 claude 路由
        //    所需的条目绑定。
        patch_channel_model(
            AdminAuth,
            State(state.clone()),
            Path("cm-1".to_owned()),
            Json(
                serde_json::from_value::<ModelPatch>(
                    serde_json::json!({"protocols": ["command_code"]}),
                )
                .unwrap(),
            ),
        )
        .await
        .expect("the claude route must not block the model edit");
        assert_eq!(
            bindings(&state).await,
            [
                "claude",
                "command_code",
                "openai_compatible",
                "openai_responses"
            ]
        );
    }

    /// 渠道表单的 API key 与其他字段在同一事务中提交 ——
    /// key 被拒绝时整个补丁回滚。
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
        // 1. 一次 PATCH 同时带 name + api_key：两者一起提交。
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
        // 2. key 被拒绝（空）时整个补丁回滚。
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

    /// `GET /command-code/status` 透出开关、已验证的协议基线和版本漂移；
    /// 它不得泄露任何凭据材料。
    #[tokio::test]
    async fn command_code_status_reports_switch_baseline_and_drift() {
        let (state, _dir) = test_state().await;
        let response = command_code_status(AdminAuth, State(state.clone()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body.get("enabled").and_then(Value::as_bool), Some(false));
        assert_eq!(
            body.get("verified_baseline").and_then(Value::as_str),
            Some(crate::commandcode::DEFAULT_CLI_VERSION)
        );
        assert_eq!(body.get("drift").and_then(Value::as_bool), Some(false));
        assert_eq!(
            body.get("channel_count").and_then(Value::as_i64),
            Some(0)
        );
        assert!(
            body.get("warning")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .contains("403 upgrade_required")
        );
        assert!(!body.to_string().contains("api_key"));

        // 探测到的 CLI 版本与夹具基线不同时报告为漂移。
        sqlx::query(
            "INSERT INTO settings(key,value_json,updated_at) VALUES('command_code_cli_version','\"1.99.0\"',?)",
        )
        .bind("2026-08-04T01:00:00+00:00")
        .execute(state.db.pool())
        .await
        .unwrap();
        let response = command_code_status(AdminAuth, State(state.clone()))
            .await
            .unwrap();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            body.get("cli_version").and_then(Value::as_str),
            Some("1.99.0")
        );
        assert_eq!(body.get("drift").and_then(Value::as_bool), Some(true));
    }

    /// 静态预设目录以 Command Code Go 预设打头，
    /// 并携带 UI 必须确认的强制风险警告。
    #[tokio::test]
    async fn provider_presets_lead_with_command_code_go() {
        let response = list_provider_presets(AdminAuth).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        let items = body.get("items").and_then(Value::as_array).unwrap();
        let first = &items[0];
        assert_eq!(first.get("id").and_then(Value::as_str), Some("command_code_go"));
        assert_eq!(
            first.get("base_url").and_then(Value::as_str),
            Some("https://api.commandcode.ai")
        );
        assert_eq!(
            first.get("protocol").and_then(Value::as_str),
            Some("command_code")
        );
        assert_eq!(
            first.get("kind").and_then(Value::as_str),
            Some("command_code")
        );
        assert_eq!(
            first.get("auth").and_then(Value::as_str),
            Some("browser_login"),
            "Go 套餐必须在预设里声明网页登录授权"
        );
        let warning = first
            .get("warning")
            .and_then(Value::as_str)
            .expect("preset warning");
        assert!(warning.contains("403 upgrade_required"));
        assert!(warning.contains("账号封禁"));
        // 通用预设不得带警告或 kind。
        assert!(items[1..].iter().all(|item| item.get("warning").is_none_or(Value::is_null)));
    }

    /// Command Code 网页登录的一次性交接：管理端只拿到 `login_id`，密钥由
    /// 服务端取走并加密入库；句柄单次有效，重放与未知句柄都必须被拒绝。
    #[tokio::test]
    async fn channel_creation_consumes_a_command_code_login_handoff() {
        let (state, _dir) = test_state().await;
        sqlx::query("INSERT INTO providers(id,name,base_url,kind,created_at,updated_at) VALUES('prov-cc','cc','https://api.commandcode.ai','command_code',?,?)")
            .bind("2026-08-04T01:00:00+00:00")
            .bind("2026-08-04T01:00:00+00:00")
            .execute(state.db.pool())
            .await
            .unwrap();
        let status = state.command_code_login.store_validated_key(
            "user_handoff".to_owned(),
            "alice".to_owned(),
            "cli".to_owned(),
        );
        let login_id = match status {
            crate::commandcode_login::LoginStatus::Success { login_id, .. } => login_id,
            other => panic!("expected success status, got {other:?}"),
        };
        let input = |provider_id: &str, name: &str, login_id: Option<String>| ChannelInput {
            provider_id: provider_id.into(),
            name: name.into(),
            protocol: None,
            protocols: vec!["command_code".into()],
            api_key: String::new(),
            login_id,
            manual_enabled: true,
            health_check_model_id: None,
        };

        // 失败的创建（provider 不存在）不得消耗交接：凭据仍可重试。
        let error = create_channel(
            AdminAuth,
            State(state.clone()),
            Json(input("missing-provider", "cc-chan", Some(login_id.clone()))),
        )
        .await
        .expect_err("unknown provider must fail");
        assert_eq!(error.status, StatusCode::NOT_FOUND);
        assert_eq!(
            state.command_code_login.peek_key(&login_id).as_deref(),
            Some("user_handoff"),
            "a failed create must not burn the one-time handoff"
        );

        let response = create_channel(
            AdminAuth,
            State(state.clone()),
            Json(input("prov-cc", "cc-chan", Some(login_id.clone()))),
        )
        .await
        .expect("handoff must create the channel");
        assert_eq!(response.status(), StatusCode::CREATED);
        let encrypted: Vec<u8> =
            sqlx::query_scalar("SELECT api_key_encrypted FROM channels WHERE name='cc-chan'")
                .fetch_one(state.db.pool())
                .await
                .unwrap();
        assert_eq!(
            state.secrets.decrypt(&encrypted).unwrap(),
            "user_handoff",
            "the Studio key lands encrypted in the channel row"
        );

        // 单次有效：重放同一 login_id 会被拒绝且不创建任何东西。
        let error = create_channel(
            AdminAuth,
            State(state.clone()),
            Json(input("prov-cc", "cc-chan-2", Some(login_id))),
        )
        .await
        .expect_err("replayed handoff must be rejected");
        assert_eq!(error.status, StatusCode::UNPROCESSABLE_ENTITY);
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM channels")
            .fetch_one(state.db.pool())
            .await
            .unwrap();
        assert_eq!(count, 1, "no second channel may be created");

        // 未知句柄同样被拒绝。
        let error = create_channel(
            AdminAuth,
            State(state.clone()),
            Json(input("prov-cc", "cc-chan-3", Some("missing-login".into()))),
        )
        .await
        .expect_err("unknown handle must be rejected");
        assert_eq!(error.status, StatusCode::UNPROCESSABLE_ENTITY);
    }

    /// 登录状态端点透出状态机且永不包含密钥本身。
    #[tokio::test]
    async fn command_code_login_status_never_exposes_the_key() {
        let (state, _dir) = test_state().await;
        let response = command_code_login_status(AdminAuth, State(state.clone()))
            .await
            .unwrap();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            body.pointer("/login/state").and_then(Value::as_str),
            Some("idle")
        );
        assert_eq!(
            body.get("cli_key_available").and_then(Value::as_bool),
            Some(state.command_code_login.cli_key_available())
        );
        assert!(!body.to_string().contains("apiKey"));
        assert!(!body.to_string().contains("api_key"));
    }

}
