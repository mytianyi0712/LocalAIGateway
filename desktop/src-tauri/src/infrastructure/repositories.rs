use std::sync::Arc;

use anyhow::Result;

use sqlx::Row;

use crate::{
    db::Database,
    domain::{Candidate, CompactionMode, CompactionSupport, RoutableModel},
    ports::{
        ChannelRepository, ChannelRow, Clock, RouteRepository, UpstreamClient,
    },

};

/// `Candidate` 位于 `domain`（不依赖 sqlx），行映射在此手工完成：
/// SQL 的列名与字段名一致（`AS` 别名见各查询）。
fn candidate_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Candidate, sqlx::Error> {
    Ok(Candidate {
        candidate_id: row.try_get("candidate_id")?,
        channel_id: row.try_get("channel_id")?,
        channel_name: row.try_get("channel_name")?,
        priority: row.try_get("priority")?,
        base_url: row.try_get("base_url")?,
        kind: row.try_get("kind")?,
        api_key_encrypted: row.try_get("api_key_encrypted")?,
        model_id: row.try_get("model_id")?,
        remote_compaction_v1_support: row.try_get("remote_compaction_v1_support")?,
        remote_compaction_v2_support: row.try_get("remote_compaction_v2_support")?,
    })
}

/// 同 [`candidate_from_row`]：`RoutableModel` 位于 `domain`。
fn routable_model_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<RoutableModel, sqlx::Error> {
    Ok(RoutableModel {
        id: row.try_get("id")?,
        display_name: row.try_get("display_name")?,
        created_at: row.try_get("created_at")?,
    })
}

/// 系统时钟：`Clock` 端口的生产实现。
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_utc(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }
}

/// 路由端口的 SQLite 实现：候选解析、目录与映射查询。
pub struct SqliteRouteRepository {
    db: Database,
}

impl SqliteRouteRepository {
    pub fn new(db: Database) -> Arc<Self> {
        Arc::new(Self { db })
    }
}

impl RouteRepository for SqliteRouteRepository {
    /// 与 [`crate::proxy`] 的压缩成功路径共用的一条 UPDATE；
    /// SQL 语义（列名、条件、时间戳）与原先内联在代理里的语句逐字一致。
    fn set_compaction_support(
        &self,
        channel_id: &str,
        mode: CompactionMode,
        supported: bool,
    ) -> futures_util::future::BoxFuture<'static, Result<()>> {
        let db = self.db.clone();
        let channel_id = channel_id.to_owned();
        let column = match mode {
            CompactionMode::V1 => "remote_compaction_v1_support",
            CompactionMode::V2 => "remote_compaction_v2_support",
        };
        let value = if supported {
            CompactionSupport::Supported.as_db()
        } else {
            CompactionSupport::Unsupported.as_db()
        };
        Box::pin(async move {
            let now = chrono::Utc::now().to_rfc3339();
            let sql = format!(
                "UPDATE channel_protocols SET {column} = ?, remote_compaction_probed_at = ? \
                 WHERE channel_id = ? AND protocol = 'openai_responses'"
            );
            if let Err(error) = sqlx::query(&sql)
                .bind(value)
                .bind(&now)
                .bind(&channel_id)
                .execute(db.pool())
                .await
            {
                tracing::warn!(
                    channel_id,
                    mode = mode.as_str(),
                    error = ?error,
                    "failed to persist remote compaction capability"
                );
            }
            Ok(())
        })
    }

    fn resolve_candidates(
        &self,
        protocol: &str,
        model_id: &str,
        limit: i64,
    ) -> futures_util::future::BoxFuture<'static, Result<Vec<Candidate>>> {
        let db = self.db.clone();
        let protocol = protocol.to_owned();
        let model_id = model_id.to_owned();
        Box::pin(async move {
            let rows = sqlx::query(
            "SELECT rc.id AS candidate_id, c.id AS channel_id, c.name AS channel_name, \
                    rc.priority, p.base_url, p.kind, c.api_key_encrypted, cm.model_id, \
                    COALESCE(cp.remote_compaction_v1_support, 0) AS remote_compaction_v1_support, \
                    COALESCE(cp.remote_compaction_v2_support, 0) AS remote_compaction_v2_support \
             FROM route_candidates rc \
             JOIN model_routes mr ON mr.id = rc.route_id \
             JOIN channel_models cm ON cm.id = rc.channel_model_id \
             JOIN channel_model_protocols cmp ON cmp.channel_model_id = cm.id AND cmp.protocol = mr.protocol \
             JOIN channels c ON c.id = cm.channel_id \
             JOIN providers p ON p.id = c.provider_id \
             JOIN channel_health ch ON ch.channel_id = c.id \
             LEFT JOIN channel_protocols cp ON cp.channel_id = c.id AND cp.protocol = mr.protocol \
             WHERE mr.protocol = ? AND mr.requested_model_id = ? AND mr.enabled = 1 \
               AND rc.enabled = 1 AND cm.available = 1 AND c.manual_enabled = 1 AND ch.state = 'active' \
             ORDER BY rc.priority ASC LIMIT ?"
        )
        .bind(protocol.as_str())
        .bind(model_id.as_str())
        .bind(limit)
        .fetch_all(db.pool())
        .await?;
            Ok(rows.iter().map(candidate_from_row).collect::<Result<Vec<_>, _>>()?)
        })
    }

    fn resolve_compaction_candidates(
        &self,
        protocol: &str,
        model_id: &str,
        mode: CompactionMode,
        limit: i64,
    ) -> futures_util::future::BoxFuture<'static, Result<Vec<Candidate>>> {
        let db = self.db.clone();
        let protocol = protocol.to_owned();
        let model_id = model_id.to_owned();
        let capability_column = match mode {
            CompactionMode::V1 => "cp.remote_compaction_v1_support",
            CompactionMode::V2 => "cp.remote_compaction_v2_support",
        };
        // Command Code 没有 `/v1/responses/compact`：其目录行为了路由而带有
        // `openai_responses` 绑定（见 `protocol::model_binding_protocols`），
        // 因此只靠能力过滤会把 Codex 压缩请求发给 CC 渠道。
        let sql = format!(
            "SELECT rc.id AS candidate_id, c.id AS channel_id, c.name AS channel_name, \
                    rc.priority, p.base_url, p.kind, c.api_key_encrypted, cm.model_id, \
                    COALESCE(cp.remote_compaction_v1_support, 0) AS remote_compaction_v1_support, \
                    COALESCE(cp.remote_compaction_v2_support, 0) AS remote_compaction_v2_support \
             FROM route_candidates rc \
             JOIN model_routes mr ON mr.id = rc.route_id \
             JOIN channel_models cm ON cm.id = rc.channel_model_id \
             JOIN channel_model_protocols cmp ON cmp.channel_model_id = cm.id AND cmp.protocol = mr.protocol \
             JOIN channels c ON c.id = cm.channel_id \
             JOIN providers p ON p.id = c.provider_id \
             JOIN channel_health ch ON ch.channel_id = c.id \
             LEFT JOIN channel_protocols cp ON cp.channel_id = c.id AND cp.protocol = mr.protocol \
             WHERE mr.protocol = ? AND mr.requested_model_id = ? AND mr.enabled = 1 \
               AND rc.enabled = 1 AND cm.available = 1 AND c.manual_enabled = 1 AND ch.state = 'active' \
               AND COALESCE(p.kind, '') != 'command_code' \
               AND COALESCE({capability_column}, 0) != 2 \
             ORDER BY rc.priority ASC LIMIT ?"
        );
        Box::pin(async move {
            let rows = sqlx::query(&sql)
                .bind(protocol.as_str())
                .bind(model_id.as_str())
                .bind(limit)
                .fetch_all(db.pool())
                .await?;
            Ok(rows.iter().map(candidate_from_row).collect::<Result<Vec<_>, _>>()?)
        })
    }

    fn list_routable_models(
        &self,
        protocol: Option<&str>,
    ) -> futures_util::future::BoxFuture<'static, Result<Vec<RoutableModel>>> {
        let db = self.db.clone();
        let protocol = protocol.map(str::to_owned);
        Box::pin(async move {
            let rows = sqlx::query(
            "SELECT mr.requested_model_id AS id, \
                    COALESCE((SELECT MAX(cm.display_name) FROM channel_models cm \
                      JOIN route_candidates rc ON rc.channel_model_id = cm.id \
                      JOIN model_routes r2 ON r2.id = rc.route_id \
                      WHERE r2.requested_model_id = mr.requested_model_id AND r2.enabled = 1), \
                     mr.requested_model_id) AS display_name, \
                    MIN(mr.created_at) AS created_at \
             FROM model_routes mr \
             WHERE mr.enabled = 1 AND (? IS NULL OR mr.protocol = ?) \
               AND EXISTS (SELECT 1 FROM route_candidates rc \
                 JOIN channel_models cm ON cm.id = rc.channel_model_id \
                 JOIN channel_model_protocols cmp ON cmp.channel_model_id = cm.id AND cmp.protocol = mr.protocol \
                 JOIN channels c ON c.id = cm.channel_id \
                 JOIN channel_health ch ON ch.channel_id = c.id \
                 WHERE rc.route_id = mr.id AND rc.enabled = 1 AND cm.available = 1 \
                   AND c.manual_enabled = 1 AND ch.state = 'active') \
             GROUP BY mr.requested_model_id ORDER BY mr.requested_model_id"
        )
        .bind(protocol.as_deref())
        .bind(protocol.as_deref())
        .fetch_all(db.pool())
        .await?;
            Ok(rows
                .iter()
                .map(routable_model_from_row)
                .collect::<Result<Vec<_>, _>>()?)
        })
    }

    fn routable_endpoints_for_model(
        &self,
        model_id: &str,
    ) -> futures_util::future::BoxFuture<'static, Result<Vec<String>>> {
        let db = self.db.clone();
        let model_id = model_id.to_owned();
        Box::pin(async move {
            let protocols: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT mr.protocol FROM model_routes mr \
             WHERE mr.requested_model_id = ? AND mr.enabled = 1 \
               AND EXISTS (SELECT 1 FROM route_candidates rc \
                 JOIN channel_models cm ON cm.id = rc.channel_model_id \
                 JOIN channel_model_protocols cmp ON cmp.channel_model_id = cm.id AND cmp.protocol = mr.protocol \
                 JOIN channels c ON c.id = cm.channel_id \
                 JOIN channel_health ch ON ch.channel_id = c.id \
                 WHERE rc.route_id = mr.id AND rc.enabled = 1 AND cm.available = 1 \
                   AND c.manual_enabled = 1 AND ch.state = 'active') \
             ORDER BY CASE mr.protocol WHEN 'openai_compatible' THEN 0 \
               WHEN 'openai_responses' THEN 1 WHEN 'claude' THEN 2 WHEN 'command_code' THEN 4 ELSE 3 END",
        )
        .bind(model_id.as_str())
        .fetch_all(db.pool())
        .await?;
            Ok(crate::routing::endpoints_for_protocols(
                &protocols.iter().map(String::as_str).collect::<Vec<_>>(),
                &model_id,
            ))
        })
    }
}

/// 渠道端口的 SQLite 实现：探测、discovery 与 admin 守卫背后的行加载。
pub struct SqliteChannelRepository {
    db: Database,
}

impl SqliteChannelRepository {
    pub fn new(db: Database) -> Arc<Self> {
        Arc::new(Self { db })
    }
}

impl ChannelRepository for SqliteChannelRepository {
    fn load_channel(
        &self,
        channel_id: &str,
    ) -> futures_util::future::BoxFuture<'static, Result<Option<ChannelRow>>> {
        let db = self.db.clone();
        let channel_id = channel_id.to_owned();
        Box::pin(async move {
            let row = sqlx::query(
            "SELECT c.id, c.name, c.protocol, c.health_check_model_id, c.api_key_encrypted, p.base_url, p.kind, c.manual_enabled \
             FROM channels c JOIN providers p ON p.id=c.provider_id WHERE c.id=?",
        )
        .bind(channel_id)
        .fetch_optional(db.pool())
        .await?;
            Ok(row.map(|row| ChannelRow {
                id: row.get("id"),
                name: row.get("name"),
                protocol: row.get("protocol"),
                kind: row.get("kind"),
                health_check_model_id: row.get("health_check_model_id"),
                api_key_encrypted: row.get("api_key_encrypted"),
                base_url: row.get("base_url"),
                manual_enabled: row.get("manual_enabled"),
            }))
        })
    }

    fn exists(
        &self,
        channel_id: &str,
    ) -> futures_util::future::BoxFuture<'static, Result<bool>> {
        let db = self.db.clone();
        let channel_id = channel_id.to_owned();
        Box::pin(async move {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM channels WHERE id=?")
                .bind(channel_id)
                .fetch_one(db.pool())
                .await?;
            Ok(count > 0)
        })
    }

    fn provider_kind(
        &self,
        channel_id: &str,
    ) -> futures_util::future::BoxFuture<'static, Result<Option<String>>> {
        let db = self.db.clone();
        let channel_id = channel_id.to_owned();
        Box::pin(async move {
            Ok(sqlx::query_scalar(
                "SELECT p.kind FROM channels c JOIN providers p ON p.id = c.provider_id WHERE c.id=?",
            )
            .bind(channel_id)
            .fetch_optional(db.pool())
            .await?)
        })
    }

    fn protocols(
        &self,
        channel_id: &str,
    ) -> futures_util::future::BoxFuture<'static, Result<Vec<String>>> {
        let db = self.db.clone();
        let channel_id = channel_id.to_owned();
        Box::pin(async move {
            Ok(sqlx::query_scalar(
                "SELECT protocol FROM channel_protocols WHERE channel_id=? ORDER BY rowid",
            )
            .bind(channel_id)
            .fetch_all(db.pool())
            .await?)
        })
    }
}

/// [`crate::ports::SettingsReader`] 的生产实现：settings 表 + 密钥库。
pub struct SettingsStore {
    db: Database,
    secrets: crate::crypto::SecretStore,
}

impl SettingsStore {
    pub fn new(db: Database, secrets: crate::crypto::SecretStore) -> Arc<Self> {
        Arc::new(Self { db, secrets })
    }
}

impl crate::ports::SettingsReader for SettingsStore {
    fn runtime_settings(
        &self,
    ) -> futures_util::future::BoxFuture<'static, Result<crate::settings::RuntimeSettings>> {
        let db = self.db.clone();
        Box::pin(async move { crate::settings::runtime_settings_from(&db).await })
    }

    fn opencode_session_id(&self) -> futures_util::future::BoxFuture<'static, String> {
        let db = self.db.clone();
        Box::pin(async move { crate::settings::opencode_session_id(&db).await })
    }

    fn authorize_gateway<'a>(
        &'a self,
        headers: &'a http::HeaderMap,
        query: Option<&'a str>,
        protocol: &'a str,
    ) -> futures_util::future::BoxFuture<'a, Result<bool>> {
        Box::pin(async move {
            crate::settings::authorize_gateway_from(&self.db, &self.secrets, headers, query, protocol)
                .await
        })
    }
}

/// [`crate::ports::CommandCodeState`] 的生产实现：`commandcode` 的状态函数
/// （KV 节流、传输模式、身份头、配额冷却）。
pub struct CommandCodeStore {
    db: Database,
    http: Arc<dyn UpstreamClient>,
}

impl CommandCodeStore {
    pub fn new(db: Database, http: Arc<dyn UpstreamClient>) -> Arc<Self> {
        Arc::new(Self { db, http })
    }
}

impl crate::ports::CommandCodeState for CommandCodeStore {
    fn transport(
        &self,
        channel_id: &str,
    ) -> futures_util::future::BoxFuture<'static, crate::commandcode::Transport> {
        let db = self.db.clone();
        let channel_id = channel_id.to_owned();
        Box::pin(async move { crate::commandcode::transport(&db, &channel_id).await })
    }

    fn set_transport(
        &self,
        channel_id: &str,
        transport: crate::commandcode::Transport,
    ) -> futures_util::future::BoxFuture<'static, Result<()>> {
        let db = self.db.clone();
        let channel_id = channel_id.to_owned();
        Box::pin(async move { crate::commandcode::set_transport(&db, &channel_id, transport).await })
    }

    fn identity(
        &self,
        channel_id: &str,
    ) -> futures_util::future::BoxFuture<'static, Result<crate::protocol::CommandCodeIdentity>> {
        let db = self.db.clone();
        let channel_id = channel_id.to_owned();
        Box::pin(async move { crate::commandcode::identity(&db, &channel_id).await })
    }

    fn ensure_initialized(
        &self,
        channel_id: &str,
        api_key: &str,
        base_url: &str,
        interval_hours: i64,
    ) -> futures_util::future::BoxFuture<'static, Result<()>> {
        let db = self.db.clone();
        let http = Arc::clone(&self.http);
        let channel_id = channel_id.to_owned();
        let api_key = api_key.to_owned();
        let base_url = base_url.to_owned();
        Box::pin(async move {
            crate::commandcode::ensure_initialized(
                &db,
                http.as_ref(),
                &channel_id,
                &api_key,
                &base_url,
                interval_hours,
            )
            .await
        })
    }

    fn mark_quota_exhausted(
        &self,
        channel_id: &str,
        reset_at: Option<chrono::DateTime<chrono::Utc>>,
        open_seconds: i64,
        status: u16,
    ) -> futures_util::future::BoxFuture<'static, Result<()>> {
        let db = self.db.clone();
        let channel_id = channel_id.to_owned();
        Box::pin(async move {
            crate::commandcode::mark_quota_exhausted(
                &db,
                &channel_id,
                reset_at,
                open_seconds,
                status,
            )
            .await
        })
    }
}
