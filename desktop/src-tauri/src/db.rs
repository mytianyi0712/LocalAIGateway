use std::{path::Path, str::FromStr, time::Duration};

use anyhow::{Context, Result};
use sqlx::{SqlitePool, sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions}};

#[derive(Clone)]
pub struct Database {
    pool: SqlitePool,
}

impl Database {
    pub async fn open(path: &Path) -> Result<Self> {
        let url = format!("sqlite://{}", path.to_string_lossy());
        let options = SqliteConnectOptions::from_str(&url)?
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(8)
            .acquire_timeout(Duration::from_secs(10))
            .connect_with(options).await
            .with_context(|| format!("无法打开数据库 {}", path.display()))?;
        sqlx::migrate!("./migrations").run(&pool).await
            .context("无法初始化数据库结构")?;
        let database = Self { pool };
        database.ensure_legacy_columns().await?;
        database.backfill_protocol_bindings().await?;
        Ok(database)
    }

    pub fn pool(&self) -> &SqlitePool { &self.pool }

    async fn ensure_legacy_columns(&self) -> Result<()> {
        self.add_column_if_missing("model_caps", "profile_id", "TEXT REFERENCES capability_profiles(id) ON DELETE SET NULL").await?;
        self.add_column_if_missing("request_attempts", "upstream_protocol", "TEXT").await?;
        self.add_column_if_missing("request_attempts", "upstream_model_id", "TEXT").await?;
        self.add_column_if_missing("claude_model_mappings", "upstream_model_id", "TEXT").await?;
        sqlx::query("UPDATE claude_model_mappings SET upstream_model_id = claude_model_id WHERE upstream_model_id IS NULL")
            .execute(&self.pool).await?;
        Ok(())
    }

    async fn add_column_if_missing(&self, table: &str, column: &str, declaration: &str) -> Result<()> {
        let pragma = format!("PRAGMA table_info({table})");
        let columns: Vec<(i64, String, String, i64, Option<String>, i64)> = sqlx::query_as(&pragma)
            .fetch_all(&self.pool).await?;
        if !columns.iter().any(|(_, name, _, _, _, _)| name == column) {
            sqlx::query(&format!("ALTER TABLE {table} ADD COLUMN {column} {declaration}"))
                .execute(&self.pool).await?;
        }
        Ok(())
    }

    async fn backfill_protocol_bindings(&self) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT OR IGNORE INTO channel_protocols(channel_id, protocol) SELECT id, protocol FROM channels"
        ).execute(&mut *tx).await?;
        sqlx::query(
            "INSERT OR IGNORE INTO channel_model_protocols(channel_model_id, protocol) \
             SELECT cm.id, c.protocol FROM channel_models cm JOIN channels c ON c.id = cm.channel_id \
             WHERE NOT EXISTS (SELECT 1 FROM channel_model_protocols cmp WHERE cmp.channel_model_id = cm.id)"
        ).execute(&mut *tx).await?;
        sqlx::query(
            "INSERT OR IGNORE INTO channel_model_protocols(channel_model_id, protocol) \
             SELECT cm.id, cp.protocol FROM channel_models cm \
             JOIN channel_protocols cp ON cp.channel_id = cm.channel_id \
             WHERE cp.protocol IN ('openai_compatible', 'openai_responses') \
               AND EXISTS (SELECT 1 FROM channel_model_protocols current \
                           WHERE current.channel_model_id = cm.id \
                             AND current.protocol IN ('openai_compatible', 'openai_responses'))"
        ).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
}
