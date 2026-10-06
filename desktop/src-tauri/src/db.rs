//! 数据库层：SQLite 连接池的建立、迁移执行与历史库的兼容修补。
//!
//! 职责：打开/创建 gateway.db、运行 `migrations/`、为旧库补齐缺失列并回填协议
//! 绑定；对外只暴露 `pool()`。
//! 边界：只承载建表/迁移与一次性回填（`backfill_protocol_bindings` 的协议别名
//! 规则属于历史迁移）；日常读写由各服务自行组织。
//! 关键不变量：数据库文件以 0600 权限创建、并在每次打开时收紧；连接启用 WAL 与
//! `foreign_keys`；`channel_model_protocols` 的别名回填绝不写入 `command_code`。

use std::{path::Path, str::FromStr, time::Duration};

use anyhow::{Context, Result};
use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};

#[derive(Clone)]
pub struct Database {
    pool: SqlitePool,
}

impl Database {
    pub async fn open(path: &Path) -> Result<Self> {
        // 库里存着加密的渠道密钥与全部请求日志，文件权限必须是 0600。
        // 先按 0600 预创建（已存在则忽略），再显式 chmod——旧库或被改过权限的
        // 文件同样会被纠正。数据库连接之前完成，避免出现一段“宽权限”窗口。
        #[cfg(unix)]
        {
            use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
            match std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(path)
            {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("无法创建数据库文件 {}", path.display()));
                }
            }
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
                .with_context(|| format!("无法设置数据库文件权限 {}", path.display()))?;
        }
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
            .connect_with(options)
            .await
            .with_context(|| format!("无法打开数据库 {}", path.display()))?;
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .context("无法初始化数据库结构")?;
        let database = Self { pool };
        database.ensure_legacy_columns().await?;
        database.backfill_protocol_bindings().await?;
        Ok(database)
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    async fn ensure_legacy_columns(&self) -> Result<()> {
        self.add_column_if_missing(
            "model_caps",
            "profile_id",
            "TEXT REFERENCES capability_profiles(id) ON DELETE SET NULL",
        )
        .await?;
        self.add_column_if_missing("request_attempts", "upstream_protocol", "TEXT")
            .await?;
        self.add_column_if_missing("request_attempts", "upstream_model_id", "TEXT")
            .await?;
        self.add_column_if_missing("health_probe_logs", "protocol", "TEXT")
            .await?;
        Ok(())
    }

    async fn add_column_if_missing(
        &self,
        table: &str,
        column: &str,
        declaration: &str,
    ) -> Result<()> {
        let pragma = format!("PRAGMA table_info({table})");
        let columns: Vec<(i64, String, String, i64, Option<String>, i64)> =
            sqlx::query_as(&pragma).fetch_all(&self.pool).await?;
        if !columns.iter().any(|(_, name, _, _, _, _)| name == column) {
            sqlx::query(&format!(
                "ALTER TABLE {table} ADD COLUMN {column} {declaration}"
            ))
            .execute(&self.pool)
            .await?;
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
        // 历史别名：支持任一 OpenAI 协议的模型同时获得两种绑定，
        // 使 catalog/route 查询可以互换。
        // 有意不包含 Command Code：该别名绝不能把只有 `command_code` 的行
        // 变成 OpenAI 家族——`kind='command_code'` 的渠道，其可转换的入口绑定
        // 由 discovery（`protocol::model_binding_protocols`）写入。
        sqlx::query(
            "INSERT OR IGNORE INTO channel_model_protocols(channel_model_id, protocol) \
             SELECT cm.id, cp.protocol FROM channel_models cm \
             JOIN channel_protocols cp ON cp.channel_id = cm.channel_id \
             WHERE cp.protocol IN ('openai_compatible', 'openai_responses') \
               AND EXISTS (SELECT 1 FROM channel_model_protocols current \
                           WHERE current.channel_model_id = cm.id \
                             AND current.protocol IN ('openai_compatible', 'openai_responses'))",
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    /// 数据库文件必须只有属主可读写（0600）。
    #[cfg(unix)]
    #[tokio::test]
    async fn database_file_is_created_with_owner_only_permissions() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = std::env::temp_dir().join(format!("lagw-db-perm-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("gateway.db");
        let _db = Database::open(&path).await.unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "a fresh gateway.db must not be readable by others");

        // 已被写成宽权限的旧库（例如早期版本创建的、或用户手动 chmod 过）
        // 打开后必须被纠正回 0600。
        let legacy = dir.join("legacy.db");
        std::fs::write(&legacy, b"").unwrap();
        std::fs::set_permissions(&legacy, std::fs::Permissions::from_mode(0o644)).unwrap();
        let _legacy_db = Database::open(&legacy).await.unwrap();
        let legacy_mode = std::fs::metadata(&legacy).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            legacy_mode, 0o600,
            "an existing wide-open gateway.db must be tightened on open"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
    use super::*;
    use crate::test_support::TempDir;

    /// Migration 0006 为既有的 Command Code catalog 行回填可转换的入口绑定，
    /// 使路由候选池无需等待下一轮 discovery 即可提供它们。判定依据是 provider 的
    /// `kind`——仅“会说” `command_code` 协议的渠道不受影响。
    #[tokio::test]
    async fn command_code_binding_migration_backfills_existing_rows() {
        let dir = TempDir::new("db");
        let db = Database::open(&dir.path().join("test.db")).await.unwrap();
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query(
            "INSERT INTO providers(id,name,base_url,kind,created_at,updated_at) \
             VALUES('prov-cc','cc','https://api.commandcode.ai','command_code',?,?), \
                   ('prov-x','x','https://relay.example.com',NULL,?,?)",
        )
        .bind(time)
        .bind(time)
        .bind(time)
        .bind(time)
        .execute(db.pool())
        .await
        .unwrap();
        // (渠道, provider, 模型, 渠道协议)
        let seeded = [
            ("ch-cc", "prov-cc", "cc-model", "command_code"),
            ("ch-x", "prov-x", "other-model", "command_code"),
        ];
        for (channel, provider, model, protocol_name) in seeded {
            sqlx::query("INSERT INTO channels(id,provider_id,name,protocol,api_key_encrypted,api_key_hint,manual_enabled,created_at,updated_at) VALUES(?,?,'c',?,X'00','h',1,?,?)")
                .bind(channel)
                .bind(provider)
                .bind(protocol_name)
                .bind(time)
                .bind(time)
                .execute(db.pool())
                .await
                .unwrap();
            let model_row = format!("cm-{channel}");
            sqlx::query("INSERT INTO channel_models(id,channel_id,model_id,display_name,source,available,first_seen_at,created_at,updated_at) VALUES(?,?,?,'M','discovered',1,?,?,?)")
                .bind(&model_row)
                .bind(channel)
                .bind(model)
                .bind(time)
                .bind(time)
                .bind(time)
                .execute(db.pool())
                .await
                .unwrap();
            sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES(?,?)")
                .bind(&model_row)
                .bind(protocol_name)
                .execute(db.pool())
                .await
                .unwrap();
        }

        // 重新执行一遍迁移语句：测试库是对空表跑完整迁移集创建的，
        // 而这里是“已拥有这些行”的存量安装的升级路径。
        sqlx::query(include_str!("../migrations/0006_command_code_model_bindings.sql"))
            .execute(db.pool())
            .await
            .unwrap();

        let mut command_code: Vec<String> = sqlx::query_scalar(
            "SELECT protocol FROM channel_model_protocols WHERE channel_model_id='cm-ch-cc'",
        )
        .fetch_all(db.pool())
        .await
        .unwrap();
        command_code.sort();
        assert_eq!(
            command_code,
            [
                "claude",
                "command_code",
                "openai_compatible",
                "openai_responses"
            ]
        );
        let untouched: Vec<String> = sqlx::query_scalar(
            "SELECT protocol FROM channel_model_protocols WHERE channel_model_id='cm-ch-x'",
        )
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(
            untouched,
            ["command_code"],
            "only providers with kind='command_code' are backfilled"
        );

        // 幂等：再次执行不会新增任何东西。
        sqlx::query(include_str!("../migrations/0006_command_code_model_bindings.sql"))
            .execute(db.pool())
            .await
            .unwrap();
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM channel_model_protocols WHERE channel_model_id='cm-ch-cc'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(count, 4);
    }
}
