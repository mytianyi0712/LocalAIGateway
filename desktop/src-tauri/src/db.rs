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

/// 已应用迁移的校验和容错：迁移内容一旦与旧库记录的字节不同（例如历史整理
/// 时只改了注释头），sqlx 会以 `VersionMismatch` 拒绝启动，让已经部署的旧库
/// 在升级时直接打不开。这里把这类已应用行的校验和对齐到本次构建内嵌的版本并
/// 留下告警：迁移内容本身有 `migration_files_are_pinned` 测试看守，
/// 结构变更必须以新迁移承载，本函数只负责让历史库继续可用。
async fn reconcile_migration_checksums(
    pool: &SqlitePool,
    migrator: &sqlx::migrate::Migrator,
) -> Result<()> {
    let exists: Option<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='_sqlx_migrations'",
    )
    .fetch_optional(pool)
    .await?;
    if exists.is_none() {
        return Ok(());
    }
    let applied: Vec<(i64, Vec<u8>)> =
        sqlx::query_as("SELECT version, checksum FROM _sqlx_migrations")
            .fetch_all(pool)
            .await?;
    for (version, stored) in applied {
        let Some(migration) = migrator.iter().find(|item| item.version == version) else {
            continue;
        };
        if stored.as_slice() == migration.checksum.as_ref() {
            continue;
        }
        tracing::warn!(
            version,
            description = %migration.description,
            "已应用迁移的内容与本次构建不同；对齐校验和并继续（结构变更应以新迁移承载）"
        );
        sqlx::query("UPDATE _sqlx_migrations SET checksum=? WHERE version=?")
            .bind(migration.checksum.as_ref())
            .bind(version)
            .execute(pool)
            .await?;
    }
    Ok(())
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
        let migrator = sqlx::migrate!("./migrations");
        reconcile_migration_checksums(&pool, &migrator).await?;
        migrator
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

    /// 已应用迁移的校验和容错：旧库记录的校验和与本构建内嵌内容不同（历史
    /// 整理时只改了注释头就会这样）时，必须对齐校验和并继续启动，
    /// 而不是以 `VersionMismatch` 让已部署的库直接打不开。
    #[tokio::test]
    async fn migration_checksum_drift_is_reconciled_on_open() {
        let dir = TempDir::new("db-checksum");
        let path = dir.path().join("gateway.db");
        let db = Database::open(&path).await.unwrap();
        sqlx::query("UPDATE _sqlx_migrations SET checksum=? WHERE version=1")
            .bind(vec![0u8; 48])
            .execute(db.pool())
            .await
            .unwrap();
        drop(db);

        let db = Database::open(&path).await.unwrap();
        let stored: Vec<u8> =
            sqlx::query_scalar("SELECT checksum FROM _sqlx_migrations WHERE version=1")
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(
            stored.as_slice(),
            sqlx::migrate!("./migrations")
                .iter()
                .find(|item| item.version == 1)
                .unwrap()
                .checksum
                .as_ref(),
            "the stored checksum must be aligned with the embedded migration"
        );
    }

    /// 迁移文件一旦应用就必须保持字节不变：内容漂移会让已部署的库无法升级，
    /// 结构变更只能以新的（编号更大的）迁移承载。新增迁移时把它的哈希追加到
    /// 这里；修改既有迁移文件会让本测试失败。
    #[test]
    fn migration_files_are_pinned() {
        // (文件名, 内容 CRLF→LF 归一化后的 SHA-384)
        let pinned: [(&str, &str); 7] = [
            (
                "0001_gateway_schema.sql",
                "43e1e3b42cc7ac850c893e9fb2c582c7e02a065085d36cf966c9284fb8d51b66635b3d9cb4047ea451e009547eadc6ab",
            ),
            (
                "0002_token_usage.sql",
                "8947131e80babb58abb0fac354bd7db26fda97e2d37c75528e64491113b9c0369b5e2a32c876637cf99ca8d51c5f1c56",
            ),
            (
                "0003_remote_compaction.sql",
                "b7a2cba8191cb6042525f925af84246be4488823f80eb19d538f49a32d488e6cb4fbec1c8c1e385e9811aee2c003ebf1",
            ),
            (
                "0004_channel_balance.sql",
                "385deceae85c561c978b37e4174999c62925b7fc54f0c2b494209555166c32e04c8ad6c144d9b8a16dbc774b0e611584",
            ),
            (
                "0005_command_code.sql",
                "1929d47d8ccfc8a0daa7584f72a8b93835b42904af195d9968e3a690cd5beb4e83c41150a4a13f6a1d9b943634b4aa58",
            ),
            (
                "0006_command_code_model_bindings.sql",
                "a1b5fc1520a9f94d4c7ba6635180546150c14edb01fa5e05adcb2d25696688c77e8d6d0939730b18e3b13a1b1be73394",
            ),
            (
                "0007_drop_model_mappings.sql",
                "2a38eb7bb54e00592eb252bfca368f73d4a8c2415f4ef4d3d00bb23b8403c3f4ff8e5988cc3fb7f1e30409f2d9b88aa9",
            ),
        ];
        let mut failures = Vec::new();
        for (name, expected) in pinned {
            let raw = std::fs::read(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("migrations")
                    .join(name),
            )
            .unwrap();
            let normalized: Vec<u8> = {
                let mut out = Vec::with_capacity(raw.len());
                let mut iter = raw.iter().copied().peekable();
                while let Some(byte) = iter.next() {
                    if byte == b'\r' && iter.peek() == Some(&b'\n') {
                        continue;
                    }
                    out.push(byte);
                }
                out
            };
            let digest = {
                use sha2::{Digest as _, Sha384};
                let mut hasher = Sha384::new();
                hasher.update(&normalized);
                hasher.finalize()
            };
            let actual = digest
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            if actual != expected {
                failures.push(format!("{name}: {actual} != {expected}"));
            }
        }
        assert!(
            failures.is_empty(),
            "迁移文件被修改了；结构变更请新增迁移：{failures:?}"
        );
    }

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
