-- Command Code Go 集成（路线 B）。
--
-- `providers.kind` 是显式的身份标记：只有在 `kind = 'command_code'` 时才会注入
-- Command Code 身份头（session、fingerprint、CLI 版本），绝不靠嗅探 `base_url` 判断，
-- 这样自建桥接或任何第三方上游都不可能收到该指纹。
-- 已存在的供应商保持 `kind = NULL`，即使其 base_url 指向 api.commandcode.ai 也不会
-- 被当作 Command Code 处理。
--
-- 集成所需的其余状态（fingerprint、session、transport memory、初始化限流、CLI 版本）
-- 都放在 `settings` 键值表中，因此无需再改结构。
--
-- 幂等性：ALTER TABLE ADD COLUMN 不可重复执行，本迁移只由 sqlx::migrate! 应用一次；
--         其中的建索引用 `IF NOT EXISTS`。
-- 回滚注意：没有 down 脚本，回滚只能靠数据库备份；已升级的库会校验本文件内容。

ALTER TABLE providers ADD COLUMN kind TEXT;

CREATE INDEX IF NOT EXISTS idx_providers_kind ON providers(kind);
