-- 渠道余额 / 用量查询的附属表。
--
-- 余额能力默认关闭：`channel_balance_configs` 里没有该渠道的行时，绝不会对该渠道
-- 发出任何上游余额请求。用户需按渠道选用一个适配器并打开开关才会生效。
--
-- `channel_balance_configs` 保存用户选定的适配器与可选的自定义请求模板；
-- `channel_balance_snapshots` 每渠道只保留最新一次读数（主键为 channel_id）——
-- 余额本质上是「当前值」。
--
-- 安全：从不落库上游原始响应体；访问令牌只以密文存在于 `balance_token_encrypted`
-- （Fernet），快照只保存归一化后的非敏感字段。
--
-- 幂等性：建表未用 `IF NOT EXISTS`，重复执行会报错，因此只由 sqlx::migrate! 应用一次。
-- 回滚注意：没有 down 脚本，回滚只能靠数据库备份；已升级的库会校验本文件内容。

CREATE TABLE channel_balance_configs (
  channel_id TEXT PRIMARY KEY REFERENCES channels(id) ON DELETE CASCADE,
  adapter TEXT NOT NULL,                     -- 适配器标识，取值见 balance::BalanceAdapter（newapi/sub2api/…/custom）
  enabled INTEGER NOT NULL DEFAULT 0,        -- 手动刷新与后台刷新的唯一开关
  method TEXT NOT NULL DEFAULT 'GET',        -- 仅 custom 可改；内置适配器固定用 GET
  path TEXT,                                 -- custom 必填；内置适配器保存各自预设路径
  auth TEXT NOT NULL DEFAULT 'bearer',       -- bearer|raw|none
  headers_json TEXT,                         -- {"X-Foo":"${token}"}，最多 8 条
  body_json TEXT,                            -- custom 的请求体模板
  mapping_json TEXT,                         -- {"remaining":"$.data.balance", ...}
  balance_token_encrypted BLOB,              -- 可选的专用令牌（密文）
  balance_token_hint TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE channel_balance_snapshots (
  channel_id TEXT PRIMARY KEY REFERENCES channels(id) ON DELETE CASCADE,
  adapter TEXT NOT NULL,
  status TEXT NOT NULL,                      -- ok|error
  remaining REAL,
  currency TEXT,
  used REAL,
  total REAL,
  unlimited INTEGER NOT NULL DEFAULT 0,
  label TEXT,
  windows_json TEXT,                         -- [{label,used_percent,remaining_percent,resets_at}]
  detail_json TEXT,                          -- 适配器专属的非敏感明细（余额构成、套餐信息等）
  error_kind TEXT,
  status_code INTEGER,
  duration_ms INTEGER,
  checked_at TEXT NOT NULL
);
