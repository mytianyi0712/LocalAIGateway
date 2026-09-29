-- Codex 的 Remote Compaction 能力标记（V1 的 /responses/compact 与 V2 的
-- compaction_trigger）。按「渠道 + 协议」记录，因为上游是否支持专用 compact 端点
-- 或 trigger-item 流程属于供应商/协议层属性，而不是模型层属性。
--
-- 取值：
--   0 = 未知 / 未探测
--   1 = 支持
--   2 = 不支持
--
-- `remote_compaction_probed_at` 记录最后一次成功写入的探测时间；
-- `remote_compaction_last_error`：预留列，当前探测不写入、管理端不展示。
--
-- 幂等性：ALTER TABLE ADD COLUMN 本身不可重复执行（重复会报 duplicate column），
--         因此本迁移只由 sqlx::migrate! 按版本号应用一次。
-- 回滚注意：没有 down 脚本，回滚只能靠数据库备份；已升级的库会校验本文件内容。

ALTER TABLE channel_protocols
  ADD COLUMN remote_compaction_v1_support INTEGER NOT NULL DEFAULT 0;

ALTER TABLE channel_protocols
  ADD COLUMN remote_compaction_v2_support INTEGER NOT NULL DEFAULT 0;

ALTER TABLE channel_protocols
  ADD COLUMN remote_compaction_probed_at TEXT;

ALTER TABLE channel_protocols
  ADD COLUMN remote_compaction_last_error TEXT;
