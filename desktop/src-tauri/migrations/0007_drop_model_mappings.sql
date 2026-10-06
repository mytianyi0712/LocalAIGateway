-- 移除「Claude / Codex 模型映射」功能：两张映射表已无任何读写方
--（映射入口 `/claudecode`、`/codex` 与管理端映射 API 均随功能一并删除）。
--
-- 幂等性：`DROP TABLE IF EXISTS`，重复执行不会失败（新库先由 0001 建表，再由本迁移删除；
-- 历史库直接删除）。
-- 回滚注意：没有 down 脚本，回滚只能靠数据库备份；已升级的库会校验本文件内容，
-- 因此 0001 里的建表语句保持原样——删除只能由本迁移完成。
DROP TABLE IF EXISTS claude_model_mappings;
DROP TABLE IF EXISTS codex_model_mappings;
