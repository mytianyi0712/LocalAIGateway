-- Command Code 渠道可以承载网关能转换成 `command_code` 的每一种入口协议；
-- 这里为已存在的目录行补上绑定，使「模型路由」无需等下一次发现就能选到该渠道。
-- 协议集合与 `protocol::model_binding_protocols` 一致
--（openai_compatible / openai_responses / claude），且只针对 kind='command_code' 的供应商。
--
-- 幂等性：`INSERT OR IGNORE`，对已有数据的库重复执行不会新增行
--（db.rs 的测试会二次执行本语句以验证这一点）。
-- 回滚注意：没有 down 脚本，回滚只能靠数据库备份；已升级的库会校验本文件内容。
INSERT OR IGNORE INTO channel_model_protocols(channel_model_id, protocol)
SELECT cm.id, entry.protocol
  FROM channel_models cm
  JOIN channels c ON c.id = cm.channel_id
  JOIN providers p ON p.id = c.provider_id
  JOIN (SELECT 'openai_compatible' AS protocol
        UNION ALL SELECT 'openai_responses'
        UNION ALL SELECT 'claude') entry
 WHERE p.kind = 'command_code';
