-- token_usage：用量快照，独立于请求日志持久化。
--
-- 本表刻意不建立指向 request_logs / request_attempts / channels 的外键：
-- 日志保留期清理与渠道删除永远不会级联进用量统计。每条记录按请求的真实开始时间
-- 归属，protocol / model_id 与在线写入路径一样从 request_logs 对应行快照而来。
--
-- occurred_at 统一规范化为 UTC 的单一表示（YYYY-MM-DDTHH:MM:SS.sssZ，固定毫秒），
-- 这样基于索引的 TEXT 区间比较不受历史日志行格式影响（历史行混用 Z / +00:00 偏移
-- 与不同的小数位宽）。查询边界必须先转换成同一表示再比较。
--
-- 幂等性：建表/建索引用 `IF NOT EXISTS`；回填以 attempt_id 为主键的 INSERT…SELECT
--         对已有数据的库重复执行会主键冲突，因此只会被 sqlx::migrate! 应用一次。
-- 回滚注意：没有 down 脚本，回滚只能靠数据库备份；已升级的库会校验本文件内容。

CREATE TABLE IF NOT EXISTS token_usage (
  attempt_id TEXT PRIMARY KEY NOT NULL,
  occurred_at TEXT NOT NULL,
  bucket TEXT NOT NULL,
  protocol TEXT NOT NULL,
  model_id TEXT NOT NULL,
  input_tokens INTEGER,
  cache_read_tokens INTEGER,
  cache_write_tokens INTEGER,
  cache_miss_input_tokens INTEGER,
  output_tokens INTEGER,
  first_token_ms INTEGER,
  duration_ms INTEGER
);

CREATE INDEX IF NOT EXISTS idx_token_usage_occurred ON token_usage(occurred_at);
CREATE INDEX IF NOT EXISTS idx_token_usage_bucket ON token_usage(bucket);

-- 从既有成功路径的用量记录（response_started = 1）回填：每条请求按其真实开始时间
-- （request_logs.started_at）归属，与在线写入路径的快照来源一致。strftime 的 %f
-- 会把所有历史时间戳（混用 Z / +00:00 偏移与不同小数位宽）规范化成固定毫秒精度的
-- UTC，与写入路径保持一致。
INSERT INTO token_usage (
  attempt_id, occurred_at, bucket, protocol, model_id,
  input_tokens, cache_read_tokens, cache_write_tokens, cache_miss_input_tokens,
  output_tokens, first_token_ms, duration_ms
)
SELECT
  a.id,
  strftime('%Y-%m-%dT%H:%M:%fZ', l.started_at),
  strftime('%Y-%m-%dT%H:00:00Z', l.started_at),
  l.protocol,
  COALESCE(l.model_id, ''),
  a.input_tokens,
  a.cache_read_tokens,
  a.cache_write_tokens,
  a.cache_miss_input_tokens,
  a.output_tokens,
  a.first_token_ms,
  a.duration_ms
FROM request_attempts a
JOIN request_logs l ON l.id = a.request_id
WHERE a.response_started = 1;
