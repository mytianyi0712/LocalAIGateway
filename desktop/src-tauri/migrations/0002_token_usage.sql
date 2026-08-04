-- Token usage snapshots, persisted independently of request logs.
--
-- This table intentionally has NO foreign keys to request_logs /
-- request_attempts / channels: log retention cleanup and channel deletion can
-- never cascade into token statistics. Each request is attributed to its
-- actual start time; protocol / model_id are snapshotted from the request log
-- row the same way the live write path does.
--
-- occurred_at is normalized to a single UTC representation
-- (YYYY-MM-DDTHH:MM:SS.sssZ, fixed milliseconds) so TEXT range comparisons on
-- the index are exact regardless of the format used in legacy log rows
-- (mixed Z / +00:00 offsets and fractional widths). Query bounds must be
-- converted to the same representation before comparing.

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

-- Backfill from existing success-path usage records (response_started = 1),
-- attributing each request to its actual start time (request_logs.started_at),
-- the same source the live write path snapshots from. strftime with %f
-- normalizes every legacy timestamp (mixed Z / +00:00 offsets and fractional
-- widths) to UTC with fixed millisecond precision, matching the write path.
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
