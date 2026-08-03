CREATE TABLE IF NOT EXISTS providers (
  id TEXT PRIMARY KEY NOT NULL,
  name TEXT NOT NULL UNIQUE,
  base_url TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS channels (
  id TEXT PRIMARY KEY NOT NULL,
  provider_id TEXT NOT NULL REFERENCES providers(id),
  name TEXT NOT NULL,
  protocol TEXT NOT NULL,
  api_key_encrypted BLOB NOT NULL,
  api_key_hint TEXT NOT NULL,
  manual_enabled INTEGER NOT NULL DEFAULT 1,
  health_check_model_id TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(provider_id, name)
);

CREATE TABLE IF NOT EXISTS channel_protocols (
  channel_id TEXT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
  protocol TEXT NOT NULL,
  PRIMARY KEY(channel_id, protocol)
);

CREATE TABLE IF NOT EXISTS channel_health (
  channel_id TEXT PRIMARY KEY NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
  state TEXT NOT NULL DEFAULT 'active',
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  disabled_until TEXT,
  last_success_at TEXT,
  last_failure_at TEXT,
  last_error_kind TEXT,
  last_status_code INTEGER,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS channel_models (
  id TEXT PRIMARY KEY NOT NULL,
  channel_id TEXT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
  model_id TEXT NOT NULL,
  display_name TEXT,
  source TEXT NOT NULL DEFAULT 'discovered',
  available INTEGER NOT NULL DEFAULT 1,
  metadata_json JSON,
  first_seen_at TEXT NOT NULL,
  last_seen_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(channel_id, model_id)
);

CREATE TABLE IF NOT EXISTS channel_model_protocols (
  channel_model_id TEXT NOT NULL REFERENCES channel_models(id) ON DELETE CASCADE,
  protocol TEXT NOT NULL,
  PRIMARY KEY(channel_model_id, protocol)
);

CREATE TABLE IF NOT EXISTS model_routes (
  id TEXT PRIMARY KEY NOT NULL,
  protocol TEXT NOT NULL,
  requested_model_id TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(protocol, requested_model_id)
);

CREATE TABLE IF NOT EXISTS capability_profiles (
  id TEXT PRIMARY KEY NOT NULL,
  name TEXT NOT NULL UNIQUE,
  description TEXT,
  context_window INTEGER,
  max_tokens INTEGER,
  supports_image_input INTEGER,
  reasoning INTEGER,
  thinking_level_map JSON,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS model_caps (
  requested_model_id TEXT PRIMARY KEY NOT NULL,
  context_window INTEGER,
  max_tokens INTEGER,
  supports_image_input INTEGER,
  reasoning INTEGER,
  thinking_level_map JSON,
  cost_input REAL,
  cost_output REAL,
  cost_cache_read REAL,
  cost_cache_write REAL,
  source TEXT NOT NULL DEFAULT 'auto',
  profile_id TEXT REFERENCES capability_profiles(id) ON DELETE SET NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS route_candidates (
  id TEXT PRIMARY KEY NOT NULL,
  route_id TEXT NOT NULL REFERENCES model_routes(id) ON DELETE CASCADE,
  channel_model_id TEXT NOT NULL REFERENCES channel_models(id) ON DELETE RESTRICT,
  priority INTEGER NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(route_id, channel_model_id),
  UNIQUE(route_id, priority)
);

CREATE TABLE IF NOT EXISTS request_logs (
  id TEXT PRIMARY KEY NOT NULL,
  protocol TEXT NOT NULL,
  model_id TEXT,
  endpoint TEXT NOT NULL,
  stream INTEGER,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  total_duration_ms INTEGER,
  final_status_code INTEGER,
  outcome TEXT NOT NULL DEFAULT 'pending',
  attempt_count INTEGER NOT NULL DEFAULT 0,
  final_channel_id TEXT REFERENCES channels(id) ON DELETE SET NULL,
  request_bytes INTEGER,
  response_bytes INTEGER
);

CREATE TABLE IF NOT EXISTS request_attempts (
  id TEXT PRIMARY KEY NOT NULL,
  request_id TEXT NOT NULL REFERENCES request_logs(id) ON DELETE CASCADE,
  channel_id TEXT REFERENCES channels(id) ON DELETE SET NULL,
  channel_name TEXT NOT NULL,
  attempt_no INTEGER NOT NULL,
  priority_snapshot INTEGER NOT NULL,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  status_code INTEGER,
  outcome TEXT NOT NULL,
  error_kind TEXT,
  failover_eligible INTEGER NOT NULL DEFAULT 0,
  response_started INTEGER NOT NULL DEFAULT 0,
  first_byte_ms INTEGER,
  first_token_ms INTEGER,
  duration_ms INTEGER,
  input_tokens INTEGER,
  cache_read_tokens INTEGER,
  cache_write_tokens INTEGER,
  cache_miss_input_tokens INTEGER,
  output_tokens INTEGER,
  tps REAL,
  raw_usage_json JSON,
  response_bytes INTEGER,
  upstream_protocol TEXT,
  upstream_model_id TEXT,
  UNIQUE(request_id, attempt_no)
);

CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY NOT NULL,
  value_json JSON NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS discovery_runs (
  id TEXT PRIMARY KEY NOT NULL,
  channel_id TEXT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
  trigger TEXT NOT NULL DEFAULT 'manual',
  started_at TEXT NOT NULL,
  finished_at TEXT,
  success INTEGER,
  model_count INTEGER,
  status_code INTEGER,
  error_kind TEXT
);

CREATE TABLE IF NOT EXISTS health_probe_logs (
  id TEXT PRIMARY KEY NOT NULL,
  channel_id TEXT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
  model_id TEXT NOT NULL,
  started_at TEXT NOT NULL,
  duration_ms INTEGER,
  success INTEGER NOT NULL,
  status_code INTEGER,
  error_kind TEXT,
  next_probe_at TEXT
);

CREATE TABLE IF NOT EXISTS claude_model_mappings (
  id TEXT PRIMARY KEY NOT NULL,
  claude_model_id TEXT NOT NULL UNIQUE,
  display_name TEXT,
  upstream_protocol TEXT NOT NULL,
  upstream_model_id TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS codex_model_mappings (
  id TEXT PRIMARY KEY NOT NULL,
  codex_model_id TEXT NOT NULL UNIQUE,
  display_name TEXT,
  upstream_protocol TEXT NOT NULL,
  upstream_model_id TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_channel_models_model ON channel_models(model_id);
CREATE INDEX IF NOT EXISTS idx_model_routes_lookup ON model_routes(protocol, requested_model_id, enabled);
CREATE INDEX IF NOT EXISTS idx_route_candidates_route ON route_candidates(route_id, priority);
CREATE INDEX IF NOT EXISTS idx_request_logs_started ON request_logs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_request_attempts_request ON request_attempts(request_id, attempt_no);
