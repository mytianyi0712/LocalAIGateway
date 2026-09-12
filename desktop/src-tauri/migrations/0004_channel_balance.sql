-- Channel balance / usage query sidecar.
--
-- The balance capability is OFF by default: a channel without a row in
-- `channel_balance_configs` never produces a single upstream balance
-- request. Users opt in per channel by picking one of the five adapters and
-- enabling the switch.
--
-- `channel_balance_configs` stores the user's adapter choice plus the
-- optional custom request template. `channel_balance_snapshots` keeps only
-- the latest reading per channel (PK = channel_id): balance is a "current
-- value" by nature.
--
-- Security: raw upstream response bodies are never stored, access tokens
-- only live encrypted in `balance_token_encrypted` (Fernet); snapshots keep
-- only normalized, non-sensitive values.

CREATE TABLE channel_balance_configs (
  channel_id TEXT PRIMARY KEY REFERENCES channels(id) ON DELETE CASCADE,
  adapter TEXT NOT NULL,                     -- newapi|sub2api|opencode_go|deepseek|custom
  enabled INTEGER NOT NULL DEFAULT 0,        -- the only switch for manual/background refresh
  method TEXT NOT NULL DEFAULT 'GET',        -- custom only; built-in adapters are always GET
  path TEXT,                                 -- custom required; built-ins store their preset path
  auth TEXT NOT NULL DEFAULT 'bearer',       -- bearer|none
  headers_json TEXT,                         -- {"X-Foo":"${token}"}; at most 8 entries
  body_json TEXT,                            -- custom request body template
  mapping_json TEXT,                         -- {"remaining":"$.data.balance", ...}
  balance_token_encrypted BLOB,              -- optional dedicated token (ciphertext)
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
  detail_json TEXT,                          -- today usage / expiry etc. (non-sensitive)
  error_kind TEXT,
  status_code INTEGER,
  duration_ms INTEGER,
  checked_at TEXT NOT NULL
);
