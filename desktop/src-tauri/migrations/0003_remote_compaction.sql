-- Remote Compaction capability for Codex (V1 /responses/compact and V2
-- compaction_trigger). Stored per channel + protocol because whether an
-- upstream supports the dedicated compact endpoint or the trigger-item flow is
-- a provider/protocol-level property, not a model-level property.
--
-- Values:
--   0 = unknown / not probed
--   1 = supported
--   2 = unsupported
--
-- `remote_compaction_probed_at` records the last successful probe write;
-- `remote_compaction_last_error` keeps the most recent inconclusive reason for
-- admin visibility.

ALTER TABLE channel_protocols
  ADD COLUMN remote_compaction_v1_support INTEGER NOT NULL DEFAULT 0;

ALTER TABLE channel_protocols
  ADD COLUMN remote_compaction_v2_support INTEGER NOT NULL DEFAULT 0;

ALTER TABLE channel_protocols
  ADD COLUMN remote_compaction_probed_at TEXT;

ALTER TABLE channel_protocols
  ADD COLUMN remote_compaction_last_error TEXT;
