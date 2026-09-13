-- Command Code Go integration (route B).
--
-- `providers.kind` is an explicit identity marker. Command Code identity
-- headers (session, fingerprint, CLI version) are injected ONLY when
-- `kind = 'command_code'` — never by sniffing `base_url`, so a self-hosted
-- bridge or any third-party upstream can never receive the fingerprint
-- (plan decision 2). Existing providers keep `kind = NULL` and are never
-- treated as Command Code, even when their base_url points at
-- api.commandcode.ai.
--
-- Everything else the integration needs (fingerprint, session, transport
-- memory, init throttle, CLI version) lives in the `settings` KV table, so
-- no further schema change is required.

ALTER TABLE providers ADD COLUMN kind TEXT;

CREATE INDEX IF NOT EXISTS idx_providers_kind ON providers(kind);
