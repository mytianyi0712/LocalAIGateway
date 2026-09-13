use serde::Serialize;
use sqlx::FromRow;

use crate::remote_compaction::CompactionSupport;

/// A configured mapping from an entry model to an upstream protocol+model
/// (claudecode/codex entries).
#[derive(Clone)]
pub struct MappingTarget {
    pub entry: String,
    pub upstream_protocol: String,
    pub upstream_model: String,
}

#[derive(Clone, Debug, FromRow)]
pub struct Candidate {
    pub candidate_id: String,
    pub channel_id: String,
    pub channel_name: String,
    pub priority: i64,
    pub base_url: String,
    /// `providers.kind`: `command_code` opts into CLI identity headers.
    pub kind: Option<String>,
    pub api_key_encrypted: Vec<u8>,
    pub model_id: String,
    /// Raw `channel_protocols.remote_compaction_v1_support`:
    /// 0 unknown, 1 supported, 2 unsupported.
    pub remote_compaction_v1_support: i64,
    /// Raw `channel_protocols.remote_compaction_v2_support`:
    /// 0 unknown, 1 supported, 2 unsupported.
    pub remote_compaction_v2_support: i64,
}

impl Candidate {
    pub fn remote_compaction_v1(&self) -> CompactionSupport {
        CompactionSupport::from_db(Some(self.remote_compaction_v1_support))
    }

    pub fn remote_compaction_v2(&self) -> CompactionSupport {
        CompactionSupport::from_db(Some(self.remote_compaction_v2_support))
    }
}

#[derive(Clone, Debug, Serialize, FromRow)]
pub struct RoutableModel {
    pub id: String,
    pub display_name: String,
    pub created_at: String,
}

/// Primary proxy entrypoint per protocol, as advertised in the model catalog.
/// OpenAI Compatible only advertises `/v1/chat/completions` — embeddings and
/// completions are deliberately not guessed (see requirements.md 3.7).
/// Command Code is upstream-only (reached through claude/codex mappings), so
/// it has no entry endpoint and is dropped from the catalog.
pub fn protocol_main_endpoint(protocol: &str, model_id: &str) -> Option<String> {
    match protocol {
        "openai_compatible" => Some("/v1/chat/completions".to_owned()),
        "openai_responses" => Some("/v1/responses".to_owned()),
        "claude" => Some("/v1/messages".to_owned()),
        "gemini" => Some(format!("/v1beta/models/{model_id}:generateContent")),
        "command_code" => None,
        _ => None,
    }
}

/// All primary entrypoints the model can currently be routed through: the
/// distinct enabled route protocols with at least one live candidate, ordered
/// by `PROTOCOL_ORDER`. Mirrors the candidate-existence filter of
/// `list_routable_models` so endpoints only appear when the route is actually
/// callable.
/// Pure mapping used by `list_routable_model_endpoints` (and unit-tested):
/// unknown protocols are dropped, known ones keep their input order.
pub fn endpoints_for_protocols(protocols: &[&str], model_id: &str) -> Vec<String> {
    protocols
        .iter()
        .filter_map(|protocol| protocol_main_endpoint(protocol, model_id))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_endpoints_follow_protocol_order_and_drop_unknown() {
        let endpoints = endpoints_for_protocols(
            &[
                "openai_compatible",
                "openai_responses",
                "claude",
                "gemini",
                "bogus",
            ],
            "mimo-v2.5",
        );
        assert_eq!(
            endpoints,
            [
                "/v1/chat/completions",
                "/v1/responses",
                "/v1/messages",
                "/v1beta/models/mimo-v2.5:generateContent",
            ]
        );
    }

    #[test]
    fn gemini_endpoint_embeds_model_id() {
        assert_eq!(
            protocol_main_endpoint("gemini", "gpt-5.6-sol").as_deref(),
            Some("/v1beta/models/gpt-5.6-sol:generateContent")
        );
    }

    #[test]
    fn openai_compatible_advertises_chat_completions_only() {
        assert_eq!(
            protocol_main_endpoint("openai_compatible", "any-model").as_deref(),
            Some("/v1/chat/completions")
        );
        // Embeddings/completions are not guessed.
        assert_eq!(
            endpoints_for_protocols(&["bogus"], "any-model"),
            Vec::<String>::new()
        );
    }
}
