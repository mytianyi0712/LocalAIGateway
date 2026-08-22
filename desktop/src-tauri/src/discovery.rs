//! Model discovery — port of backend/app/services/discovery.py.
//!
//! Fixes over the original port: per-protocol pagination (Claude `after_id`,
//! Gemini `pageToken`), per-protocol metadata merge inside
//! `channel_models.metadata_json` (instead of overwriting the whole dict),
//! stale protocol-binding cleanup with `available` recomputation, and
//! discovery runs that record `status_code` / aggregated `error_kind`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context as _, Result, bail};
use serde_json::Value;
use sqlx::Row;
use tokio::time::{Duration, timeout};

use crate::{
    application::Context,
    crypto::SecretStore,
    db::Database,
    ports::{ChannelRepository, Clock, UpstreamClient},
    protocol, remote_compaction,
    settings,
};

/// Immutable outcome of the network phase (P2-5): nothing is written to the
/// DB while upstreams are being called. The caller applies the snapshot to
/// the catalog — models, bindings, `available` recomputation and the run
/// terminal state — in ONE transaction, so a mid-write failure can never
/// leave a half-applied discovery.
pub struct DiscoverySnapshot {
    /// Protocols whose catalog fetch succeeded.
    succeeded: HashSet<String>,
    /// Per-protocol model catalogs (model_id -> item).
    seen_by_protocol: HashMap<String, HashMap<String, Value>>,
    /// Distinct model ids seen across all succeeded protocols.
    model_count: i64,
    /// Last upstream status code observed (any protocol).
    status_code: Option<i64>,
    /// Aggregated failure detail when at least one protocol failed.
    error_kind: Option<String>,
    /// Remote-compaction capability probe result for `openai_responses`.
    remote_compaction: Option<RemoteCompactionProbe>,
}

/// Outcome of probing one channel's remote-compaction endpoints.
#[derive(Debug, Clone)]
pub struct RemoteCompactionProbe {
    pub v1: ProbeVerdict,
    pub v2: ProbeVerdict,
    pub probed_at: String,
}

/// A remote-compaction probe verdict. `Inconclusive` preserves the previously
/// persisted value and only adds a discovery-run diagnostic.
#[derive(Debug, Clone)]
pub enum ProbeVerdict {
    Supported,
    Unsupported,
    Inconclusive(String),
}

/// Discovery service (P2-1): network phase + atomic apply, depending only on
/// ports and storage. Handlers and the maintenance supervisor reach it
/// through [`Context::discovery`].
pub struct DiscoveryService {
    db: Database,
    secrets: SecretStore,
    http: Arc<dyn UpstreamClient>,
    channels: Arc<dyn ChannelRepository>,
    clock: Arc<dyn Clock>,
    background: Arc<crate::infrastructure::RuntimeSupervisor>,
    limits: Arc<crate::runtime::RuntimeLimits>,
}

impl DiscoveryService {
    pub fn new(
        db: Database,
        secrets: SecretStore,
        http: Arc<dyn UpstreamClient>,
        channels: Arc<dyn ChannelRepository>,
        clock: Arc<dyn Clock>,
        background: Arc<crate::infrastructure::RuntimeSupervisor>,
        limits: Arc<crate::runtime::RuntimeLimits>,
    ) -> Arc<Self> {
        Arc::new(Self {
            db,
            secrets,
            http,
            channels,
            clock,
            background,
            limits,
        })
    }

    pub async fn queue(&self, state: &Context, channel_id: String) -> Result<String> {
        self.queue_with_trigger(state, channel_id, "manual").await
    }

    /// Used by the maintenance supervisor for the scheduled re-discovery cycle.
    pub async fn queue_scheduled(&self, state: &Context, channel_id: String) -> Result<String> {
        self.queue_with_trigger(state, channel_id, "scheduled")
            .await
    }

    async fn queue_with_trigger(
        &self,
        state: &Context,
        channel_id: String,
        trigger: &str,
    ) -> Result<String> {
        let run_id = uuid::Uuid::new_v4().to_string();
        let time = self.clock.now_utc().to_rfc3339();
        sqlx::query("INSERT INTO discovery_runs(id,channel_id,trigger,started_at) VALUES(?,?,?,?)")
            .bind(&run_id)
            .bind(&channel_id)
            .bind(trigger)
            .bind(&time)
            .execute(self.db.pool())
            .await?;
        let task_run_id = run_id.clone();
        let task_channel_id = channel_id.clone();
        let discovery_timeout = self.limits.discovery_timeout;
        let background = Arc::clone(&self.background);
        let service = Arc::new(DiscoveryService {
            db: self.db.clone(),
            secrets: self.secrets.clone(),
            http: Arc::clone(&self.http),
            channels: Arc::clone(&self.channels),
            clock: Arc::clone(&self.clock),
            background: Arc::clone(&self.background),
            limits: Arc::clone(&self.limits),
        });
        let state = state.clone();
        if background
        .spawn_tracked("discovery_run", async move {
            let result = timeout(discovery_timeout, service.discover(&state, &task_channel_id))
                .await;
            let finished = service.clock.now_utc().to_rfc3339();
            let mut task_failed = false;
            match result {
                Ok(Ok(snapshot)) => {
                    // P2-5: catalog + run terminal commit atomically. A
                    // persistence failure must NOT end silently — the UI
                    // waits on this run (P2-8).
                    if let Err(error) = service
                        .apply_discovery(&task_channel_id, &snapshot, &task_run_id, &finished)
                        .await
                    {
                        tracing::error!(
                            run_id = %task_run_id,
                            channel_id = %task_channel_id,
                            %error,
                            "discovery persistence failed"
                        );
                        task_failed = true;
                        if let Err(write_error) = service
                            .record_run_failure(&task_run_id, &finished, &format!("persistence_error: {error}"))
                            .await
                        {
                            tracing::error!(
                                run_id = %task_run_id,
                                %write_error,
                                "failed to record discovery run failure"
                            );
                        }
                    }
                }
                Ok(Err(error)) => {
                    tracing::warn!(channel_id = %task_channel_id, %error, "model discovery failed");
                    if let Err(write_error) = service
                        .record_run_failure(&task_run_id, &finished, &error.to_string())
                        .await
                    {
                        tracing::error!(
                            run_id = %task_run_id,
                            %write_error,
                            "failed to record discovery run failure"
                        );
                        task_failed = true;
                    }
                }
                Err(_) => {
                    tracing::warn!(channel_id = %task_channel_id, "model discovery timed out");
                    if let Err(write_error) = service
                        .record_run_failure(&task_run_id, &finished, "模型探测超时")
                        .await
                    {
                        tracing::error!(
                            run_id = %task_run_id,
                            %write_error,
                            "failed to record discovery run failure"
                        );
                        task_failed = true;
                    }
                }
            }
            if task_failed {
                crate::infrastructure::TaskOutcome::Failed
            } else {
                crate::infrastructure::TaskOutcome::Success
            }
        })
    .await
    .is_err()
    {
        // Shutdown already started: the run row stays pending; the UI will
        // surface it as a persistence_error (P2-8).
        tracing::warn!(run_id = %run_id, channel_id = %channel_id, "discovery not started: runtime shutting down");
    }
        Ok(run_id)
    }

    /// Fetch one model catalog with per-protocol pagination, up to 50 pages.
    /// Returns (model_id -> item, last status code).
    async fn fetch_models(
        &self,
        state: &Context,
        base_url: &str,
        protocol_name: &str,
        api_key: &str,
    ) -> Result<(HashMap<String, Value>, i64)> {
        let mut models: HashMap<String, Value> = HashMap::new();
        let runtime = settings::runtime_settings(state).await?;
        let mut url = protocol::upstream_url(
            base_url,
            protocol::discovery_path(protocol_name),
            None,
            protocol_name,
        )?;
        let mut status_code: i64 = 0;
        let mut visited: HashSet<String> = HashSet::new();
        for _ in 0..self.limits.discovery_max_pages {
            if visited.contains(url.as_str()) {
                break;
            }
            visited.insert(url.to_string());
            let headers =
                protocol::outbound_headers(&axum::http::HeaderMap::new(), protocol_name, api_key)?;
            let response = state
                .http
                .send(crate::ports::UpstreamRequest {
                    url: url.clone(),
                    headers,
                    body: None,
                    connect_timeout: std::time::Duration::from_secs(
                        runtime.connect_timeout_seconds.max(1) as u64,
                    ),
                    deadline: self.limits.probe_timeout,
                })
                .await
                .map_err(|error| {
                    anyhow::anyhow!("{protocol_name} discovery transport: {error:?}")
                })?;
            status_code = response.status.as_u16() as i64;
            if !response.status.is_success() {
                bail!("{protocol_name} discovery returned {status_code}");
            }
            // Catalogs are bounded like upstream responses (P1-1).
            let (body, truncated) = response
                .body
                .read_capped((self.limits.discovery_max_pages * 1024 * 1024).max(16 * 1024 * 1024))
                .await;
            if truncated {
                bail!("{protocol_name} discovery catalog too large");
            }
            let (items, next) = protocol::parse_catalog(protocol_name, &body, &url)?;
            for item in items {
                if let Some(model_id) = item.get("id").and_then(Value::as_str) {
                    models.insert(model_id.to_owned(), item);
                }
            }
            let Some(next_url) = next else { break };
            url = next_url;
        }
        Ok((models, status_code))
    }

    /// Run a discovery for one channel: network + parse ONLY (P2-5). Returns the
    /// immutable snapshot for the caller to apply atomically.
    async fn discover(&self, state: &Context, channel_id: &str) -> Result<DiscoverySnapshot> {
        let channel = state
            .channels
            .load_channel(channel_id)
            .await?
            .context("Channel not found")?;
        let primary = channel.protocol;
        let configured: Vec<String> = sqlx::query_scalar(
            "SELECT protocol FROM channel_protocols WHERE channel_id=? ORDER BY protocol",
        )
        .bind(channel_id)
        .fetch_all(self.db.pool())
        .await?;
        let protocols = if configured.is_empty() {
            vec![primary]
        } else {
            configured
        };
        let api_key = self.secrets.decrypt(&channel.api_key_encrypted)?;
        let base_url = channel.base_url;

        // Group protocols sharing one discovery URL + auth (the openai family
        // shares /v1/models with the same Bearer header, mirroring Python).
        let mut groups: Vec<(String, String, Vec<String>)> = Vec::new();
        for protocol_name in &protocols {
            let url = protocol::upstream_url(
                &base_url,
                protocol::discovery_path(protocol_name),
                None,
                protocol_name,
            )?;
            let headers =
                protocol::outbound_headers(&axum::http::HeaderMap::new(), protocol_name, &api_key)?;
            let mut header_key = String::new();
            for (name, value) in headers.iter() {
                header_key.push_str(&format!("{}:{};", name, value.to_str().unwrap_or("")));
            }
            let url_key = url.to_string();
            match groups.iter_mut().find(|(group_url, group_headers, _)| {
                *group_url == url_key && *group_headers == header_key
            }) {
                Some((_, _, group_protocols)) => group_protocols.push(protocol_name.clone()),
                None => groups.push((url_key, header_key, vec![protocol_name.clone()])),
            }
        }

        let mut succeeded: HashSet<String> = HashSet::new();
        let mut failed: Vec<String> = Vec::new();
        let mut seen_by_protocol: HashMap<String, HashMap<String, Value>> = HashMap::new();
        let mut last_status_code: Option<i64> = None;
        for (_, _, group_protocols) in &groups {
            let primary_protocol = &group_protocols[0];
            match timeout(
                Duration::from_secs(120),
                self.fetch_models(state, &base_url, primary_protocol, &api_key),
            )
            .await
            {
                Ok(Ok((models, status_code))) => {
                    last_status_code = Some(status_code);
                    for protocol_name in group_protocols {
                        succeeded.insert(protocol_name.clone());
                        seen_by_protocol.insert(protocol_name.clone(), models.clone());
                    }
                }
                Ok(Err(error)) => {
                    tracing::warn!(channel_id, %error, "model discovery failed");
                    for protocol_name in group_protocols {
                        failed.push(protocol_name.clone());
                    }
                }
                Err(_) => {
                    tracing::warn!(channel_id, "model discovery timed out");
                    for protocol_name in group_protocols {
                        failed.push(protocol_name.clone());
                    }
                }
            }
        }

        let model_count = seen_by_protocol
            .values()
            .flat_map(|models| models.keys().cloned().collect::<Vec<_>>())
            .collect::<HashSet<_>>()
            .len() as i64;
        let mut remote_compaction_probe = None;
        if succeeded.contains("openai_responses")
            && let Some(models) = seen_by_protocol.get("openai_responses")
            && let Some(model_id) = models.keys().next()
        {
            remote_compaction_probe = Some(
                self.probe_remote_compaction(
                    state,
                    &base_url,
                    &api_key,
                    model_id,
                    channel_id,
                )
                .await,
            );
        }
        let mut error_kind = if failed.is_empty() {
            None
        } else {
            Some(format!("protocol_discovery_failed:{}", failed.join(",")))
        };
        if let Some(probe) = &remote_compaction_probe {
            let mut diagnostics = Vec::new();
            if let ProbeVerdict::Inconclusive(reason) = &probe.v1 {
                diagnostics.push(format!("remote_compaction_v1_inconclusive:{reason}"));
            }
            if let ProbeVerdict::Inconclusive(reason) = &probe.v2 {
                diagnostics.push(format!("remote_compaction_v2_inconclusive:{reason}"));
            }
            if !diagnostics.is_empty() {
                let suffix = diagnostics.join(",");
                error_kind = Some(match error_kind.take() {
                    Some(prefix) => format!("{prefix};{suffix}"),
                    None => suffix,
                });
            }
        }
        Ok(DiscoverySnapshot {
            succeeded,
            seen_by_protocol,
            model_count,
            status_code: last_status_code,
            error_kind,
            remote_compaction: remote_compaction_probe,
        })
    }

    /// Probes both remote-compaction protocols on an `openai_responses`
    /// channel. The probe never fails discovery: inconclusive outcomes are
    /// recorded as diagnostics and preserve previously stored capability.
    async fn probe_remote_compaction(
        &self,
        state: &Context,
        base_url: &str,
        api_key: &str,
        model_id: &str,
        channel_id: &str,
    ) -> RemoteCompactionProbe {
        let probed_at = self.clock.now_utc().to_rfc3339();
        let v1 = self
            .probe_remote_compaction_v1(state, base_url, api_key, model_id, channel_id)
            .await;
        let v2 = self
            .probe_remote_compaction_v2(state, base_url, api_key, model_id, channel_id)
            .await;
        RemoteCompactionProbe {
            v1,
            v2,
            probed_at,
        }
    }

    async fn probe_remote_compaction_v1(
        &self,
        state: &Context,
        base_url: &str,
        api_key: &str,
        model_id: &str,
        channel_id: &str,
    ) -> ProbeVerdict {
        let _ = channel_id;
        let url = match protocol::upstream_url(
            base_url,
            "/v1/responses/compact",
            None,
            "openai_responses",
        ) {
            Ok(url) => url,
            Err(error) => return ProbeVerdict::Inconclusive(format!("url:{error}")),
        };
        let mut headers = match protocol::outbound_headers(
            &axum::http::HeaderMap::new(),
            "openai_responses",
            api_key,
        ) {
            Ok(headers) => headers,
            Err(error) => return ProbeVerdict::Inconclusive(format!("headers:{error}")),
        };
        headers.insert(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("application/json"),
        );
        let payload = match serde_json::to_vec(&remote_compaction::v1_probe_body(model_id)) {
            Ok(payload) => payload,
            Err(error) => return ProbeVerdict::Inconclusive(format!("body:{error}")),
        };
        let response = match state
            .http
            .send(crate::ports::UpstreamRequest {
                url,
                headers,
                body: Some(bytes::Bytes::from(payload)),
                connect_timeout: std::time::Duration::from_secs(10),
                deadline: self.limits.probe_timeout,
            })
            .await
        {
            Ok(response) => response,
            Err(error) => return ProbeVerdict::Inconclusive(format!("transport:{error:?}")),
        };
        let status = response.status.as_u16();
        let (body, _truncated) = match timeout(self.limits.probe_timeout, response.body.read_capped(self.limits.error_body_max)).await {
            Ok(result) => result,
            Err(_) => return ProbeVerdict::Inconclusive("timeout".into()),
        };
        if response.status.is_success() && remote_compaction::validate_v1_response(&body) {
            ProbeVerdict::Supported
        } else if matches!(status, 404 | 405 | 501) {
            ProbeVerdict::Unsupported
        } else {
            ProbeVerdict::Inconclusive(format!("status:{status}"))
        }
    }

    async fn probe_remote_compaction_v2(
        &self,
        state: &Context,
        base_url: &str,
        api_key: &str,
        model_id: &str,
        channel_id: &str,
    ) -> ProbeVerdict {
        let _ = channel_id;
        let url = match protocol::upstream_url(base_url, "/v1/responses", None, "openai_responses")
        {
            Ok(url) => url,
            Err(error) => return ProbeVerdict::Inconclusive(format!("url:{error}")),
        };
        let mut headers = match protocol::outbound_headers(
            &axum::http::HeaderMap::new(),
            "openai_responses",
            api_key,
        ) {
            Ok(headers) => headers,
            Err(error) => return ProbeVerdict::Inconclusive(format!("headers:{error}")),
        };
        headers.insert(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("application/json"),
        );
        headers.insert(
            axum::http::header::ACCEPT,
            axum::http::HeaderValue::from_static("text/event-stream"),
        );
        headers.insert(
            axum::http::HeaderName::from_static("x-codex-beta-features"),
            axum::http::HeaderValue::from_static("remote_compaction_v2"),
        );
        let payload = match serde_json::to_vec(&remote_compaction::v2_probe_body(model_id)) {
            Ok(payload) => payload,
            Err(error) => return ProbeVerdict::Inconclusive(format!("body:{error}")),
        };
        let response = match state
            .http
            .send(crate::ports::UpstreamRequest {
                url,
                headers,
                body: Some(bytes::Bytes::from(payload)),
                connect_timeout: std::time::Duration::from_secs(10),
                deadline: self.limits.probe_timeout,
            })
            .await
        {
            Ok(response) => response,
            Err(error) => return ProbeVerdict::Inconclusive(format!("transport:{error:?}")),
        };
        let status = response.status.as_u16();
        let (body, _truncated) = match timeout(
            self.limits.probe_timeout,
            response
                .body
                .read_capped((self.limits.error_body_max * 4).max(4 * 1024 * 1024)),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => return ProbeVerdict::Inconclusive("timeout".into()),
        };
        let mut validator = remote_compaction::CompactionV2Validator::new();
        validator.feed(&body);
        let verdict = validator.finish();
        if response.status.is_success() && verdict.is_ok() {
            ProbeVerdict::Supported
        } else if matches!(status, 404 | 405 | 501) {
            ProbeVerdict::Unsupported
        } else if response.status.is_success()
            && matches!(
                verdict,
                Err(remote_compaction::CompactionV2Error::NotCompaction)
            )
        {
            ProbeVerdict::Unsupported
        } else {
            ProbeVerdict::Inconclusive(format!(
                "status:{status};verdict:{:?}",
                verdict.err().map(|error| error.to_string())
            ))
        }
    }

    /// Applies a discovery snapshot to the catalog in ONE transaction (P2-5):
    /// model upserts, protocol bindings, stale-binding pruning, `available`
    /// recomputation, and the run terminal self. Either all of it lands, or
    /// none of it — a failed write rolls back to the previous catalog instead
    /// of leaving new models without bindings or a run stuck in `pending`.
    async fn apply_discovery(
        &self,
        channel_id: &str,
        snapshot: &DiscoverySnapshot,
        run_id: &str,
        finished_at: &str,
    ) -> Result<()> {
        let mut tx = self.db.pool().begin().await?;
        let existing_rows = sqlx::query(
        "SELECT id, model_id, display_name, source, metadata_json FROM channel_models WHERE channel_id=?",
    )
    .bind(channel_id)
    .fetch_all(&mut *tx)
    .await?;
        let mut existing: HashMap<String, (String, Option<String>, bool, Option<String>)> =
            HashMap::new();
        for row in existing_rows {
            existing.insert(
                row.try_get("model_id")?,
                (
                    row.try_get("id")?,
                    row.try_get("display_name")?,
                    row.try_get::<String, _>("source")? == "manual",
                    row.try_get("metadata_json")?,
                ),
            );
        }
        let binding_rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT cm.model_id, cmp.protocol FROM channel_model_protocols cmp \
         JOIN channel_models cm ON cm.id = cmp.channel_model_id WHERE cm.channel_id=?",
        )
        .bind(channel_id)
        .fetch_all(&mut *tx)
        .await?;
        let mut protocol_sets: HashMap<String, HashSet<String>> = HashMap::new();
        for (model_id, protocol_name) in binding_rows {
            protocol_sets
                .entry(model_id)
                .or_default()
                .insert(protocol_name);
        }

        let now = self.clock.now_utc().to_rfc3339();
        for protocol_name in &snapshot.succeeded {
            let Some(models) = snapshot.seen_by_protocol.get(protocol_name) else {
                continue;
            };
            for (model_id, item) in models {
                let mut metadata: serde_json::Map<String, Value> = existing
                    .get(model_id)
                    .and_then(|(_, _, _, metadata)| metadata.clone())
                    .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
                    .and_then(|value| value.as_object().cloned())
                    .unwrap_or_default();
                let mut protocol_meta = item.clone();
                if let Some(object) = protocol_meta.as_object_mut() {
                    object.remove("id");
                }
                metadata.insert(protocol_name.clone(), protocol_meta);
                let display_name = item
                    .get("display_name")
                    .or_else(|| item.get("displayName"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .unwrap_or_else(|| model_id.clone());
                let metadata_json = serde_json::to_string(&Value::Object(metadata))?;
                match existing.get_mut(model_id) {
                    Some((row_id, stored_display, _, _)) => {
                        let updated_display = if stored_display
                            .as_deref()
                            .is_some_and(|value| !value.is_empty())
                        {
                            stored_display.clone()
                        } else {
                            Some(display_name)
                        };
                        sqlx::query(
                        "UPDATE channel_models SET display_name=?,available=1,metadata_json=?,last_seen_at=?,updated_at=? WHERE id=?",
                    )
                    .bind(updated_display)
                    .bind(&metadata_json)
                    .bind(&now)
                    .bind(&now)
                    .bind(row_id.as_str())
                    .execute(&mut *tx)
                    .await?;
                    }
                    None => {
                        let row_id = uuid::Uuid::new_v4().to_string();
                        sqlx::query(
                        "INSERT INTO channel_models(id,channel_id,model_id,display_name,source,available,metadata_json,first_seen_at,last_seen_at,created_at,updated_at) VALUES(?,?,?,?, 'discovered',1,?,?,?,?,?)",
                    )
                    .bind(&row_id)
                    .bind(channel_id)
                    .bind(model_id)
                    .bind(&display_name)
                    .bind(&metadata_json)
                    .bind(&now)
                    .bind(&now)
                    .bind(&now)
                    .bind(&now)
                    .execute(&mut *tx)
                    .await?;
                        existing.insert(
                            model_id.clone(),
                            (
                                row_id,
                                Some(display_name),
                                false,
                                Some(metadata_json.clone()),
                            ),
                        );
                    }
                }
                let row_id = existing
                    .get(model_id)
                    .map(|(id, _, _, _)| id.clone())
                    .unwrap_or_default();
                sqlx::query("INSERT OR IGNORE INTO channel_model_protocols(channel_model_id,protocol) VALUES(?,?)")
                .bind(&row_id)
                .bind(protocol_name)
                .execute(&mut *tx)
                .await?;
                protocol_sets
                    .entry(model_id.clone())
                    .or_default()
                    .insert(protocol_name.clone());
            }
        }

        // Prune bindings for protocols that no longer list the model, and
        // recompute `available` — a model with no remaining protocols is hidden.
        for (model_id, (row_id, _, manual, _)) in &existing {
            if *manual {
                continue;
            }
            let mut stale: Vec<String> = Vec::new();
            if let Some(bound) = protocol_sets.get(model_id) {
                for protocol_name in &snapshot.succeeded {
                    let still_seen = snapshot
                        .seen_by_protocol
                        .get(protocol_name)
                        .is_some_and(|models| models.contains_key(model_id));
                    if bound.contains(protocol_name) && !still_seen {
                        stale.push(protocol_name.clone());
                    }
                }
            }
            for protocol_name in &stale {
                sqlx::query(
                    "DELETE FROM channel_model_protocols WHERE channel_model_id=? AND protocol=?",
                )
                .bind(row_id)
                .bind(protocol_name)
                .execute(&mut *tx)
                .await?;
            }
            let remaining = protocol_sets
                .get(model_id)
                .map(|bound| {
                    bound.len() - stale.iter().filter(|item| bound.contains(*item)).count()
                })
                .unwrap_or(0);
            if !stale.is_empty() || remaining == 0 {
                sqlx::query("UPDATE channel_models SET available=?,updated_at=? WHERE id=?")
                    .bind(remaining > 0)
                    .bind(&now)
                    .bind(row_id)
                    .execute(&mut *tx)
                    .await?;
            }
        }

        // Persist remote-compaction probe results in the same transaction.
        // Inconclusive verdicts leave existing capability values untouched.
        if let Some(probe) = &snapshot.remote_compaction {
            let v1 = match &probe.v1 {
                ProbeVerdict::Supported => Some(1i64),
                ProbeVerdict::Unsupported => Some(2i64),
                ProbeVerdict::Inconclusive(_) => None,
            };
            let v2 = match &probe.v2 {
                ProbeVerdict::Supported => Some(1i64),
                ProbeVerdict::Unsupported => Some(2i64),
                ProbeVerdict::Inconclusive(_) => None,
            };
            if v1.is_some() || v2.is_some() {
                sqlx::query(
                    "UPDATE channel_protocols \
                     SET remote_compaction_v1_support = COALESCE(?, remote_compaction_v1_support), \
                         remote_compaction_v2_support = COALESCE(?, remote_compaction_v2_support), \
                         remote_compaction_probed_at = ? \
                     WHERE channel_id = ? AND protocol = 'openai_responses'",
                )
                .bind(v1)
                .bind(v2)
                .bind(&probe.probed_at)
                .bind(channel_id)
                .execute(&mut *tx)
                .await?;
            }
        }

        // The run terminal state commits in the same transaction (P2-5): a
        // catalog change can never be persisted without its run outcome, and a
        // failed write rolls the whole discovery back.
        // A run is a success when at least one protocol catalog was fetched:
        // providers with messy/partial protocol support (e.g. only the
        // openai family answering /v1/models) still discover their models
        // and must not surface as a failed run. `error_kind` records which
        // protocols failed.
        sqlx::query(
        "UPDATE discovery_runs SET finished_at=?,success=?,model_count=?,status_code=?,error_kind=? WHERE id=?",
    )
    .bind(finished_at)
    .bind(!snapshot.succeeded.is_empty())
    .bind(snapshot.model_count)
    .bind(snapshot.status_code)
    .bind(&snapshot.error_kind)
    .bind(run_id)
    .execute(&mut *tx)
    .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Writes the failure terminal for a discovery run in a short standalone
    /// transaction (P2-5): the previous catalog is left untouched. Never
    /// silently swallowed — the caller logs the result (P2-8).
    async fn record_run_failure(
        &self,
        run_id: &str,
        finished_at: &str,
        reason: &str,
    ) -> Result<()> {
        sqlx::query(
        "UPDATE discovery_runs SET finished_at=?,success=0,status_code=NULL,error_kind=? WHERE id=?",
    )
    .bind(finished_at)
    .bind(reason)
    .bind(run_id)
    .execute(self.db.pool())
    .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{application::Context, config::AppConfig, db::Database};
    use std::sync::Arc;

    async fn test_state(upstream_port: u16) -> (Context, std::path::PathBuf) {
        let dir =
            std::env::temp_dir().join(format!("lagw-discovery-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = Database::open(&dir.join("test.db")).await.unwrap();
        let secrets = crate::crypto::SecretStore::load(&dir.join("master.key"))
            .await
            .unwrap();
        let (telemetry, _rx) = crate::telemetry::Telemetry::new(64);
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("INSERT INTO providers(id,name,base_url,created_at,updated_at) VALUES('prov-1','mock',?,?,?)")
            .bind(format!("http://127.0.0.1:{upstream_port}"))
            .bind(time)
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channels(id,provider_id,name,protocol,api_key_encrypted,api_key_hint,manual_enabled,created_at,updated_at) VALUES('ch-1','prov-1','chan','openai_compatible',?,?,1,?,?)")
            .bind(secrets.encrypt("test-key"))
            .bind("...key")
            .bind(time)
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO channel_protocols(channel_id,protocol) VALUES('ch-1','openai_compatible')",
        )
        .execute(db.pool())
        .await
        .unwrap();
        sqlx::query("INSERT INTO discovery_runs(id,channel_id,trigger,started_at) VALUES('run-1','ch-1','manual',?)")
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
        let cancel = tokio_util::sync::CancellationToken::new();
        let http: Arc<dyn crate::ports::UpstreamClient> =
            Arc::new(crate::infrastructure::HttpClientPool::default());
        let routes: Arc<dyn crate::ports::RouteRepository> =
            crate::infrastructure::SqliteRouteRepository::new(db.clone());
        let channels: Arc<dyn crate::ports::ChannelRepository> =
            crate::infrastructure::SqliteChannelRepository::new(db.clone());
        let clock: Arc<dyn crate::ports::Clock> = Arc::new(crate::infrastructure::SystemClock);
        let background = crate::infrastructure::RuntimeSupervisor::new(cancel);
        let limits = Arc::new(crate::runtime::RuntimeLimits::default());
        let discovery = crate::discovery::DiscoveryService::new(
            db.clone(),
            secrets.clone(),
            Arc::clone(&http),
            Arc::clone(&channels),
            Arc::clone(&clock),
            Arc::clone(&background),
            Arc::clone(&limits),
        );
        let notifier: std::sync::Arc<dyn crate::ports::Notifier> =
            crate::notification::DesktopNotifier::new(std::time::Duration::from_millis(50));
        let proxy = crate::proxy::ProxyService::new(
            db.clone(),
            secrets.clone(),
            Arc::clone(&http),
            routes.clone(),
            telemetry.clone(),
            Arc::clone(&clock),
            Arc::clone(&limits),
            crate::notification::DesktopNotifier::new(std::time::Duration::from_millis(50)),
        );
        let state = Context {
            config: Arc::new(AppConfig::default()),
            db: db.clone(),
            secrets: secrets.clone(),
            http,
            routes,
            channels,
            clock,
            notifier,
            discovery,
            proxy,
            telemetry,
            background,
            limits,
            admin: crate::admin::AdminService::new(db.clone(), secrets.clone()),
            recovery: crate::auth::RecoverySession::new(),
        };
        (state, dir)
    }

    /// Raw upstream answering `GET /v1/models` with a two-model catalog.
    async fn spawn_models_upstream() -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = [0u8; 4096];
                let mut total = 0usize;
                loop {
                    match stream.read(&mut buf[total..]).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            total += n;
                            if buf[..total].windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                        }
                    }
                }
                let body = r#"{"data":[{"id":"m1","display_name":"Model One"},{"id":"m2","display_name":"Model Two"}]}"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
        port
    }

    /// P2-5: a successful discovery applies models, bindings and the run
    /// terminal state atomically.
    #[tokio::test]
    async fn discovery_applies_catalog_and_run_atomically() {
        let port = spawn_models_upstream().await;
        let (state, _dir) = test_state(port).await;
        let snapshot = state.discovery.discover(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.model_count, 2);
        assert!(snapshot.error_kind.is_none());
        state
            .discovery
            .apply_discovery("ch-1", &snapshot, "run-1", "2026-08-04T02:00:00+00:00")
            .await
            .unwrap();
        let models: Vec<String> = sqlx::query_scalar(
            "SELECT model_id FROM channel_models WHERE channel_id='ch-1' ORDER BY model_id",
        )
        .fetch_all(state.db.pool())
        .await
        .unwrap();
        assert_eq!(models, vec!["m1".to_owned(), "m2".to_owned()]);
        let bindings: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM channel_model_protocols cmp \
             JOIN channel_models cm ON cm.id = cmp.channel_model_id WHERE cm.channel_id='ch-1'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(bindings, 2, "every discovered model gets its binding");
        let (success, model_count, finished): (bool, i64, Option<String>) = sqlx::query_as(
            "SELECT success, model_count, finished_at FROM discovery_runs WHERE id='run-1'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert!(success);
        assert_eq!(model_count, 2);
        assert!(
            finished.is_some(),
            "the run terminal commits with the catalog"
        );
    }

    /// P2-5: a mid-write failure (fault-injected via a SQL trigger) rolls the
    /// WHOLE discovery back — no new models, no bindings, run still pending.
    #[tokio::test]
    async fn discovery_mid_write_failure_rolls_back() {
        let port = spawn_models_upstream().await;
        let (state, _dir) = test_state(port).await;
        sqlx::query(
            "CREATE TRIGGER fail_binding BEFORE INSERT ON channel_model_protocols \
             BEGIN SELECT RAISE(ABORT, 'boom'); END",
        )
        .execute(state.db.pool())
        .await
        .unwrap();
        let snapshot = state.discovery.discover(&state, "ch-1").await.unwrap();
        let result = state
            .discovery
            .apply_discovery("ch-1", &snapshot, "run-1", "2026-08-04T02:00:00+00:00")
            .await;
        assert!(result.is_err(), "the injected write failure must surface");
        let models: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM channel_models WHERE channel_id='ch-1'")
                .fetch_one(state.db.pool())
                .await
                .unwrap();
        assert_eq!(models, 0, "model inserts must roll back");
        let finished: Option<String> =
            sqlx::query_scalar("SELECT finished_at FROM discovery_runs WHERE id='run-1'")
                .fetch_one(state.db.pool())
                .await
                .unwrap();
        assert!(
            finished.is_none(),
            "the run must stay pending when the catalog write fails"
        );
    }

    /// P2-5: a network-failed discovery leaves the old catalog untouched and
    /// marks the run failed with the aggregated reason.
    #[tokio::test]
    async fn failed_network_discovery_marks_run_failed() {
        // Nothing listens on this port: the fetch fails immediately.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_port = listener.local_addr().unwrap().port();
        drop(listener);
        let (state, _dir) = test_state(dead_port).await;
        let snapshot = state.discovery.discover(&state, "ch-1").await.unwrap();
        assert!(
            snapshot.error_kind.is_some(),
            "a fully failed discovery reports the aggregated error"
        );
        state
            .discovery
            .apply_discovery("ch-1", &snapshot, "run-1", "2026-08-04T02:00:00+00:00")
            .await
            .unwrap();
        let (success, error_kind): (bool, Option<String>) =
            sqlx::query_as("SELECT success, error_kind FROM discovery_runs WHERE id='run-1'")
                .fetch_one(state.db.pool())
                .await
                .unwrap();
        assert!(!success);
        assert!(
            error_kind
                .as_deref()
                .is_some_and(|kind| kind.starts_with("protocol_discovery_failed")),
            "run failure reason must be recorded, got {error_kind:?}"
        );
        let models: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM channel_models WHERE channel_id='ch-1'")
                .fetch_one(state.db.pool())
                .await
                .unwrap();
        assert_eq!(models, 0, "the old catalog stays untouched");
    }

    /// Upstream that answers `GET /v1/models` only for Bearer auth (the
    /// openai family) and rejects the Claude auth header with 401.
    async fn spawn_partial_upstream() -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buf = [0u8; 4096];
                    let mut total = 0usize;
                    loop {
                        match stream.read(&mut buf[total..]).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => {
                                total += n;
                                if buf[..total].windows(4).any(|w| w == b"\r\n\r\n") {
                                    break;
                                }
                            }
                        }
                    }
                    let request = String::from_utf8_lossy(&buf[..total]);
                    let bearer = request
                        .lines()
                        .any(|line| line.to_ascii_lowercase().starts_with("authorization: bearer"));
                    let (status, body) = if bearer {
                        (
                            "200 OK",
                            r#"{"data":[{"id":"m1","display_name":"Model One"}]}"#,
                        )
                    } else {
                        (
                            "401 Unauthorized",
                            r#"{"error":{"message":"bad auth"}}"#,
                        )
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        });
        port
    }

    /// A provider whose protocols are only partially supported (the openai
    /// family answers /v1/models, Claude auth does not) still discovers its
    /// models: the run is a success, the failure is recorded per protocol,
    /// and bindings land only for the succeeded protocol.
    #[tokio::test]
    async fn partial_discovery_marks_run_success_and_applies_models() {
        let port = spawn_partial_upstream().await;
        let (state, _dir) = test_state(port).await;
        sqlx::query("INSERT INTO channel_protocols(channel_id,protocol) VALUES('ch-1','claude')")
            .execute(state.db.pool())
            .await
            .unwrap();
        let snapshot = state.discovery.discover(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.model_count, 1, "the bearer protocol's models are seen");
        assert!(
            snapshot.error_kind.as_deref().is_some_and(|kind| {
                kind.starts_with("protocol_discovery_failed:claude")
            }),
            "the failed protocol must be named, got {:?}",
            snapshot.error_kind
        );
        state
            .discovery
            .apply_discovery("ch-1", &snapshot, "run-1", "2026-08-04T02:00:00+00:00")
            .await
            .unwrap();
        let (success, model_count): (bool, i64) = sqlx::query_as(
            "SELECT success, model_count FROM discovery_runs WHERE id='run-1'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert!(
            success,
            "a partial discovery is a success: models were found"
        );
        assert_eq!(model_count, 1);
        let binding: String = sqlx::query_scalar(
            "SELECT protocol FROM channel_model_protocols cmp \
             JOIN channel_models cm ON cm.id = cmp.channel_model_id \
             WHERE cm.channel_id='ch-1' AND cm.model_id='m1'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(binding, "openai_compatible");
    }
}
