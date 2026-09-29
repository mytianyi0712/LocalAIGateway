//! 模型目录探测：按协议拉取上游模型目录，快照后一次性原子落库。
//!
//! 职责：逐个协议分组拉取模型目录（Claude `after_id`、Gemini `pageToken` 等
//! 按协议翻页），合并各协议在 `channel_models.metadata_json` 下的元数据，
//! 并清理过期协议绑定、重算 `available`。
//! 边界：网络阶段只读上游、绝不写库；调用方在单事务内应用快照（模型、绑定、
//! `available` 与运行终态），任一写入失败整体回滚。关键不变量：失败按协议聚合
//! 记录 `status_code` / `error_kind`，目录兜底与探测不确定作为诊断附加其中。

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

/// 模型目录的 HTTP 失败（保留状态码，供 Command Code 决定是否启用兜底目录）。
#[derive(Debug)]
struct CatalogHttpError {
    status: u16,
    message: String,
}

impl std::fmt::Display for CatalogHttpError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.message)
    }
}
impl std::error::Error for CatalogHttpError {}

/// Command Code 的静态兜底目录：`id -> item`。
fn bundled_catalog_map() -> HashMap<String, Value> {
    crate::commandcode::bundled_catalog_items()
        .into_iter()
        .filter_map(|item| {
            let id = item.get("id").and_then(Value::as_str)?.to_owned();
            Some((id, item))
        })
        .collect()
}

/// 网络阶段产出的不可变结果：调用上游期间不向数据库写任何内容。调用方把该
/// 快照应用到目录——模型、绑定、`available` 重算与运行终态——且只在**一个事务**
/// 内完成，因此写入中途失败绝不会留下半应用的探测结果。
pub struct DiscoverySnapshot {
    /// 目录拉取成功的协议集合。
    succeeded: HashSet<String>,
    /// 各协议各自的模型目录（model_id -> item）。
    seen_by_protocol: HashMap<String, HashMap<String, Value>>,
    /// 所有成功协议中出现过的去重模型 id 数量。
    model_count: i64,
    /// 观测到的最后一个上游状态码（任意协议）。
    status_code: Option<i64>,
    /// 至少一个协议失败时聚合出的错误细节；也承载目录兜底、探测不确定等诊断。
    error_kind: Option<String>,
    /// `openai_responses` 的远程压缩能力探测结果。
    remote_compaction: Option<RemoteCompactionProbe>,
}

/// 探测某渠道远程压缩端点得到的结果。
#[derive(Debug, Clone)]
pub struct RemoteCompactionProbe {
    pub v1: ProbeVerdict,
    pub v2: ProbeVerdict,
    pub probed_at: String,
}

/// 远程压缩探测的判定结论。`Inconclusive` 会保留此前持久化的值，仅在探测运行
/// 上追加一条诊断。
#[derive(Debug, Clone)]
pub enum ProbeVerdict {
    Supported,
    Unsupported,
    Inconclusive(String),
}

/// 发现服务：网络阶段 + 原子应用，仅依赖端口与存储。处理器与维护 supervisor
/// 通过 `AppState::discovery` 访问它。
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

    /// 供维护 supervisor 周期性重新发现时调用。
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
                    // 目录与运行终态在同一事务内提交。持久化失败绝不能静默收场——
                    // UI 会一直等待这次运行的终态。
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
        // 关闭流程已启动：运行记录保持 pending，UI 会把它呈现为
        // persistence_error 状态。
        tracing::warn!(run_id = %run_id, channel_id = %channel_id, "discovery not started: runtime shutting down");
    }
        Ok(run_id)
    }

    /// 按协议翻页拉取单个模型目录，页数上限为 `discovery_max_pages`（默认 50）。
    /// 返回 (model_id -> item, 最后一次状态码)。
    async fn fetch_models(
        &self,
        state: &Context,
        base_url: &str,
        protocol_name: &str,
        api_key: &str,
        channel_id: &str,
        provider_kind: Option<&str>,
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
        let opencode_session = if protocol::requires_opencode_session(base_url) {
            Some(settings::opencode_session_id(&state.db).await)
        } else {
            None
        };
        for _ in 0..self.limits.discovery_max_pages {
            if visited.contains(url.as_str()) {
                break;
            }
            visited.insert(url.to_string());
            let mut headers =
                protocol::outbound_headers(&axum::http::HeaderMap::new(), protocol_name, api_key)?;
            if let Some(session_id) = &opencode_session {
                protocol::apply_opencode_session(&mut headers, base_url, session_id)?;
            }
            if protocol_name == "command_code"
                && protocol::requires_command_code_identity(provider_kind)
            {
                let identity =
                    crate::commandcode::identity_for_probe(&state.db, channel_id).await;
                protocol::apply_command_code_identity(&mut headers, &identity)?;
            }
            let response = state
                .http
                .send(crate::ports::UpstreamRequest {
                    url: url.clone(),
                    headers,
                    method: http::Method::GET,
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
                return Err(anyhow::Error::new(CatalogHttpError {
                    status: response.status.as_u16(),
                    message: format!("{protocol_name} discovery returned {status_code}"),
                }));
            }
            // 目录体与上游响应一样按上限截断。
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

    /// 执行单个渠道的发现：仅做网络请求与解析。返回不可变快照，供调用方原子应用。
    async fn discover(&self, state: &Context, channel_id: &str) -> Result<DiscoverySnapshot> {
        let channel = state
            .channels
            .load_channel(channel_id)
            .await?
            .context("Channel not found")?;
        let primary = channel.protocol;
        let configured = self.channels.protocols(channel_id).await?;
        let protocols = if configured.is_empty() {
            vec![primary]
        } else {
            configured
        };
        // 被全局关掉的集成不发任何上游请求，因此完全跳过 Command Code 探测。
        let command_code_enabled = settings::runtime_settings(state)
            .await
            .map(|runtime| runtime.command_code_enabled)
            .unwrap_or(false);
        let protocols: Vec<String> = if command_code_enabled {
            protocols
        } else {
            protocols
                .into_iter()
                .filter(|protocol| protocol != "command_code")
                .collect()
        };
        if protocols.is_empty() {
            // 唯一协议被全局开关关掉：给出明确原因，而不是一次“成功但零模型”
            // 的空探测（用户看到的就是“无法探测模型”）。
            anyhow::bail!("command_code_disabled");
        }
        let api_key = self.secrets.decrypt(&channel.api_key_encrypted)?;
        let base_url = channel.base_url;

        // 把共用同一发现 URL + 认证头的协议归为一组（openai 家族共用
        // /v1/models 与同一个 Bearer 头，因此一次抓取即可覆盖）。
        let mut groups: Vec<(String, String, Vec<String>)> = Vec::new();
        let opencode_session = if protocol::requires_opencode_session(&base_url) {
            Some(settings::opencode_session_id(&state.db).await)
        } else {
            None
        };
        for protocol_name in &protocols {
            let url = protocol::upstream_url(
                &base_url,
                protocol::discovery_path(protocol_name),
                None,
                protocol_name,
            )?;
            let mut headers =
                protocol::outbound_headers(&axum::http::HeaderMap::new(), protocol_name, &api_key)?;
            if let Some(session_id) = &opencode_session {
                protocol::apply_opencode_session(&mut headers, &base_url, session_id)?;
            }
            if protocol_name == "command_code"
                && protocol::requires_command_code_identity(channel.kind.as_deref())
            {
                let identity = crate::commandcode::identity_for_probe(&state.db, channel_id).await;
                protocol::apply_command_code_identity(&mut headers, &identity)?;
            }
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
        // `/provider/v1/models` 对 Go 套餐稳定返回 403，而官方权威目录其实在
        // CLI 包内的 models.md；实时探测失败时退回静态 Go 目录，保证模型可用。
        let mut bundled_catalog = false;
        for (_, _, group_protocols) in &groups {
            let primary_protocol = &group_protocols[0];
            match timeout(
                Duration::from_secs(120),
                self.fetch_models(
                    state,
                    &base_url,
                    primary_protocol,
                    &api_key,
                    channel_id,
                    channel.kind.as_deref(),
                ),
            )
            .await
            {
                Ok(Ok((models, status_code))) => {
                    last_status_code = Some(status_code);
                    if primary_protocol == "command_code" && models.is_empty() {
                        tracing::warn!(
                            channel_id,
                            "command code live catalog was empty; using bundled Go catalog"
                        );
                        bundled_catalog = true;
                        for protocol_name in group_protocols {
                            succeeded.insert(protocol_name.clone());
                            seen_by_protocol
                                .insert(protocol_name.clone(), bundled_catalog_map());
                        }
                    } else {
                        for protocol_name in group_protocols {
                            succeeded.insert(protocol_name.clone());
                            seen_by_protocol.insert(protocol_name.clone(), models.clone());
                        }
                    }
                }
                Ok(Err(error)) => {
                    // 401 说明凭据本身不可用，绝不静默换成静态目录；其余失败
                    // （403 upgrade_required、网络、5xx、目录格式）都可以兜底。
                    let status = error
                        .downcast_ref::<CatalogHttpError>()
                        .map(|value| value.status);
                    if primary_protocol == "command_code" && status != Some(401) {
                        tracing::warn!(
                            channel_id,
                            status,
                            %error,
                            "command code live catalog unavailable; using bundled Go catalog"
                        );
                        last_status_code = Some(status.unwrap_or(0) as i64);
                        bundled_catalog = true;
                        for protocol_name in group_protocols {
                            succeeded.insert(protocol_name.clone());
                            seen_by_protocol
                                .insert(protocol_name.clone(), bundled_catalog_map());
                        }
                    } else {
                        tracing::warn!(channel_id, %error, "model discovery failed");
                        for protocol_name in group_protocols {
                            failed.push(protocol_name.clone());
                        }
                    }
                }
                Err(_) => {
                    if primary_protocol == "command_code" {
                        tracing::warn!(
                            channel_id,
                            "command code discovery timed out; using bundled Go catalog"
                        );
                        last_status_code = Some(0);
                        bundled_catalog = true;
                        for protocol_name in group_protocols {
                            succeeded.insert(protocol_name.clone());
                            seen_by_protocol
                                .insert(protocol_name.clone(), bundled_catalog_map());
                        }
                    } else {
                        tracing::warn!(channel_id, "model discovery timed out");
                        for protocol_name in group_protocols {
                            failed.push(protocol_name.clone());
                        }
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
        if bundled_catalog {
            // 成功但带诊断：UI/日志可提示目录来自包内快照而非实时目录。
            let diagnostic = "command_code_bundled_catalog".to_owned();
            error_kind = Some(match error_kind.take() {
                Some(prefix) => format!("{prefix};{diagnostic}"),
                None => diagnostic,
            });
        }
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

    /// 在 `openai_responses` 渠道上探测两种远程压缩协议。探测绝不让发现失败：
    /// 判定不确定的结果记录为诊断，并保留此前存储的能力值。
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
        // 连接超时取自运行时设置 `connect_timeout_seconds`。
        let connect_timeout = match settings::runtime_settings(state).await {
            Ok(runtime) => std::time::Duration::from_secs(runtime.connect_timeout_seconds.max(1) as u64),
            Err(error) => return ProbeVerdict::Inconclusive(format!("settings:{error}")),
        };
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
        if protocol::requires_opencode_session(base_url) {
            let session_id = settings::opencode_session_id(&state.db).await;
            if let Err(error) =
                protocol::apply_opencode_session(&mut headers, base_url, &session_id)
            {
                return ProbeVerdict::Inconclusive(format!("session:{error}"));
            }
        }
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
                method: http::Method::POST,
                body: Some(bytes::Bytes::from(payload)),
                connect_timeout,
                deadline: self.limits.probe_timeout,
            })
            .await
        {
            Ok(response) => response,
            Err(error) => return ProbeVerdict::Inconclusive(format!("transport:{error:?}")),
        };
        let status = response.status.as_u16();
        let (body, truncated) = match timeout(self.limits.probe_timeout, response.body.read_capped(self.limits.error_body_max)).await {
            Ok(result) => result,
            Err(_) => return ProbeVerdict::Inconclusive("timeout".into()),
        };
        // 撞上读取上限时判定依据不完整，不能据此断言 Supported/Unsupported。
        if truncated {
            return ProbeVerdict::Inconclusive("truncated".into());
        }
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
        // 连接超时取自运行时设置 `connect_timeout_seconds`。
        let connect_timeout = match settings::runtime_settings(state).await {
            Ok(runtime) => std::time::Duration::from_secs(runtime.connect_timeout_seconds.max(1) as u64),
            Err(error) => return ProbeVerdict::Inconclusive(format!("settings:{error}")),
        };
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
        if protocol::requires_opencode_session(base_url) {
            let session_id = settings::opencode_session_id(&state.db).await;
            if let Err(error) =
                protocol::apply_opencode_session(&mut headers, base_url, &session_id)
            {
                return ProbeVerdict::Inconclusive(format!("session:{error}"));
            }
        }
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
                method: http::Method::POST,
                body: Some(bytes::Bytes::from(payload)),
                connect_timeout,
                deadline: self.limits.probe_timeout,
            })
            .await
        {
            Ok(response) => response,
            Err(error) => return ProbeVerdict::Inconclusive(format!("transport:{error:?}")),
        };
        let status = response.status.as_u16();
        // 与代理路径共用同一套读取/判读骨架：校验器一确认完整就停止读取，
        // 因此上游发完事件后保持连接也不会把探测拖到超时。
        let mut validator = remote_compaction::CompactionV2Validator::new();
        match crate::remote_compaction::read_validated_compaction_stream(
            response.body.into_stream(),
            self.limits.probe_timeout,
            (self.limits.error_body_max * 4).max(4 * 1024 * 1024),
            &mut validator,
            None,
        )
        .await
        {
            crate::remote_compaction::ValidatedCompactionStream::Read { truncated, .. } => {
                // 撞上读取上限时判定依据不完整，不能据此断言 Supported/Unsupported。
                if truncated {
                    return ProbeVerdict::Inconclusive("truncated".into());
                }
            }
            crate::remote_compaction::ValidatedCompactionStream::Transport { .. } => {
                return ProbeVerdict::Inconclusive("timeout".into());
            }
            // 探测路径不传解码器，因此不会出现解码失败；真出现也按不确定处理。
            crate::remote_compaction::ValidatedCompactionStream::Decode { error, .. } => {
                return ProbeVerdict::Inconclusive(format!("decode:{error:?}"));
            }
        }
        let verdict = validator.finish();
        // 两种“不支持”判定：上游明确回 404/405/501，或 2xx 流里没有 compaction
        // 块（NotCompaction）。合并成一个分支，避免同一结果写两遍。
        let unsupported = matches!(status, 404 | 405 | 501)
            || (response.status.is_success()
                && matches!(
                    &verdict,
                    Err(remote_compaction::CompactionV2Error::NotCompaction)
                ));
        if response.status.is_success() && verdict.is_ok() {
            ProbeVerdict::Supported
        } else if unsupported {
            ProbeVerdict::Unsupported
        } else {
            ProbeVerdict::Inconclusive(format!(
                "status:{status};verdict:{:?}",
                verdict.err().map(|error| error.to_string())
            ))
        }
    }

    /// 在**单个事务**内把发现快照应用到目录：模型 upsert、协议绑定、过期绑定
    /// 清理、`available` 重算以及运行终态。要么全部生效，要么全部不生效——写入
    /// 失败会回滚到原有目录，而不会留下缺绑定的新模型或卡在 `pending` 的运行。
    async fn apply_discovery(
        &self,
        channel_id: &str,
        snapshot: &DiscoverySnapshot,
        run_id: &str,
        finished_at: &str,
    ) -> Result<()> {
        let mut tx = self.db.pool().begin().await?;
        // Command Code 渠道服务于转换层能用 `command_code` 驱动的每一个入口协议：
        // 目录行镜像这些绑定（及其元数据），好让模型路由在 Claude / OpenAI 路由下
        // 也能选出该渠道，而不只限于 `command_code` 路由。
        let provider_kind = self.channels.provider_kind(channel_id).await?;
        let mut succeeded: Vec<String> = snapshot.succeeded.iter().cloned().collect();
        succeeded.sort();
        let extra_protocols: Vec<String> =
            protocol::model_binding_protocols(provider_kind.as_deref(), &succeeded)
                .into_iter()
                .filter(|protocol| !snapshot.succeeded.contains(protocol))
                .collect();
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
                // 自身没有目录的入口协议（如 Command Code 渠道）继承本协议的
                // 元数据——能力与价格读取器按 `metadata_json[路由协议]` 查找。
                for extra_protocol in &extra_protocols {
                    metadata
                        .entry(extra_protocol.clone())
                        .or_insert(protocol_meta.clone());
                }
                metadata.insert(protocol_name.clone(), protocol_meta);
                let display_name = item
                    .get("display_name")
                    .or_else(|| item.get("displayName"))
                    // Provider API 的条目字段是 `name`（如 "Claude Sonnet 5"）；
                    // 只认 display_name 会让 UI 退化成裸模型 id。
                    .or_else(|| item.get("name"))
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty())
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
                for extra_protocol in &extra_protocols {
                    sqlx::query("INSERT OR IGNORE INTO channel_model_protocols(channel_model_id,protocol) VALUES(?,?)")
                    .bind(&row_id)
                    .bind(extra_protocol)
                    .execute(&mut *tx)
                    .await?;
                    protocol_sets
                        .entry(model_id.clone())
                        .or_default()
                        .insert(extra_protocol.clone());
                }
            }
        }

        // 清理不再列出该模型的协议绑定，并重算 `available`——没有任何剩余协议的
        // 模型会被隐藏。
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

        // 在同一事务内持久化远程压缩探测结果。Inconclusive 判定不改动已有的
        // 能力值。
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

        // 运行终态在同一事务内提交：目录变更绝不会脱离其运行结果被持久化，写入
        // 失败会把整次发现回滚。
        // 只要至少一个协议目录拉取成功，运行就算成功：协议支持参差不齐的提供商
        // （例如只有 openai 家族应答 /v1/models）仍能发现其模型，不应呈现为失败
        // 运行。`error_kind` 记录哪些协议失败。
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

    /// 在一个简短的独立事务里写入发现运行的失败终态：原目录保持不变。结果绝不
    /// 静默吞掉——由调用方负责记录日志。
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
    use super::ProbeVerdict;
    use crate::state::AppState;
    use crate::test_support::TempDir;
    use serde_json::Value;

    async fn test_state(upstream_port: u16) -> (AppState, TempDir) {
        // 统一夹具：临时目录 + 整套 Context + 标准 provider/channel 播种。
        let crate::test_support::TestEnv {
            dir,
            context: state,
        } = crate::test_support::context("discovery").await;
        crate::test_support::seed_provider(
            &state.db,
            "prov-1",
            "mock",
            &format!("http://127.0.0.1:{upstream_port}"),
        )
        .await;
        crate::test_support::seed_channel(
            &state.db,
            &state.secrets,
            "ch-1",
            "prov-1",
            "openai_compatible",
            "test-key",
        )
        .await;
        sqlx::query("INSERT INTO discovery_runs(id,channel_id,trigger,started_at) VALUES('run-1','ch-1','manual',?)")
            .bind(crate::test_support::SEED_TIME)
            .execute(state.db.pool())
            .await
            .unwrap();
        (state, dir)
    }

    /// 原始上游：对任何请求回固定状态行与响应体（用于探测体超限等场景）。
    async fn spawn_raw_upstream(status: &'static str, body: Vec<u8>) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            if let Ok((mut stream, _)) = listener.accept().await {
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
                let head = format!(
                    "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(head.as_bytes()).await;
                let _ = stream.write_all(&body).await;
            }
        });
        port
    }

    /// V1 探测体撞上读取上限时判定依据不完整 → `Inconclusive("truncated")`，
    /// 绝不拿半个响应体去断言 Supported/Unsupported。
    #[tokio::test]
    async fn v1_compaction_probe_reports_truncation_instead_of_support() {
        let port = spawn_raw_upstream("200 OK", vec![b'x'; 2 * 1024 * 1024]).await;
        let (state, _dir) = test_state(port).await;
        let verdict = state
            .discovery
            .probe_remote_compaction_v1(
                &state,
                &format!("http://127.0.0.1:{port}"),
                "test-key",
                "test-model",
                "ch-1",
            )
            .await;
        match verdict {
            ProbeVerdict::Inconclusive(reason) => {
                assert_eq!(reason, "truncated", "truncation must be the reason");
            }
            other => panic!("a truncated probe body must be inconclusive, got {other:?}"),
        }
    }

    /// 原始上游：对 `GET /v1/models` 回一个双模型目录。
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


    /// Command Code 目录 mock：应答 `GET /provider/v1/models`，并记录每个请求头
    /// 供头部断言使用。
    async fn spawn_command_code_catalog_upstream() -> (
        u16,
        tokio::sync::mpsc::UnboundedReceiver<String>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let tx = tx.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];
                    loop {
                        let n = match stream.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => n,
                        };
                        buf.extend_from_slice(&chunk[..n]);
                        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                    }
                    let _ = tx.send(String::from_utf8_lossy(&buf).into_owned());
                    let body = r#"{"data":[{"id":"deepseek/deepseek-v4-flash"},{"id":"cc-model"}]}"#;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        });
        (port, rx)
    }

    /// Command Code 发现走 Provider API 目录路径并带上 CLI 身份头（仅当 provider
    /// 的 `kind='command_code'` 时），同时解析 OpenAI 形状的目录条目。
    #[tokio::test]
    async fn command_code_discovery_uses_provider_catalog_with_identity() {
        let (port, mut heads) = spawn_command_code_catalog_upstream().await;
        let (state, _dir) = test_state(port).await;
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("UPDATE providers SET kind='command_code' WHERE id='prov-1'")
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE channels SET protocol='command_code' WHERE id='ch-1'")
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM channel_protocols WHERE channel_id='ch-1'")
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_protocols(channel_id,protocol) VALUES('ch-1','command_code')")
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO settings(key,value_json,updated_at) VALUES('command_code_enabled','true',?)")
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();

        let snapshot = state.discovery.discover(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.model_count, 2);
        assert!(snapshot.error_kind.is_none());

        let head = tokio::time::timeout(std::time::Duration::from_secs(5), heads.recv())
            .await
            .expect("catalog request timed out")
            .expect("catalog request missing");
        assert!(head.starts_with("GET /provider/v1/models"), "{head}");
        let lower = head.to_ascii_lowercase();
        for needle in [
            "x-cli-environment: production",
            "x-command-code-version:",
            "x-session-id:",
            "x-co-flag: false",
            "traceparent:",
        ] {
            assert!(lower.contains(needle), "{needle} missing in {head}");
        }

        // Command Code 渠道的目录行为转换层能用 `command_code` 驱动的每个入口协议
        // 建立绑定，并在这些协议键下镜像目录元数据，好让能力/价格读取器在
        // claude/openai 路由上也能找到它们。
        state
            .discovery
            .apply_discovery("ch-1", &snapshot, "run-1", "2026-08-04T02:00:00+00:00")
            .await
            .unwrap();
        let mut bindings: Vec<String> = sqlx::query_scalar(
            "SELECT cmp.protocol FROM channel_model_protocols cmp \
             JOIN channel_models cm ON cm.id = cmp.channel_model_id \
             WHERE cm.model_id = 'cc-model'",
        )
        .fetch_all(state.db.pool())
        .await
        .unwrap();
        bindings.sort();
        assert_eq!(
            bindings,
            [
                "claude",
                "command_code",
                "openai_compatible",
                "openai_responses"
            ]
        );
        let raw_metadata: String =
            sqlx::query_scalar("SELECT metadata_json FROM channel_models WHERE model_id='cc-model'")
                .fetch_one(state.db.pool())
                .await
                .unwrap();
        let metadata: Value = serde_json::from_str(&raw_metadata).unwrap();
        for protocol in [
            "command_code",
            "claude",
            "openai_compatible",
            "openai_responses",
        ] {
            assert!(
                metadata.get(protocol).is_some(),
                "{protocol} metadata missing: {metadata}"
            );
        }
        // 渠道级（探测用）协议保持与配置完全一致。
        let channel_protocols: Vec<String> =
            sqlx::query_scalar("SELECT protocol FROM channel_protocols WHERE channel_id='ch-1'")
                .fetch_all(state.db.pool())
                .await
                .unwrap();
        assert_eq!(channel_protocols, ["command_code"]);
    }


    /// 目录 mock：对每个请求都回同一个 HTTP 状态码。
    async fn spawn_status_catalog_upstream(status: u16) -> u16 {
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
                    let body = format!(r#"{{"error":{{"message":"catalog {status}"}}}}"#);
                    let response = format!(
                        "HTTP/1.1 {status} Test\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        });
        port
    }


    /// 真实 `/provider/v1/models` 的条目字段是 `name`/`context_length`（没有
    /// `display_name`）：显示名必须取 `name`，否则 UI 退化成裸模型 id。
    #[tokio::test]
    async fn real_provider_catalog_name_becomes_the_display_name() {
        async fn spawn_upstream() -> u16 {
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
                    let body = r#"{"object":"list","data":[{"id":"m1","object":"model","owned_by":"command-code","name":"Model One","context_length":1000000}]}"#;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                }
            });
            port
        }
        let port = spawn_upstream().await;
        let (state, _dir) = test_state(port).await;
        let snapshot = state.discovery.discover(&state, "ch-1").await.unwrap();
        assert!(snapshot.succeeded.contains("openai_compatible"));
        state
            .discovery
            .apply_discovery("ch-1", &snapshot, "run-1", "2026-08-04T02:00:00+00:00")
            .await
            .unwrap();
        let (display_name, metadata): (Option<String>, Option<String>) = sqlx::query_as(
            "SELECT display_name, metadata_json FROM channel_models WHERE channel_id='ch-1' AND model_id='m1'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_eq!(display_name.as_deref(), Some("Model One"));
        let metadata: Value = serde_json::from_str(&metadata.unwrap()).unwrap();
        assert_eq!(
            metadata
                .pointer("/openai_compatible/context_length")
                .and_then(Value::as_i64),
            Some(1_000_000)
        );
    }

    /// 把已播种的渠道/provider 切换到 Command Code 协议。
    async fn switch_to_command_code(state: &AppState) {
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("UPDATE providers SET kind='command_code' WHERE id='prov-1'")
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE channels SET protocol='command_code' WHERE id='ch-1'")
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM channel_protocols WHERE channel_id='ch-1'")
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_protocols(channel_id,protocol) VALUES('ch-1','command_code')")
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO settings(key,value_json,updated_at) VALUES('command_code_enabled','true',?)")
            .bind(time)
            .execute(state.db.pool())
            .await
            .unwrap();
    }

    /// Go 套餐的 `/provider/v1/models` 返回 403：必须退回官方 CLI 包内的
    /// Go 目录，目录可用且带 `command_code_bundled_catalog` 诊断。
    #[tokio::test]
    async fn command_code_discovery_falls_back_to_bundled_catalog_on_403() {
        let port = spawn_status_catalog_upstream(403).await;
        let (state, _dir) = test_state(port).await;
        switch_to_command_code(&state).await;
        let snapshot = state.discovery.discover(&state, "ch-1").await.unwrap();
        assert!(
            snapshot.succeeded.contains("command_code"),
            "fallback catalog must count as a fetched protocol"
        );
        assert!(
            snapshot.model_count >= 40,
            "expected the bundled Go catalog, got {}",
            snapshot.model_count
        );
        let models = snapshot
            .seen_by_protocol
            .get("command_code")
            .expect("bundled models");
        let flash = models
            .get("deepseek/deepseek-v4-flash")
            .expect("a Go-eligible model is present");
        assert_eq!(
            flash.get("command_code_bundled").and_then(Value::as_bool),
            Some(true)
        );
        let error_kind = snapshot.error_kind.clone().unwrap_or_default();
        assert!(
            error_kind.contains("command_code_bundled_catalog"),
            "diagnostic missing: {error_kind}"
        );
        // 不是 Pro/Max 专属模型。
        assert!(!models.contains_key("claude-opus-5"));
    }

    /// 401 是凭据问题，绝不能静默换成静态目录。
    #[tokio::test]
    async fn command_code_discovery_401_is_not_masked_by_the_fallback() {
        let port = spawn_status_catalog_upstream(401).await;
        let (state, _dir) = test_state(port).await;
        switch_to_command_code(&state).await;
        let snapshot = state.discovery.discover(&state, "ch-1").await.unwrap();
        assert!(!snapshot.succeeded.contains("command_code"));
        assert!(snapshot.seen_by_protocol.get("command_code").is_none());
        let error_kind = snapshot.error_kind.clone().unwrap_or_default();
        assert!(
            error_kind.contains("protocol_discovery_failed"),
            "{error_kind}"
        );
    }

    /// 一次成功的发现会原子地应用模型、绑定与运行终态。
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

    /// 写入中途失败（通过 SQL 触发器注入故障）会把**整次**发现回滚——没有新模型、
    /// 没有绑定，运行仍停在 pending。
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

    /// 网络失败的发现不会改动原目录，并以聚合出的原因把运行标记为失败。
    #[tokio::test]
    async fn failed_network_discovery_marks_run_failed() {
        // 该端口没有任何监听者：抓取会立刻失败。
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

    /// 只对 Bearer 认证（openai 家族）应答 `GET /v1/models`、并以 401 拒绝 Claude
    /// 认证头的上游。
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

    /// 协议仅被部分支持的提供商（openai 家族应答 /v1/models，Claude 认证不应答）
    /// 仍能发现其模型：运行算成功，失败按协议记录，且只为成功的协议落绑定。
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
