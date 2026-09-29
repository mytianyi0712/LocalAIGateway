//! 请求准备：设置读取、正文读取、网关鉴权、模型与映射解析、候选解析，
//! 以及 Command Code 的正文预转换。

use anyhow::Result;
use axum::{
    body::{Body, to_bytes},
    extract::Request,
    http::{Response, StatusCode},
};
use bytes::Bytes;

use crate::{
    convert,
    domain::{Candidate, CompactionMode, Event, MappingTarget},
    protocol,
    remote_compaction::{self},
    routing,
    settings,
};

// 同目录兄弟模块的内部项（`pub(super)` / `pub(crate)`）。
use super::error::*;
use super::service::*;

pub(super) fn mapped_path(
    mapping: &MappingTarget,
    request_path: &str,
    stream_requested: bool,
    model: &str,
    compaction: Option<CompactionMode>,
) -> String {
    let mapped = |upstream_protocol: &str| {
        // 协议 → 主路径的唯一来源是 `protocol::main_path`；未知协议保留入参路径。
        crate::protocol::main_path(upstream_protocol, model, stream_requested)
            .unwrap_or_else(|| request_path.into())
    };
    if mapping.entry == "claude" && request_path.starts_with("/claudecode/") {
        return mapped(&mapping.upstream_protocol);
    }
    if mapping.entry == "openai_responses" && request_path.starts_with("/codex/") {
        // V1 远程压缩有专用端点；其余情况与映射入口走同一张协议→路径表。
        if compaction == Some(CompactionMode::V1) && mapping.upstream_protocol == "openai_responses"
        {
            return "/v1/responses/compact".into();
        }
        return mapped(&mapping.upstream_protocol);
    }
    request_path.into()
}

// 遥测边界组装器：各字段来自互不相干的调用点上下文
// （候选、尝试记账、计时、usage），把它们分组只会把冗长
// 转移到八个调用点。故允许参数数量超限。
pub(super) async fn read_body(request: Request, max_bytes: usize) -> Result<Bytes> {
    to_bytes(request.into_body(), max_bytes)
        .await
        .map_err(Into::into)
}

/// 请求准备阶段的结果：尝试循环所需的全部内容，
/// 或一个提前返回的网关错误响应。
pub(super) struct PreparedRequest {
    pub(super) runtime: settings::RuntimeSettings,
    pub(super) headers: axum::http::HeaderMap,
    pub(super) path: String,
    pub(super) query: Option<String>,
    pub(super) entry_model: String,
    pub(super) stream_requested: bool,
    pub(super) mapping: Option<MappingTarget>,
    pub(super) upstream_protocol: String,
    pub(super) upstream_model: String,
    pub(super) converted_body: Bytes,
    /// 仅 Command Code：官方 Provider API 传输用的规范 OpenAI chat 正文
    /// （generate 正文是 `converted_body`）。
    pub(super) provider_body: Option<Bytes>,
    /// 当路由协议不是 `command_code`、但候选是 Command Code provider 时
    /// 预先备好的 Command Code 正文：尝试循环会按候选覆盖线上正文
    /// （见 [`CommandCodeBodies`]）。
    pub(super) command_code_bodies: Option<CommandCodeBodies>,
    pub(super) candidates: Vec<Candidate>,
    pub(super) compaction_mode: Option<CompactionMode>,
}

/// 一个 Command Code 候选在经由非 `command_code` 路由协议
/// （普通 Claude / OpenAI 路由）到达时所需的正文：
/// `/alpha/generate` 正文及其 Provider API 配套正文。
/// 在 `prepare_request` 中备好一次；`converted_body`/`provider_body`
/// 继续逐字节服务其它每个候选。
pub(super) struct CommandCodeBodies {
    pub(super) generate: Bytes,
    pub(super) provider: Bytes,
}

/// 请求准备：设置、正文读取、网关鉴权、模型解析、
/// 映射解析与候选路由。任何失败都
/// 产生一个提前返回的网关错误响应，
/// 而不进入尝试循环。
pub(super) async fn prepare_request(
    svc: &ProxyService,
    request: Request,
    entry_protocol: &str,
    fixed_path: Option<String>,
    request_id: &str,
    started: chrono::DateTime<chrono::Utc>,
) -> Result<PreparedRequest, Response<Body>> {
    let started_at = started.to_rfc3339();
    let (headers, request_path, query_string) = {
        let headers = request.headers().clone();
        let path = request.uri().path().to_owned();
        let query = request.uri().query().map(str::to_owned);
        (headers, path, query)
    };
    let path = fixed_path.unwrap_or(request_path);
    let query = query_string.as_deref();
    // 第一相：运行时设置 → 有界正文 → 网关鉴权。
    let (runtime, body) =
        prepare_authorize(svc, request, entry_protocol, &headers, query, request_id).await?;
    let (entry_model, stream_requested) =
        protocol::inspect_request(entry_protocol, &path, query, &body);
    let Some(entry_model) = entry_model else {
        return Err(gateway_error(
            entry_protocol,
            StatusCode::BAD_REQUEST,
            "model_required",
            "Unable to determine model from request.",
            request_id,
        ));
    };
    let compaction_mode = match remote_compaction::detect_compaction(&path, &body) {
        Ok(mode) => mode,
        Err(error) => {
            tracing::warn!(request_id = %request_id, %error, "invalid remote compaction request");
            return Err(gateway_error(
                entry_protocol,
                StatusCode::BAD_REQUEST,
                "invalid_compaction_request",
                &error.to_string(),
                request_id,
            ));
        }
    };
    // 第二相：映射入口解析（未知/禁用映射以 404 失败）。
    let mapping = resolve_entry_mapping(
        svc,
        entry_protocol,
        &path,
        &entry_model,
        stream_requested,
        started,
        &started_at,
        &body,
        request_id,
    )
    .await?;
    let mut upstream_protocol = mapping
        .as_ref()
        .map(|value| value.upstream_protocol.as_str())
        .unwrap_or(entry_protocol);
    // 静默转换兜底：直接入口自身没有路由、但该模型可通过某个
    // 已注册转换的协议到达时（OpenAI chat → Command Code），
    // 切换到该协议，而不是以 `no_active_channel` 失败。
    if mapping.is_none()
        && let Some(fallback) = routing::fallback_upstream_protocol(entry_protocol)
    {
        let native_available = svc
            .routes
            .resolve_candidates(upstream_protocol, &entry_model, 1)
            .await
            .map(|candidates| !candidates.is_empty())
            .unwrap_or(true);
        if !native_available {
            let fallback_available = svc
                .routes
                .resolve_candidates(fallback, &entry_model, 1)
                .await
                .map(|candidates| !candidates.is_empty())
                .unwrap_or(false);
            if fallback_available {
                upstream_protocol = fallback;
            }
        }
    }
    let upstream_model = mapping
        .as_ref()
        .map(|value| value.upstream_model.as_str())
        .unwrap_or(entry_model.as_str());
    // 该集成默认关闭：关闭期间必须发起零个上游请求。
    if upstream_protocol == "command_code" && !runtime.command_code_enabled {
        return Err(gateway_error(
            entry_protocol,
            StatusCode::FORBIDDEN,
            "command_code_disabled",
            "Command Code integration is disabled. Enable it in settings after acknowledging the risk.",
            request_id,
        ));
    }
    if compaction_mode.is_some() && upstream_protocol != "openai_responses" {
        let finished = svc.clock.now_utc();
        emit_request_start(
            svc,
            request_id,
            entry_protocol,
            &entry_model,
            &path,
            stream_requested,
            &started_at,
            body.len() as i64,
        );
        svc.telemetry.emit(Event::RequestFinish {
            id: request_id.to_owned(),
            finished_at: finished.to_rfc3339(),
            duration_ms: finished.signed_duration_since(started).num_milliseconds(),
            status: Some(422),
            outcome: "gateway_error".into(),
            attempts: 0,
            channel_id: None,
            response_bytes: 0,
        });
        return Err(gateway_error(
            entry_protocol,
            StatusCode::UNPROCESSABLE_ENTITY,
            "remote_compaction_unsupported_upstream",
            "Remote compaction is only supported for openai_responses upstream channels.",
            request_id,
        ));
    }
    // 第三相：上游正文（路由级正文与 Command Code 的 Provider 正文）。
    let ConvertedBodies {
        converted_body,
        provider_body,
    } = build_converted_bodies(
        entry_protocol,
        &body,
        mapping.as_ref(),
        upstream_protocol,
        upstream_model,
        request_id,
    )?;
    let (model_id, stream) =
        protocol::inspect_request(upstream_protocol, &path, query, &converted_body);
    // Command Code 上游始终以 NDJSON 流式返回；面向客户端的流式决策
    // 仍由入口请求决定（非流式客户端由 `bounded_non_stream`
    // 聚合 NDJSON 来服务）。
    let stream_requested = stream_requested || (stream && upstream_protocol != "command_code");
    let route_model = model_id.as_deref().unwrap_or(upstream_model);
    emit_request_start(
        svc,
        request_id,
        entry_protocol,
        &entry_model,
        &path,
        stream_requested,
        &started_at,
        body.len() as i64,
    );
    // 第四相：候选解析（压缩请求按能力过滤）。
    let mut candidates = resolve_candidates_for(
        svc,
        upstream_protocol,
        route_model,
        compaction_mode,
        &runtime,
        entry_protocol,
        request_id,
    )
    .await?;
    // 非 CC 路由协议（普通 Claude / OpenAI 路由）下的 Command Code 候选：
    // 上游始终说 CC 线格式，因此它的请求正文在此备好一次，
    // 尝试循环按候选覆盖正文。`command_code_enabled` 仍是总开关——
    // 关闭时这类候选被整体丢弃（零上游请求），
    // 入口若再无剩余候选，就以通常的 `no_active_channel` 失败。
    // （正文只在路由协议本身不是 `command_code` 时才需要覆盖。）
    let override_needed = upstream_protocol != "command_code"
        && candidates
            .iter()
            .any(|candidate| protocol::requires_command_code_identity(candidate.kind.as_deref()));
    let command_code_bodies = if override_needed && runtime.command_code_enabled {
        build_command_code_bodies(
            entry_protocol,
            &body,
            mapping.as_ref(),
            upstream_model,
            request_id,
        )
    } else {
        None
    };
    if override_needed && command_code_bodies.is_none() {
        candidates.retain(|candidate| {
            !protocol::requires_command_code_identity(candidate.kind.as_deref())
        });
    }
    if candidates.is_empty() {
        let (status_code, code, message) = if compaction_mode.is_some() {
            (
                422,
                "remote_compaction_unavailable",
                "No active channel supports remote compaction for this model.",
            )
        } else {
            (
                503,
                "no_active_channel",
                "No active channel is available for this model.",
            )
        };
        svc.telemetry.emit(Event::RequestFinish {
            id: request_id.to_owned(),
            finished_at: svc.clock.now_utc().to_rfc3339(),
            duration_ms: svc
                .clock
                .now_utc()
                .signed_duration_since(started)
                .num_milliseconds(),
            status: Some(status_code),
            outcome: "gateway_error".into(),
            attempts: 0,
            channel_id: None,
            response_bytes: 0,
        });
        return Err(gateway_error(
            entry_protocol,
            StatusCode::from_u16(status_code as u16).unwrap_or(StatusCode::SERVICE_UNAVAILABLE),
            code,
            message,
            request_id,
        ));
    }
    let upstream_protocol = upstream_protocol.to_owned();
    let upstream_model = upstream_model.to_owned();
    Ok(PreparedRequest {
        runtime,
        headers,
        path,
        query: query.map(str::to_owned),
        entry_model,
        stream_requested,
        mapping,
        upstream_protocol,
        upstream_model,
        converted_body,
        provider_body,
        command_code_bodies,
        candidates,
        compaction_mode,
    })
}

#[allow(clippy::result_large_err)]
/// 准备阶段的第一相：运行时设置 → 有界正文 → 网关鉴权。
/// 失败时返回可直接回给客户端的网关错误响应。
async fn prepare_authorize(
    svc: &ProxyService,
    request: Request,
    entry_protocol: &str,
    headers: &axum::http::HeaderMap,
    query: Option<&str>,
    request_id: &str,
) -> Result<(settings::RuntimeSettings, Bytes), Response<Body>> {
    let runtime = match svc.settings.runtime_settings().await {
        Ok(value) => value,
        Err(error) => {
            tracing::error!(request_id = %request_id, error = %error, "settings load failed");
            return Err(gateway_error(
                entry_protocol,
                StatusCode::INTERNAL_SERVER_ERROR,
                "settings_error",
                "Settings error",
                request_id,
            ));
        }
    };
    let body = match read_body(
        request,
        (runtime.max_request_body_mb.max(1) as usize) * 1024 * 1024,
    )
    .await
    {
        Ok(body) => body,
        Err(_) => {
            tracing::error!(request_id, "request body read failed");
            return Err(gateway_error(
                entry_protocol,
                StatusCode::PAYLOAD_TOO_LARGE,
                "request_too_large",
                "Request body exceeds the configured limit.",
                request_id,
            ));
        }
    };
    if !svc
        .settings
        .authorize_gateway(headers, query, entry_protocol)
        .await
        .unwrap_or(false)
    {
        return Err(gateway_error(
            entry_protocol,
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Gateway access denied.",
            request_id,
        ));
    }
    Ok((runtime, body))
}

#[allow(clippy::result_large_err)]
/// 第二相：模型映射只适用于专用映射入口（`/codex/v1/responses`、
/// `/claudecode/v1/messages`）；普通协议端点（`/v1/responses`、`/v1/messages`）
/// 始终直接路由，不经过映射解析。
/// 映射未知或已禁用时以 404 `unknown_mapped_model` 失败，
/// 而不是把请求当作普通协议请求悄悄路由。
#[allow(clippy::too_many_arguments)]
async fn resolve_entry_mapping(
    svc: &ProxyService,
    entry_protocol: &str,
    path: &str,
    entry_model: &str,
    stream_requested: bool,
    started: chrono::DateTime<chrono::Utc>,
    started_at: &str,
    body: &Bytes,
    request_id: &str,
) -> Result<Option<MappingTarget>, Response<Body>> {
    let is_mapped_entry = (entry_protocol == "openai_responses" && path.starts_with("/codex/"))
        || (entry_protocol == "claude" && path.starts_with("/claudecode/"));
    if !is_mapped_entry {
        return Ok(None);
    }
    let mapping = match svc.routes.resolve_mapping(entry_protocol, entry_model).await {
        Ok(value) => value,
        Err(error) => {
            tracing::error!(request_id = %request_id, error = %error, "mapping lookup failed");
            return Err(gateway_error(
                entry_protocol,
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "Database error",
                request_id,
            ));
        }
    };
    if mapping.is_none() {
        let finished = svc.clock.now_utc();
        emit_request_start(
            svc,
            request_id,
            entry_protocol,
            entry_model,
            path,
            stream_requested,
            started_at,
            body.len() as i64,
        );
        svc.telemetry.emit(Event::RequestFinish {
            id: request_id.to_owned(),
            finished_at: finished.to_rfc3339(),
            duration_ms: finished.signed_duration_since(started).num_milliseconds(),
            status: Some(404),
            outcome: "gateway_error".into(),
            attempts: 0,
            channel_id: None,
            response_bytes: 0,
        });
        return Err(gateway_error(
            entry_protocol,
            StatusCode::NOT_FOUND,
            "unknown_mapped_model",
            &format!("Model '{entry_model}' is not a configured model mapping."),
            request_id,
        ));
    }
    Ok(mapping)
}

#[allow(clippy::result_large_err)]
/// 第四相：候选解析。压缩请求走「按能力过滤」的压缩候选，
/// 其余走普通候选；失败时返回 `routing_error`。
async fn resolve_candidates_for(
    svc: &ProxyService,
    upstream_protocol: &str,
    route_model: &str,
    compaction_mode: Option<CompactionMode>,
    runtime: &settings::RuntimeSettings,
    entry_protocol: &str,
    request_id: &str,
) -> Result<Vec<Candidate>, Response<Body>> {
    let resolved = match if let Some(mode) = compaction_mode {
        svc.routes
            .resolve_compaction_candidates(
                upstream_protocol,
                route_model,
                mode,
                runtime.max_failover_attempts,
            )
            .await
    } else {
        svc.routes
            .resolve_candidates(
                upstream_protocol,
                route_model,
                runtime.max_failover_attempts,
            )
            .await
    } {
        Ok(value) => value,
        Err(error) => {
            tracing::error!(request_id = %request_id, error = %error, "candidate resolution failed");
            return Err(gateway_error(
                entry_protocol,
                StatusCode::INTERNAL_SERVER_ERROR,
                "routing_error",
                "Routing error",
                request_id,
            ));
        }
    };
    Ok(resolved)
}

/// 路由级正文（`converted_body`）与 Command Code Provider API 正文
/// （`provider_body`，仅 CC 路由需要）。
struct ConvertedBodies {
    converted_body: Bytes,
    provider_body: Option<Bytes>,
}

#[allow(clippy::result_large_err)]
/// 第三相：上游正文。映射入口按映射目标转换；未映射但协议不同时做静默转换；
/// 同协议则原样转发。
fn build_converted_bodies(
    entry_protocol: &str,
    body: &Bytes,
    mapping: Option<&MappingTarget>,
    upstream_protocol: &str,
    upstream_model: &str,
    request_id: &str,
) -> Result<ConvertedBodies, Response<Body>> {
    let invalid_request = |error: &dyn std::fmt::Display| {
        gateway_error(
            entry_protocol,
            StatusCode::BAD_REQUEST,
            "invalid_request",
            &error.to_string(),
            request_id,
        )
    };
    let converted_body = if let Some(value) = mapping {
        match convert::convert_request(
            &value.entry,
            &value.upstream_protocol,
            &value.upstream_model,
            body,
        ) {
            Ok(body) => Bytes::from(body),
            Err(error) => return Err(invalid_request(&error)),
        }
    } else if upstream_protocol != entry_protocol {
        // 未映射的静默转换（如 OpenAI chat → Command Code）：
        // 客户端正文需要转换成上游请求形状。
        match convert::convert_request(entry_protocol, upstream_protocol, upstream_model, body) {
            Ok(body) => Bytes::from(body),
            Err(error) => return Err(invalid_request(&error)),
        }
    } else {
        body.clone()
    };
    // Command Code：官方 Provider API 接收规范 OpenAI chat 正文，
    // 因此与 `/alpha/generate` 正文一并备好。
    // 传输路由器只在账号未被升级门禁拦截时才发送它。
    let provider_body = if upstream_protocol == "command_code" {
        let (provider_entry, provider_model) = mapping
            .map(|value| (value.entry.as_str(), value.upstream_model.as_str()))
            .unwrap_or((entry_protocol, upstream_model));
        match convert::convert_request(provider_entry, "openai_compatible", provider_model, body) {
            Ok(body) => Some(Bytes::from(body)),
            Err(error) => return Err(invalid_request(&error)),
        }
    } else {
        None
    };
    Ok(ConvertedBodies {
        converted_body,
        provider_body,
    })
}

/// 非 `command_code` 路由协议下的 Command Code 候选所需的双正文
/// （generate + provider），与映射/非映射正文准备使用同一个转换来源。
/// 入口没有注册转换（如 gemini）时返回 `None`——这是预期路径，不是失败。
fn build_command_code_bodies(
    entry_protocol: &str,
    body: &Bytes,
    mapping: Option<&MappingTarget>,
    upstream_model: &str,
    request_id: &str,
) -> Option<CommandCodeBodies> {
    let (source_protocol, source_model) = mapping
        .map(|value| (value.entry.as_str(), value.upstream_model.as_str()))
        .unwrap_or((entry_protocol, upstream_model));
    if !crate::protocol::converts_to_command_code(source_protocol) {
        return None;
    }
    let generate = convert::convert_request(source_protocol, "command_code", source_model, body);
    let provider = convert::convert_request(source_protocol, "openai_compatible", source_model, body);
    match (generate, provider) {
        (Ok(generate), Ok(provider)) => Some(CommandCodeBodies {
            generate: Bytes::from(generate),
            provider: Bytes::from(provider),
        }),
        _ => {
            tracing::warn!(
                %request_id,
                "command code override conversion failed; CC candidates dropped"
            );
            None
        }
    }
}

/// `RequestStart` 事件的统一发射点（准备阶段有多处早退都要记录起始）。
#[allow(clippy::too_many_arguments)]
fn emit_request_start(
    svc: &ProxyService,
    request_id: &str,
    entry_protocol: &str,
    entry_model: &str,
    path: &str,
    stream_requested: bool,
    started_at: &str,
    request_bytes: i64,
) {
    svc.telemetry.emit(Event::RequestStart {
        id: request_id.to_owned(),
        protocol: entry_protocol.to_owned(),
        model_id: Some(entry_model.to_owned()),
        endpoint: path.to_owned(),
        stream: stream_requested,
        started_at: started_at.to_owned(),
        request_bytes,
    });
}
