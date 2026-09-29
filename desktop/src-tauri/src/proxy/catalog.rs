//! 模型目录与信息端点：目录鉴权、各协议的原生列表形状，以及
//! claudecode / codex 的信息端点。

use anyhow::Result;
use axum::{
    body::Body,
    extract::{Request, State},
    http::{Response, StatusCode},
};
use serde_json::{Value, json};

use crate::{
    capabilities,
    domain::RoutableModel,
    settings,
    state::AppState,
};

// 同目录兄弟模块的内部项（`pub(super)` / `pub(crate)`）。
use super::error::*;

#[allow(clippy::result_large_err)]
/// 为一个目录条目构造 `x_local_gateway` 元数据：该模型当前可路由经过的
/// 全部端点之并集（按 `PROTOCOL_ORDER` 排序），
/// 加上能力数据（上下文窗口、最大 token、
/// 推理、图像输入、成本）与 pi 模型配置，
/// 让 omp 扩展等目录消费者能用真实值而非内置默认值。
/// `auto` 行会即时探测能力；
/// 无能力数据的模型只省略 `capabilities`/`pi_model_config` 两个子字段。
pub(super) async fn gateway_metadata(
    state: &AppState,
    item: &RoutableModel,
) -> Result<Option<Value>, Response<Body>> {
    let mut gateway = json!({});
    match state.routes.routable_endpoints_for_model(&item.id).await {
        Ok(endpoints) => {
            if !endpoints.is_empty() {
                gateway["supported_endpoints"] = json!(endpoints);
            }
        }
        Err(error) => {
            tracing::error!(error = %error, "model endpoints query failed");
            return Err(gateway_error(
                "catalog",
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "Database error",
                "catalog",
            ));
        }
    }
    match capabilities::get_model_caps(state, &item.id).await {
        Ok(caps) => {
            if capabilities::has_capability_data(&caps) {
                let pi_config = capabilities::pi_model_config(&caps);
                gateway["capabilities"] = caps;
                gateway["pi_model_config"] = pi_config;
            }
        }
        Err(error) => {
            tracing::error!(error = %error, "model caps query failed");
            return Err(gateway_error(
                "catalog",
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "Database error",
                "catalog",
            ));
        }
    }
    Ok(if gateway
        .as_object()
        .is_some_and(|object| !object.is_empty())
    {
        Some(gateway)
    } else {
        None
    })
}

#[allow(clippy::result_large_err)]
/// 目录/信息类接口的统一鉴权：`None` 表示放行；`Some(response)` 表示调用方
/// 直接把响应返回给客户端。
///
/// 三种情况都是 401：查询返回 false、以及查询本身失败（fail-closed——授权查询
/// 出错时绝不放行，错误码与文案与改造前一致）。失败会留下一条告警日志。
///
/// 参数用 `headers` + `query` 而非 `&Request`：`&Request` 不是 `Send`，
/// 持有它会让 handler 的 future 不再是 `Send`。
pub(super) async fn authorize_catalog(
    state: &AppState,
    headers: &axum::http::HeaderMap,
    query: Option<&str>,
    protocol: &str,
    request_id: &str,
) -> Option<Response<Body>> {
    let denied = || {
        gateway_error(
            protocol,
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Gateway access denied.",
            request_id,
        )
    };
    match settings::authorize_gateway(state, headers, query, protocol).await {
        Ok(true) => None,
        Ok(false) => Some(denied()),
        Err(error) => {
            tracing::warn!(error = ?error, protocol, "gateway 授权查询失败");
            Some(denied())
        }
    }
}

/// OpenAI 列表形式的单个目录条目（`id`/`object`/`owned_by`/`created`）。
/// 各协议目录、映射目录与 `/v1/models` 聚合共用同一字段集合。
pub(super) fn openai_model_value(item: &RoutableModel) -> Value {
    json!({
        "id": item.id,
        "object": "model",
        "owned_by": "local-gateway",
        "created": item.created_at,
    })
}

/// OpenAI 列表形式的目录条目（含共享的 `x_local_gateway` 元数据）；
/// 协议目录与 `/v1/models` 聚合都走这里。
pub(super) async fn openai_catalog_items(
    state: &AppState,
    items: &[RoutableModel],
) -> Result<Vec<Value>, Response<Body>> {
    let mut data = Vec::new();
    for item in items {
        let mut value = openai_model_value(item);
        match gateway_metadata(state, item).await {
            Ok(Some(metadata)) => value["x_local_gateway"] = metadata,
            Ok(None) => {}
            Err(response) => return Err(response),
        }
        data.push(value);
    }
    Ok(data)
}

/// 按协议的模型目录。各协议以其原生列表形状作答：
/// OpenAI 列表形式（`/v1/responses/models`、经协议选择的
/// `/v1/models`）、Claude 列表形式（`/v1/messages/models`），
/// 或 Gemini（`/v1beta/models`）。
pub async fn models(
    State(state): State<AppState>,
    request: Request,
    protocol: &str,
) -> Response<Body> {
    if let Some(response) = authorize_catalog(&state, request.headers(), request.uri().query(), protocol, "catalog").await {
        return response;
    }
    let items = match state.routes.list_routable_models(Some(protocol)).await {
        Ok(value) => value,
        Err(error) => {
            tracing::error!(error = %error, "model catalog query failed");
            return gateway_error(
                protocol,
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "Database error",
                "catalog",
            );
        }
    };
    match protocol {
        "gemini" => json_response(
            StatusCode::OK,
            json!({"models":items.iter().map(|item|json!({"name":format!("models/{}",item.id),"displayName":item.display_name,"supportedGenerationMethods":["generateContent"]})).collect::<Vec<_>>() }),
        ),
        "claude" => {
            let mut data = Vec::new();
            for item in &items {
                let mut value = json!({
                    "type": "model",
                    "id": item.id,
                    "display_name": item.display_name,
                    "created_at": item.created_at,
                });
                match gateway_metadata(&state, item).await {
                    Ok(Some(metadata)) => value["x_local_gateway"] = metadata,
                    Ok(None) => {}
                    Err(response) => return response,
                }
                data.push(value);
            }
            json_response(
                StatusCode::OK,
                json!({
                    "data": data,
                    "has_more": false,
                    "first_id": data.first().map(|value| value["id"].clone()).unwrap_or(Value::Null),
                    "last_id": data.last().map(|value| value["id"].clone()).unwrap_or(Value::Null),
                }),
            )
        }
        _ => match openai_catalog_items(&state, &items).await {
            Ok(data) => json_response(StatusCode::OK, json!({ "object": "list", "data": data })),
            Err(response) => response,
        },
    }
}

/// `GET /v1/models` 在未显式指定协议时的聚合目录：
/// 跨全部协议的每个启用且可路由的模型；
/// 去重由底层查询的 `GROUP BY` 完成（按模型 id）。
pub async fn aggregate_models(
    State(state): State<AppState>,
    request: Request,
) -> Response<Body> {
    if let Some(response) = authorize_catalog(&state, request.headers(), request.uri().query(), "catalog", "catalog").await {
        return response;
    }
    let items = match state.routes.list_routable_models(None).await {
        Ok(value) => value,
        Err(error) => {
            tracing::error!(error = %error, "model catalog query failed");
            return gateway_error(
                "catalog",
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "Database error",
                "catalog",
            );
        }
    };
    match openai_catalog_items(&state, &items).await {
        Ok(data) => json_response(StatusCode::OK, json!({ "object": "list", "data": data })),
        Err(response) => response,
    }
}

pub async fn mapped_models(
    State(state): State<AppState>,
    request: Request,
    entry: &'static str,
) -> Response<Body> {
    if let Some(response) = authorize_catalog(&state, request.headers(), request.uri().query(), entry, "catalog").await {
        return response;
    }
    let items = match state.routes.list_mapping_models(entry).await {
        Ok(value) => value,
        Err(error) => {
            tracing::error!(error = %error, "mapping model catalog query failed");
            return gateway_error(
                entry,
                StatusCode::INTERNAL_SERVER_ERROR,
                "database_error",
                "Database error",
                "catalog",
            );
        }
    };
    json_response(
        StatusCode::OK,
        json!({"object":"list","data":items.iter().map(openai_model_value).collect::<Vec<_>>() }),
    )
}

pub async fn claudecode_info(State(state): State<AppState>, request: Request) -> Response<Body> {
    if let Some(response) = authorize_catalog(
        &state,
        request.headers(),
        request.uri().query(),
        "claude",
        "info",
    )
    .await
    {
        return response;
    }
    json_response(
        StatusCode::OK,
        json!({"protocol":"claude","endpoint":"/claudecode/v1/messages","models":"/claudecode/v1/models"}),
    )
}
pub async fn codex_info(State(state): State<AppState>, request: Request) -> Response<Body> {
    if let Some(response) = authorize_catalog(
        &state,
        request.headers(),
        request.uri().query(),
        "openai_responses",
        "info",
    )
    .await
    {
        return response;
    }
    json_response(
        StatusCode::OK,
        json!({"protocol":"openai_responses","endpoint":"/codex/v1/responses","models":"/codex/v1/models"}),
    )
}

/// `GET /v1/models`。协议选择顺序：`protocol` 查询参数、
/// `X-Local-Gateway-Protocol` 头，或 `anthropic-version` 头（Claude SDK）；
/// 各自返回对应协议的原生目录。选择器不在白名单
/// （openai_compatible / openai_responses / claude）内时返回 400。
/// 未提供任何选择器时，聚合全部启用且可路由的模型。
pub async fn openai_models(State(state): State<AppState>, request: Request) -> Response<Body> {
    let selected = request
        .uri()
        .query()
        .and_then(|query| {
            url::form_urlencoded::parse(query.as_bytes())
                .find(|(key, _)| key == "protocol")
                .map(|(_, value)| value.into_owned())
        })
        .or_else(|| {
            request
                .headers()
                .get("x-local-gateway-protocol")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        });
    if let Some(protocol) = selected.as_deref() {
        // 入口协议白名单：Command Code 没有面向客户端的目录端点
        // （其模型经 claude / openai 路由或 claude/codex 映射到达），
        // 因此在此被有意拒绝。
        if matches!(protocol, "openai_compatible" | "openai_responses" | "claude") {
            return models(State(state), request, protocol).await;
        }
        return json_response(
            StatusCode::BAD_REQUEST,
            json!({"error":{"message":"Unsupported model catalog protocol."}}),
        );
    }
    if request.headers().contains_key("anthropic-version") {
        models(State(state), request, "claude").await
    } else {
        aggregate_models(State(state), request).await
    }
}
pub async fn responses_models(State(state): State<AppState>, request: Request) -> Response<Body> {
    models(State(state), request, "openai_responses").await
}
pub async fn claude_models(State(state): State<AppState>, request: Request) -> Response<Body> {
    models(State(state), request, "claude").await
}
pub async fn gemini_models(State(state): State<AppState>, request: Request) -> Response<Body> {
    models(State(state), request, "gemini").await
}
pub async fn claudecode_models(
    State(state): State<AppState>,
    request: Request,
) -> Response<Body> {
    mapped_models(State(state), request, "claude").await
}
pub async fn codex_models(State(state): State<AppState>, request: Request) -> Response<Body> {
    mapped_models(State(state), request, "openai_responses").await
}
