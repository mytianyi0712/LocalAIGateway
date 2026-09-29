//! 统一错误模型与请求关联中间件：`ApiError` 把状态码、稳定的机器可读错误码
//! 与消息打包为 JSON 响应；`correlation_middleware` 为每个管理端响应附加
//! `request_id`，并把非 JSON 的拒绝也规范成同一错误信封。
//!
//! 边界：本模块只负责响应形态与关联 id，不决定业务错误语义（由各 handler 决定）。
//! 关键不变量：内部错误文本绝不外泄（只进日志）；每个状态码都有显式错误码映射。
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use tracing::Instrument;

/// 稳定的机器可读错误码，序列化为 kebab-case。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    BadRequest,
    Unauthorized,
    Forbidden,
    NotFound,
    Conflict,
    Validation,
    TooManyRequests,
    RequestTooLarge,
    Internal,
    ServiceUnavailable,
    BadGateway,
    GatewayTimeout,
    NotImplemented,
    ConfigCorrupted,
    /// 手动为未配置余额适配器的渠道发起余额查询时使用
    /// （默认关闭的语义）。
    BalanceNotConfigured,
}

impl ErrorCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorCode::BadRequest => "bad_request",
            ErrorCode::Unauthorized => "unauthorized",
            ErrorCode::Forbidden => "forbidden",
            ErrorCode::NotFound => "not_found",
            ErrorCode::Conflict => "conflict",
            ErrorCode::Validation => "validation",
            ErrorCode::TooManyRequests => "too_many_requests",
            ErrorCode::RequestTooLarge => "request_too_large",
            ErrorCode::Internal => "internal",
            ErrorCode::ServiceUnavailable => "service_unavailable",
            ErrorCode::BadGateway => "bad_gateway",
            ErrorCode::GatewayTimeout => "gateway_timeout",
            ErrorCode::NotImplemented => "not_implemented",
            ErrorCode::ConfigCorrupted => "config_corrupted",
            ErrorCode::BalanceNotConfigured => "balance_not_configured",
        }
    }
}

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: ErrorCode,
    pub message: String,
}

/// 由关联中间件注入的请求 id 扩展，使 handler（及后续日志路径）
/// 能引用客户端可见的 id。
#[derive(Debug, Clone)]
pub struct RequestId(pub String);

/// 给定状态码的稳定机器可读错误码，`ApiError::new` 与中间件的非 JSON
/// 错误信封都会用到。网关实际产生的每个状态码都有显式映射；
/// 其余状态码会记一条告警日志，而不是静默塌缩为 `internal`。
fn code_for_status(status: StatusCode) -> ErrorCode {
    match status {
        StatusCode::BAD_REQUEST => ErrorCode::BadRequest,
        StatusCode::UNAUTHORIZED => ErrorCode::Unauthorized,
        StatusCode::FORBIDDEN => ErrorCode::Forbidden,
        StatusCode::NOT_FOUND => ErrorCode::NotFound,
        StatusCode::CONFLICT => ErrorCode::Conflict,
        StatusCode::UNPROCESSABLE_ENTITY => ErrorCode::Validation,
        StatusCode::TOO_MANY_REQUESTS => ErrorCode::TooManyRequests,
        StatusCode::PAYLOAD_TOO_LARGE => ErrorCode::RequestTooLarge,
        StatusCode::INTERNAL_SERVER_ERROR => ErrorCode::Internal,
        StatusCode::SERVICE_UNAVAILABLE => ErrorCode::ServiceUnavailable,
        StatusCode::BAD_GATEWAY => ErrorCode::BadGateway,
        StatusCode::GATEWAY_TIMEOUT => ErrorCode::GatewayTimeout,
        StatusCode::NOT_IMPLEMENTED => ErrorCode::NotImplemented,
        _ => {
            tracing::warn!(
                status = %status,
                "unmapped error status; falling back to internal code"
            );
            ErrorCode::Internal
        }
    }
}

impl ApiError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            code: code_for_status(status),
            message: message.into(),
        }
    }
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }
    pub fn unauthorized() -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "Unauthorized")
    }
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, message)
    }
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, message)
    }
    pub fn validation(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNPROCESSABLE_ENTITY, message)
    }
    /// 未保存适配器时的手动余额查询：422 加稳定错误码，
    /// 使 UI 能提示“配置余额”而非泛化错误。
    pub fn balance_not_configured() -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            code: ErrorCode::BalanceNotConfigured,
            message: "balance_not_configured".into(),
        }
    }
    pub fn not_implemented(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_IMPLEMENTED, message)
    }
    /// 内部故障绝不泄露底层错误文本：它只进日志，客户端看到固定消息。
    pub fn internal(error: impl std::fmt::Display) -> Self {
        tracing::error!(error = %error, "internal error");
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: ErrorCode::Internal,
            message: "Internal server error".into(),
        }
    }
    /// 访问密钥存储损坏；管理端 UI 需要独立信号，
    /// 以便提示重新生成，而不是让设置页直接失败。
    pub fn config_corrupted() -> Self {
        Self::config_corrupted_with("访问密钥配置损坏，请重新生成访问密钥")
    }

    /// 任一持久化配置行结构损坏。消息只点名表/键，绝不包含存储的值。
    pub fn config_corrupted_with(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: ErrorCode::ConfigCorrupted,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({"code": self.code.as_str(), "message": self.message})),
        )
            .into_response()
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        Self::internal(error)
    }
}
impl From<sqlx::Error> for ApiError {
    fn from(error: sqlx::Error) -> Self {
        Self::internal(error)
    }
}
impl From<serde_json::Error> for ApiError {
    fn from(_error: serde_json::Error) -> Self {
        // 泛化处理，使存储的 JSON 文本绝不泄露到响应里。
        Self::validation("Invalid JSON data")
    }
}

/// 为每个带有 `request_id` 的管理端响应（客户端提供的 `x-request-id`
/// 或新生成的 UUID）附加该 id，并把它注入 JSON 错误体，使失败可端到端追溯。
/// 非 JSON 的拒绝（Axum 提取器失败、管理端 404）会被重建为稳定 JSON 信封，
/// 保持同样的 `code`/`message`/`request_id` 形状。
pub async fn correlation_middleware(
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let request_id = request
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let path = request.uri().path().to_owned();
    request
        .extensions_mut()
        .insert(RequestId(request_id.clone()));
    // handler 内的每个事件（包括 `ApiError::internal` 的错误日志）都继承该
    // span，因此客户端可见的 request id 能定位到携带真实底层错误的日志行。
    let span = tracing::info_span!("admin_request", request_id = %request_id, path = %path);
    let mut response = next.run(request).instrument(span).await;
    if let Ok(value) = axum::http::HeaderValue::from_str(&request_id) {
        response.headers_mut().insert("x-request-id", value);
    }
    if response.status().is_client_error() || response.status().is_server_error() {
        let is_json = response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.contains("application/json"));
        let (mut parts, body) = response.into_parts();
        if is_json {
            match axum::body::to_bytes(body, 1024 * 1024).await {
                Ok(bytes) => match serde_json::from_slice::<Value>(&bytes) {
                    Ok(mut value) => {
                        if let Some(object) = value.as_object_mut() {
                            object.insert("request_id".into(), json!(request_id));
                        }
                        // 重建：content-type 由 `Json` 重新设置，过期的
                        // content-length 丢弃，其余头保留——且错误状态码必须
                        // 在重建后依然保持。
                        let status = parts.status;
                        response = axum::Json(value).into_response();
                        *response.status_mut() = status;
                        parts.headers.remove(axum::http::header::CONTENT_LENGTH);
                        parts.headers.remove(axum::http::header::CONTENT_TYPE);
                        for (name, value) in parts.headers {
                            if let Some(name) = name {
                                response.headers_mut().append(name, value);
                            }
                        }
                    }
                    Err(_) => {
                        response =
                            axum::http::Response::from_parts(parts, axum::body::Body::from(bytes));
                    }
                },
                Err(_) => {
                    response = axum::http::Response::from_parts(parts, axum::body::Body::empty());
                }
            }
        } else {
            // 非 JSON 的拒绝（提取器失败、管理端 404）：规范化为稳定错误信封。
            // `message` 取 HTTP 规范的 canonical reason——固定文本，不含内部信息。
            let status = parts.status;
            let body = json!({
                "code": code_for_status(status).as_str(),
                "message": status.canonical_reason().unwrap_or("error"),
                "request_id": request_id,
            });
            response = (status, axum::Json(body)).into_response();
            parts.headers.remove(axum::http::header::CONTENT_LENGTH);
            parts.headers.remove(axum::http::header::CONTENT_TYPE);
            for (name, value) in parts.headers {
                if let Some(name) = name {
                    response.headers_mut().append(name, value);
                }
            }
        }
    }
    if response.status().is_server_error() {
        tracing::error!(
            request_id = %request_id,
            status = %response.status().as_u16(),
            path = %path,
            "admin request failed"
        );
    }
    response
}

pub fn json_response(status: StatusCode, value: Value) -> Response {
    (status, Json(value)).into_response()
}
