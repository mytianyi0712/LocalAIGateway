use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use tracing::Instrument;

/// Stable machine-readable error code, serialized as kebab-case.
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
        }
    }
}

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: ErrorCode,
    pub message: String,
}

/// Request-id extension injected by the correlation middleware, so handlers
/// (and future log paths) can reference the client-visible id.
#[derive(Debug, Clone)]
pub struct RequestId(pub String);

/// Stable machine-readable code for a status, used both by `ApiError::new`
/// and by the middleware's non-JSON error envelope (P2-3). Every status the
/// gateway actually produces has an explicit mapping (P2-7); anything else
/// logs a warning instead of silently collapsing into `internal`.
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
    pub fn not_implemented(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_IMPLEMENTED, message)
    }
    /// Internal failures never leak the underlying error text: it goes to the
    /// logs only, and the client sees a fixed message.
    pub fn internal(error: impl std::fmt::Display) -> Self {
        tracing::error!(error = %error, "internal error");
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: ErrorCode::Internal,
            message: "Internal server error".into(),
        }
    }
    /// The access-key store is corrupt; the admin UI needs a distinct signal
    /// so it can offer regeneration instead of failing the settings page.
    pub fn config_corrupted() -> Self {
        Self::config_corrupted_with("访问密钥配置损坏，请重新生成访问密钥")
    }

    /// Any persisted configuration row is structurally corrupt. The message
    /// names the table/key only — never the stored value.
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
        // Generalized so stored JSON text never leaks into responses.
        Self::validation("Invalid JSON data")
    }
}

/// Attaches `request_id` to every admin response that carries one (client-
/// supplied `x-request-id` or a fresh UUID), and injects it into JSON error
/// bodies so failures are traceable end to end. Non-JSON rejections (Axum
/// extractor failures, admin 404s) are rebuilt as a stable JSON envelope
/// with the same `code`/`message`/`request_id` shape (P2-3).
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
    // P2-7: every event inside the handler (including `ApiError::internal`'s
    // error log) inherits this span, so the client-visible request id can
    // locate the log lines that carry the real underlying error.
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
                        // Rebuild: content-type is re-set by `Json`, the stale
                        // content-length is dropped, every other header is kept —
                        // and the error status must survive the rebuild.
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
            // Non-JSON rejection (extractor failure, admin 404): normalize to
            // the stable error envelope. `message` is the HTTP spec's
            // canonical reason — fixed text, no internals.
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
