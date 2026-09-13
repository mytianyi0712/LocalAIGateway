//! admin API 域模块：Command Code 网页登录授权（handler 薄壳）。
//!
//! 登录流程本身在 `crate::commandcode_login`（回环回调 + whoami 校验）。
//! 这里只做输入规范化与状态透出；**密钥绝不经过这些端点**——成功时只返回
//! 一次性 `login_id`，由 `POST /channels`（或 PATCH）在服务端取走并加密入库。

use super::*;
use axum::extract::State;
use serde::Deserialize;

#[derive(Deserialize, Default)]
pub struct CommandCodeLoginInput {
    /// 渠道/provider 的 API 根地址；与 Studio 基址成对映射（staging/localhost）。
    /// 缺省用官方 `https://api.commandcode.ai`。
    #[serde(default)]
    pub api_base: Option<String>,
}

fn normalize_login_api_base(input: Option<String>) -> Result<String, ApiError> {
    match input
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(value) => normalize_base_url(value)
            .map_err(|error| ApiError::validation(error.to_string())),
        None => Ok(crate::commandcode::DEFAULT_API_BASE.to_owned()),
    }
}

fn login_json(state: &Context) -> Value {
    json!({
        "login": state.command_code_login.status(),
        "cli_key_available": state.command_code_login.cli_key_available(),
    })
}

/// 开始（或加入）一次浏览器登录；返回待打开的 Studio 授权 URL。
pub(super) async fn command_code_login_begin(
    _: AdminAuth,
    State(state): State<Context>,
    Json(input): Json<CommandCodeLoginInput>,
) -> ApiResult {
    let api_base = normalize_login_api_base(input.api_base)?;
    state
        .command_code_login
        .begin(crate::commandcode_login::LoginConfig {
            api_base,
            ..Default::default()
        })
        .await
        .map_err(|error| ApiError::validation(error.to_string()))?;
    Ok(ok(login_json(&state)))
}

pub(super) async fn command_code_login_status(
    _: AdminAuth,
    State(state): State<Context>,
) -> ApiResult {
    Ok(ok(login_json(&state)))
}

pub(super) async fn command_code_login_cancel(
    _: AdminAuth,
    State(state): State<Context>,
) -> ApiResult {
    state.command_code_login.cancel();
    Ok(ok(login_json(&state)))
}

/// 官方 CLI 凭据导入（`~/.commandcode/auth.json` / `COMMANDCODE_API_KEY`），
/// 走与网页登录相同的 whoami 校验与一次性交接。
pub(super) async fn command_code_login_import_cli(
    _: AdminAuth,
    State(state): State<Context>,
    Json(input): Json<CommandCodeLoginInput>,
) -> ApiResult {
    let api_base = normalize_login_api_base(input.api_base)?;
    state
        .command_code_login
        .import_cli_key(&api_base, None)
        .await
        .map_err(|error| ApiError::validation(error.to_string()))?;
    Ok(ok(login_json(&state)))
}
