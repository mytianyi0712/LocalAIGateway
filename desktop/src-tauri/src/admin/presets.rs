//! admin API 域模块：供应商预设目录（provider preset 域）
//!
//! 预设是纯静态数据：前端用 `base_url` / `protocol` / `kind` 自动填充新建
//! 供应商表单。`command_code_go` 首项携带强制风险提示（路线 B 的越界面），
//! 前端必须显示并要求二次确认。

use super::*;

/// One preset: `{id,name,base_url,protocol,kind,docs_url,warning}`.
pub(super) fn provider_presets() -> Value {
    json!({
        "items": [
            {
                "id": "command_code_go",
                "name": "Command Code Go",
                "base_url": "https://api.commandcode.ai",
                "protocol": "command_code",
                "kind": "command_code",
                "auth": "browser_login",
                "docs_url": "https://commandcode.ai/docs/plans/go",
                "warning": "本套餐没有官方 API 访问：网关默认先用官方 Provider API，服务端以 403 upgrade_required 拒绝后，仅对该账号降级到 CLI 兼容路径（/alpha/generate），并使用 CLI 身份头。这是对服务端明确拒绝路径的绕过，可能违反服务条款并导致账号封禁；开启即表示已知晓并自行承担全部风险。该套餐也无法在面板创建 API Key，请使用「网页登录授权」（等价官方 cmd login）或从已登录的 CLI 导入凭据。"
            },
            {
                "id": "openai",
                "name": "OpenAI",
                "base_url": "https://api.openai.com",
                "protocol": "openai_compatible",
                "kind": null,
                "auth": null,
                "docs_url": "https://platform.openai.com/docs/api-reference",
                "warning": null
            },
            {
                "id": "openai_responses",
                "name": "OpenAI Responses",
                "base_url": "https://api.openai.com",
                "protocol": "openai_responses",
                "kind": null,
                "auth": null,
                "docs_url": "https://platform.openai.com/docs/api-reference/responses",
                "warning": null
            },
            {
                "id": "anthropic",
                "name": "Anthropic",
                "base_url": "https://api.anthropic.com",
                "protocol": "claude",
                "kind": null,
                "auth": null,
                "docs_url": "https://docs.anthropic.com/en/api/messages",
                "warning": null
            },
            {
                "id": "gemini",
                "name": "Google Gemini",
                "base_url": "https://generativelanguage.googleapis.com",
                "protocol": "gemini",
                "kind": null,
                "auth": null,
                "docs_url": "https://ai.google.dev/api/generate-content",
                "warning": null
            }
        ]
    })
}

pub(super) async fn list_provider_presets(_: AdminAuth) -> ApiResult {
    Ok(ok(provider_presets()))
}
