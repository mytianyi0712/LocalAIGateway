//! 路由领域层：上游回退协议映射与按协议组装目录入口端点。
//!
//! 边界：本模块只做纯映射，不读写数据库、不发起请求。
//! 共享的数据形状（`Candidate` / `RoutableModel` / `MappingTarget`）已移入
//! `domain`，此处不再定义类型。
//! 关键不变量：入口端点顺序由调用方保证（生产调用方在 SQL 中按
//! `PROTOCOL_ORDER` 排序）；本模块只按输入顺序映射并丢弃未知协议。

/// 客户端入口在没有专属路由时可以静默回退到的上游协议。
///
/// 目前是三种客户端入口协议 → `command_code`：这三种入口都有对应的请求与流式
/// 转换器，所以直接发往 `/v1/chat/completions`、`/v1/messages`、`/v1/responses`
/// 的请求无需 Claude/Codex 映射即可驱动 `command_code` 路由。`gemini` 不在其中
/// ——它没有指向 `command_code` 的转换器。
pub fn fallback_upstream_protocol(entry: &str) -> Option<&'static str> {
    // 判定来自转换注册表（`converts_to_command_code`），不再维护第二份白名单。
    crate::protocol::converts_to_command_code(entry).then_some("command_code")
}

/// 每个协议在模型目录中公布的主代理入口点。
/// OpenAI Compatible 只公布 `/v1/chat/completions`——embeddings 与 completions
/// 刻意不做猜测（见 requirements.md 3.7）。
/// Command Code 没有面向客户端的入口端点（只能经 claude / openai 路由或
/// claude/codex 映射抵达），因此从目录中剔除。
pub fn protocol_main_endpoint(protocol: &str, model_id: &str) -> Option<String> {
    crate::protocol::main_path(protocol, model_id, false)
}

/// 该模型当前可经其路由的全部主入口点：至少有一个存活候选的已启用路由协议。
/// 它镜像 `list_routable_models` 的候选存在性过滤，因此只有当路由确实可调用时
/// 端点才会出现。
/// 这是 `RouteRepository::routable_endpoints_for_model` 使用的纯映射
/// （并有单测覆盖）：未知协议被丢弃，已知协议保持输入顺序——顺序由调用方保证
/// （生产调用方在 SQL 中按 `PROTOCOL_ORDER` 排序）。
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
        // embeddings/completions 不做猜测。
        assert_eq!(
            endpoints_for_protocols(&["bogus"], "any-model"),
            Vec::<String>::new()
        );
    }
}
