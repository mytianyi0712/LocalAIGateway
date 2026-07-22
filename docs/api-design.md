# API 设计

状态：设计基线  
更新日期：2026-07-21

## 1. API 分区

| 分区 | 前缀 | 用途 |
| --- | --- | --- |
| 管理 API | `/api/admin/v1` | 供应商、渠道、模型、路由、日志和设置 |
| OpenAI Compatible | `/v1/chat/completions` 等 | 原生协议透明代理 |
| OpenAI Responses | `/v1/responses` | 原生协议透明代理 |
| Claude | `/v1/messages` | 原生协议透明代理 |
| Gemini | `/v1beta/models/...` | 原生协议透明代理 |
| 管理页面 | `/` | 原生 HTML、CSS、JavaScript 单页管理台 |

管理 API 有独立版本号。代理入口保持上游 SDK 预期路径，不增加网关专用前缀。

## 2. 通用约定

### 2.1 管理 API 访问控制

“信任局域网访问”默认开启，此时管理 API 不要求认证。关闭该设置后才要求：

```http
Authorization: Bearer <admin_token>
```

未提供或密钥无效时返回 `401`。管理 API 只接受 `application/json`。

### 2.2 管理 API 响应

单资源直接返回对象，列表统一返回：

```json
{
  "items": [],
  "total": 0,
  "page": 1,
  "page_size": 50
}
```

管理 API 错误统一返回：

```json
{
  "error": {
    "code": "priority_conflict",
    "message": "Priority 0 already exists in this route.",
    "details": {}
  }
}
```

管理 API 使用以下状态码：

- `200` 查询或更新成功
- `201` 创建成功
- `202` 后台任务已接受
- `204` 删除成功
- `400` 请求格式错误
- `404` 资源不存在
- `409` 唯一约束、状态或关联冲突
- `422` 字段校验失败

### 2.3 分页与时间

- `page` 从 1 开始，默认 1。
- `page_size` 默认 50，最大 200。
- 所有 API 时间使用带 `Z` 的 UTC ISO 8601，例如 `2026-07-20T08:30:00Z`。
- 图表接口使用半开时间区间 `[from, to)`。

### 2.4 密钥字段

- 创建渠道时 `api_key` 必填。
- 普通读取只返回 `has_api_key` 和 `api_key_hint`。
- 更新渠道时缺少 `api_key` 表示保留原密钥。
- 空字符串不表示删除密钥，避免界面误操作。

## 3. 供应商 API

### 3.1 资源

```json
{
  "id": "provider-uuid",
  "name": "OpenAI Official",
  "base_url": "https://api.openai.com",
  "channel_count": 2,
  "created_at": "2026-07-20T08:30:00Z",
  "updated_at": "2026-07-20T08:30:00Z"
}
```

### 3.2 端点

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/providers` | 分页查询供应商 |
| `POST` | `/providers` | 创建供应商 |
| `GET` | `/providers/{provider_id}` | 查询详情 |
| `PATCH` | `/providers/{provider_id}` | 修改名称或 Base URL |
| `DELETE` | `/providers/{provider_id}` | 删除无渠道的供应商 |

创建请求：

```json
{
  "name": "OpenAI Official",
  "base_url": "https://api.openai.com"
}
```

Base URL 保存前执行语法校验，并移除尾部 `/`、`/v1` 或 `/v1beta`，不主动访问网络。修改 Base URL 不自动触发模型探测。

## 4. 渠道 API

### 4.1 资源

```json
{
  "id": "channel-uuid",
  "provider_id": "provider-uuid",
  "name": "personal-account",
  "protocol": "openai_compatible",
  "protocols": ["openai_compatible", "openai_responses"],
  "manual_enabled": true,
  "health_check_model_id": "gpt-4.1-mini",
  "has_api_key": true,
  "api_key_hint": "...9abc",
  "health": {
    "state": "active",
    "consecutive_failures": 0,
    "disabled_until": null,
    "last_success_at": null,
    "last_failure_at": null
  },
  "model_count": 12,
  "created_at": "2026-07-20T08:30:00Z",
  "updated_at": "2026-07-20T08:30:00Z"
}
```

### 4.2 端点

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/channels` | 查询渠道，支持供应商、协议、状态筛选 |
| `POST` | `/channels` | 创建渠道并加密 API Key |
| `GET` | `/channels/{channel_id}` | 查询渠道及健康详情 |
| `PATCH` | `/channels/{channel_id}` | 修改名称、协议、开关或探测模型 |
| `PUT` | `/channels/{channel_id}/api-key` | 替换 API Key |
| `DELETE` | `/channels/{channel_id}` | 删除未绑定路由的渠道 |
| `POST` | `/channels/{channel_id}/reset-health` | 清零自动熔断并恢复 active |
| `POST` | `/channels/{channel_id}/probe` | 立即发起一次健康探测 |

修改渠道协议集合时，共用上游 `/v1/models` 目录的 OpenAI Compatible 与 OpenAI Responses 会立即同步已有模型的协议绑定。管理端随后自动发起模型探测，以同步 Claude、Gemini 等独立目录的实际模型能力；不自动创建、删除或重排路由候选。

创建请求：

```json
{
  "provider_id": "provider-uuid",
  "name": "personal-account",
  "protocols": ["openai_compatible", "openai_responses"],
  "api_key": "secret-value",
  "manual_enabled": true,
  "health_check_model_id": null
}
```

渠道至少选择一个协议。关闭某协议时，如果已有该协议路由候选，返回 `409`，需要先解除绑定；增加协议不会影响已有路由。

手动探测返回 `202`：

```json
{
  "probe_id": "probe-uuid",
  "status": "queued"
}
```

## 5. 模型发现 API

### 5.1 面向客户端的模型目录

`GET /v1/models` 默认返回聚合 OpenAI 兼容列表：

```json
{
  "object": "list",
  "data": [{
    "id": "model-id",
    "object": "model",
    "created": 1784600000,
    "owned_by": "local-ai-gateway",
    "x_local_gateway": {
      "supported_endpoints": ["/v1/chat/completions", "/v1/responses"]
    }
  }]
}
```

命名空间扩展不会改变 `data[].id` 等 OpenAI 标准字段，客户端可以安全忽略。显式 `protocol` 查询、`X-Local-Gateway-Protocol` 请求头以及各协议兼容别名继续返回单协议目录。

### 5.2 管理端点

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/channels/{channel_id}/discover-models` | 异步执行上游模型探测 |
| `GET` | `/channels/{channel_id}/discovery-runs` | 查询该渠道探测历史 |
| `GET` | `/discovery-runs/{run_id}` | 查询单次探测状态 |
| `GET` | `/channel-models` | 查询所有渠道模型 |
| `POST` | `/channels/{channel_id}/models` | 手动添加模型 |
| `PATCH` | `/channel-models/{channel_model_id}` | 修改手动显示名或可用状态 |
| `DELETE` | `/channel-models/{channel_model_id}` | 删除未被路由引用的手动模型 |

触发探测返回：

```json
{
  "run_id": "discovery-uuid",
  "status": "queued"
}
```

探测状态：`queued`、`running`、`succeeded`、`failed`。失败响应只保存标准错误分类和上游状态码，不保存上游错误正文。

手动添加模型：

```json
{
  "model_id": "gpt-4.1-mini",
  "display_name": "GPT 4.1 Mini"
}
```

## 6. 模型路由 API

### 6.1 路由资源

```json
{
  "id": "route-uuid",
  "route_ids": {
    "openai_compatible": "chat-route-uuid",
    "openai_responses": "responses-route-uuid"
  },
  "protocols": ["openai_compatible", "openai_responses"],
  "requested_model_id": "gpt-4.1-mini",
  "enabled": true,
  "candidates": [
    {
      "channel_model_id": "channel-model-a",
      "channel_id": "channel-a",
      "provider_name": "Provider A",
      "priority": 0,
      "protocols": ["openai_compatible", "openai_responses"],
      "enabled": true,
      "health_state": "active"
    },
    {
      "channel_model_id": "channel-model-b",
      "channel_id": "channel-b",
      "provider_name": "Provider B",
      "priority": 1,
      "protocols": ["openai_responses"],
      "enabled": true,
      "health_state": "open"
    }
  ]
}
```

### 6.2 端点

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/routes` | 查询模型路由 |
| `POST` | `/routes` | 创建模型路由 |
| `GET` | `/routes/{route_id}` | 查询路由和候选状态 |
| `PATCH` | `/routes/{route_id}` | 修改路由总开关 |
| `PUT` | `/routes/{route_id}/candidates` | 原子替换完整候选列表与优先级 |
| `DELETE` | `/routes/{route_id}` | 删除路由及候选绑定 |

创建请求：

```json
{
  "requested_model_id": "gpt-4.1-mini",
  "enabled": true
}
```

替换候选列表：

```json
{
  "candidates": [
    {"channel_model_id": "channel-model-a", "priority": 0, "enabled": true},
    {"channel_model_id": "channel-model-b", "priority": 1, "enabled": true}
  ]
}
```

管理 API 将相同 `requested_model_id` 的内部协议路由合并成一个逻辑资源。服务端在一个事务中验证并替换列表：

- `priority` 不重复且大于等于 0。
- `channel_model_id` 不重复。
- 每个渠道模型至少支持逻辑路由的一个协议。
- 每个渠道模型的原始模型 ID 与 `requested_model_id` 一致。
- 同一优先级会写入该渠道实际支持的各内部协议路由。例如 A=`0`、B=`2` 支持 Chat/Responses，C=`1` 仅支持 Responses：Chat 顺序为 A→B，Responses 顺序为 A→C→B。

任一条件不满足时整个操作失败，原顺序保持不变。

## 7. 日志 API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/requests` | 查询请求日志 |
| `GET` | `/requests/{request_id}` | 查询请求及全部渠道尝试 |
| `GET` | `/health-probes` | 查询健康探测日志 |
| `DELETE` | `/logs` | 按确认参数清空日志 |

请求日志筛选参数：

- `from`、`to`
- `protocol`
- `model_id`
- `channel_id`
- `outcome`
- `status_code`
- `min_duration_ms`
- `page`、`page_size`

日志详情不返回请求、响应或错误正文。`raw_usage_json` 仅包含协议适配器识别到的 usage 对象。

清空日志要求显式确认：

```http
DELETE /api/admin/v1/logs?before=2026-07-01T00:00:00Z&confirm=true
```

## 8. 统计 API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/stats/summary` | 总请求、成功率、平均延迟、Token 汇总及按协议类别的缓存命中率 |
| `GET` | `/stats/timeseries` | 按小时或天聚合的趋势 |
| `GET` | `/stats/models` | 按协议和模型聚合 |
| `GET` | `/stats/channels` | 按渠道聚合成功率、延迟和故障转移 |
| `GET` | `/stats/cache` | 四类 Token 统计 |

通用参数为 `from`、`to` 和可选的 `protocol`、`model_id`、`channel_id`。`timeseries` 额外接受 `interval=hour|day`。

缓存统计响应：

```json
{
  "cache_read_tokens": 1200,
  "cache_write_tokens": 320,
  "cache_miss_input_tokens": 840,
  "output_tokens": 640,
  "unknown_attempts": 4
}
```

聚合时忽略 `null` Token，同时返回未知记录数，避免把未知误解为零。所有统计接口明确不包含价格字段。

## 9. 设置与系统 API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/settings` | 查询可运行时修改的设置 |
| `PATCH` | `/settings` | 原子更新设置 |
| `POST` | `/settings/access-keys/generate` | 随机生成并加密保存管理与代理密钥；明文只返回一次 |
| `GET` | `/system/status` | 数据库、日志队列和后台任务状态 |
| `GET` | `/system/protocols` | 返回协议枚举和支持的入口 |

设置更新示例：

```json
{
  "trust_local_network": true,
  "failure_threshold": 3,
  "circuit_open_seconds": 900,
  "max_failover_attempts": 3,
  "connect_timeout_seconds": 10,
  "first_byte_timeout_seconds": 60,
  "first_token_timeout_seconds": 60,
  "stream_idle_timeout_seconds": 300,
  "non_stream_total_timeout_seconds": 600,
  "model_discovery_interval_hours": 24,
  "log_retention_days": 30
}
```

设置更新只影响更新后开始的请求。

## 10. 代理入口契约

### 10.1 协议识别

| 请求路径 | 固定协议 |
| --- | --- |
| `/v1/chat/completions` | `openai_compatible` |
| `/v1/completions` | `openai_compatible` |
| `/v1/embeddings` | `openai_compatible` |
| `/v1/responses` | `openai_responses` |
| `/v1/messages` | `claude` |
| `/v1beta/models/{model}:generateContent` | `gemini` |
| `/v1beta/models/{model}:streamGenerateContent` | `gemini` |

不会因为 OpenAI Compatible 渠道声称支持 `/v1/responses` 就把它加入 Responses 路由池。渠道配置的协议枚举必须精确匹配入口。

模型目录端点：

| 协议池 | 方法与路径 | 返回格式 |
| --- | --- | --- |
| OpenAI Compatible | `GET /v1/models` | OpenAI model list |
| OpenAI Responses | `GET /v1/responses/models` 或 `GET /v1/models?protocol=openai_responses` | OpenAI model list |
| Claude | `GET /v1/messages/models` 或携带 `anthropic-version` 的 `GET /v1/models` | Claude model list |
| Gemini | `GET /v1beta/models` | Gemini model list |

`GET /v1/models` 也接受 `X-Local-Gateway-Protocol` 显式指定 `openai_compatible`、`openai_responses` 或 `claude`。未指定时聚合所有协议中已配置且存在可用候选的模型路由，按模型 ID 去重。`x_local_gateway` 仅保留 `supported_endpoints`；OpenAI Compatible 只广告 `/v1/chat/completions`，不猜测 `/v1/embeddings` 能力。

### 10.2 代理访问控制

“信任局域网访问”默认开启，以下所有代理入口均可不传本地密钥；即使客户端传入任意 API Key，本地网关也不拦截，并会在发送上游前替换为渠道 API Key。

关闭信任后，代理密钥可放在以下位置：

- OpenAI 两类入口：`Authorization: Bearer <gateway_key>`。
- Claude：`x-api-key: <gateway_key>`。
- Gemini：`x-goog-api-key: <gateway_key>` 或 `?key=<gateway_key>`。
- 所有入口也接受 `X-Local-Gateway-Key: <gateway_key>`。

本地凭据在发送上游前被移除或替换。其他端到端头和查询参数保持不变。

### 10.3 上游响应

上游 HTTP 响应遵循以下规则：

- 成功响应的状态码、端到端响应头和响应体原样转发。
- 所有候选失败时，最终选定的上游 HTTP 错误原样转发。
- 网关不在上游响应体中加入渠道、尝试次数或统计信息。
- 网关不把中间失败响应发送给客户端。
- HTTP 重定向不自动跟随，可作为最终上游响应或按非成功尝试切换。

### 10.4 网关自身错误

只有认证失败、模型无法解析、没有路由、无可用渠道、请求体过大和传输失败等本地情况由网关生成响应。为兼容 SDK，错误外形按入口协议生成。

OpenAI Compatible / Responses：

```json
{
  "error": {
    "message": "No active channel is available for this model.",
    "type": "gateway_error",
    "code": "no_active_channel"
  }
}
```

Claude：

```json
{
  "type": "error",
  "error": {
    "type": "gateway_error",
    "message": "No active channel is available for this model."
  }
}
```

Gemini：

```json
{
  "error": {
    "code": 503,
    "message": "No active channel is available for this model.",
    "status": "UNAVAILABLE"
  }
}
```

生成错误只包含稳定错误码和内部请求 ID，不暴露 Base URL、API Key、堆栈或上游错误正文。

## 11. 前端页面与 API 对应关系

| 页面 | 主要 API |
| --- | --- |
| 概览 | `/stats/summary`、`/stats/timeseries`、`/system/status` |
| 供应商与渠道 | `/providers`、`/channels`、模型探测与手动探测 API |
| 模型路由 | `/routes`、`/routes/{id}/candidates` |
| 请求日志 | `/requests`、`/requests/{id}` |
| 健康状态 | `/channels`、`/health-probes`、`reset-health`、`probe` |
| 设置 | `/settings` |

前端删除操作必须显示实际关联数量；清空日志必须二次确认。渠道状态使用文字和图标共同表达，不只依赖颜色。
