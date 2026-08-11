# Rust 网关 vs Python 后端：功能差异审计报告

> **历史快照（2026-08-04）**：本文是 Rust 重写初期的差异审计。此后
> `backend/`（Python 参考实现）已从交付物中移除，Rust 网关成为唯一实现，
> 报告中的 **Critical 项（C1–C6）与大部分 Major 项已在此后各轮整改中
> 关闭**（详见 `code-review-report.md` 的复核记录与当前 `architecture.md`）：
> 映射入口 404、映射流式增量转换、熔断自动半开恢复、定时发现/日志保留/
> 陈旧请求回收/启动修复、能力检测、转换层工具/图片/thinking/DSML 语义等
> 均已落地。本文不再作为当前行为基线，仅保留历史审计结论供追溯。
>
> 当前架构与行为基线请以 `docs/architecture.md` 与源码为准。

- 审计日期：2026-08-04（历史快照）
- 审计对象：
  - Python 参考实现：`backend/`（FastAPI，`app/main.py` 入口，v0.2.1）
  - Rust 网关：`desktop/src-tauri/`（axum + Tauri，`server.rs` 建路由）
- 审计方法：逐文件阅读两侧全部端点处理器与支撑模块（约 16k 行），对每个端点比对方法/路径、鉴权、请求/响应结构、状态码、错误消息、副作用与后台任务；关键结论均直接引用两侧源码。

---

## 1. 总体结论

1. **路由面完全对齐**：Rust 复刻了 Python 的全部 79 个公开路径（59 个管理端点 + 19 个代理/目录端点 + `/api/health`），无缺失端点、无多余端点。
2. **但"复刻"停留在路由层**。在行为层存在大量差异，其中 **6 项严重（Critical）** 会直接影响客户端兼容性或核心功能：
   - 映射入口（`/claudecode`、`/codex`）未知模型行为不同（Rust 直接按入口协议转发，Python 返回 404）；
   - Rust 映射流式响应整体缓冲后再转换，不是逐事件流式（TTFB 与首 token 行为完全不同）；
   - 熔断后**无自动探测恢复**（Python 有 HealthSupervisor 自动半开探测；Rust 渠道会永久停在 open 状态直到人工操作）；
   - 定时模型发现、日志保留清理、陈旧 pending 请求回收、启动修复等维护任务**整体缺失**（Rust 中对应设置项是死配置）；
   - 模型能力检测（capability detection）**未移植**：Rust 的 detect 端点是空写入，`/v1/models` 目录也不做自动检测；
   - 转换层（convert）大量语义丢失：工具调用（tool_use/function_call）、图片、thinking、DSML、tool_choice、stop_reason 映射、响应信封字段等。
3. 另有 **1 个确定性 Bug**：Rust `list_mapping_models` 的 `"openai_responses"` vs `"codex"` 分支不匹配，导致 **`GET /codex/v1/models` 与 `/codex/v1/responses/models` 永远返回空列表**。
4. 严重程度分级：Critical 6 项、Major 18 项、Minor 若干，详见第 9 节。

---

## 2. 路由面覆盖（完全相同）

### 2.1 代理入口（两侧一致）

| 方法/路径 | Python 处理器 | Rust 处理器 |
|---|---|---|
| GET `/v1/models` | `openai_or_claude_models` | `openai_models`（行为不同，见 4.3） |
| GET `/v1/responses/models` | `openai_responses_models` | `responses_models` |
| GET `/v1/messages/models` | `claude_models` | `claude_models`（响应形状不同） |
| GET `/v1beta/models` | `gemini_models` | `gemini_models`（响应形状不同） |
| POST `/v1/chat/completions` `/v1/completions` `/v1/embeddings` | `openai_compatible_proxy` | `openai` |
| POST `/v1/responses` | `openai_responses_proxy` | `responses` |
| POST `/v1/messages` | `claude_proxy` | `claude` |
| POST `/v1beta/models/{model_action:path}` | `gemini_proxy`（带 404 校验） | `gemini`（无校验） |
| GET `/claudecode` | `claudecode_info` | `claudecode_info`（响应体不同） |
| GET `/claudecode/v1/models`、`/claudecode/v1/messages/models` | `claudecode_models` | `claudecode_models` |
| POST `/claudecode/v1/messages` | `proxy_mapped_entry` | `claudecode` |
| GET `/codex` | `codex_info` | `codex_info`（响应体不同） |
| GET `/codex/v1/models`、`/codex/v1/responses/models` | `codex_models` | `codex_models`（**恒为空列表，Bug**） |
| POST `/codex/v1/responses` | `proxy_codex_entry` | `codex` |
| GET `/api/health` | `{"status":"ok"}` | `{"status":"ok","runtime":"rust"}`（多一个字段） |

### 2.2 管理端点（两侧一一对应，共 41 组路径）

providers / channels / api-key / reset-health / probe / discover-models / discovery-runs / channel-models / channels/{id}/models / routes / candidates / capability-profiles / model-capabilities(detect) / claude-presets(+refresh) / claude-mappings / codex-presets(+refresh) / codex-mappings / requests / health-probes / logs / stats(summary|cache|models|channels|timeseries) / settings(+access-keys/generate) / system(status|protocols)，前缀均为 `/api/admin/v1`。

---

## 3. 严重（Critical）差异

### C1. 映射入口（/claudecode、/codex）未知模型的行为

- Python（`services/proxy.py` `proxy_mapped_entry`）：映射表中不存在或 `enabled=false` 时返回 **404** `unknown_mapped_model`，消息 `Model '<id>' is not a configured model mapping.`，并写入 `request_finish` 遥测。
- Rust（`proxy.rs`）：`resolve_mapping` 查不到时 `mapping = None`，**直接按入口协议当普通路由继续**——若系统里恰好存在同名模型则成功转发，否则 503。客户端得不到"模型未配置映射"的明确错误。
- 影响：错误语义与 Python 不一致，客户端（Claude Code/Codex CLI）无法区分配置错误与网关故障。

### C2. 映射流式响应：Rust 整体缓冲，Python 逐事件流式

- Python：映射入口流式请求走 `get_mapped_streaming_converter`，`StreamingResponse` 逐 chunk `feed`/`flush`，边收边转边下发（首 token 延迟 = 上游首 token）。
- Rust（`proxy.rs`）：`if stream_requested && mapping.is_none()` 才走流式透传；**映射 + 流式时**落在非流式分支，`response.bytes()` 在 `non_stream_total_timeout_seconds`（默认 600s）内**收完整条响应**，再 `convert_stream` 一次性转换后以 `text/event-stream` 返回。
- 影响：`/claudecode/v1/messages`、`/codex/v1/responses` 的流式对客户端变成"假流式"——首字节要等上游完全结束；长流（Agent 场景）体验完全不同，且映射流式路径**不解析 usage**（`Usage::default()`，Python 通过 observer 在原始流上采集）。

### C3. 熔断恢复：Rust 无自动半开探测

- Python：`HealthSupervisor` 常驻后台线程，`disabled_until` 到期后自动对 `state='open'` 且 `manual_enabled` 的渠道发起探测（`probe_channel`）；探测成功 `record_success` 复位，失败 `record_failure(threshold=1)` 立即重新熔断。请求路径熔断打开时 `health_supervisor.reschedule()` 唤醒调度。
- Rust：`ChannelFailure` 遥测会写 `channel_health`（state=open、disabled_until），但**没有任何后台任务在到期后探测**；`health::queue` 仅由管理端点 `POST /channels/{id}/probe` 和 `reset-health` 触发。
- 影响：渠道一旦熔断，除非用户手动点探测/重置，否则**永久不可用**；`circuit_open_seconds` 设置形同虚设。

### C4. 维护任务整体缺失（scheduled discovery / 日志保留 / 陈旧请求 / 启动修复）

Python `MaintenanceSupervisor`（60s 循环）与启动修复：
1. `_schedule_discovery`：按 `model_discovery_interval_hours`（24h）对无近期 DiscoveryRun 的启用渠道自动发起 `trigger="scheduled"` 发现；
2. `_finalize_stale_pending_requests`：把超过 `max(stream_idle*2, first_byte*2, 600)s` 仍 pending 的请求标记为 cancelled；
3. `_cleanup_logs`：按 `log_retention_days`（30d）每小时清理 request_attempts/request_logs/health_probe_logs/discovery_runs；
4. 启动时 `reconcile_completed_stream_cancellations`：修复"已完成流但记为 cancelled"的历史日志。

Rust：**以上全部没有**。`model_discovery_interval_hours`、`log_retention_days` 是死配置；数据库只会无限增长（`token_usage` 甚至无法被 `DELETE /logs` 清理）。

### C5. 模型能力检测（capability detection）未移植

- Python（`services/capabilities.py`）：
  - `detect_model_capabilities` 从路由候选的 `metadata_json` 提取上下文窗口/最大输出/图像/推理/thinking 等级/成本（约 40 个字段路径），跨渠道聚合（数值取 min、布尔取 AND）；
  - `GET /model-capabilities/{id}` 与目录端点：`source="auto"` 或无行时**读时实时重检测**；
  - `POST /model-capabilities/detect/{id}`：真实计算并存储；
  - 目录 `x_local_gateway.capabilities` + `pi_model_config` 由此而来。
- Rust：`get_caps_value` 只读存储行；`detect_capabilities` 等价于 `put_capabilities(source=auto, 全 None)`——**写入一行全空值，不做任何检测**；目录端点只附加存储行（全空也会附加）。无 `pi_model_config`。
- 影响：Rust 端"自动检测能力"功能完全失效，`/v1/models` 的 `x_local_gateway.capabilities` 对未手动配置的模型输出全 null。

### C6. 转换层（convert.rs vs convert.py）语义大幅缩减

Python `adapters/convert.py`（2859 行）vs Rust `convert.rs`（539 行）：

| 方向 | Python | Rust | 缺失项 |
|---|---|---|---|
| claude→openai_compatible | tool_use→tool_calls、image→image_url(png)、tools→OpenAI function schema、tool_choice 映射 | tool_use **丢弃**、image→image_url(默认 `application/octet-stream`)、tools **原样透传**（Claude schema 直接发给 OpenAI）、无 tool_choice | 工具调用、schema 转换、tool_choice |
| claude→openai_responses | 工具展平、tool_choice、max_tokens 仅在存在时拷贝 | 工具不转换、无 tool_choice、**强制默认 `max_output_tokens=1024`** | 工具、默认值语义 |
| claude→gemini | inlineData 图片、functionCall/functionResponse、thinkingConfig（budget_tokens）、topK、systemInstruction | 仅文本；system 拼成 user 前缀消息；无图片/函数/thinking/topK；tools 原样包进 functionDeclarations（schema 不匹配） | 图片、函数调用、thinking、topK |
| responses→openai_compatible | function_call→tool_calls + reasoning_content | function_call 项**被丢弃** | 工具调用 |
| responses→claude | tool_use/tool_result/image 完整转换 | 全部折叠为文本；**强制默认 max_tokens=1024** | 工具、图片 |
| responses→gemini | 完整内容/工具/温度/topP | 仅文本 + maxOutputTokens 默认 1024；不拷贝温度/topP/tools | 大部分字段 |
| 响应→claude | thinking 块、tool_use 块、_STOP_REASON_OPENAI/GEMINI 映射、cache 用量 | 纯文本；stop_reason 原样透传（`"stop"` 不会映射为 `"end_turn"`）；usage 缺省 0；无工具块 | thinking、工具、stop_reason、usage |
| 响应→responses | 完整 `_responses_envelope`（error/incomplete_details/instructions/…/metadata）、DSML 解析、incomplete 状态、reasoning tokens | 最小信封（id/object/created_at/status/model/output/output_text/usage），无 DSML、无 incomplete 映射 | 信封字段、DSML、状态 |
| 流式转换 | 逐事件、工具按 id/index 合并、usage 采集、error_event | 整体缓冲后合成固定事件序列；message_delta usage 恒 0；无 error_event | 增量、工具、usage |

> 注：同协议映射（claude→claude、responses→responses）两侧均为纯透传，行为一致。

---

## 4. 代理管线（proxy pipeline）差异

### 4.1 鉴权（gateway access）

| 项目 | Python | Rust |
|---|---|---|
| 401 消息 | `Invalid local gateway key.` | `Gateway access denied.` |
| 错误体 openai 系 | `{"error":{"message", "type":"gateway_error", "code"},"request_id"}` | `{"error":{"message","type":code,"code":code,"request_id":…}}`——`error.type` 是 code 而非 `gateway_error`，request_id 在 error 内部 |
| 错误体 claude | `error.type="gateway_error"` | `error.type=code` |
| 错误体 gemini | `status` 为枚举名（400→INVALID_ARGUMENT、401→UNAUTHENTICATED…缺省 UNKNOWN） | `status` 直接是 code 字符串（如 `unauthorized`） |
| catalog 凭据来源 | `x-local-gateway-key` → Bearer → `x-api-key` → `x-goog-api-key` → query `key` | `x-local-gateway-key` → **仅 Bearer**（`x-api-key`/`x-goog-api-key`/query key 不接受） |
| 关闭信任后的密钥回退 | 环境变量 `AI_GATEWAY_ADMIN_TOKEN`/`AI_GATEWAY_GATEWAY_KEY`（config.py fallback） | 无回退（DB 无密钥时为空串，任何请求 401） |
| 比较方式 | `secrets.compare_digest` | 自定义常数时间比较（等价） |

### 4.2 请求体大小限制

- Python：`request_body_limit_bytes` 配置项，**默认 256 MiB**（`AI_GATEWAY_REQUEST_BODY_LIMIT_BYTES` 可调）；超限 413 `{"error":…,"message":"Request body is too large."}`。
- Rust：**常量 `8 MiB`**，不可配置；超限 413 消息为 axum 底层错误字符串（`failed to buffer the request body: …`）。
- 影响：大图/大文档请求在 Rust 端被拒。

### 4.3 模型目录端点（catalog）

| 项目 | Python | Rust |
|---|---|---|
| GET /v1/models | `?protocol=`（3 选 1）、`X-Local-Gateway-Protocol` 头、`anthropic-version` 头→claude、缺省→**四协议聚合目录**；非法协议→400 `Unsupported model catalog protocol.` | 固定 openai_compatible；**不支持任何协议选择/聚合** |
| openai 目录 item | `created`=unix 整数；`owned_by:"local-ai-gateway"`；`x_local_gateway` 含 `supported_endpoints`、有数据时的 `capabilities`+`pi_model_config`、映射模型的 `mapping` | `created`=RFC3339 字符串；`owned_by:"local-gateway"`；`x_local_gateway` 仅存储行 capabilities（全空也附加）；**无 supported_endpoints/mapping/pi_model_config** |
| claude 目录（/v1/messages/models） | `{"data":[{type:"model",id,display_name,created_at(ISO),x_local_gateway}],has_more,first_id,last_id}` | **OpenAI 列表形状**（`{"object":"list","data":[{id,object,owned_by,created}]}`），无 display_name/has_more |
| gemini 目录 | `baseModelId`、`version`、`supportedGenerationMethods:[generateContent,streamGenerateContent]`、`x_local_gateway` | 仅 name/displayName/`supportedGenerationMethods:["generateContent"]`，无 x_local_gateway |
| /claudecode /codex 信息页 | 完整 JSON（name/base_url/endpoints/configure 环境变量指引） | 3 字段 `{"protocol","endpoint","models"}` |
| 映射模型目录数据源 | `list_claude_mapping_models`（含 `x_local_gateway.mapping`） | `list_mapping_models`；**codex 恒为空（见 C7 Bug）** |

### 4.4 超时与流式

| 项目 | Python | Rust |
|---|---|---|
| 连接超时 | 运行设置 `connect_timeout_seconds`(10) | 客户端硬编码 10s；设置项死配置 |
| 流式读超时 | `stream_idle_timeout_seconds`(300) | **无**（流式无空闲超时，上游挂死则一直挂） |
| 非流式读超时 | `first_byte_timeout_seconds`(60) | `non_stream_total_timeout_seconds`(600)（Python 中该项是死配置） |
| 首 token 超时 | `first_token_timeout_seconds`(60) 用于 prelude 读取 | 死配置，不使用 |
| 首字节前错误检测 | 读 prelude（≤1MiB/首 token/首个工具调用为止），**2xx 响应体内的错误 JSON 可触发故障转移** | 无 prelude；2xx 即开始透传，**200+错误体无法触发故障转移** |
| 全部失败后的状态码 | 超时类→**504**，否则 502 | **恒 502**（无 504 分支） |
| 流式透传 | 原始字节 + 保留 content-length | 原始字节，但 **content-length 被 hop-by-hop 列表剥离** |
| 取消语义 | 客户端断开→204 + 遥测 499/cancelled；已完成流记为 success | CancelAware 包装器→同样 204 + cancelled；**取消路径 response_bytes/usage 恒 0**（Python 有 observer 数据） |
| 流中断错误遥测 | 运行时 threshold/open_seconds | **硬编码 threshold:3, open_seconds:900**（绕过设置） |

### 4.5 路由与熔断

| 项目 | Python | Rust |
|---|---|---|
| 候选 SQL | 条件完全一致（route.enabled、candidate.enabled、model.available、manual_enabled、protocol 绑定、`state='active'`、priority 升序、limit=max_failover_attempts） | 一致 |
| 无候选时 | 区分协议不匹配→**400** `unsupported_model_endpoint`（含 supported_protocols/supported_endpoints 明细，各协议不同错误体）；否则 503 no_active_channel | **恒 503**，无 400 分支 |
| 401/403 计数 | `auth_error` **countable=true**（会计入熔断） | `upstream_4xx` countable=false（不计入） |
| 错误记录 | 同步写 `channel_health`（record_failure）+ 熔断打开时 reschedule | 异步遥测事件写 `channel_health`（打开无 reschedule） |
| key 解密失败 | 抛出 RuntimeError→500 | 记 attempt 事件后继续故障转移（更健壮） |

### 4.6 映射入口（/claudecode、/codex）细节

| 项目 | Python | Rust |
|---|---|---|
| 未映射/禁用 | 404 unknown_mapped_model（C1） | 当作普通路由（C1） |
| 上游协议非法 | 422 invalid_upstream_protocol | 无此分支（get_adapter 不抛错） |
| 转换失败 | 400 `conversion_error` + `_mapped_error_body` | 400 `invalid_request`（消息即转换错误文本） |
| 非流式转换失败（响应） | `_mapped_error_body`（gateway 错误 JSON）+ channel_failure 遥测 | `unwrap_or(raw)`——**静默返回未转换的上游原始字节** |
| 上游错误体 | `convert_mapped_error_response`（claude 透传；responses 转 `{error:{message,type,code,param}}`） | `convert_error`（claude 包 api_error；其他 `{error:{type:"upstream_error",message}}`，无 code/param） |
| gemini 上游路径 | 按 `candidate.model_id` 构造（`quote` 编码） | 按 mapping.upstream_model 构造（candidate 相同，等价） |
| 无候选时消息 | `No active channel is available for mapped model '<id>'.` | `No active channel is available for this model.`（无模型名） |

---

## 5. 管理端点差异（按端点）

| 端点 | Python | Rust |
|---|---|---|
| POST /providers | 201 体 `channel_count: null`（未 eager-load） | `channel_count: 0`；409 消息 `Provider already exists` vs Python `Provider name already exists` |
| PATCH /providers | `channel_count: null` | 实际计数 |
| PATCH /channels/{id} | 响应为部分对象 `{id,protocol,protocols,**values}`；health_check_model_id 变更时校验"必须是本渠道可用模型且协议匹配"→422 `Health check model must be an available channel model`；协议变更重算 `model.available`、同步共享协议路由、409 消息 `Remove route candidates before disabling a channel protocol` | 响应为完整 channel_json；**无 health_check_model_id 校验**（任意字符串，且 serde Option 无法置 NULL）；协议变更**不重算 available**、**无共享协议路由同步**；409 消息 `Protocol is used by route candidates` |
| POST /channels/{id}/models | protocols 必须是渠道已启用协议的子集→422 `Model protocols must be enabled on the channel`；201 体 `{id,channel_id,model_id,protocols,source,available}` | **无子集校验**；201 体为完整 model_json |
| PATCH /channel-models/{id} | 协议子集校验；无路由候选防护 | **无子集校验**；有 409 路由候选防护（`Protocol is used by route candidates`）；display_name 无法置 NULL |
| DELETE /channel-models/{id} | **任意来源模型可删**（409 `Remove route candidates before deleting this model`） | **仅 source='manual' 可删**，discovered → 404 `Manual model not found` |
| GET /discovery-runs/{id} | 计算 `status`（running/succeeded/failed），无 trigger/success 字段 | 原样行（trigger/success 字段），无 status |
| GET /claude-mappings、/codex-mappings | item 键 `claude_model_id`/`codex_model_id`；candidates=`{channel_id,channel_name,model_id,priority}`（resolve_candidates limit 50）；含 page/page_size | item 键 **`model_id`**（接受别名输入）；candidates=route_bundle 候选对象（字段不同）；**无 page/page_size** |
| POST/PATCH mappings | `upstream_protocol` 默认 `"openai_compatible"`；校验 1) 模型必须已存在于 channel_models 2) 路由必须启用，两段式 422 消息 | `upstream_protocol` **必填**；仅校验路由存在（422 `Upstream model route not found`）；patch 缺 model_id→422 `model id missing`（Python 允许省略） |
| GET /model-capabilities/{id} | 无行或 source=auto 时**实时重检测**；capabilities 有值才输出 | 只读存储；无检测（C5） |
| PUT /model-capabilities/{id} | Pydantic 校验：thinking_level_map 键限 7 档（`Unsupported thinking level`）、cost≥0、context/max≥1、source Literal | **无任何数值/枚举校验**；source 任意字符串 |
| POST .../detect/{id} | 真实检测并存储 | 全空写入（C5） |
| GET /capability-profiles | capabilities 仅非空键 | 全键含 null |
| PATCH /routes/{id} | enabled 必填（一致）；响应 `{id,enabled}`（一致） | 一致 |
| PUT /routes/{id}/candidates | 409 拆两条消息（`Candidate priorities must be unique` / `Channel models must be unique`） | 合并一条 `Candidate priorities and models must be unique` |
| GET /requests | 过滤：protocol/model_id/outcome/status_code/min_duration_ms/channel_id/upstream_model_id/upstream_protocol/from/to；**total 按过滤条件计数** | 仅 protocol/model_id/page/page_size；**total=全表 COUNT(*)（未过滤）** |
| DELETE /logs | 缺 confirm → **400**；支持 `before`；额外清空 discovery_runs | 缺 confirm → **422**；无 before；**不清 discovery_runs、不清 token_usage** |
| GET /health-probes | 含 page/page_size | 仅 items/total |
| GET /stats/models | `{protocol,model_id,requests,average_duration_ms,success_rate}`（按 protocol+model 分组） | `{model_id,requests}`（仅按 model） |
| GET /stats/channels | `{channel_id,channel_name,attempts,average_duration_ms,success_rate}`（attempts 表） | `{channel_id,requests}`（request_logs.final_channel_id） |
| GET /stats/timeseries | `{time,requests,successes,average_duration_ms}` | `{bucket,requests}`（键名不同，无 successes/avg） |
| GET /stats/summary | 全量统计（request_attempts 聚合）；无时间窗口参数 | 支持 `from`/`to` 窗口（token_usage 聚合）+ `token_range` 字段；requests/success_rate 不受窗口影响 |
| GET /settings | 额外会带出 claude_presets/codex_presets 缓存（get_runtime_settings 只排除私钥键） | 仅 11 个设置键 + hints |
| PATCH /settings | 未知键/范围错 → **422**（ValueError→HTTPException）；关闭信任需 **admin+gateway 双键**（消息含"管理密钥和代理密钥"）；响应无 hints | 校验错误经 anyhow→**500**；关闭信任**仅需 admin 键**（`关闭局域网信任前必须设置管理密钥`）；响应含 hints |
| GET /system/status | 无 host/port | 多 host/port 字段 |
| 管理鉴权失败 | 401 `{"detail":"Invalid admin token"}` | 401 `{"detail":"Unauthorized"}` |
| 通用 404 消息 | `Channel model not found`、`Request log not found`、`Claude model mapping not found` 等 | `Model not found`、`Request not found`、`Mapping not found` 等（措辞不同） |
| 422 校验错误体 | FastAPI 默认 `{"detail":[{...pydantic 数组}]}` | `{"detail":"<serde 消息字符串>"}` |

---

## 6. 后台/支撑机制差异

### 6.1 健康检查与熔断（详见 C3）

| 项目 | Python | Rust |
|---|---|---|
| 探测请求 | `adapter.health_probe`：四协议均 `stream:true`、`"Reply only OK"`、max_tokens 2 | 相同 body，但**无 stream:true**；gemini 用 `:generateContent`（非流式路径） |
| 成功判定 | `2xx 且 (saw_completion 或 first_token)`——要求流内有完成/首 token | **仅 2xx**（读完即弃） |
| 失败计数 | `record_failure(threshold=1)`→探测失败立即重新熔断；冷却=运行设置 `circuit_open_seconds` | **硬编码 3 次 / 900s**；`next_probe_at=now+900s` 硬编码 |
| 自动恢复 | HealthSupervisor 到期自动探测（C3） | 无 |
| 探测超时 | httpx 读超时（请求级） | 硬编码 20s |

### 6.2 模型发现

| 项目 | Python | Rust |
|---|---|---|
| 触发 | 手动 + **定时（MaintenanceSupervisor，24h）** | 仅手动（`model_discovery_interval_hours` 死配置） |
| 超时 | 无整体超时（逐协议 httpx） | 整体 120s，超时错误 `模型探测超时` |
| 协议处理 | 按渠道 protocols 全量发现，openai 共享组去重 | 相同（跳过 openai_responses 若已见 openai_compatible） |
| 结果写入 | ChannelModel upsert + metadata_json[protocol] **按协议合并** + 陈旧协议 binding 清理 + `available` 重算 + run 状态（success/model_count/status_code/error_kind） | upsert 覆盖 metadata_json、无陈旧清理、无 available 重算、run 记录 success/model_count/error_kind |
| 发现端点 | 支持分页（next_discovery_url / after_id / pageToken） | 无分页（单次 GET） |

### 6.3 预设（presets）

- Python：`GET /claude-presets|/codex-presets` 返回内置默认（Claude 13 项 / Codex 11 项）+ 缓存渠道聚合；`POST .../refresh` 后台**真实查询已配置渠道的 /v1/models 并缓存**（20s 超时、逐渠道容错）。
- Rust：内置默认更少（Claude 7 项 / Codex 8 项，缺 opus-4-8、sonnet-4-6、opus-4-5、haiku-4-5、旧模型、gpt-4.1-mini/nano、gpt-4o-mini）；`refresh_presets` **是空桩**（202 `{"status":"queued"}`，无任何副作用）——刷新功能完全失效。

### 6.4 遥测

- 两侧事件集一致（request_start/finish、attempt、channel_success/failure），队列均 1000，溢出计数 dropped。
- Rust 独有：`token_usage` 表（按小时 bucket、attempt_id 去重、migration 0002 回填）——Python 无此表，统计直接从 request_attempts 聚合。
- Rust 失败路径尝试的 usage 恒为 default（Python 会保留 raw 但清零计数——两侧语义基本一致，差异在映射流式路径：Python 从原始流解析 usage，Rust 恒空，见 C2）。
- Rust 遥测 worker 是单事务写 attempt+token_usage；Python 每事件独立 session。行为等价。

### 6.5 数据库层

- Python：`Base.metadata.create_all`（运行时）+ `_ensure_schema_columns`（补 model_caps.profile_id）+ `_backfill_protocol_bindings`；alembic/ 目录是历史迁移，运行时未执行。
- Rust：sqlx 内嵌迁移 `0001_gateway_schema.sql`（17 表，含索引）+ `0002_token_usage.sql` + 启动补列（`ensure_legacy_columns`：model_caps.profile_id、request_attempts.upstream_protocol/upstream_model_id、claude_model_mappings.upstream_model_id）+ 相同的 backfill。
- 表结构除 `token_usage`（Rust 独有）外一一对应；`claude_mapping_candidates` 只存在于 Python 历史 alembic 迁移中，当前模型与 Rust 均无。

---

## 7. 其他差异

| 项目 | Python | Rust |
|---|---|---|
| CORS | CORSMiddleware：允许 localhost/127.0.0.1/[::1]、file://、onlyoffice://、ascdesktop://、null；方法 GET/POST/OPTIONS | **无 CORS 层**（仅 TraceLayer） |
| SPA 回退 | 前端目录存在时挂载；API 路径（api/、v1/、v1beta/、claudecode/、codex/）404 不吞；assets 禁缓存 | fallback 等价实现（is_api_path 相同前缀集合；assets 禁缓存） |
| /api/health | `{"status":"ok"}` | `{"status":"ok","runtime":"rust"}` |
| 配置 | .env（AI_GATEWAY_*）+ 环境变量；数据目录默认 `./data` | launcher.json + 环境变量（AI_GATEWAY_HOST/PORT/DATA_DIR）；数据目录优先平台目录，legacy `./data` 兜底 |
| 密钥回退 | 环境变量 fallback（见 4.1） | 无 |
| gemini POST 校验 | 非 `:generateContent`/`:streamGenerateContent` → 404 `Unsupported Gemini endpoint` | **无校验**，任意 action 继续转发 |
| 管理端版本 | FastAPI version 硬编码 0.2.1 | CARGO_PKG_VERSION（version.sh 同步） |

---

## 8. Rust 独有特性（Python 没有）

1. `token_usage` 表 + 统计 `from`/`to` 时间窗口（stats/summary）——Python 统计是全量的。
2. `CancelAware` 流包装：客户端断开也能记录 cancelled 尝试（Python 靠生成器取消，效果等价）。
3. key 解密失败优雅降级（Python 会 500）。
4. 管理端点 `state` 过滤参数（GET /channels）。
5. 桌面托盘/启动器生命周期（Tauri）、headless 二进制。

---

## 9. 差异清单与修复优先级

### Critical（建议优先修复）

| # | 差异 | 位置 |
|---|---|---|
| C1 | 映射入口未知模型：Rust 直接转发 vs Python 404 | proxy.rs resolve_mapping 分支 |
| C2 | 映射流式整体缓冲、无增量转换、无 usage | proxy.rs 成功分支 |
| C3 | 无自动熔断恢复（半开探测） | 缺 HealthSupervisor 等价物 |
| C4 | 定时发现/日志保留/陈旧请求/启动修复缺失 | 缺 MaintenanceSupervisor 等价物 |
| C5 | 能力检测未移植（detect 空写、无读时检测、目录 null） | admin.rs put_capabilities / get_caps_value / models() |
| C6 | 转换层工具/图片/thinking/DSML/信封/默认值语义丢失 | convert.rs |

### Major

| # | 差异 |
|---|---|
| M1 | GET /codex/v1/models、/codex/v1/responses/models 恒空（`"openai_responses"` vs `"codex"` 分支错配） |
| M2 | 请求体上限 8 MiB 固定 vs 256 MiB 可配置 |
| M3 | GET /v1/models 无协议选择/聚合目录（protocol 参数、X-Local-Gateway-Protocol、anthropic-version） |
| M4 | claude 目录形状（/v1/messages/models、/claudecode/v1/models）非 Claude 格式 |
| M5 | gemini 目录缺 baseModelId/version/streamGenerateContent/x_local_gateway |
| M6 | 流式无空闲超时（stream_idle_timeout_seconds 未生效）；非流式读超时用 600s 而非 60s |
| M7 | 无 prelude/200+错误体故障转移（first_token_timeout_seconds 死配置） |
| M8 | 全部失败恒 502，无 504 分支 |
| M9 | 401/403 不计入熔断（Python 计入） |
| M10 | 错误体形状：gateway_error type/code/request_id 位置、gemini status 枚举、401 文案 |
| M11 | catalog 凭据规则（Rust 仅 Bearer）与 gemini 目录鉴权协议 |
| M12 | 关闭信任后无环境变量密钥回退 |
| M13 | PATCH /channels：无 health_check_model_id 校验/无法置空、不重算 available、不同步共享协议路由 |
| M14 | 映射/渠道模型管理校验差异（子集校验、discovered 模型删除、响应键名 model_id vs claude_model_id、page 字段） |
| M15 | stats/models、stats/channels、stats/timeseries 响应结构不同；requests total 未过滤 |
| M16 | PATCH /settings 校验错误 500 vs 422；关闭信任只查 admin 键 |
| M17 | refresh_presets 空桩；默认预设清单不全 |
| M18 | 无 CORS |

### Minor

- 404/409/422 文案差异、错误体 detail 数组 vs 字符串、/api/health 多 runtime 字段、get_discovery_run 字段差异、presets 排序/去重差异、探测成功判定（2xx only vs 要求完成）、探测/冷却硬编码 3/900、流中断遥测硬编码阈值、取消路径 usage/bytes 恒 0、content-length 剥离、claudecode/codex 信息页体、mapping candidates 形状、system/status host/port、DELETE /logs 状态码 400 vs 422 与 discovery_runs 清理、Rust put_capabilities 无 thinking_level 校验、get_settings 泄漏预设缓存（Python 侧怪癖）。

---

## 10. 附录：主要源码位置索引

| 主题 | Python | Rust |
|---|---|---|
| 路由注册 | backend/app/main.py, api/proxy.py, api/admin.py | desktop/src-tauri/src/server.rs, admin.rs |
| 代理管线 | services/proxy.py | src/proxy.rs |
| 转换 | adapters/convert.py (2859 行) | src/convert.rs (539 行) |
| 适配器/协议 | adapters/base.py | src/protocol.rs |
| 路由解析 | services/routing.py | src/routing.rs |
| 熔断/健康 | services/circuit_breaker.py, services/health.py | src/telemetry.rs(状态机), src/health.rs |
| 维护 | services/maintenance.py | （缺失） |
| 能力检测 | services/capabilities.py | （缺失，admin.rs 空写） |
| 预设 | services/presets.py | admin.rs（refresh 桩） |
| 目录 | services/catalog.py | proxy.rs models()/mapped_models() |
| 设置/鉴权 | services/settings.py, core/access.py, api/deps.py | src/settings.rs, src/auth.rs |
| 遥测 | services/telemetry.py | src/telemetry.rs |
| 数据模型 | db/models.py + db/database.py | src/db.rs + migrations/ |
| 加密 | core/security.py | src/crypto.rs |
