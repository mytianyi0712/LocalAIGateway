# Command Code Go 协议基准（阶段 0）

> 本文件是「网关内原生集成 Command Code Go（路线 B）」的协议基准。
> 所有事实来自两个 MIT 许可的社区实现（仅作事实依据，未复制其代码）：
>
> - [`MAXeaglet/commandcode-proxy@HEAD`](https://github.com/MAXeaglet/commandcode-proxy)（MIT，`proxy.mjs` 2947 行；三向转换、指纹、会话、初始化节流）
> - [`patlux/pi-commandcode-provider@HEAD`](https://github.com/patlux/pi-commandcode-provider)（MIT，`src/transport.ts` transport router、`src/converters.ts`、`src/quota*.ts`、`src/models.ts`、`src/overflow.ts`、20 个测试、CI）
>
> 厂商侧从未开源：`CommandCodeAI/command-code` 是公告/issue 仓库（3924★，无源码），`cmd-old-public` 仅含 logo/sticker/issue 模板。
> 唯一权威线协议来源是 npm 包 `command-code` 的 `dist/cli.mjs`。本文记录的是**社区交叉验证过的**线协议快照，
> 用于漂移检测基线；上游每个版本都可能静默改线（见 §9）。

基准版本：`command-code@1.53.1`（社区实现快照：MAXeaglet CC 版本常量 / patlux `COMMAND_CODE_CLI_VERSION=1.44.0` 时的目录）。
网关运行时会自行探测/记录实际版本（`settings.command_code_cli_version`）。

---

## 1. 端点

`apiBase` 默认 `https://api.commandcode.ai`（渠道 `base_url` 即此值，可指向自建桥；指向自建桥时必须关闭身份注入）。

| 端点 | 方法 | 用途 |
| --- | --- | --- |
| `{apiBase}/alpha/generate` | POST | **主推理端点**（反代路径，NDJSON 流） |
| `{apiBase}/alpha/fingerprint/record` | POST | 上报机器指纹（body = 指纹对象） |
| `{apiBase}/alpha/lifecycle-events` | POST | 生命周期事件（`eventType: cli_session_exists`） |
| `{apiBase}/alpha/whoami` | GET | 账号信息（`org` **可能为 `null`**；也用作健康探测，鉴权即可、不耗 token） |
| `{apiBase}/alpha/billing/credits[?orgId=]` | GET | 额度：月/购买/赠送 credits + `fiveHour`/`weekly` 窗口；无 org 时**省略参数** |
| `{apiBase}/alpha/billing/subscriptions[?orgId=]` | GET | 订阅信息（`data.planId/status/currentPeriodStart/currentPeriodEnd`）；同上 |
| `{apiBase}/alpha/usage/summary[?orgId=][&since=]` | GET | 用量汇总（`totalCost`/`totalCount`/`totalTokens`）；同上 |
| `{providerBase}/models` | GET | **模型目录**（`providerBase` 默认 `https://api.commandcode.ai/provider/v1`） |
| `{providerBase}/chat/completions` | POST | **官方 Provider API（OpenAI 形）**，Go 套餐 403 `upgrade_required` |
| `{providerBase}/messages` | POST | **官方 Provider API（Anthropic 形）**，Go 套餐 403 `upgrade_required` |

> Go 套餐下 `{providerBase}` 的可达性属阶段 0 未决问题（计划 §7.1）：网关按「先官方、后反代」的
> transport router 运行时判定（§6），不把结论写死。

## 2. `/alpha/generate` 请求头

```
Authorization: Bearer <apiKey>          # user_... 格式
Content-Type: application/json
x-cli-environment: production
x-command-code-version: <CC_VERSION>    # 与版本探测联动
x-session-id: <sessionId>
x-co-flag: false
x-taste-learning: false                 # 社区实现有 false / true 两种写法，网关固定 false（用户开关见计划 §7.4）
x-project-slug: <projectSlug>
traceparent: <W3C trace context>        # 每请求生成
x-cmd-zdr: 1                            # 可选（ZDR）
User-Agent: cli                         # patlux 实现使用；MAXeaglet 未发
```

`/alpha/fingerprint/record` 与 `/alpha/lifecycle-events` 使用同一组基础头（无 session/project/traceparent）。

## 3. 机器指纹（反滥用核心）

随机选取：CPU 型号 + 核心数（Windows x64 对照表）、内存 GiB、时区、MAC 数量（2–5）。

```
machineIdHash = sha256(randHex(32))
osUserHash    = sha256(randHex(16))
hostnameHash  = sha256(randHex(16))
gitEmailHash  = sha256(randHex(16))
macHashes[i]  = sha256(randHex(32))
thumbmark     = sha256([machineIdHash, ...macHashes, osUserHash, hostnameHash, gitEmailHash,
                        'win32', '10.0.22631', cpuModel, cpuCores, memGiB].join('|'))
```

```json
{
  "thumbmark": "<sha256 hex>",
  "components": {
    "machineIdHash": "...", "macHashes": ["..."], "osUserHash": "...", "hostnameHash": "...",
    "gitEmailHash": "...", "platform": "win32", "arch": "x64", "osRelease": "10.0.22631",
    "cpuModel": "...", "cpuCount": 8, "memGiB": 16, "isContainer": false,
    "timezone": "Asia/Shanghai", "runtime": "cli", "collectorVersion": 1
  }
}
```

- **每个 API Key 独立指纹**（社区实现：`keyStateStore: apiKey → {fingerprint, nextInitAt}`）。
- 初始化节奏：**首次 + 每 8h（+ 0–2h 抖动）**：并行上报 `fingerprint/record` + `lifecycle-events`。
- `lifecycle-events` body：

```json
{"eventType":"cli_session_exists",
 "metadata":{"sessionId":"sess_<16hex>","cliVersion":"<CC_VERSION>","mode":"interactive","os":"<platform>-<arch>"}}
```

> 网关实现按「每渠道（=每 API Key）」持久化指纹与节流时间：`settings` 键 `command_code_fingerprint_<channel>` /
> `command_code_init_at_<channel>`，语义与上游「每 key 独立」对齐。

## 4. 会话与 projectSlug

- `sessionId` 取值优先级：客户端 `x-session-id` → `x-claude-code-session-id` → `session_id` → `prompt_cache_key`；
  长度 < 8 视为无效；无有效值时按 API Key 派生。
- 会话 TTL：**12h + 1h 抖动，按 API Key**（网关持久化，过期重建；粘滞以保 prompt cache 命中）。
- `projectSlug`：由 sessionId 确定性派生（十六进制时按索引取字符，否则字符哈希）。

## 5. NDJSON 事件表（`/alpha/generate` 响应）

响应体是**逐行 JSON**（NDJSON），每行一个 `{type, ...}` 事件（不是 SSE）。

| `event.type` | 载荷 | 映射 |
| --- | --- | --- |
| `start` / `start-step` / `text-start` / `reasoning-start` | 信号 | 忽略 |
| `text-delta` | `{text}` 或 `{delta}` | 文本增量 |
| `reasoning-delta` | `{text}` | `reasoning_content` / Anthropic `thinking` block |
| `tool-call` | `{toolCallId, toolName, input}`（**input 已是完整对象**） | OpenAI `tool_calls` / Anthropic `tool_use`（单次 `input_json_delta`） |
| `finish-step` | `{finishReason, usage}` | 记录 usage（可被 `finish` 覆盖） |
| `finish` | `{finishReason, totalUsage}` | 终止 + usage |
| `error` | `{error:{message}}` | 错误。**不得发出 finish_reason**（否则下游 agent 循环提前停止） |
| `reasoning-end` / `provider-metadata` / `tool-input-start` / `tool-input-delta` / `tool-input-end` / `tool-error` / `text-end` | — | 静默忽略 |
| 未知类型 | — | 记 warn，忽略（前向兼容必须项） |

finishReason 词表：`tool-calls` → OpenAI `tool_calls` / Anthropic `tool_use`；`length` → `length` / `max_tokens`；
`stop` → `stop` / `end_turn`；其他原样传递（兜底 `stop`）。

usage（`finish-step.usage` / `finish.totalUsage`）：

```json
{"inputTokens": 120, "outputTokens": 34, "cachedInputTokens": 100,
 "inputTokenDetails": {"noCacheTokens": 20, "cacheReadTokens": 100, "cacheWriteTokens": 0}}
```

- `inputTokens` 是**总数**（含缓存命中）；Anthropic 口径的 `input_tokens` 取
  `inputTokenDetails.noCacheTokens`，缺失时 `max(0, inputTokens - cacheRead - cacheWrite)`。
- 社区实现的反虚假计费：`outputTokens` 为 0/null 时把 input/cached 一并归零。

### 5.1 脱敏夹具（依据事件表构造，仅用于回放回归）

```ndjson
{"type":"start"}
{"type":"start-step"}
{"type":"reasoning-start"}
{"type":"reasoning-delta","text":"Let me think."}
{"type":"reasoning-end"}
{"type":"text-start"}
{"type":"text-delta","text":"Hello"}
{"type":"text-delta","delta":" world"}
{"type":"text-end"}
{"type":"finish-step","finishReason":"stop","usage":{"inputTokens":120,"outputTokens":5,"cachedInputTokens":100,"inputTokenDetails":{"noCacheTokens":20,"cacheReadTokens":100,"cacheWriteTokens":0}}}
{"type":"finish","finishReason":"stop","totalUsage":{"inputTokens":120,"outputTokens":5,"cachedInputTokens":100,"inputTokenDetails":{"noCacheTokens":20,"cacheReadTokens":100,"cacheWriteTokens":0}}}
```

工具调用夹具：

```ndjson
{"type":"start"}
{"type":"text-delta","text":"checking"}
{"type":"tool-call","toolCallId":"call_abc","toolName":"read_file","input":{"path":"a.txt"}}
{"type":"tool-input-start","toolCallId":"call_abc"}
{"type":"tool-input-delta","toolCallId":"call_abc","delta":"{\"path\":"}
{"type":"tool-input-end","toolCallId":"call_abc"}
{"type":"finish","finishReason":"tool-calls","totalUsage":{"inputTokens":50,"outputTokens":12,"cachedInputTokens":0,"inputTokenDetails":{"noCacheTokens":50,"cacheReadTokens":0,"cacheWriteTokens":0}}}
```

错误事件夹具（**不得**触发下游 finish_reason）：

```ndjson
{"type":"start"}
{"type":"text-delta","text":"partial"}
{"type":"error","error":{"message":"upstream exploded"}}
```

上下文溢出夹具（错误文本，无稳定错误码）：

```ndjson
{"type":"error","error":{"message":"Prompt is too long: context length exceeded (requested 300000 tokens)"}}
```

## 6. transport router（先合法、后反代）

`transport ∈ {unknown, provider, generate}`，**按 API Key（渠道）**记忆：

1. `unknown`：先请求官方 Provider API（`/provider/v1/chat/completions` 或 `/messages`）。
2. 命中 **403 + `error.code == "upgrade_required"`** → 记住 `generate`，改走 `/alpha/generate`。
3. 其他响应（成功/业务错误）→ 记住 `provider`，永不降级。
4. 已记住 `generate` → 直接 `/alpha/generate`。

Go 套餐用户第一次请求会得到 403（服务端已文档化），随后所有请求走反代路径；
GOAT/Pro/Max 用户永不触发反代路径，越界面积被限制在最小集合。

## 7. CC 请求体形状（`/alpha/generate`）

```json
{
  "config": {"workingDir":"/", "date":"YYYY-MM-DD", "environment":"...", "structure":[],
             "isGitRepo":false, "currentBranch":"", "mainBranch":"", "gitStatus":"", "recentCommits":[]},
  "memory": null,
  "taste": null,
  "skills": "",
  "permissionMode": "standard",
  "params": {
    "model": "deepseek/deepseek-v4-flash",
    "messages": [ ... ],
    "max_tokens": 64000,
    "stream": true,
    "system": "..."
  }
}
```

- 角色**仅接受 `user` / `assistant` / `tool`**；`system`/`developer` 提升为 `params.system`；
  会话中途的 `developer`（如 OMP advisor notes）**降级为 `user`** 而非丢弃。
- `params.system` 恒为字符串；缺省时上游会注入自身约 7.5K token 默认提示词 →
  网关用**空格占位**绕过（社区实测 prompt_tokens 7653 → 85）。
- content part 类型：
  - `{type:"text", text}`
  - `{type:"reasoning", text}`（**必须回传且排在最前**）
  - `{type:"image", image:"data:image/jpeg;base64,..."}`
  - `{type:"tool-call", toolCallId, toolName, input}`（input 为对象）
  - `{type:"tool-result", toolCallId, toolName, output:{type:"text"|"error-text", value}}`
- `params` 可选字段：`reasoning_effort`、`temperature`、`tools`（Anthropic 形 `{type,name,description,input_schema}`）、
  `tool_choice`（`{type:"auto"|"any"|"none"|"tool", name}`）、`parallel_tool_calls`。
- `max_tokens` 夹取上限 200000（社区实现）。

## 8. 已知硬坑（回归用例清单）

1. **推理内容必须随历史回传**，顺序 `[reasoning, text, tool-call]`（reasoning 最前），否则上游拒绝。
2. Anthropic `thinking` block → 必须转成 `reasoning_content` 回传，缺失即被拒。
3. **缺失的 tool result 必须补齐**：合成 `error-text`
   `No result — the tool call did not complete (interrupted or lost).`，否则上游报 `Tool result is missing`。
4. **空系统提示需用空格占位**，否则部分请求异常（并注入上游默认提示词）。
5. `error` 事件**不得**转成 finish_reason。
6. 图片格式必须是 `{type:'image', image:'data:image/jpeg;base64,...'}`。
7. Anthropic `tool_result` 需重排到 assistant `tool_calls` 之后、user 文本之前。
8. Anthropic 流需为 thinking block 合成 `signature_delta`（上游不提供签名）。
9. Anthropic `input_tokens` 按缓存口径换算（`noCacheTokens` / `cachedInputTokens`），不能直用原始 `inputTokens`。
10. `tool-input-delta` 是增量事件但被忽略 —— 因为 `tool-call` 已带完整 input；若上游改为只发增量，需回退逻辑。

## 9. 上下文溢出识别

上游以**错误文本**表达上下文溢出（无稳定错误码）。网关复用 `convert/error.rs` 的识别层；
社区模式（patlux `src/overflow.ts`）要点：

- 前缀归一：`context_length_exceeded: <原文>`；
- 肯定模式约 8 条：`context length/window … exceeded|overflow|too large/long`、
  `prompt/input tokens limit exceeded`、`maximum allowed context …` 等；
- **反例**：`rate limit`、`too many requests`、`capacity/quota/throttle/concurrency/overloaded`、
  `service unavailable`、`status 429` → 不是溢出。

## 10. 模型目录与额度数据

目录：`GET {apiBase}/provider/v1/models`，开源形 `{"object":"list","data":[{"id","object","created",
"owned_by","name","context_length"}]}`；实测该端点**公开**（匿名 GET 200，Authorization 被忽略），
社区实现也只发 `accept: application/json`。模型名原样保留（如 `deepseek/deepseek-v4-flash`、`xxx[1m]`），
网关把 `name` 映射为显示名、其余字段存入 `channel_models.metadata_json`。

Go 套餐的兜底：若实时目录请求失败（网络/5xx/空目录），网关退回**官方 CLI 包内
`dist/bundled/command-code-knowledge/reference/models.md`**（`command-code@1.53.1`）中标记
“Go and above” 的 44 个模型快照（`src/commandcode.rs::BUNDLED_GO_CATALOG`），并在探测运行里记录
诊断 `command_code_bundled_catalog`。**401 不兜底**（凭据问题不能被静态目录掩盖）。该快照不随上游
自动更新，漂移由版本探测与 UI 告警提示，用户仍可手工增删模型。

额度：所有查询串必须作为真正的 query 发送（`/alpha/billing/credits?orgId=...`）；若把 `?`
并入 path 会被百分号转义成 `%3F` 并得到 404。`/alpha/whoami` 的 orgId 兼容两种形状：
社区实现的顶层 `org.id` 与官方 CLI usage 包装的 `data.org.id`。

**无 org 账号（真实观测，2026-09-12）**：`/alpha/whoami` 返回 `"org":null` 的个人 key。
此时 billing 系列的正确做法是**完全省略 `orgId` 参数**：

- `GET /alpha/billing/credits?orgId=`（空值）→ **400 `Validation error: Invalid UUID at "orgId"`**；
  拼成 `orgId=null`（官方 CLI 的 `withOrgId(null)` 行为）同样 400。
- `GET /alpha/billing/credits`（无参数）→ **200**，返回该用户个人套餐的完整额度。
- 用 `user.id` 冒充 orgId → 403 `You do not have permission to view credits`。
- `/alpha/billing/subscriptions` 无参数同样 200（真实值 `individual-go`/`active`）；
  `/alpha/usage/summary` 无参数仍 200（空 orgId 也 200，但不要依赖）。

额度（真实形状；`resetAt:0` = 窗口未消耗、**无待重置时间**，不能当 1970 展示）：

```json
// GET /alpha/whoami（无 org 账号）
{"success":true,"user":{"id":"<uuid>","userName":"..."},"org":null}
// GET /alpha/billing/credits（无 org 时省略 ?orgId=）
{"credits":{"belowThreshold":false,"creditThreshold":0,
           "monthlyCredits":10,"purchasedCredits":0,"freeCredits":0},
 "windowLimits":{"limited":true,"exceeded":null,
   "fiveHour":{"used":0,"cap":3,"exceeded":false,"resetAt":0},
   "weekly":{"used":0,"cap":6,"exceeded":false,"resetAt":0}}}
// GET /alpha/billing/subscriptions（无 org 时省略 ?orgId=）
{"data":{"planId":"individual-go","status":"active","currentPeriodStart":"...","currentPeriodEnd":"..."}}
// GET /alpha/usage/summary（无 org 时省略 ?orgId=；也是空用量时的形状）
{"totalCount":0,"totalCost":0,"averageCost":0,"successRate":0,"completedCount":0,
 "failedCount":0,"totalTokensIn":0,"totalTokensOut":0,"totalTokens":0,
 "totalCredits":0,"totalFreeCredits":0,"totalMonthlyCredits":0,
 "totalPurchasedCredits":0,"periodBasis":"billing-period"}
```

网关映射：`remaining = monthly + purchased + free`；`used = summary.totalCost`；`total = remaining + used`；
`fiveHour`/`weekly` → `QuotaWindow{label:"5h"/"周", used_percent=used/cap, resets_at=resetAt}`。
额度类错误（429/402）→ 写 `channel_health.state='open'` + `disabled_until=窗口 resetAt`（解析不到时用熔断时长），
窗口重置后由既有健康探测放回。

**CC 思考流 → Claude SSE 的不变量**（真实回归，`convert/stream.rs`）：
CC 的 `reasoning-delta` 经 canonical OpenAI 转换后，必须映射为**同一个** `thinking`
content block（曾按 delta 逐条新开 block）；`text`/`tool_use` 开始前先 `content_block_stop`
掉 thinking（补 `signature_delta`）；`index` **单调递增不复用**（否则 Claude 客户端按 index
累积时会用文本覆盖已关闭的 thinking 块）。

**OAI 聊天入口 → CC 的静默转换**（`ChatToCommandCode`，2026-09-12）：
`/v1/chat/completions` 无映射直连时，若模型只有 `command_code` 路由，网关自动回落到
`command_code` 上游（无需 Claude/Codex 映射）：请求转成 `/alpha/generate` body，流式
NDJSON 解码回 OpenAI SSE，非流式聚合为单个 `chat.completion`。`reasoning_effort` →
`params.reasoning_effort`；CC 的 `reasoning-delta` 以 `reasoning_content` 返回；usage
保持 OpenAI 形状（`prompt_tokens` 含缓存、`prompt_tokens_details.cached_tokens` 单列）。
因此 OMP/OpenCode 的 `local-gateway`（`@ai-sdk/openai-compatible`）provider 可直接选择
CC 模型，思考等级走 `reasoning_effort`（off/low/high/max）。

## 11. 版本漂移基线

| 指标 | 值（调研时） |
| --- | --- |
| npm 已发布版本总数 | 430 |
| 近 7 / 30 / 90 天发布 | 10 / 59 / 147 |
| 调研时版本 | `1.53.1`（2026-09-11） |
| CHANGELOG | 856 个标题，无 `BREAKING` 标记、不描述线协议 → **线变更静默发布** |

⇒ 约 **2 个版本/天**。任何逆向集成都是易碎件：网关必须做版本探测（npm `latest` 对照）、
未知事件容忍、夹具回归、快速降级与明确错误分类。

## 12. 许可与归属

- 事实依据：MAXeaglet/commandcode-proxy（MIT, © MAXeaglet）、patlux/pi-commandcode-provider（MIT, © Pat Woz）。
- 本文与网关实现**未复制**上述项目的代码；仅根据其公开行为整理事实。
- 若后续复用其代码片段，必须在文件头保留版权与 MIT 许可声明。

## 13. 凭据获取：网页登录授权（等价官方 `cmd login`）

**Go 套餐在 Studio 面板无法创建 API Key**。官方凭据只能由 CLI 的浏览器授权流程签发：
Studio 在用户授权后把 API Key POST 回本机回环服务。网关内建同一流程（无需安装 CLI）：

```
POST 127.0.0.1:5959–5968 /callback   （一次性回环服务）
浏览器 → {studio}/studio/auth/cli?callback=http%3A%2F%2Flocalhost%3A{port}%2Fcallback&state={state}

回调 JSON：
 成功 {"apiKey":"user_...","state":"...","userId":"...","userName":"...","keyName":"..."}
 拒绝 {"error":"access_denied","state":"...","error_description":"..."}
```

### 13.1 协议细节（三份 MIT 实现交叉验证）

| 项 | 约定 |
| --- | --- |
| Studio 基址 | `api.commandcode.ai` → `https://commandcode.ai`；`staging-api.*` → `https://staging.commandcode.ai`；`localhost` → `http://localhost:3000` |
| 回调地址 | 固定 `http://localhost:{port}/callback`（服务只绑定 `127.0.0.1`） |
| 端口 | 5959 起顺延最多 10 个；全部占用则失败 |
| `state` | 32 随机字节 base64url；**所有**回调分支（含 error）先校验 state |
| 回调体上限 | 10 KB（按 wire bytes，防 CJK 绕过） |
| 等待窗口 | 120 秒（与 CLI 一致），可取消；单飞：等待中重复 begin 复用同一次尝试 |
| CORS | 仅回显白名单 Origin（`http://localhost:3000`、`https://staging.commandcode.ai`、`https://commandcode.ai`）；附 `Access-Control-Allow-Private-Network: true`（HTTPS → localhost 的 PNA 预检） |
| 方法/路径 | 仅 `POST /callback`；`OPTIONS` 预检 204；其余 404/405 |
| 密钥校验 | `GET {api_base}/alpha/whoami`（Bearer）：200 通过；401 → `invalid-key`；其他非 2xx → 服务端错误；传输失败 → `network` |
| 密钥有效期 | 不过期（`access == refresh == apiKey`，社区实现给 10 年远期） |
| 失败原因 | `denied` / `timeout` / `invalid-key` / `network` / `error` / `cancelled` |
| 兜底 | 自动传输失败时，Studio 会展示可复制的 key；网关支持从官方 CLI 凭据文件导入：`~/.commandcode/auth.json`、`~/.config/commandcode/auth.json`，或环境变量 `COMMAND_CODE_API_KEY` / `COMMANDCODE_API_KEY` / `CMD_API_KEY` |

### 13.2 安全约定（网关实现）

- 密钥**绝不返回管理端前端**：校验通过后只给 UI 一次性 `login_id`（5 分钟 TTL、单次有效），
  渠道创建/改写时服务端用 `login_id` 取走密钥并立即加密入库。
- 回环服务只绑定 `127.0.0.1`，决定性回调后立即关闭；新登录作废旧的未使用交接。
- `state` 校验先于错误分支：任意网页无法仅凭一条 CORS 简单请求结束正在进行的登录。
- 凭据文件解析镜像社区规则（`apiKey`/`api_key`/`access`/`accessToken`/`token`/`key`，
  以及 `commandcode`/`auth`/`credentials`/`oauth`/`account` 嵌套）。

> 依据：patlux/pi-commandcode-provider（`src/auth-server.ts`、`src/oauth.ts`）、
> Mars-Sea/dsh-commandcode-provider（`src/login.ts` 与 wire 测试）、
> rashidrazak/opencode-cmd-provider（`src/plugin/auth.ts`），三者均为 MIT。
