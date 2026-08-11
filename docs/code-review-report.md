# Local AI Gateway 深度静态审查与修复计划

- 审查对象：当前工作树中的 Rust/Axum/Tauri 后端、SQLite 迁移、原生 JavaScript 前端和架构文档
- 审查基准：依赖方向、模块边界、事务、SOLID、防御式编程、异常、副作用、并发、命名与可读性
- 复核日期：2026-08-11
- 结论：上一轮代理响应边界、协议转换、遥测终态、密钥恢复、发现/健康事务等核心缺陷已大幅整改，当前未发现 P0。仍有 7 项 P1 行为缺陷和 5 项 P2 架构/维护问题；其中后台任务的真实所有权、运行时配置损坏后的 fail-open、管理更新部分提交和预设刷新空实现应优先处理。

## 1. 范围与本轮验证

当前可执行后端位于 `desktop/src-tauri/src`，管理前端位于 `frontend/public/assets`。本报告以当前源码为准，不把 Java 专属检查项机械套用到 Rust：项目没有 `@Table`、`@RestController`、`@Transactional`、继承层次或手工数据库连接释放；对应风险按 SQLx 事务、Axum state、Rust trait/所有权和 Tokio task 语义检查。

本轮已执行：

- `cargo test --all-targets`：通过，3 个测试套件共 100 个测试。
- `cargo clippy --all-targets -- -D warnings`：失败；`proxy.rs:3455`、`proxy.rs:3515` 两个测试循环触发 `clippy::while_let_loop`。
- `node --check frontend/public/assets/app.js`、`api.js`、`ui.js`：通过。
- `node --check desktop/ui/launcher.js`：通过。
- `scripts/check-layers.sh`：通过。
- 源码复核：代理 body/stream、压缩解码、遥测终态、运行时任务所有权、controller 重启、管理事务、设置读取、预设刷新、前端表单和架构分层。

本轮未执行真实供应商集成测试、Playwright/Chromium UI 测试、内存剖析和操作系统级退出压力测试。测试通过只能证明现有断言成立；例如 `update_profile_propagates_to_model_caps_in_one_transaction` 只覆盖成功路径，当前实现实际上没有事务。

## 2. 总体结论

| 等级 | 数量 | 结论 |
|---|---:|---|
| P0 | 0 | 未发现当前必然导致全局数据破坏或持续不可用的缺陷 |
| P1 | 7 | 任务脱管、异常退出不可恢复、鉴权配置 fail-open、部分提交、功能空桩和设置失效会直接影响生产行为 |
| P2 | 5 | 服务层重复、宽上下文、跨请求写入、错误吞没和超大模块继续放大维护与回归风险 |
| 质量门 | 1 | Clippy 当前失败，不能宣称发布质量门全绿 |

## 3. 已确认关闭的历史问题

以下问题已由当前实现和测试关闭，不再列入待修项：

- 首 Token deadline 已锚定 attempt start；普通流与映射流均覆盖延迟响应头。
- mapped prelude timeout 与非流式连接重置已区分 504/502，并写入 attempt。
- 非流式上游响应、映射 body 和非 2xx 错误 body 已有明确上限；压缩观察与强制解码已拆分为 `ObservableDecoder`/`RequiredDecoder`。
- gzip/deflate/Brotli/Zstandard 映射流不再重复解码，解码器已有累计、扩张比和损坏帧处理。
- 流完成、上游中断和客户端取消已使用单一终态路径；取消会保留已观察到的 bytes/usage。
- 转换失败通过 `AttemptOutcome` 统一 channel/attempt/request 遥测，不再计为成功。
- 请求体限制、connect/first-byte/first-token/idle/total timeout 已进入运行时设置并区分错误类型。
- 401/403、5xx 和 transport failure 的熔断分类已有专门逻辑。
- 候选协议支持性校验、能力检测协议 join、多协议健康探测均已整改。
- discovery snapshot 与成功 run 终态、probe logs 与 channel health 已分别进入单事务。
- 设置与访问密钥写入已进入同一事务；损坏密钥有 loopback + Host/Origin + 一次性 nonce 恢复流程。
- 管理 API 已有稳定错误信封和 request-id tracing span，内部错误文本不直接暴露。
- `ProtocolId`、协议 adapter、conversion strategy、`ports.rs`/`infrastructure.rs`、`admin/`、`convert/`、`api.js`/`ui.js` 已形成第一轮拆分。

## 4. P1 行为缺陷

### P1-1：RuntimeSupervisor 仍未拥有真实后台任务

**位置**：

- `desktop/src-tauri/src/server.rs:139-181`
- `desktop/src-tauri/src/health.rs:277-342`
- `desktop/src-tauri/src/maintenance.rs:94-129`
- `desktop/src-tauri/src/telemetry.rs:142-174`
- `desktop/src-tauri/src/infrastructure.rs:372-453`

`server::spawn_background` 向 supervisor 注册的是外层 future；外层 future 再调用 `health::spawn_supervisor`、`maintenance::spawn_supervisor`、`Telemetry::spawn_writer`，这些函数内部各自执行 `tokio::spawn` 并返回 `JoinHandle`。外层只负责 await handle，真正任务不在 supervisor 的 `JoinSet` 中。

正常取消时内层任务通常会观察 token 并退出；但 shutdown deadline 到期后，supervisor abort 的只是等待 `JoinHandle` 的外层任务。外层被 abort 会丢弃 handle，而 Tokio 丢弃 handle 会 detach 内层任务。此时“超时后 abort 并 join，返回时无任务存活”的契约不成立；telemetry writer 还可能继续等待 sender 关闭，健康/维护任务可能继续持有旧 `Context`、数据库 pool 和凭据。

**修复**：

1. 将三个 `spawn_*` API 改为不自行 spawn 的 `run_*` async future；由 `RuntimeSupervisor::spawn` 直接注册实际任务。
2. 健康 supervisor 内部 probe `JoinSet` 可继续由健康任务局部拥有；健康任务被取消或 abort 时必须 `shutdown()` 该集合。
3. 删除返回 `JoinHandle` 后再 await 的双层模式；生产代码除明确由 controller 持有的生命周期 monitor 外不得直接 `tokio::spawn`。
4. 为 supervisor 增加活动任务计数/测试探针，验证 deadline abort 后实际任务和数据库引用均释放。

**验收**：构造永不结束的 writer、health、maintenance future，使用极短 shutdown deadline；`shutdown()` 返回后全部 drop guard 被触发、活动任务为 0、旧数据库目录可删除，重复启动 10 次不累积任务。

### P1-2：HTTP serve 异常退出后 runtime 仍占用 controller slot

**位置**：`desktop/src-tauri/src/controller.rs:71-108`

serve task 出错时只把 `runtime.running=false` 和错误文本写入状态；`ServerController.server` 中的 `RunningServer` 仍为 `Some`，共享 cancellation token 也没有被取消。后续 `start()` 在 `slot.is_some()` 时直接返回成功，实际上不会重新绑定端口；health、maintenance 和 telemetry 等后台任务仍可继续运行。

**修复**：

1. 每次启动生成 runtime generation ID。
2. serve task 通过 completion channel 报告正常/异常结束；controller-owned monitor 仅在 generation 仍匹配时取走 slot、触发统一 shutdown 并更新状态。
3. `start()` 只把“slot 存在且 supervisor 未结束”视为已运行；陈旧 slot 必须先清理再启动。
4. monitor 不能从 supervisor 自己的 `JoinSet` 内调用 `shutdown()`，避免任务等待自身；其 handle 由 `RunningServer` 显式持有和 join。

**验收**：注入 accept/serve 错误，确认后台任务被取消、slot 清空、错误可见；随后在同一端口调用 `start()` 能重新监听。并发 `start/stop/set_port` 不得启动两个 generation。

### P1-3：损坏的运行时设置会静默回退，鉴权可 fail-open

**位置**：`desktop/src-tauri/src/settings.rs:57-70`

`runtime_settings_from` 遍历 settings 行时只在 JSON 解析成功时覆盖默认值；解析失败的行被静默跳过。若持久化的 `trust_local_network` JSON 损坏，读取结果会回退到默认 `true`，管理 API 和代理入口可从“要求密钥”变成“信任本地网络”。这与访问密钥损坏时显式 `CorruptKey` 的 fail-closed 策略不一致。

该查询还读取除密钥外的所有 settings 行，预设缓存等非运行时键依赖 serde 默认忽略未知字段，边界不清晰。

**修复**：

1. 定义运行时设置键白名单，只查询/解析 `RuntimeSettings` 字段；预设等其他设置不得进入该对象。
2. 任一已存在的运行时行 JSON 损坏、类型错误或越界时返回带键名的 `ConfigCorrupted` 内部错误并记录日志，禁止回退默认值。
3. 鉴权读取失败必须拒绝请求；只有“数据库中不存在该键”才能使用代码默认值。
4. 启动/设置页提供不泄露原始值的损坏提示和修复入口。

**验收**：分别破坏 boolean、integer 和 JSON 文本；管理/代理鉴权均 fail-closed，日志含键名和 request ID，修复后恢复。特别断言损坏 `trust_local_network` 不会变成 `true`。

### P1-4：供应商和能力档案更新仍可部分提交

**位置**：

- `desktop/src-tauri/src/admin/providers.rs:80-113`
- `desktop/src-tauri/src/admin/mod.rs:241-269`
- `desktop/src-tauri/src/admin/profiles.rs:96-113`

供应商 PATCH 先提交 name，再校验并提交 base URL；当 URL 非法或第二条 SQL 失败时，接口返回错误但 name 已改变。该逻辑在 handler 和 `AdminService` 中重复存在。

能力档案更新先提交 `capability_profiles`，再查询并更新引用它的 `model_caps`，没有事务。第二步失败会让档案与模型能力不一致。现有测试名称声称“one transaction”，但只检查成功后的值，没有故障注入，无法发现该缺陷。

**修复**：

1. 所有输入在写入前完成 trim、URL、范围和唯一性校验。
2. 供应商使用单条动态 UPDATE 或一个事务，name/base_url 作为一个 PATCH 原子提交。
3. 档案行更新与所有 `model_caps` 传播放入同一事务；事务内不执行网络调用。
4. 删除 provider handler 的重复实现，仅保留 service 用例。

**验收**：在第二条 UPDATE 上通过 SQLite trigger 注入失败，断言供应商/档案/模型能力全部保持旧值；成功路径只生成一个一致的 `updated_at` 快照。

### P1-5：预设刷新端点是返回成功的空实现

**位置**：

- `desktop/src-tauri/src/admin/mappings.rs:264-323`
- `desktop/src-tauri/src/admin/mod.rs:126-140`
- `frontend/public/assets/app.js:1159-1163,1205-1213`

Claude/Codex 两个 refresh 路由共用 `refresh_presets`，函数不区分类型、不查询渠道、不排队 discovery、不更新 settings，只返回 `202 {"status":"queued"}`。前端收到 202 后也不轮询、不重新获取预设、不显示成功结果。按钮和帮助文本明确宣称“经已配置渠道实时刷新”，当前行为与产品契约相反。

**修复**：

1. 拆分 Claude/Codex refresh handler，明确目标协议集合：Claude 使用 `claude`；Codex 使用 `openai_responses`/`openai_compatible` 中产品认可的集合。
2. 对支持目标协议且启用的渠道排队真实 discovery，返回 `run_ids`；无可用渠道返回明确 409/422，不得假报 queued。
3. GET presets 直接从当前 `channel_models + channel_model_protocols` 聚合可用模型并与内置默认去重，避免维护第二份易损缓存；若保留缓存，则必须在所有 run 完成后单事务更新。
4. 前端轮询 run 终态，完成后重新 GET presets 并刷新 select；部分失败显示成功/失败渠道数量。

**验收**：使用两个 mock 渠道返回不同目录，刷新后预设合并且去重；一个渠道失败时结果和提示明确；无渠道、导航离开、重复点击和后台持久化失败均有确定状态。

### P1-6：健康探测模型无法从指定值清回自动

**位置**：

- `desktop/src-tauri/src/admin/channels.rs:63-70,210-238`
- `frontend/public/assets/app.js:474-489,506-524`

前端选择“自动”时发送 `health_check_model_id: null`。后端字段是 `Option<String>`，JSON `null` 与字段缺失都反序列化为 `None`；更新逻辑只在 `is_some()` 时执行，因此无法把数据库中的旧模型改回 NULL。接口返回成功，但后续探测仍使用旧配置。

**修复**：PATCH DTO 使用 `Option<Option<String>>` 或明确 patch-field 类型，区分未提交、清空和值；`Some(None)` 写 NULL，`Some(Some(value))` 校验该模型属于渠道后写入。

**验收**：指定 A → PATCH null → GET 返回 null；下一次多协议 probe 使用各协议自动选择的可用模型，而不是 A。

### P1-7：设置页显示了响应缓冲上限，但保存时漏提交

**位置**：`frontend/public/assets/app.js:1340-1366`

页面渲染 `max_buffered_upstream_body_mb` 输入框，但 `saveSettings()` 的 payload 没有该字段。用户修改后点击保存会收到“运行设置已保存”，后端值实际不变。

**修复**：将字段加入 payload，并将设置表单字段定义收敛为一个 schema，用同一 schema生成渲染、提交和范围，避免新增字段只接入一半。

**验收**：UI 将值从 64 改为 128，PATCH 请求包含 128，重新加载仍显示 128；非法值由前端约束和后端 422 同时阻止。

## 5. P2 架构与维护问题

### P2-1：AdminService 与 handler 存在两套供应商实现

`admin/providers.rs` 保留完整 SQL CRUD，`admin/mod.rs` 又实现一套 `AdminService` provider CRUD；router 当前绑定前者。两套实现会独立演化，且都包含 P1-4 的部分提交缺陷。渠道 handler 已委托 `state.admin`，供应商没有采用同一模式。

**修复**：router handler 只做 extractor/DTO，全部委托 `AdminService`；删除重复 SQL、JSON assembly 和验证分支。随后按 routes/profiles/mappings/settings/models/logs/stats 子域逐个迁移，不保留双实现。

### P2-2：Context 仍是宽 service locator，高层仍直接拿 Database/SecretStore

`application::Context` 同时公开 `db`、`secrets`、HTTP/repository ports、多个 service、telemetry、supervisor 和 limits。大量 admin handler、health、maintenance 仍通过 `state.db.pool()` 直接写 SQL。`check-layers.sh` 能阻止 application/domain 导入框架，但不能阻止 handler 绕过应用服务与 repository 边界。

**修复**：为每个 router 使用窄 state（如 `AdminApiState`、`ProxyApiState`）；管理命令进入 application service，查询进入 repository/query service。`Context` 最终只存在于 composition root，不作为所有 handler 的公共依赖。

### P2-3：编辑渠道的一个表单跨两个 API，仍会产生部分成功

前端 `saveChannel()` 先 PATCH 名称/协议/健康模型，再单独 PUT API key。第二个请求失败时页面报错，但前半部分已经提交。该行为虽然符合两个独立端点各自的事务边界，却不符合“保存渠道表单”这一用户命令的原子预期。

**修复**：允许渠道 PATCH 可选携带新 API key，由同一 service 事务更新渠道、协议、模型绑定和密钥；独立 key endpoint 保留给明确的“仅轮换密钥”操作时，UI 必须拆成两个独立命令和反馈。

### P2-4：持久化 JSON 仍有静默降级路径

`admin/mappings.rs:280-301` 对预设 JSON 解析失败直接回内置默认；`admin/profiles.rs:24-35,254-258` 对 `thinking_level_map` 解析失败返回 null。此类损坏没有日志、错误码或修复提示，会把数据损坏伪装成“没有配置”。

**修复**：数据库 JSON 统一经 typed decoder 读取；损坏时记录表/主键/字段并返回 `config_corrupted`，或由明确的后台修复任务隔离坏行。不得在 API 查询热路径用 `.ok()`/`unwrap_or_default()` 吞掉持久化格式错误。

### P2-5：第一轮拆分完成，但核心模块和文档仍过度声明完成度

`proxy.rs` 当前 4551 行（含大量回归测试），`frontend/public/assets/app.js` 1673 行；`application::Context` 注释仍写“下一步替换宽上下文”，而 `architecture.md` 同时宣称所有任务都由 supervisor 真实持有、所有管理写路径均有事务。文档与 P1-1/P1-4 的实际实现冲突。

**修复**：行为缺陷关闭后再做结构性拆分：将 proxy tests 移入按场景组织的测试模块，将 attempt orchestration/response policy/finalization 保持独立；前端继续按页面域拆分。架构文档只描述已经由源码和测试证明的边界，进行中事项单列为迁移状态。

## 6. 审查清单结论

| 检查项 | 当前结论 | 证据/说明 |
|---|---|---|
| 高层依赖低层实现 | **部分违反** | proxy/discovery 已使用 ports；admin/health/maintenance 仍直接依赖 SQLx/Database |
| 框架注解污染领域 | **不适用/未发现** | Rust 无 Java 注解；问题是边界不足而非注解污染 |
| 模块边界 | **部分改善** | admin/convert/frontend 已拆分，但 Context 宽且多个子域仍直写 SQL |
| 循环依赖 | **已改善** | `server -> handler -> server::AppState` 已由 application Context 消除；宽上下文仍是 service locator |
| 事务位置 | **部分违反** | discovery/probe/settings 已事务化；provider/profile PATCH 仍部分提交 |
| 事务内 RPC/MQ | **未发现** | 当前显式事务内未发现 HTTP send 或 telemetry emit |
| SRP/规模 | **违反** | proxy/app.js 与宽 Context 仍承担过多职责 |
| OCP | **明显改善** | ProtocolId/adapter/ConversionStrategy 已收敛大部分协议分支；新增协议仍需枚举和注册表扩展 |
| ISP | **无直接违反** | ports 较窄；主要问题是 handler 仍绕过 ports |
| LSP | **不适用** | 当前无业务继承/可替换子类型层次 |
| 空集合/null | **未发现集合违规** | 列表返回 Vec/数组；null 用于 unknown/可清空字段，但 PATCH DTO 需区分缺失与 null |
| public 入参校验 | **部分满足** | 大多数文本/范围/协议已校验；provider 校验顺序导致先写后报错 |
| 异常吞没 | **部分违反** | 任务错误已有日志/计数；持久化 JSON 仍存在 `.ok()` 静默降级 |
| ErrorCode/友好提示 | **基本满足** | API 有稳定 code/message/request_id；配置损坏需覆盖非密钥设置 |
| 请求上下文存 Field | **未发现** | 请求中间态位于局部 future；controller/runtime state 属于合法生命周期状态 |
| DTO/VO 不可变性 | **无并发缺陷证据** | Rust move/borrow 保证请求 DTO 不被跨线程共享修改 |
| 资源释放 | **违反** | 双层 spawn 使 deadline abort 可能 detach 真实任务 |
| 布尔命名 | **不作为缺陷** | `enabled/available/running/success` 符合 Rust/JS 习惯 |
| 命令查询分离 | **部分满足** | 查询大多无写入；refresh 命令当前是假命令，channel 表单跨两个命令 |
| 魔法值 | **已改善** | RuntimeLimits 已集中；用户设置与内部安全上限已基本分离 |

## 7. 分阶段修复计划

### 阶段 A：发布门禁与运行时所有权

1. 修正 `proxy.rs` 两个测试循环，使 Clippy 恢复全绿。
2. 将 health/maintenance/telemetry 改为由 `RuntimeSupervisor` 直接运行实际 future，删除双层 `tokio::spawn`。
3. 引入 runtime generation + serve completion monitor；异常退出自动取消后台任务、清空 slot，并允许重启。
4. 增加 deadline abort、异常 serve、并发 start/stop/set_port、10 次重启和数据库释放测试。

**完成标准**：测试、Clippy、JavaScript 语法和分层门禁全部通过；supervisor 活动任务归零后才允许 controller 报告 stopped。

### 阶段 B：配置 fail-closed 与原子更新

1. runtime settings 改为键白名单 + 严格 typed decode；损坏配置返回 `config_corrupted`，鉴权 fail-closed。
2. provider PATCH 预校验后单事务/单 UPDATE；删除 handler/service 双实现。
3. profile + model_caps 更新进入单事务，补 trigger 故障注入。
4. ChannelPatch 区分 omitted/null/value，修复健康模型清空。
5. 前端设置 schema 补齐 `max_buffered_upstream_body_mb` 并统一渲染/提交范围。

**完成标准**：每个第二步失败场景均完全回滚；损坏 `trust_local_network` 不能开启信任；UI 保存后重载值一致。

### 阶段 C：实现真实预设刷新

1. 拆分 Claude/Codex refresh handler，定义目标协议集合。
2. 对匹配渠道排队 discovery 并返回 run IDs；GET presets 从当前可用模型实时聚合。
3. 前端轮询终态、刷新 select、展示部分失败；导航后不覆盖新页面。
4. 删除无用途的 settings 预设缓存，或为保留缓存建立明确的单事务更新任务，二者只保留一个来源。

**完成标准**：mock 上游目录变化能通过按钮反映到预设；无渠道、失败、重复刷新和导航竞态均有明确行为。

### 阶段 D：完成应用服务边界

1. provider/channel 先完成单一路径，再按 routes/profiles/mappings/settings/models/logs/stats 迁移。
2. 命令使用 application service，查询使用 repository/query service；handler 只处理 HTTP DTO。
3. 用窄 router state 替换公共 `Context`，禁止业务模块直接取得完整 Database/SecretStore。
4. 扩展 `check-layers.sh`：除 infrastructure/repository 实现外，application service 和 handler 禁止新增散落 SQL。

**完成标准**：每个管理用例只有一个实现；删除旧 helper/重复 SQL，不保留 shim。

### 阶段 E：前端命令原子性与结构整理

1. 将渠道配置与可选密钥轮换合为一个原子 service 命令，或在 UI 中明确拆成两个独立操作。
2. 页面 mutation 全部使用 `{path, renderVersion}`，查询使用页面级 AbortController。
3. 按 providers/routes/profiles/mappings/logs/settings 页面域继续拆分 `app.js`，共享 state mutation 通过明确接口完成。

**完成标准**：Playwright 覆盖保存、删除、预设刷新、密钥轮换过程中导航/后退/失败；旧页面不得覆盖新页面，错误命令不得留下未提示的部分成功。

### 阶段 F：文档与发布验证

1. 按最终实现更新 `docs/architecture.md` 的任务所有权、事务和服务层状态。
2. 保持 `docs/rust-vs-python-gateway-diff.md` 为明确标记的历史快照，不把旧差异当当前缺陷。
3. 执行完整质量门、真实上游 smoke、浏览器场景、内存边界和 OS 级退出压力测试。

## 8. 验收矩阵

| 场景 | 当前状态 | 修复后要求 |
|---|---|---|
| Rust 回归测试 | 100 通过 | 保持通过 |
| Clippy `-D warnings` | 2 个测试 lint 失败 | 0 warning |
| JS 语法/分层门禁 | 通过 | 保持通过并接入 CI |
| supervisor deadline abort | 实际任务可能 detach | 返回后实际任务数 0、资源可释放 |
| serve 异常退出 | slot 残留、后台继续 | 自动 shutdown、slot 清空、可重启 |
| 损坏 trust setting | 静默回默认 true | fail-closed + config_corrupted |
| provider/profile 第二步失败 | 可部分提交 | 单事务完全回滚 |
| 清空 health model | PATCH 成功但值不变 | GET 返回 null，probe 自动选择 |
| 响应缓冲上限 UI 保存 | 字段未提交 | PATCH/重载一致 |
| Claude/Codex 预设刷新 | 202 空操作 | 真实 discovery + 新预设可见 |
| 渠道配置 + key 更新 | 两请求可部分成功 | 一个原子命令或两个明确独立命令 |
| 持久化 JSON 损坏 | 部分字段静默 null/default | 结构化错误、日志和修复路径 |
| 管理服务边界 | 多子域直写 SQL | 每个用例单一 service/repository 实现 |
| 浏览器导航竞态 | 本轮未执行 | mutation 完成不覆盖新页面 |
| OS 退出/10 次重启 | 仅现有单元/集成测试 | 无旧 task、pool、writer 或端口占用 |

## 9. 推荐执行顺序

严格按 A → B → C → D → E → F 推进。A 解决资源所有权和发布门禁，B 解决安全与数据一致性，C 关闭用户可见的空功能；只有这些行为由测试锁定后再继续服务层和前端拆分。不要在双层 spawn、部分提交和配置 fail-open 尚存在时扩大模块重构，否则故障会被结构迁移掩盖，回归定位成本更高。
