# 变更日志

Local AI Gateway 的版本变更记录，按版本倒序排列。版本号形如 `MAJOR.MINOR.PATCH[-fixN]`，权威来源是仓库根目录的 `VERSION` 文件。

> **历史文档说明**：本文件合并了此前的历史改动与审查文档的结论——`docs/release-notes-0.2.5.md`（0.2.5 发布说明）、
> `docs/code-review-report.md`（2026-08-11 代码审查快照）、`docs/rust-vs-python-gateway-diff.md`（2026-08-04
> Rust/Python 差异审计快照）、计划文档 `docs/implementation-plan.md` 与 `.mimocode/plans/*`。
> 这些文件已在 2026-09-29 的文档整理中移除，原文可在 git 历史中检索：`git log --diff-filter=D --name-only -- docs/`。
> 未入库的本地快照（根目录 `CODE-REVIEW-REPORT.md`、`docs/code-health-report.md`）不在版本库中，已不在工作区。

## [0.2.6] — 2026-09-29

### ✨ 新增

- **自定义模型：路由名与上游模型解耦**
  - 「模型路由」页的请求模型改为自由填写，并勾选该路由支持的协议（不再要求与某个渠道模型同名）。
  - 一个自定义模型可绑定多个请求源：候选可来自不同渠道、各自携带不同的上游 `model_id`。
  - 仅当候选上游 ID 与请求模型不同时才改写请求体中的 `model`（Gemini 改写 URL 路径段），ID 相同时保持逐字节透传。
  - 「添加路由」下拉只列出仍有供应渠道（`available=1`、渠道启用、未熔断）的模型；已配置路由与孤儿路由保留至手动删除。
  - 候选抽屉展示每个候选映射到的上游模型 ID；新增 `frontend/public/assets/model-search.js` 模糊搜索。
  - `/channel-models` 增加 `channel_enabled` / `health_state` 字段；attempt 遥测记录实际发送的上游模型 ID。

### ♻️ 重构

- 两轮代码健康整改（运行时行为变更仅「修复」一节所列两处）：
  - `proxy.rs`（8601 行）拆为 `proxy/` 下 11 个文件，`proxy()` 由 757 行降到约 170 行；`infrastructure.rs`、`convert/stream.rs` 目录化。
  - 新增叶子模块 `domain.rs`（领域类型，禁止依赖 axum/sqlx/reqwest）、组合根 `state.rs::AppState`、统一 SSE 原语 `sse.rs`、测试支撑 `test_support.rs`。
  - 重复实现收口：SSE 分帧 4 套 → 1、cache-miss 公式 3 → 1、`Bearer` 头 4 → 1、settings KV 3 → 1、协议 → 路径表 3 → 1。
  - 新增依赖门禁 `scripts/check-module-graph.py`（Tarjan 找环，35 个顶层模块 0 环）；`scripts/check-layers.sh` 在 CRLF 工作区也可运行。
  - 测试用例 268 → 312。

### 🐛 修复

- 透明转发的终态判定统一为 `convert::stream_completed`（判定集合取并集）：修复中间分片携带 `"finish_reason": null` 被误判为终态、导致尾部 usage 丢失的回归；客户端断开时不再把已完成的流记为 `cancelled`。
- cache-miss 计量口径统一为 `domain::cache_miss_input` 单一来源。

### 📝 文档

- 新增本文件；移除计划与历史快照文档（见文首说明）。

## [0.2.5] — 2026-09-13

本版本新增两项**默认关闭**的旁路能力：渠道余额查询与 Command Code Go 上游集成。两者都不改动既有转发路径——未配置、未开启时不产生任何上游请求，现有供应商、渠道、映射与密钥无需调整。

### ✨ 新增

- **渠道余额查询**（旁路，默认关闭）：按渠道手动选择 New API、Sub2API、OpenCode Go、DeepSeek 或自定义适配器并打开开关；启用后每小时后台刷新，也可在渠道表单内「立即查询」或用工具栏「刷新余额」批量刷新。
  - New API 支持渠道 Key（令牌额度）与面板「系统访问令牌 / PAT」（账户可用余额）两种口径；Sub2API 读取 `/v1/usage`；OpenCode Go 展示 5h / 周 / 月三档剩余百分比；DeepSeek 读取 `/user/balance`；自定义适配器支持 JSON 路径映射与 `${api_key}` / `${token}` 模板（自定义 method / headers / body）。
  - 余额查询是旁路：上游失败只写归一化快照与稳定 `error_kind`，不影响代理路径，也不保存原始响应正文或令牌。
- **Command Code Go 上游集成 + 网页登录授权**（旁路，默认关闭）：Command Code 作为**上游专用**协议接入（没有客户端入口，只经 Claude / Codex 映射使用）：首次请求走官方 Provider API，命中 `403 upgrade_required` 后**仅对该渠道**记忆并降级到 CLI 兼容路径 `/alpha/generate`，把 NDJSON 流转成 Claude / Codex 入口可用的响应（流式与非流式）。
  - 凭据获取：内建网页登录授权（与官方 `cmd login` 相同的 loopback 流程，回调端口 5959–5968、state 校验、`/alpha/whoami` 校验），签发的 API Key 直接加密入库，浏览器页面与管理端前端都接触不到密钥；也可「从 CLI 导入」`~/.commandcode/auth.json` 或环境变量。
  - 身份头只按 `providers.kind='command_code'`（迁移 `0005`）显式注入（会话、指纹、CLI 版本），**绝不嗅探 `base_url`**：把地址指向自建桥或第三方上游时不会发送任何 CLI 身份信息。
  - 会话按渠道粘滞 12h（+抖动）以保持提示缓存命中；指纹与生命周期事件在首次 + 每 8h（+抖动）上报；`/alpha/generate` 在途请求按渠道限流（默认 2）。
  - 额度类错误（402 / 429）把渠道冷却到窗口重置时间，窗口过后由既有健康探测自动放回；渠道余额新增 Command Code 适配器（5h / 周窗口 + credits）。
  - 新增供应商预设目录（`Command Code Go` 预设带强制风险确认）与 `GET /command-code/status`、`POST/GET/DELETE /command-code/login`、`POST /command-code/import-cli` 管理端点；设置面板显示 CLI 版本与协议基准的漂移告警。
  - ⚠️ **风险提示**：Command Code Go 套餐没有官方 API 访问。网关默认先请求官方 Provider API，服务端以 `403 upgrade_required` 拒绝后，仅对该账号降级到 CLI 兼容路径并使用 CLI 身份头。这是对服务端明确拒绝路径的绕过，**可能违反服务条款并导致账号封禁**；启用即表示已知晓并自行承担全部风险。集成默认关闭，关闭状态下网关不会向 Command Code 发出任何请求（含探测、发现与额度查询）。

### 🐛 修复

- 透传并自动补全 opencode session 请求头：渠道指向 OpenCode Zen/Go（`opencode.ai` 主机或路径含 `zen` 段）时，出站请求自动携带 `x-opencode-session`——客户端已携带的非空值原样透传（重复头收敛为一个），缺失时回退到按安装持久化的 uuid，避免上游因缺少该头拒绝请求（2026-09 起 Go/Zen 的强制要求）。推理、模型目录发现、协议探测与健康检查四条出站路径均已接入；非 opencode 渠道不受影响。
- 远程压缩失败不再计入渠道熔断：远程压缩是 Codex 映射入口的降级路径，上游不支持时按能力回退，尝试失败不应影响渠道正常流量的健康判定；压缩期间的 failover 不再发送桌面故障转移通知。

### 📦 构建

- 版本号统一升级到 0.2.5：`VERSION`、`Cargo.toml`、`Cargo.lock`、`tauri.conf.json` 与 `packaging/arch/PKGBUILD` 同步。

### ⬆️ 升级说明

- 首次启动自动应用 `0004_channel_balance` 与 `0005_command_code` 数据库迁移，无需手动操作。
- 两项新能力默认关闭；既有供应商 `kind` 为 `NULL`，行为与升级前完全一致，供应商、渠道、映射与密钥无需修改。
- 实时模型目录 `/provider/v1/models` 被 403 拒绝时，探测会回退到官方 CLI 包内的 Go 模型快照（运行记录带 `command_code_bundled_catalog` 诊断），也可手工增删模型。

## [0.2.4] — 2026-08-23

- ✨ 新增 Codex V1/V2 远程压缩支持。
- 📦 版本号统一升级到 0.2.4；修复 wine 构建在非 ASCII 或不可写目录下失败（构建前切换到用户主目录）。

## [0.2.3] — 2026-08-17

- 🐛 修复能力档案保存时报 `getModalMode` 未定义。
- 🐛 修复成功流 `first_token_ms` 为空：识别推理、思考与工具调用增量。
- 📦 Windows 上改 `VERSION` 即可同步清单；`make package-all` 把 Linux 构建放到 WSL 独立 target，避开 drvfs 与 target 冲突；补齐 Tauri 多平台图标资源。

## [0.2.2] — 2026-08-14

- ✨ 统一桌面端、网页 favicon 与安装器图标，并以 SVG 作为图标生成源。
- ✨ 调整日志表格布局；按供应商名称与渠道名称稳定排序渠道列表。

## [0.2.1-fix3] — 2026-08-07 — 2026-08-14

- 🐛 修复压缩 SSE 流异常中断：流式透传时解码上游压缩响应并移除 `Content-Encoding`，避免截断压缩体触发客户端解压错误，解码失败记录为 `stream_interrupted`。
- ✨ 渠道故障转移时通过桌面通知提醒用户，并附带稳定的错误标签（`connect_timeout`、HTTP 500 等）；1 秒窗口内的多次转移合并为一条通知，无桌面会话时静默降级。同时重构 `/v1/models` 的协议选择（查询参数、请求头、聚合回退）与 Makefile 的双宿主 Windows/WSL 构建流程，修复 MinGW 下 common-controls-v6 导致的启动失败。
- 🐛 修复映射流缺失输入与缓存命中用量记录（Responses 嵌套 `input_tokens_details.cached_tokens`；passthrough 映射流同步观察 usage）。
- 🐛 补齐模型目录端点信息。

## [0.2.1-fix2] — 2026-08-07

- ♻️ 移除 Python 后端并归档代码，版本管理改为仅 Rust 端。
- 🐛 修复 Codex 模型目录端点返回空列表。
- 📝 README 更新缓存统计与模型目录字段描述。

## [0.2.1] — 2026-08-04

- ✨ 新增 Token 用量统计与时间窗筛选。

## [0.2.0-fix1 / fix2] — 2026-08-03 — 2026-08-07

- 🐛 对齐 Python 网关功能差异并修复打包版本流程：映射入口 404、完整转换层、增量流式、熔断自动恢复、维护任务、能力检测、Claude 分页条件；打包自动清理旧 release 并按版本过滤产物。
- 🐛 修复 Windows 控制台窗口与 Linux 标题栏失效，内建标题栏并新增自启动选项。
- 🐛 修复 `/v1/responses` 与 `/v1/messages` 空回复：改为常规路由不再查映射表，同协议转换恒等透传保留工具调用，并补全流式 usage 遥测。
- ✨ 模型目录新增能力元数据；新增桌面端、托盘常驻与多平台打包；修复数据库迁移兼容与请求统计记录。

## [0.2.0] — 2026-08-03

- ✨ 新增 Rust 桌面网关与持久化代理能力（axum + Tauri，SQLite 迁移与请求日志）。
- ✨ 新增 Claude / Codex 模型映射助手与能力档案。
- ✨ 新增模型能力探测与路由候选拖拽排序。
- ✨ 四种协议（OpenAI Compatible、OpenAI Responses、Claude、Gemini）独立适配器、健康探测、同协议故障转移与熔断。
- 📝 `docs/` 下建立需求基线、总体架构、API 设计与数据模型文档。

## [0.1.0] — 2026-07-22 — 2026-07-31

- ✨ 初始化本地 AI 网关项目（Python/FastAPI 后端 + 原生 HTML/CSS/JavaScript 管理端），首版实现四种协议适配器、模型探测、认证注入、健康探测与 usage 归一化；后端随后由 Rust 实现取代（见 0.2.0 与 0.2.1-fix2）。

---

## 未修复问题（迁移自已移除的审查快照）

以下条目在 2026-09-28 的整改中经用户决策**登记保留**，尚未修复；细则可查 git 历史中 `docs/code-review-report.md` 被移除前的最后一版。2026-09-28 代码健康整改的完整清单原为未入库的本地快照（`docs/code-health-report.md`），已不在工作区。

- **P1｜管理面默认对局域网开放且不校验对端 IP**：`host` 默认 `0.0.0.0`、`trust_local_network` 默认 `true`，`authorize_admin` 只看请求头不看 peer IP；同网段设备可直接访问 `/api/admin/v1/*`。建议默认 `127.0.0.1` 或校验 `ConnectInfo`。
- **P1｜余额配置可用绝对 URL 且渲染渠道密钥**：自定义适配器的 `base_url` 仅校验为绝对 HTTP(S)，`render_template` 会把 `api_key` / `token` 注入 path / headers / body，可将渠道密钥外发到任意主机。建议只允许相对路径或主机白名单。
- **P1｜上游地址不拦私网 / 云元数据地址（SSRF 面）**：`normalize_base_url` 仅校验 scheme 与 host，渠道 `base_url` 可指向 `127.0.0.1`、`169.254.169.254` 等。
- **P2（局部，应排期）**：运行时设置损坏后 fail-open；加密 / 序列化失败静默写空串；明文密钥在探测与登录流程中驻留内存；Windows 上 `master.key` 无 0600 等价保护；`command_code_slots` 不随渠道删除清理；探测无缓存 / 降频；通知通道无背压（`mpsc::unbounded_channel`）；转换层无界缓冲与 O(n²) 分帧；`list_channels` 的 1+3N 查询放大；模块图仍有一个被批准的环 `{application, health, settings, telemetry}`。
