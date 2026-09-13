# Local AI Gateway

面向个人本地环境的多供应商 AI 透明网关。核心网关与桌面端基于 Rust（Tauri 2），管理端使用原生 HTML、CSS 和 JavaScript，提供同协议渠道故障转移、模型自动探测、渠道熔断恢复和请求用量统计。

## 当前能力

- OpenAI Compatible、OpenAI Responses、Claude、Gemini 四个独立协议池；同一渠道可启用多个协议。
- 按模型 ID 合并路由，渠道优先级只配置一次，请求时按入口协议过滤后进行同协议故障转移。
- 请求、成功响应和最终上游 HTTP 错误保持原始字节不变。
- 连续错误自动熔断 15 分钟，到期后以最小 `OK` 请求探测并静默恢复。
- 旁路统计首字节、首 Token、TPS 与输出 Token，并透传上游 usage 中的提示词缓存统计（缓存读取/写入/未命中）。
- Claude 模型映射助手：对外暴露 Claude Code 标准模型名（如 `claude-opus-5`），控制台按模型独立配置上游协议与候选渠道，请求时自动完成协议转换（参考 CLIProxyAPI 的转换思路），路由实时生效。
- Codex 模型映射助手：对外暴露 Codex 标准模型名（如 `gpt-5-codex`），与 Claude 映射同一套转换体系，让 Codex CLI 通过独立入口 `/codex` 使用任意上游模型。
- 模型能力档案：内置 `capability_profiles` 表收集可复用的模型能力集合，支持手动创建/编辑/删除；在「模型路由 → 配置能力」中可直接选择档案应用（多个模型可共用同一套能力，如 GPT-5.6 Sol/Terra/Luna），修改档案自动同步到所有引用模型；成本仍按模型独立配置。
- 渠道余额查询（旁路、默认关闭）：按渠道手动选择 New API、Sub2API、OpenCode Go、DeepSeek 或自定义适配器；启用后每小时自动刷新，也可在渠道表单内「立即查询」或使用工具栏「刷新余额」批量刷新。上游失败只写归一化快照与稳定错误分类，不影响代理路径，也不保存原始响应正文或令牌。
- Command Code 集成（上游专用协议、默认关闭）：为 Go 套餐渠道提供「先官方 Provider API、403 `upgrade_required` 后降级 CLI 兼容路径」的 transport router，自动注入 CLI 身份头（会话/指纹/版本，按渠道独立持久化），把 NDJSON 流转换为 Claude / Codex 入口可用的响应，并支持额度（5h/周/credits）查询。凭据通过内建**网页登录授权**获取（与官方 `cmd login` 同一 loopback 流程，密钥不经过浏览器页面），也可从 CLI 凭据文件导入。风险与合规提示见下文。
- 原生管理端覆盖供应商、渠道、模型探测、路由、能力档案、Claude 映射、Codex 映射、日志、健康状态和运行设置，无需 Node.js 或前端构建步骤。

## 使用说明

- 首先添加供应商，包含名字和baseurl两个字段。
- 然后添加渠道，选择对应的供应商后填入apikey，即可完成渠道创建。
- 创建渠道后可以手动进行模型探测，会自动从上游探测可用模型。
- 必须在路由中配置模型以及路由规则，才能在v1/models端点检索到模型，并通过对应端点调用。
- 模型能力（上下文、最大输出、图像/思考支持、成本）可在「模型路由 → 配置能力」中手动配置；内置「能力档案」页可创建可复用的能力集合，在配置能力抽屉中选择档案即可一键应用并建立关联（如 GPT-5.6 Sol/Terra/Luna 共用一套），也可直接点「保存为档案」把当前表单存为新档案，修改档案会同步到所有引用它的模型。
- 渠道余额查询默认关闭且不产生任何探测请求：编辑渠道 →「余额查询」选择适配器并打开开关即可；「立即查询」只查当前渠道，每个渠道余额右侧还有独立的刷新按钮，工具栏「刷新余额」批量刷新全部已启用渠道，后台每小时自动刷新一次。未配置或 `enabled=0` 的渠道不参与后台/批量刷新；OpenCode Go 渠道按 5h / 周 / 月展示剩余百分比。New API（如 CCTQ）：渠道 API Key 只能读该令牌额度，把面板生成的「系统访问令牌 / PAT」填入「独立令牌」后，会改为查询并显示账户可用余额。
- 本项目模型路由只对请求进行简单转发，不进行任何额外处理，因此在配置渠道端点时务必确保上游支持所选端点。
- Claude 模型映射助手（Claude Code 标准名 → 系统中已配置的模型，独立于常规请求）：
  1. 先在供应商与渠道页配置好上游渠道并探测/登记模型，再到「模型路由」页为上游模型配置候选优先级（候选渠道直接在系统中已配置的模型里选择，映射无需单独配置候选）。
  2. 打开「Claude 映射」页，点击「添加映射」：可从预设下拉快速选择 Anthropic 当前模型名（如 `claude-opus-5`，内置默认 + 经已配置的 Claude 渠道实时刷新），再从「上游模型」下拉选择——数据源直接来自「模型路由」页已配置路由的模型，并按所选上游协议自动过滤；再选择该模型独立使用的上游协议（OpenAI Chat / OpenAI Responses / Claude 原生 / Gemini）。
  3. 保存后立即生效。候选渠道自动继承上游模型在「模型路由」中的配置，修改路由实时生效，无需单独设置映射候选。
  4. 映射仅通过独立入口 `/claudecode` 提供服务，不影响其他应用调用常规 `/v1/*` 端点：
     - `GET /claudecode`：接入信息（端点清单与配置提示）
     - `GET /claudecode/v1/models`、`GET /claudecode/v1/messages/models`：Claude Code 探测模型目录（Claude 格式）
     - `POST /claudecode/v1/messages`：模型请求入口，自动转换请求为上游协议、转发并故障转移，再把响应转换回 Claude 格式（流式响应逐事件转换）
  5. Claude Code 接入：设置 `ANTHROPIC_BASE_URL=http://<网关地址>:3000/claudecode`，`ANTHROPIC_API_KEY` 任意值，`ANTHROPIC_MODEL` 填映射名（如 `claude-opus-5`）。
- Codex 模型映射助手（Codex 标准名 → 系统中已配置的模型，独立于常规请求）：
  1. 配置方式与 Claude 映射相同：先在供应商与渠道页配置好上游渠道并探测/登记模型，再到「模型路由」页为上游模型配置候选优先级，然后打开「Codex 映射」页添加映射（预设下拉含 `gpt-5-codex` 等 Codex 标准模型名，内置默认 + 经已配置的 OpenAI Responses 渠道实时刷新；上游模型与上游协议选择同 Claude 映射）。
  2. 保存后立即生效。候选渠道自动继承上游模型在「模型路由」中的配置，修改路由实时生效，无需单独设置映射候选。
  3. 映射仅通过独立入口 `/codex` 提供服务，不影响其他应用调用常规 `/v1/*` 端点：
     - `GET /codex`：接入信息（端点清单与配置提示）
     - `GET /codex/v1/models`、`GET /codex/v1/responses/models`：Codex 探测模型目录（OpenAI 格式）
     - `POST /codex/v1/responses`：模型请求入口，自动转换请求为上游协议、转发并故障转移，再把响应转换回 OpenAI Responses 格式（流式响应逐事件转换）；支持 `input` 末尾带 `compaction_trigger` 的 V2 远程压缩请求
     - `POST /codex/v1/responses/compact`：Codex V1 远程压缩专属端点（仅 `openai_responses` 上游）
  4. Codex CLI 接入：在 `~/.codex/config.toml` 中设置（Codex 会向 base URL 追加 `/responses` 与 `/models`，因此必须带 `/v1` 前缀）：
     ```toml
     openai_base_url = "http://<网关地址>:3000/codex/v1"
     model = "gpt-5-codex" # 映射名，如 gpt-5-codex
     ```
     并设置任意值的 `OPENAI_API_KEY`。也可以改用自定义 provider（可同时关闭 WebSocket 探测，避免先连 WS 失败再回退 HTTP）：
     ```toml
     model = "gpt-5-codex"
     model_provider = "gateway"

     [model_providers.gateway]
     name = "Local AI Gateway"
     base_url = "http://<网关地址>:3000/codex/v1"
     env_key = "LOCAL_GATEWAY_KEY"
     supports_websockets = false
     ```
  5. Codex 远程压缩（Remote Compaction）：仅映射到 `openai_responses` 上游时可用。模型探测会自动探测渠道的 V1/V2 能力并原子落库；压缩请求在向客户端输出任何字节前完成校验，并按渠道优先级故障转移。管理端渠道列表会显示各渠道的 V1/V2 能力徽标。

### 控制台操作示意

#### 1. 添加供应商与渠道

供应商保存名称和 Base URL；在其下创建渠道后，选择上游支持的请求格式并填入 API Key。密钥在列表中仅显示脱敏提示。

![供应商与渠道页面](docs/images/providers-and-channels.jpg)

#### 2. 为模型配置路由优先级

将已探测到的模型加入路由，并按渠道设置候选顺序。优先级 `0` 最高，请求会在相同协议的可用候选中按此顺序尝试。

![模型路由页面](docs/images/model-routes.jpg)

#### 3. 查看请求结果与实际响应渠道

请求日志记录协议、响应渠道、结果、HTTP 状态、尝试次数和耗时。每个响应渠道独立显示，便于区分多次路由。

![请求日志页面](docs/images/request-logs.jpg)

## 快速启动

需要 Rust 工具链（[rustup](https://rustup.rs/)）与 Tauri 2 CLI：

```bash
cargo install --locked tauri-cli --version '^2'
cd desktop/src-tauri && cargo tauri dev
```

或仅启动无头网关（无桌面托盘，适合服务器部署）：

```bash
cd desktop/src-tauri && cargo run --bin gateway-headless
```

打开 [http://127.0.0.1:3000](http://127.0.0.1:3000)，信任局域网模式默认无需输入管理密钥。关闭信任后再使用设置页中的手动或随机密钥。首次配置顺序为：

1. 新建供应商并填写不带 `/v1` 或 `/v1beta` 的 API 根地址。
2. 在供应商下新建渠道，选择一个或多个请求格式并填写 API Key。
3. 执行模型探测。
4. 创建模型路由并为候选渠道设置唯一优先级。

默认监听 `0.0.0.0:3000` 并信任局域网访问，因此管理端和代理入口不要求本地访问密钥。关闭“信任局域网访问”后，可以在设置页手动填写或随机生成管理密钥与代理密钥。上游 API Key 使用本地主密钥加密后写入 SQLite，管理 API 不回显明文。

## 调用示例

OpenAI Compatible：

```bash
curl http://127.0.0.1:3000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"YOUR_MODEL_ID","messages":[{"role":"user","content":"Hello"}]}'
```

其他协议使用各自原生路径。关闭局域网信任后，可使用各协议原生认证位置或统一的 `X-Local-Gateway-Key` 提供代理密钥。

客户端模型目录端点：

- 通用聚合目录：`GET /v1/models`，仅返回已配置且存在可用候选的模型；每个条目在 `x_local_gateway` 中附带 `supported_endpoints`（该模型当前可路由的主入口，如 `["/v1/chat/completions"]`），有能力数据的模型另附 `capabilities` 与 `pi_model_config`（供目录消费方读取真实能力值）。
- OpenAI Compatible：`GET /v1/models?protocol=openai_compatible`
- OpenAI Responses：`GET /v1/models?protocol=openai_responses`；兼容别名为 `GET /v1/responses/models`
- Claude：携带 `anthropic-version` 请求 `GET /v1/models`；兼容别名为 `GET /v1/messages/models`
- Gemini：`GET /v1beta/models`

聚合目录保持 OpenAI 标准的 `object: "list"` 和 `data[].id` 结构，额外字段可被 CC Switch 等只读取标准字段的客户端忽略。

## 桌面打包

桌面端基于 Tauri 2。Windows 本机需要 Rust（`x86_64-pc-windows-gnu`）与 Tauri CLI；Linux 包通过 Arch WSL 构建。

```bash
cargo install --locked tauri-cli --version '^2'
make wsl-setup              # 首次：配置 Arch WSL 工具链与 builder 用户
```

发布流程：只改仓库根目录 `VERSION`，再一条命令出齐当前宿主能产出的全部安装包。

```bash
make package-all            # 先同步版本，再构建全部 release 到 releases/
make package-windows        # 本机 NSIS exe
make package-appimage       # Arch WSL：.deb + AppImage
make package-arch           # Arch WSL：.pkg.tar.zst
make package-windows-wine   # 可选：WSL/Linux 上 Wine 交叉编译 NSIS
make doctor                 # 打印宿主 / WSL 路径与工具，不编译
```

Windows 上 `package-all` 产出 NSIS exe + AppImage + deb + Arch pkg。Linux 宿主则本机构建 Linux 三件套，Windows 安装包改走 Wine。产物收集到仓库根目录 `releases/`（按当前 `VERSION` 过滤，并清掉旧版本残留）。发布标签也会通过 `.github/workflows/desktop.yml` 构建相同平台矩阵。

桌面启动器常驻系统托盘：关闭主窗口仅隐藏到托盘，托盘菜单可重新显示或彻底退出（同时停止网关）。启动器使用内建标题栏（无系统装饰，标题栏可拖动，自带最小化与关闭按钮——Linux Wayland 下系统标题栏按钮在隐藏/恢复后会失效，故弃用），窗口内可切换**开机自启动**（Windows 注册表 Run 键 / Linux autostart desktop 项 / macOS LaunchAgent）与**启动时最小化到托盘**（下次启动不显示主窗口，仅保留托盘图标）；自启动偏好保存在数据目录的 `launcher.json`，自启动状态以操作系统实际注册为准。

## 版本管理

项目版本遵循 SemVer，**只改仓库根目录的 `VERSION` 文件**。Cargo.toml、`tauri.conf.json`、`Cargo.lock`、`packaging/arch/PKGBUILD` 都从它同步（Arch `pkgver` 会把连字符映射成下划线，如 `0.3.0-rc.1 -> 0.3.0_rc.1`）。依赖版本、第三方锁文件条目与 `releases/` 历史产物绝不被改动。

```bash
# 1. 编辑 VERSION（一行 SemVer，例如 0.3.0 或 0.3.0-rc.1）
# 2. 同步到全部第一方声明（package-all 会自动做这一步）
make version-sync
make version-check          # 本地或 CI 校验各声明一致（退出码 0/1）
make version                # 查看当前版本

# 可选：一条命令同时写入 VERSION 并同步
make version-set VERSION=0.3.0-rc.1
```

- `sync` / `set` 只接受严格 SemVer；非法输入在任何写入前即失败。重复同步同一版本是幂等空操作。`VERSION` 允许 Windows 编辑器的 CRLF / UTF-8 BOM，同步时会规范成单一 LF 行。锁文件由 `cargo metadata` 重新解析更新，不做手工全文替换。
- 取舍说明：Tauri CLI 在 `tauri.conf.json` 缺省 `version` 时会回退到 Cargo.toml，但 `tauri-build` 的 Windows 可执行文件版本资源（FileVersion/ProductVersion）只读取 `tauri.conf.json`，因此保留并同步该字段，避免 Windows 构建产物丢失版本元数据。`PKGBUILD` 模板中的 `pkgver` 由本工具保持可读一致，构建时 `packaging/build-arch.sh` 还会从 Cargo.toml 覆盖，两条路径由 `check` 保证结果相同。
- 自校验：`./scripts/test-version.sh`（沙箱内验证 set/sync/check、非法输入零写入、幂等、预发布、CRLF、锁文件解析与打包文件名版本解析，无需构建安装包）。CI 的 `desktop.yml` 在打包前也会执行 `version.sh check`，并在 tag 推送时对账 `v$(cat VERSION)`。

## 开发与验证

```bash
cd desktop/src-tauri && cargo test
```

Rust 测试覆盖协议转换、加密与管理 API 等核心模块，使用 mock 上游验证字节一致性、同协议隔离、故障转移、熔断和流式响应边界，不需要真实供应商密钥。

## systemd 部署

仓库提供[服务模板](deploy/local-ai-gateway.service.example)（无头网关 `gateway-headless`，随 deb 包安装到 `/usr/bin`）。复制到 `/etc/systemd/system/local-ai-gateway.service` 后，替换 `YOUR_USER`、`YOUR_GROUP` 为实际运行用户，并确保其可读写 `AI_GATEWAY_DATA_DIR` 指向的数据目录（默认 `/var/lib/local-ai-gateway`），再执行：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now local-ai-gateway
```

运行数据库、主密钥和本机专用服务文件均被 Git 忽略，不会上传到远端。

## 设计文档

- [Command Code 协议基准](docs/command-code-protocol.md)
- [需求基线](docs/requirements.md)
- [总体架构](docs/architecture.md)
- [API 设计](docs/api-design.md)
- [数据模型](docs/data-model.md)
- [实现计划](docs/implementation-plan.md)

## 核心边界

- 支持 OpenAI Compatible、OpenAI Responses、Claude、Gemini 四种入口协议；Command Code 是**上游专用**协议（经 Claude/Codex 映射接入，没有客户端入口）。
- 模型路由功能只在完全相同的协议内进行故障转移，不做跨协议转换。
- 用户请求体、上游响应体和最终上游 HTTP 错误保持原始字节不变；Command Code 是唯一例外（入口请求/响应需按 CLI 线协议转换）。
- 网关只处理路由、认证替换、逐跳头处理、故障转移、旁路统计和健康检查。
- 不进行价格或费用计算。

详细架构与验收边界见上述设计文档。

## Command Code Go（路线 B，默认关闭）

> **风险提示**：Go 套餐没有官方 API 访问。网关默认先请求官方 Provider API，服务端以 `403 upgrade_required`
> 拒绝后，**仅对该账号**降级到 CLI 兼容路径（`/alpha/generate`），并使用 CLI 身份头（会话、指纹、版本）。
> 这是对服务端明确拒绝路径的绕过，可能违反服务条款并导致账号封禁。启用即表示已知晓并自行承担全部风险。
> 集成默认关闭；关闭状态下网关不会向 Command Code 发出任何请求（含探测/发现/额度）。

配置步骤：

1. 「供应商与渠道」→「添加供应商」→ 选择预设 **Command Code Go**（`https://api.commandcode.ai`，`kind=command_code`），
   阅读并勾选风险确认后创建。
2. 在该供应商下「添加渠道」：请求格式勾选 `command_code`。
3. **获取凭据（Go 套餐无法在面板创建 API Key）**：在渠道表单点击「网页登录授权」——网关会启动与官方
   `cmd login` 完全相同的本机回环流程并打开 `commandcode.ai` 授权页；授权完成后 Studio 签发的 API Key
   直接写入网关（浏览器页面与管理端前端都不会接触到密钥），表单显示已授权的账号后保存即可。
   也可以点「从 CLI 导入」读取已登录 CLI 的 `~/.commandcode/auth.json`（或环境变量
   `COMMAND_CODE_API_KEY`）。密钥会先经 `GET /alpha/whoami` 校验，失败会在表单内提示原因
   （拒绝 / 超时 / 校验失败 / 网络错误 / 已取消）。若浏览器无法回传，可在授权页复制 key 到「API Key」输入框手动粘贴。
4. 为渠道探测/手工登记模型（目录端点 `GET /provider/v1/models`；Go 套餐被 403 拒绝时请手工添加模型）。
5. 在「Claude 映射」或「Codex 映射」中把标准模型名映射到 `command_code` 上游模型——Command Code 只通过映射接入。
6. 「设置」→ 打开 **Command Code 集成** 开关并确认风险；同一面板显示 CLI 版本与协议基准的漂移告警。
7. 可选：编辑渠道 →「余额查询」选择 `Command Code` 适配器并启用，显示 5h/周窗口与 credits。

> 登录授权协议（回环回调、state 校验、CORS/PNA、失败分类、CLI 凭据导入）记录在
> `docs/command-code-protocol.md` §13，与三份 MIT 参考实现交叉验证。

模型与额度：`/provider/v1/models` 实测为公开目录，网关正常探测；实时目录不可用（网络/5xx/空）时自动
退回官方 CLI 包内的 Go 模型快照（探测运行会带 `command_code_bundled_catalog` 诊断）。额度走
`whoami → billing/credits → usage/summary`，需要先在渠道表单启用 `Command Code` 余额适配器；若页面提示
「集成未启用」，请到「设置 → Command Code」打开开关（默认关闭，关闭时探测/额度/代理都不会发出请求）。

运行时行为：首次请求先走官方 Provider API，命中 `403 upgrade_required` 后记忆该渠道为 `generate` 路径；
`GOAT/Pro/Max` 用户全程留在官方 API。指纹/生命周期事件按渠道（每 API Key）在首次 + 每 8h（+抖动）上报；
会话按渠道粘滞 12h（+抖动）以保持提示缓存命中；额度类错误（402/429）会把渠道冷却到窗口重置时间；
`/alpha/generate` 在途请求按渠道限流（默认 2，设置项 `command_code_max_concurrency`），避免单账号突发并发。

身份头只按 `providers.kind='command_code'` 注入，绝不嗅探 `base_url`：把 `base_url` 指向自建桥
（或其他第三方上游）时，只要供应商没有 `kind` 标记，网关不会发送任何 CLI 身份信息。
