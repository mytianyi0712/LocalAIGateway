Local AI Gateway 0.2.5

本版本新增两项**默认关闭**的旁路能力：渠道余额查询与 Command Code Go 上游集成。两者都不改动既有转发路径——未配置、未开启时不产生任何上游请求，现有供应商、渠道、映射与密钥无需调整。

## ✨ 新增功能

- ✨ feat(balance): 新增渠道余额查询旁路能力

  按渠道手动选择 New API、Sub2API、OpenCode Go、DeepSeek 或自定义适配器并打开开关；启用后每小时后台刷新，也可在渠道表单内「立即查询」或用工具栏「刷新余额」批量刷新。

  - New API 支持渠道 Key（令牌额度）与面板「系统访问令牌 / PAT」（账户可用余额）两种口径；Sub2API 读取 `/v1/usage`；OpenCode Go 展示 5h / 周 / 月三档剩余百分比；DeepSeek 读取 `/user/balance`；自定义适配器支持 JSON 路径映射与 `${api_key}` / `${token}` 模板（自定义 method / headers / body）。
  - 余额查询是旁路：上游失败只写归一化快照与稳定 `error_kind`，不影响代理路径，也不保存原始响应正文或令牌。

- ✨ feat(command-code): 新增 Command Code Go 上游集成与网页登录授权

  Command Code 作为**上游专用**协议接入（没有客户端入口，只经 Claude / Codex 映射使用）：首次请求走官方 Provider API，命中 `403 upgrade_required` 后**仅对该渠道**记忆并降级到 CLI 兼容路径 `/alpha/generate`，把 NDJSON 流转成 Claude / Codex 入口可用的响应（流式与非流式）。

  - 凭据获取：内建**网页登录授权**（与官方 `cmd login` 相同的 loopback 流程，回调端口 5959–5968、state 校验、`/alpha/whoami` 校验），Studio 签发的 API Key 直接加密入库，浏览器页面与管理端前端都接触不到密钥；也可「从 CLI 导入」`~/.commandcode/auth.json` 或环境变量。
  - 身份头只按 `providers.kind='command_code'`（迁移 `0005`）显式注入（会话、指纹、CLI 版本），**绝不嗅探 `base_url`**：把地址指向自建桥或第三方上游时不会发送任何 CLI 身份信息。
  - 会话按渠道粘滞 12h（+抖动）以保持提示缓存命中；指纹与生命周期事件在首次 + 每 8h（+抖动）上报；`/alpha/generate` 在途请求按渠道限流（默认 2）。
  - 额度类错误（402 / 429）把渠道冷却到窗口重置时间，窗口过后由既有健康探测自动放回；渠道余额新增 `Command Code` 适配器（5h / 周窗口 + credits）。
  - 新增供应商预设目录（`Command Code Go` 预设带强制风险确认）与 `GET /command-code/status`、`POST/GET/DELETE /command-code/login`、`POST /command-code/import-cli` 管理端点；设置面板显示 CLI 版本与协议基准的漂移告警。

## ⚠️ 风险提示

Command Code Go 套餐**没有官方 API 访问**：网关默认先请求官方 Provider API，服务端以 `403 upgrade_required` 拒绝后，仅对该账号降级到 CLI 兼容路径并使用 CLI 身份头。这是对服务端明确拒绝路径的绕过，**可能违反服务条款并导致账号封禁**；启用即表示已知晓并自行承担全部风险。集成默认关闭，关闭状态下网关不会向 Command Code 发出任何请求（含探测、发现与额度查询）。

## 📦 构建与打包

- 📦 build: 版本号统一升级到 0.2.5

  `VERSION`、`Cargo.toml`、`Cargo.lock`、`tauri.conf.json` 与 `packaging/arch/PKGBUILD` 同步升级到 0.2.5。

## 📥 下载

| 平台 | 安装包 |
| --- | --- |
| Windows x64 | `Local AI Gateway_0.2.5_x64-setup.exe` |
| Linux x86_64（AppImage） | `Local AI Gateway_0.2.5_amd64.AppImage` |
| Debian / Ubuntu | `Local AI Gateway_0.2.5_amd64.deb` |
| Arch Linux | `local-ai-gateway-0.2.5-1-x86_64.pkg.tar.zst` |

## ⬆️ 升级说明

- 首次启动自动应用 `0004_channel_balance` 与 `0005_command_code` 数据库迁移，无需手动操作。
- 两项新能力默认关闭：未配置余额适配器的渠道不产生任何余额请求；未开启「设置 → Command Code」时探测、发现、额度与代理都不会访问 Command Code。
- 既有供应商 `kind` 为 `NULL`，行为与升级前完全一致；供应商、渠道、映射与密钥无需修改。
- 使用 Command Code 的顺序：新建 `kind=command_code` 供应商与 `command_code` 渠道（Go 套餐无法在面板创建 API Key，用「网页登录授权」或「从 CLI 导入」）→「设置 → Command Code」开启开关并确认风险 → 在 Claude / Codex 映射中把标准模型名指向该上游模型。
- 实时模型目录 `/provider/v1/models` 被 403 拒绝时，探测会回退到官方 CLI 包内的 Go 模型快照（运行记录带 `command_code_bundled_catalog` 诊断），也可手工增删模型。

## 📚 相关文档

- [Command Code 协议基准](https://github.com/mytianyi0712/LocalAIGateway/blob/main/docs/command-code-protocol.md)
- [API 设计](https://github.com/mytianyi0712/LocalAIGateway/blob/main/docs/api-design.md)
- [数据模型](https://github.com/mytianyi0712/LocalAIGateway/blob/main/docs/data-model.md)
