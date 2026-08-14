# 总体架构

状态：当前实现基线（Rust 重写版）
更新日期：2026-08-11

## 1. 架构目标

架构围绕三个约束设计：

1. 代理热路径保持简单，原始请求字节和响应正文不经过业务重编码；上游压缩（gzip/deflate/brotli/zstd）仅在传输层解码为明文后转发，不改变业务内容。
2. 故障转移严格限制在相同协议和相同模型 ID 内。
3. 路由、熔断、探测和统计可以独立演进，任何旁路能力失败都不影响代理返回。

## 2. 技术选型

### 2.1 后端（当前实现）

| 领域 | 选型 | 说明 |
| --- | --- | --- |
| 运行时 | Rust（stable toolchain） | 单一 crate `desktop/src-tauri` |
| Web 框架 | Axum | 管理 API、代理路由、流式响应、ConnectInfo |
| HTTP 客户端 | reqwest | 按连接超时分组的客户端池（`HttpClients`） |
| 数据访问 | SQLx（SQLite，WAL） | 运行时迁移 + 显式事务 |
| 密钥保护 | Fernet（`crypto.rs`） | 主密钥文件 + 加密 API Key |
| 桌面外壳 | Tauri（`lib.rs`/`controller.rs`） | 托盘常驻、端口切换 |
| 无头运行 | `bin/gateway-headless.rs` | 服务/打包场景 |
| 测试 | `cargo test` + 本地 TCP mock 上游 | 端到端代理路径回归 |

前端仍为零构建的原生 HTML/CSS/JS（`frontend/public`），由网关自身托管（`assets.rs`）。

### 2.2 运行时所有权（P1-1/P1-3）

所有后台任务——HTTP serve、健康探测 supervisor、维护 supervisor、遥测 writer、一次性探测/发现任务——都由 `RuntimeSupervisor`（`infrastructure.rs`）统一持有，且 supervisor 的 `JoinSet` 持有的是**任务本体**：

- health/maintenance/telemetry 以 `run_supervisor`/`run_writer` 任务体形式直接注册（`server::spawn_background`），不存在「外层 await 内层 handle」的双层 `tokio::spawn`——deadline abort 不会把内层任务 detach 掉。
- 健康 supervisor 内部的 probe `JoinSet` 由健康任务局部持有；正常取消时 `shutdown()` join，abort 时 `Drop` 同步 `abort_all()`，probe 永远不会比健康任务活得更久。
- 注册是「加锁后立即入 JoinSet」的同步操作，取消开始后拒绝新注册（`spawn -> Result<(), ShuttingDown>`），不存在未登记窗口或孤儿任务。
- `shutdown(deadline)` 是唯一关闭入口：置位 → cancel → 限时 join → 超时 `abort_all` 再 join。controller / headless / Tauri 托盘退出共用同一入口，不再有 10s/30s 双套期限。`active_task_count()` 在 shutdown 返回后恒为 0（活动计数守卫在任务完成或被 abort 时递减），测试用 drop guard 验证 abort 后资源释放。
- 一次性任务通过 `spawn_tracked` 返回 `TaskOutcome`，失败由 supervisor 记录并计数（P2-8），后台边界不再静默丢错误。

### 2.2.1 异常 serve 退出（P1-2）

`ServerController` 每次 `start()` 递增 runtime generation；serve 任务（supervisor JoinSet 内）只做状态簿记并经 completion channel 上报结果。controller 持有的 monitor（JoinSet 外、由 `RunningServer` 显式持有）在异常退出时核对 generation 后取走 slot、先更新状态再统一 shutdown；`start()` 只把「slot 存在且 serve 未结束」视为已运行，陈旧 slot 先 drain 再重建。并发 start/stop/set_port 由 slot 锁串行化，不会产生第二个 generation。

### 2.3 运行常量（P2-10）

调度间隔与内部安全上限集中在 `runtime::RuntimeLimits`（reaper 500ms、probe 20s/5s、discovery 120s/50 页、maintenance 60s/3600s、错误体 1 MiB、关闭期限 30s），构建一次、不可变、全 supervisor 共享；测试注入短配置。用户可配的上游期限仍在 `settings::RuntimeSettings`（含新增 `max_buffered_upstream_body_mb`，默认 64，范围 1–1024）。

## 3. 模块结构（当前）

```text
desktop/src-tauri/src/
├── main.rs / lib.rs          # Tauri 入口（托盘、命令）
├── bin/gateway-headless.rs   # 无头服务入口
├── controller.rs             # 桌面端 ServerController（start/stop/set_port）
├── server.rs                 # 组装根：build()、RuntimeSupervisor、HttpClients
├── application.rs            # 共享请求上下文 Context（P2-2）
├── runtime.rs                # RuntimeLimits
├── auth.rs                   # AdminAuth / RecoveryAuth / RecoverySession
├── api_error.rs              # 稳定错误信封 + request-id 中间件
├── admin/                    # 管理 handler 域拆分（P2-4）
│   ├── mod.rs                # router、AdminService（db+secrets 窄服务）、共享 helpers、测试
│   ├── providers.rs          # 供应商 CRUD（薄 handler，全部委托 AdminService）
│   ├── channels.rs           # 渠道 CRUD/密钥/健康重置（薄 handler + AdminService impl）
│   ├── models.rs             # 渠道模型清单
│   ├── routes.rs             # 路由与候选（协议支持校验 P2-6）
│   ├── profiles.rs           # 能力画像（写入校验 P2-4、删除事务化）
│   ├── mappings.rs           # Claude/Codex 映射与预设
│   ├── settings.rs           # 设置/密钥/系统状态（含恢复 nonce 流程）
│   ├── logs.rs / stats.rs / discovery.rs
├── proxy.rs                  # 代理热路径（ProxyService：prepare/attempt/stream 策略/终态）
├── routing.rs                # 候选解析
├── convert/                  # 协议互转域拆分（P2-4）
│   ├── mod.rs                # 共享转换 helpers、pub use 重导出
│   ├── request.rs            # 请求方向（claude/responses/gemini → 上游）
│   ├── response.rs           # 非流式响应 + DSML 解析
│   ├── stream.rs             # MappedStreamConverter 增量流式转换
│   ├── error.rs / scan.rs
├── protocol.rs               # ProtocolId 枚举与协议边界函数
├── compression.rs            # 有界解码（Observable/Required）
├── health.rs                 # 探测与熔断恢复
├── discovery.rs              # 模型发现（快照 + 单事务应用）
├── maintenance.rs            # 周期维护
├── telemetry.rs              # 事件队列与 writer
├── capabilities.rs           # 能力档案
├── settings.rs               # 运行时设置
├── crypto.rs / db.rs / config.rs / assets.rs
```

分层方向（当前已落地）：

- `ports.rs` 定义五个应用端口：`UpstreamClient`（上游 HTTP）、`RouteRepository`（候选/目录/映射查询）、`ChannelRepository`（渠道行加载）、`EventSink`（遥测）、`Clock`（时间）。
- `infrastructure.rs` 提供全部端口实现与运行时所有权：`HttpClientPool`（reqwest 池）、`SqliteRouteRepository`/`SqliteChannelRepository`、`SystemClock`、`RuntimeSupervisor`。
- `application::Context` 只持有端口与组合好的服务：`http`/`routes`/`channels`/`clock`/`discovery`。业务模块（proxy/health/discovery/routing）经端口访问基础设施——proxy 不再直接 import `reqwest`/`sqlx`；`server.rs` 退化为纯组装根（build/serve/spawn_background），业务模块不再反向引用它（P2-1/P2-2）。
- `DiscoveryService`（discovery.rs）与 `ProxyService`（proxy.rs，纯端口依赖：db/secrets/http/routes/telemetry/clock/limits）按端口/窄依赖组织；`AdminService`（admin/mod.rs，db+secrets 窄服务）承载 provider/channel 域 SQL（P2-1）。`AttemptFinalizer` 经 `EventSink` 端口发遥测。
- 协议身份收敛为 `protocol::ProtocolId` 枚举 + `ProtocolAdapter` registry（认证、入口解析、发现、健康探测/判定、usage 观察、公开错误外形）；转换矩阵收敛为 `convert::ConversionStrategy` registry（`(entry, upstream) -> strategy`，覆盖请求/响应/流式转换的调度）；前端协议列表来自 `/system/protocols`（P2-3）。
- `scripts/check-layers.sh`（CI 门禁）：`application`/`domain`/`ports` 层禁止直接 import `axum`/`sqlx`/`reqwest`；已迁移的 admin 域文件（providers.rs、channels.rs）额外受 handler 范围检查——handler 函数体内禁止出现 `sqlx::`（SQL 只允许在 `impl AdminService` 或存储 helper 中），其余子域迁移完成后加入该检查列表。
- P2-1 已落地：`AdminService`/`ProxyService` 结构形式化；provider 域双实现已删除（router 只绑定服务用例）；`proxy()` 函数级拆分（bounded_non_stream/final_gateway_response）完成。
- 迁移状态（进行中，按报告附录逐子域推进）：models/routes/profiles/mappings/settings/logs/stats 仍以直写 SQL 的 handler 为主，尚未全部迁入 `AdminService`；`Context` 仍是宽 service locator。这些是已知的进行中事项，不作为已完成边界描述。

## 4. 代理热路径（proxy.rs）

`proxy()` 由四个阶段组成（P2-4 已拆分）：

1. `prepare_request`：设置读取、请求体有界读取、网关鉴权、模型识别、映射解析、候选路由；失败直接返回网关错误。
2. 候选尝试循环：逐渠道尝试，全部失败按最后状态分类收尾（502/504）。
3. 响应策略：
   - `transparent_stream`：非映射流式 2xx 原样转发，明文观察有界（P1-1）。
   - `mapped_stream`：映射流式（prelude 缓冲 + 增量转换），prelude 失败可故障转移。
   - 非流式（映射与非映射统一）：有界缓冲 + RequiredDecoder + 转换（P1-1/P1-2）。
4. 终态：所有路径共用 `AttemptFinalizer::finalize`（P1-5）——channel/attempt/request 三类事件由单一 `AttemptOutcome` 驱动，不再可能互相矛盾。

### 4.1 上游响应内存边界（P1-1）

- 非映射流式：全程流式转发，无整体缓冲。
- 映射非流式：`Content-Length` 预检 + chunk 累计硬上限 `max_buffered_upstream_body_mb`；超限返回稳定 `502 upstream_response_too_large`，绝不把部分 JSON 交给转换器。
- 非 2xx 错误体：只保留 `RuntimeLimits::error_body_max`（1 MiB），截断记录 `body_truncated=true`。

### 4.2 解码器（P1-2）

- `ObservableDecoder`：仅用于透明转发的 usage 观察；超限/失败后静默停喂，转发不受影响。
- `RequiredDecoder`：用于映射转换；保留未消费输入余量（无损续传）、累计明文硬上限、`finished()` 截断检测；任何失败都是终态错误，绝不静默截断。

### 4.3 转换失败终态（P1-5）

转换失败保持协议兼容的 HTTP 200 + 固定错误体，但遥测记录 `outcome="gateway_error"`、`error_kind="conversion_error"`、无成功 usage、无 `ChannelSuccess`——统计不再把失败请求计为成功。

## 5. 事务边界

- 模型发现（`discovery.rs`，P2-5）：网络阶段只构造不可变 `DiscoverySnapshot`；模型 upsert、协议绑定、陈旧绑定删除、`available` 重算与成功 run 终态在**一个事务**内提交；失败 run 单独短事务写失败原因，不回滚旧目录。
- 健康探测（`health.rs`，P2-6）：网络探测收集 `ProbeResult[]`，全部日志与 `channel_health` 聚合在**一个事务**内提交。
- 供应商 PATCH（P1-4）：name/base_url 全部输入先校验，再以**单条动态 UPDATE** 原子提交——非法 URL 或语句失败不会留下半更新行；删除路径保持显式事务。
- 能力画像更新（P1-4）：`capability_profiles` 行与全部引用它的 `model_caps` 传播在**一个事务**内提交，事务内无网络调用；故障注入测试（SQLite trigger）锁定回滚行为。
- 渠道 PATCH（P2-3）：名称、协议、健康模型与可选的新 API key 在同一事务内提交；「仅轮换密钥」的独立端点保留给明确操作。
- 其余管理写路径使用显式 `begin/commit`，事务内无 HTTP 调用。

## 6. 访问控制与密钥恢复

- 局域网信任模式默认开启；关闭后代理入口与管理 API 分别校验密钥（`settings.rs`），密钥加密存储。
- 运行时设置读取是 **fail-closed**（P1-3）：按键白名单查询，任一已存在的运行时行 JSON 损坏或类型错误即返回带键名的 `ConfigCorrupted`——鉴权与代理路径拒绝请求，绝不回退默认值（损坏的 `trust_local_network` 不可能变成 `true`）；范围校验由写路径 `validate_updates` 负责，格式合法但越界的旧值仍可读（消费端按 `max(1)` 等防御性夹取）。
- 设置页读取走 `runtime_settings_ui`：损坏行跳过并返回 `config_corrupted_keys` 列表，页面显示修复提示，重新填写保存即修复。
- 密钥损坏时进入恢复模式（P1-4）：`RecoveryAuth` 校验 loopback peer + Host；生成端点额外校验同源 `Origin` 与一次性 256-bit nonce（`x-recovery-nonce` 头，60s TTL，一次有效）。跨站表单、DNS rebinding、重放、过期 nonce 全部拒绝；同源 UI 流程：status 发 nonce → generate 消费 nonce → 立即恢复正常鉴权。

## 7. 可观测性

- 管理 API：`RequestId` 中间件注入/回写 `x-request-id`，handler 内部事件通过 tracing span 继承 request_id（P2-7），客户端 ID 可精确定位底层错误日志。
- 代理路径：`request_logs` + `request_attempts` + `token_usage`（writer 单事务写，attempt 幂等）。
- 后台失败计数：`RuntimeSupervisor::failed_task_count()`；discovery 持久化失败转为可查询的 `persistence_error`（P2-8）。

## 8. 前端（frontend/public）

- 原生 SPA：History API + 网关静态回退；`app.js` 单一入口。
- 导航竞态（P2-9）：mutation 开始时捕获 `{path, renderVersion}`，数据写入照常完成，但 reload/render 只在页面仍匹配时执行；`loadX` 内部校验目标 path；纯查询可被导航丢弃。
- 预设刷新（P1-5）：按钮触发后端真实 discovery 排队（Claude→`claude` 协议渠道；Codex→`openai_responses`/`openai_compatible` 渠道），前端轮询各 run 终态，完成后重新 GET presets 并刷新下拉；无渠道/部分失败/导航离开/重复点击均有确定状态。预设列表由 `channel_models + channel_model_protocols` 实时聚合，无第二份缓存。
- 设置表单（P1-7）：数字字段由单一 schema（`SETTINGS_SECTIONS`）驱动渲染、提交载荷与前端范围校验；`max_buffered_upstream_body_mb` 随 schema 一并提交；设置损坏时页面显示 `config_corrupted_keys` 提示并可重新保存修复。
- 渠道保存（P2-3）：API key 并入单个原子 PATCH，不再拆成两个请求。
- 协议列表数据驱动自 `/system/protocols`（常量仅作离线兜底）。
- 密钥恢复 UI：损坏时渲染恢复页，携带 nonce 完成一次生成。

## 9. 部署形态

- 桌面：Tauri 托盘常驻；退出时 `controller.stop()` 走统一 supervisor shutdown，保证遥测尾部落盘。
- 无头：`gateway-headless`（Ctrl-C → 统一 shutdown）。
- 单进程固定（后台任务持有进程内所有权，未来多进程化需先替换任务锁与日志队列）。
