# Local AI Gateway

面向个人本地环境的多供应商 AI 透明网关。后端使用 Python，管理端使用原生 HTML、CSS 和 JavaScript，提供同协议渠道故障转移、模型自动探测、渠道熔断恢复和请求用量统计。

## 当前能力

- OpenAI Compatible、OpenAI Responses、Claude、Gemini 四个独立协议池；同一渠道可启用多个协议。
- 按模型 ID 合并路由，渠道优先级只配置一次，请求时按入口协议过滤后进行同协议故障转移。
- 请求、成功响应和最终上游 HTTP 错误保持原始字节不变。
- 连续错误自动熔断 15 分钟，到期后以最小 `OK` 请求探测并静默恢复。
- 旁路统计首字节、首 Token、TPS、缓存读取/写入/未命中和输出 Token。
- 原生管理端覆盖供应商、渠道、模型探测、路由、日志、健康状态和运行设置，无需 Node.js 或前端构建步骤。

## 使用说明

- 首先添加供应商，包含名字和baseurl两个字段。
- 然后添加渠道，选择对应的供应商后填入apikey，即可完成渠道创建。
- 创建渠道后可以手动进行模型探测，会自动从上游探测可用模型。
- 必须在路由中配置模型以及路由规则，才能在v1/models端点检索到模型，并通过对应端点调用。
- 本项目只对请求进行简单转发，不进行任何额外处理，因此在配置渠道端点时务必确保上游支持所选端点。

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

需要 Python 3.12+ 和 [uv](https://docs.astral.sh/uv/)。

```bash
make install
cp backend/.env.example backend/.env
```

编辑 `backend/.env` 后运行：

```bash
make run
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

- 通用聚合目录：`GET /v1/models`，仅返回已配置且存在可用候选的模型，`x_local_gateway` 仅列出 `supported_endpoints`。
- OpenAI Compatible：`GET /v1/models?protocol=openai_compatible`
- OpenAI Responses：`GET /v1/models?protocol=openai_responses`；兼容别名为 `GET /v1/responses/models`
- Claude：携带 `anthropic-version` 请求 `GET /v1/models`；兼容别名为 `GET /v1/messages/models`
- Gemini：`GET /v1beta/models`

聚合目录保持 OpenAI 标准的 `object: "list"` 和 `data[].id` 结构，额外字段可被 CC Switch 等只读取标准字段的客户端忽略。`GET /v1/models` 也接受 `X-Local-Gateway-Protocol` 显式选择单个协议池。

## 开发与验证

```bash
make test
```

后端测试使用 mock 上游验证字节一致性、四协议入口、同协议隔离、故障转移、熔断和流式响应边界，不需要真实供应商密钥。

## systemd 部署

仓库提供 [服务模板](deploy/local-ai-gateway.service.example)。复制到 `/etc/systemd/system/local-ai-gateway.service` 后，替换 `YOUR_USER`、`YOUR_GROUP` 和 `/opt/local-ai-gateway` 为实际安装路径，再执行：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now local-ai-gateway
```

运行数据库、主密钥、`.env` 和本机专用服务文件均被 Git 忽略，不会上传到远端。

## 设计文档

- [需求基线](docs/requirements.md)
- [总体架构](docs/architecture.md)
- [API 设计](docs/api-design.md)
- [数据模型](docs/data-model.md)
- [实现计划](docs/implementation-plan.md)

## 核心边界

- 支持 OpenAI Compatible、OpenAI Responses、Claude、Gemini 四种协议。
- 只在完全相同的协议内进行故障转移，不做跨协议转换。
- 用户请求体、上游响应体和最终上游 HTTP 错误保持原始字节不变。
- 网关只处理路由、认证替换、逐跳头处理、故障转移、旁路统计和健康检查。
- 不进行价格或费用计算。

详细架构与验收边界见上述设计文档。
