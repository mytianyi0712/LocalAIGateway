/**
 * 管理界面主控模块：应用外壳、路由调度与各页面渲染与交互。
 *
 * 职责：维护全局 `state`，按 `pageMeta` 分发页面渲染，处理所有表单提交、
 * 轮询与 DOM 事件，并调用 `api.js` 访问管理 API。
 * 边界：只做展示与交互；鉴权、协议转换、熔断等一律由后端决定，前端仅按
 * 后端返回值呈现。
 * 关键不变量：所有异步渲染都以「页面上下文」（renderVersion + 当前路径）
 * 校验，过期动作不得覆盖新页面。
 */
import {
  elements, icon, escapeHtml, escapeAttr, number, tokenCount, percent,
  parseStandardTime, formatTime, formatLogTime, duration, protocols,
  responseChannelTags, healthInfo, outcomeInfo, statusDot, button, iconButton,
  toolbar, panel, emptyState, skeleton, toast, openModal, closeModal,
  openDrawer, closeDrawer, confirmAction, resolveConfirmation, metric,
  tokenMetric, settingNumberField, pageError, getModalMode,
} from './ui.js';
import { getToken, setToken, api, get, post, put, patch, remove, setUnauthorizedHandler } from './api.js';
import { rankModelMatches } from './model-search.js';

// 协议列表以 /system/protocols 为准（启动时写入 state.protocols）。
// 此常量是 state.protocols 为空时的兜底：注册表尚未取到或网关返回空时，
// protocolOptions() 仍能渲染渠道表单与日志筛选里的协议选项。
// 须与 `protocol.rs::ProtocolId::ALL` 同步（新增协议时两处一起改）。
const FALLBACK_PROTOCOLS = ['openai_compatible', 'openai_responses', 'claude', 'gemini', 'command_code'];
function protocolOptions() {
  return (state.protocols && state.protocols.length ? state.protocols : FALLBACK_PROTOCOLS);
}

const pageMeta = {
  '/': { title: '运行概览', description: '请求、Token 与渠道健康状态' },
  '/providers': { title: '供应商与渠道', description: '上游端点、账号和模型探测目录' },
  '/routes': { title: '模型路由', description: '按模型统一配置候选优先级，请求时按格式过滤' },
  '/profiles': { title: '能力档案', description: '可复用的模型能力集合，多个模型可共用同一套能力' },
  '/logs': { title: '请求日志', description: '请求结果、性能指标与上游尝试' },
  '/settings': { title: '运行设置', description: '访问策略、熔断与超时参数' },
};

const state = {
  renderVersion: 0,
  currentPath: '/',
  connected: false,
  trustedLocal: true,
  providers: [],
  providerPresets: [],
  commandCodeStatus: null,
  channels: [],
  channelModels: [],
  routes: [],
  profiles: [],
  summary: null,
  system: null,
  protocols: null,
  logs: { page: 1, total: 0, items: [], filters: { protocol: '', upstream_protocol: '', model_id: '', upstream_model_id: '', outcome: '' } },
  dashboard: { filters: { range: 'all', start: '', end: '' } },
  settings: null,
  generatedKeys: null,
  recovery: null,
  drawerRoute: null,
  candidateView: 'list',
  candidatePicker: { query: '', channelId: '' },
  candidateDraft: [],
};


































function setConnection(connected, trustedLocal = false) {
  state.connected = connected;
  state.trustedLocal = trustedLocal;
  elements.sideStatus.classList.toggle('is-online', connected);
  elements.sideStatus.innerHTML = `<i></i>${connected ? '网关在线' : '连接中断'}`;
  elements.sideMode.textContent = trustedLocal ? '局域网可信模式' : connected ? '密钥已认证' : '等待认证';
  elements.topStatus.classList.toggle('is-online', connected);
  elements.topStatus.classList.toggle('is-offline', !connected);
  elements.topStatus.innerHTML = `<i></i>${connected ? '运行中' : '不可用'}`;
  elements.lockButton.hidden = trustedLocal || !connected;
}

async function checkConnection({ renderAfter = false } = {}) {
  try {
    const system = await get('/system/status');
    state.system = system;
    // 协议注册表来自网关；取不到时保留 FALLBACK_PROTOCOLS 兜底。
    try {
      const registry = await get('/system/protocols');
      if (registry.items?.length) state.protocols = registry.items.map((item) => item.id);
    } catch {
      // 网关过旧或不可达：继续使用兜底协议列表。
    }
    if (elements.topEndpoint) {
      elements.topEndpoint.textContent =
        (system.host || location.hostname) + ':' + (system.port ?? (location.port || '3000'));
    }
    setConnection(true, Boolean(system.trust_local_network));
    if (renderAfter) renderPage();
    return system;
  } catch (error) {
    setConnection(false, false);
    return null;
  }
}

function currentPath() {
  const path = location.pathname.replace(/\/+$/, '') || '/';
  return pageMeta[path] ? path : '/';
}

function updateShell(path) {
  const meta = pageMeta[path] || pageMeta['/'];
  elements.title.textContent = meta.title;
  elements.description.textContent = meta.description;
  document.title = `${meta.title} | Local AI Gateway`;
  document.querySelectorAll('[data-route]').forEach((link) => link.classList.toggle('is-active', link.dataset.route === path));
}

function navigate(path) {
  if (!pageMeta[path]) path = '/';
  if (path !== location.pathname) history.pushState({}, '', path);
  closeSidebar();
  closeDrawer();
  renderPage();
}

function openSidebar() {
  elements.sidebar.classList.add('is-open');
  elements.sidebarScrim.classList.add('is-visible');
}

function closeSidebar() {
  elements.sidebar.classList.remove('is-open');
  elements.sidebarScrim.classList.remove('is-visible');
}


async function renderPage() {
  const path = currentPath();
  state.currentPath = path;
  const version = ++state.renderVersion;
  updateShell(path);
  elements.page.innerHTML = skeleton();
  try {
    if (path === '/') await loadDashboard(version);
    else if (path === '/providers') await loadProviders(version);
    else if (path === '/routes') await loadRoutes(version);
    else if (path === '/profiles') await loadProfiles(version);
    else if (path === '/logs') await loadLogs(version);
    else if (path === '/settings') await loadSettings(version);
  } catch (error) {
    if (version !== state.renderVersion) return;
    elements.page.innerHTML = pageError(error);
  }
}



function cacheHitCard(provider, metrics) {
  const rate = metrics?.cache_hit_rate ?? null;
  const normalizedRate = rate === null ? 0 : Math.max(0, Math.min(1, rate));
  const ringDash = (normalizedRate * 276.46).toFixed(2);
  const requestCount = metrics?.request_count || 0;
  const hasData = metrics?.total_input_tokens > 0;
  return `<article class="cache-hit-card ${hasData ? '' : 'is-empty'}">
    <div class="cache-hit-card-head"><h3>${escapeHtml(provider)}</h3><span>${requestCount ? `${number(requestCount)} 请求` : '暂无请求'}</span></div>
    <div class="cache-hit-card-body">
      <div class="cache-hit-ring"><svg viewBox="0 0 100 100" aria-hidden="true"><circle class="cache-hit-ring-track" cx="50" cy="50" r="44"></circle><circle class="cache-hit-ring-value" cx="50" cy="50" r="44" stroke-dasharray="${ringDash} 276.46"></circle></svg><div><strong>${rate === null ? '-' : percent(rate)}</strong><span>命中率</span></div></div>
      <dl class="cache-hit-details">
        <div><dt>缓存读取</dt><dd>${hasData ? tokenCount(metrics.cache_read_tokens) : '-'}</dd></div>
        <div><dt>缓存写入</dt><dd>${hasData ? tokenCount(metrics.cache_write_tokens) : '-'}</dd></div>
        <div><dt>缓存未命中</dt><dd>${hasData ? tokenCount(metrics.cache_miss_input_tokens) : '-'}</dd></div>
        <div><dt>总输入 Tokens</dt><dd>${hasData ? tokenCount(metrics.total_input_tokens) : '-'}</dd></div>
      </dl>
    </div>
  </article>`;
}

function remoteCompactionBadge(channel) {
  const entry = (channel.remote_compaction || {})['openai_responses'];
  if (!entry) return '<span class="subtle-text">-</span>';
  const status = [];
  if (entry.v1 === 'supported') status.push('V1 支持');
  else if (entry.v1 === 'unsupported') status.push('V1 不支持');
  else status.push('V1 未探测');
  if (entry.v2 === 'supported') status.push('V2 支持');
  else if (entry.v2 === 'unsupported') status.push('V2 不支持');
  else status.push('V2 未探测');
  return `<span class="subtle-text">${escapeHtml(status.join(' / '))}</span>`;
}


// Command Code 的稳定错误码 → 可读文案（探测/额度共用）。
const COMMAND_CODE_ERROR_TEXT = {
  command_code_disabled: 'Command Code 集成未启用：请到「设置 → Command Code」开启并确认风险后再试',
  disabled: 'Command Code 集成未启用：请到「设置 → Command Code」开启并确认风险后再试',
  command_code_bundled_catalog: '实时目录不可用，已使用网关内置的 Go 模型快照',
  'invalid-key': 'API Key 未通过校验，请重新发起网页登录授权',
};

function friendlyError(kind) {
  if (!kind) return '';
  return COMMAND_CODE_ERROR_TEXT[kind] || kind;
}

const BALANCE_ADAPTERS = [
  ['newapi', 'New API'],
  ['sub2api', 'Sub2API'],
  ['opencode_go', 'OpenCode Go'],
  ['deepseek', 'DeepSeek'],
  ['mimo', '小米 MiMo（Token Plan）'],
  ['openrouter', 'OpenRouter'],
  ['siliconflow', 'SiliconFlow'],
  ['stepfun', 'StepFun 阶跃星辰'],
  ['novita', 'Novita AI'],
  ['moonshot', 'Moonshot Kimi 开放平台'],
  ['zhipu', '智谱 GLM Coding Plan'],
  ['minimax', 'MiniMax Coding Plan'],
  ['kimi_code', 'Kimi For Coding'],
  ['command_code', 'Command Code'],
  ['custom', '自定义'],
];

// 「独立令牌」字段的含义各适配器不同（缺省 = New API 文案）。
const DEFAULT_BALANCE_TOKEN_HELP = 'New API（如 CCTQ）：留空时查询该 sk- 令牌额度；填入面板「系统访问令牌 / PAT」后改为查询账户余额。';
const BALANCE_ADAPTER_HELP = {
  mimo: '小米 MiMo：Token Plan 没有 API Key 查询接口，请粘贴浏览器 Cookie（约 1 天过期，过期后重新复制）。',
  openrouter: 'OpenRouter：填普通 API Key 看该 Key 的额度；填 Management Key 看账户余额（credits - usage）。',
  zhipu: '智谱 GLM Coding Plan：填控制台 API Key，展示 5 小时 / 周配额窗口（不是账户余额）。',
  minimax: 'MiniMax Coding Plan：填订阅的 API Key，展示 5 小时 / 周剩余百分比窗口。',
  kimi_code: 'Kimi For Coding：填订阅的 API Key，展示 5 小时 / 周配额窗口。',
  stepfun: 'StepFun：填 API Key，展示账户余额（CNY）；Step Plan 的月度 Credit 无公开查询接口。',
  moonshot: 'Moonshot 开放平台：填 API Key，展示账户余额（CNY）。',
  novita: 'Novita AI：填 API Key，展示账户余额（USD）。',
  siliconflow: 'SiliconFlow：填 API Key，展示账户余额（.cn 为 CNY、.com 为 USD）。',
  command_code: 'Command Code：留空时用渠道 API Key 查询 Go 套餐额度（whoami → credits → usage）。',
};

function balanceAmount(snapshot) {
  const amount = Number(snapshot.remaining);
  if (!Number.isFinite(amount)) return '-';
  const negative = amount < 0;
  const fixed = Math.abs(amount).toFixed(2);
  const prefix = negative ? '-' : '';
  if (snapshot.currency === 'USD') return `${prefix}$${fixed}`;
  if (snapshot.currency) return `${prefix}${snapshot.currency} ${fixed}`;
  return `${prefix}${fixed}`;
}

function balancePercent(value) {
  const amount = Number(value);
  if (!Number.isFinite(amount)) return '-';
  return `${amount.toFixed(Number.isInteger(amount) ? 0 : 1)}%`;
}

function balanceSnapshotText(snapshot) {
  if (!snapshot) return '<span class="subtle-text">尚未查询</span>';
  const when = snapshot.checked_at ? `查询于 ${formatTime(snapshot.checked_at)}` : '';
  if (snapshot.status === 'error') {
    return `<span class="balance-error" title="${escapeAttr(snapshot.error_kind || 'error')}">查询失败：${escapeHtml(friendlyError(snapshot.error_kind) || 'error')}</span><span class="subtle-text">${escapeHtml(when)}</span>`;
  }
  const parts = [];
  // 先显示额度窗口；具体余额数值随后并列显示
  //（Command Code 会同时报告 5h/周 窗口与可用额度）。
  if (snapshot.windows?.length) parts.push(snapshot.windows.map((window) => `${window.label} ${balancePercent(window.remaining_percent)}`).join(' / '));
  if (snapshot.remaining !== null && snapshot.remaining !== undefined) parts.push(balanceAmount(snapshot));
  else if (!snapshot.windows?.length && snapshot.unlimited) parts.push('不限额');
  if (snapshot.used !== null && snapshot.used !== undefined) parts.push(`已用 ${number(snapshot.used)}`);
  if (snapshot.total !== null && snapshot.total !== undefined) parts.push(`总额 ${number(snapshot.total)}`);
  const summary = parts.length ? escapeHtml(parts.join(' · ')) : '-';
  return `<span class="mono">${summary}</span><span class="subtle-text">${escapeHtml(when)}</span>`;
}

function balanceCellContent(channel) {
  const balance = channel.balance || {};
  if (!balance.configured) return '<span class="subtle-text">未启用</span>';
  const snapshot = balance.snapshot;
  if (!snapshot) return '<span class="subtle-text">未查询</span>';
  if (snapshot.status === 'error') {
    return `<span class="balance-error" title="${escapeAttr(friendlyError(snapshot.error_kind) || 'error')}">查询失败</span>`;
  }
  if (snapshot.windows?.length) {
    const rows = snapshot.windows.map((window) => {
      const title = window.resets_at ? `重置：${formatTime(window.resets_at)}` : '';
      return `<span class="balance-window"${title ? ` title="${escapeAttr(title)}"` : ''}><b>${escapeHtml(window.label)}</b><span>${balancePercent(window.remaining_percent)}</span></span>`;
    }).join('');
    const credits = snapshot.remaining !== null && snapshot.remaining !== undefined
      ? `<span class="balance-amount" title="${escapeAttr(snapshot.label || '')}">${escapeHtml(balanceAmount(snapshot))}</span>`
      : '';
    return `<div class="balance-windows">${rows}${credits}</div>`;
  }
  // 只要有具体数值就优先显示：部分上游会设置「不限额」标记，
  // 同时仍返回可用余额（甚至是负数/透支）。
  if (snapshot.remaining !== null && snapshot.remaining !== undefined) {
    const title = snapshot.checked_at ? `查询于 ${formatTime(snapshot.checked_at)}` : '';
    const negative = Number(snapshot.remaining) < 0 ? ' is-negative' : '';
    return `<span class="balance-amount${negative}"${title ? ` title="${escapeAttr(title)}"` : ''}>${escapeHtml(balanceAmount(snapshot))}</span>`;
  }
  if (snapshot.unlimited) return '<span class="balance-unlimited">不限额</span>';
  return '<span class="subtle-text">-</span>';
}

function balanceCell(channel) {
  const configured = Boolean(channel.balance?.configured);
  const refresh = iconButton({
    action: 'refresh-channel-balance',
    iconName: 'refresh-cw',
    label: configured ? '刷新该渠道余额' : '配置余额查询',
    attrs: `data-channel-id="${escapeAttr(channel.id)}"`,
  });
  return `<div class="balance-cell"><div class="balance-cell-value">${balanceCellContent(channel)}</div>${refresh}</div>`;
}

function balancePayloadFromForm(form) {
  const adapter = form?.elements?.balance_adapter?.value || '';
  if (!adapter) return null;
  const headers = {};
  String(form.elements.balance_headers?.value || '')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .forEach((line) => {
      const index = line.indexOf(':');
      if (index > 0) headers[line.slice(0, index).trim()] = line.slice(index + 1).trim();
    });
  const mapping = {};
  [['remaining', 'balance_mapping_remaining'], ['currency', 'balance_mapping_currency'], ['used', 'balance_mapping_used'], ['total', 'balance_mapping_total'], ['label', 'balance_mapping_label']].forEach(([key, name]) => {
    const value = String(form.elements[name]?.value || '').trim();
    if (value) mapping[key] = value;
  });
  return {
    adapter,
    enabled: Boolean(form.elements.balance_enabled?.checked),
    method: form.elements.balance_method?.value || 'GET',
    path: String(form.elements.balance_path?.value || '').trim() || null,
    auth: form.elements.balance_auth?.value || 'bearer',
    headers,
    body: String(form.elements.balance_body?.value || '').trim() || null,
    mapping,
    token: String(form.elements.balance_token?.value || '').trim() || null,
    clear_token: Boolean(form.elements.balance_clear_token?.checked),
  };
}

function channelRows(channels, includeProvider = true) {
  return channels.map((channel) => {
    const health = healthInfo(channel);
    return `<tr>
      ${includeProvider ? `<td>${escapeHtml(channel.provider_name || '-')}</td>` : ''}
      <td>${escapeHtml(channel.name)}</td>
      <td>${protocols(channel.protocols || [channel.protocol])}</td>
      <td>${statusDot(health.label, health.statusClass)}</td>
      <td class="align-right">${number(channel.health?.consecutive_failures || 0)}</td>
    </tr>`;
  }).join('');
}

const DASHBOARD_RANGES = [
  ['all', '全部历史'],
  ['today', '今天'],
  ['yesterday', '昨天'],
  ['last7', '近 7 天'],
  ['last30', '近 30 天'],
  ['custom', '自定义'],
];

function localMidnight(offsetDays = 0) {
  const now = new Date();
  return new Date(now.getFullYear(), now.getMonth(), now.getDate() + offsetDays);
}

function parseDateTimeLocal(value) {
  if (!value) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

function dashboardBounds(filters) {
  if (filters.range === 'today') return [localMidnight(0), localMidnight(1)];
  if (filters.range === 'yesterday') return [localMidnight(-1), localMidnight(0)];
  if (filters.range === 'last7') return [localMidnight(-6), localMidnight(1)];
  if (filters.range === 'last30') return [localMidnight(-29), localMidnight(1)];
  if (filters.range === 'custom') {
    const start = parseDateTimeLocal(filters.start);
    const end = parseDateTimeLocal(filters.end);
    return start && end && start < end ? [start, end] : null;
  }
  return null;
}

function dashboardQuery() {
  const query = new URLSearchParams();
  const bounds = dashboardBounds(state.dashboard.filters);
  if (bounds) {
    query.set('from', bounds[0].toISOString());
    query.set('to', bounds[1].toISOString());
  }
  return query;
}

function formatRangeTime(date) {
  const pad = (item) => String(item).padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function dashboardRangeText() {
  const bounds = dashboardBounds(state.dashboard.filters);
  return bounds ? `${formatRangeTime(bounds[0])} ~ ${formatRangeTime(bounds[1])}（本机时区）` : '';
}

function renderDashboardFilter() {
  const { range, start, end } = state.dashboard.filters;
  const custom = range === 'custom';
  const disabledAttr = custom ? '' : ' disabled';
  return `<form class="filter-bar dashboard-filter" data-form="dashboard-filter">
    <span class="filter-label">统计范围</span>
    <select class="select" name="range" data-range-select aria-label="Token 统计范围">${DASHBOARD_RANGES.map(([value, label]) => `<option value="${value}" ${range === value ? 'selected' : ''}>${label}</option>`).join('')}</select>
    <input class="input" name="start" type="datetime-local" aria-label="开始时间" value="${escapeAttr(start)}"${disabledAttr}>
    <span class="filter-sep">至（不含）</span>
    <input class="input" name="end" type="datetime-local" aria-label="结束时间（不含）" value="${escapeAttr(end)}"${disabledAttr}>
    ${button({ action: 'submit-dashboard-filter', label: '应用', iconName: 'search', primary: true })}
  </form>`;
}

async function submitDashboardFilter(form) {
  const values = new FormData(form);
  const range = String(values.get('range') || 'all');
  const start = String(values.get('start') || '').trim();
  const end = String(values.get('end') || '').trim();
  if (range === 'custom') {
    const startDate = parseDateTimeLocal(start);
    const endDate = parseDateTimeLocal(end);
    if (!startDate || !endDate) {
      toast('请填写自定义范围的开始与结束时间', 'error');
      return;
    }
    if (!(startDate < endDate)) {
      toast('开始时间必须早于结束时间', 'error');
      return;
    }
  }
  state.dashboard.filters = { range, start, end };
  await loadDashboard(state.renderVersion);
}

async function loadDashboard(version) {
  const query = dashboardQuery();
  const summaryPath = query.toString() ? `/stats/summary?${query}` : '/stats/summary';
  const [summary, channelData, system] = await Promise.all([get(summaryPath), get('/channels'), get('/system/status')]);
  if (version !== state.renderVersion || currentPath() !== '/') return;
  state.summary = summary;
  state.channels = channelData.items;
  state.system = system;
  setConnection(true, Boolean(system.trust_local_network));
  elements.page.innerHTML = renderDashboard();
}

function renderDashboard() {
  const summary = state.summary || {};
  const system = state.system || {};
  const channels = state.channels || [];
  const configuredChannels = Number(summary.channels ?? channels.length ?? 0);
  const activeChannels = Number(summary.active_channels ?? 0);
  const cacheByProvider = new Map((summary.cache_by_provider || []).map((item) => [item.provider, item]));
  const hasChannels = configuredChannels > 0;
  const channelState = hasChannels ? `${activeChannels} / ${configuredChannels} 渠道可用` : '尚未配置渠道';
  const systemHealthy = system.database === 'ok';
  const rangeText = dashboardRangeText();
  const scopeLabel = rangeText ? '当前统计范围内' : '独立保存的全部历史';
  return `<div class="page-stack">
    ${toolbar('实时状态', '当前日志保留期内的运行汇总', button({ action: 'refresh-dashboard', label: '刷新数据', iconName: 'refresh-cw' }))}
    <section class="overview-strip" aria-label="网关状态">
      <div class="overview-strip-leading">
        <span class="overview-strip-icon">${icon('activity')}</span>
        <div><span class="overview-strip-label">网关状态</span><strong>${escapeHtml(channelState)}</strong><p>${systemHealthy ? `数据库正常，${number(system.protocols?.length)} 个协议池` : '数据库状态需要检查'}</p></div>
      </div>
      <div class="overview-strip-meta"><span>日志队列 <b>${number(system.telemetry_queue_size)}</b></span><span>丢弃日志 <b class="${system.telemetry_dropped ? 'is-danger' : ''}">${number(system.telemetry_dropped)}</b></span></div>
    </section>
    <section class="metric-grid metric-grid--summary" aria-label="核心指标">
      ${metric('请求总数', number(summary.requests), '本地日志保留期内', 'is-accent', 'activity')}
      ${metric('成功率', percent(summary.success_rate), '最终返回成功的请求', 'is-info', 'check')}
      ${metric('平均首 Token', `${number(summary.average_first_token_ms)}<small>${summary.average_first_token_ms == null ? '' : 'ms'}</small>`, `${scopeLabel} · 仅统计可识别的流式响应`, 'is-warning', 'server')}
      ${metric('平均 TPS', number(summary.average_tps, { maximumFractionDigits: 3 }), `${scopeLabel} · 按完整响应耗时计算`, 'is-violet', 'activity')}
    </section>
    ${panel('统计范围', '筛选 Token、首 Token、TPS 与缓存统计；请求总数与成功率不受影响', renderDashboardFilter(), 'dashboard-filter-panel')}
    <section class="dashboard-token-section" aria-labelledby="token-usage-title">
      <div class="dashboard-section-heading"><div><h2 id="token-usage-title">Token 使用</h2><p>${rangeText ? `当前统计范围：${rangeText}` : '独立保存的全部历史累计用量'}</p></div></div>
      <div class="token-grid">
        ${tokenMetric('缓存命中', tokenCount(summary.cache_read_tokens), 'is-accent', 'database')}
        ${tokenMetric('缓存写入', tokenCount(summary.cache_write_tokens), 'is-info', 'server')}
        ${tokenMetric('缓存未命中', tokenCount(summary.cache_miss_input_tokens), 'is-warning', 'activity')}
        ${tokenMetric('输出', tokenCount(summary.output_tokens), 'is-violet', 'network')}
      </div>
    </section>
    <section class="cache-hit-section" aria-labelledby="cache-hit-title">
      <div class="dashboard-section-heading"><div><h2 id="cache-hit-title">缓存命中率</h2><p>${rangeText ? `按协议类别汇总缓存使用情况 · ${rangeText}` : '按协议类别汇总缓存使用情况'}</p></div></div>
      <div class="cache-hit-grid">
        ${cacheHitCard('OpenAI', cacheByProvider.get('OpenAI'))}
        ${cacheHitCard('Claude', cacheByProvider.get('Claude'))}
        ${cacheHitCard('Gemini', cacheByProvider.get('Gemini'))}
      </div>
    </section>
    ${panel('渠道健康', `${summary.active_channels || 0} / ${summary.channels || 0} 可用`, `<div class="section-body-flush">${channels.length ? `<div class="table-scroll"><table class="data-table"><thead><tr><th>供应商</th><th>渠道</th><th>请求格式</th><th>状态</th><th class="align-right">连续失败</th></tr></thead><tbody>${channelRows(channels)}</tbody></table></div>` : emptyState('尚未配置渠道', '先添加供应商和渠道，再执行模型探测。', 'network')}</div>`) }
  </div>`;
}

async function loadProviders(version) {
  const [providerData, channelData, modelData, presetData] = await Promise.all([get('/providers'), get('/channels'), get('/channel-models'), get('/provider-presets')]);
  // 本加载器只负责绘制供应商页；即使版本计数意外仍然相等，
  // 也绝不能绘制到其它页面。
  if (version !== state.renderVersion || currentPath() !== '/providers') return;
  state.providers = providerData.items;
  state.channels = channelData.items;
  state.channelModels = modelData.items;
  state.providerPresets = presetData.items || [];
  elements.page.innerHTML = renderProvidersMarkup();
}

function renderProvidersMarkup() {
  const providers = state.providers;
  const channels = state.channels;
  const providerRows = providers.map((provider) => `<tr>
    <td>${escapeHtml(provider.name)}</td>
    <td><span class="mono">${escapeHtml(provider.base_url)}</span></td>
    <td class="align-right">${number(provider.channel_count)}</td>
    <td class="action-cell"><div class="table-actions">${iconButton({ action: 'open-channel', iconName: 'plus', label: '添加渠道', attrs: `data-provider-id="${escapeAttr(provider.id)}"` })}${iconButton({ action: 'edit-provider', iconName: 'pencil', label: '编辑供应商', attrs: `data-provider-id="${escapeAttr(provider.id)}"` })}${iconButton({ action: 'delete-provider', iconName: 'trash-2', label: '删除供应商', danger: true, attrs: `data-provider-id="${escapeAttr(provider.id)}"` })}</div></td>
  </tr>`).join('');
  const channelRowsHtml = channels.map((channel) => {
    const health = healthInfo(channel);
    return `<tr>
      <td>${escapeHtml(channel.provider_name || '-')}</td>
      <td>${escapeHtml(channel.name)}</td>
      <td>${protocols(channel.protocols || [channel.protocol])}</td>
      <td>${remoteCompactionBadge(channel)}</td>
      <td><span class="mono">${escapeHtml(channel.api_key_hint || '-')}</span></td>
      <td class="align-right">${number(channel.model_count)}</td>
      <td>${statusDot(health.label, health.statusClass)}</td>
      <td>${balanceCell(channel)}</td>
      <td><input class="switch-control" type="checkbox" aria-label="启用 ${escapeAttr(channel.name)}" data-channel-toggle data-channel-id="${escapeAttr(channel.id)}" ${channel.manual_enabled ? 'checked' : ''}></td>
      <td class="action-cell"><div class="table-actions">
        ${iconButton({ action: 'edit-channel', iconName: 'pencil', label: '编辑渠道', attrs: `data-channel-id="${escapeAttr(channel.id)}"` })}
        ${iconButton({ action: 'show-models', iconName: 'eye', label: '查看模型', attrs: `data-channel-id="${escapeAttr(channel.id)}"` })}
        ${iconButton({ action: 'discover-channel', iconName: 'refresh-cw', label: '探测模型', attrs: `data-channel-id="${escapeAttr(channel.id)}"` })}
        ${iconButton({ action: 'probe-channel', iconName: 'play', label: '健康探测', attrs: `data-channel-id="${escapeAttr(channel.id)}"` })}
        ${iconButton({ action: 'delete-channel', iconName: 'trash-2', label: '删除渠道', danger: true, attrs: `data-channel-id="${escapeAttr(channel.id)}"` })}
      </div></td>
    </tr>`;
  }).join('');
  return `<div class="page-stack">
    ${toolbar('上游资源', '供应商只保存名称与 API 根地址，渠道承载账号和请求格式', `${button({ action: 'refresh-providers', label: '刷新', iconName: 'refresh-cw' })}${button({ action: 'refresh-balances', label: '刷新余额', iconName: 'refresh-cw', disabled: !channels.length })}${button({ action: 'open-provider', label: '添加供应商', iconName: 'plus', primary: true })}${button({ action: 'open-channel', label: '添加渠道', iconName: 'network', disabled: !providers.length })}`)}
    ${panel('供应商', '仅保存名称与 Base URL', `<div class="section-body-flush">${providers.length ? `<div class="table-scroll"><table class="data-table provider-table"><thead><tr><th>名称</th><th>Base URL</th><th class="align-right">渠道</th><th class="action-cell">操作</th></tr></thead><tbody>${providerRows}</tbody></table></div>` : emptyState('尚未创建供应商', '添加供应商后即可配置一个或多个渠道。', 'network')}</div>`) }
    ${panel('账号与渠道', '路由与熔断的最小单位', `<div class="section-body-flush">${channels.length ? `<div class="table-scroll"><table class="data-table channels-table"><thead><tr><th>供应商</th><th>渠道</th><th>请求格式</th><th>远程压缩</th><th>密钥</th><th class="align-right">模型</th><th>健康</th><th>余额</th><th>启用</th><th class="action-cell">操作</th></tr></thead><tbody>${channelRowsHtml}</tbody></table></div>` : emptyState('尚未配置渠道', '请在供应商下添加渠道，并选择上游支持的请求格式。', 'network')}</div>`) }
  </div>`;
}

function providerForm(editing = null) {
  const presets = state.providerPresets || [];
  const presetOptions = presets.map((preset) => `<option value="${escapeAttr(preset.id)}" data-warning="${escapeAttr(preset.warning || '')}" data-base-url="${escapeAttr(preset.base_url)}" data-protocol="${escapeAttr(preset.protocol)}" data-kind="${escapeAttr(preset.kind || '')}">${escapeHtml(preset.name)}</option>`).join('');
  // Command Code 供应商带 `kind='command_code'` 且默认关闭；只有服务端
  // 自己返回 403 upgrade_required 后才会进入反向路径。预设风险提示必须阅读。
  const presetField = editing ? '' : `<div class="field"><label for="provider-preset">供应商预设</label><select class="select" id="provider-preset" name="preset" data-provider-preset><option value="">自定义（不套用预设）</option>${presetOptions}</select><span class="field-help">选择预设会自动填充名称与 API 根地址；Command Code Go 预设会启用 CLI 兼容身份，其渠道凭据需通过「网页登录授权」获取（Go 套餐无法在面板创建 API Key）。</span></div>`;
  const warning = editing ? '' : `<div class="notice is-error" id="provider-preset-warning" hidden></div>
      <label class="checkbox-row" id="provider-preset-confirm-row" hidden><input type="checkbox" id="provider-preset-confirm" data-provider-preset-confirm> <span>我已阅读并知悉上述风险，确认创建该供应商</span></label>`;
  openModal({
    title: editing ? '编辑供应商' : '新建供应商',
    mode: editing ? 'provider-edit' : 'provider-create',
    body: `<form class="form-stack" id="provider-form" data-form="provider" data-provider-id="${escapeAttr(editing?.id || '')}" data-provider-kind="${escapeAttr(editing?.kind || '')}">
      ${presetField}
      <div class="field"><label for="provider-name">名称</label><input class="input" id="provider-name" name="name" required maxlength="120" value="${escapeAttr(editing?.name || '')}" autocomplete="off"></div>
      <div class="field"><label for="provider-base-url">API 根地址</label><input class="input" id="provider-base-url" name="base_url" type="url" required placeholder="https://api.example.com" value="${escapeAttr(editing?.base_url || '')}" autocomplete="url"><span class="field-help">无需填写末尾的 /v1 或 /v1beta。</span></div>
      ${warning}
    </form>`,
    footer: `${button({ action: 'close-modal', label: '取消' })}${button({ action: 'submit-provider', label: editing ? '保存' : '创建', iconName: editing ? 'check' : 'plus', primary: true })}`,
  });
}

function channelBalanceSection(editing, balance) {
  if (!editing) {
    return `<section class="balance-section"><div class="balance-section-head"><h3>余额查询</h3><span class="subtle-text">默认不查询</span></div><p class="field-help">渠道创建后，编辑渠道即可选择余额适配器并启用查询。</p></section>`;
  }
  const configured = Boolean(balance?.configured);
  const adapter = balance?.adapter || '';
  const enabled = Boolean(balance?.enabled);
  const mapping = balance?.mapping || {};
  const headersText = Object.entries(balance?.headers || {}).map(([name, value]) => `${name}: ${value}`).join('\n');
  const adapterOptions = BALANCE_ADAPTERS.map(([value, label]) => `<option value="${value}" ${value === adapter ? 'selected' : ''}>${escapeHtml(label)}</option>`).join('');
  const customHidden = adapter === 'custom' ? '' : ' hidden';
  return `<section class="balance-section" data-balance-section data-balance-configured="${configured ? '1' : '0'}">
      <div class="balance-section-head"><h3>余额查询</h3><span class="subtle-text">默认不查询；启用后每小时自动刷新</span></div>
      <div class="form-grid is-two">
        <div class="field"><label for="balance-adapter">适配器</label><select class="select" id="balance-adapter" name="balance_adapter"><option value="">请选择</option>${adapterOptions}</select></div>
        <div class="field"><label for="balance-enabled">启用查询（含每小时自动刷新）</label><input class="switch-control" id="balance-enabled" type="checkbox" name="balance_enabled" ${enabled ? 'checked' : ''}></div>
      </div>
      <div class="balance-custom" data-balance-custom${customHidden}>
        <div class="form-grid is-two">
          <div class="field"><label for="balance-method">请求方法</label><select class="select" id="balance-method" name="balance_method">${['GET', 'POST', 'PUT'].map((method) => `<option value="${method}" ${method === (balance?.method || 'GET') ? 'selected' : ''}>${method}</option>`).join('')}</select></div>
          <div class="field"><label for="balance-auth">鉴权</label><select class="select" id="balance-auth" name="balance_auth"><option value="bearer" ${balance?.auth !== 'none' ? 'selected' : ''}>Bearer（渠道密钥或独立令牌）</option><option value="none" ${balance?.auth === 'none' ? 'selected' : ''}>无</option></select></div>
        </div>
        <div class="field"><label for="balance-path">路径</label><input class="input" id="balance-path" name="balance_path" placeholder="/v1/usage 或 https://..." value="${escapeAttr(balance?.path || '')}"><span class="field-help">/ 开头为站点根相对；否则相对渠道 Base URL 追加。</span></div>
        <div class="field"><label for="balance-headers">请求头（每行 Name: Value，可用 ${'${api_key}'} / ${'${token}'}）</label><textarea class="textarea" id="balance-headers" name="balance_headers" placeholder="X-Api-Key: ${'${token}'}">${escapeHtml(headersText)}</textarea></div>
        <div class="field"><label for="balance-body">请求体模板（GET 忽略）</label><textarea class="textarea" id="balance-body" name="balance_body" placeholder='{"key":"${'${api_key}'}"}'>${escapeHtml(balance?.body || '')}</textarea></div>
        <div class="form-grid is-two">
          <div class="field"><label for="balance-mapping-remaining">剩余额度路径</label><input class="input" id="balance-mapping-remaining" name="balance_mapping_remaining" placeholder="$.data.remaining" value="${escapeAttr(mapping.remaining || '')}"></div>
          <div class="field"><label for="balance-mapping-currency">币种路径</label><input class="input" id="balance-mapping-currency" name="balance_mapping_currency" placeholder="$.data.unit" value="${escapeAttr(mapping.currency || '')}"></div>
          <div class="field"><label for="balance-mapping-used">已用路径</label><input class="input" id="balance-mapping-used" name="balance_mapping_used" placeholder="$.data.used" value="${escapeAttr(mapping.used || '')}"></div>
          <div class="field"><label for="balance-mapping-total">总额路径</label><input class="input" id="balance-mapping-total" name="balance_mapping_total" placeholder="$.data.total" value="${escapeAttr(mapping.total || '')}"></div>
          <div class="field"><label for="balance-mapping-label">标签路径</label><input class="input" id="balance-mapping-label" name="balance_mapping_label" placeholder="$.data.label" value="${escapeAttr(mapping.label || '')}"></div>
        </div>
      </div>
      <div class="form-grid is-two">
        <div class="field"><label for="balance-token">独立令牌（留空保持不变）</label><input class="input" id="balance-token" type="password" name="balance_token" autocomplete="new-password" placeholder="${escapeAttr(balance?.token_hint || '')}"><span class="field-help" data-balance-token-help>${escapeHtml(BALANCE_ADAPTER_HELP[adapter] || DEFAULT_BALANCE_TOKEN_HELP)}</span></div>
        <div class="field"><label>&nbsp;</label><label class="checkbox-row"><input type="checkbox" name="balance_clear_token"> 清除已保存的独立令牌</label></div>
      </div>
      <div class="balance-snapshot" data-balance-snapshot>${balanceSnapshotText(balance?.snapshot)}</div>
      <div class="balance-actions">
        ${button({ action: 'query-balance', label: '立即查询', iconName: 'refresh-cw', attrs: `data-channel-id="${escapeAttr(editing.id)}"` })}
        ${configured ? button({ action: 'delete-balance-config', label: '删除配置', iconName: 'trash-2', danger: true, attrs: `data-channel-id="${escapeAttr(editing.id)}"` }) : ''}
      </div>
  </section>`;
}


// ---------------------------------------------------------------------------
// Command Code 网页登录授权（等价官方 cmd login 的 loopback 流程）
// ---------------------------------------------------------------------------

const CC_LOGIN_REASON_TEXT = {
  denied: '浏览器授权被拒绝',
  timeout: '等待浏览器授权超时（2 分钟）',
  'invalid-key': '签发的 API Key 未通过校验，请重试',
  network: '无法连接 Command Code API，请检查网络后重试',
  error: '登录流程出错，请重试',
  cancelled: '已取消网页登录',
};

function commandCodeLoginPayload() {
  const providerSelect = document.getElementById('channel-provider');
  const provider = (state.providers || []).find((item) => item.id === providerSelect?.value);
  return { api_base: provider?.base_url || null };
}

function stopCommandCodeLoginPolling() {
  if (state.ccLogin?.pollTimer) {
    clearTimeout(state.ccLogin.pollTimer);
    state.ccLogin.pollTimer = null;
  }
}

function ccLoginStatusHtml() {
  const login = state.ccLogin || { status: 'idle' };
  if (login.status === 'stored') {
    const hint = login.hint ? `（${escapeHtml(login.hint)}）` : '';
    return `<strong>已保存凭据</strong>${hint}：本渠道已有有效密钥，可直接保存/使用；如需更换密钥再点击「网页登录授权」。`;
  }
  if (login.status === 'waiting') {
    return '<span class="subtle-text">等待浏览器授权…请在打开的 commandcode.ai 页面完成登录</span>';
  }
  if (login.status === 'success') {
    const who = [login.userName, login.keyName].filter(Boolean).map(escapeHtml).join(' / ');
    return `<strong>已授权</strong>${who ? `：${who}` : ''} — 保存渠道即可将密钥写入本渠道`;
  }
  if (login.status === 'failed') {
    const reason = CC_LOGIN_REASON_TEXT[login.reason] || login.reason || '未知原因';
    return `<span class="balance-error">授权失败：${escapeHtml(reason)}</span>${login.message ? ` <span class="subtle-text">${escapeHtml(login.message)}</span>` : ''}`;
  }
  return '<span class="subtle-text">尚未开始；Go 套餐无法在面板创建 API Key，请使用网页登录授权或从 CLI 导入</span>';
}

function paintCommandCodeLogin() {
  const box = document.querySelector('[data-cc-login-status]');
  if (box) box.innerHTML = ccLoginStatusHtml();
  const cancel = document.querySelector('[data-action="cc-login-cancel"]');
  if (cancel) cancel.hidden = state.ccLogin?.status !== 'waiting';
  const start = document.querySelector('[data-action="cc-login-start"]');
  // 已授权后禁用重开：再次 begin 会作废尚未保存的一次性交接。
  if (start) start.disabled = ['waiting', 'success'].includes(state.ccLogin?.status);
}

function applyCommandCodeLoginStatus(login) {
  state.ccLogin = state.ccLogin || {};
  if (login.state === 'waiting') {
    state.ccLogin.status = 'waiting';
    state.ccLogin.loginId = null;
    state.ccLogin.authUrl = login.auth_url;
  } else if (login.state === 'success') {
    state.ccLogin.status = 'success';
    state.ccLogin.loginId = login.login_id;
    state.ccLogin.userName = login.user_name;
    state.ccLogin.keyName = login.key_name;
    stopCommandCodeLoginPolling();
  } else if (login.state === 'failed') {
    state.ccLogin.status = 'failed';
    state.ccLogin.loginId = null;
    state.ccLogin.reason = login.reason;
    state.ccLogin.message = login.message || '';
    stopCommandCodeLoginPolling();
  } else {
    state.ccLogin.status = 'idle';
    state.ccLogin.loginId = null;
    stopCommandCodeLoginPolling();
  }
  paintCommandCodeLogin();
}

// 轮询 Command Code 登录授权状态。总时长上限为 1.2s × 75 = 90s，
// 超时后回到 idle 并提示重试，避免按钮被永久锁定在 waiting。
function pollCommandCodeLogin() {
  stopCommandCodeLoginPolling();
  const deadline = Date.now() + 1200 * 75;
  const tick = async () => {
    if (Date.now() >= deadline) {
      stopCommandCodeLoginPolling();
      state.ccLogin = { status: 'idle' };
      toast('登录授权超时，请重试', 'error');
      paintCommandCodeLogin();
      return;
    }
    try {
      const data = await get('/command-code/login');
      applyCommandCodeLoginStatus(data.login || { state: 'idle' });
      if (state.ccLogin?.status === 'waiting') {
        state.ccLogin.pollTimer = setTimeout(tick, 1200);
      }
    } catch (error) {
      state.ccLogin = { status: 'failed', reason: 'error', message: error.message };
      paintCommandCodeLogin();
    }
  };
  state.ccLogin.pollTimer = setTimeout(tick, 800);
}

async function startCommandCodeLogin() {
  try {
    const data = await post('/command-code/login', commandCodeLoginPayload());
    applyCommandCodeLoginStatus(data.login || { state: 'idle' });
    if (data.login?.auth_url) window.open(data.login.auth_url, '_blank', 'noopener');
    if (data.login?.state === 'waiting') pollCommandCodeLogin();
  } catch (error) {
    toast(error.message, 'error');
  }
}

async function importCommandCodeCliKey() {
  try {
    const data = await post('/command-code/import-cli', commandCodeLoginPayload());
    applyCommandCodeLoginStatus(data.login || { state: 'idle' });
    if (data.login?.state === 'success') toast('已从官方 CLI 凭据导入');
  } catch (error) {
    toast(error.message, 'error');
  }
}

async function cancelCommandCodeLogin() {
  try {
    const data = await remove('/command-code/login');
    applyCommandCodeLoginStatus(data.login || { state: 'idle' });
  } catch (error) {
    toast(error.message, 'error');
  }
}

async function channelForm(providerId = '', editing = null) {
  const currentProviderId = editing?.provider_id || providerId || state.providers[0]?.id || '';
  const currentProvider = (state.providers || []).find((provider) => provider.id === currentProviderId);
  const isCommandCode = (editing ? editing.provider_kind : currentProvider?.kind) === 'command_code';
  // 编辑已有渠道且已保存密钥时，不要假装“尚未授权”逼用户重新登录。
  state.ccLogin = editing?.has_api_key
    ? { status: 'stored', hint: editing.api_key_hint || '' }
    : { status: 'idle' };
  const selected = new Set(editing?.protocols || (editing?.protocol ? [editing.protocol] : ['openai_compatible']));
  const options = state.providers.map((provider) => `<option value="${escapeAttr(provider.id)}" ${provider.id === (editing?.provider_id || providerId) ? 'selected' : ''}>${escapeHtml(provider.name)}</option>`).join('');
  const compactionStatus = editing
    ? remoteCompactionBadge(editing)
    : '<span class="subtle-text">保存并探测后自动检测</span>';
  const compactionField = selected.has('openai_responses')
    ? `<div class="field"><label for="channel-compaction">Codex 远程压缩</label><div id="channel-compaction">${compactionStatus}</div><span class="field-help">模型探测时会自动检测渠道的 V1/V2 远程压缩能力；远程压缩只作用于 openai_responses（Codex / Responses API）协议的上游渠道，无需单独配置。</span></div>`
    : '';
  const healthModels = editing
    ? state.channelModels.filter((model) => model.channel_id === editing.id && model.available && (model.protocols || [model.protocol]).includes(editing.protocol))
    : [];
  const healthModelOptions = healthModels.map((model) => `<option value="${escapeAttr(model.model_id)}" ${model.model_id === editing?.health_check_model_id ? 'selected' : ''}>${escapeHtml(model.model_id)}</option>`).join('');
  let balance = null;
  if (editing) {
    try {
      balance = await get(`/channels/${editing.id}/balance`);
    } catch {
      balance = null;
    }
  }
  const balanceSection = channelBalanceSection(editing, balance);
  openModal({
    title: editing ? '编辑渠道' : '新建渠道',
    mode: editing ? 'channel-edit' : 'channel-create',
    body: `<form class="form-stack" id="channel-form" data-form="channel" data-channel-id="${escapeAttr(editing?.id || '')}" data-cc-has-key="${editing?.has_api_key ? '1' : '0'}" data-original-protocols="${escapeAttr(JSON.stringify([...selected]))}" data-balance-configured="${editing && balance?.configured ? '1' : '0'}">
      <div class="field"><label for="channel-provider">供应商</label><select class="select" id="channel-provider" name="provider_id" required ${editing ? 'disabled' : ''}><option value="">请选择供应商</option>${options}</select>${editing ? `<input type="hidden" name="provider_id" value="${escapeAttr(editing.provider_id)}">` : ''}</div>
      <div class="field"><label for="channel-name">渠道名称</label><input class="input" id="channel-name" name="name" required maxlength="120" value="${escapeAttr(editing?.name || '')}" autocomplete="off"></div>
      <fieldset class="protocol-fieldset"><legend class="fieldset-title">支持的请求格式</legend><div class="protocol-options">${protocolOptions().map((protocol) => `<label class="protocol-option"><input type="checkbox" name="protocols" value="${protocol}" ${selected.has(protocol) ? 'checked' : ''}>${escapeHtml(protocol)}</label>`).join('')}</div></fieldset>
      ${compactionField}
      <section class="cc-login-section" data-cc-login-panel ${isCommandCode ? '' : 'hidden'}>
        <div class="cc-login-head"><strong>Command Code 凭据</strong><span class="subtle-text">Go 套餐无法在面板创建 API Key</span></div>
        <p class="field-help">点击「网页登录授权」会在浏览器打开 commandcode.ai 授权页（与官方 <code>cmd login</code> 同一流程）；授权完成后密钥由网关直接写入本渠道，不会经过浏览器页面。也可从已登录的官方 CLI 凭据导入。</p>
        <div class="cc-login-actions" data-cc-login-actions>
          ${button({ action: 'cc-login-start', label: '网页登录授权', iconName: 'key-round', primary: true })}
          ${button({ action: 'cc-login-import', label: '从 CLI 导入', iconName: 'file-text' })}
          ${button({ action: 'cc-login-cancel', label: '取消登录', iconName: 'x' })}
        </div>
        <div class="notice" data-cc-login-status></div>
      </section>
      <div class="field"><label for="channel-api-key">${isCommandCode ? (editing ? 'API Key（可选；推荐用上方网页登录，留空保持不变）' : 'API Key（可选；推荐用上方网页登录）') : editing ? 'API Key（留空则保持不变）' : 'API Key'}</label><input class="input" id="channel-api-key" name="api_key" type="password" ${!isCommandCode && !editing ? 'required' : ''} autocomplete="new-password" placeholder="${escapeAttr(editing?.api_key_hint || '')}"></div>
      <div class="field"><label for="channel-health-model">健康探测模型</label><select class="select" id="channel-health-model" name="health_check_model_id"><option value="" ${editing?.health_check_model_id ? '' : 'selected'}>自动（模型列表第一个）</option>${healthModelOptions}</select><span class="field-help">熔断到期或手动探测时使用；自动模式选择该渠道模型列表中的第一个可用模型。</span></div>
      ${balanceSection}
    </form>`,
    footer: `${button({ action: 'close-modal', label: '取消' })}${button({ action: 'submit-channel', label: editing ? '保存' : '创建', iconName: editing ? 'check' : 'plus', primary: true })}`,
  });
  paintCommandCodeLogin();
}

async function saveProvider(form) {
  const values = new FormData(form);
  const providerId = form.dataset.providerId;
  const presetSelect = form.querySelector('[data-provider-preset]');
  const preset = presetSelect ? (state.providerPresets || []).find((item) => item.id === presetSelect.value) : null;
  const kind = preset?.kind || form.dataset.providerKind || '';
  if (preset?.warning) {
    const confirmed = document.getElementById('provider-preset-confirm')?.checked;
    if (!confirmed) {
      toast('请先阅读并勾选确认风险提示', 'error');
      return;
    }
  }
  const payload = { name: values.get('name').trim(), base_url: values.get('base_url').trim(), kind: kind || null };
  if (providerId) await patch(`/providers/${providerId}`, payload);
  else await post('/providers', payload);
  closeModal();
  toast(providerId ? '供应商已更新' : '供应商已创建');
  renderPage();
}

async function saveChannel(form) {
  // 在任何 await 之前捕获上下文：写入后的重新加载只能在本页仍是
  // 当前页面时绘制。
  const ctx = captureContext();
  const values = new FormData(form);
  const protocolsValue = values.getAll('protocols');
  if (!protocolsValue.length) throw new Error('至少选择一种请求格式');
  const channelId = form.dataset.channelId;
  const commandCodeForm = Boolean(form.querySelector('[data-cc-login-panel]'));
  const commandCodeLoginId = commandCodeForm ? state.ccLogin?.loginId || null : null;
  const manualApiKey = values.get('api_key').trim();
  const hasStoredKey = form.dataset.ccHasKey === '1';
  if (commandCodeForm && !commandCodeLoginId && !manualApiKey && !hasStoredKey) {
    throw new Error('请先完成 Command Code 网页登录授权，或手动粘贴 API Key');
  }
  const payload = {
    name: values.get('name').trim(),
    protocols: protocolsValue,
    health_check_model_id: values.get('health_check_model_id') || null,
  };
  if (channelId) {
    const original = JSON.parse(form.dataset.originalProtocols || '[]');
    const protocolsChanged = original.length !== protocolsValue.length || original.some((value) => !protocolsValue.includes(value));
    // 渠道字段与 API Key 在同一个 PATCH 中提交，二者要么一起成功要么一起
    // 失败，被拒的 Key 不会留下只改了一半的渠道。注意：随后的余额配置是
    // 单独的 PUT/DELETE，不在这一原子范围内。
    if (manualApiKey) payload.api_key = manualApiKey;
    if (commandCodeLoginId) payload.login_id = commandCodeLoginId;
    await patch(`/channels/${channelId}`, payload);
    // 余额配置随渠道表单一起保存。选择「请选择」（适配器为空）会删除
    // 已有配置，回到默认关闭状态。
    if (form.querySelector('[data-balance-section]')) {
      const balancePayload = balancePayloadFromForm(form);
      if (balancePayload) await put(`/channels/${channelId}/balance-config`, balancePayload);
      else if (form.dataset.balanceConfigured === '1') await remove(`/channels/${channelId}/balance-config`);
    }
    closeModal();
    toast('渠道已更新');
    if (isCurrent(ctx)) await refreshProviders(ctx.renderVersion);
    if (protocolsChanged && isCurrent(ctx)) await discoverChannel(channelId, { quiet: true });
  } else {
    await post('/channels', {
      provider_id: values.get('provider_id'),
      ...payload,
      api_key: manualApiKey,
      ...(commandCodeLoginId ? { login_id: commandCodeLoginId } : {}),
    });
    closeModal();
    toast('渠道已创建');
    if (isCurrent(ctx)) await refreshProviders(ctx.renderVersion);
  }
}

function refreshProviders(version = state.renderVersion) {
  return loadProviders(version);
}

async function toggleChannel(channelId, enabled) {
  const version = state.renderVersion;
  const channel = state.channels.find((item) => item.id === channelId);
  try {
    await patch(`/channels/${channelId}`, { manual_enabled: enabled });
    if (channel) channel.manual_enabled = enabled;
    toast(enabled ? '渠道已启用' : '渠道已禁用');
    renderIfCurrent(version, () => {
      elements.page.innerHTML = renderProvidersMarkup();
    });
  } catch (error) {
    toast(error.message, 'error');
    renderIfCurrent(version, () => {
      elements.page.innerHTML = renderProvidersMarkup();
    });
  }
}

async function discoverChannel(channelId, { quiet = false } = {}) {
  // 写入（启动探测）总会完成；轮询只在本页仍是当前页面时继续，
  // 最终的重新加载也不会绘制到其它页面。
  const ctx = captureContext();
  const version = ctx.renderVersion;
  const result = await post(`/channels/${channelId}/discover-models`);
  if (!quiet) toast('模型探测已开始');
  for (let attempt = 0; attempt < 30; attempt += 1) {
    await new Promise((resolve) => window.setTimeout(resolve, 500));
    if (!isCurrent(ctx)) return;
    const run = await get(`/discovery-runs/${result.run_id}`);
    if (run.status === 'succeeded') {
      toast(`探测到 ${run.model_count} 个模型`);
      if (isCurrent(ctx)) await loadProviders(version);
      return;
    }
    if (run.status === 'failed') {
      throw new Error(`模型探测失败：${friendlyError(run.error_kind) || run.status_code}`);
    }
  }
  if (isCurrent(ctx)) toast('探测仍在后台运行', 'warning');
}

async function probeChannel(channelId) {
  await post(`/channels/${channelId}/probe`);
  toast('健康探测已加入队列');
}

async function queryChannelBalance(channelId) {
  const version = state.renderVersion;
  const form = document.getElementById('channel-form');
  const payload = form ? balancePayloadFromForm(form) : null;
  if (!payload) {
    toast('请先选择余额适配器', 'warning');
    return;
  }
  // 「立即查询」会先把表单里的余额配置单独保存（PUT /balance-config），
  // 使查询反映用户刚编辑的余额设置，然后再执行一次性查询。
  await put(`/channels/${channelId}/balance-config`, payload);
  form.dataset.balanceConfigured = '1';
  const result = await post(`/channels/${channelId}/balance`);
  const target = form.querySelector('[data-balance-snapshot]');
  if (target) target.innerHTML = balanceSnapshotText(result);
  if (result.status === 'ok') toast('余额查询成功');
  else toast(`余额查询失败：${friendlyError(result.error_kind) || 'error'}`, 'warning');
  renderIfCurrent(version, renderPage);
}

async function deleteBalanceConfig(channelId) {
  const version = state.renderVersion;
  await remove(`/channels/${channelId}/balance-config`);
  toast('余额配置已删除');
  closeModal();
  renderIfCurrent(version, renderPage);
}

async function refreshBalances() {
  const version = state.renderVersion;
  const result = await post('/balances/refresh');
  if (!result.total) toast('没有启用余额查询的渠道', 'warning');
  else if (result.failed) toast(`余额刷新完成：成功 ${result.ok}，失败 ${result.failed}`, 'warning');
  else toast(`余额刷新完成：成功 ${result.ok} 个渠道`);
  renderIfCurrent(version, renderPage);
}

async function refreshChannelBalance(channelId, button) {
  const version = state.renderVersion;
  const channel = state.channels.find((item) => item.id === channelId);
  if (!channel) return;
  if (!channel.balance?.configured) {
    toast('该渠道尚未配置余额查询，请先选择适配器', 'warning');
    return channelForm('', channel);
  }
  if (button) {
    button.disabled = true;
    button.classList.add('is-loading');
  }
  try {
    // 这里连 `enabled=0` 的渠道也会查询：逐行点击与表单里的
    // 「立即查询」是同等明确的意图。
    const result = await post(`/channels/${channelId}/balance`);
    channel.balance = { ...(channel.balance || {}), configured: true, snapshot: result };
    if (result.status === 'ok') toast(`${channel.name} 余额已刷新`);
    else toast(`${channel.name} 余额刷新失败：${friendlyError(result.error_kind) || 'error'}`, 'warning');
    renderIfCurrent(version, () => {
      elements.page.innerHTML = renderProvidersMarkup();
    });
  } finally {
    // 该行可能已被重绘；只恢复仍在 DOM 中的按钮。
    if (button && document.contains(button)) {
      button.disabled = false;
      button.classList.remove('is-loading');
    }
  }
}

function showModels(channelId) {
  const channel = state.channels.find((item) => item.id === channelId);
  const models = state.channelModels.filter((item) => item.channel_id === channelId);
  const rows = models.map((model) => `<tr><td><span class="mono">${escapeHtml(model.model_id)}</span></td><td>${protocols(model.protocols || [model.protocol])}</td><td>${escapeHtml(model.source || '-')}</td><td>${model.available ? statusDot('可用', 'is-success') : statusDot('不可用', 'is-danger')}</td></tr>`).join('');
  openDrawer({
    title: channel ? `${channel.name} 的模型` : '渠道模型',
    subtitle: channel?.provider_name || '',
    body: models.length ? `<div class="table-scroll"><table class="data-table"><thead><tr><th>模型 ID</th><th>请求格式</th><th>来源</th><th>状态</th></tr></thead><tbody>${rows}</tbody></table></div>` : emptyState('尚未探测模型', '执行模型探测后，模型目录会显示在这里。', 'search'),
  });
}

async function deleteProvider(providerId) {
  const version = state.renderVersion;
  const provider = state.providers.find((item) => item.id === providerId);
  if (!await confirmAction({ title: '删除供应商', message: `删除供应商“${provider?.name || ''}”？其全部渠道及相关路由候选项将一并删除。`, confirmLabel: '删除', danger: true })) return;
  await remove(`/providers/${providerId}`);
  toast('供应商已删除');
  renderIfCurrent(version, renderPage);
}

async function deleteChannel(channelId) {
  const version = state.renderVersion;
  const channel = state.channels.find((item) => item.id === channelId);
  if (!await confirmAction({ title: '删除渠道', message: `删除渠道“${channel?.name || ''}”？该渠道的模型和相关路由候选项将一并删除。`, confirmLabel: '删除', danger: true })) return;
  await remove(`/channels/${channelId}`);
  toast('渠道已删除');
  renderIfCurrent(version, renderPage);
}

// 判断渠道模型是否「在用」：上游仍报可用（available）、所属渠道未被手动
// 禁用，且未处于熔断打开状态。
function isLiveModel(model) {
  return Boolean(model.available) && model.channel_enabled !== false && (model.health_state ?? 'active') === 'active';
}

// model_id → protocols：跨所有「在用」供应方渠道取并集。
function liveModelProtocols() {
  const map = new Map();
  for (const model of state.channelModels) {
    if (!isLiveModel(model)) continue;
    const protocols = map.get(model.model_id) || new Set();
    for (const protocol of model.protocols || []) protocols.add(protocol);
    map.set(model.model_id, protocols);
  }
  return map;
}

// 没有可用供应方的模型不会出现在「新建路由」的可选列表中；已配置的路由
// 不会被删除，仍留在路由表里。
function routeOptions() {
  const routed = new Set(state.routes.map((route) => route.requested_model_id));
  return [...liveModelProtocols().keys()]
    .filter((modelId) => !routed.has(modelId))
    .sort();
}

// 为自定义模型默认勾选的协议（Command Code 没有客户端入口端点，
// 需显式开启）。
const ROUTE_DEFAULT_PROTOCOLS = ['openai_compatible', 'openai_responses', 'claude', 'gemini'];

function syncRouteProtocols() {
  const input = document.getElementById('route-model');
  if (!input) return;
  const protocols = liveModelProtocols().get(input.value.trim());
  if (!protocols) return;
  document.querySelectorAll('[data-route-protocol]').forEach((box) => { box.checked = protocols.has(box.value); });
}

async function loadRoutes(version) {
  const [routeData, modelData, profileData] = await Promise.all([get('/routes'), get('/channel-models'), get('/capability-profiles')]);
  if (version !== state.renderVersion || currentPath() !== '/routes') return;
  state.routes = routeData.items;
  state.channelModels = modelData.items;
  state.profiles = profileData.items || [];
  elements.page.innerHTML = renderRoutesPage();
}

function capabilitySummary(caps = {}) {
  const parts = [];
  if (caps.context_window) parts.push(`上下文 ${number(caps.context_window)}`);
  if (caps.max_tokens) parts.push(`输出 ${number(caps.max_tokens)}`);
  if (caps.supports_image_input === true) parts.push('图像');
  if (caps.supports_image_input === false) parts.push('仅文本');
  if (caps.reasoning === true) parts.push('思考');
  if (caps.reasoning === false) parts.push('无思考');
  const profileChip = caps.profile_name ? `<span class="capability-profile" title="能力档案">档案：${escapeHtml(caps.profile_name)}</span>` : '';
  return `<div class="capability-summary"><span class="capability-source">${caps.source === 'manual' ? '手动' : '自动'}</span>${profileChip}${parts.length ? parts.map((item) => `<span>${escapeHtml(item)}</span>`).join('') : '<span class="subtle-text">未识别</span>'}</div>`;
}

function renderRoutesPage() {
  const options = routeOptions();
  const rows = state.routes.map((route) => `<tr>
    <td><span class="mono">${escapeHtml(route.requested_model_id)}</span></td>
    <td>${protocols(route.protocols)}</td>
    <td>${capabilitySummary(route.capabilities || {})}</td>
    <td><div class="route-chain">${route.candidates.length ? route.candidates.map((candidate) => `<span class="route-candidate ${candidate.health_state === 'open' || candidate.manual_enabled === false ? 'is-open' : ''}" title="${escapeAttr(candidate.model_id || '')}"><b>P${number(candidate.priority)}</b>${escapeHtml(candidate.channel_name)}${candidate.model_id && candidate.model_id !== route.requested_model_id ? `<code class="route-candidate-model">${escapeHtml(candidate.model_id)}</code>` : ''}</span>`).join('') : '<span class="subtle-text">未配置候选渠道</span>'}</div></td>
    <td class="action-cell"><div class="table-actions">${iconButton({ action: 'edit-caps', iconName: 'settings', label: '配置能力', attrs: `data-route-id="${escapeAttr(route.id)}"` })}${iconButton({ action: 'edit-candidates', iconName: 'pencil', label: '编辑候选', attrs: `data-route-id="${escapeAttr(route.id)}"` })}${iconButton({ action: 'delete-route', iconName: 'trash-2', label: '删除路由', danger: true, attrs: `data-route-id="${escapeAttr(route.id)}"` })}</div></td>
  </tr>`).join('');
  return `<div class="page-stack">
    ${toolbar('模型候选优先级', '一个模型路由包含跨格式共享的候选顺序', `${button({ action: 'refresh-routes', label: '刷新', iconName: 'refresh-cw' })}${button({ action: 'open-route', label: '添加路由', iconName: 'plus', primary: true })}`)}
    ${panel('路由表', '优先级 0 最高', `<div class="section-body-flush">${state.routes.length ? `<div class="table-scroll"><table class="data-table routes-table"><thead><tr><th>请求模型</th><th>请求格式</th><th>能力</th><th>候选顺序</th><th class="action-cell">操作</th></tr></thead><tbody>${rows}</tbody></table></div>` : emptyState('尚未创建模型路由', options.length ? '选择一个已探测模型，或填写自定义模型 ID，再为它配置候选渠道。' : '填写一个自定义模型 ID，或先在供应商与渠道页面探测可用模型。', 'route')}</div>`) }
  </div>`;
}

function routeForm() {
  const options = routeOptions();
  const protocolBoxes = Object.keys(PROTOCOL_LABELS)
    .map((protocol) => `<label class="checkbox-row"><input type="checkbox" name="protocols" value="${escapeAttr(protocol)}" data-route-protocol ${ROUTE_DEFAULT_PROTOCOLS.includes(protocol) ? 'checked' : ''}><span>${escapeHtml(protocolLabel(protocol))}</span></label>`)
    .join('');
  openModal({
    title: '新建模型路由',
    mode: 'route',
    body: `<form class="form-stack" id="route-form" data-form="route">
      <div class="field"><label for="route-model">模型 ID</label><input class="input" id="route-model" name="requested_model_id" list="route-model-options" required maxlength="255" placeholder="填写自定义模型 ID，或选择已探测模型" autocomplete="off"><datalist id="route-model-options">${options.map((model) => `<option value="${escapeAttr(model)}"></option>`).join('')}</datalist><span class="field-help">可自由填写网关对外暴露的模型 ID；创建后在候选抽屉中选择「渠道 + 模型」作为请求源，多个 ID 不同的来源可合并到同一个模型。</span></div>
      <div class="field"><label>请求格式</label><div class="checkbox-grid">${protocolBoxes}</div><span class="field-help">网关按候选各自支持的协议自动过滤；输入已探测模型 ID 时会自动勾选它支持的协议。Command Code 渠道可直接服务 Claude / OpenAI Chat / Responses 三种入口，请求格式由网关自动转换。</span></div>
    </form>`,
    footer: `${button({ action: 'close-modal', label: '取消' })}${button({ action: 'submit-route', label: '创建', iconName: 'plus', primary: true })}`,
  });
}

async function saveRoute(form) {
  const ctx = captureContext();
  const values = new FormData(form);
  const requestedModelId = String(values.get('requested_model_id') || '').trim();
  if (!requestedModelId) throw new Error('请填写模型 ID');
  const protocols = values.getAll('protocols');
  if (!protocols.length) throw new Error('请至少选择一个请求格式');
  const created = await post('/routes', { requested_model_id: requestedModelId, protocols });
  closeModal();
  toast('模型路由已创建');
  // 重新加载与抽屉只在本变更发起的页面里执行。
  if (!isCurrent(ctx)) return;
  await loadRoutes(ctx.renderVersion);
  openCandidates(created.id);
}

function capabilityEditorMarkup(route) {
  const caps = route.capabilities || {};
  const cost = caps.cost || {};
  const boolOption = (value, selected, label) => `<option value="${value}" ${selected ? 'selected' : ''}>${label}</option>`;
  const boolSelect = (name, value) => `<select class="select" name="${name}">
    ${boolOption('', value == null, '自动 / 未知')}
    ${boolOption('true', value === true, '支持')}
    ${boolOption('false', value === false, '不支持')}
  </select>`;
  const thinkingLevelMap = caps.thinking_level_map ? JSON.stringify(caps.thinking_level_map, null, 2) : '';
  const profileOptions = state.profiles.map((profile) => `<option value="${escapeAttr(profile.id)}" ${caps.profile_id === profile.id ? 'selected' : ''}>${escapeHtml(profile.name)}${profile.usage_count ? `（${number(profile.usage_count)} 个模型）` : ''}</option>`).join('');
  const body = `<form class="form-stack" id="caps-form" data-form="caps">
    <section class="drawer-section"><h3>能力档案</h3><div class="form-grid is-two"><div class="field"><label for="caps-profile">复用档案</label><select class="select" id="caps-profile" name="profile_id" data-profile-select><option value="">不使用档案</option>${profileOptions}</select><span class="field-help">选择后自动填充下方能力字段；同一档案可被多个模型共用，修改档案会同步到所有引用模型。</span><div class="caps-preview" data-caps-preview></div></div><div class="field"><label>&nbsp;</label>${button({ action: 'save-as-profile', label: '保存为档案', iconName: 'database' })}</div></div></section>
    <section class="drawer-section"><h3>来源</h3><div class="field"><label for="caps-source">能力来源</label><select class="select" id="caps-source" name="source"><option value="auto" ${caps.source !== 'manual' ? 'selected' : ''}>自动从上游聚合</option><option value="manual" ${caps.source === 'manual' ? 'selected' : ''}>手动配置</option></select></div></section>
    <section class="drawer-section"><h3>上下文与输出</h3><div class="form-grid is-two"><div class="field"><label for="caps-context">上下文 Token</label><input class="number-input" id="caps-context" name="context_window" type="number" min="1" step="1" value="${caps.context_window || ''}" placeholder="自动"></div><div class="field"><label for="caps-max-tokens">最大输出 Token</label><input class="number-input" id="caps-max-tokens" name="max_tokens" type="number" min="1" step="1" value="${caps.max_tokens || ''}" placeholder="自动"></div></div></section>
    <section class="drawer-section"><h3>输入与思考</h3><div class="form-grid is-two"><div class="field"><label>图像输入</label>${boolSelect('supports_image_input', caps.supports_image_input)}</div><div class="field"><label>思考能力</label>${boolSelect('reasoning', caps.reasoning)}</div></div><div class="field"><label for="caps-thinking-map">thinkingLevelMap JSON</label><textarea class="textarea" id="caps-thinking-map" name="thinking_level_map" rows="5" placeholder='{"high":"default","max":"max"}'>${escapeHtml(thinkingLevelMap)}</textarea></div></section>
    <section class="drawer-section"><h3>成本（每百万 Token）</h3><div class="form-grid is-two"><div class="field"><label>输入</label><input class="number-input" name="cost_input" type="number" min="0" step="0.000001" value="${cost.input ?? ''}" placeholder="0"></div><div class="field"><label>输出</label><input class="number-input" name="cost_output" type="number" min="0" step="0.000001" value="${cost.output ?? ''}" placeholder="0"></div><div class="field"><label>缓存读取</label><input class="number-input" name="cost_cache_read" type="number" min="0" step="0.000001" value="${cost.cacheRead ?? ''}" placeholder="0"></div><div class="field"><label>缓存写入</label><input class="number-input" name="cost_cache_write" type="number" min="0" step="0.000001" value="${cost.cacheWrite ?? ''}" placeholder="0"></div></div></section>
  </form>`;
  const footer = `${button({ action: 'detect-caps', label: '重新自动探测', iconName: 'refresh-cw' })}${button({ action: 'close-drawer', label: '取消' })}${button({ action: 'save-caps', label: '保存能力', iconName: 'check', primary: true })}`;
  return { body, footer };
}

function openCapabilityEditor(routeId) {
  const route = state.routes.find((item) => item.id === routeId);
  if (!route) return;
  state.drawerRoute = route;
  openDrawer({
    title: route.requested_model_id,
    subtitle: '模型能力会写入 model catalog 的 x_local_gateway.pi_model_config',
    ...capabilityEditorMarkup(route),
  });
  refreshCapsPreview();
}

function costPreviewNumber(value) {
  return Number(value).toLocaleString('zh-CN', { maximumFractionDigits: 6 });
}

function refreshCapsPreview() {
  const container = document.querySelector('[data-caps-preview]');
  const form = document.getElementById('caps-form');
  const select = document.getElementById('caps-profile');
  if (!container || !form || !select) return;
  const profile = profileById(select.value);
  const values = new FormData(form);
  const num = (name) => {
    const raw = values.get(name);
    return raw === null || String(raw).trim() === '' ? null : Number(raw);
  };
  const bool = (name) => {
    const raw = values.get(name);
    return raw === 'true' ? true : raw === 'false' ? false : null;
  };
  const parts = [];
  const ctx = num('context_window');
  const max = num('max_tokens');
  if (ctx) parts.push(`上下文 ${number(ctx)}`);
  if (max) parts.push(`输出 ${number(max)}`);
  const img = bool('supports_image_input');
  if (img === true) parts.push('图像输入');
  if (img === false) parts.push('仅文本');
  const reas = bool('reasoning');
  if (reas === true) parts.push('思考');
  if (reas === false) parts.push('无思考');
  let mapKeys = null;
  let mapInvalid = false;
  try {
    const map = parseThinkingLevelMap(values);
    mapKeys = map ? Object.keys(map) : null;
  } catch {
    mapInvalid = true;
  }
  if (mapKeys && mapKeys.length) parts.push(`思考档 ${mapKeys.join('/')}`);
  if (mapInvalid) parts.push('思考档 JSON 无效');
  const costParts = [];
  for (const [name, label] of [['cost_input', '输入'], ['cost_output', '输出'], ['cost_cache_read', '缓存读'], ['cost_cache_write', '缓存写']]) {
    const value = num(name);
    if (value != null) costParts.push(`${label} $${costPreviewNumber(value)}`);
  }
  const sourceChip = profile ? `<span class="caps-preview-profile">档案：${escapeHtml(profile.name)}</span>` : '';
  const chips = parts.length ? parts.map((item) => `<span>${escapeHtml(item)}</span>`).join('') : '<span class="subtle-text">未配置能力字段</span>';
  const costRow = costParts.length ? `<div class="capability-summary caps-preview-cost">${costParts.map((item) => `<span>${escapeHtml(item)}</span>`).join('')}</div>` : '';
  container.innerHTML = `<div class="caps-preview-head">应用后预览${sourceChip}</div><div class="capability-summary">${chips}</div>${costRow}`;
}

// thinkingLevelMap 表单字段的唯一解析点：空值 → null；非法 JSON → 抛错
// （调用方决定是提示还是中断提交）。
function parseThinkingLevelMap(values) {
  const raw = String(values.get('thinking_level_map') || '').trim();
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    throw new Error('thinkingLevelMap 必须是有效 JSON');
  }
}

function formNumberOrNull(values, name) {
  const value = values.get(name);
  return value === null || String(value).trim() === '' ? null : Number(value);
}

function formBoolOrNull(values, name) {
  const value = values.get(name);
  if (value === 'true') return true;
  if (value === 'false') return false;
  return null;
}

async function saveCapabilities(form) {
  const ctx = captureContext();
  const route = state.drawerRoute;
  if (!route) return;
  const values = new FormData(form);
  const thinkingLevelMap = parseThinkingLevelMap(values);
  const payload = {
    source: values.get('source') || 'manual',
    profile_id: values.get('profile_id') || null,
    context_window: formNumberOrNull(values, 'context_window'),
    max_tokens: formNumberOrNull(values, 'max_tokens'),
    supports_image_input: formBoolOrNull(values, 'supports_image_input'),
    reasoning: formBoolOrNull(values, 'reasoning'),
    thinking_level_map: thinkingLevelMap,
    cost_input: formNumberOrNull(values, 'cost_input'),
    cost_output: formNumberOrNull(values, 'cost_output'),
    cost_cache_read: formNumberOrNull(values, 'cost_cache_read'),
    cost_cache_write: formNumberOrNull(values, 'cost_cache_write'),
  };
  await put(`/model-capabilities/${encodeURIComponent(route.requested_model_id)}`, payload);
  closeDrawer();
  toast('模型能力已保存');
  // 重新加载绝不绘制到用户已经导航离开的页面。
  if (isCurrent(ctx)) await loadRoutes(ctx.renderVersion);
}

async function detectCapabilities() {
  const ctx = captureContext();
  const route = state.drawerRoute;
  if (!route) return;
  const capabilities = await post(`/model-capabilities/detect/${encodeURIComponent(route.requested_model_id)}`);
  route.capabilities = capabilities;
  openDrawer({
    title: route.requested_model_id,
    subtitle: '模型能力会写入 model catalog 的 x_local_gateway.pi_model_config',
    ...capabilityEditorMarkup(route),
  });
  refreshCapsPreview();
  toast('已重新聚合上游能力，结果已显示在下方表单中');
  if (isCurrent(ctx)) await loadRoutes(ctx.renderVersion);
}

// 候选渠道抽屉（模型 → 渠道）分两层：
//   1) 列表层只显示「已添加」的候选，可拖拽/方向键排序、单独启停、移除；
//   2) 选择层由「添加模型」按钮进入，按渠道筛选 + 模型名模糊搜索，默认
//      填入当前模型名，结果按相似度排序。
// 两层改动都只落在 state.candidateDraft 上，点「保存」才 PUT 覆盖候选。

// 一次最多渲染的搜索结果；其余靠细化关键词或渠道筛选收敛。
const CANDIDATE_RESULT_LIMIT = 60;

function routeProtocolSet(route) {
  return new Set(route.protocols || []);
}

// 候选池 = 仍可用的渠道模型（上游可见、渠道启用、未熔断）且至少支持路由的
// 一个请求格式；不可用的模型不会出现在搜索结果里。
function candidatePool(route) {
  const wanted = routeProtocolSet(route);
  return state.channelModels.filter((model) => isLiveModel(model) && (model.protocols || []).some((protocol) => wanted.has(protocol)));
}

// 已添加候选的展示数据；渠道或渠道模型已被删除时退化为路由快照，仍可移除。
function candidateEntry(route, channelModelId) {
  const live = state.channelModels.find((model) => model.id === channelModelId);
  if (live) return { ...live, live: isLiveModel(live) };
  const snapshot = route.candidates.find((candidate) => candidate.channel_model_id === channelModelId);
  if (!snapshot) return null;
  return {
    id: channelModelId,
    channel_id: snapshot.channel_id,
    channel_name: snapshot.channel_name,
    model_id: snapshot.model_id,
    display_name: snapshot.display_name,
    protocols: snapshot.protocols || [],
    live: false,
  };
}

function openCandidates(routeId) {
  const route = state.routes.find((item) => item.id === routeId);
  if (!route) return;
  state.drawerRoute = route;
  state.candidateView = 'list';
  state.candidatePicker = { query: route.requested_model_id || '', channelId: '' };
  state.candidateDraft = [...route.candidates]
    .sort((a, b) => a.priority - b.priority)
    .map((candidate) => ({ channel_model_id: candidate.channel_model_id, enabled: candidate.enabled !== false }));
  renderCandidateDrawer();
}

function renderCandidateDrawer() {
  const route = state.drawerRoute;
  if (!route) return;
  const picker = state.candidateView === 'picker';
  openDrawer({
    title: route.requested_model_id,
    subtitle: picker ? '按渠道筛选或搜索模型名，逐条添加到候选' : '列表越靠上调度优先级越高；可单独启停每个候选',
    body: picker ? candidatePickerMarkup(route) : candidateListMarkup(route),
    footer: picker
      ? button({ action: 'close-candidate-picker', label: '返回候选列表', iconName: 'arrow-left' })
      : `${button({ action: 'close-drawer', label: '取消' })}${button({ action: 'open-candidate-picker', label: '添加模型', iconName: 'plus' })}${button({ action: 'save-candidates', label: '保存', iconName: 'check', primary: true })}`,
  });
  if (!picker) return;
  const input = elements.drawerBody.querySelector('[data-candidate-query]');
  if (input) {
    input.focus();
    input.select();
  }
  refreshCandidateResults();
}

function candidateRowMarkup(route, entry, index) {
  const model = candidateEntry(route, entry.channel_model_id);
  if (!model) return '';
  const label = `${model.channel_name} / ${model.model_id}`;
  return `<div class="candidate-row ${model.live ? '' : 'is-unavailable'}" data-candidate-row data-model-id="${escapeAttr(model.id)}">
      <button class="candidate-drag-handle" type="button" data-candidate-drag-handle data-model-id="${escapeAttr(model.id)}" draggable="true" aria-label="调整 ${escapeAttr(label)} 的顺序" title="拖动调整顺序；可用上下方向键移动">${icon('grip-vertical')}</button>
      <span class="candidate-order" data-candidate-order>${number(index + 1)}</span>
      <div class="candidate-copy"><strong>${escapeHtml(model.channel_name)}</strong><small class="subtle-text"><code>${escapeHtml(model.model_id)}</code>${model.display_name ? ` · ${escapeHtml(model.display_name)}` : ''} · ${escapeHtml((model.protocols || []).join(' / '))}</small>${model.live ? '' : '<small class="candidate-warning">渠道当前不可用，请求时会跳过</small>'}</div>
      <input class="switch-control" type="checkbox" aria-label="启用 ${escapeAttr(label)}" data-candidate-enabled data-model-id="${escapeAttr(model.id)}" ${entry.enabled === false ? '' : 'checked'}>
      ${iconButton({ action: 'remove-candidate-model', iconName: 'trash-2', label: '移除候选', danger: true, attrs: `data-model-id="${escapeAttr(model.id)}"` })}
    </div>`;
}

function candidateListMarkup(route) {
  const rows = state.candidateDraft.map((entry, index) => candidateRowMarkup(route, entry, index)).filter(Boolean).join('');
  return `<section class="drawer-section">
    <div class="candidate-section-head"><h3>已添加候选</h3><span class="subtle-text">${number(state.candidateDraft.length)} 个</span></div>
    <div class="candidate-list" data-candidate-list>${rows || emptyState('尚未添加候选渠道', '点击下方「添加模型」，按渠道筛选或按模型名模糊搜索后加入。', 'route')}</div>
    <p class="field-help">拖动左侧手柄或用上下方向键调整优先级，开关控制单个候选启停，垃圾桶移除；点击「保存」后生效。上游模型 ID 与请求模型不同的候选，转发时会改写请求里的 <code>model</code>。</p>
  </section>`;
}

function candidatePickerMarkup(route) {
  const pool = candidatePool(route);
  const channels = [...new Map(pool.map((model) => [model.channel_id, model.channel_name])).entries()]
    .sort((a, b) => String(a[1]).localeCompare(String(b[1])))
    .map(([id, name]) => `<option value="${escapeAttr(id)}" ${state.candidatePicker.channelId === id ? 'selected' : ''}>${escapeHtml(name)}</option>`)
    .join('');
  return `<section class="drawer-section">
    <p class="field-help">候选池：支持 ${escapeHtml((route.protocols || []).join(' / '))} 中任一格式、且当前可用的渠道模型。</p>
    <div class="candidate-filter-bar">
      <input class="input" type="search" data-candidate-query value="${escapeAttr(state.candidatePicker.query)}" placeholder="搜索模型名，如 deepseek-v4.1-flash" aria-label="搜索模型名" autocomplete="off" spellcheck="false">
      <select class="select" data-candidate-channel aria-label="按渠道筛选"><option value="">全部渠道</option>${channels}</select>
    </div>
    <p class="field-help" data-candidate-summary></p>
    <div class="candidate-result-list" data-candidate-results></div>
  </section>`;
}

function candidateResultMarkup(model, added) {
  return `<div class="candidate-result ${added ? 'is-added' : ''}">
      <div class="candidate-result-copy"><span class="mono">${escapeHtml(model.model_id)}</span><small class="subtle-text">${escapeHtml(model.channel_name)} · ${escapeHtml((model.protocols || []).join(' / '))}</small></div>
      ${button({ action: 'add-candidate-model', label: added ? '已添加' : '添加', iconName: added ? 'check' : 'plus', attrs: `data-model-id="${escapeAttr(model.id)}"` })}
    </div>`;
}

// 输入/切换渠道只重绘结果区，输入框焦点与已添加状态都不丢。
function refreshCandidateResults() {
  const route = state.drawerRoute;
  if (!route) return;
  const list = elements.drawerBody.querySelector('[data-candidate-results]');
  if (!list) return;
  const { query, channelId } = state.candidatePicker;
  const pool = candidatePool(route);
  const scoped = channelId ? pool.filter((model) => model.channel_id === channelId) : pool;
  const ranked = rankModelMatches(query, scoped, { limit: CANDIDATE_RESULT_LIMIT });
  const added = new Set(state.candidateDraft.map((entry) => entry.channel_model_id));
  list.innerHTML = ranked.length
    ? ranked.map(({ item }) => candidateResultMarkup(item, added.has(item.id))).join('')
    : emptyState('没有匹配的模型', '换个关键词，或把渠道筛选改为「全部渠道」。', 'search');
  const summary = elements.drawerBody.querySelector('[data-candidate-summary]');
  if (!summary) return;
  const scope = channelId ? '该渠道' : '全部渠道';
  const truncated = ranked.length >= CANDIDATE_RESULT_LIMIT ? `，仅显示前 ${CANDIDATE_RESULT_LIMIT} 个` : '';
  summary.textContent = `${scope}共 ${number(scoped.length)} 个可选模型，匹配 ${number(ranked.length)} 个${truncated}${query.trim() ? '；按相似度排序' : ''}。`;
}

// 选择层里的「添加」是开关：再点一次即从候选移除；未保存前都可撤销。
function toggleCandidateModel(channelModelId) {
  const index = state.candidateDraft.findIndex((entry) => entry.channel_model_id === channelModelId);
  if (index >= 0) state.candidateDraft.splice(index, 1);
  else state.candidateDraft.push({ channel_model_id: channelModelId, enabled: true });
}

function removeCandidateModel(channelModelId) {
  toggleCandidateModel(channelModelId);
  renderCandidateDrawer();
}

// DOM 顺序即优先级顺序；任何重排都回写草稿，两层视图来回切换不丢顺序。
function updateCandidateOrder() {
  let priority = 1;
  const byId = new Map(state.candidateDraft.map((entry) => [entry.channel_model_id, entry]));
  const ordered = [];
  for (const row of elements.drawerBody.querySelectorAll('[data-candidate-row]')) {
    row.querySelector('[data-candidate-order]').textContent = String(priority);
    priority += 1;
    const entry = byId.get(row.dataset.modelId);
    if (!entry) continue;
    ordered.push(entry);
    byId.delete(row.dataset.modelId);
  }
  ordered.push(...byId.values());
  state.candidateDraft = ordered;
}

function moveCandidateRow(row, direction) {
  const rows = [...elements.drawerBody.querySelectorAll('[data-candidate-row]')];
  const index = rows.indexOf(row);
  const destination = rows[index + direction];
  if (!destination) return;
  if (direction < 0) destination.before(row);
  else destination.after(row);
  updateCandidateOrder();
}

async function saveCandidates() {
  const ctx = captureContext();
  const route = state.drawerRoute;
  if (!route) return;
  const draft = new Map(state.candidateDraft.map((entry) => [entry.channel_model_id, entry]));
  const candidates = [...elements.drawerBody.querySelectorAll('[data-candidate-row]')].map((row, priority) => ({
    channel_model_id: row.dataset.modelId,
    priority,
    enabled: draft.get(row.dataset.modelId)?.enabled !== false,
  }));
  await put(`/routes/${route.id}/candidates`, { candidates });
  closeDrawer();
  toast(candidates.length ? `已保存 ${candidates.length} 个候选渠道` : '候选渠道已清空');
  if (isCurrent(ctx)) await loadRoutes(ctx.renderVersion);
}

async function deleteRoute(routeId) {
  const route = state.routes.find((item) => item.id === routeId);
  if (!await confirmAction({ title: '删除模型路由', message: `删除模型路由“${route?.requested_model_id || ''}”？`, confirmLabel: '删除', danger: true })) return;
  await remove(`/routes/${routeId}`);
  toast('模型路由已删除');
  renderPage();
}

function profileCapabilitySummary(profile) {
  return capabilitySummary({ source: 'manual', ...(profile.capabilities || {}) });
}

async function loadProfiles(version) {
  const data = await get('/capability-profiles');
  if (version !== state.renderVersion || currentPath() !== '/profiles') return;
  state.profiles = data.items || [];
  elements.page.innerHTML = renderProfilesPage();
}

function renderProfilesPage() {
  const rows = state.profiles.map((profile) => `<tr>
    <td><strong>${escapeHtml(profile.name)}</strong>${profile.description ? `<div class="subtle-text">${escapeHtml(profile.description)}</div>` : ''}</td>
    <td>${profileCapabilitySummary(profile)}</td>
    <td>${profile.usage_count ? `<span title="${escapeAttr(profile.used_by.join(', '))}">${number(profile.usage_count)} 个模型</span>` : '<span class="subtle-text">未使用</span>'}</td>
    <td class="action-cell"><div class="table-actions">${iconButton({ action: 'edit-profile', iconName: 'pencil', label: '编辑档案', attrs: `data-profile-id="${escapeAttr(profile.id)}"` })}${iconButton({ action: 'delete-profile', iconName: 'trash-2', label: '删除档案', danger: true, attrs: `data-profile-id="${escapeAttr(profile.id)}"` })}</div></td>
  </tr>`).join('');
  return `<div class="page-stack">
    ${toolbar('可复用能力档案', '多个模型可引用同一档案；修改档案会同步到所有引用模型（成本仍为各模型独立配置）', `${button({ action: 'refresh-profiles', label: '刷新', iconName: 'refresh-cw' })}${button({ action: 'open-profile', label: '新建能力档案', iconName: 'plus', primary: true })}`)}
    ${panel('能力档案', '在「模型路由 → 配置能力」中选择档案即可应用', `<div class="section-body-flush">${state.profiles.length ? `<div class="table-scroll"><table class="data-table"><thead><tr><th>名称</th><th>能力</th><th>引用模型</th><th class="action-cell">操作</th></tr></thead><tbody>${rows}</tbody></table></div>` : emptyState('尚未创建能力档案', '新建一个档案，或在模型能力的配置抽屉中点击「保存为档案」直接创建。', 'database')}</div>`) }
  </div>`;
}

function profileForm(profile = null) {
  const caps = (profile && profile.capabilities) || {};
  const thinkingLevelMap = caps.thinking_level_map ? JSON.stringify(caps.thinking_level_map, null, 2) : '';
  openModal({
    title: profile ? '编辑能力档案' : '新建能力档案',
    mode: profile ? 'profile-edit' : 'profile-new',
    body: `<form class="form-stack" id="profile-form" data-form="profile" ${profile ? `data-profile-id="${escapeAttr(profile.id)}"` : ''}>
      <div class="field"><label for="profile-name">名称</label><input class="text-input" id="profile-name" name="name" type="text" maxlength="255" required value="${escapeAttr(profile?.name || '')}" placeholder="如：GPT-5.6 系列"><span class="field-help">同名的多个模型可共用同一档案。</span></div>
      <div class="field"><label for="profile-description">描述</label><input class="text-input" id="profile-description" name="description" type="text" maxlength="1000" value="${escapeAttr(profile?.description || '')}" placeholder="可选"></div>
      <div class="form-grid is-two"><div class="field"><label for="profile-context">上下文 Token</label><input class="number-input" id="profile-context" name="context_window" type="number" min="1" step="1" value="${caps.context_window || ''}" placeholder="自动"></div><div class="field"><label for="profile-max-tokens">最大输出 Token</label><input class="number-input" id="profile-max-tokens" name="max_tokens" type="number" min="1" step="1" value="${caps.max_tokens || ''}" placeholder="自动"></div></div>
      <div class="form-grid is-two"><div class="field"><label>图像输入</label><select class="select" name="supports_image_input"><option value="" ${caps.supports_image_input == null ? 'selected' : ''}>自动 / 未知</option><option value="true" ${caps.supports_image_input === true ? 'selected' : ''}>支持</option><option value="false" ${caps.supports_image_input === false ? 'selected' : ''}>不支持</option></select></div><div class="field"><label>思考能力</label><select class="select" name="reasoning"><option value="" ${caps.reasoning == null ? 'selected' : ''}>自动 / 未知</option><option value="true" ${caps.reasoning === true ? 'selected' : ''}>支持</option><option value="false" ${caps.reasoning === false ? 'selected' : ''}>不支持</option></select></div></div>
      <div class="field"><label for="profile-thinking-map">thinkingLevelMap JSON</label><textarea class="textarea" id="profile-thinking-map" name="thinking_level_map" rows="5" placeholder='{"high":"default","max":"max"}'>${escapeHtml(thinkingLevelMap)}</textarea></div>
    </form>`,
    footer: `${button({ action: 'close-modal', label: '取消' })}${button({ action: 'submit-profile', label: '保存档案', iconName: 'check', primary: true })}`,
  });
}

async function saveProfile(form) {
  const ctx = captureContext();
  const values = new FormData(form);
  const name = String(values.get('name') || '').trim();
  if (!name) throw new Error('请填写档案名称');
  const thinkingLevelMap = parseThinkingLevelMap(values);
  const payload = {
    name,
    description: String(values.get('description') || '').trim() || null,
    context_window: formNumberOrNull(values, 'context_window'),
    max_tokens: formNumberOrNull(values, 'max_tokens'),
    supports_image_input: formBoolOrNull(values, 'supports_image_input'),
    reasoning: formBoolOrNull(values, 'reasoning'),
    thinking_level_map: thinkingLevelMap,
  };
  if (getModalMode() === 'profile-edit') {
    const profile = state.profiles.find((item) => item.id === form.dataset.profileId);
    if (!profile) throw new Error('档案不存在');
    await put(`/capability-profiles/${profile.id}`, payload);
    toast('能力档案已更新，引用它的模型已同步');
  } else {
    await post('/capability-profiles', payload);
    toast('能力档案已创建');
  }
  closeModal();
  if (isCurrent(ctx)) await loadProfiles(ctx.renderVersion);
}

async function deleteProfile(profileId) {
  const ctx = captureContext();
  const profile = state.profiles.find((item) => item.id === profileId);
  if (!await confirmAction({ title: '删除能力档案', message: `删除档案“${profile?.name || ''}”？引用它的模型将解除绑定（已应用的能力值保留）。`, confirmLabel: '删除', danger: true })) return;
  await remove(`/capability-profiles/${profileId}`);
  toast('能力档案已删除');
  if (isCurrent(ctx)) await loadProfiles(ctx.renderVersion);
}

function profileSelectOptions(selectedId) {
  return state.profiles.map((profile) => `<option value="${escapeAttr(profile.id)}" ${profile.id === selectedId ? 'selected' : ''}>${escapeHtml(profile.name)}${profile.usage_count ? `（${number(profile.usage_count)} 个模型）` : ''}</option>`).join('');
}

function syncProfileSelect(selectedId) {
  const select = document.getElementById('caps-profile');
  if (!select) return;
  select.innerHTML = `<option value="">不使用档案</option>${profileSelectOptions(selectedId)}`;
  select.value = selectedId || '';
}

function profileById(profileId) {
  return state.profiles.find((profile) => profile.id === profileId) || null;
}

function applyProfileToForm(profileId) {
  const profile = profileById(profileId);
  if (!profile) return;
  const caps = profile.capabilities || {};
  const form = document.getElementById('caps-form');
  if (!form) return;
  const setField = (name, value) => {
    const input = form.querySelector(`[name="${name}"]`);
    if (input) input.value = value ?? '';
  };
  setField('context_window', caps.context_window);
  setField('max_tokens', caps.max_tokens);
  setField('supports_image_input', caps.supports_image_input === undefined ? '' : String(caps.supports_image_input));
  setField('reasoning', caps.reasoning === undefined ? '' : String(caps.reasoning));
  const mapInput = form.querySelector('[name="thinking_level_map"]');
  if (mapInput) mapInput.value = caps.thinking_level_map ? JSON.stringify(caps.thinking_level_map, null, 2) : '';
  refreshCapsPreview();
  toast(`已应用档案「${profile.name}」的能力，可继续修改后保存`);
}

async function saveCurrentAsProfile() {
  const form = document.getElementById('caps-form');
  if (!form) return;
  openModal({
    title: '保存为能力档案',
    mode: 'save-as-profile',
    body: `<form class="form-stack" id="save-as-profile-form" data-form="save-as-profile">
      <div class="field"><label for="save-as-profile-name">档案名称</label><input class="text-input" id="save-as-profile-name" name="name" type="text" maxlength="255" required placeholder="如：GPT-5.6 系列"><span class="field-help">档案保存当前表单中的能力字段（不含成本），创建后其他模型可直接复用。</span></div>
    </form>`,
    footer: `${button({ action: 'close-modal', label: '取消' })}${button({ action: 'submit-save-as-profile', label: '创建并复用', iconName: 'check', primary: true })}`,
  });
}

async function submitSaveAsProfile(form) {
  const name = String(new FormData(form).get('name') || '').trim();
  if (!name) throw new Error('请填写档案名称');
  const capsForm = document.getElementById('caps-form');
  if (!capsForm) throw new Error('能力表单已关闭');
  const values = new FormData(capsForm);
  const thinkingLevelMap = parseThinkingLevelMap(values);
  const created = await post('/capability-profiles', {
    name,
    description: null,
    context_window: formNumberOrNull(values, 'context_window'),
    max_tokens: formNumberOrNull(values, 'max_tokens'),
    supports_image_input: formBoolOrNull(values, 'supports_image_input'),
    reasoning: formBoolOrNull(values, 'reasoning'),
    thinking_level_map: thinkingLevelMap,
  });
  closeModal();
  const data = await get('/capability-profiles');
  state.profiles = data.items || [];
  syncProfileSelect(created.id);
  refreshCapsPreview();
  toast(`能力档案「${created.name}」已创建，当前模型已关联`);
}

const PROTOCOL_LABELS = {
  openai_compatible: 'OpenAI Chat',
  openai_responses: 'OpenAI Responses',
  claude: 'Claude 原生',
  gemini: 'Gemini',
  command_code: 'Command Code（CLI 兼容，无客户端入口）',
};

function protocolLabel(protocol) {
  return PROTOCOL_LABELS[protocol] || protocol;
}

async function loadLogs(version) {
  const { page, filters } = state.logs;
  const query = new URLSearchParams({ page: String(page), page_size: '50' });
  Object.entries(filters).forEach(([key, value]) => { if (value) query.set(key, value); });
  const data = await get(`/requests?${query}`);
  if (version !== state.renderVersion || currentPath() !== '/logs') return;
  state.logs = { ...state.logs, items: data.items, total: data.total, page: data.page };
  elements.page.innerHTML = renderLogsPage();
}

function renderLogsPage() {
  const { items, total, page, filters } = state.logs;
  const maxPage = Math.max(1, Math.ceil(total / 50));
  const rows = items.map((item) => {
    const outcome = outcomeInfo(item.outcome);
    return `<tr class="log-row" data-log-request-id="${escapeAttr(item.id)}" tabindex="0" aria-label="查看 ${escapeAttr(item.model_id || '请求')} 的详情">
      <td class="logs-cell-time">${escapeHtml(formatLogTime(item.started_at))}</td>
      <td><span class="mono logs-model" title="${escapeAttr(item.model_id || '')}">${escapeHtml(item.model_id)}</span></td>
      <td class="logs-cell-upstream">${item.upstream_model_id ? `<span class="mono">${escapeHtml(item.upstream_model_id)}</span>` : '<span class="subtle-text">-</span>'}</td>
      <td class="logs-cell-route">${responseChannelTags(item.response_channels || [])}</td>
      <td class="logs-cell-protocol">${protocols([item.protocol])}</td>
      <td>${statusDot(outcome.label, outcome.statusClass)}</td>
      <td class="align-right">${number(item.final_status_code)}</td>
      <td class="align-right">${number(item.attempt_count)}</td>
      <td class="align-right">${escapeHtml(duration(item.total_duration_ms))}</td>
    </tr>`;
  }).join('');
  return `<div class="page-stack">
    ${toolbar('请求与渠道尝试', '查看每一次请求的结果、响应时间与上游尝试', button({ action: 'refresh-logs', label: '刷新', iconName: 'refresh-cw' }))}
    ${panel('请求记录', '', `<form class="filter-bar" data-form="logs-filter"><span class="filter-label">筛选条件</span>
      <select class="select" name="protocol" aria-label="入口协议"><option value="">全部入口协议</option>${protocolOptions().map((protocol) => `<option value="${protocol}" ${filters.protocol === protocol ? 'selected' : ''}>${protocol}</option>`).join('')}</select>
      <select class="select" name="upstream_protocol" aria-label="上游协议"><option value="">全部上游协议</option>${protocolOptions().map((protocol) => `<option value="${protocol}" ${filters.upstream_protocol === protocol ? 'selected' : ''}>${protocol}</option>`).join('')}</select>
      <input class="input" name="model_id" value="${escapeAttr(filters.model_id)}" placeholder="请求模型" aria-label="请求模型" autocomplete="off">
      <input class="input" name="upstream_model_id" value="${escapeAttr(filters.upstream_model_id)}" placeholder="上游模型" aria-label="上游模型" autocomplete="off">
      <select class="select is-outcome" name="outcome" aria-label="请求结果"><option value="">全部结果</option>${['success', 'upstream_error', 'gateway_error', 'stream_interrupted', 'cancelled'].map((outcome) => `<option value="${outcome}" ${filters.outcome === outcome ? 'selected' : ''}>${outcomeInfo(outcome).label}</option>`).join('')}</select>
      ${button({ action: 'submit-log-filter', label: '查询', iconName: 'search', primary: true })}
    </form><div class="section-body-flush">${items.length ? `<div class="table-scroll"><table class="data-table logs-table" aria-label="请求记录"><thead><tr><th>时间</th><th>请求模型</th><th>上游模型</th><th>响应渠道</th><th>入口协议</th><th>结果</th><th class="align-right">HTTP</th><th class="align-right">尝试</th><th class="align-right">耗时</th></tr></thead><tbody>${rows}</tbody></table></div>` : emptyState('暂无请求日志', '发送模型请求后，这里会显示请求与渠道尝试。', 'file-text')}</div><div class="pagination"><span>第 ${page} / ${maxPage} 页，共 ${number(total)} 条</span>${button({ action: 'logs-prev', label: '上一页', iconName: 'chevron-left', disabled: page <= 1 })}${button({ action: 'logs-next', label: '下一页', iconName: 'chevron-right', disabled: page >= maxPage })}</div>`) }
  </div>`;
}

async function viewRequest(requestId) {
  const detail = await get(`/requests/${requestId}`);
  const outcome = outcomeInfo(detail.outcome);
  const attempts = detail.attempts || [];
  const upstream = attempts.find((attempt) => attempt.upstream_protocol || attempt.upstream_model_id) || {};
  const cards = attempts.map((attempt) => {
    const attemptOutcome = outcomeInfo(attempt.outcome);
    const upstreamMeta = attempt.upstream_model_id
      ? `<span>上游 ${escapeHtml(attempt.upstream_model_id)}${attempt.upstream_protocol ? ` · ${escapeHtml(protocolLabel(attempt.upstream_protocol))}` : ''}</span>`
      : '';
    return `<article class="attempt-card"><div class="attempt-head"><div><strong>${escapeHtml(attempt.channel_name || `尝试 ${attempt.attempt_no}`)}</strong><div class="attempt-meta"><span>尝试 ${number(attempt.attempt_no)}</span><span>HTTP ${number(attempt.status_code)}</span><span>首 Token ${duration(attempt.first_token_ms)}</span><span>TPS ${number(attempt.tps, { maximumFractionDigits: 3 })}</span>${upstreamMeta}</div></div>${statusDot(attemptOutcome.label, attemptOutcome.statusClass)}</div><div class="attempt-metrics"><div class="attempt-metric"><span>缓存命中</span><strong>${tokenCount(attempt.cache_read_tokens)}</strong></div><div class="attempt-metric"><span>缓存写入</span><strong>${tokenCount(attempt.cache_write_tokens)}</strong></div><div class="attempt-metric"><span>缓存未命中</span><strong>${tokenCount(attempt.cache_miss_input_tokens)}</strong></div><div class="attempt-metric"><span>输出</span><strong>${tokenCount(attempt.output_tokens)}</strong></div></div></article>`;
  }).join('');
  openDrawer({
    title: '请求详情',
    subtitle: detail.model_id || '',
    body: `<div class="detail-grid"><span>请求 ID</span><strong class="mono">${escapeHtml(detail.id)}</strong><span>模型</span><strong class="mono">${escapeHtml(detail.model_id)}</strong><span>上游模型</span><strong class="mono">${escapeHtml(upstream.upstream_model_id || '-')}</strong><span>上游协议</span><strong>${escapeHtml(upstream.upstream_protocol ? protocolLabel(upstream.upstream_protocol) : '-')}</strong><span>结果</span><strong>${statusDot(outcome.label, outcome.statusClass)}</strong><span>总耗时</span><strong>${escapeHtml(duration(detail.total_duration_ms))}</strong></div><section class="drawer-section"><h3>渠道尝试</h3>${cards || emptyState('无上游尝试', '本次请求没有记录到上游渠道尝试。', 'activity')}</section>`,
  });
}

function openRequestLog(requestId) {
  viewRequest(requestId).catch((error) => toast(error.message || '加载详情失败', 'error'));
}

async function loadSettings(version) {
  let settings;
  try {
    settings = await get('/settings');
    try {
      state.commandCodeStatus = await get('/command-code/status');
    } catch {
      state.commandCodeStatus = null;
    }
  } catch (error) {
    if (version !== state.renderVersion || currentPath() !== '/settings') return;
    if (error.body?.code === 'config_corrupted') {
      // 管理面因持久化配置损坏而 fail-closed。区分两种来源：
      // 访问密钥损坏 → 密钥恢复页；运行时设置损坏 → 修复表单。
      try {
        const status = await get('/settings/access-keys/status');
        if (version !== state.renderVersion || currentPath() !== '/settings') return;
        state.recovery = status;
        if (status.admin_key_status === 'corrupt' || status.gateway_key_status === 'corrupt') {
          elements.page.innerHTML = renderRecoveryPage(status);
          return;
        }
        state.settingsRepair = true;
        state.settingsCorruptKeys = status.config_corrupted_keys || [];
        elements.page.innerHTML = renderSettingsRepairPage();
        return;
      } catch {
        elements.page.innerHTML = pageError(error);
        return;
      }
    }
    throw error;
  }
  if (version !== state.renderVersion || currentPath() !== '/settings') return;
  state.settings = settings;
  elements.page.innerHTML = renderSettingsPage();
}

function renderRecoveryPage(status) {
  const keys = state.generatedKeys;
  const corruptCount = [status.admin_key_status, status.gateway_key_status].filter((value) => value === 'corrupt').length;
  const generated = keys ? `<div class="generated-keys"><div class="generated-key"><span>管理密钥</span><code>${escapeHtml(keys.admin_access_key)}</code>${iconButton({ action: 'copy-key', iconName: 'copy', label: '复制管理密钥', attrs: 'data-key-name="admin_access_key"' })}</div><div class="generated-key"><span>代理密钥</span><code>${escapeHtml(keys.gateway_access_key)}</code>${iconButton({ action: 'copy-key', iconName: 'copy', label: '复制代理密钥', attrs: 'data-key-name="gateway_access_key"' })}</div></div>` : '';
  return `<div class="settings-stack">
    ${toolbar('访问密钥已损坏', '管理端访问密钥无法解密，已进入恢复模式', `${button({ action: 'refresh-settings', label: '重试', iconName: 'refresh-cw' })}${button({ action: 'generate-keys', label: '随机生成两组密钥', iconName: 'key-round', primary: true })}`)}
    ${panel('密钥恢复', '仅本机可执行，恢复后管理界面立即解锁', `<div class="section-body"><div class="notice is-error">检测到 ${corruptCount} 组访问密钥损坏，无法解密。点击"随机生成两组密钥"将原子地重新生成两组密钥，并立即恢复管理端访问。</div>${generated}</div></div>`, 'settings-panel')}
  </div>`;
}

// 运行时设置损坏时的修复表单：经恢复模式端点（loopback + nonce）原子重写
// 损坏行，不接收访问密钥。初始值取默认值并明确提示。
function renderSettingsRepairPage() {
  const settings = { ...SETTINGS_DEFAULTS };
  const corruptKeys = (state.settingsCorruptKeys || []).map(escapeHtml).join('、');
  return `<form class="settings-stack" id="settings-form" data-form="settings">
    ${toolbar('设置数据已损坏', '仅本机可修复', `${button({ action: 'submit-settings', label: '保存修复', iconName: 'check', primary: true })}`)}
    ${panel('修复提示', '损坏的设置项无法安全读取', `<div class="section-body"><div class="notice is-error">检测到运行时设置数据损坏${corruptKeys ? `（${corruptKeys}）` : ''}，已锁定管理面与代理鉴权。请逐项确认下方设置（已预填默认值）并保存，将原子地覆盖损坏数据并立即恢复访问。</div></div>`, 'settings-panel')}
    ${panel('局域网访问', '默认信任局域网请求', `<div class="section-body"><div class="form-stack"><div class="switch-row"><div class="switch-copy"><strong>信任局域网访问</strong><p id="trust-description">${settings.trust_local_network ? '代理和管理员界面不要求密钥' : '代理和管理员界面要求对应密钥'}</p></div><input class="switch-control" id="trust-local-network" name="trust_local_network" type="checkbox" aria-label="信任局域网访问" data-trust-toggle ${settings.trust_local_network ? 'checked' : ''}></div></div></div>`, 'settings-panel')}
    ${settingsSectionsHtml(settings)}
  </form>`;
}


// 设置表单 schema：一份定义同时驱动渲染、提交载荷与前端范围校验，
// 新增字段不可能只接入一半。
// 范围值须与后端 `settings.rs::setting_ranges` 同步（非同一来源）。
const SETTINGS_SECTIONS = [
  { id: 'fault', title: '故障转移', note: '优先级路由与自动熔断', grid: '',
    fields: [['failure_threshold', '连续失败阈值', 1, 20], ['circuit_open_seconds', '熔断时间（秒）', 30, 86400], ['max_failover_attempts', '最大渠道尝试', 1, 20]] },
  { id: 'timeout', title: '超时', note: '上游连接与响应期限', grid: '',
    fields: [['connect_timeout_seconds', '连接超时（秒）', 1, 120], ['first_byte_timeout_seconds', '首字节超时（秒）', 1, 600], ['first_token_timeout_seconds', '首 Token 超时（秒）', 1, 600], ['stream_idle_timeout_seconds', '流式空闲超时（秒）', 10, 3600], ['non_stream_total_timeout_seconds', '非流式总超时（秒）', 10, 3600]] },
  { id: 'limits', title: '请求限制', note: '代理请求体与上游响应缓冲上限', grid: '', help: '协议转换等必须整体缓冲的上游响应硬上限；超过返回 502。流式转发不受此限制。',
    fields: [['max_request_body_mb', '最大请求体（MiB）', 1, 1024], ['max_buffered_upstream_body_mb', '上游响应缓冲上限（MiB）', 1, 1024]] },
  { id: 'maintenance', title: '维护', note: '周期任务与数据保留', grid: 'is-two',
    fields: [['model_discovery_interval_hours', '模型探测周期（小时）', 1, 168], ['log_retention_days', '日志保留（天）', 1, 365]] },
  { id: 'command_code', title: 'Command Code 运行参数', note: '仅在上方开关启用后生效', grid: 'is-two',
    help: '指纹/生命周期按渠道（每 API Key）独立节流；额度窗口 5h 重置较快，因此额度刷新周期默认 15 分钟。',
    fields: [['command_code_idle_timeout_seconds', '流空闲超时（秒）', 10, 3600], ['command_code_init_interval_hours', '指纹刷新周期（小时）', 1, 168], ['command_code_version_check_interval_hours', '版本探测周期（小时）', 1, 720], ['command_code_quota_interval_minutes', '额度刷新周期（分钟）', 1, 1440], ['command_code_max_concurrency', '单账号并发上限', 1, 32]] },
];

// 设置损坏修复表单的初始值，与后端 RuntimeSettings::default() 一致。
// 损坏行无法安全读取，因此修复表单从这些默认值开始。
const SETTINGS_DEFAULTS = {
  trust_local_network: true,
  failure_threshold: 3,
  circuit_open_seconds: 900,
  max_failover_attempts: 3,
  connect_timeout_seconds: 10,
  first_byte_timeout_seconds: 60,
  first_token_timeout_seconds: 60,
  stream_idle_timeout_seconds: 300,
  non_stream_total_timeout_seconds: 600,
  max_request_body_mb: 256,
  max_buffered_upstream_body_mb: 64,
  model_discovery_interval_hours: 24,
  log_retention_days: 30,
  command_code_enabled: false,
  command_code_idle_timeout_seconds: 120,
  command_code_init_interval_hours: 8,
  command_code_version_check_interval_hours: 24,
  command_code_quota_interval_minutes: 15,
  command_code_max_concurrency: 2,
};

function settingsSectionsHtml(settings) {
  return SETTINGS_SECTIONS.map((section) => {
    const fields = section.fields.map(([name, label, min, max]) =>
      settingNumberField(name, label, settings[name], min, max)).join('');
    const help = section.help ? `<span class="field-help">${escapeHtml(section.help)}</span>` : '';
    return panel(section.title, section.note,
      `<div class="section-body"><div class="form-grid ${section.grid}">${fields}${help}</div></div>`,
      'settings-panel');
  }).join('');
}

function commandCodePanel(settings) {
  const enabled = Boolean(settings.command_code_enabled);
  const status = state.commandCodeStatus || {};
  const drift = Boolean(status.drift);
  const driftNotice = drift
    ? `<div class="notice is-error">上游 CLI 版本 ${escapeHtml(status.cli_version || '?')} 已偏离协议基准 ${escapeHtml(status.verified_baseline || '?')}，线协议可能已静默变更；遇到异常请先更新网关或停用该集成。</div>`
    : (status.cli_version ? `<div class="notice">CLI 版本 ${escapeHtml(status.cli_version)}（协议基准 ${escapeHtml(status.verified_baseline || '?')}）${status.version_checked_at ? ` · 探测于 ${escapeHtml(status.version_checked_at)}` : ''}</div>` : '');
  const warning = 'Go 套餐没有官方 API 访问：网关默认先请求官方 Provider API，服务端以 403 upgrade_required 拒绝后，仅对该账号降级到 CLI 兼容路径（/alpha/generate），并使用 CLI 身份头（会话、指纹、版本）。这是对服务端明确拒绝路径的绕过，可能违反服务条款并导致账号封禁；启用即表示已知晓并自行承担全部风险。';
  return panel('Command Code（Go 套餐）', '默认关闭；关闭时零上游请求', `<div class="section-body"><div class="form-stack">
    <div class="switch-row"><div class="switch-copy"><strong>启用 Command Code 集成</strong><p>${enabled ? '已启用：Command Code 渠道的请求可路由（先官方 API，403 后降级）' : '已关闭：Command Code 渠道的请求会被拒绝，探测/发现/额度也都不会发出'}</p></div><input class="switch-control" id="command-code-enabled" name="command_code_enabled" type="checkbox" aria-label="启用 Command Code 集成" ${enabled ? 'checked' : ''}></div>
    <div class="notice is-error" data-command-code-warning ${enabled ? '' : 'hidden'}>${warning}</div>
    <label class="checkbox-row" data-command-code-confirm-row ${enabled ? 'hidden' : ''}><input type="checkbox" data-command-code-confirm> <span>我已阅读并知悉上述风险，确认启用</span></label>
    ${driftNotice}
    ${Number(status.channel_count) > 0 ? `<div class="notice">当前已配置 ${number(status.channel_count)} 个 Command Code 渠道；每个渠道（API Key）拥有独立指纹与会话。</div>` : ''}
  </div></div>`, 'settings-panel');
}

function renderSettingsPage() {
  const settings = state.settings || {};
  const keys = state.generatedKeys;
  const keyBlock = `<div class="access-keys" id="access-keys" ${settings.trust_local_network && settings.admin_key_status !== 'corrupt' && settings.gateway_key_status !== 'corrupt' ? 'hidden' : ''}>
      <div class="notice">关闭信任后，保存设置会立即启用密钥校验。留空表示保留现有密钥。</div>
      ${settings.admin_key_status === 'corrupt' || settings.gateway_key_status === 'corrupt' ? '<div class="notice is-error">访问密钥数据损坏，请点击"随机生成两组密钥"恢复访问控制。</div>' : ''}
      <div class="form-grid is-two">
        <div class="field"><label for="admin-access-key">管理密钥</label><input class="input" id="admin-access-key" name="admin_access_key" type="password" autocomplete="new-password" placeholder="${escapeAttr(settings.admin_key_hint || '手动输入或使用随机生成')}"></div>
        <div class="field"><label for="gateway-access-key">代理密钥</label><input class="input" id="gateway-access-key" name="gateway_access_key" type="password" autocomplete="new-password" placeholder="${escapeAttr(settings.gateway_key_hint || '手动输入或使用随机生成')}"></div>
      </div>
      ${button({ action: 'generate-keys', label: '随机生成两组密钥', iconName: 'key-round' })}
      ${keys ? `<div class="generated-keys"><div class="generated-key"><span>管理密钥</span><code>${escapeHtml(keys.admin_access_key)}</code>${iconButton({ action: 'copy-key', iconName: 'copy', label: '复制管理密钥', attrs: 'data-key-name="admin_access_key"' })}</div><div class="generated-key"><span>代理密钥</span><code>${escapeHtml(keys.gateway_access_key)}</code>${iconButton({ action: 'copy-key', iconName: 'copy', label: '复制代理密钥', attrs: 'data-key-name="gateway_access_key"' })}</div></div>` : ''}
    </div>`;
  return `<form class="settings-stack" id="settings-form" data-form="settings">
    ${toolbar('访问与运行参数', '配置局域网信任、故障转移与运行时限制', `${button({ action: 'refresh-settings', label: '还原', iconName: 'refresh-cw' })}${button({ action: 'submit-settings', label: '保存设置', iconName: 'check', primary: true })}`)}
    ${panel('局域网访问', '默认信任局域网请求', `<div class="section-body"><div class="form-stack"><div class="switch-row"><div class="switch-copy"><strong>信任局域网访问</strong><p id="trust-description">${settings.trust_local_network ? '代理和管理员界面不要求密钥' : '代理和管理员界面要求对应密钥'}</p></div><input class="switch-control" id="trust-local-network" name="trust_local_network" type="checkbox" aria-label="信任局域网访问" data-trust-toggle ${settings.trust_local_network ? 'checked' : ''}></div>${settings.config_corrupted_keys?.length ? `<div class="notice is-error">以下设置项数据损坏（已显示默认值），重新填写并保存即可修复：${settings.config_corrupted_keys.map(escapeHtml).join('、')}</div>` : ''}${keyBlock}</div></div>`, 'settings-panel')}
    ${commandCodePanel(settings)}
    ${settingsSectionsHtml(settings)}
  </form>`;
}

async function saveSettings(form) {
  const version = state.renderVersion;
  const values = new FormData(form);
  const commandCodeEnabled = form.querySelector('#command-code-enabled')?.checked || false;
  const commandCodeWasEnabled = Boolean(state.settings?.command_code_enabled);
  if (commandCodeEnabled && !commandCodeWasEnabled) {
    const confirmed = form.querySelector('[data-command-code-confirm]')?.checked;
    if (!confirmed) {
      toast('启用 Command Code 前必须确认风险提示', 'error');
      return;
    }
  }
  const payload = {
    trust_local_network: values.get('trust_local_network') === 'on',
    command_code_enabled: commandCodeEnabled,
  };
  // 提交载荷由渲染这些字段的同一份 schema 生成，且每个值在 PATCH 前
  // 都做前端范围校验（后端的 422 仍作为第二道关卡）。
  for (const [name, , min, max] of SETTINGS_SECTIONS.flatMap((section) => section.fields)) {
    const raw = values.get(name);
    const number = Number(raw);
    if (!Number.isInteger(number) || number < min || number > max) {
      toast(`设置 ${name} 必须在 ${min}–${max} 之间`, 'error');
      return;
    }
    payload[name] = number;
  }
  const adminKey = values.get('admin_access_key')?.trim();
  const gatewayKey = values.get('gateway_access_key')?.trim();
  if (adminKey) payload.admin_access_key = adminKey;
  if (gatewayKey) payload.gateway_access_key = gatewayKey;
  if (state.settingsRepair) {
    // 设置损坏修复：经 loopback + 一次性 nonce 门控，不接收访问密钥。
    try {
      await patch('/settings/recovery-repair', payload, {
        headers: { 'x-recovery-nonce': state.recovery?.recovery_nonce },
      });
    } catch (error) {
      if (error.status === 401) {
        // nonce 已失效(一次性/过期):重新进入恢复流程换取新 nonce。
        state.settingsRepair = false;
        state.settingsCorruptKeys = null;
        await loadSettings(version);
        toast('修复凭据已过期，请重新确认后保存', 'error');
        return;
      }
      throw error;
    }
    state.settingsRepair = false;
    state.settingsCorruptKeys = null;
    toast('设置已修复');
    await checkConnection();
    renderIfCurrent(version, renderPage);
    return;
  }
  await patch('/settings', payload);
  if (!payload.trust_local_network) {
    const effectiveAdminKey = adminKey || state.generatedKeys?.admin_access_key;
    if (effectiveAdminKey) setToken(effectiveAdminKey);
  }
  state.generatedKeys = null;
  toast('运行设置已保存');
  await checkConnection();
  renderIfCurrent(version, renderPage);
}

async function generateKeys() {
  const version = state.renderVersion;
  // 恢复模式下，生成端点要求携带随恢复状态下发的一次性挑战值；该值
  // 单次有效，因此失败意味着需重新进入恢复流程以获取新的值。
  const nonce = state.recovery?.recovery_nonce;
  const options = nonce ? { headers: { 'x-recovery-nonce': nonce } } : {};
  let result;
  try {
    result = await api('/settings/access-keys/generate', { method: 'POST', body: '{}', ...options });
  } catch (error) {
    if (state.recovery) {
      state.recovery = null;
      await loadSettings(version);
      toast(error.message || '生成失败，请重试', 'error');
      return;
    }
    throw error;
  }
  state.generatedKeys = result;
  if (state.recovery) {
    // 恢复模式：密钥已重新有效——采用新生成的管理密钥并重新加载
    // 完整的设置页面。
    state.recovery = null;
    setToken(result.admin_access_key);
    await loadSettings(version);
    toast('密钥已生成，请立即复制并保存', 'warning');
    return;
  }
  if (version !== state.renderVersion) return;
  state.settings.trust_local_network = false;
  renderIfCurrent(version, () => {
    elements.page.innerHTML = renderSettingsPage();
  });
  toast('密钥已生成，请立即复制并保存', 'warning');
}

async function copyKey(name) {
  const value = state.generatedKeys?.[name];
  if (!value) return;
  try {
    await navigator.clipboard.writeText(value);
    toast('密钥已复制');
  } catch {
    toast('无法访问剪贴板，请手动复制', 'warning');
  }
}

function renderIfCurrent(version, render) {
  if (version === state.renderVersion) render();
}

// 变更在发起时捕获页面上下文，导航会使其失效。数据写入仍会完成，但变更
// 之后触发的重新加载/渲染只在同一页面仍为当前页面时执行——过期的动作
// 绝不会把它的标记绘制到另一个页面上。
function captureContext() {
  return { path: currentPath(), renderVersion: state.renderVersion };
}
function isCurrent(context) {
  return context.renderVersion === state.renderVersion && context.path === currentPath();
}

function openAuthModal() {
  if (elements.modal.open && getModalMode() === 'auth') return;
  openModal({
    title: '连接本地网关',
    mode: 'auth',
    body: `<form class="form-stack" id="auth-form" data-form="auth"><div class="field"><label for="admin-token">管理密钥</label><input class="input" id="admin-token" name="token" type="password" autocomplete="current-password" required autofocus></div></form>`,
    footer: button({ action: 'submit-auth', label: '连接', iconName: 'lock', primary: true }),
  });
}

async function saveAuth(form) {
  const token = new FormData(form).get('token').trim();
  setToken(token);
  try {
    const status = await get('/system/status');
    setConnection(true, Boolean(status.trust_local_network));
    closeModal();
    toast('已连接本地网关');
    renderPage();
  } catch {
    setToken('');
    toast('管理密钥无效', 'error');
  }
}

async function submitLogFilter(form) {
  const values = new FormData(form);
  state.logs.filters = { protocol: values.get('protocol') || '', upstream_protocol: values.get('upstream_protocol') || '', model_id: values.get('model_id')?.trim() || '', upstream_model_id: values.get('upstream_model_id')?.trim() || '', outcome: values.get('outcome') || '' };
  state.logs.page = 1;
  await loadLogs(state.renderVersion);
}

async function handleAction(target) {
  const action = target.dataset.action;
  try {
    if (action === 'open-sidebar') return openSidebar();
    if (action === 'close-sidebar') return closeSidebar();
    if (action === 'close-modal') return closeModal();
    if (action === 'close-drawer') return closeDrawer();
    if (action === 'confirm-cancel') return resolveConfirmation(false);
    if (action === 'confirm-accept') return resolveConfirmation(true);
    if (action === 'lock-console') { setToken(''); setConnection(false, false); return openAuthModal(); }
    if (action === 'refresh-dashboard') return renderPage();
    if (action === 'refresh-providers') return renderPage();
    if (action === 'cc-login-start') return startCommandCodeLogin();
    if (action === 'cc-login-import') return importCommandCodeCliKey();
    if (action === 'cc-login-cancel') return cancelCommandCodeLogin();
    if (action === 'open-provider') return providerForm();
    if (action === 'edit-provider') return providerForm(state.providers.find((item) => item.id === target.dataset.providerId));
    if (action === 'open-channel') return channelForm(target.dataset.providerId || '');
    if (action === 'edit-channel') return channelForm('', state.channels.find((item) => item.id === target.dataset.channelId));
    if (action === 'show-models') return showModels(target.dataset.channelId);
    if (action === 'discover-channel') return discoverChannel(target.dataset.channelId);
    if (action === 'probe-channel') return probeChannel(target.dataset.channelId);
    if (action === 'query-balance') return queryChannelBalance(target.dataset.channelId);
    if (action === 'delete-balance-config') return deleteBalanceConfig(target.dataset.channelId);
    if (action === 'refresh-balances') return refreshBalances();
    if (action === 'refresh-channel-balance') return refreshChannelBalance(target.dataset.channelId, target);
    if (action === 'delete-provider') return deleteProvider(target.dataset.providerId);
    if (action === 'delete-channel') return deleteChannel(target.dataset.channelId);
    if (action === 'refresh-routes') return renderPage();
    if (action === 'open-route') return routeForm();
    if (action === 'edit-caps') return openCapabilityEditor(target.dataset.routeId);
    if (action === 'edit-candidates') return openCandidates(target.dataset.routeId);
    if (action === 'open-candidate-picker') {
      state.candidateView = 'picker';
      if (!state.candidatePicker.query) state.candidatePicker.query = state.drawerRoute?.requested_model_id || '';
      return renderCandidateDrawer();
    }
    if (action === 'close-candidate-picker') { state.candidateView = 'list'; return renderCandidateDrawer(); }
    if (action === 'add-candidate-model') {
      toggleCandidateModel(target.dataset.modelId);
      return refreshCandidateResults();
    }
    if (action === 'remove-candidate-model') return removeCandidateModel(target.dataset.modelId);
    if (action === 'save-candidates') return saveCandidates();
    if (action === 'save-caps') return document.getElementById('caps-form')?.requestSubmit();
    if (action === 'detect-caps') return detectCapabilities();
    if (action === 'delete-route') return deleteRoute(target.dataset.routeId);
    if (action === 'refresh-profiles') return renderPage();
    if (action === 'open-profile') return profileForm();
    if (action === 'edit-profile') return profileForm(state.profiles.find((item) => item.id === target.dataset.profileId));
    if (action === 'delete-profile') return deleteProfile(target.dataset.profileId);
    if (action === 'save-as-profile') return saveCurrentAsProfile();
    if (action === 'refresh-logs') return loadLogs(state.renderVersion);
    if (action === 'logs-prev') { state.logs.page -= 1; return loadLogs(state.renderVersion); }
    if (action === 'logs-next') { state.logs.page += 1; return loadLogs(state.renderVersion); }
    if (action === 'view-request') return viewRequest(target.dataset.requestId);
    if (action === 'refresh-settings') { state.generatedKeys = null; return renderPage(); }
    if (action === 'generate-keys') return generateKeys();
    if (action === 'copy-key') return copyKey(target.dataset.keyName);
    if (action === 'submit-provider') return document.getElementById('provider-form')?.requestSubmit();
    if (action === 'submit-channel') return document.getElementById('channel-form')?.requestSubmit();
    if (action === 'submit-route') return document.getElementById('route-form')?.requestSubmit();
    if (action === 'submit-profile') return document.getElementById('profile-form')?.requestSubmit();
    if (action === 'submit-save-as-profile') return document.getElementById('save-as-profile-form')?.requestSubmit();
    if (action === 'submit-auth') return document.getElementById('auth-form')?.requestSubmit();
    if (action === 'submit-settings') return document.getElementById('settings-form')?.requestSubmit();
    if (action === 'submit-dashboard-filter') return document.querySelector('[data-form="dashboard-filter"]')?.requestSubmit();
    if (action === 'submit-log-filter') return document.querySelector('[data-form="logs-filter"]')?.requestSubmit();
  } catch (error) {
    toast(error.message || '操作失败', 'error');
  }
}

async function handleSubmit(event) {
  const form = event.target.closest('form[data-form]');
  if (!form) return;
  event.preventDefault();
  try {
    if (form.dataset.form === 'provider') await saveProvider(form);
    else if (form.dataset.form === 'channel') await saveChannel(form);
    else if (form.dataset.form === 'route') await saveRoute(form);
    else if (form.dataset.form === 'profile') await saveProfile(form);
    else if (form.dataset.form === 'save-as-profile') await submitSaveAsProfile(form);
    else if (form.dataset.form === 'caps') await saveCapabilities(form);
    else if (form.dataset.form === 'auth') await saveAuth(form);
    else if (form.dataset.form === 'settings') await saveSettings(form);
    else if (form.dataset.form === 'dashboard-filter') await submitDashboardFilter(form);
    else if (form.dataset.form === 'logs-filter') await submitLogFilter(form);
  } catch (error) {
    toast(error.message || '保存失败', 'error');
  }
}

function handleChange(event) {
  const target = event.target;
  if (target.matches('[data-range-select]')) {
    const form = target.closest('[data-form="dashboard-filter"]');
    if (!form) return;
    const custom = target.value === 'custom';
    form.querySelectorAll('[name="start"], [name="end"]').forEach((input) => { input.disabled = !custom; });
  }
  if (target.matches('[data-channel-toggle]')) toggleChannel(target.dataset.channelId, target.checked);
  if (target.matches('[data-candidate-enabled]')) {
    const entry = state.candidateDraft.find((item) => item.channel_model_id === target.dataset.modelId);
    if (entry) entry.enabled = target.checked;
  }
  if (target.matches('[data-candidate-channel]')) {
    state.candidatePicker.channelId = target.value;
    refreshCandidateResults();
  }
  if (target.matches('[data-provider-preset]')) {
    const option = target.selectedOptions[0];
    if (option) {
      const nameInput = document.getElementById('provider-name');
      const urlInput = document.getElementById('provider-base-url');
      const baseUrl = option.dataset.baseUrl || '';
      if (baseUrl && urlInput) urlInput.value = baseUrl;
      if (baseUrl && nameInput) nameInput.value = option.textContent.trim();
    }
    const preset = (state.providerPresets || []).find((item) => item.id === target.value);
    const warningBox = document.getElementById('provider-preset-warning');
    const confirmRow = document.getElementById('provider-preset-confirm-row');
    const confirmInput = document.getElementById('provider-preset-confirm');
    if (warningBox) {
      warningBox.hidden = !preset?.warning;
      warningBox.textContent = preset?.warning || '';
    }
    if (confirmRow) confirmRow.hidden = !preset?.warning;
    if (confirmInput) confirmInput.checked = false;
  }
  if (target.matches('#channel-provider')) {
    const provider = (state.providers || []).find((item) => item.id === target.value);
    const isCommandCode = provider?.kind === 'command_code';
    const panel = document.querySelector('[data-cc-login-panel]');
    if (panel) panel.hidden = !isCommandCode;
    const keyInput = document.getElementById('channel-api-key');
    if (keyInput) keyInput.required = !isCommandCode && !target.closest('form')?.dataset?.channelId;
    stopCommandCodeLoginPolling();
    state.ccLogin = { status: 'idle' };
    paintCommandCodeLogin();
  }
  if (target.matches('#command-code-enabled')) {
    const warning = document.querySelector('[data-command-code-warning]');
    const confirmRow = document.querySelector('[data-command-code-confirm-row]');
    const confirmInput = document.querySelector('[data-command-code-confirm]');
    if (warning) warning.hidden = !target.checked;
    if (confirmRow) confirmRow.hidden = !target.checked;
    if (confirmInput) confirmInput.checked = false;
  }
  if (target.matches('[data-trust-toggle]')) {
    const keyArea = document.getElementById('access-keys');
    const description = document.getElementById('trust-description');
    if (keyArea) keyArea.hidden = target.checked;
    if (description) description.textContent = target.checked ? '代理和管理员界面不要求密钥' : '代理和管理员界面要求对应密钥';
  }
  if (target.matches('[data-profile-select]')) {
    if (target.value) applyProfileToForm(target.value);
    return;
  }
  if (target.closest('[data-form="caps"]')) refreshCapsPreview();
  if (target.matches('[name="balance_adapter"]')) {
    const section = target.closest('[data-balance-section]');
    const custom = section?.querySelector('[data-balance-custom]');
    if (custom) custom.hidden = target.value !== 'custom';
    const help = section?.querySelector('[data-balance-token-help]');
    if (help) help.textContent = BALANCE_ADAPTER_HELP[target.value] || DEFAULT_BALANCE_TOKEN_HELP;
  }
}

setUnauthorizedHandler(() => { if (!state.recovery) openAuthModal(); });

document.addEventListener('click', (event) => {
  const routeLink = event.target.closest('[data-route]');
  if (routeLink) {
    if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    event.preventDefault();
    navigate(routeLink.dataset.route);
    return;
  }
  const logRow = event.target.closest('[data-log-request-id]');
  if (logRow) return openRequestLog(logRow.dataset.logRequestId);
  const target = event.target.closest('[data-action]');
  if (target && !target.disabled) handleAction(target);
});
document.addEventListener('keydown', (event) => {
  const dragHandle = event.target.closest('[data-candidate-drag-handle]');
  if (dragHandle && ['ArrowUp', 'ArrowDown'].includes(event.key)) {
    event.preventDefault();
    moveCandidateRow(dragHandle.closest('[data-candidate-row]'), event.key === 'ArrowUp' ? -1 : 1);
    return;
  }
  const logRow = event.target.closest('[data-log-request-id]');
  if (!logRow || !['Enter', ' '].includes(event.key)) return;
  event.preventDefault();
  openRequestLog(logRow.dataset.logRequestId);
});
document.addEventListener('submit', handleSubmit);
document.addEventListener('change', handleChange);
elements.modal.addEventListener('close', () => {
  stopCommandCodeLoginPolling();
  state.ccLogin = { status: 'idle' };
});
document.addEventListener('input', (event) => {
  if (event.target.closest('[data-form="caps"]')) refreshCapsPreview();
  if (event.target.matches('#route-model')) syncRouteProtocols();
  if (event.target.matches('[data-candidate-query]')) {
    state.candidatePicker.query = event.target.value;
    refreshCandidateResults();
  }
});
document.addEventListener('dragstart', (event) => {
  const dragHandle = event.target.closest('[data-candidate-drag-handle]');
  if (!dragHandle || dragHandle.disabled) return;
  const row = dragHandle.closest('[data-candidate-row]');
  row.classList.add('is-dragging');
  event.dataTransfer.effectAllowed = 'move';
  event.dataTransfer.setData('text/plain', row.dataset.modelId);
});
document.addEventListener('dragover', (event) => {
  const list = event.target.closest('[data-candidate-list]');
  const target = event.target.closest('[data-candidate-row]');
  const dragging = elements.drawerBody.querySelector('.candidate-row.is-dragging');
  if (!list || !target || !dragging || target === dragging) return;
  event.preventDefault();
  const insertAfter = event.clientY > target.getBoundingClientRect().top + target.offsetHeight / 2;
  if (insertAfter) target.after(dragging);
  else target.before(dragging);
  updateCandidateOrder();
});
document.addEventListener('dragend', () => {
  const dragging = elements.drawerBody.querySelector('.candidate-row.is-dragging');
  if (!dragging) return;
  dragging.classList.remove('is-dragging');
  updateCandidateOrder();
});
window.addEventListener('popstate', renderPage);
elements.modal.addEventListener('cancel', () => {
  resolveConfirmation(false);
});
elements.modal.addEventListener('close', () => { closeModal(); });
window.addEventListener('drawer-closed', () => {
  state.drawerRoute = null;
  state.candidateView = 'list';
  state.candidatePicker = { query: '', channelId: '' };
  state.candidateDraft = [];
});

checkConnection();
renderPage();

