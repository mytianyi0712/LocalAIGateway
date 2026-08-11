// P2-4：UI 渲染助手与 API 请求层已拆分为独立 ES module。
import {
  elements, icon, escapeHtml, escapeAttr, number, tokenCount, percent,
  parseStandardTime, formatTime, formatLogTime, duration, protocols,
  responseChannelTags, healthInfo, outcomeInfo, statusDot, button, iconButton,
  toolbar, panel, emptyState, skeleton, toast, openModal, closeModal,
  openDrawer, closeDrawer, confirmAction, resolveConfirmation, metric,
  tokenMetric, settingNumberField, pageError,
} from './ui.js';
import { getToken, setToken, api, get, post, put, patch, remove, setUnauthorizedHandler } from './api.js';

// P2-3: the protocol list is data-driven from /system/protocols (populated
// into state.protocols at boot); this constant is only the offline fallback
// so the UI still renders before/without the gateway.
const FALLBACK_PROTOCOLS = ['openai_compatible', 'openai_responses', 'claude', 'gemini'];
function protocolOptions() {
  return (state.protocols && state.protocols.length ? state.protocols : FALLBACK_PROTOCOLS);
}

const MAPPING_KINDS = {
  claude: {
    route: '/mappings',
    title: 'Claude 模型映射助手',
    description: '以 Claude Code 标准模型名对外提供服务，按模型独立配置上游协议与转换',
    api: '/claude-mappings',
    presetsApi: '/claude-presets',
    modelIdField: 'claude_model_id',
    modelLabel: 'Claude 标准模型名',
    tableHead: 'Claude 模型名',
    placeholder: 'claude-opus-5',
    displayPlaceholder: 'Claude Opus 5',
    toolbarTitle: 'Claude 标准模型名',
    emptyDetail: '添加一个 Claude Code 标准模型名，选择系统中已配置的模型与上游协议即可。',
    presetLabel: '从 Claude 预设快速选择',
    presetHelp: '预设来自 Anthropic 当前模型目录（内置默认 + 经已配置的 Claude 渠道实时刷新），选择后自动填充标准模型名与显示名称。',
    mappingHelp: '客户端（如 Claude Code）请求 /claudecode/v1/messages 时使用的模型名称。',
    protocolHelp: '请求会被转换为该协议后转发；选择 Claude 原生则不转换，仅替换上游模型名。每个映射可独立设置。',
    entry: {
      base: '/claudecode',
      title: 'Claude Code 接入地址（独立于常规请求）',
      body: '映射仅在该路径下生效，常规 <code>/v1/*</code> 端点不受影响。在 Claude Code 中设置 <code>ANTHROPIC_BASE_URL={base}</code>，即可从这里探测模型目录并请求映射模型。',
    },
  },
  codex: {
    route: '/codex-mappings',
    title: 'Codex 模型映射助手',
    description: '以 Codex 标准模型名对外提供服务，按模型独立配置上游协议与转换',
    api: '/codex-mappings',
    presetsApi: '/codex-presets',
    modelIdField: 'codex_model_id',
    modelLabel: 'Codex 标准模型名',
    tableHead: 'Codex 模型名',
    placeholder: 'gpt-5-codex',
    displayPlaceholder: 'GPT-5 Codex',
    toolbarTitle: 'Codex 标准模型名',
    emptyDetail: '添加一个 Codex 标准模型名，选择系统中已配置的模型与上游协议即可。',
    presetLabel: '从 Codex 预设快速选择',
    presetHelp: '预设来自 OpenAI 当前模型目录（内置默认 + 经已配置的 OpenAI 渠道实时刷新），选择后自动填充标准模型名与显示名称。',
    mappingHelp: '客户端（如 Codex CLI）请求 /codex/v1/responses 时使用的模型名称。',
    protocolHelp: '请求会被转换为该协议后转发；选择 OpenAI Responses 原生则不转换，仅替换上游模型名。每个映射可独立设置。',
    entry: {
      base: '/codex/v1',
      title: 'Codex 接入地址（独立于常规请求）',
      body: '映射仅在该路径下生效，常规 <code>/v1/*</code> 端点不受影响。Codex CLI 会向 base URL 追加 <code>/responses</code> 与 <code>/models</code>，因此 base 需包含 <code>/v1</code>。在 <code>~/.codex/config.toml</code> 中设置 <code>openai_base_url = "{base}"</code>、<code>model = "映射模型名"</code>（如 <code>gpt-5-codex</code>），并设置任意值的 <code>OPENAI_API_KEY</code>，即可从这里探测模型目录并请求映射模型。',
    },
  },
};

function mappingKindForPath(path) {
  return path === '/codex-mappings' ? 'codex' : 'claude';
}

const pageMeta = {
  '/': { title: '运行概览', description: '请求、Token 与渠道健康状态' },
  '/providers': { title: '供应商与渠道', description: '上游端点、账号和模型探测目录' },
  '/routes': { title: '模型路由', description: '按模型统一配置候选优先级，请求时按格式过滤' },
  '/profiles': { title: '能力档案', description: '可复用的模型能力集合，多个模型可共用同一套能力' },
  '/mappings': { title: MAPPING_KINDS.claude.title, description: MAPPING_KINDS.claude.description },
  '/codex-mappings': { title: MAPPING_KINDS.codex.title, description: MAPPING_KINDS.codex.description },
  '/logs': { title: '请求日志', description: '请求结果、性能指标与上游尝试' },
  '/settings': { title: '运行设置', description: '访问策略、熔断与超时参数' },
};

const state = {
  renderVersion: 0,
  currentPath: '/',
  connected: false,
  trustedLocal: true,
  providers: [],
  channels: [],
  channelModels: [],
  routes: [],
  profiles: [],
  mappingKind: 'claude',
  mappings: { claude: [], codex: [] },
  presets: { claude: null, codex: null },
  summary: null,
  system: null,
  protocols: null,
  logs: { page: 1, total: 0, items: [], filters: { protocol: '', upstream_protocol: '', model_id: '', upstream_model_id: '', outcome: '' } },
  dashboard: { filters: { range: 'all', start: '', end: '' } },
  settings: null,
  generatedKeys: null,
  recovery: null,
  drawerRoute: null,
  confirmResolve: null,
  modalMode: '',
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
    // P2-3: protocol registry comes from the gateway; failures keep the
    // fallback constant.
    try {
      const registry = await get('/system/protocols');
      if (registry.items?.length) state.protocols = registry.items.map((item) => item.id);
    } catch {
      // Gateway too old or unreachable: the fallback list still works.
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
    else if (path === '/mappings' || path === '/codex-mappings') await loadMappings(version);
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
  const [providerData, channelData, modelData] = await Promise.all([get('/providers'), get('/channels'), get('/channel-models')]);
  // P2-9: this loader paints the providers page; it must never paint onto
  // another page, even when the version counter somehow still matches.
  if (version !== state.renderVersion || currentPath() !== '/providers') return;
  state.providers = providerData.items;
  state.channels = channelData.items;
  state.channelModels = modelData.items;
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
      <td><span class="mono">${escapeHtml(channel.api_key_hint || '-')}</span></td>
      <td class="align-right">${number(channel.model_count)}</td>
      <td>${statusDot(health.label, health.statusClass)}</td>
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
    ${toolbar('上游资源', '供应商只保存名称与 API 根地址，渠道承载账号和请求格式', `${button({ action: 'refresh-providers', label: '刷新', iconName: 'refresh-cw' })}${button({ action: 'open-provider', label: '添加供应商', iconName: 'plus', primary: true })}${button({ action: 'open-channel', label: '添加渠道', iconName: 'network', disabled: !providers.length })}`)}
    ${panel('供应商', '仅保存名称与 Base URL', `<div class="section-body-flush">${providers.length ? `<div class="table-scroll"><table class="data-table provider-table"><thead><tr><th>名称</th><th>Base URL</th><th class="align-right">渠道</th><th class="action-cell">操作</th></tr></thead><tbody>${providerRows}</tbody></table></div>` : emptyState('尚未创建供应商', '添加供应商后即可配置一个或多个渠道。', 'network')}</div>`) }
    ${panel('账号与渠道', '路由与熔断的最小单位', `<div class="section-body-flush">${channels.length ? `<div class="table-scroll"><table class="data-table channels-table"><thead><tr><th>供应商</th><th>渠道</th><th>请求格式</th><th>密钥</th><th class="align-right">模型</th><th>健康</th><th>启用</th><th class="action-cell">操作</th></tr></thead><tbody>${channelRowsHtml}</tbody></table></div>` : emptyState('尚未配置渠道', '请在供应商下添加渠道，并选择上游支持的请求格式。', 'network')}</div>`) }
  </div>`;
}

function providerForm(editing = null) {
  openModal({
    title: editing ? '编辑供应商' : '新建供应商',
    mode: editing ? 'provider-edit' : 'provider-create',
    body: `<form class="form-stack" id="provider-form" data-form="provider" data-provider-id="${escapeAttr(editing?.id || '')}">
      <div class="field"><label for="provider-name">名称</label><input class="input" id="provider-name" name="name" required maxlength="120" value="${escapeAttr(editing?.name || '')}" autocomplete="off"></div>
      <div class="field"><label for="provider-base-url">API 根地址</label><input class="input" id="provider-base-url" name="base_url" type="url" required placeholder="https://api.example.com" value="${escapeAttr(editing?.base_url || '')}" autocomplete="url"><span class="field-help">无需填写末尾的 /v1 或 /v1beta。</span></div>
    </form>`,
    footer: `${button({ action: 'close-modal', label: '取消' })}${button({ action: 'submit-provider', label: editing ? '保存' : '创建', iconName: editing ? 'check' : 'plus', primary: true })}`,
  });
}

function channelForm(providerId = '', editing = null) {
  const selected = new Set(editing?.protocols || (editing?.protocol ? [editing.protocol] : ['openai_compatible']));
  const options = state.providers.map((provider) => `<option value="${escapeAttr(provider.id)}" ${provider.id === (editing?.provider_id || providerId) ? 'selected' : ''}>${escapeHtml(provider.name)}</option>`).join('');
  const healthModels = editing
    ? state.channelModels.filter((model) => model.channel_id === editing.id && model.available && (model.protocols || [model.protocol]).includes(editing.protocol))
    : [];
  const healthModelOptions = healthModels.map((model) => `<option value="${escapeAttr(model.model_id)}" ${model.model_id === editing?.health_check_model_id ? 'selected' : ''}>${escapeHtml(model.model_id)}</option>`).join('');
  openModal({
    title: editing ? '编辑渠道' : '新建渠道',
    mode: editing ? 'channel-edit' : 'channel-create',
    body: `<form class="form-stack" id="channel-form" data-form="channel" data-channel-id="${escapeAttr(editing?.id || '')}" data-original-protocols="${escapeAttr(JSON.stringify([...selected]))}">
      <div class="field"><label for="channel-provider">供应商</label><select class="select" id="channel-provider" name="provider_id" required ${editing ? 'disabled' : ''}><option value="">请选择供应商</option>${options}</select>${editing ? `<input type="hidden" name="provider_id" value="${escapeAttr(editing.provider_id)}">` : ''}</div>
      <div class="field"><label for="channel-name">渠道名称</label><input class="input" id="channel-name" name="name" required maxlength="120" value="${escapeAttr(editing?.name || '')}" autocomplete="off"></div>
      <fieldset class="protocol-fieldset"><legend class="fieldset-title">支持的请求格式</legend><div class="protocol-options">${protocolOptions().map((protocol) => `<label class="protocol-option"><input type="checkbox" name="protocols" value="${protocol}" ${selected.has(protocol) ? 'checked' : ''}>${escapeHtml(protocol)}</label>`).join('')}</div></fieldset>
      <div class="field"><label for="channel-api-key">${editing ? 'API Key（留空则保持不变）' : 'API Key'}</label><input class="input" id="channel-api-key" name="api_key" type="password" ${editing ? '' : 'required'} autocomplete="new-password" placeholder="${escapeAttr(editing?.api_key_hint || '')}"></div>
      <div class="field"><label for="channel-health-model">健康探测模型</label><select class="select" id="channel-health-model" name="health_check_model_id"><option value="" ${editing?.health_check_model_id ? '' : 'selected'}>自动（模型列表第一个）</option>${healthModelOptions}</select><span class="field-help">熔断到期或手动探测时使用；自动模式选择该渠道模型列表中的第一个可用模型。</span></div>
    </form>`,
    footer: `${button({ action: 'close-modal', label: '取消' })}${button({ action: 'submit-channel', label: editing ? '保存' : '创建', iconName: editing ? 'check' : 'plus', primary: true })}`,
  });
}

async function saveProvider(form) {
  const values = new FormData(form);
  const payload = { name: values.get('name').trim(), base_url: values.get('base_url').trim() };
  const providerId = form.dataset.providerId;
  if (providerId) await patch(`/providers/${providerId}`, payload);
  else await post('/providers', payload);
  closeModal();
  toast(providerId ? '供应商已更新' : '供应商已创建');
  renderPage();
}

async function saveChannel(form) {
  // P2-9: captured before any await — the reload after the write must only
  // paint while the providers page is still the current page.
  const ctx = captureContext();
  const values = new FormData(form);
  const protocolsValue = values.getAll('protocols');
  if (!protocolsValue.length) throw new Error('至少选择一种请求格式');
  const channelId = form.dataset.channelId;
  const payload = {
    name: values.get('name').trim(),
    protocols: protocolsValue,
    health_check_model_id: values.get('health_check_model_id') || null,
  };
  if (channelId) {
    const original = JSON.parse(form.dataset.originalProtocols || '[]');
    const protocolsChanged = original.length !== protocolsValue.length || original.some((value) => !protocolsValue.includes(value));
    // P2-3: the API key is part of ONE atomic PATCH — fields and key either
    // all commit or none do; a rejected key can no longer leave the channel
    // half-updated.
    const apiKey = values.get('api_key').trim();
    if (apiKey) payload.api_key = apiKey;
    await patch(`/channels/${channelId}`, payload);
    closeModal();
    toast('渠道已更新');
    if (isCurrent(ctx)) await refreshProviders(ctx.renderVersion);
    if (protocolsChanged && isCurrent(ctx)) await discoverChannel(channelId, { quiet: true });
  } else {
    const apiKey = values.get('api_key').trim();
    await post('/channels', { provider_id: values.get('provider_id'), ...payload, api_key: apiKey });
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
  // P2-9: the write (starting the run) always completes; the poll only
  // continues while the providers page is still current, and the final
  // reload never paints onto another page.
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
      throw new Error(`模型探测失败：${run.error_kind || run.status_code}`);
    }
  }
  if (isCurrent(ctx)) toast('探测仍在后台运行', 'warning');
}

async function probeChannel(channelId) {
  await post(`/channels/${channelId}/probe`);
  toast('健康探测已加入队列');
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

function routeOptions() {
  const routed = new Set(state.routes.map((route) => route.requested_model_id));
  const seen = new Set();
  return state.channelModels
    .filter((model) => model.available && !routed.has(model.model_id) && !seen.has(model.model_id) && seen.add(model.model_id))
    .map((model) => model.model_id)
    .sort();
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
    <td><div class="route-chain">${route.candidates.length ? route.candidates.map((candidate) => `<span class="route-candidate ${candidate.health_state === 'open' || candidate.manual_enabled === false ? 'is-open' : ''}"><b>P${number(candidate.priority)}</b>${escapeHtml(candidate.channel_name)}</span>`).join('') : '<span class="subtle-text">未配置候选渠道</span>'}</div></td>
    <td class="action-cell"><div class="table-actions">${iconButton({ action: 'edit-caps', iconName: 'settings', label: '配置能力', attrs: `data-route-id="${escapeAttr(route.id)}"` })}${iconButton({ action: 'edit-candidates', iconName: 'pencil', label: '编辑候选', attrs: `data-route-id="${escapeAttr(route.id)}"` })}${iconButton({ action: 'delete-route', iconName: 'trash-2', label: '删除路由', danger: true, attrs: `data-route-id="${escapeAttr(route.id)}"` })}</div></td>
  </tr>`).join('');
  return `<div class="page-stack">
    ${toolbar('模型候选优先级', '一个模型路由包含跨格式共享的候选顺序', `${button({ action: 'refresh-routes', label: '刷新', iconName: 'refresh-cw' })}${button({ action: 'open-route', label: '添加路由', iconName: 'plus', primary: true, disabled: !options.length })}`)}
    ${panel('路由表', '优先级 0 最高', `<div class="section-body-flush">${state.routes.length ? `<div class="table-scroll"><table class="data-table routes-table"><thead><tr><th>请求模型</th><th>请求格式</th><th>能力</th><th>候选顺序</th><th class="action-cell">操作</th></tr></thead><tbody>${rows}</tbody></table></div>` : emptyState('尚未创建模型路由', options.length ? '选择一个已探测模型，为它创建统一的路由入口。' : '请先在供应商与渠道页面探测可用模型。', 'route')}</div>`) }
  </div>`;
}

function routeForm() {
  const options = routeOptions();
  openModal({
    title: '新建模型路由',
    mode: 'route',
    body: `<form class="form-stack" id="route-form" data-form="route"><div class="field"><label for="route-model">模型</label><select class="select" id="route-model" name="requested_model_id" required><option value="">选择已探测模型</option>${options.map((model) => `<option value="${escapeAttr(model)}">${escapeHtml(model)}</option>`).join('')}</select><span class="field-help">同一模型的候选优先级只需配置一次，网关会按入口格式自动过滤。</span></div></form>`,
    footer: `${button({ action: 'close-modal', label: '取消' })}${button({ action: 'submit-route', label: '创建', iconName: 'plus', primary: true })}`,
  });
}

async function saveRoute(form) {
  const ctx = captureContext();
  const requestedModelId = new FormData(form).get('requested_model_id');
  if (!requestedModelId) throw new Error('请选择模型');
  const created = await post('/routes', { requested_model_id: requestedModelId });
  closeModal();
  toast('模型路由已创建');
  // P2-9: the reload and the drawer only run on the page this mutation
  // started from.
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
  const rawMap = String(values.get('thinking_level_map') || '').trim();
  if (rawMap) {
    try {
      const map = JSON.parse(rawMap);
      const keys = Object.keys(map);
      if (keys.length) parts.push(`思考档 ${keys.join('/')}`);
    } catch {
      parts.push('思考档 JSON 无效');
    }
  }
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
  const rawMap = String(values.get('thinking_level_map') || '').trim();
  let thinkingLevelMap = null;
  if (rawMap) {
    try {
      thinkingLevelMap = JSON.parse(rawMap);
    } catch {
      throw new Error('thinkingLevelMap 必须是有效 JSON');
    }
  }
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
  // P2-9: the reload never paints onto a page the user navigated to.
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

function openCandidates(routeId) {
  const route = state.routes.find((item) => item.id === routeId);
  if (!route) return;
  state.drawerRoute = route;
  const current = new Map(route.candidates.map((candidate) => [candidate.channel_model_id, candidate]));
  const candidates = state.channelModels
    .filter((model) => model.available && model.model_id === route.requested_model_id)
    .map((model) => ({ ...model, existing: current.get(model.id) }))
    .sort((a, b) => {
      if (a.existing && b.existing) return a.existing.priority - b.existing.priority;
      if (a.existing) return -1;
      if (b.existing) return 1;
      return (a.channel_name || '').localeCompare(b.channel_name || '');
    });
  let visiblePriority = 1;
  const rows = candidates.map((candidate) => {
    const selected = Boolean(candidate.existing);
    return `<div class="candidate-row ${selected ? 'is-selected' : ''}" data-candidate-row data-model-id="${escapeAttr(candidate.id)}">
      <button class="candidate-drag-handle" type="button" data-candidate-drag-handle data-model-id="${escapeAttr(candidate.id)}" aria-label="调整 ${escapeAttr(candidate.channel_name)} 的顺序" title="拖动调整顺序；可用上下方向键移动" ${selected ? 'draggable="true"' : 'disabled'}>${icon('grip-vertical')}</button>
      <span class="candidate-order" data-candidate-order>${selected ? number(visiblePriority++) : '-'}</span>
      <label class="checkbox-row"><input type="checkbox" data-candidate-selected data-model-id="${escapeAttr(candidate.id)}" ${selected ? 'checked' : ''}><span><strong>${escapeHtml(candidate.channel_name)}</strong><small class="subtle-text">${escapeHtml((candidate.protocols || [candidate.protocol]).join(' / '))}</small></span></label>
      <input class="switch-control" type="checkbox" aria-label="启用 ${escapeAttr(candidate.channel_name)}" data-candidate-enabled data-model-id="${escapeAttr(candidate.id)}" ${candidate.existing?.enabled !== false ? 'checked' : ''} ${selected ? '' : 'disabled'}>
    </div>`;
  }).join('');
  openDrawer({
    title: route.requested_model_id,
    subtitle: '列表越靠上，调度优先级越高',
    body: `<section class="drawer-section"><h3>候选渠道</h3><div class="candidate-list" data-candidate-list>${rows || emptyState('没有可用候选渠道', '当前模型没有可用的上游渠道。', 'route')}</div></section>`,
    footer: `${button({ action: 'close-drawer', label: '取消' })}${button({ action: 'save-candidates', label: '保存顺序', iconName: 'check', primary: true })}`,
  });
}

async function saveCandidates() {
  const ctx = captureContext();
  const route = state.drawerRoute;
  if (!route) return;
  const candidates = [...elements.drawerBody.querySelectorAll('[data-candidate-row]')]
    .filter((row) => row.querySelector('[data-candidate-selected]').checked)
    .map((row, priority) => {
      const modelId = row.dataset.modelId;
      const enabledInput = row.querySelector('[data-candidate-enabled]');
      return { channel_model_id: modelId, priority, enabled: enabledInput.checked };
  });
  await put(`/routes/${route.id}/candidates`, { candidates });
  closeDrawer();
  toast('候选顺序已保存');
  if (isCurrent(ctx)) await loadRoutes(ctx.renderVersion);
}

function updateCandidateOrder() {
  let priority = 1;
  for (const row of elements.drawerBody.querySelectorAll('[data-candidate-row]')) {
    const selected = row.querySelector('[data-candidate-selected]').checked;
    row.classList.toggle('is-selected', selected);
    row.querySelector('[data-candidate-enabled]').disabled = !selected;
    const dragHandle = row.querySelector('[data-candidate-drag-handle]');
    dragHandle.disabled = !selected;
    dragHandle.draggable = selected;
    row.querySelector('[data-candidate-order]').textContent = selected ? String(priority++) : '-';
  }
}

function moveCandidateRow(row, direction) {
  const rows = [...elements.drawerBody.querySelectorAll('[data-candidate-row]')]
    .filter((item) => item.querySelector('[data-candidate-selected]').checked);
  const index = rows.indexOf(row);
  const destination = rows[index + direction];
  if (!destination) return;
  if (direction < 0) destination.before(row);
  else destination.after(row);
  updateCandidateOrder();
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
  const rawMap = String(values.get('thinking_level_map') || '').trim();
  let thinkingLevelMap = null;
  if (rawMap) {
    try {
      thinkingLevelMap = JSON.parse(rawMap);
    } catch {
      throw new Error('thinkingLevelMap 必须是有效 JSON');
    }
  }
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
  const rawMap = String(values.get('thinking_level_map') || '').trim();
  let thinkingLevelMap = null;
  if (rawMap) {
    try {
      thinkingLevelMap = JSON.parse(rawMap);
    } catch {
      throw new Error('thinkingLevelMap 必须是有效 JSON');
    }
  }
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
};

function protocolLabel(protocol) {
  return PROTOCOL_LABELS[protocol] || protocol;
}

async function loadMappings(version) {
  const kind = mappingKindForPath(state.currentPath);
  const config = MAPPING_KINDS[kind];
  const [mappingData, modelData, presetData, routeData] = await Promise.all([get(config.api), get('/channel-models'), get(config.presetsApi), get('/routes')]);
  if (version !== state.renderVersion || !['/mappings', '/codex-mappings'].includes(currentPath())) return;
  state.mappingKind = kind;
  state.mappings[kind] = mappingData?.items || [];
  state.channelModels = modelData?.items || [];
  state.presets[kind] = presetData;
  state.routes = routeData?.items || [];
  elements.page.innerHTML = renderMappingsPage();
}

function entryBaseUrl(kind) {
  return `${window.location.origin}${MAPPING_KINDS[kind].entry.base}`;
}

function entryBanner(kind) {
  const config = MAPPING_KINDS[kind];
  const base = entryBaseUrl(kind);
  const body = config.entry.body.replace('{base}', base);
  return `<div class="entry-banner">
    <div class="entry-banner-copy"><strong>${escapeHtml(config.entry.title)}</strong><p>${body}</p></div>
    <div class="entry-banner-actions"><code class="mono">${escapeHtml(base)}</code>${iconButton({ action: 'copy-entry-url', iconName: 'copy', label: '复制接入地址', attrs: `data-entry-url="${escapeAttr(base)}"` })}</div>
  </div>`;
}

function renderMappingsPage() {
  const kind = state.mappingKind;
  const config = MAPPING_KINDS[kind];
  const mappings = state.mappings[kind] || [];
  const rows = mappings.map((mapping) => `<tr class="${mapping.enabled ? '' : 'is-disabled-row'}">
    <td><span class="mono">${escapeHtml(mapping[config.modelIdField])}</span>${mapping.display_name ? `<small class="subtle-text">${escapeHtml(mapping.display_name)}</small>` : ''}</td>
    <td><span class="protocol">${escapeHtml(protocolLabel(mapping.upstream_protocol))}</span></td>
    <td><span class="mono">${escapeHtml(mapping.upstream_model_id)}</span></td>
    <td><div class="route-chain">${mapping.candidates.length ? mapping.candidates.map((candidate) => `<span class="route-candidate"><b>P${number(candidate.priority)}</b>${escapeHtml(candidate.channel_name)}</span>`).join('') : '<span class="subtle-text">该模型暂无可用路由候选</span>'}</div></td>
    <td>${mapping.enabled ? statusDot('已启用', 'is-success') : statusDot('已停用', 'is-muted')}</td>
    <td class="action-cell"><div class="table-actions">${iconButton({ action: 'edit-mapping', iconName: 'pencil', label: '编辑映射', attrs: `data-mapping-id="${escapeAttr(mapping.id)}"` })}${iconButton({ action: 'delete-mapping', iconName: 'trash-2', label: '删除映射', danger: true, attrs: `data-mapping-id="${escapeAttr(mapping.id)}"` })}</div></td>
  </tr>`).join('');
  return `<div class="page-stack">
    ${entryBanner(kind)}
    ${toolbar(config.toolbarTitle, '映射指向系统中已配置的模型，候选渠道直接继承该模型的路由', `${button({ action: 'refresh-mappings', label: '刷新', iconName: 'refresh-cw' })}${button({ action: 'open-mapping', label: '添加映射', iconName: 'plus', primary: true })}`)}
    ${panel('映射表', '候选渠道取自上游模型在「模型路由」中的配置，无需单独设置', `<div class="section-body-flush">${mappings.length ? `<div class="table-scroll"><table class="data-table mappings-table"><thead><tr><th>${escapeHtml(config.tableHead)}</th><th>上游协议</th><th>上游模型</th><th>候选渠道（继承路由）</th><th>状态</th><th class="action-cell">操作</th></tr></thead><tbody>${rows}</tbody></table></div>` : emptyState('尚未创建模型映射', config.emptyDetail, 'shuffle')}</div>`) }
  </div>`;
}

function configuredModelOptions(selected, protocol) {
  const seen = new Set();
  const models = (state.routes || [])
    .filter((route) => route.enabled && (route.protocols || []).includes(protocol) && !seen.has(route.requested_model_id) && seen.add(route.requested_model_id))
    .map((route) => route.requested_model_id)
    .sort();
  if (!models.length) return '<option value="" disabled>该协议暂无已配置路由的模型，请先在「模型路由」页配置</option>';
  return models.map((model) => `<option value="${escapeAttr(model)}" ${selected === model ? 'selected' : ''}>${escapeHtml(model)}</option>`).join('');
}

function mappingForm(editing = null) {
  const kind = state.mappingKind;
  const config = MAPPING_KINDS[kind];
  const protocols = Object.keys(PROTOCOL_LABELS);
  const currentProtocol = editing?.upstream_protocol || 'openai_compatible';
  const protocolOptions = protocols.map((protocol) => `<option value="${protocol}" ${currentProtocol === protocol ? 'selected' : ''}>${escapeHtml(protocolLabel(protocol))}</option>`).join('');
  const presets = state.presets[kind]?.items || [];
  const presetOptions = presetOptionsOf(state.presets[kind]);
  openModal({
    title: editing ? '编辑模型映射' : '新建模型映射',
    mode: editing ? 'mapping-edit' : 'mapping-create',
    body: `<form class="form-stack" id="mapping-form" data-form="mapping" data-mapping-id="${escapeAttr(editing?.id || '')}">
      <div class="field"><label for="mapping-preset">${escapeHtml(config.presetLabel)}</label><div class="preset-row"><select class="select" id="mapping-preset" data-preset-select ${editing ? 'disabled' : ''}><option value="">-- 手动输入或从预设中选择 --</option>${presetOptions}</select>${button({ action: 'refresh-presets', label: '刷新预设', iconName: 'refresh-cw', disabled: Boolean(editing) })}</div><span class="field-help">${escapeHtml(config.presetHelp)}</span></div>
      <div class="field"><label for="mapping-model-id">${escapeHtml(config.modelLabel)}</label><input class="input" id="mapping-model-id" name="${escapeAttr(config.modelIdField)}" required maxlength="255" placeholder="${escapeAttr(config.placeholder)}" value="${escapeAttr(editing?.[config.modelIdField] || '')}" autocomplete="off"><span class="field-help">${escapeHtml(config.mappingHelp)}</span></div>
      <div class="field"><label for="mapping-display-name">显示名称</label><input class="input" id="mapping-display-name" name="display_name" maxlength="255" placeholder="${escapeAttr(config.displayPlaceholder)}" value="${escapeAttr(editing?.display_name || '')}" autocomplete="off"><span class="field-help">出现在模型目录中的展示名称，留空则使用模型名。</span></div>
      <div class="field"><label for="mapping-upstream-protocol">上游协议</label><select class="select" id="mapping-upstream-protocol" name="upstream_protocol" data-upstream-protocol required>${protocolOptions}</select><span class="field-help">${escapeHtml(config.protocolHelp)}</span></div>
      <div class="field"><label for="mapping-upstream-model">上游模型（已配置路由）</label><select class="select" id="mapping-upstream-model" name="upstream_model_id" required>${configuredModelOptions(editing?.upstream_model_id || '', currentProtocol)}</select><span class="field-help">数据源为「模型路由」页已配置路由的模型，按所选上游协议过滤；候选渠道自动继承该模型的路由配置，无需单独设置。</span></div>
    </form>`,
    footer: `${button({ action: 'close-modal', label: '取消' })}${button({ action: 'submit-mapping', label: editing ? '保存' : '创建', iconName: editing ? 'check' : 'plus', primary: true })}`,
  });
}

async function saveMapping(form) {
  const ctx = captureContext();
  const kind = state.mappingKind;
  const config = MAPPING_KINDS[kind];
  const values = new FormData(form);
  const payload = {
    [config.modelIdField]: values.get(config.modelIdField).trim(),
    display_name: values.get('display_name').trim() || null,
    upstream_model_id: values.get('upstream_model_id').trim(),
    upstream_protocol: values.get('upstream_protocol'),
  };
  const mappingId = form.dataset.mappingId;
  if (mappingId) {
    const mapping = (state.mappings[kind] || []).find((item) => item.id === mappingId);
    if (mapping) payload.enabled = mapping.enabled;
    await patch(`${config.api}/${mappingId}`, payload);
  } else {
    await post(config.api, payload);
  }
  closeModal();
  toast(mappingId ? '映射已更新' : '映射已创建，候选渠道继承上游模型路由');
  if (isCurrent(ctx)) await loadMappings(ctx.renderVersion);
}


async function deleteMapping(mappingId) {
  const kind = state.mappingKind;
  const config = MAPPING_KINDS[kind];
  const mapping = (state.mappings[kind] || []).find((item) => item.id === mappingId);
  if (!await confirmAction({ title: '删除模型映射', message: `删除映射“${mapping?.[config.modelIdField] || ''}”？其全部候选渠道将一并删除。`, confirmLabel: '删除', danger: true })) return;
  await remove(`${config.api}/${mappingId}`);
  toast('映射已删除');
  renderPage();
}

// 预设刷新去重:一次刷新流程进行中时忽略重复点击 (P1-5)。
let presetRefreshInFlight = false;

async function refreshPresets() {
  if (presetRefreshInFlight) return;
  presetRefreshInFlight = true;
  const kind = state.mappingKind;
  const config = MAPPING_KINDS[kind];
  const version = state.renderVersion;
  try {
    let queued;
    try {
      queued = await post(`${config.presetsApi}/refresh`);
    } catch (error) {
      // 无可用渠道时后端返回明确 409,不得假装 queued (P1-5)。
      toast(error.message || '预设刷新失败', 'error');
      return;
    }
    const runIds = queued?.run_ids || [];
    if (!runIds.length) return;
    toast(`已排队 ${runIds.length} 个渠道的模型探测，完成后自动更新预设…`);
    // 轮询每个 run 的终态;用户导航离开或关闭弹窗后立即放弃,
    // 不覆盖新页面 (P1-5)。
    const results = await pollDiscoveryRuns(runIds, () =>
      version !== state.renderVersion || !document.getElementById('mapping-form'));
    if (results === null) return;
    const succeeded = results.filter((run) => run.status === 'succeeded').length;
    const failed = results.length - succeeded;
    if (failed === 0) {
      toast(`预设已刷新（${succeeded} 个渠道）`);
    } else if (succeeded > 0) {
      toast(`预设部分刷新：成功 ${succeeded}，失败 ${failed}`, 'warning');
    } else {
      toast('所有渠道模型探测失败，请检查渠道配置', 'error');
    }
    // 重新拉取预设并刷新弹窗里的下拉框。
    const presetData = await get(config.presetsApi);
    if (version !== state.renderVersion || !document.getElementById('mapping-form')) return;
    state.presets[kind] = presetData;
    const select = document.querySelector('#mapping-preset');
    if (select && !select.disabled) {
      const current = select.value;
      select.innerHTML = `<option value="">-- 手动输入或从预设中选择 --</option>${presetOptionsOf(presetData)}`;
      if (current) select.value = current;
    }
  } catch (error) {
    toast(error.message || '预设刷新失败', 'error');
  } finally {
    presetRefreshInFlight = false;
  }
}

function presetOptionsOf(presetData) {
  return (presetData?.items || [])
    .map((preset) => `<option value="${escapeAttr(preset.id)}">${escapeHtml(preset.display_name || preset.id)}</option>`)
    .join('');
}

// 轮询 discovery runs 直到全部到达终态。页面上下文已消失(导航/关闭
// 弹窗)时返回 null。
async function pollDiscoveryRuns(runIds, gone) {
  const terminal = new Map();
  while (terminal.size < runIds.length) {
    if (gone()) return null;
    await Promise.all(runIds.filter((id) => !terminal.has(id)).map(async (id) => {
      try {
        const run = await get(`/discovery-runs/${id}`);
        if (run.status === 'succeeded' || run.status === 'failed') terminal.set(id, run);
      } catch {
        // 单次轮询失败不终止流程,下一轮重试。
      }
    }));
    if (terminal.size < runIds.length) {
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
  return [...terminal.values()];
}

async function copyEntryUrl(url) {
  try {
    await navigator.clipboard.writeText(url);
    toast('接入地址已复制');
  } catch {
    toast('无法访问剪贴板，请手动复制', 'warning');
  }
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
  } catch (error) {
    if (version !== state.renderVersion || currentPath() !== '/settings') return;
    if (error.body?.code === 'config_corrupted') {
      // P1-3: 管理面因持久化配置损坏而 fail-closed。区分两种来源:
      // 访问密钥损坏 → 密钥恢复页;运行时设置损坏 → 修复表单。
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

/// P1-3: 运行时设置损坏时的修复表单——经恢复模式端点(loopback + nonce)
/// 原子重写损坏行,不接收访问密钥。初始值取默认值并明确提示。
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


// 设置表单 schema (P1-7): 一份定义同时驱动渲染、提交载荷与前端范围
// 校验——新增字段不可能只接入一半。
const SETTINGS_SECTIONS = [
  { id: 'fault', title: '故障转移', note: '优先级路由与自动熔断', grid: '',
    fields: [['failure_threshold', '连续失败阈值', 1, 20], ['circuit_open_seconds', '熔断时间（秒）', 30, 86400], ['max_failover_attempts', '最大渠道尝试', 1, 20]] },
  { id: 'timeout', title: '超时', note: '上游连接与响应期限', grid: '',
    fields: [['connect_timeout_seconds', '连接超时（秒）', 1, 120], ['first_byte_timeout_seconds', '首字节超时（秒）', 1, 600], ['first_token_timeout_seconds', '首 Token 超时（秒）', 1, 600], ['stream_idle_timeout_seconds', '流式空闲超时（秒）', 10, 3600], ['non_stream_total_timeout_seconds', '非流式总超时（秒）', 10, 3600]] },
  { id: 'limits', title: '请求限制', note: '代理请求体与上游响应缓冲上限', grid: '', help: '映射转换等必须整体缓冲的上游响应硬上限；超过返回 502。流式转发不受此限制。',
    fields: [['max_request_body_mb', '最大请求体（MiB）', 1, 1024], ['max_buffered_upstream_body_mb', '上游响应缓冲上限（MiB）', 1, 1024]] },
  { id: 'maintenance', title: '维护', note: '周期任务与数据保留', grid: 'is-two',
    fields: [['model_discovery_interval_hours', '模型探测周期（小时）', 1, 168], ['log_retention_days', '日志保留（天）', 1, 365]] },
];

// 设置损坏修复表单的初始值,与后端 RuntimeSettings::default() 一致
// (P1-3)。损坏行无法安全读取,因此修复表单从这些默认值开始。
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
    ${settingsSectionsHtml(settings)}
  </form>`;
}

async function saveSettings(form) {
  const version = state.renderVersion;
  const values = new FormData(form);
  const payload = {
    trust_local_network: values.get('trust_local_network') === 'on',
  };
  // P1-7: the payload is generated from the SAME schema that renders the
  // fields, and each value is range-checked client-side before the PATCH
  // (the backend 422 stays as the second gate).
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
    // P1-3: 设置损坏修复——loopback + 一次性 nonce 门控,不接收访问密钥。
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
  // P1-4: while in recovery mode the generate endpoint demands the
  // one-time challenge issued with the recovery status; it is single-use,
  // so a failure means re-entering recovery and getting a fresh one.
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
    // Recovery mode: the keys are valid again — adopt the fresh admin key
    // and reload the full settings page.
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

// P2-9: a mutation captures the page context when it starts; navigation
// invalidates it. The data write still completes, but any reload/render the
// mutation triggers afterwards only runs while the same page is still
// current — a stale action can never paint its markup onto another page.
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
    if (action === 'open-provider') return providerForm();
    if (action === 'edit-provider') return providerForm(state.providers.find((item) => item.id === target.dataset.providerId));
    if (action === 'open-channel') return channelForm(target.dataset.providerId || '');
    if (action === 'edit-channel') return channelForm('', state.channels.find((item) => item.id === target.dataset.channelId));
    if (action === 'show-models') return showModels(target.dataset.channelId);
    if (action === 'discover-channel') return discoverChannel(target.dataset.channelId);
    if (action === 'probe-channel') return probeChannel(target.dataset.channelId);
    if (action === 'delete-provider') return deleteProvider(target.dataset.providerId);
    if (action === 'delete-channel') return deleteChannel(target.dataset.channelId);
    if (action === 'refresh-routes') return renderPage();
    if (action === 'open-route') return routeForm();
    if (action === 'edit-caps') return openCapabilityEditor(target.dataset.routeId);
    if (action === 'edit-candidates') return openCandidates(target.dataset.routeId);
    if (action === 'save-candidates') return saveCandidates();
    if (action === 'save-caps') return document.getElementById('caps-form')?.requestSubmit();
    if (action === 'detect-caps') return detectCapabilities();
    if (action === 'delete-route') return deleteRoute(target.dataset.routeId);
    if (action === 'refresh-profiles') return renderPage();
    if (action === 'open-profile') return profileForm();
    if (action === 'edit-profile') return profileForm(state.profiles.find((item) => item.id === target.dataset.profileId));
    if (action === 'delete-profile') return deleteProfile(target.dataset.profileId);
    if (action === 'save-as-profile') return saveCurrentAsProfile();
    if (action === 'refresh-mappings') return renderPage();
    if (action === 'open-mapping') return mappingForm();
    if (action === 'edit-mapping') return mappingForm((state.mappings[state.mappingKind] || []).find((item) => item.id === target.dataset.mappingId));
    if (action === 'delete-mapping') return deleteMapping(target.dataset.mappingId);
    if (action === 'refresh-presets') return refreshPresets();
    if (action === 'copy-entry-url') return copyEntryUrl(target.dataset.entryUrl);
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
    if (action === 'submit-mapping') return document.getElementById('mapping-form')?.requestSubmit();
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
    else if (form.dataset.form === 'mapping') await saveMapping(form);
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
  if (target.matches('[data-candidate-selected]')) {
    const row = target.closest('.candidate-row');
    if (target.checked) elements.drawerBody.querySelector('[data-candidate-list]').append(row);
    updateCandidateOrder();
  }
  if (target.matches('[data-preset-select]')) {
    const presetId = target.value;
    const preset = (state.presets[state.mappingKind]?.items || []).find((item) => item.id === presetId);
    if (preset) {
      const modelInput = document.getElementById('mapping-model-id');
      const nameInput = document.getElementById('mapping-display-name');
      if (modelInput) modelInput.value = preset.id;
      if (nameInput) nameInput.value = preset.display_name || '';
    }
  }
  if (target.matches('[data-upstream-protocol]')) {
    const modelSelect = document.getElementById('mapping-upstream-model');
    if (modelSelect) {
      const current = modelSelect.value;
      modelSelect.innerHTML = configuredModelOptions(current, target.value);
      const stillValid = [...modelSelect.options].some((option) => option.value === current);
      if (!stillValid) modelSelect.value = '';
    }
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
document.addEventListener('input', (event) => {
  if (event.target.closest('[data-form="caps"]')) refreshCapsPreview();
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
window.addEventListener('drawer-closed', () => { state.drawerRoute = null; });

checkConnection();
renderPage();

