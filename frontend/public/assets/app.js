const API_ROOT = '/api/admin/v1';
const TOKEN_KEY = 'ai-gateway-admin-token';
const PROTOCOLS = ['openai_compatible', 'openai_responses', 'claude', 'gemini'];

const pageMeta = {
  '/': { title: '运行概览', description: '请求、Token 与渠道健康状态' },
  '/providers': { title: '供应商与渠道', description: '上游端点、账号和模型探测目录' },
  '/routes': { title: '模型路由', description: '按模型统一配置候选优先级，请求时按格式过滤' },
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
  summary: null,
  system: null,
  logs: { page: 1, total: 0, items: [], filters: { protocol: '', model_id: '', outcome: '' } },
  settings: null,
  generatedKeys: null,
  drawerRoute: null,
  confirmResolve: null,
  modalMode: '',
};

const elements = {
  page: document.getElementById('page-content'),
  title: document.getElementById('page-title'),
  description: document.getElementById('page-description'),
  sideStatus: document.getElementById('side-status'),
  sideMode: document.getElementById('side-mode'),
  topStatus: document.getElementById('top-status'),
  lockButton: document.getElementById('lock-button'),
  sidebar: document.getElementById('sidebar'),
  sidebarScrim: document.getElementById('sidebar-scrim'),
  modal: document.getElementById('modal'),
  modalTitle: document.getElementById('modal-title'),
  modalBody: document.getElementById('modal-body'),
  modalFooter: document.getElementById('modal-footer'),
  drawer: document.getElementById('drawer'),
  drawerTitle: document.getElementById('drawer-title'),
  drawerSubtitle: document.getElementById('drawer-subtitle'),
  drawerBody: document.getElementById('drawer-body'),
  drawerFooter: document.getElementById('drawer-footer'),
  toasts: document.getElementById('toast-region'),
};

function icon(name, className = '') {
  return `<svg class="icon ${className}" aria-hidden="true"><use href="#i-${name}"></use></svg>`;
}

function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function escapeAttr(value) {
  return escapeHtml(value);
}

function number(value, options = {}) {
  if (value === null || value === undefined || value === '') return '-';
  return Number(value).toLocaleString('zh-CN', { maximumFractionDigits: 2, ...options });
}

function tokenCount(value) {
  if (value === null || value === undefined || value === '') return '-';
  const amount = Number(value);
  if (!Number.isFinite(amount)) return '-';
  if (amount >= 1_000_000_000_000) return `${(amount / 1_000_000_000_000).toFixed(2)}T`;
  if (amount >= 1_000_000_000) return `${(amount / 1_000_000_000).toFixed(2)}B`;
  if (amount >= 1_000_000) return `${(amount / 1_000_000).toFixed(2)}M`;
  if (amount >= 1_000) return `${(amount / 1_000).toFixed(2)}K`;
  return number(amount);
}

function percent(value) {
  if (value === null || value === undefined) return '-';
  return `${(Number(value) * 100).toFixed(1)}%`;
}

function parseStandardTime(value) {
  if (!value) return null;
  const raw = String(value).trim().replace(' ', 'T');
  const normalized = /(?:Z|[+-]\d{2}:?\d{2})$/i.test(raw) ? raw : `${raw}Z`;
  const date = new Date(normalized);
  return Number.isNaN(date.getTime()) ? null : date;
}

function formatTime(value) {
  const date = parseStandardTime(value);
  return date ? date.toLocaleString('zh-CN', { hour12: false }) : '-';
}

function formatLogTime(value) {
  const date = parseStandardTime(value);
  if (!date) return '-';
  const pad = (item) => String(item).padStart(2, '0');
  return `${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

function duration(value) {
  return value === null || value === undefined ? '-' : `${number(value)} ms`;
}

function protocols(items = []) {
  return `<div class="protocol-list">${items.map((item) => `<span class="protocol">${escapeHtml(item)}</span>`).join('')}</div>`;
}

function responseChannelTags(items = []) {
  if (!items.length) return '<span class="log-route-empty">-</span>';
  const routeTitle = items.join(' -> ');
  return `<div class="protocol-list log-route-list" title="${escapeAttr(routeTitle)}">${items.map((item) => `<span class="protocol log-route-tag">${escapeHtml(item)}</span>`).join('')}</div>`;
}

function healthInfo(channel) {
  const health = channel.health || {};
  const raw = channel.manual_enabled === false ? 'disabled' : (health.state || 'active');
  const labels = { active: '正常', open: '熔断', half_open: '探测中', disabled: '已禁用' };
  const statusClass = raw === 'active' ? 'is-success' : raw === 'half_open' ? 'is-warning' : raw === 'disabled' ? 'is-muted' : 'is-danger';
  return { label: labels[raw] || raw, statusClass, raw };
}

function outcomeInfo(outcome) {
  const labels = { success: '成功', http_error: '上游错误', transport_error: '传输错误', upstream_error: '上游错误', gateway_error: '网关错误', stream_interrupted: '流中断', cancelled: '已取消', pending: '等待中' };
  const statusClass = outcome === 'success' ? 'is-success' : outcome === 'pending' ? 'is-warning' : 'is-danger';
  return { label: labels[outcome] || outcome || '-', statusClass };
}

function statusDot(label, statusClass) {
  return `<span class="status-dot ${statusClass}"><i></i>${escapeHtml(label)}</span>`;
}

function button({ action, label, iconName, primary = false, danger = false, disabled = false, attrs = '', type = 'button' }) {
  const classes = ['button'];
  if (primary) classes.push('button-primary');
  if (danger) classes.push('button-danger');
  return `<button type="${type}" class="${classes.join(' ')}" data-action="${escapeAttr(action)}" ${disabled ? 'disabled' : ''} ${attrs}>${iconName ? icon(iconName) : ''}<span>${escapeHtml(label)}</span></button>`;
}

function iconButton({ action, iconName, label, danger = false, attrs = '', disabled = false }) {
  return `<button type="button" class="icon-button ${danger ? 'button-danger' : ''}" data-action="${escapeAttr(action)}" aria-label="${escapeAttr(label)}" title="${escapeAttr(label)}" ${disabled ? 'disabled' : ''} ${attrs}>${icon(iconName)}</button>`;
}

function toolbar(title, note, actions) {
  return `<div class="page-toolbar"><div class="page-toolbar-copy"><h2>${escapeHtml(title)}</h2>${note ? `<p class="toolbar-note">${escapeHtml(note)}</p>` : ''}</div><div class="action-row">${actions}</div></div>`;
}

function panel(title, note, body, className = '') {
  return `<section class="section-panel ${className}"><header class="section-header"><h2>${escapeHtml(title)}</h2>${note ? `<span class="section-header-note">${escapeHtml(note)}</span>` : ''}</header>${body}</section>`;
}

function emptyState(title, detail, iconName = 'server') {
  return `<div class="empty-state"><div class="empty-state-inner">${icon(iconName)}<h3>${escapeHtml(title)}</h3><p>${escapeHtml(detail)}</p></div></div>`;
}

function skeleton() {
  return `<div class="skeleton-stack"><div class="skeleton-toolbar"></div><div class="skeleton-metrics"><div class="skeleton-card"></div><div class="skeleton-card"></div><div class="skeleton-card"></div><div class="skeleton-card"></div></div><div class="skeleton-panel"></div></div>`;
}

function getToken() {
  return sessionStorage.getItem(TOKEN_KEY) || '';
}

function setToken(value) {
  if (value) sessionStorage.setItem(TOKEN_KEY, value);
  else sessionStorage.removeItem(TOKEN_KEY);
}

async function api(path, options = {}) {
  const headers = new Headers(options.headers || {});
  const token = getToken();
  if (token) headers.set('Authorization', `Bearer ${token}`);
  if (options.body && !(options.body instanceof FormData)) headers.set('Content-Type', 'application/json');
  const response = await fetch(`${API_ROOT}${path}`, { ...options, headers });
  if (response.status === 204) return null;
  const body = await response.json().catch(() => null);
  if (!response.ok) {
    const error = new Error(body?.detail || body?.error?.message || `请求失败 (${response.status})`);
    error.status = response.status;
    error.body = body;
    if (response.status === 401) openAuthModal();
    throw error;
  }
  return body;
}

const get = (path) => api(path);
const post = (path, body = {}) => api(path, { method: 'POST', body: JSON.stringify(body) });
const put = (path, body = {}) => api(path, { method: 'PUT', body: JSON.stringify(body) });
const patch = (path, body = {}) => api(path, { method: 'PATCH', body: JSON.stringify(body) });
const remove = (path) => api(path, { method: 'DELETE' });

function toast(message, type = 'success') {
  const node = document.createElement('div');
  node.className = `toast ${type === 'error' ? 'is-error' : type === 'warning' ? 'is-warning' : ''}`;
  node.innerHTML = `${icon(type === 'error' ? 'x' : type === 'warning' ? 'activity' : 'check')}<span>${escapeHtml(message)}</span>`;
  elements.toasts.append(node);
  window.setTimeout(() => node.remove(), 4200);
}

function openModal({ title, body, footer = '', mode = '' }) {
  if (elements.modal.open) elements.modal.close();
  state.modalMode = mode;
  elements.modalTitle.textContent = title;
  elements.modalBody.innerHTML = body;
  elements.modalFooter.innerHTML = footer;
  elements.modal.showModal();
}

function closeModal() {
  if (elements.modal.open) elements.modal.close();
}

function openDrawer({ title, subtitle = '', body, footer = '' }) {
  elements.drawerTitle.textContent = title;
  elements.drawerSubtitle.textContent = subtitle;
  elements.drawerSubtitle.hidden = !subtitle;
  elements.drawerBody.innerHTML = body;
  elements.drawerFooter.innerHTML = footer;
  elements.drawer.classList.add('is-open');
  elements.drawer.setAttribute('aria-hidden', 'false');
}

function closeDrawer() {
  elements.drawer.classList.remove('is-open');
  elements.drawer.setAttribute('aria-hidden', 'true');
  state.drawerRoute = null;
}

function confirmAction({ title, message, confirmLabel = '确认', danger = false }) {
  return new Promise((resolve) => {
    state.confirmResolve = resolve;
    openModal({
      title,
      mode: 'confirm',
      body: `<p class="subtle-text">${escapeHtml(message)}</p>`,
      footer: `${button({ action: 'confirm-cancel', label: '取消' })}${button({ action: 'confirm-accept', label: confirmLabel, primary: true, danger, iconName: danger ? 'trash-2' : 'check' })}`,
    });
  });
}

function resolveConfirmation(value) {
  const resolve = state.confirmResolve;
  state.confirmResolve = null;
  closeModal();
  if (resolve) resolve(value);
}

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

function pageError(error) {
  return `<div class="page-stack">${panel('无法加载页面', '', emptyState('请求管理 API 失败', error?.message || '请检查网关服务与访问权限。', 'activity'))}</div>`;
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
    else if (path === '/logs') await loadLogs(version);
    else if (path === '/settings') await loadSettings(version);
  } catch (error) {
    if (version !== state.renderVersion) return;
    elements.page.innerHTML = pageError(error);
  }
}

function metric(label, value, foot, className = '', iconName = 'activity') {
  return `<article class="metric-card ${className}"><div class="metric-heading"><span class="metric-icon">${icon(iconName)}</span><span class="metric-label">${escapeHtml(label)}</span></div><strong class="metric-value">${value}</strong><p class="metric-foot">${escapeHtml(foot)}</p></article>`;
}

function tokenMetric(label, value, className = '', iconName = 'database') {
  return `<article class="token-metric ${className}"><div class="token-metric-heading"><span class="token-metric-icon">${icon(iconName)}</span><span>${escapeHtml(label)}</span></div><strong>${value}</strong></article>`;
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

async function loadDashboard(version) {
  const [summary, channelData, system] = await Promise.all([get('/stats/summary'), get('/channels'), get('/system/status')]);
  if (version !== state.renderVersion) return;
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
      ${metric('平均首 Token', `${number(summary.average_first_token_ms)}<small>${summary.average_first_token_ms == null ? '' : 'ms'}</small>`, '仅统计可识别的流式响应', 'is-warning', 'server')}
      ${metric('平均 TPS', number(summary.average_tps, { maximumFractionDigits: 3 }), '按完整响应耗时计算', 'is-violet', 'activity')}
    </section>
    <section class="dashboard-token-section" aria-labelledby="token-usage-title">
      <div class="dashboard-section-heading"><div><h2 id="token-usage-title">Token 使用</h2><p>当前日志保留期内的累计用量</p></div></div>
      <div class="token-grid">
        ${tokenMetric('缓存命中', tokenCount(summary.cache_read_tokens), 'is-accent', 'database')}
        ${tokenMetric('缓存写入', tokenCount(summary.cache_write_tokens), 'is-info', 'server')}
        ${tokenMetric('缓存未命中', tokenCount(summary.cache_miss_input_tokens), 'is-warning', 'activity')}
        ${tokenMetric('输出', tokenCount(summary.output_tokens), 'is-violet', 'network')}
      </div>
    </section>
    <section class="cache-hit-section" aria-labelledby="cache-hit-title">
      <div class="dashboard-section-heading"><div><h2 id="cache-hit-title">缓存命中率</h2><p>按协议类别汇总缓存使用情况</p></div></div>
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
  if (version !== state.renderVersion) return;
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
      <fieldset class="protocol-fieldset"><legend class="fieldset-title">支持的请求格式</legend><div class="protocol-options">${PROTOCOLS.map((protocol) => `<label class="protocol-option"><input type="checkbox" name="protocols" value="${protocol}" ${selected.has(protocol) ? 'checked' : ''}>${escapeHtml(protocol)}</label>`).join('')}</div></fieldset>
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
    await patch(`/channels/${channelId}`, payload);
    const apiKey = values.get('api_key').trim();
    if (apiKey) await put(`/channels/${channelId}/api-key`, { api_key: apiKey });
    closeModal();
    toast('渠道已更新');
    await refreshProviders();
    if (protocolsChanged) await discoverChannel(channelId, { quiet: true });
  } else {
    const apiKey = values.get('api_key').trim();
    await post('/channels', { provider_id: values.get('provider_id'), ...payload, api_key: apiKey });
    closeModal();
    toast('渠道已创建');
    await refreshProviders();
  }
}

async function refreshProviders() {
  await loadProviders(state.renderVersion);
}

async function toggleChannel(channelId, enabled) {
  const channel = state.channels.find((item) => item.id === channelId);
  try {
    await patch(`/channels/${channelId}`, { manual_enabled: enabled });
    if (channel) channel.manual_enabled = enabled;
    toast(enabled ? '渠道已启用' : '渠道已禁用');
    elements.page.innerHTML = renderProvidersMarkup();
  } catch (error) {
    toast(error.message, 'error');
    elements.page.innerHTML = renderProvidersMarkup();
  }
}

async function discoverChannel(channelId, { quiet = false } = {}) {
  const result = await post(`/channels/${channelId}/discover-models`);
  if (!quiet) toast('模型探测已开始');
  for (let attempt = 0; attempt < 30; attempt += 1) {
    await new Promise((resolve) => window.setTimeout(resolve, 500));
    const run = await get(`/discovery-runs/${result.run_id}`);
    if (run.status === 'succeeded') {
      toast(`探测到 ${run.model_count} 个模型`);
      await refreshProviders();
      return;
    }
    if (run.status === 'failed') {
      throw new Error(`模型探测失败：${run.error_kind || run.status_code}`);
    }
  }
  toast('探测仍在后台运行', 'warning');
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
  const provider = state.providers.find((item) => item.id === providerId);
  if (!await confirmAction({ title: '删除供应商', message: `删除供应商“${provider?.name || ''}”？其全部渠道及相关路由候选项将一并删除。`, confirmLabel: '删除', danger: true })) return;
  await remove(`/providers/${providerId}`);
  toast('供应商已删除');
  renderPage();
}

async function deleteChannel(channelId) {
  const channel = state.channels.find((item) => item.id === channelId);
  if (!await confirmAction({ title: '删除渠道', message: `删除渠道“${channel?.name || ''}”？该渠道的模型和相关路由候选项将一并删除。`, confirmLabel: '删除', danger: true })) return;
  await remove(`/channels/${channelId}`);
  toast('渠道已删除');
  renderPage();
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
  const [routeData, modelData] = await Promise.all([get('/routes'), get('/channel-models')]);
  if (version !== state.renderVersion) return;
  state.routes = routeData.items;
  state.channelModels = modelData.items;
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
  return `<div class="capability-summary"><span class="capability-source">${caps.source === 'manual' ? '手动' : '自动'}</span>${parts.length ? parts.map((item) => `<span>${escapeHtml(item)}</span>`).join('') : '<span class="subtle-text">未识别</span>'}</div>`;
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
  const requestedModelId = new FormData(form).get('requested_model_id');
  if (!requestedModelId) throw new Error('请选择模型');
  const created = await post('/routes', { requested_model_id: requestedModelId });
  closeModal();
  toast('模型路由已创建');
  await loadRoutes(state.renderVersion);
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
  const body = `<form class="form-stack" id="caps-form" data-form="caps">
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
  await loadRoutes(state.renderVersion);
}

async function detectCapabilities() {
  const route = state.drawerRoute;
  if (!route) return;
  const capabilities = await post(`/model-capabilities/detect/${encodeURIComponent(route.requested_model_id)}`);
  route.capabilities = capabilities;
  openDrawer({
    title: route.requested_model_id,
    subtitle: '模型能力会写入 model catalog 的 x_local_gateway.pi_model_config',
    ...capabilityEditorMarkup(route),
  });
  toast('已重新聚合上游能力，结果已显示在下方表单中');
  await loadRoutes(state.renderVersion);
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
  await loadRoutes(state.renderVersion);
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

async function loadLogs(version) {
  const { page, filters } = state.logs;
  const query = new URLSearchParams({ page: String(page), page_size: '50' });
  Object.entries(filters).forEach(([key, value]) => { if (value) query.set(key, value); });
  const data = await get(`/requests?${query}`);
  if (version !== state.renderVersion) return;
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
      <select class="select" name="protocol"><option value="">全部协议</option>${PROTOCOLS.map((protocol) => `<option value="${protocol}" ${filters.protocol === protocol ? 'selected' : ''}>${protocol}</option>`).join('')}</select>
      <input class="input" name="model_id" value="${escapeAttr(filters.model_id)}" placeholder="模型 ID" autocomplete="off">
      <select class="select is-outcome" name="outcome"><option value="">全部结果</option>${['success', 'upstream_error', 'gateway_error', 'stream_interrupted', 'cancelled'].map((outcome) => `<option value="${outcome}" ${filters.outcome === outcome ? 'selected' : ''}>${outcomeInfo(outcome).label}</option>`).join('')}</select>
      ${button({ action: 'submit-log-filter', label: '查询', iconName: 'search', primary: true })}
    </form><div class="section-body-flush">${items.length ? `<div class="table-scroll"><table class="data-table logs-table" aria-label="请求记录"><thead><tr><th>时间</th><th>模型</th><th>响应渠道</th><th>协议</th><th>结果</th><th class="align-right">HTTP</th><th class="align-right">尝试</th><th class="align-right">耗时</th></tr></thead><tbody>${rows}</tbody></table></div>` : emptyState('暂无请求日志', '发送模型请求后，这里会显示请求与渠道尝试。', 'file-text')}</div><div class="pagination"><span>第 ${page} / ${maxPage} 页，共 ${number(total)} 条</span>${button({ action: 'logs-prev', label: '上一页', iconName: 'chevron-left', disabled: page <= 1 })}${button({ action: 'logs-next', label: '下一页', iconName: 'chevron-right', disabled: page >= maxPage })}</div>`) }
  </div>`;
}

async function viewRequest(requestId) {
  const detail = await get(`/requests/${requestId}`);
  const outcome = outcomeInfo(detail.outcome);
  const attempts = detail.attempts || [];
  const cards = attempts.map((attempt) => {
    const attemptOutcome = outcomeInfo(attempt.outcome);
    return `<article class="attempt-card"><div class="attempt-head"><div><strong>${escapeHtml(attempt.channel_name || `尝试 ${attempt.attempt_no}`)}</strong><div class="attempt-meta"><span>尝试 ${number(attempt.attempt_no)}</span><span>HTTP ${number(attempt.status_code)}</span><span>首 Token ${duration(attempt.first_token_ms)}</span><span>TPS ${number(attempt.tps, { maximumFractionDigits: 3 })}</span></div></div>${statusDot(attemptOutcome.label, attemptOutcome.statusClass)}</div><div class="attempt-metrics"><div class="attempt-metric"><span>缓存命中</span><strong>${tokenCount(attempt.cache_read_tokens)}</strong></div><div class="attempt-metric"><span>缓存写入</span><strong>${tokenCount(attempt.cache_write_tokens)}</strong></div><div class="attempt-metric"><span>缓存未命中</span><strong>${tokenCount(attempt.cache_miss_input_tokens)}</strong></div><div class="attempt-metric"><span>输出</span><strong>${tokenCount(attempt.output_tokens)}</strong></div></div></article>`;
  }).join('');
  openDrawer({
    title: '请求详情',
    subtitle: detail.model_id || '',
    body: `<div class="detail-grid"><span>请求 ID</span><strong class="mono">${escapeHtml(detail.id)}</strong><span>模型</span><strong class="mono">${escapeHtml(detail.model_id)}</strong><span>结果</span><strong>${statusDot(outcome.label, outcome.statusClass)}</strong><span>总耗时</span><strong>${escapeHtml(duration(detail.total_duration_ms))}</strong></div><section class="drawer-section"><h3>渠道尝试</h3>${cards || emptyState('无上游尝试', '本次请求没有记录到上游渠道尝试。', 'activity')}</section>`,
  });
}

function openRequestLog(requestId) {
  viewRequest(requestId).catch((error) => toast(error.message || '加载详情失败', 'error'));
}

async function loadSettings(version) {
  const settings = await get('/settings');
  if (version !== state.renderVersion) return;
  state.settings = settings;
  elements.page.innerHTML = renderSettingsPage();
}

function settingNumberField(name, label, value, min, max) {
  return `<div class="field"><label for="setting-${name}">${escapeHtml(label)}</label><input class="number-input" id="setting-${name}" name="${name}" type="number" min="${min}" max="${max}" step="1" value="${number(value)}" required></div>`;
}

function renderSettingsPage() {
  const settings = state.settings || {};
  const keys = state.generatedKeys;
  const keyBlock = `<div class="access-keys" id="access-keys" ${settings.trust_local_network ? 'hidden' : ''}>
      <div class="notice">关闭信任后，保存设置会立即启用密钥校验。留空表示保留现有密钥。</div>
      <div class="form-grid is-two">
        <div class="field"><label for="admin-access-key">管理密钥</label><input class="input" id="admin-access-key" name="admin_access_key" type="password" autocomplete="new-password" placeholder="${escapeAttr(settings.admin_key_hint || '手动输入或使用随机生成')}"></div>
        <div class="field"><label for="gateway-access-key">代理密钥</label><input class="input" id="gateway-access-key" name="gateway_access_key" type="password" autocomplete="new-password" placeholder="${escapeAttr(settings.gateway_key_hint || '手动输入或使用随机生成')}"></div>
      </div>
      ${button({ action: 'generate-keys', label: '随机生成两组密钥', iconName: 'key-round' })}
      ${keys ? `<div class="generated-keys"><div class="generated-key"><span>管理密钥</span><code>${escapeHtml(keys.admin_access_key)}</code>${iconButton({ action: 'copy-key', iconName: 'copy', label: '复制管理密钥', attrs: 'data-key-name="admin_access_key"' })}</div><div class="generated-key"><span>代理密钥</span><code>${escapeHtml(keys.gateway_access_key)}</code>${iconButton({ action: 'copy-key', iconName: 'copy', label: '复制代理密钥', attrs: 'data-key-name="gateway_access_key"' })}</div></div>` : ''}
    </div>`;
  return `<form class="settings-stack" id="settings-form" data-form="settings">
    ${toolbar('访问与运行参数', '配置局域网信任、故障转移与运行时限制', `${button({ action: 'refresh-settings', label: '还原', iconName: 'refresh-cw' })}${button({ action: 'submit-settings', label: '保存设置', iconName: 'check', primary: true })}`)}
    ${panel('局域网访问', '默认信任局域网请求', `<div class="section-body"><div class="form-stack"><div class="switch-row"><div class="switch-copy"><strong>信任局域网访问</strong><p id="trust-description">${settings.trust_local_network ? '代理和管理员界面不要求密钥' : '代理和管理员界面要求对应密钥'}</p></div><input class="switch-control" id="trust-local-network" name="trust_local_network" type="checkbox" aria-label="信任局域网访问" data-trust-toggle ${settings.trust_local_network ? 'checked' : ''}></div>${keyBlock}</div></div>`, 'settings-panel')}
    ${panel('故障转移', '优先级路由与自动熔断', `<div class="section-body"><div class="form-grid">${settingNumberField('failure_threshold', '连续失败阈值', settings.failure_threshold, 1, 20)}${settingNumberField('circuit_open_seconds', '熔断时间（秒）', settings.circuit_open_seconds, 30, 86400)}${settingNumberField('max_failover_attempts', '最大渠道尝试', settings.max_failover_attempts, 1, 20)}</div></div>`, 'settings-panel')}
    ${panel('超时', '上游连接与响应期限', `<div class="section-body"><div class="form-grid">${settingNumberField('connect_timeout_seconds', '连接超时（秒）', settings.connect_timeout_seconds, 1, 120)}${settingNumberField('first_byte_timeout_seconds', '首字节超时（秒）', settings.first_byte_timeout_seconds, 1, 600)}${settingNumberField('first_token_timeout_seconds', '首 Token 超时（秒）', settings.first_token_timeout_seconds, 1, 600)}${settingNumberField('stream_idle_timeout_seconds', '流式空闲超时（秒）', settings.stream_idle_timeout_seconds, 10, 3600)}${settingNumberField('non_stream_total_timeout_seconds', '非流式总超时（秒）', settings.non_stream_total_timeout_seconds, 10, 3600)}</div></div>`, 'settings-panel')}
    ${panel('维护', '周期任务与数据保留', `<div class="section-body"><div class="form-grid is-two">${settingNumberField('model_discovery_interval_hours', '模型探测周期（小时）', settings.model_discovery_interval_hours, 1, 168)}${settingNumberField('log_retention_days', '日志保留（天）', settings.log_retention_days, 1, 365)}</div></div>`, 'settings-panel')}
  </form>`;
}

async function saveSettings(form) {
  const values = new FormData(form);
  const payload = {
    trust_local_network: values.get('trust_local_network') === 'on',
    failure_threshold: Number(values.get('failure_threshold')),
    circuit_open_seconds: Number(values.get('circuit_open_seconds')),
    max_failover_attempts: Number(values.get('max_failover_attempts')),
    connect_timeout_seconds: Number(values.get('connect_timeout_seconds')),
    first_byte_timeout_seconds: Number(values.get('first_byte_timeout_seconds')),
    first_token_timeout_seconds: Number(values.get('first_token_timeout_seconds')),
    stream_idle_timeout_seconds: Number(values.get('stream_idle_timeout_seconds')),
    non_stream_total_timeout_seconds: Number(values.get('non_stream_total_timeout_seconds')),
    model_discovery_interval_hours: Number(values.get('model_discovery_interval_hours')),
    log_retention_days: Number(values.get('log_retention_days')),
  };
  const adminKey = values.get('admin_access_key')?.trim();
  const gatewayKey = values.get('gateway_access_key')?.trim();
  if (adminKey) payload.admin_access_key = adminKey;
  if (gatewayKey) payload.gateway_access_key = gatewayKey;
  await patch('/settings', payload);
  if (!payload.trust_local_network) {
    const effectiveAdminKey = adminKey || state.generatedKeys?.admin_access_key;
    if (effectiveAdminKey) setToken(effectiveAdminKey);
  }
  state.generatedKeys = null;
  toast('运行设置已保存');
  await checkConnection();
  renderPage();
}

async function generateKeys() {
  const result = await post('/settings/access-keys/generate');
  state.generatedKeys = result;
  state.settings.trust_local_network = false;
  elements.page.innerHTML = renderSettingsPage();
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

function openAuthModal() {
  if (elements.modal.open && state.modalMode === 'auth') return;
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
  state.logs.filters = { protocol: values.get('protocol') || '', model_id: values.get('model_id')?.trim() || '', outcome: values.get('outcome') || '' };
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
    if (action === 'submit-auth') return document.getElementById('auth-form')?.requestSubmit();
    if (action === 'submit-settings') return document.getElementById('settings-form')?.requestSubmit();
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
    else if (form.dataset.form === 'caps') await saveCapabilities(form);
    else if (form.dataset.form === 'auth') await saveAuth(form);
    else if (form.dataset.form === 'settings') await saveSettings(form);
    else if (form.dataset.form === 'logs-filter') await submitLogFilter(form);
  } catch (error) {
    toast(error.message || '保存失败', 'error');
  }
}

function handleChange(event) {
  const target = event.target;
  if (target.matches('[data-channel-toggle]')) toggleChannel(target.dataset.channelId, target.checked);
  if (target.matches('[data-candidate-selected]')) {
    const row = target.closest('.candidate-row');
    if (target.checked) elements.drawerBody.querySelector('[data-candidate-list]').append(row);
    updateCandidateOrder();
  }
  if (target.matches('[data-trust-toggle]')) {
    const keyArea = document.getElementById('access-keys');
    const description = document.getElementById('trust-description');
    if (keyArea) keyArea.hidden = target.checked;
    if (description) description.textContent = target.checked ? '代理和管理员界面不要求密钥' : '代理和管理员界面要求对应密钥';
  }
}

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
  if (state.confirmResolve) resolveConfirmation(false);
});
elements.modal.addEventListener('close', () => { state.modalMode = ''; });

checkConnection();
renderPage();
