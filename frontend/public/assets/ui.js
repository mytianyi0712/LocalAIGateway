// UI 渲染助手（P2-4 模块拆分）：无状态 DOM 片段构造与交互原语。
// 由 app.js 以 ES module 方式导入。

// UI 交互状态（模块内自持，app.js 经 getModalMode 只读）。
const uiState = { modalMode: '', confirmResolve: null };
export function getModalMode() {
  return uiState.modalMode;
}



export const elements = {
  page: document.getElementById('page-content'),
  title: document.getElementById('page-title'),
  description: document.getElementById('page-description'),
  sideStatus: document.getElementById('side-status'),
  sideMode: document.getElementById('side-mode'),
  topStatus: document.getElementById('top-status'),
  topEndpoint: document.querySelector('.topbar-endpoint'),
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

export function icon(name, className = '') {
  return `<svg class="icon ${className}" aria-hidden="true"><use href="#i-${name}"></use></svg>`;
}

export function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

export function escapeAttr(value) {
  return escapeHtml(value);
}

export function number(value, options = {}) {
  if (value === null || value === undefined || value === '') return '-';
  return Number(value).toLocaleString('zh-CN', { maximumFractionDigits: 2, ...options });
}

export function tokenCount(value) {
  if (value === null || value === undefined || value === '') return '-';
  const amount = Number(value);
  if (!Number.isFinite(amount)) return '-';
  if (amount >= 1_000_000_000_000) return `${(amount / 1_000_000_000_000).toFixed(2)}T`;
  if (amount >= 1_000_000_000) return `${(amount / 1_000_000_000).toFixed(2)}B`;
  if (amount >= 1_000_000) return `${(amount / 1_000_000).toFixed(2)}M`;
  if (amount >= 1_000) return `${(amount / 1_000).toFixed(2)}K`;
  return number(amount);
}

export function percent(value) {
  if (value === null || value === undefined) return '-';
  return `${(Number(value) * 100).toFixed(1)}%`;
}

export function parseStandardTime(value) {
  if (!value) return null;
  const raw = String(value).trim().replace(' ', 'T');
  const normalized = /(?:Z|[+-]\d{2}:?\d{2})$/i.test(raw) ? raw : `${raw}Z`;
  const date = new Date(normalized);
  return Number.isNaN(date.getTime()) ? null : date;
}

export function formatTime(value) {
  const date = parseStandardTime(value);
  return date ? date.toLocaleString('zh-CN', { hour12: false }) : '-';
}

export function formatLogTime(value) {
  const date = parseStandardTime(value);
  if (!date) return '-';
  const pad = (item) => String(item).padStart(2, '0');
  return `${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

export function duration(value) {
  return value === null || value === undefined ? '-' : `${number(value)} ms`;
}

export function protocols(items = []) {
  return `<div class="protocol-list">${items.map((item) => `<span class="protocol">${escapeHtml(item)}</span>`).join('')}</div>`;
}

export function responseChannelTags(items = []) {
  if (!items.length) return '<span class="log-route-empty">-</span>';
  const routeTitle = items.join(' -> ');
  return `<div class="protocol-list log-route-list" title="${escapeAttr(routeTitle)}">${items.map((item) => `<span class="protocol log-route-tag">${escapeHtml(item)}</span>`).join('')}</div>`;
}

export function healthInfo(channel) {
  const health = channel.health || {};
  const raw = channel.manual_enabled === false ? 'disabled' : (health.state || 'active');
  const labels = { active: '正常', open: '熔断', half_open: '探测中', disabled: '已禁用' };
  const statusClass = raw === 'active' ? 'is-success' : raw === 'half_open' ? 'is-warning' : raw === 'disabled' ? 'is-muted' : 'is-danger';
  return { label: labels[raw] || raw, statusClass, raw };
}

export function outcomeInfo(outcome) {
  const labels = { success: '成功', http_error: '上游错误', transport_error: '传输错误', upstream_error: '上游错误', gateway_error: '网关错误', stream_interrupted: '流中断', cancelled: '已取消', pending: '等待中' };
  const statusClass = outcome === 'success' ? 'is-success' : outcome === 'pending' ? 'is-warning' : 'is-danger';
  return { label: labels[outcome] || outcome || '-', statusClass };
}

export function statusDot(label, statusClass) {
  return `<span class="status-dot ${statusClass}"><i></i>${escapeHtml(label)}</span>`;
}

export function button({ action, label, iconName, primary = false, danger = false, disabled = false, attrs = '', type = 'button' }) {
  const classes = ['button'];
  if (primary) classes.push('button-primary');
  if (danger) classes.push('button-danger');
  return `<button type="${type}" class="${classes.join(' ')}" data-action="${escapeAttr(action)}" ${disabled ? 'disabled' : ''} ${attrs}>${iconName ? icon(iconName) : ''}<span>${escapeHtml(label)}</span></button>`;
}

export function iconButton({ action, iconName, label, danger = false, attrs = '', disabled = false }) {
  return `<button type="button" class="icon-button ${danger ? 'button-danger' : ''}" data-action="${escapeAttr(action)}" aria-label="${escapeAttr(label)}" title="${escapeAttr(label)}" ${disabled ? 'disabled' : ''} ${attrs}>${icon(iconName)}</button>`;
}

export function toolbar(title, note, actions) {
  return `<div class="page-toolbar"><div class="page-toolbar-copy"><h2>${escapeHtml(title)}</h2>${note ? `<p class="toolbar-note">${escapeHtml(note)}</p>` : ''}</div><div class="action-row">${actions}</div></div>`;
}

export function panel(title, note, body, className = '') {
  return `<section class="section-panel ${className}"><header class="section-header"><h2>${escapeHtml(title)}</h2>${note ? `<span class="section-header-note">${escapeHtml(note)}</span>` : ''}</header>${body}</section>`;
}

export function emptyState(title, detail, iconName = 'server') {
  return `<div class="empty-state"><div class="empty-state-inner">${icon(iconName)}<h3>${escapeHtml(title)}</h3><p>${escapeHtml(detail)}</p></div></div>`;
}

export function skeleton() {
  return `<div class="skeleton-stack"><div class="skeleton-toolbar"></div><div class="skeleton-metrics"><div class="skeleton-card"></div><div class="skeleton-card"></div><div class="skeleton-card"></div><div class="skeleton-card"></div></div><div class="skeleton-panel"></div></div>`;
}

export function toast(message, type = 'success') {
  const node = document.createElement('div');
  node.className = `toast ${type === 'error' ? 'is-error' : type === 'warning' ? 'is-warning' : ''}`;
  node.innerHTML = `${icon(type === 'error' ? 'x' : type === 'warning' ? 'activity' : 'check')}<span>${escapeHtml(message)}</span>`;
  elements.toasts.append(node);
  window.setTimeout(() => node.remove(), 4200);
}

export function openModal({ title, body, footer = '', mode = '' }) {
  if (elements.modal.open) elements.modal.close();
  uiState.modalMode = mode;
  elements.modalTitle.textContent = title;
  elements.modalBody.innerHTML = body;
  elements.modalFooter.innerHTML = footer;
  elements.modal.showModal();
}

export function closeModal() {
  if (elements.modal.open) elements.modal.close();
}

export function openDrawer({ title, subtitle = '', body, footer = '' }) {
  elements.drawerTitle.textContent = title;
  elements.drawerSubtitle.textContent = subtitle;
  elements.drawerSubtitle.hidden = !subtitle;
  elements.drawerBody.innerHTML = body;
  elements.drawerFooter.innerHTML = footer;
  elements.drawer.classList.add('is-open');
  elements.drawer.setAttribute('aria-hidden', 'false');
}

export function closeDrawer() {
  elements.drawer.classList.remove('is-open');
  elements.drawer.setAttribute('aria-hidden', 'true');
  window.dispatchEvent(new CustomEvent('drawer-closed'));
}

export function confirmAction({ title, message, confirmLabel = '确认', danger = false }) {
  return new Promise((resolve) => {
    uiState.confirmResolve = resolve;
    openModal({
      title,
      mode: 'confirm',
      body: `<p class="subtle-text">${escapeHtml(message)}</p>`,
      footer: `${button({ action: 'confirm-cancel', label: '取消' })}${button({ action: 'confirm-accept', label: confirmLabel, primary: true, danger, iconName: danger ? 'trash-2' : 'check' })}`,
    });
  });
}

export function resolveConfirmation(value) {
  const resolve = uiState.confirmResolve;
  uiState.confirmResolve = null;
  closeModal();
  if (resolve) resolve(value);
}

export function pageError(error) {
  return `<div class="page-stack">${panel('无法加载页面', '', emptyState('请求管理 API 失败', error?.message || '请检查网关服务与访问权限。', 'activity'))}</div>`;
}

export function metric(label, value, foot, className = '', iconName = 'activity') {
  return `<article class="metric-card ${className}"><div class="metric-heading"><span class="metric-icon">${icon(iconName)}</span><span class="metric-label">${escapeHtml(label)}</span></div><strong class="metric-value">${value}</strong><p class="metric-foot">${escapeHtml(foot)}</p></article>`;
}

export function tokenMetric(label, value, className = '', iconName = 'database') {
  return `<article class="token-metric ${className}"><div class="token-metric-heading"><span class="token-metric-icon">${icon(iconName)}</span><span>${escapeHtml(label)}</span></div><strong>${value}</strong></article>`;
}

export function settingNumberField(name, label, value, min, max) {
  return `<div class="field"><label for="setting-${name}">${escapeHtml(label)}</label><input class="number-input" id="setting-${name}" name="${name}" type="number" min="${min}" max="${max}" step="1" value="${number(value)}" required></div>`;
}
