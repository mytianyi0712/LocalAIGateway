// 管理 API 请求层（P2-4 模块拆分）：token 存储与 fetch 封装。


export const API_ROOT = '/api/admin/v1';

export const TOKEN_KEY = 'ai-gateway-admin-token';

export function getToken() {
  return sessionStorage.getItem(TOKEN_KEY) || '';
}

export function setToken(value) {
  if (value) sessionStorage.setItem(TOKEN_KEY, value);
  else sessionStorage.removeItem(TOKEN_KEY);
}

export async function api(path, options = {}) {
  const headers = new Headers(options.headers || {});
  const token = getToken();
  if (token) headers.set('Authorization', `Bearer ${token}`);
  if (options.body && !(options.body instanceof FormData)) headers.set('Content-Type', 'application/json');
  const response = await fetch(`${API_ROOT}${path}`, { ...options, headers });
  if (response.status === 204) return null;
  let body = null;
  try {
    body = await response.json();
  } catch {
    // 2xx 却无法解析为 JSON：多半是旧后端进程缺路由，请求被 SPA 回退成
    // index.html。给出可读错误，而不是让上层报 null.items 这类 TypeError。
    if (!response.ok) throw new Error(`请求失败 (${response.status})`);
    throw new Error(`管理 API 返回了非 JSON 响应（HTTP ${response.status}），请确认网关已升级并重启`);
  }
  if (!response.ok) {
    const error = new Error(body?.message || body?.detail || body?.error?.message || `请求失败 (${response.status})`);
    error.status = response.status;
    error.code = body?.code;
    error.body = body;
    // Recovery mode runs without a valid key: a 401 there is the recovery
    // challenge failing, not a missing login. The app decides via the
    // registered handler (it knows whether recovery mode is active).
    if (response.status === 401) unauthorizedHandler?.();
    throw error;
  }
  return body;
}

export const get = (path) => api(path);

export const post = (path, body = {}) => api(path, { method: 'POST', body: JSON.stringify(body) });

export const put = (path, body = {}) => api(path, { method: 'PUT', body: JSON.stringify(body) });

export const patch = (path, body = {}, options = {}) =>
  api(path, { method: 'PATCH', body: JSON.stringify(body), ...options });

export const remove = (path) => api(path, { method: 'DELETE' });

// 401 时由 app.js 决定行为（恢复模式不弹窗），避免模块循环依赖。
let unauthorizedHandler = null;
export function setUnauthorizedHandler(fn) {
  unauthorizedHandler = fn;
}
