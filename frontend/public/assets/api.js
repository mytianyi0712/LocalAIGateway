/**
 * 管理 API 请求层：访问令牌存储与 fetch 封装。
 *
 * 职责：拼接 `/api/admin/v1` 前缀、注入 `Authorization` 头，统一解析 JSON
 * 响应并把非 2xx 转成带 `status`/`code` 的 Error。
 * 边界：只处理 HTTP 层；401 的具体处置（恢复模式不弹窗）交由 app.js 注册
 * 的处理器决定，避免模块循环依赖。
 * 关键不变量：令牌存于 `sessionStorage`；除 204 无响应体外，响应体一律按
 * JSON 解析，解析失败按可读错误抛出而非静默返回 null。
 */


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
    // 恢复模式本就没有有效密钥：此处 401 表示恢复挑战校验失败，而不是
    // 缺少登录。具体如何处置交给注册的处理器（它才知道恢复模式是否开启）。
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
