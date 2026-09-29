/**
 * 桌面启动器窗口脚本：端口设置、开关项与一键打开管理端。
 *
 * 职责：渲染端口与「开机自启 / 启动到托盘」等开关，提交时经 Tauri
 * `invoke` 调用后端命令。
 * 边界：只做界面与命令调用；运行状态以后端返回为准，不在前端持久化。
 * 关键不变量：刷新与初始化失败只记录到 console，绝不向外抛出；定时刷新
 * 句柄在 `beforeunload` 时清理。
 */
const invoke = window.__TAURI_INTERNALS__.invoke;

const elements = {
  dot: document.querySelector('#status-dot'),
  label: document.querySelector('#status-label'),
  detail: document.querySelector('#status-detail'),
  error: document.querySelector('#status-error'),
  port: document.querySelector('#port'),
  hint: document.querySelector('#port-hint'),
  open: document.querySelector('#open-dashboard'),
  form: document.querySelector('#port-form'),
  autostart: document.querySelector('#autostart'),
  startToTray: document.querySelector('#start-to-tray'),
  settingsHint: document.querySelector('#settings-hint'),
  minimize: document.querySelector('#minimize-btn'),
  close: document.querySelector('#close-btn'),
};

let savedPort = '';
let portDirty = false;

function render(state) {
  const nextSavedPort = String(state.port);
  if (!portDirty) elements.port.value = nextSavedPort;
  savedPort = nextSavedPort;
  elements.dot.className = `status-dot ${state.running ? 'running' : state.error ? 'failed' : ''}`;
  elements.label.textContent = state.running ? '运行中' : state.error ? '启动失败' : '正在启动';
  elements.detail.textContent = state.url;
  elements.error.textContent = state.error || '';
  elements.error.hidden = !state.error;
  elements.open.disabled = !state.running;
}

async function refresh() {
  // 刷新失败只记录到 console，不得向外抛出，避免定时刷新把整个模块带崩。
  try {
    render(await invoke('launcher_state'));
  } catch (error) {
    console.error('launcher refresh failed:', error);
  }
}

async function withBusy(button, task) {
  button.disabled = true;
  try {
    await task();
    elements.hint.classList.remove('error');
    await refresh();
  } catch (error) {
    elements.hint.textContent = String(error);
    elements.hint.classList.add('error');
  } finally {
    button.disabled = false;
  }
}

elements.port.addEventListener('input', () => {
  portDirty = elements.port.value !== savedPort;
});

elements.form.addEventListener('submit', async (event) => {
  event.preventDefault();
  const port = Number(elements.port.value);
  await withBusy(event.submitter, async () => {
    await invoke('set_port', { port });
    portDirty = elements.port.value !== String(port);
  });
});

elements.open.addEventListener('click', () => withBusy(elements.open, () => invoke('open_dashboard')));

elements.minimize.addEventListener('click', () => invoke('minimize_window').catch(() => {}));
elements.close.addEventListener('click', () => invoke('close_to_tray').catch(() => {}));

async function refreshSettings() {
  const settings = await invoke('launcher_settings');
  elements.autostart.checked = settings.autostart_enabled;
  elements.startToTray.checked = settings.start_to_tray;
}

async function toggleSetting(invokeName, checkbox) {
  checkbox.disabled = true;
  try {
    await invoke(invokeName, { enabled: checkbox.checked });
    // 以后端为准（操作系统的自启动状态与持久化配置）。
    await refreshSettings();
    elements.settingsHint.hidden = true;
  } catch (error) {
    elements.settingsHint.textContent = String(error);
    elements.settingsHint.hidden = false;
    await refreshSettings();
  } finally {
    checkbox.disabled = false;
  }
}

elements.autostart.addEventListener('change', () =>
  toggleSetting('set_autostart', elements.autostart),
);
elements.startToTray.addEventListener('change', () =>
  toggleSetting('set_start_to_tray', elements.startToTray),
);

// 顶层初始化：任何失败只记录到 console，绝不让整个模块因异常而白屏。
try {
  await refreshSettings();
  await refresh();
} catch (error) {
  console.error('launcher init failed:', error);
}

// 定时刷新：窗口不可见时跳过；句柄在 beforeunload 时清理。
const refreshTimer = setInterval(() => {
  if (document.hidden) return;
  refresh();
}, 1500);
window.addEventListener('beforeunload', () => clearInterval(refreshTimer));
