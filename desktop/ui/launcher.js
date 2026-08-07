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
  render(await invoke('launcher_state'));
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
    // The backend is the source of truth (OS autostart state, persisted config).
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

await refreshSettings();
await refresh();
setInterval(refresh, 1500);
