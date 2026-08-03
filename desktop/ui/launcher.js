const invoke = window.__TAURI_INTERNALS__.invoke;

const elements = {
  dot: document.querySelector('#status-dot'),
  label: document.querySelector('#status-label'),
  detail: document.querySelector('#status-detail'),
  port: document.querySelector('#port'),
  hint: document.querySelector('#port-hint'),
  toggle: document.querySelector('#toggle'),
  open: document.querySelector('#open-dashboard'),
  form: document.querySelector('#port-form'),
  version: document.querySelector('#version'),
};

function render(state) {
  elements.port.value = state.port;
  elements.version.textContent = state.version;
  elements.dot.className = `status-dot ${state.running ? 'running' : state.error ? 'failed' : ''}`;
  elements.label.textContent = state.running ? '网关运行中' : state.error ? '启动失败' : '网关已停止';
  elements.detail.textContent = state.error || (state.running ? state.url : '点击启动服务恢复本地网关');
  elements.toggle.textContent = state.running ? '停止服务' : '启动服务';
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

elements.form.addEventListener('submit', (event) => {
  event.preventDefault();
  const port = Number(elements.port.value);
  withBusy(event.submitter, () => invoke('set_port', { port }));
});

elements.toggle.addEventListener('click', () => withBusy(elements.toggle, () => invoke('toggle_server')));
elements.open.addEventListener('click', () => withBusy(elements.open, () => invoke('open_dashboard')));

await refresh();
setInterval(refresh, 1500);
