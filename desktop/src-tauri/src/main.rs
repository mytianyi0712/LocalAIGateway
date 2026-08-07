// The desktop bin is a GUI app: without this, Windows builds the exe as a
// console-subsystem (CUI) binary, opening a cmd window on launch. Closing
// that console kills the whole process, tray included. Debug builds keep the
// console for log output; the headless bin is unaffected (separate main).
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    if std::env::args().any(|arg| arg == "--version" || arg == "-V") {
        println!("Local AI Gateway {}", env!("CARGO_PKG_VERSION"));
        return;
    }
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    if let Err(error) = local_ai_gateway::run_desktop() {
        eprintln!("Local AI Gateway failed: {error:#}");
        std::process::exit(1);
    }
}
