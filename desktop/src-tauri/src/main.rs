fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    if let Err(error) = local_ai_gateway::run_desktop() {
        eprintln!("Local AI Gateway failed: {error:#}");
        std::process::exit(1);
    }
}
