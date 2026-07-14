//! CLI entry point for Khronos server.

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "khronos", about = "Lightweight workflow orchestration server")]
struct Cli {
    /// Server port
    #[arg(short, long, default_value_t = 7233)]
    port: u16,

    /// Data directory for SQLite database
    #[arg(short, long, default_value = "./data")]
    data_dir: String,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cli = Cli::parse();

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(format!("khronos={},{}", cli.log_level, "tonic=warn"))
        .init();

    // Create data directory
    std::fs::create_dir_all(&cli.data_dir)?;

    let db_path = format!("{}/khronos.db", cli.data_dir);

    // Initialize database
    let db = khronos_db::Database::new(&db_path)?;

    // Start server (spawns scheduler + engine internally)
    let addr = format!("0.0.0.0:{}", cli.port).parse()?;

    tracing::info!("Starting Khronos server on {}", addr);

    khronos_server::grpc::run_server(addr, db).await?;

    Ok(())
}
