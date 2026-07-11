//! Khronos Worker — polls for activity tasks and executes them.
//!
//! LGPL v2.1 only. See https://www.gnu.org/licenses/old-licenses/lgpl-2.1.html

// Include generated protobuf types at crate root.
include!(concat!(env!("OUT_DIR"), "/khronos.rs"));

mod client;
mod executor;
mod handler;
mod poller;
mod registry;
mod worker;

use clap::Parser;
use tracing::info;
use worker::{Worker, WorkerConfig};

/// Khronos activity worker — executes scheduled workflow activities.
#[derive(Parser, Debug)]
#[command(name = "khronos-worker", version, about)]
struct Args {
    /// Server gRPC endpoint (host:port)
    #[arg(short, long, default_value = "127.0.0.1:50053")]
    server: String,

    /// Task queue to poll from
    #[arg(short, long, default_value = "default")]
    queue: String,

    /// Maximum concurrent activity executions
    #[arg(short, long, default_value_t = 4)]
    max_concurrent: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize logging.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,khronos_worker=debug".into()),
        )
        .init();

    let args = Args::parse();
    info!(?args, "khronos worker starting");

    // Build worker config.
    let config = WorkerConfig {
        server_url: args.server,
        task_queue: args.queue,
        max_concurrent: args.max_concurrent,
    };

    // Create and run the worker (handles shutdown signals internally).
    info!("press Ctrl+C to stop");
    Worker::new(config).run().await
}
