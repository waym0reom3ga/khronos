//! Worker orchestrator — coordinates poller and executor in a run loop.

use tracing::info;

use crate::client::GrpcClient;
use crate::executor::TaskExecutor;
use crate::handler::{DummyHandler, PythonHandler, ScriptHandler};
use crate::poller::ActivityPoller;
use crate::registry::ActivityRegistry;
use std::path::PathBuf;

/// Worker configuration.
pub struct WorkerConfig {
    pub server_url: String,
    pub task_queue: String,
    pub max_concurrent: usize,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            server_url: "127.0.0.1:50053".to_string(),
            task_queue: "default".to_string(),
            max_concurrent: 4,
        }
    }
}

/// The Worker — connects all components and runs the event loop.
pub struct Worker {
    config: WorkerConfig,
}

impl Worker {
    pub fn new(config: WorkerConfig) -> Self {
        Self { config }
    }

    /// Build and run the worker. Handles shutdown signals (SIGINT/SIGTERM).
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!(server = %self.config.server_url, queue = %self.config.task_queue, "starting worker");

        // 1. Connect to server via gRPC.
        let client = GrpcClient::connect(&self.config.server_url).await?;
        info!("connected to khronos server");

        // 2. Build activity registry with default handlers.
        let mut registry = ActivityRegistry::new();
        register_default_handlers(&mut registry);
        info!(count = registry.len(), "handlers registered");

        // 3. Create executor with concurrency control.
        let executor = TaskExecutor::new(
            client.clone(),
            registry,
            self.config.max_concurrent,
        );

        // 4. Start the poller — it returns a channel receiver.
        let mut task_rx = ActivityPoller::new(client, &self.config.task_queue).spawn();

        info!("worker running — polling for activities");

        // Set up shutdown signals.
        let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

        // 5. Main event loop: receive tasks from poller → send to executor.
        loop {
            tokio::select! {
                // Shutdown signal (SIGINT or SIGTERM).
                _ = sigint.recv() => {
                    info!("received SIGINT — shutting down");
                    break;
                }
                _ = sigterm.recv() => {
                    info!("received SIGTERM — shutting down");
                    break;
                }

                // New activity task from the poller.
                task = task_rx.recv() => {
                    match task {
                        Some(t) => {
                            info!(activity_id = %t.activity_id, name = %t.name, "received task — dispatching");
                            executor.execute(t).await;
                        }
                        None => {
                            // Channel closed (poller crashed or dropped).
                            info!("poller channel closed — exiting");
                            break;
                        }
                    }
                }
            }
        }

        info!("worker stopped");
        Ok(())
    }
}

/// Register all default activity handlers.
fn register_default_handlers(registry: &mut ActivityRegistry) {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/home/waymore".to_string());
    let workdir = PathBuf::from(&home);

    // Memory condenser — Python script
    registry.register(
        "lycus-memory-condenser",
        PythonHandler {
            script_path: PathBuf::from(format!("{}/scripts/memory_condenser.py", home)),
            workdir: workdir.clone(),
        },
    );

    // Cron failure notifier — shell script (may not exist yet)
    registry.register(
        "lycus-cron-notifier",
        ScriptHandler {
            script_path: PathBuf::from(format!("{}/scripts/cron_notifier.sh", home)),
            workdir: workdir.clone(),
        },
    );

    // SearXNG health check — shell script (may not exist yet)
    registry.register(
        "lycus-searxng-healthcheck",
        ScriptHandler {
            script_path: PathBuf::from(format!("{}/scripts/searxng_health_check.sh", home)),
            workdir: workdir.clone(),
        },
    );

    // SearXNG 429 reactive check
    registry.register(
        "lycus-searxng-error-reactive",
        ScriptHandler {
            script_path: PathBuf::from(format!("{}/scripts/searxng_error_check.sh", home)),
            workdir: workdir.clone(),
        },
    );

    // Math pipeline — arxiv factory (stub)
    registry.register(
        "lycus-arxiv-factory",
        ScriptHandler {
            script_path: PathBuf::from(format!("{}/scripts/arxiv_factory.sh", home)),
            workdir: workdir.clone(),
        },
    );

    // Math pipeline — mathnexus extract (Python)
    registry.register(
        "lycus-mathnexus-extract",
        PythonHandler {
            script_path: PathBuf::from(format!("{}/Documents/AI_researched_math/mathNEXUS/extract_mindmap.py", home)),
            workdir: workdir.clone(),
        },
    );

    // Math pipeline — mathlab agent (Python)
    registry.register(
        "lycus-mathlab-agent",
        PythonHandler {
            script_path: PathBuf::from(format!("{}/Documents/AI_researched_math/mathLaboratory/laboratory_agent.py", home)),
            workdir: workdir.clone(),
        },
    );

    // Dummy handler — always succeeds, for testing
    registry.register("dummy-activity", DummyHandler);
}
