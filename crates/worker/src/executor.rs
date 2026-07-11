//! Task executor — receives tasks, runs them concurrently with semaphore gating.

use std::sync::Arc;
use tracing::{error, info, warn};

use crate::client::GrpcClient;
use crate::registry::ActivityRegistry;

/// Executes activity tasks concurrently, gated by a semaphore.
pub struct TaskExecutor {
    client: GrpcClient,
    registry: Arc<ActivityRegistry>,
    semaphore: Arc<tokio::sync::Semaphore>,
}

impl TaskExecutor {
    pub fn new(client: GrpcClient, registry: ActivityRegistry, max_concurrent: usize) -> Self {
        Self {
            client,
            registry: Arc::new(registry),
            semaphore: Arc::new(tokio::sync::Semaphore::new(max_concurrent)),
        }
    }

    /// Submit a task for execution. Spawns an async task that runs the handler.
    pub async fn execute(&self, task: crate::ActivityTask) {
        // Clone everything we need — all owned types, no borrows of self.
        let permit = self.semaphore.clone().acquire_owned().await;
        let client = self.client.clone();
        let registry = self.registry.clone();
        let activity_id = task.activity_id.clone();
        let activity_name = task.name.clone();

        tokio::spawn(async move {
            info!(activity_id = %activity_id, name = %activity_name, "executing activity");

            // Acquire the permit (blocks if at capacity).
            let permit = match permit {
                Ok(p) => p,
                Err(e) => {
                    error!(error = %e, "failed to acquire semaphore — task dropped");
                    return;
                }
            };

            // Look up handler inside the spawned task.
            let result = if let Some(handler) = registry.get(&activity_name) {
                handler.execute(&task).await
            } else {
                warn!(activity = %activity_name, "no handler registered — reporting failure");
                let _ = client.report_failure(
                    activity_id.clone(),
                    format!("No handler registered for activity: {}", activity_name),
                ).await;
                return;
            };

            match result {
                Ok(output) => {
                    info!(activity_id = %activity_id, "completed");
                    let result_json = serde_json::json!({
                        "stdout": output.trim(),
                    }).to_string();
                    if let Err(e) = client.report_result(activity_id.clone(), result_json).await {
                        error!(activity_id = %activity_id, error = %e, "failed to report result");
                    }
                }
                Err(err_msg) => {
                    error!(activity_id = %activity_id, error = %err_msg, "failed");
                    if let Err(e) = client.report_failure(activity_id.clone(), err_msg).await {
                        error!(activity_id = %activity_id, error = %e, "failed to report failure");
                    }
                }
            }

            drop(permit);
        });
    }
}
