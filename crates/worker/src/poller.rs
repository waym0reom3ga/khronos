//! Activity poller — long-polls the server and sends tasks via mpsc channel.

use std::time::Duration;
use tracing::{info, warn};

use crate::client::GrpcClient;

/// Polls the server for activity tasks and forwards them to a channel receiver.
pub struct ActivityPoller {
    client: GrpcClient,
    task_queue: String,
}

impl ActivityPoller {
    pub fn new(client: GrpcClient, task_queue: &str) -> Self {
        Self {
            client,
            task_queue: task_queue.to_string(),
        }
    }

    /// Start polling in a background task. Returns a receiver for tasks.
    pub fn spawn(self) -> tokio::sync::mpsc::Receiver<crate::ActivityTask> {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        
        tokio::spawn(async move {
            loop {
                match self.client.poll_activity(self.task_queue.clone(), vec![]).await {
                    Ok(Some(task)) => {
                        info!(activity_id = %task.activity_id, name = %task.name, "polled activity");
                        if tx.send(task).await.is_err() {
                            info!("receiver dropped — shutting down poller");
                            break;
                        }
                    }
                    Ok(None) => {
                        // No task available — back off briefly before retrying.
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    Err(e) => {
                        warn!(error = %e, "poll error — retrying in 1s");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });

        rx
    }
}
