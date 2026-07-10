use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// State of an activity execution.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActivityState {
    #[default]
    Pending,
    Running,
    Completed,
    Failed,
    Retried,
}

/// Retry configuration for activities and workflow steps.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Maximum number of attempts (including the first). Defaults to 1.
    pub maximum_attempts: u32,
    /// Initial retry interval in seconds. Defaults to 1.0.
    pub initial_interval_secs: f64,
}

impl RetryPolicy {
    /// Create a new retry policy with defaults (1 attempt, 1.0s interval).
    pub fn new() -> Self {
        Self::default()
    }
}

/// Result of an activity execution.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ActivityResult {
    /// Activity completed successfully with a JSON result.
    Success(serde_json::Value),
    /// Activity failed with an error message.
    Failure(String),
}

/// A task representing a single activity to be executed by a worker.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActivityTask {
    /// Unique activity identifier.
    pub activity_id: Uuid,
    /// Parent step instance ID.
    pub step_id: Uuid,
    /// Parent workflow run ID.
    pub workflow_run_id: Uuid,
    /// Name of the activity type.
    pub name: String,
    /// Arguments for this activity invocation.
    pub args: std::collections::BTreeMap<String, String>,
    /// Retry configuration.
    pub retry_policy: RetryPolicy,
    /// Maximum time between heartbeats before timeout (seconds).
    pub heartbeat_timeout_secs: u64,
    /// Maximum wall-clock time from start to close (seconds).
    pub start_to_close_timeout_secs: u64,
}
