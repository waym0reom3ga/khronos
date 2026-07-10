use chrono::DateTime;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// State of a workflow execution.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkflowState {
    #[default]
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

/// Definition of a workflow composed of activity steps.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkflowDefinition {
    /// Name identifying this workflow definition.
    pub name: String,
    /// Ordered list of activities that make up the workflow.
    pub steps: Vec<ActivityStep>,
}

/// A single step within a workflow definition.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActivityStep {
    /// Name of the activity to execute.
    pub activity_name: String,
    /// Template for arguments passed to this step (supports interpolation).
    pub args_template: std::collections::BTreeMap<String, String>,
    /// Retry configuration for this step.
    pub retry_policy: crate::activity::RetryPolicy,
    /// Maximum time allowed for this step to complete.
    pub timeout_secs: u64,
    /// Maximum time between heartbeats before the step is considered failed.
    pub heartbeat_timeout_secs: Option<u64>,
}

/// A concrete instance of a workflow execution.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkflowInstance {
    /// Unique run identifier.
    pub run_id: Uuid,
    /// Stable workflow ID (deduplication key).
    pub workflow_id: String,
    /// Name of the workflow definition being executed.
    pub name: String,
    /// Task queue this workflow is assigned to.
    pub task_queue: String,
    /// Current state of execution.
    pub state: WorkflowState,
    /// Input arguments as JSON.
    pub args_json: serde_json::Value,
    /// Result value after completion (if any).
    pub result_json: Option<serde_json::Value>,
    /// Timeout configuration for this run.
    pub timeouts: crate::schedule::Timeouts,
    /// When execution started.
    pub started_at: Option<DateTime<chrono::Utc>>,
    /// When execution completed or failed.
    pub completed_at: Option<DateTime<chrono::Utc>>,
    /// Namespace this workflow belongs to.
    pub namespace: String,
}

/// A step within a running workflow instance.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkflowStepInstance {
    /// Unique step identifier.
    pub id: Uuid,
    /// Parent workflow run ID.
    pub workflow_run_id: Uuid,
    /// Index of this step in the workflow definition.
    pub step_index: usize,
    /// Name of the activity for this step.
    pub activity_name: String,
    /// Resolved arguments as JSON.
    pub args_json: serde_json::Value,
    /// Retry configuration inherited from the step definition.
    pub retry_policy: crate::activity::RetryPolicy,
    /// Maximum time allowed for this step.
    pub timeout_secs: u64,
    /// Heartbeat deadline in seconds.
    pub heartbeat_timeout_secs: Option<u64>,
    /// Current state of this step.
    pub state: WorkflowState,
    /// Current retry attempt number (1-based).
    pub attempt: u32,
    /// Result after completion (if any).
    pub result_json: Option<serde_json::Value>,
    /// When the next retry is scheduled.
    pub next_retry_at: Option<DateTime<chrono::Utc>>,
}
