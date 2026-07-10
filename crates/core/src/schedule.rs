use std::collections::BTreeMap;
use std::fmt;

use chrono::DateTime;
use serde::{Deserialize, Serialize};

/// Specification for how a schedule should trigger.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ScheduleSpec {
    /// Cron expression(s) that define when the schedule fires.
    Cron(Vec<String>),
    /// Fixed interval between triggers.
    Interval(std::time::Duration),
}

impl fmt::Display for ScheduleSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScheduleSpec::Cron(expressions) => write!(f, "Cron({})", expressions.join(", ")),
            ScheduleSpec::Interval(duration) => write!(f, "Interval({:?})", duration),
        }
    }
}

/// Policy for handling overlapping workflow executions.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OverlapPolicy {
    /// Skip the new execution if one is already running.
    Skip,
    /// Buffer up to N pending executions while one runs.
    Buffer(usize),
    /// Terminate any existing execution before starting a new one.
    TerminateExisting,
}

impl fmt::Display for OverlapPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OverlapPolicy::Skip => write!(f, "Skip"),
            OverlapPolicy::Buffer(n) => write!(f, "Buffer({})", n),
            OverlapPolicy::TerminateExisting => write!(f, "TerminateExisting"),
        }
    }
}

/// Timeout configuration for workflow execution.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Timeouts {
    /// Maximum time the entire workflow execution can take (including retries).
    pub execution_timeout_secs: Option<u64>,
    /// Maximum time for a single run of the workflow.
    pub run_timeout_secs: Option<u64>,
    /// Maximum time for an individual task within the workflow.
    pub task_timeout_secs: Option<u64>,
}

/// Action to perform when a schedule fires.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkflowAction {
    /// Name of the workflow to invoke.
    pub workflow_name: String,
    /// Arguments passed to the workflow.
    pub args: BTreeMap<String, String>,
    /// Task queue where the workflow should be dispatched.
    pub task_queue: String,
    /// Unique identifier for this action.
    pub id: String,
    /// Timeout configuration.
    pub timeouts: Timeouts,
}

/// A complete schedule definition.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Schedule {
    /// Unique schedule identifier.
    pub schedule_id: String,
    /// Namespace this schedule belongs to.
    pub namespace: String,
    /// When the schedule should fire.
    pub spec: ScheduleSpec,
    /// What happens when the schedule fires.
    pub action: WorkflowAction,
    /// Policy for overlapping executions.
    pub policy: OverlapPolicy,
    /// When this schedule was created.
    pub created_at: DateTime<chrono::Utc>,
    /// Last time this schedule was updated.
    pub updated_at: DateTime<chrono::Utc>,
}
