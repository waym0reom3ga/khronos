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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_workflow_instance() -> WorkflowInstance {
        WorkflowInstance {
            run_id: Uuid::new_v4(),
            workflow_id: "wf-001".to_string(),
            name: "test_workflow".to_string(),
            task_queue: "default".to_string(),
            state: WorkflowState::Pending,
            args_json: serde_json::json!({"key": "value"}),
            result_json: None,
            timeouts: crate::schedule::Timeouts {
                execution_timeout_secs: Some(3600),
                run_timeout_secs: Some(1800),
                task_timeout_secs: Some(300),
            },
            started_at: None,
            completed_at: None,
            namespace: "default".to_string(),
        }
    }

    fn make_step_instance(run_id: Uuid) -> WorkflowStepInstance {
        WorkflowStepInstance {
            id: Uuid::new_v4(),
            workflow_run_id: run_id,
            step_index: 0,
            activity_name: "test_activity".to_string(),
            args_json: serde_json::json!({"input": "data"}),
            retry_policy: crate::activity::RetryPolicy {
                maximum_attempts: 3,
                initial_interval_secs: 2.0,
            },
            timeout_secs: 60,
            heartbeat_timeout_secs: Some(30),
            state: WorkflowState::Pending,
            attempt: 0,
            result_json: None,
            next_retry_at: None,
        }
    }

    // ── WorkflowState tests ───────────────────────────────────────

    #[test]
    fn test_workflow_state_default() {
        let state = WorkflowState::default();
        assert_eq!(state, WorkflowState::Pending);
    }

    #[test]
    fn test_workflow_state_serialization() {
        for (state, expected_str) in [
            (WorkflowState::Pending, "Pending"),
            (WorkflowState::Running, "Running"),
            (WorkflowState::Completed, "Completed"),
            (WorkflowState::Failed, "Failed"),
            (WorkflowState::Cancelled, "Cancelled"),
        ] {
            let json = serde_json::to_string(&state).unwrap();
            assert!(json.contains(expected_str), "Expected {} in {}", expected_str, json);

            let deserialized: WorkflowState = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, state);
        }
    }

    #[test]
    fn test_workflow_state_equality() {
        assert_eq!(WorkflowState::Pending, WorkflowState::Pending);
        assert_ne!(WorkflowState::Pending, WorkflowState::Running);
        assert_ne!(WorkflowState::Completed, WorkflowState::Failed);
    }

    // ── WorkflowInstance tests ────────────────────────────────────

    #[test]
    fn test_workflow_instance_round_trip() {
        let wf = make_workflow_instance();
        let json = serde_json::to_string(&wf).unwrap();
        let deserialized: WorkflowInstance = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.run_id, wf.run_id);
        assert_eq!(deserialized.workflow_id, "wf-001");
        assert_eq!(deserialized.name, "test_workflow");
        assert_eq!(deserialized.state, WorkflowState::Pending);
        assert_eq!(deserialized.namespace, "default");
    }

    #[test]
    fn test_workflow_instance_with_result() {
        let mut wf = make_workflow_instance();
        wf.state = WorkflowState::Completed;
        wf.result_json = Some(serde_json::json!({"output": "success"}));
        wf.completed_at = Some(chrono::Utc::now());

        let json = serde_json::to_string(&wf).unwrap();
        let deserialized: WorkflowInstance = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.state, WorkflowState::Completed);
        assert!(deserialized.result_json.is_some());
        assert!(deserialized.completed_at.is_some());
    }

    #[test]
    fn test_workflow_instance_clone() {
        let wf = make_workflow_instance();
        let cloned = wf.clone();
        assert_eq!(wf.run_id, cloned.run_id);
        assert_eq!(wf.workflow_id, cloned.workflow_id);
    }

    // ── WorkflowStepInstance tests ────────────────────────────────

    #[test]
    fn test_step_instance_round_trip() {
        let run_id = Uuid::new_v4();
        let step = make_step_instance(run_id);
        let json = serde_json::to_string(&step).unwrap();
        let deserialized: WorkflowStepInstance = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.workflow_run_id, run_id);
        assert_eq!(deserialized.step_index, 0);
        assert_eq!(deserialized.activity_name, "test_activity");
        assert_eq!(deserialized.state, WorkflowState::Pending);
        assert_eq!(deserialized.attempt, 0);
    }

    #[test]
    fn test_step_instance_with_retry() {
        let run_id = Uuid::new_v4();
        let mut step = make_step_instance(run_id);
        step.state = WorkflowState::Running;
        step.attempt = 2;
        step.next_retry_at = Some(chrono::Utc::now());

        let json = serde_json::to_string(&step).unwrap();
        let deserialized: WorkflowStepInstance = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.state, WorkflowState::Running);
        assert_eq!(deserialized.attempt, 2);
        assert!(deserialized.next_retry_at.is_some());
    }

    #[test]
    fn test_step_instance_with_result() {
        let run_id = Uuid::new_v4();
        let mut step = make_step_instance(run_id);
        step.state = WorkflowState::Completed;
        step.result_json = Some(serde_json::json!({"result": 42}));

        let json = serde_json::to_string(&step).unwrap();
        let deserialized: WorkflowStepInstance = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.state, WorkflowState::Completed);
        assert!(deserialized.result_json.is_some());
    }

    // ── ActivityStep tests ────────────────────────────────────────

    #[test]
    fn test_activity_step_serialization() {
        let step = ActivityStep {
            activity_name: "process".to_string(),
            args_template: [("input".into(), "{{data}}".into())].iter().cloned().collect(),
            retry_policy: crate::activity::RetryPolicy {
                maximum_attempts: 5,
                initial_interval_secs: 1.0,
            },
            timeout_secs: 120,
            heartbeat_timeout_secs: Some(30),
        };

        let json = serde_json::to_string(&step).unwrap();
        assert!(json.contains("process"));
        assert!(json.contains("{{data}}"));

        let deserialized: ActivityStep = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.activity_name, "process");
        assert_eq!(deserialized.timeout_secs, 120);
        assert_eq!(deserialized.heartbeat_timeout_secs, Some(30));
    }

    // ── WorkflowDefinition tests ──────────────────────────────────

    #[test]
    fn test_workflow_definition_serialization() {
        let def = WorkflowDefinition {
            name: "multi_step".to_string(),
            steps: vec![
                ActivityStep {
                    activity_name: "step1".to_string(),
                    args_template: std::collections::BTreeMap::new(),
                    retry_policy: crate::activity::RetryPolicy::default(),
                    timeout_secs: 60,
                    heartbeat_timeout_secs: None,
                },
                ActivityStep {
                    activity_name: "step2".to_string(),
                    args_template: std::collections::BTreeMap::new(),
                    retry_policy: crate::activity::RetryPolicy::default(),
                    timeout_secs: 120,
                    heartbeat_timeout_secs: Some(30),
                },
            ],
        };

        let json = serde_json::to_string(&def).unwrap();
        assert!(json.contains("multi_step"));
        assert!(json.contains("step1"));
        assert!(json.contains("step2"));

        let deserialized: WorkflowDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.name, "multi_step");
        assert_eq!(deserialized.steps.len(), 2);
    }
}
