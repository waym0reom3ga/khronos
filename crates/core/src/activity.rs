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

#[cfg(test)]
mod tests {
    use super::*;

    // ── ActivityState tests ───────────────────────────────────────

    #[test]
    fn test_activity_state_default() {
        let state = ActivityState::default();
        assert_eq!(state, ActivityState::Pending);
    }

    #[test]
    fn test_activity_state_serialization() {
        for (state, expected_str) in [
            (ActivityState::Pending, "Pending"),
            (ActivityState::Running, "Running"),
            (ActivityState::Completed, "Completed"),
            (ActivityState::Failed, "Failed"),
            (ActivityState::Retried, "Retried"),
        ] {
            let json = serde_json::to_string(&state).unwrap();
            assert!(json.contains(expected_str));

            let deserialized: ActivityState = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, state);
        }
    }

    #[test]
    fn test_activity_state_equality() {
        assert_eq!(ActivityState::Pending, ActivityState::Pending);
        assert_ne!(ActivityState::Running, ActivityState::Completed);
        assert_ne!(ActivityState::Failed, ActivityState::Retried);
    }

    // ── RetryPolicy tests ─────────────────────────────────────────

    #[test]
    fn test_retry_policy_default() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.maximum_attempts, 0);
        assert_eq!(policy.initial_interval_secs, 0.0);
    }

    #[test]
    fn test_retry_policy_new() {
        let policy = RetryPolicy::new();
        // new() delegates to default()
        assert_eq!(policy.maximum_attempts, 0);
        assert_eq!(policy.initial_interval_secs, 0.0);
    }

    #[test]
    fn test_retry_policy_custom() {
        let policy = RetryPolicy {
            maximum_attempts: 5,
            initial_interval_secs: 2.0,
        };

        let json = serde_json::to_string(&policy).unwrap();
        assert!(json.contains("5"));
        assert!(json.contains("2"));

        let deserialized: RetryPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.maximum_attempts, 5);
        assert_eq!(deserialized.initial_interval_secs, 2.0);
    }

    #[test]
    fn test_retry_policy_clone() {
        let policy = RetryPolicy {
            maximum_attempts: 3,
            initial_interval_secs: 1.5,
        };
        let cloned = policy.clone();
        assert_eq!(policy.maximum_attempts, cloned.maximum_attempts);
        assert_eq!(policy.initial_interval_secs, cloned.initial_interval_secs);
    }

    // ── ActivityResult tests ──────────────────────────────────────

    #[test]
    fn test_activity_result_success() {
        let result = ActivityResult::Success(serde_json::json!({"output": "done"}));
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("Success"));
        assert!(json.contains("done"));

        let deserialized: ActivityResult = serde_json::from_str(&json).unwrap();
        match deserialized {
            ActivityResult::Success(val) => {
                assert_eq!(val["output"], "done");
            }
            _ => panic!("Expected Success variant"),
        }
    }

    #[test]
    fn test_activity_result_failure() {
        let result = ActivityResult::Failure("connection timeout".to_string());
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("Failure"));
        assert!(json.contains("connection timeout"));

        let deserialized: ActivityResult = serde_json::from_str(&json).unwrap();
        match deserialized {
            ActivityResult::Failure(msg) => assert_eq!(msg, "connection timeout"),
            _ => panic!("Expected Failure variant"),
        }
    }

    // ── ActivityTask tests ────────────────────────────────────────

    #[test]
    fn test_activity_task_round_trip() {
        let task = ActivityTask {
            activity_id: Uuid::new_v4(),
            step_id: Uuid::new_v4(),
            workflow_run_id: Uuid::new_v4(),
            name: "process_data".to_string(),
            args: [("input".into(), "data".into())].iter().cloned().collect(),
            retry_policy: RetryPolicy {
                maximum_attempts: 3,
                initial_interval_secs: 2.0,
            },
            heartbeat_timeout_secs: 60,
            start_to_close_timeout_secs: 300,
        };

        let json = serde_json::to_string(&task).unwrap();
        assert!(json.contains("process_data"));

        let deserialized: ActivityTask = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.name, "process_data");
        assert_eq!(deserialized.heartbeat_timeout_secs, 60);
        assert_eq!(deserialized.start_to_close_timeout_secs, 300);
    }

    #[test]
    fn test_activity_task_clone() {
        let task = ActivityTask {
            activity_id: Uuid::new_v4(),
            step_id: Uuid::new_v4(),
            workflow_run_id: Uuid::new_v4(),
            name: "test".to_string(),
            args: std::collections::BTreeMap::new(),
            retry_policy: RetryPolicy::default(),
            heartbeat_timeout_secs: 30,
            start_to_close_timeout_secs: 120,
        };

        let cloned = task.clone();
        assert_eq!(task.activity_id, cloned.activity_id);
        assert_eq!(task.name, cloned.name);
    }
}
