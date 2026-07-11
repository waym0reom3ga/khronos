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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_schedule() -> Schedule {
        let now = chrono::Utc::now();
        Schedule {
            schedule_id: "test-schedule".to_string(),
            namespace: "default".to_string(),
            spec: ScheduleSpec::Cron(vec!["0 * * * *".to_string()]),
            action: WorkflowAction {
                workflow_name: "my_workflow".to_string(),
                args: [("key".into(), "value".into())].iter().cloned().collect(),
                task_queue: "default".to_string(),
                id: "action-1".to_string(),
                timeouts: Timeouts::default(),
            },
            policy: OverlapPolicy::Skip,
            created_at: now,
            updated_at: now,
        }
    }

    // ── ScheduleSpec tests ────────────────────────────────────────

    #[test]
    fn test_cron_spec_serialization() {
        let spec = ScheduleSpec::Cron(vec!["0 * * * *".into(), "*/5 * * * *".into()]);
        let json = serde_json::to_string(&spec).unwrap();
        assert!(json.contains("Cron"));

        let deserialized: ScheduleSpec = serde_json::from_str(&json).unwrap();
        match deserialized {
            ScheduleSpec::Cron(exprs) => {
                assert_eq!(exprs.len(), 2);
                assert_eq!(exprs[0], "0 * * * *");
                assert_eq!(exprs[1], "*/5 * * * *");
            }
            _ => panic!("Expected Cron variant"),
        }
    }

    #[test]
    fn test_interval_spec_serialization() {
        let spec = ScheduleSpec::Interval(std::time::Duration::from_secs(300));
        let json = serde_json::to_string(&spec).unwrap();
        assert!(json.contains("Interval"));

        let deserialized: ScheduleSpec = serde_json::from_str(&json).unwrap();
        match deserialized {
            ScheduleSpec::Interval(dur) => {
                assert_eq!(dur.as_secs(), 300);
            }
            _ => panic!("Expected Interval variant"),
        }
    }

    #[test]
    fn test_cron_spec_display() {
        let spec = ScheduleSpec::Cron(vec!["0 * * * *".into()]);
        assert_eq!(format!("{}", spec), "Cron(0 * * * *)");
    }

    #[test]
    fn test_interval_spec_display() {
        let spec = ScheduleSpec::Interval(std::time::Duration::from_secs(60));
        let display = format!("{}", spec);
        assert!(display.starts_with("Interval("));
        assert!(display.ends_with(')'));
    }

    // ── OverlapPolicy tests ───────────────────────────────────────

    #[test]
    fn test_overlap_policy_skip() {
        let policy = OverlapPolicy::Skip;
        let json = serde_json::to_string(&policy).unwrap();
        assert!(json.contains("Skip"));
        let deserialized: OverlapPolicy = serde_json::from_str(&json).unwrap();
        match deserialized {
            OverlapPolicy::Skip => {}
            _ => panic!("Expected Skip"),
        }
    }

    #[test]
    fn test_overlap_policy_buffer() {
        let policy = OverlapPolicy::Buffer(5);
        let json = serde_json::to_string(&policy).unwrap();
        assert!(json.contains("Buffer"));
        let deserialized: OverlapPolicy = serde_json::from_str(&json).unwrap();
        match deserialized {
            OverlapPolicy::Buffer(n) => assert_eq!(n, 5),
            _ => panic!("Expected Buffer"),
        }
    }

    #[test]
    fn test_overlap_policy_terminate() {
        let policy = OverlapPolicy::TerminateExisting;
        let json = serde_json::to_string(&policy).unwrap();
        assert!(json.contains("TerminateExisting"));
        let deserialized: OverlapPolicy = serde_json::from_str(&json).unwrap();
        match deserialized {
            OverlapPolicy::TerminateExisting => {}
            _ => panic!("Expected TerminateExisting"),
        }
    }

    #[test]
    fn test_overlap_policy_display() {
        assert_eq!(format!("{}", OverlapPolicy::Skip), "Skip");
        assert_eq!(format!("{}", OverlapPolicy::Buffer(3)), "Buffer(3)");
        assert_eq!(format!("{}", OverlapPolicy::TerminateExisting), "TerminateExisting");
    }

    // ── Timeouts tests ────────────────────────────────────────────

    #[test]
    fn test_timeouts_default() {
        let timeouts = Timeouts::default();
        assert!(timeouts.execution_timeout_secs.is_none());
        assert!(timeouts.run_timeout_secs.is_none());
        assert!(timeouts.task_timeout_secs.is_none());
    }

    #[test]
    fn test_timeouts_serialization() {
        let timeouts = Timeouts {
            execution_timeout_secs: Some(3600),
            run_timeout_secs: Some(1800),
            task_timeout_secs: Some(300),
        };
        let json = serde_json::to_string(&timeouts).unwrap();
        assert!(json.contains("3600"));
        assert!(json.contains("1800"));
        assert!(json.contains("300"));

        let deserialized: Timeouts = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.execution_timeout_secs, Some(3600));
        assert_eq!(deserialized.run_timeout_secs, Some(1800));
        assert_eq!(deserialized.task_timeout_secs, Some(300));
    }

    // ── WorkflowAction tests ──────────────────────────────────────

    #[test]
    fn test_workflow_action_with_args() {
        let mut args = BTreeMap::new();
        args.insert("input".to_string(), "data".to_string());
        args.insert("mode".to_string(), "fast".to_string());

        let action = WorkflowAction {
            workflow_name: "process_data".to_string(),
            args: args.clone(),
            task_queue: "workers".to_string(),
            id: "act-123".to_string(),
            timeouts: Timeouts {
                execution_timeout_secs: Some(60),
                run_timeout_secs: None,
                task_timeout_secs: Some(30),
            },
        };

        let json = serde_json::to_string(&action).unwrap();
        assert!(json.contains("process_data"));
        assert!(json.contains("workers"));
        assert!(json.contains("act-123"));

        let deserialized: WorkflowAction = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.workflow_name, "process_data");
        assert_eq!(deserialized.args.get("input").unwrap(), "data");
        assert_eq!(deserialized.args.get("mode").unwrap(), "fast");
        assert_eq!(deserialized.timeouts.execution_timeout_secs, Some(60));
    }

    #[test]
    fn test_workflow_action_empty_args() {
        let action = WorkflowAction {
            workflow_name: "simple".to_string(),
            args: BTreeMap::new(),
            task_queue: "default".to_string(),
            id: "".to_string(),
            timeouts: Timeouts::default(),
        };

        let json = serde_json::to_string(&action).unwrap();
        let deserialized: WorkflowAction = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.workflow_name, "simple");
        assert!(deserialized.args.is_empty());
    }

    // ── Full Schedule round-trip tests ────────────────────────────

    #[test]
    fn test_schedule_cron_round_trip() {
        let schedule = make_schedule();
        let json = serde_json::to_string_pretty(&schedule).unwrap();
        let deserialized: Schedule = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.schedule_id, "test-schedule");
        assert_eq!(deserialized.namespace, "default");
        match &deserialized.spec {
            ScheduleSpec::Cron(exprs) => {
                assert_eq!(exprs.len(), 1);
                assert_eq!(exprs[0], "0 * * * *");
            }
            _ => panic!("Expected Cron spec"),
        }
        assert_eq!(deserialized.action.workflow_name, "my_workflow");
        match deserialized.policy {
            OverlapPolicy::Skip => {}
            _ => panic!("Expected Skip policy"),
        }
    }

    #[test]
    fn test_schedule_interval_round_trip() {
        let now = chrono::Utc::now();
        let schedule = Schedule {
            schedule_id: "interval-schedule".to_string(),
            namespace: "production".to_string(),
            spec: ScheduleSpec::Interval(std::time::Duration::from_secs(600)),
            action: WorkflowAction {
                workflow_name: "cleanup".to_string(),
                args: BTreeMap::new(),
                task_queue: "maintenance".to_string(),
                id: "action-cleanup".to_string(),
                timeouts: Timeouts {
                    execution_timeout_secs: Some(300),
                    run_timeout_secs: None,
                    task_timeout_secs: None,
                },
            },
            policy: OverlapPolicy::Buffer(10),
            created_at: now,
            updated_at: now,
        };

        let json = serde_json::to_string(&schedule).unwrap();
        let deserialized: Schedule = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.schedule_id, "interval-schedule");
        assert_eq!(deserialized.namespace, "production");
        match &deserialized.spec {
            ScheduleSpec::Interval(dur) => assert_eq!(dur.as_secs(), 600),
            _ => panic!("Expected Interval spec"),
        }
        match deserialized.policy {
            OverlapPolicy::Buffer(n) => assert_eq!(n, 10),
            _ => panic!("Expected Buffer policy"),
        }
    }

    #[test]
    fn test_schedule_json_contains_all_fields() {
        let schedule = make_schedule();
        let json = serde_json::to_string(&schedule).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert!(parsed.get("schedule_id").is_some());
        assert!(parsed.get("namespace").is_some());
        assert!(parsed.get("spec").is_some());
        assert!(parsed.get("action").is_some());
        assert!(parsed.get("policy").is_some());
        assert!(parsed.get("created_at").is_some());
        assert!(parsed.get("updated_at").is_some());

        let action = parsed.get("action").unwrap();
        assert!(action.get("workflow_name").is_some());
        assert!(action.get("args").is_some());
        assert!(action.get("task_queue").is_some());
        assert!(action.get("id").is_some());
        assert!(action.get("timeouts").is_some());
    }

    #[test]
    fn test_schedule_clone() {
        let schedule = make_schedule();
        let cloned = schedule.clone();
        assert_eq!(schedule.schedule_id, cloned.schedule_id);
        assert_eq!(schedule.namespace, cloned.namespace);
    }
}
