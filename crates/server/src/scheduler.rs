//! Background scheduler that evaluates schedules every second.

use std::str::FromStr;
use std::time::Duration;

use chrono::Utc;
use khronos_db::Database;
use tracing::{debug, info, warn};

pub struct Scheduler {
    db: Database,
}

impl Scheduler {
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    /// Run the scheduler loop. Evaluates all schedules every second.
    pub async fn run(self) {
        info!("Scheduler started");
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        // Skip the first immediate tick
        interval.tick().await;

        loop {
            interval.tick().await;
            if let Err(e) = self.evaluate_schedules() {
                warn!(error = %e, "Error evaluating schedules");
            }
        }
    }

    /// Evaluate all active schedules and trigger workflows for those that should fire.
    fn evaluate_schedules(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.db.connection();

        // Load ALL schedules from the database (all namespaces)
        let mut stmt = conn.prepare(
            "SELECT schedule_id, namespace, spec_type, cron_expressions, interval_seconds, overlap_policy, action_workflow_name, action_task_queue, action_args_json, action_id, timeouts_json FROM schedules",
        )?;

        let rows: Vec<_> = stmt.query_map(rusqlite::params![], |row| {
            Ok((
                row.get::<_, String>(0)?,  // schedule_id
                row.get::<_, String>(1)?,  // namespace
                row.get::<_, String>(2)?,  // spec_type
                row.get::<_, Option<String>>(3)?,  // cron_expressions (JSON)
                row.get::<_, Option<i64>>(4)?,  // interval_seconds
                row.get::<_, String>(5)?,  // overlap_policy
                row.get::<_, String>(6)?,  // action_workflow_name
                row.get::<_, String>(7)?,  // action_task_queue
                row.get::<_, String>(8)?,  // action_args_json
                row.get::<_, Option<String>>(9)?,  // action_id
                row.get::<_, String>(10)?,  // timeouts_json
            ))
        })?.filter_map(|r| r.ok()).collect();

        let now = Utc::now();

        for (schedule_id, namespace, spec_type, cron_expressions, interval_seconds, overlap_policy_str, action_workflow_name, action_task_queue, action_args_json, _action_id, timeouts_json) in rows {
            // Check if this schedule should fire now
            let should_fire = match spec_type.as_str() {
                "cron" => {
                    if let Some(cron_json) = cron_expressions {
                        let expressions: Vec<String> = serde_json::from_str(&cron_json)?;
                        check_cron_match(&expressions, &now)
                    } else {
                        false
                    }
                }
                "interval" => {
                    if let Some(secs) = interval_seconds {
                        self.check_interval(&schedule_id, secs as u64, &now)?
                    } else {
                        false
                    }
                }
                _ => false,
            };

            if !should_fire {
                continue;
            }

            // Check overlap policy
            let has_running = self.check_running_workflow(&schedule_id)?;
            match overlap_policy_str.as_str() {
                "skip" => {
                    if has_running {
                        debug!(schedule_id = %schedule_id, "Skipping: workflow already running (policy=skip)");
                        continue;
                    }
                }
                "terminate" => {
                    if has_running {
                        warn!(schedule_id = %schedule_id, "Terminating existing workflow (policy=terminate)");
                        self.terminate_existing(&schedule_id)?;
                    }
                }
                "buffer" => {
                    debug!(schedule_id = %schedule_id, "Buffering workflow (policy=buffer)");
                }
                _ => {}
            }

            info!(
                schedule_id = %schedule_id,
                namespace = %namespace,
                workflow = %action_workflow_name,
                "Schedule fired, starting workflow"
            );

            self.start_schedule_workflow(
                &schedule_id,
                &namespace,
                &action_workflow_name,
                &action_task_queue,
                &action_args_json,
                &timeouts_json,
            )?;
        }

        Ok(())
    }

    /// Check if an interval schedule should fire based on last workflow start time.
    fn check_interval(&self, schedule_id: &str, interval_secs: u64, now: &chrono::DateTime<Utc>) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.db.connection();

        let pattern = format!("schedule:{}-%", schedule_id);
        let last_started: Option<String> = conn.query_row(
            "SELECT MAX(started_at) FROM workflows WHERE workflow_id LIKE ?1",
            [&pattern],
            |row| row.get(0),
        ).unwrap_or(None);

        match last_started {
            Some(ts_str) => {
                let last: chrono::DateTime<Utc> = chrono::DateTime::parse_from_str(&ts_str, "%Y-%m-%d %H:%M:%S")?
                    .with_timezone(&Utc);
                let elapsed = now.signed_duration_since(last).num_seconds();
                Ok(elapsed >= interval_secs as i64)
            }
            None => {
                // No previous run, fire immediately
                Ok(true)
            }
        }
    }

    /// Check if there's a running workflow from this schedule.
    fn check_running_workflow(&self, schedule_id: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.db.connection();
        let pattern = format!("schedule:{}-%", schedule_id);
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM workflows WHERE workflow_id LIKE ?1 AND state IN ('pending', 'running')",
            [&pattern],
            |row| row.get(0),
        )?;

        Ok(count > 0)
    }

    /// Terminate existing workflows from this schedule.
    fn terminate_existing(&self, schedule_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.db.connection();
        let pattern = format!("schedule:{}-%", schedule_id);

        let mut stmt = conn.prepare(
            "SELECT run_id FROM workflows WHERE workflow_id LIKE ?1 AND state IN ('pending', 'running')",
        )?;

        let ids: Vec<String> = stmt.query_map([&pattern], |row| {
            row.get(0)
        })?.filter_map(|r| r.ok()).collect();

        for run_id in ids {
            khronos_db::workflows::cancel_workflow(&conn, &run_id)?;
            warn!(run_id = %run_id, "Terminated existing workflow");
        }

        Ok(())
    }

    /// Start a new workflow triggered by a schedule.
    fn start_schedule_workflow(
        &self,
        schedule_id: &str,
        namespace: &str,
        workflow_name: &str,
        task_queue: &str,
        args_json_str: &str,
        timeouts_json_str: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.db.connection();

        // Parse args and timeouts from JSON strings
        let args_map: std::collections::BTreeMap<String, String> = serde_json::from_str(args_json_str).unwrap_or_default();
        let timeouts: khronos_core::Timeouts = serde_json::from_str(timeouts_json_str).unwrap_or_default();

        // Parse args to JSON value for the workflow instance
        let args_json_value: serde_json::Value = if args_map.is_empty() {
            serde_json::json!({})
        } else {
            serde_json::Map::from_iter(args_map.iter().map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))).into()
        };

        let run_id = uuid::Uuid::new_v4();
        let workflow_id = format!("schedule:{}-{}", schedule_id, run_id.to_string().chars().take(8).collect::<String>());

        // Create the workflow instance
        let workflow = khronos_core::WorkflowInstance {
            run_id,
            workflow_id: workflow_id.clone(),
            name: workflow_name.to_string(),
            task_queue: task_queue.to_string(),
            state: khronos_core::WorkflowState::Pending,
            args_json: args_json_value,
            result_json: None,
            timeouts,
            started_at: Some(Utc::now()),
            completed_at: None,
            namespace: namespace.to_string(),
        };

        khronos_db::workflows::insert_workflow(&conn, &workflow)?;

        // Create workflow steps based on built-in definitions
        let steps = get_workflow_definition(workflow_name);
        for (index, step_def) in steps.iter().enumerate() {
            let step_args_json: serde_json::Value = if args_map.is_empty() {
                serde_json::json!({})
            } else {
                // Merge workflow args with step template
                let mut merged: std::collections::BTreeMap<String, String> = args_map.clone();
                for (k, v) in &step_def.args_template {
                    merged.insert(k.clone(), v.clone());
                }
                serde_json::Map::from_iter(merged.iter().map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))).into()
            };

            let step = khronos_core::WorkflowStepInstance {
                id: uuid::Uuid::new_v4(),
                workflow_run_id: run_id,
                step_index: index,
                activity_name: step_def.activity_name.clone(),
                args_json: step_args_json,
                retry_policy: step_def.retry_policy.clone(),
                timeout_secs: step_def.timeout_secs,
                heartbeat_timeout_secs: step_def.heartbeat_timeout_secs,
                state: khronos_core::WorkflowState::Pending,
                attempt: 0,
                result_json: None,
                next_retry_at: None,
            };

            khronos_db::activities::insert_workflow_step(&conn, &step)?;
        }

        info!(run_id = %run_id, workflow_name = %workflow_name, "Created workflow from schedule");
        Ok(())
    }
}

/// Check if any of the cron expressions match the current time.
fn check_cron_match(expressions: &[String], now: &chrono::DateTime<Utc>) -> bool {
    for expr in expressions {
        if let Ok(schedule) = cron::Schedule::from_str(expr) {
            // Check if this second matches any of the schedule's upcoming times
            let next = schedule.upcoming(Utc).next();
            if let Some(next_time) = next {
                // If the next occurrence is within 1 second, fire now
                let diff = (next_time - *now).num_seconds();
                if diff >= 0 && diff <= 1 {
                    return true;
                }
            }
        } else {
            warn!(expression = %expr, "Invalid cron expression");
        }
    }
    false
}

/// Get the built-in workflow definition for a given name.
fn get_workflow_definition(name: &str) -> Vec<khronos_core::ActivityStep> {
    match name {
        "cron_job_workflow" => vec![
            khronos_core::ActivityStep {
                activity_name: "execute_cron_job".to_string(),
                args_template: std::collections::BTreeMap::new(),
                retry_policy: khronos_core::RetryPolicy { maximum_attempts: 3, initial_interval_secs: 2.0 },
                timeout_secs: 300,
                heartbeat_timeout_secs: Some(60),
            },
            khronos_core::ActivityStep {
                activity_name: "save_cron_output".to_string(),
                args_template: std::collections::BTreeMap::new(),
                retry_policy: khronos_core::RetryPolicy { maximum_attempts: 2, initial_interval_secs: 1.0 },
                timeout_secs: 60,
                heartbeat_timeout_secs: None,
            },
            khronos_core::ActivityStep {
                activity_name: "deliver_cron_result".to_string(),
                args_template: std::collections::BTreeMap::new(),
                retry_policy: khronos_core::RetryPolicy { maximum_attempts: 3, initial_interval_secs: 2.0 },
                timeout_secs: 120,
                heartbeat_timeout_secs: Some(30),
            },
            khronos_core::ActivityStep {
                activity_name: "mark_job_run_activity".to_string(),
                args_template: std::collections::BTreeMap::new(),
                retry_policy: khronos_core::RetryPolicy { maximum_attempts: 1, initial_interval_secs: 1.0 },
                timeout_secs: 30,
                heartbeat_timeout_secs: None,
            },
            khronos_core::ActivityStep {
                activity_name: "trigger_on_success_jobs".to_string(),
                args_template: std::collections::BTreeMap::new(),
                retry_policy: khronos_core::RetryPolicy { maximum_attempts: 2, initial_interval_secs: 1.0 },
                timeout_secs: 60,
                heartbeat_timeout_secs: None,
            },
        ],
        _ => vec![khronos_core::ActivityStep {
            activity_name: name.to_string(),
            args_template: std::collections::BTreeMap::new(),
            retry_policy: khronos_core::RetryPolicy { maximum_attempts: 3, initial_interval_secs: 2.0 },
            timeout_secs: 300,
            heartbeat_timeout_secs: Some(60),
        }],
    }
}
