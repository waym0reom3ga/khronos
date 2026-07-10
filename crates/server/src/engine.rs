//! Workflow execution engine — processes workflows, retries, and heartbeat checks.

use std::time::Duration;

use chrono::Utc;
use khronos_core::{WorkflowState};
use khronos_db::Database;
use tracing::{debug, info, warn};

pub struct Engine {
    db: Database,
}

impl Engine {
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    /// Run the engine loop. Processes workflows every second.
    pub async fn run(self) {
        info!("Engine started");
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        // Skip the first immediate tick
        interval.tick().await;

        loop {
            interval.tick().await;
            if let Err(e) = self.process_workflows() {
                warn!(error = %e, "Error processing workflows");
            }
            if let Err(e) = self.check_retries() {
                warn!(error = %e, "Error checking retries");
            }
            if let Err(e) = self.check_heartbeats() {
                warn!(error = %e, "Error checking heartbeats");
            }
        }
    }

    /// Process all running workflows — advance to next pending step.
    fn process_workflows(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.db.connection();

        // Find all workflows in PENDING or RUNNING state
        let mut stmt = conn.prepare(
            "SELECT run_id, namespace FROM workflows WHERE state IN ('pending', 'running') ORDER BY started_at ASC",
        )?;

        let workflows: Vec<(String, String)> = stmt.query_map(rusqlite::params![], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?.filter_map(|r| r.ok()).collect();

        for (run_id, _namespace) in workflows {
            self.process_single_workflow(&conn, &run_id)?;
        }

        Ok(())
    }

    /// Process a single workflow — find and advance the next pending step.
    fn process_single_workflow(&self, conn: &rusqlite::Connection, run_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Transition PENDING workflows to RUNNING
        let updated = conn.execute(
            "UPDATE workflows SET state = 'running', started_at = COALESCE(started_at, datetime('now')) WHERE run_id = ?1 AND state = 'pending'",
            [run_id],
        )?;

        if updated > 0 {
            info!(run_id = %run_id, "Workflow transitioned to RUNNING");
        }

        // Find the next pending step for this workflow (lowest step_index)
        let mut stmt = conn.prepare(
            "SELECT id, step_index, activity_name FROM workflow_steps WHERE workflow_run_id = ?1 AND state IN ('pending', 'retried') ORDER BY step_index ASC LIMIT 1",
        )?;

        let next_step: Option<(String, usize, String)> = stmt.query_map([run_id], |row| {
            Ok((row.get(0)?, row.get::<_, i64>(1)? as usize, row.get(2)?))
        })?.filter_map(|r| r.ok()).next();

        match next_step {
            Some((_step_id, step_index, _activity_name)) => {
                // Check if all previous steps are completed
                let prev_completed: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM workflow_steps WHERE workflow_run_id = ?1 AND step_index < ?2 AND state = 'completed'",
                    rusqlite::params![run_id, step_index as i64],
                    |row| row.get(0),
                )?;

                let total_prev: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM workflow_steps WHERE workflow_run_id = ?1 AND step_index < ?2",
                    rusqlite::params![run_id, step_index as i64],
                    |row| row.get(0),
                )?;

                // Only advance if all previous steps are completed (or this is the first step)
                if prev_completed == total_prev {
                    debug!(
                        run_id = %run_id,
                        step_index = step_index,
                        "Step ready for execution"
                    );
                    // The step is already in 'pending' state — the worker will pick it up via PollActivity.
                } else {
                    debug!(
                        run_id = %run_id,
                        step_index = step_index,
                        prev_completed = prev_completed,
                        total_prev = total_prev,
                        "Waiting for previous steps to complete"
                    );
                }
            }
            None => {
                // No pending steps — check if all steps are completed or failed
                let (total_steps, completed_count, failed_count): (i64, i64, i64) = conn.query_row(
                    "SELECT COUNT(*), SUM(CASE WHEN state='completed' THEN 1 ELSE 0 END), SUM(CASE WHEN state='failed' THEN 1 ELSE 0 END) FROM workflow_steps WHERE workflow_run_id = ?1",
                    [run_id],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<i64>>(1)?.unwrap_or(0), row.get::<_, Option<i64>>(2)?.unwrap_or(0))),
                )?;

                if total_steps > 0 {
                    if failed_count > 0 {
                        info!(run_id = %run_id, "Workflow completed with failures");
                        khronos_db::workflows::update_workflow_state(&conn, run_id, WorkflowState::Failed, None, Some(Utc::now()))?;
                    } else if completed_count == total_steps {
                        info!(run_id = %run_id, "Workflow completed successfully");
                        khronos_db::workflows::update_workflow_state(&conn, run_id, WorkflowState::Completed, None, Some(Utc::now()))?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Check for retried steps that are ready to be re-executed.
    fn check_retries(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.db.connection();
        let now_str = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();

        // Find steps with state='retried' where next_retry_at <= now
        let mut stmt = conn.prepare(
            "SELECT id, workflow_run_id FROM workflow_steps WHERE state = 'retried' AND next_retry_at IS NOT NULL AND next_retry_at <= ?1",
        )?;

        let steps: Vec<(String, String)> = stmt.query_map([&now_str], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?.filter_map(|r| r.ok()).collect();

        for (step_id, _workflow_run_id) in steps {
            conn.execute(
                "UPDATE workflow_steps SET state = 'pending', next_retry_at = NULL WHERE id = ?1",
                [&step_id],
            )?;

            debug!(step_id = %step_id, "Step reset to pending for retry");
        }

        Ok(())
    }

    /// Check for running activities that have missed their heartbeat deadline.
    fn check_heartbeats(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.db.connection();
        let now = Utc::now();

        // Get all running activities with heartbeat info
        let running_activities = khronos_db::activities::get_running_activities(&conn)?;

        for activity in &running_activities {
            if let Some(timeout_secs) = activity.heartbeat_timeout_secs {
                // Determine the reference time: last_heartbeat_at or started_at
                let reference_time = match (&activity.last_heartbeat_at_str, &activity.started_at_str) {
                    (Some(hb), _) => chrono::DateTime::parse_from_str(hb, "%Y-%m-%d %H:%M:%S")?
                        .with_timezone(&Utc),
                    (None, Some(started)) => chrono::DateTime::parse_from_str(started, "%Y-%m-%d %H:%M:%S")?
                        .with_timezone(&Utc),
                    _ => continue,
                };

                let elapsed = now.signed_duration_since(reference_time).num_seconds();

                if elapsed > timeout_secs as i64 {
                    warn!(
                        activity_id = %activity.id,
                        step_id = %activity.step_id,
                        elapsed_secs = elapsed,
                        heartbeat_timeout_secs = timeout_secs,
                        "Activity heartbeat timeout exceeded"
                    );

                    // Mark the activity as failed
                    khronos_db::activities::update_activity_state(
                        &conn,
                        &activity.id,
                        khronos_core::ActivityState::Failed,
                        None,
                        Some("Heartbeat timeout"),
                    )?;

                    // Get step retry policy to decide whether to retry
                    let retry_policy_json: String = conn.query_row(
                        "SELECT retry_policy_json FROM workflow_steps WHERE id = ?1",
                        [&activity.step_id],
                        |row| row.get(0),
                    ).unwrap_or_default();

                    let retry_policy: khronos_core::RetryPolicy = serde_json::from_str(&retry_policy_json).unwrap_or_default();
                    let current_attempt = activity.attempt;

                    if current_attempt < retry_policy.maximum_attempts {
                        // Schedule a retry with exponential backoff
                        let backoff_secs = retry_policy.initial_interval_secs * (2.0_f64.powi((current_attempt - 1) as i32));
                        let next_retry_at = now + chrono::Duration::seconds(backoff_secs as i64);

                        khronos_db::activities::update_step_state(
                            &conn,
                            &activity.step_id,
                            WorkflowState::Pending,
                            Some(current_attempt),
                            None,
                            Some(next_retry_at),
                        )?;

                        debug!(
                            activity_id = %activity.id,
                            step_id = %activity.step_id,
                            attempt = current_attempt,
                            "Heartbeat timeout: scheduling retry"
                        );
                    } else {
                        // Max retries exceeded
                        khronos_db::activities::update_step_state(
                            &conn,
                            &activity.step_id,
                            WorkflowState::Failed,
                            Some(current_attempt),
                            None,
                            None,
                        )?;

                        warn!(
                            activity_id = %activity.id,
                            step_id = %activity.step_id,
                            "Heartbeat timeout: max retries exceeded, marking as failed"
                        );
                    }
                }
            }
        }

        Ok(())
    }
}
