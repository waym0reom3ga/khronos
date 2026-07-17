//! Event-driven scheduler that sleeps until the next scheduled fire time.
//!
//! Instead of polling every second, this scheduler maintains a min-heap of
//! (next_fire_time, schedule_id) entries and uses `tokio::time::sleep_until`
//! to wait for the earliest schedule. It consumes near-zero CPU when idle.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::str::FromStr;
use std::sync::OnceLock;
use std::time::Duration;

use chrono::{DateTime, NaiveDateTime, Utc};
use khronos_db::Database;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

/// Information about a schedule loaded from the database.
#[derive(Clone)]
struct ScheduleInfo {
    schedule_id: String,
    namespace: String,
    spec_type: String,
    cron_expressions: Option<Vec<String>>,
    interval_seconds: Option<u64>,
    overlap_policy: String,
    action_workflow_name: String,
    action_task_queue: String,
    action_args_json: String,
    timeouts_json: String,
}

/// Entry in the min-heap: (next_fire_time, schedule_id).
/// Wrapped in Reverse so BinaryHeap (max-heap) behaves as a min-heap.
#[derive(Eq, PartialEq)]
struct ScheduleEntry {
    next_fire_time: chrono::DateTime<Utc>,
    schedule_id: String,
}

impl Ord for ScheduleEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse ordering so smallest (earliest) time is at the top
        self.next_fire_time
            .cmp(&other.next_fire_time)
            .then_with(|| self.schedule_id.cmp(&other.schedule_id))
    }
}

impl PartialOrd for ScheduleEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub struct Scheduler {
    db: Database,
    schedule_change_rx: Option<broadcast::Receiver<()>>,
}

/// Global sender for schedule change notifications.
/// Set by `Scheduler::new()` and accessible via `schedule_change_sender()`.
static SCHEDULE_CHANGE_TX: OnceLock<broadcast::Sender<()>> = OnceLock::new();

impl Scheduler {
    /// Create a new event-driven scheduler.
    ///
    /// The schedule change sender is stored globally and can be retrieved via
    /// [`schedule_change_sender()`](fn.schedule_change_sender.html) for use by
    /// gRPC handlers when schedules are created, updated, or deleted.
    pub fn new(db: Database) -> Self {
        let (tx, rx) = broadcast::channel(64);
        SCHEDULE_CHANGE_TX.set(tx).ok();
        Self {
            db,
            schedule_change_rx: Some(rx),
        }
    }

    /// Get a clone of the schedule change sender.
    ///
    /// Call this from gRPC handlers to notify the scheduler of schedule changes.
    pub fn schedule_change_sender() -> Option<broadcast::Sender<()>> {
        SCHEDULE_CHANGE_TX.get().cloned()
    }

    /// Run the event-driven scheduler loop.
    ///
    /// Loads all schedules, computes their next fire times, and sleeps until
    /// the earliest one. Wakes early if a schedule change is received.
    pub async fn run(mut self) {
        info!("Event-driven scheduler started");

        let mut heap: BinaryHeap<Reverse<ScheduleEntry>> = BinaryHeap::new();

        // Load all schedules on startup
        if let Err(e) = self.reload_all_schedules(&mut heap) {
            warn!(error = %e, "Error loading schedules on startup");
        }

        // Take the receiver out of self so we can use it in the select loop
        // without moving self (which we still need for process_schedule etc.)
        let mut rx = self
            .schedule_change_rx
            .take()
            .expect("schedule change receiver should be present");

        loop {
            // Determine when the next schedule fires (if any)
            let next_fire_time = heap.peek().map(|entry| entry.0.next_fire_time);

            // Create sleep future — use long duration when heap is empty
            // (message branch will wake us up when schedules change)
            let sleep_duration = {
                let raw = if let Some(fire_time) = next_fire_time {
                    let duration = fire_time.signed_duration_since(Utc::now());
                    let secs = duration.num_seconds().max(0);
                    let nanos = duration
                        .num_nanoseconds()
                        .map(|n| (n % 1_000_000_000).max(0) as u32)
                        .unwrap_or(0);
                    Duration::new(secs as u64, nanos)
                } else {
                    // No schedules — sleep for a very long time.
                    // The message branch will wake us up when schedules change.
                    Duration::from_secs(365 * 24 * 60 * 60) // 1 year
                };
                // Enforce a minimum sleep floor to prevent busy-wait loops
                // when interval schedules are very short (e.g., 1 second)
                raw.max(Duration::from_millis(500))
            };
            tokio::pin!(let sleep = tokio::time::sleep(sleep_duration););

            tokio::select! {
                // Sleep until the next scheduled fire time
                _ = &mut sleep => {
                    // Process all schedules whose fire time has arrived
                    let now = Utc::now();
                    let mut fired_ids: Vec<String> = Vec::new();
                    while let Some(Reverse(entry)) = heap.pop() {
                        if entry.next_fire_time > now {
                            // Push back — not yet time for this one
                            heap.push(Reverse(entry));
                            break;
                        }
                        // Track which schedules fired
                        fired_ids.push(entry.schedule_id.clone());
                        // Fire this schedule
                        if let Err(e) = self.process_schedule(&fired_ids.last().unwrap()) {
                            warn!(
                                schedule_id = %fired_ids.last().unwrap(),
                                error = %e,
                                "Error processing schedule"
                            );
                        }
                    }
                    // Only update the schedules that actually fired — NOT all schedules
                    for id in fired_ids {
                        if let Err(e) = self.update_schedule_next_fire(&id, &mut heap) {
                            warn!(error = %e, schedule_id = %id, "Error updating schedule next fire");
                        }
                    }
                }

                // Schedule change notification — rebuild the heap
                result = rx.recv() => {
                    match result {
                        Ok(_) => {
                            info!("Schedule change received, reloading schedules");
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!(missed = %n, "Schedule change channel lagged");
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            warn!("Schedule change channel closed");
                        }
                    }
                    // Reload all schedules
                    if let Err(e) = self.reload_all_schedules(&mut heap) {
                        warn!(error = %e, "Error reloading schedules after change");
                    }
                }
            }
        }
    }

    /// Reload all schedules from the database and rebuild the min-heap.
    fn reload_all_schedules(
        &self,
        heap: &mut BinaryHeap<Reverse<ScheduleEntry>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.db.connection();
        let now = Utc::now();

        let mut stmt = conn.prepare(
            "SELECT schedule_id, namespace, spec_type, cron_expressions, \
             interval_seconds, overlap_policy, action_workflow_name, \
             action_task_queue, action_args_json, timeouts_json \
             FROM schedules",
        )?;

        let rows: Vec<ScheduleInfo> = stmt
            .query_map(rusqlite::params![], |row| {
                let cron_json: Option<String> = row.get(3)?;
                let cron_expressions: Option<Vec<String>> =
                    cron_json.and_then(|j| serde_json::from_str(&j).ok());

                Ok(ScheduleInfo {
                    schedule_id: row.get(0)?,
                    namespace: row.get(1)?,
                    spec_type: row.get(2)?,
                    cron_expressions,
                    interval_seconds: row.get::<_, Option<i64>>(4)?.map(|s| s as u64),
                    overlap_policy: row.get(5)?,
                    action_workflow_name: row.get(6)?,
                    action_task_queue: row.get(7)?,
                    action_args_json: row.get(8)?,
                    timeouts_json: row.get(9)?,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();

        // Calculate next fire time for each schedule and insert into heap.
        // Use a map to deduplicate by schedule_id (keep earliest).
        let mut next_fires: HashMap<String, chrono::DateTime<Utc>> = HashMap::new();

        for info in rows {
            let next = match info.spec_type.as_str() {
                "cron" => self.calculate_next_cron_fire(&info, &now),
                "interval" => self.calculate_next_interval_fire(&info, &now)?,
                _ => continue,
            };

            if let Some(next_time) = next {
                next_fires
                    .entry(info.schedule_id.clone())
                    .and_modify(|existing| {
                        if next_time < *existing {
                            *existing = next_time;
                        }
                    })
                    .or_insert(next_time);
            }
        }

        // Rebuild the heap from deduplicated entries
        heap.clear();
        for (schedule_id, next_time) in next_fires {
            heap.push(Reverse(ScheduleEntry {
                next_fire_time: next_time,
                schedule_id,
            }));
        }

        debug!(
            schedules_loaded = %heap.len(),
            next_fire = ?heap.peek().map(|e| e.0.next_fire_time),
            "Schedules reloaded"
        );

        Ok(())
    }

    /// Process a single schedule: check if it should fire, handle overlap policy,
    /// and start the workflow.
    fn process_schedule(
        &self,
        schedule_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.db.connection();
        let now = Utc::now();

        // Load schedule info — skip if deleted
        let info: ScheduleInfo = match conn.query_row(
            "SELECT schedule_id, namespace, spec_type, cron_expressions, \
             interval_seconds, overlap_policy, action_workflow_name, \
             action_task_queue, action_args_json, timeouts_json \
             FROM schedules WHERE schedule_id = ?1",
            [schedule_id],
            |row| {
                let cron_json: Option<String> = row.get(3)?;
                let cron_expressions: Option<Vec<String>> =
                    cron_json.and_then(|j| serde_json::from_str(&j).ok());

                Ok(ScheduleInfo {
                    schedule_id: row.get(0)?,
                    namespace: row.get(1)?,
                    spec_type: row.get(2)?,
                    cron_expressions,
                    interval_seconds: row.get::<_, Option<i64>>(4)?.map(|s| s as u64),
                    overlap_policy: row.get(5)?,
                    action_workflow_name: row.get(6)?,
                    action_task_queue: row.get(7)?,
                    action_args_json: row.get(8)?,
                    timeouts_json: row.get(9)?,
                })
            },
        ) {
            Ok(info) => info,
            Err(_) => {
                debug!(schedule_id = %schedule_id, "Schedule not found (deleted), skipping");
                return Ok(());
            }
        };

        // Check if this schedule should fire now
        let should_fire = match info.spec_type.as_str() {
            "cron" => {
                if let Some(ref expressions) = info.cron_expressions {
                    check_cron_match(expressions, &now)
                } else {
                    false
                }
            }
            "interval" => {
                if let Some(secs) = info.interval_seconds {
                    self.check_interval(schedule_id, secs, &now)?
                } else {
                    false
                }
            }
            _ => false,
        };

        if !should_fire {
            debug!(schedule_id = %schedule_id, "Schedule did not fire (missed window)");
            return Ok(());
        }

        // Check overlap policy
        let has_running = self.check_running_workflow(schedule_id)?;
        match info.overlap_policy.as_str() {
            "skip" => {
                if has_running {
                    debug!(
                        schedule_id = %schedule_id,
                        "Skipping: workflow already running (policy=skip)"
                    );
                    return Ok(());
                }
            }
            "terminate" => {
                if has_running {
                    warn!(
                        schedule_id = %schedule_id,
                        "Terminating existing workflow (policy=terminate)"
                    );
                    self.terminate_existing(schedule_id)?;
                }
            }
            "buffer" => {
                debug!(
                    schedule_id = %schedule_id,
                    "Buffering workflow (policy=buffer)"
                );
            }
            _ => {}
        }

        info!(
            schedule_id = %schedule_id,
            namespace = %info.namespace,
            workflow = %info.action_workflow_name,
            "Schedule fired, starting workflow"
        );

        self.start_schedule_workflow(
            schedule_id,
            &info.namespace,
            &info.action_workflow_name,
            &info.action_task_queue,
            &info.action_args_json,
            &info.timeouts_json,
        )?;

        Ok(())
    }

    /// Update the next fire time for a single schedule in the heap.
    ///
    /// This is a targeted replacement for calling `reload_all_schedules` after
    /// every fire — it only queries and recalculates the one schedule that just
    /// fired, eliminating 36+ unnecessary DB queries per fire cycle.
    fn update_schedule_next_fire(
        &self,
        schedule_id: &str,
        heap: &mut BinaryHeap<Reverse<ScheduleEntry>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let now = Utc::now();

        // Load schedule info
        let conn = self.db.connection();
        let info: ScheduleInfo = match conn.query_row(
            "SELECT schedule_id, namespace, spec_type, cron_expressions, \\\n             interval_seconds, overlap_policy, action_workflow_name, \\\n             action_task_queue, action_args_json, timeouts_json \\\n             FROM schedules WHERE schedule_id = ?1",
            [schedule_id],
            |row| {
                let cron_json: Option<String> = row.get(3)?;
                let cron_expressions: Option<Vec<String>> =
                    cron_json.and_then(|j| serde_json::from_str(&j).ok());

                Ok(ScheduleInfo {
                    schedule_id: row.get(0)?,
                    namespace: row.get(1)?,
                    spec_type: row.get(2)?,
                    cron_expressions,
                    interval_seconds: row.get::<_, Option<i64>>(4)?.map(|s| s as u64),
                    overlap_policy: row.get(5)?,
                    action_workflow_name: row.get(6)?,
                    action_task_queue: row.get(7)?,
                    action_args_json: row.get(8)?,
                    timeouts_json: row.get(9)?,
                })
            },
        ) {
            Ok(info) => info,
            Err(_) => {
                debug!(schedule_id = %schedule_id, "Schedule not found (deleted), skipping heap update");
                return Ok(());
            }
        };

        // Calculate next fire time
        let next = match info.spec_type.as_str() {
            "cron" => self.calculate_next_cron_fire(&info, &now),
            "interval" => self.calculate_next_interval_fire(&info, &now)?,
            _ => return Ok(()),
        };

        if let Some(next_time) = next {
            heap.push(Reverse(ScheduleEntry {
                next_fire_time: next_time,
                schedule_id: schedule_id.to_string(),
            }));
        }

        Ok(())
    }

    /// Calculate the next fire time for a cron schedule.
    fn calculate_next_cron_fire(
        &self,
        info: &ScheduleInfo,
        _now: &chrono::DateTime<Utc>,
    ) -> Option<chrono::DateTime<Utc>> {
        let expressions = info.cron_expressions.as_ref()?;

        for expr in expressions {
            if let Ok(schedule) = cron::Schedule::from_str(expr) {
                if let Some(next_time) = schedule.upcoming(Utc).next() {
                    return Some(next_time);
                }
            } else {
                warn!(expression = %expr, "Invalid cron expression");
            }
        }
        None
    }

    /// Calculate the next fire time for an interval schedule.
    fn calculate_next_interval_fire(
        &self,
        info: &ScheduleInfo,
        now: &chrono::DateTime<Utc>,
    ) -> Result<Option<chrono::DateTime<Utc>>, Box<dyn std::error::Error + Send + Sync>> {
        let interval_secs = info.interval_seconds.unwrap_or(0);
        if interval_secs == 0 {
            return Ok(None);
        }

        let last_started = self.get_last_started(info.schedule_id.as_str())?;

        Ok(match last_started {
            Some(last) => {
                let next = last + chrono::Duration::seconds(interval_secs as i64);
                // Ensure we never schedule in the past — if the calculated
                // next fire time is already behind us, push it forward
                if next > *now {
                    Some(next)
                } else {
                    Some(*now + chrono::Duration::seconds(interval_secs as i64))
                }
            }
            None => {
                // No previous run, fire immediately
                Some(Utc::now())
            }
        })
    }

    /// Get the last workflow start time for a schedule.
    fn get_last_started(
        &self,
        schedule_id: &str,
    ) -> Result<Option<DateTime<Utc>>, Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.db.connection();
        let pattern = format!("schedule:{}-%", schedule_id);

        let last_started: Option<String> = conn.query_row(
            "SELECT MAX(started_at) FROM workflows WHERE workflow_id LIKE ?1",
            [&pattern],
            |row| row.get(0),
        )?;

        match last_started {
            Some(ts_str) => Ok(Some(parse_datetime(&ts_str)?)),
            None => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Helper functions (preserved from original implementation)
// ---------------------------------------------------------------------------

impl Scheduler {
    /// Check if an interval schedule should fire based on last workflow start time.
    fn check_interval(
        &self,
        schedule_id: &str,
        interval_secs: u64,
        now: &chrono::DateTime<Utc>,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.db.connection();

        let pattern = format!("schedule:{}-%", schedule_id);
        let last_started: Option<String> = conn
            .query_row(
                "SELECT MAX(started_at) FROM workflows WHERE workflow_id LIKE ?1",
                [&pattern],
                |row| row.get(0),
            )
            .unwrap_or(None);

        match last_started {
            Some(ts_str) => {
                let last = parse_datetime(&ts_str)?;
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
    fn check_running_workflow(
        &self,
        schedule_id: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
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
    fn terminate_existing(
        &self,
        schedule_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.db.connection();
        let pattern = format!("schedule:{}-%", schedule_id);

        let mut stmt = conn.prepare(
            "SELECT run_id FROM workflows WHERE workflow_id LIKE ?1 AND state IN ('pending', 'running')",
        )?;

        let ids: Vec<String> = stmt
            .query_map([&pattern], |row| row.get(0))?
            .filter_map(|r| r.ok())
            .collect();

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

        // args_json_str now holds the job_id as a plain string.
        let job_id = args_json_str.to_string();
        let timeouts: khronos_core::Timeouts =
            serde_json::from_str(timeouts_json_str).unwrap_or_default();

        // Parse args to JSON value for the workflow instance
        let args_json_value: serde_json::Value = if job_id.is_empty() {
            serde_json::json!({})
        } else {
            serde_json::json!({"job_id": job_id.clone()})
        };

        let run_id = uuid::Uuid::new_v4();
        let workflow_id = format!(
            "schedule:{}-{}",
            schedule_id,
            run_id.to_string().chars().take(8).collect::<String>()
        );

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

        // Create workflow steps based on built-in definitions.
        // Each step receives job_id as its single positional arg.
        // For agent jobs with lycus_origin_id in args, extract it as the job_id.
        let agent_job_id: Option<String> = if !job_id.is_empty() {
            // Try to parse as JSON with lycus_origin_id
            if let Ok(map) =
                serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&job_id)
            {
                map.get("lycus_origin_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            } else {
                // Plain string = job_id directly
                Some(job_id.clone())
            }
        } else {
            None
        };
        let _effective_job_id = agent_job_id.unwrap_or_default();
        let steps = get_workflow_definition(workflow_name);
        for (index, step_def) in steps.iter().enumerate() {
            let step_args_json: serde_json::Value = if job_id.is_empty() {
                serde_json::json!(null)
            } else {
                serde_json::json!(job_id.clone())
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

        info!(
            run_id = %run_id,
            workflow_name = %workflow_name,
            "Created workflow from schedule"
        );

        // Notify the engine that a new workflow was created
        if let Some(tx) = super::engine::engine_sender() {
            let _ = tx.send(super::engine::EngineEvent::WorkflowCreated {
                run_id: run_id.to_string(),
            });
        }

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
    // Normalize: lowercase and insert underscores before capitals (camelCase -> snake_case)
    let mut normalized = String::new();
    for (i, c) in name.chars().enumerate() {
        if c.is_uppercase() && i > 0 {
            normalized.push('_');
        }
        normalized.push(c.to_lowercase().next().unwrap_or(c));
    }
    match normalized.as_str() {
        "cron_job_workflow" => vec![
            khronos_core::ActivityStep {
                activity_name: "execute_cron_job".to_string(),
                args_template: std::collections::BTreeMap::new(),
                retry_policy: khronos_core::RetryPolicy {
                    maximum_attempts: 3,
                    initial_interval_secs: 2.0,
                },
                timeout_secs: 300,
                heartbeat_timeout_secs: Some(60),
            },
            khronos_core::ActivityStep {
                activity_name: "save_cron_output".to_string(),
                args_template: std::collections::BTreeMap::new(),
                retry_policy: khronos_core::RetryPolicy {
                    maximum_attempts: 2,
                    initial_interval_secs: 1.0,
                },
                timeout_secs: 60,
                heartbeat_timeout_secs: None,
            },
            khronos_core::ActivityStep {
                activity_name: "deliver_cron_result".to_string(),
                args_template: std::collections::BTreeMap::new(),
                retry_policy: khronos_core::RetryPolicy {
                    maximum_attempts: 3,
                    initial_interval_secs: 2.0,
                },
                timeout_secs: 120,
                heartbeat_timeout_secs: Some(30),
            },
            khronos_core::ActivityStep {
                activity_name: "mark_job_run_activity".to_string(),
                args_template: std::collections::BTreeMap::new(),
                retry_policy: khronos_core::RetryPolicy {
                    maximum_attempts: 1,
                    initial_interval_secs: 1.0,
                },
                timeout_secs: 30,
                heartbeat_timeout_secs: None,
            },
            khronos_core::ActivityStep {
                activity_name: "trigger_on_success_jobs".to_string(),
                args_template: std::collections::BTreeMap::new(),
                retry_policy: khronos_core::RetryPolicy {
                    maximum_attempts: 2,
                    initial_interval_secs: 1.0,
                },
                timeout_secs: 60,
                heartbeat_timeout_secs: None,
            },
        ],
        // All other workflows also use the cron activity pipeline.
        // The job_id is extracted from the schedule args and passed to execute_cron_job.
        _ => vec![
            khronos_core::ActivityStep {
                activity_name: "execute_cron_job".to_string(),
                args_template: std::collections::BTreeMap::new(),
                retry_policy: khronos_core::RetryPolicy {
                    maximum_attempts: 3,
                    initial_interval_secs: 2.0,
                },
                timeout_secs: 600,
                heartbeat_timeout_secs: Some(60),
            },
            khronos_core::ActivityStep {
                activity_name: "save_cron_output".to_string(),
                args_template: std::collections::BTreeMap::new(),
                retry_policy: khronos_core::RetryPolicy {
                    maximum_attempts: 2,
                    initial_interval_secs: 1.0,
                },
                timeout_secs: 60,
                heartbeat_timeout_secs: None,
            },
            khronos_core::ActivityStep {
                activity_name: "deliver_cron_result".to_string(),
                args_template: std::collections::BTreeMap::new(),
                retry_policy: khronos_core::RetryPolicy {
                    maximum_attempts: 3,
                    initial_interval_secs: 2.0,
                },
                timeout_secs: 120,
                heartbeat_timeout_secs: Some(30),
            },
            khronos_core::ActivityStep {
                activity_name: "mark_job_run_activity".to_string(),
                args_template: std::collections::BTreeMap::new(),
                retry_policy: khronos_core::RetryPolicy {
                    maximum_attempts: 1,
                    initial_interval_secs: 1.0,
                },
                timeout_secs: 30,
                heartbeat_timeout_secs: None,
            },
            khronos_core::ActivityStep {
                activity_name: "trigger_on_success_jobs".to_string(),
                args_template: std::collections::BTreeMap::new(),
                retry_policy: khronos_core::RetryPolicy {
                    maximum_attempts: 2,
                    initial_interval_secs: 1.0,
                },
                timeout_secs: 60,
                heartbeat_timeout_secs: None,
            },
        ],
    }
}

/// Parse a datetime string (ISO 8601 or SQLite format) into DateTime<Utc>.
fn parse_datetime(
    s: &str,
) -> Result<DateTime<Utc>, Box<dyn std::error::Error + Send + Sync>> {
    // Try naive formats first (no timezone info) — use NaiveDateTime then attach UTC
    for fmt in [
        "%Y-%m-%d %H:%M:%S%.f",   // SQLite with fractional seconds
        "%Y-%m-%d %H:%M:%S",       // SQLite datetime('now') default
        "%Y-%m-%dT%H:%M:%S%.f",    // ISO without timezone, with ms
        "%Y-%m-%dT%H:%M:%S",       // ISO without timezone
    ] {
        if let Ok(naive) = NaiveDateTime::parse_from_str(s, fmt) {
            return Ok(naive.and_utc());
        }
    }
    // Try formats with explicit timezone info
    for fmt in [
        "%Y-%m-%dT%H:%M:%S%.fZ",   // Rust chrono default (ISO with ms + Z)
        "%Y-%m-%dT%H:%M:%SZ",      // ISO without ms + Z
    ] {
        if let Ok(dt) = chrono::DateTime::parse_from_str(s, fmt) {
            return Ok(dt.with_timezone(&Utc));
        }
    }
    Err(format!("Failed to parse datetime: {}", s).into())
}
