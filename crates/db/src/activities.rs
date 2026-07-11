//! Workflow step and activity CRUD operations.

use chrono::{DateTime, Utc};
use khronos_core::{ActivityState, RetryPolicy, WorkflowState, WorkflowStepInstance};
use rusqlite::{params, Connection, Row};

/// Insert a workflow step into the database.
pub fn insert_workflow_step(
    conn: &Connection,
    step: &WorkflowStepInstance,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = workflow_state_to_str(step.state);
    let args_json = serde_json::to_string(&step.args_json)?;
    let retry_policy_json = serde_json::to_string(&step.retry_policy)?;
    let result_json = step.result_json.as_ref().map(|v| serde_json::to_string(v)).transpose()?;

    conn.execute(
        "INSERT INTO workflow_steps (id, workflow_run_id, step_index, activity_name, args_json, retry_policy_json, timeout_secs, heartbeat_timeout_secs, state, attempt, result_json, next_retry_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![
            step.id.to_string(),
            step.workflow_run_id.to_string(),
            step.step_index as i64,
            step.activity_name,
            args_json,
            retry_policy_json,
            step.timeout_secs as i64,
            step.heartbeat_timeout_secs.map(|v| v as i64),
            state,
            step.attempt as i64,
            result_json,
            format_datetime(step.next_retry_at),
        ],
    )?;

    Ok(())
}

/// Update the state of a workflow step.
pub fn update_step_state(
    conn: &Connection,
    step_id: &str,
    new_state: WorkflowState,
    attempt: Option<u32>,
    result_json: Option<&serde_json::Value>,
    next_retry_at: Option<DateTime<Utc>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = workflow_state_to_str(new_state);
    let result = result_json.map(|v| serde_json::to_string(v)).transpose()?;
    let retry_at = format_datetime(next_retry_at);

    conn.execute(
        "UPDATE workflow_steps SET state = ?1, attempt = COALESCE(?2, attempt), result_json = ?3, next_retry_at = ?4 WHERE id = ?5",
        params![state, attempt.map(|a| a as i64), result, retry_at, step_id],
    )?;

    Ok(())
}

/// Get pending or retried steps that are ready to execute (next_retry_at <= now).
pub fn get_pending_steps(
    conn: &Connection,
) -> Result<Vec<WorkflowStepInstance>, Box<dyn std::error::Error + Send + Sync>> {
    let mut stmt = conn.prepare(
        "SELECT id, workflow_run_id, step_index, activity_name, args_json, retry_policy_json, timeout_secs, heartbeat_timeout_secs, state, attempt, result_json, next_retry_at FROM workflow_steps WHERE (state = 'pending' AND next_retry_at IS NULL) OR (state IN ('pending', 'retried') AND next_retry_at <= datetime('now')) ORDER BY created_at ASC",
    )?;

    let mut rows = stmt.query(params![])?;
    let mut result = Vec::new();
    loop {
        match rows.next()? {
            Some(row) => result.push(row_to_step(&row)?),
            None => break,
        }
    }
    Ok(result)
}

/// Get the next pending step for a running workflow.
pub fn get_next_step_for_workflow(
    conn: &Connection,
    workflow_run_id: &str,
) -> Result<Option<WorkflowStepInstance>, Box<dyn std::error::Error + Send + Sync>> {
    let mut stmt = conn.prepare(
        "SELECT id, workflow_run_id, step_index, activity_name, args_json, retry_policy_json, timeout_secs, heartbeat_timeout_secs, state, attempt, result_json, next_retry_at FROM workflow_steps WHERE workflow_run_id = ?1 AND state IN ('pending', 'retried') ORDER BY step_index ASC LIMIT 1",
    )?;

    let mut rows = stmt.query(params![workflow_run_id])?;
    loop {
        match rows.next()? {
            Some(row) => return Ok(Some(row_to_step(&row)?)),
            None => break,
        }
    }
    Ok(None)
}

/// Insert an activity attempt record.
pub fn insert_activity_attempt(
    conn: &Connection,
    id: &str,
    step_id: &str,
    attempt: u32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    conn.execute(
        "INSERT INTO activities (id, step_id, attempt, state, started_at) VALUES (?1, ?2, ?3, 'running', datetime('now'))",
        params![id, step_id, attempt as i64],
    )?;

    Ok(())
}

/// Update the state of an activity (completion or failure).
pub fn update_activity_state(
    conn: &Connection,
    id: &str,
    new_state: ActivityState,
    result_json: Option<&serde_json::Value>,
    error_message: Option<&str>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = activity_state_to_str(new_state);
    let result = result_json.map(|v| serde_json::to_string(v)).transpose()?;

    conn.execute(
        "UPDATE activities SET state = ?1, completed_at = datetime('now'), result_json = ?2, error_message = ?3 WHERE id = ?4",
        params![state, result, error_message, id],
    )?;

    Ok(())
}

/// Update the heartbeat timestamp for a running activity.
pub fn heartbeat_update(conn: &Connection, id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let rows = conn.execute(
        "UPDATE activities SET last_heartbeat_at = datetime('now') WHERE id = ?1 AND state = 'running'",
        params![id],
    )?;

    if rows == 0 {
        return Err("Activity not found or not in running state".into());
    }

    Ok(())
}

/// Get all currently running activities (for heartbeat timeout checks).
pub fn get_running_activities(
    conn: &Connection,
) -> Result<Vec<RunningActivity>, Box<dyn std::error::Error + Send + Sync>> {
    let mut stmt = conn.prepare(
        "SELECT a.id, a.step_id, a.attempt, a.started_at, a.last_heartbeat_at, ws.workflow_run_id, ws.activity_name, ws.heartbeat_timeout_secs FROM activities a JOIN workflow_steps ws ON a.step_id = ws.id WHERE a.state = 'running'",
    )?;

    let mut rows = stmt.query(params![])?;
    let mut result = Vec::new();
    loop {
        match rows.next()? {
            Some(row) => {
                result.push(RunningActivity {
                    id: row.get(0)?,
                    step_id: row.get(1)?,
                    attempt: row.get::<_, i64>(2)? as u32,
                    started_at_str: row.get(3)?,
                    last_heartbeat_at_str: row.get(4)?,
                    workflow_run_id: row.get(5)?,
                    activity_name: row.get(6)?,
                    heartbeat_timeout_secs: row.get::<_, Option<i64>>(7)?.map(|v| v as u64),
                });
            }
            None => break,
        }
    }
    Ok(result)
}

/// A running activity with its heartbeat information.
#[derive(Debug)]
pub struct RunningActivity {
    /// Activity ID (UUID string).
    pub id: String,
    /// Parent step ID (UUID string).
    pub step_id: String,
    /// Current attempt number.
    pub attempt: u32,
    /// When the activity started (ISO 8601 string).
    pub started_at_str: Option<String>,
    /// Last heartbeat timestamp (ISO 8601 string).
    pub last_heartbeat_at_str: Option<String>,
    /// Parent workflow run ID (UUID string).
    pub workflow_run_id: String,
    /// Activity name.
    pub activity_name: String,
    /// Heartbeat timeout in seconds.
    pub heartbeat_timeout_secs: Option<u64>,
}

/// Convert a database row to a WorkflowStepInstance.
fn row_to_step(row: &Row) -> Result<WorkflowStepInstance, Box<dyn std::error::Error + Send + Sync>> {
    let id_str: String = row.get(0)?;
    let workflow_run_id_str: String = row.get(1)?;
    let step_index: i64 = row.get(2)?;
    let activity_name: String = row.get(3)?;
    let args_json_str: String = row.get(4)?;
    let retry_policy_json: String = row.get(5)?;
    let timeout_secs: i64 = row.get(6)?;
    let heartbeat_timeout_secs: Option<i64> = row.get(7)?;
    let state_str: String = row.get(8)?;
    let attempt: i64 = row.get(9)?;
    let result_json_str: Option<String> = row.get(10)?;
    let next_retry_at_str: Option<String> = row.get(11)?;

    let id = uuid::Uuid::parse_str(&id_str)
        .map_err(|e| format!("Invalid step ID UUID: {}", e))?;
    let workflow_run_id = uuid::Uuid::parse_str(&workflow_run_id_str)
        .map_err(|e| format!("Invalid workflow_run_id UUID: {}", e))?;

    let state = str_to_workflow_state(&state_str)?;
    let args_json: serde_json::Value = serde_json::from_str(&args_json_str)?;
    let retry_policy: RetryPolicy = serde_json::from_str(&retry_policy_json)?;
    let result_json = result_json_str.map(|s| serde_json::from_str(&s)).transpose()?;
    let next_retry_at = next_retry_at_str.as_deref().map(parse_datetime).transpose()?;

    Ok(WorkflowStepInstance {
        id,
        workflow_run_id,
        step_index: step_index as usize,
        activity_name,
        args_json,
        retry_policy,
        timeout_secs: timeout_secs as u64,
        heartbeat_timeout_secs: heartbeat_timeout_secs.map(|v| v as u64),
        state,
        attempt: attempt as u32,
        result_json,
        next_retry_at,
    })
}

/// Convert WorkflowState to its database string representation.
fn workflow_state_to_str(state: WorkflowState) -> &'static str {
    match state {
        WorkflowState::Pending => "pending",
        WorkflowState::Running => "running",
        WorkflowState::Completed => "completed",
        WorkflowState::Failed => "failed",
        WorkflowState::Cancelled => "cancelled",
    }
}

/// Convert a database string to WorkflowState.
fn str_to_workflow_state(s: &str) -> Result<WorkflowState, Box<dyn std::error::Error + Send + Sync>> {
    match s {
        "pending" => Ok(WorkflowState::Pending),
        "running" => Ok(WorkflowState::Running),
        "completed" => Ok(WorkflowState::Completed),
        "failed" => Ok(WorkflowState::Failed),
        "cancelled" => Ok(WorkflowState::Cancelled),
        other => Err(format!("Unknown workflow state: {}", other).into()),
    }
}

/// Convert ActivityState to its database string representation.
fn activity_state_to_str(state: ActivityState) -> &'static str {
    match state {
        ActivityState::Pending => "pending",
        ActivityState::Running => "running",
        ActivityState::Completed => "completed",
        ActivityState::Failed => "failed",
        ActivityState::Retried => "retried",
    }
}

/// Format an optional DateTime<Utc> for storage in SQLite.
fn format_datetime(dt: Option<DateTime<Utc>>) -> Option<String> {
    dt.map(|d| d.format("%Y-%m-%d %H:%M:%S").to_string())
}

/// Parse an ISO 8601 datetime string into a DateTime<Utc>.
fn parse_datetime(s: &str) -> Result<DateTime<Utc>, Box<dyn std::error::Error + Send + Sync>> {
    // Try multiple formats since SQLite can return different representations
    for fmt in [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
    ] {
        if let Ok(dt) = chrono::DateTime::parse_from_str(s, fmt) {
            return Ok(dt.with_timezone(&Utc));
        }
    }
    // Try naive format and attach UTC
    for fmt in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
    ] {
        if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
            return Ok(naive.and_utc());
        }
    }
    Err(format!("Failed to parse datetime: {}", s).into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use khronos_core::{RetryPolicy, WorkflowState};
    use rusqlite::Connection;
    use uuid::Uuid;

    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        crate::schema::migrate(&conn).unwrap();
        conn
    }

    /// Insert a parent workflow so FK constraints are satisfied.
    fn insert_parent_workflow(conn: &Connection, run_id: Uuid) {
        conn.execute(
            "INSERT INTO workflows (run_id, workflow_id, name, task_queue, state, args_json, namespace) VALUES (?1, ?2, 'test_wf', 'default', 'pending', '{}', 'default')",
            rusqlite::params![run_id.to_string(), format!("wf-{}", run_id)],
        ).unwrap();
    }

    fn make_step(workflow_run_id: Uuid, index: usize) -> WorkflowStepInstance {
        WorkflowStepInstance {
            id: Uuid::new_v4(),
            workflow_run_id,
            step_index: index,
            activity_name: "test_activity".to_string(),
            args_json: serde_json::json!({"input": "data"}),
            retry_policy: RetryPolicy {
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

    #[test]
    fn test_insert_and_get_step() {
        let conn = test_conn();
        let run_id = Uuid::new_v4();
        insert_parent_workflow(&conn, run_id);
        let step = make_step(run_id, 0);
        insert_workflow_step(&conn, &step).unwrap();

        // Verify via get_next_step_for_workflow
        let result = get_next_step_for_workflow(&conn, &run_id.to_string())
            .unwrap()
            .expect("Step should exist");
        assert_eq!(result.step_index, 0);
        assert_eq!(result.activity_name, "test_activity");
        assert_eq!(result.state, WorkflowState::Pending);
    }

    #[test]
    fn test_get_next_step_returns_lowest_index() {
        let conn = test_conn();
        let run_id = Uuid::new_v4();
        insert_parent_workflow(&conn, run_id);

        // Insert steps out of order
        insert_workflow_step(&conn, &make_step(run_id, 2)).unwrap();
        insert_workflow_step(&conn, &make_step(run_id, 0)).unwrap();
        insert_workflow_step(&conn, &make_step(run_id, 1)).unwrap();

        let result = get_next_step_for_workflow(&conn, &run_id.to_string())
            .unwrap()
            .expect("Step should exist");
        assert_eq!(result.step_index, 0);
    }

    #[test]
    fn test_get_pending_steps() {
        let conn = test_conn();
        let run_id1 = Uuid::new_v4();
        insert_parent_workflow(&conn, run_id1);
        let run_id2 = Uuid::new_v4();
        insert_parent_workflow(&conn, run_id2);

        insert_workflow_step(&conn, &make_step(run_id1, 0)).unwrap();
        insert_workflow_step(&conn, &make_step(run_id2, 0)).unwrap();

        let pending = get_pending_steps(&conn).unwrap();
        assert_eq!(pending.len(), 2);
    }

    #[test]
    fn test_update_step_state() {
        let conn = test_conn();
        let run_id = Uuid::new_v4();
        insert_parent_workflow(&conn, run_id);
        let step = make_step(run_id, 0);
        insert_workflow_step(&conn, &step).unwrap();

        // Update to running
        update_step_state(
            &conn,
            &step.id.to_string(),
            WorkflowState::Running,
            Some(1),
            None,
            None,
        )
        .unwrap();

        let result = get_next_step_for_workflow(&conn, &run_id.to_string()).unwrap();
        assert!(result.is_none()); // Running steps are not returned as pending
    }

    #[test]
    fn test_update_step_state_with_retry() {
        let conn = test_conn();
        let run_id = Uuid::new_v4();
        insert_parent_workflow(&conn, run_id);
        let step = make_step(run_id, 0);
        insert_workflow_step(&conn, &step).unwrap();

        // Schedule a retry in the past using SQL directly to avoid format issues
        conn.execute(
            "UPDATE workflow_steps SET state = 'pending', attempt = 2, next_retry_at = datetime('now', '-1 minute') WHERE id = ?1",
            rusqlite::params![step.id.to_string()],
        ).unwrap();

        // Verify the step is returned by get_pending_steps (next_retry_at <= now)
        let pending = get_pending_steps(&conn).unwrap();
        assert!(pending.iter().any(|s| s.id == step.id));
    }

    #[test]
    fn test_insert_activity_attempt() {
        let conn = test_conn();
        let run_id = Uuid::new_v4();
        insert_parent_workflow(&conn, run_id);
        let step = make_step(run_id, 0);
        insert_workflow_step(&conn, &step).unwrap();

        let activity_id = "activity-1";
        insert_activity_attempt(&conn, activity_id, &step.id.to_string(), 1).unwrap();

        // Verify via get_running_activities
        let running = get_running_activities(&conn).unwrap();
        assert_eq!(running.len(), 1);
        assert_eq!(running[0].id, activity_id);
    }

    #[test]
    fn test_update_activity_state() {
        let conn = test_conn();
        let run_id = Uuid::new_v4();
        insert_parent_workflow(&conn, run_id);
        let step = make_step(run_id, 0);
        insert_workflow_step(&conn, &step).unwrap();

        let activity_id = "activity-1";
        insert_activity_attempt(&conn, activity_id, &step.id.to_string(), 1).unwrap();

        // Complete the activity
        update_activity_state(
            &conn,
            activity_id,
            ActivityState::Completed,
            Some(&serde_json::json!({"result": "ok"})),
            None,
        )
        .unwrap();

        let running = get_running_activities(&conn).unwrap();
        assert!(running.is_empty()); // No more running activities
    }

    #[test]
    fn test_heartbeat_update() {
        let conn = test_conn();
        let run_id = Uuid::new_v4();
        insert_parent_workflow(&conn, run_id);
        let step = make_step(run_id, 0);
        insert_workflow_step(&conn, &step).unwrap();

        let activity_id = "activity-1";
        insert_activity_attempt(&conn, activity_id, &step.id.to_string(), 1).unwrap();

        // Heartbeat should succeed for running activity
        heartbeat_update(&conn, activity_id).unwrap();
    }

    #[test]
    fn test_heartbeat_nonexistent() {
        let conn = test_conn();
        assert!(heartbeat_update(&conn, "nonexistent").is_err());
    }

    #[test]
    fn test_get_running_activities_empty() {
        let conn = test_conn();
        let running = get_running_activities(&conn).unwrap();
        assert!(running.is_empty());
    }

    #[test]
    fn test_step_completion_with_result() {
        let conn = test_conn();
        let run_id = Uuid::new_v4();
        insert_parent_workflow(&conn, run_id);
        let step = make_step(run_id, 0);
        insert_workflow_step(&conn, &step).unwrap();

        // Complete the step with a result
        update_step_state(
            &conn,
            &step.id.to_string(),
            WorkflowState::Completed,
            Some(1),
            Some(&serde_json::json!({"output": "done"})),
            None,
        )
        .unwrap();

        // Verify no pending steps remain for this workflow
        let result = get_next_step_for_workflow(&conn, &run_id.to_string()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_multiple_steps_in_workflow() {
        let conn = test_conn();
        let run_id = Uuid::new_v4();
        insert_parent_workflow(&conn, run_id);

        // Insert 3 steps
        for i in 0..3 {
            insert_workflow_step(&conn, &make_step(run_id, i)).unwrap();
        }

        let pending = get_pending_steps(&conn).unwrap();
        assert_eq!(pending.len(), 3);

        // Complete step 0
        update_step_state(
            &conn,
            &pending[0].id.to_string(),
            WorkflowState::Completed,
            Some(1),
            None,
            None,
        )
        .unwrap();

        let pending = get_pending_steps(&conn).unwrap();
        assert_eq!(pending.len(), 2);
    }
}
