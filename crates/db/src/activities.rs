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
    let dt = chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")?;
    Ok(dt.with_timezone(&Utc))
}
