//! Workflow CRUD operations.

use chrono::{DateTime, NaiveDateTime, Utc};
use khronos_core::{Timeouts, WorkflowInstance, WorkflowState};
use rusqlite::{params, Connection, Row};

/// Insert a new workflow instance into the database.
pub fn insert_workflow(
    conn: &Connection,
    wf: &WorkflowInstance,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = workflow_state_to_str(wf.state);
    let args_json = serde_json::to_string(&wf.args_json)?;
    let result_json = wf.result_json.as_ref().map(|v| serde_json::to_string(v)).transpose()?;

    conn.execute(
        "INSERT INTO workflows (run_id, workflow_id, name, task_queue, state, args_json, result_json, execution_timeout_secs, run_timeout_secs, task_timeout_secs, started_at, completed_at, namespace) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        params![
            wf.run_id.to_string(),
            wf.workflow_id,
            wf.name,
            wf.task_queue,
            state,
            args_json,
            result_json,
            wf.timeouts.execution_timeout_secs.map(|v| v as i64),
            wf.timeouts.run_timeout_secs.map(|v| v as i64),
            wf.timeouts.task_timeout_secs.map(|v| v as i64),
            format_datetime(wf.started_at),
            format_datetime(wf.completed_at),
            wf.namespace,
        ],
    )?;

    Ok(())
}

/// Update the state of a workflow, optionally setting result and completion time.
pub fn update_workflow_state(
    conn: &Connection,
    run_id: &str,
    new_state: WorkflowState,
    result_json: Option<&serde_json::Value>,
    completed_at: Option<DateTime<Utc>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = workflow_state_to_str(new_state);
    let result = result_json.map(|v| serde_json::to_string(v)).transpose()?;
    let completed = format_datetime(completed_at);

    conn.execute(
        "UPDATE workflows SET state = ?1, result_json = ?2, completed_at = ?3 WHERE run_id = ?4",
        params![state, result, completed, run_id],
    )?;

    Ok(())
}

/// Get a workflow by its run ID.
pub fn get_workflow(
    conn: &Connection,
    run_id: &str,
) -> Result<Option<WorkflowInstance>, Box<dyn std::error::Error + Send + Sync>> {
    let mut stmt = conn.prepare(
        "SELECT run_id, workflow_id, name, task_queue, state, args_json, result_json, execution_timeout_secs, run_timeout_secs, task_timeout_secs, started_at, completed_at, namespace FROM workflows WHERE run_id = ?1",
    )?;

    let mut rows = stmt.query(params![run_id])?;
    loop {
        match rows.next()? {
            Some(row) => return Ok(Some(row_to_workflow(&row)?)),
            None => break,
        }
    }
    Ok(None)
}

/// List workflows in a namespace, optionally filtered by state.
pub fn list_workflows(
    conn: &Connection,
    namespace: &str,
    state_filter: Option<WorkflowState>,
) -> Result<Vec<WorkflowInstance>, Box<dyn std::error::Error + Send + Sync>> {
    let query = if state_filter.is_some() {
        "SELECT run_id, workflow_id, name, task_queue, state, args_json, result_json, execution_timeout_secs, run_timeout_secs, task_timeout_secs, started_at, completed_at, namespace FROM workflows WHERE namespace = ?1 AND state = ?2 ORDER BY started_at DESC"
    } else {
        "SELECT run_id, workflow_id, name, task_queue, state, args_json, result_json, execution_timeout_secs, run_timeout_secs, task_timeout_secs, started_at, completed_at, namespace FROM workflows WHERE namespace = ?1 ORDER BY started_at DESC"
    };

    let mut stmt = conn.prepare(query)?;

    let mut rows = if let Some(state) = state_filter {
        stmt.query(params![namespace, workflow_state_to_str(state)])?
    } else {
        stmt.query(params![namespace])?
    };

    let mut result = Vec::new();
    loop {
        match rows.next()? {
            Some(row) => result.push(row_to_workflow(&row)?),
            None => break,
        }
    }
    Ok(result)
}

/// Cancel a workflow by setting its state to 'cancelled'.
pub fn cancel_workflow(
    conn: &Connection,
    run_id: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let rows = conn.execute(
        "UPDATE workflows SET state = 'cancelled', completed_at = datetime('now') WHERE run_id = ?1 AND state IN ('pending', 'running')",
        params![run_id],
    )?;

    if rows == 0 {
        return Err("Workflow not found or already in terminal state".into());
    }

    Ok(())
}

/// Convert a database row to a WorkflowInstance.
fn row_to_workflow(row: &Row) -> Result<WorkflowInstance, Box<dyn std::error::Error + Send + Sync>> {
    let run_id_str: String = row.get(0)?;
    let workflow_id: String = row.get(1)?;
    let name: String = row.get(2)?;
    let task_queue: String = row.get(3)?;
    let state_str: String = row.get(4)?;
    let args_json_str: String = row.get(5)?;
    let result_json_str: Option<String> = row.get(6)?;
    let execution_timeout_secs: Option<i64> = row.get(7)?;
    let run_timeout_secs: Option<i64> = row.get(8)?;
    let task_timeout_secs: Option<i64> = row.get(9)?;
    let started_at_str: Option<String> = row.get(10)?;
    let completed_at_str: Option<String> = row.get(11)?;
    let namespace: String = row.get(12)?;

    let run_id = uuid::Uuid::parse_str(&run_id_str)
        .map_err(|e| format!("Invalid run_id UUID: {}", e))?;

    let state = str_to_workflow_state(&state_str)?;
    let args_json: serde_json::Value = serde_json::from_str(&args_json_str)?;
    let result_json = result_json_str.map(|s| serde_json::from_str(&s)).transpose()?;

    let timeouts = Timeouts {
        execution_timeout_secs: execution_timeout_secs.map(|v| v as u64),
        run_timeout_secs: run_timeout_secs.map(|v| v as u64),
        task_timeout_secs: task_timeout_secs.map(|v| v as u64),
    };

    let started_at = started_at_str.as_deref().map(parse_datetime).transpose()?;
    let completed_at = completed_at_str.as_deref().map(parse_datetime).transpose()?;

    Ok(WorkflowInstance {
        run_id,
        workflow_id,
        name,
        task_queue,
        state,
        args_json,
        result_json,
        timeouts,
        started_at,
        completed_at,
        namespace,
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

/// Format an optional DateTime<Utc> for storage in SQLite.
fn format_datetime(dt: Option<DateTime<Utc>>) -> Option<String> {
    dt.map(|d| d.format("%Y-%m-%d %H:%M:%S").to_string())
}

/// Parse a datetime string (ISO 8601 or SQLite format) into DateTime<Utc>.
fn parse_datetime(s: &str) -> Result<DateTime<Utc>, Box<dyn std::error::Error + Send + Sync>> {
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
