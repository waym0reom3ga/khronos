//! Schedule CRUD operations.

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{DateTime, NaiveDateTime, Utc};
use khronos_core::{OverlapPolicy, Schedule, ScheduleSpec, Timeouts, WorkflowAction};
use rusqlite::{params, Connection, Row};

/// Insert a new schedule into the database.
pub fn insert_schedule(
    conn: &Connection,
    schedule: &Schedule,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (spec_type, cron_expressions, interval_seconds) = match &schedule.spec {
        ScheduleSpec::Cron(exprs) => ("cron", Some(serde_json::to_string(exprs)?), None),
        ScheduleSpec::Interval(duration) => ("interval", None, Some(duration.as_secs() as i64)),
    };

    let overlap_policy = match &schedule.policy {
        OverlapPolicy::Skip => "skip",
        OverlapPolicy::Buffer(_) => "buffer",
        OverlapPolicy::TerminateExisting => "terminate",
    };

    let action_args_json = serde_json::to_string(&schedule.action.args)?;
    let timeouts_json = serde_json::to_string(&schedule.action.timeouts)?;

    conn.execute(
        "INSERT INTO schedules (schedule_id, namespace, spec_type, cron_expressions, interval_seconds, overlap_policy, action_workflow_name, action_task_queue, action_args_json, action_id, timeouts_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            schedule.schedule_id,
            schedule.namespace,
            spec_type,
            cron_expressions,
            interval_seconds,
            overlap_policy,
            schedule.action.workflow_name,
            schedule.action.task_queue,
            action_args_json,
            schedule.action.id,
            timeouts_json,
        ],
    )?;

    Ok(())
}

/// Update the spec fields of an existing schedule.
pub fn update_schedule_spec(
    conn: &Connection,
    schedule_id: &str,
    namespace: &str,
    schedule: &Schedule,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (spec_type, cron_expressions, interval_seconds) = match &schedule.spec {
        ScheduleSpec::Cron(exprs) => ("cron", Some(serde_json::to_string(exprs)?), None),
        ScheduleSpec::Interval(duration) => ("interval", None, Some(duration.as_secs() as i64)),
    };

    let overlap_policy = match &schedule.policy {
        OverlapPolicy::Skip => "skip",
        OverlapPolicy::Buffer(_) => "buffer",
        OverlapPolicy::TerminateExisting => "terminate",
    };

    let action_args_json = serde_json::to_string(&schedule.action.args)?;
    let timeouts_json = serde_json::to_string(&schedule.action.timeouts)?;

    conn.execute(
        "UPDATE schedules SET spec_type = ?1, cron_expressions = ?2, interval_seconds = ?3, overlap_policy = ?4, action_workflow_name = ?5, action_task_queue = ?6, action_args_json = ?7, action_id = ?8, timeouts_json = ?9, updated_at = datetime('now') WHERE schedule_id = ?10 AND namespace = ?11",
        params![
            spec_type,
            cron_expressions,
            interval_seconds,
            overlap_policy,
            schedule.action.workflow_name,
            schedule.action.task_queue,
            action_args_json,
            schedule.action.id,
            timeouts_json,
            schedule_id,
            namespace,
        ],
    )?;

    Ok(())
}

/// Delete a schedule by its ID and namespace.
pub fn delete_schedule(
    conn: &Connection,
    schedule_id: &str,
    namespace: &str,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let rows = conn.execute(
        "DELETE FROM schedules WHERE schedule_id = ?1 AND namespace = ?2",
        params![schedule_id, namespace],
    )?;
    Ok(rows)
}

/// Get a single schedule by its ID.
pub fn get_schedule(
    conn: &Connection,
    schedule_id: &str,
) -> Result<Option<Schedule>, Box<dyn std::error::Error + Send + Sync>> {
    let mut stmt = conn.prepare(
        "SELECT schedule_id, namespace, spec_type, cron_expressions, interval_seconds, overlap_policy, action_workflow_name, action_task_queue, action_args_json, action_id, timeouts_json, created_at, updated_at FROM schedules WHERE schedule_id = ?1",
    )?;

    let mut rows = stmt.query(params![schedule_id])?;
    loop {
        match rows.next()? {
            Some(row) => return Ok(Some(row_to_schedule(&row)?)),
            None => break,
        }
    }
    Ok(None)
}

/// List all schedules in a namespace.
pub fn list_schedules(
    conn: &Connection,
    namespace: &str,
) -> Result<Vec<Schedule>, Box<dyn std::error::Error + Send + Sync>> {
    let mut stmt = conn.prepare(
        "SELECT schedule_id, namespace, spec_type, cron_expressions, interval_seconds, overlap_policy, action_workflow_name, action_task_queue, action_args_json, action_id, timeouts_json, created_at, updated_at FROM schedules WHERE namespace = ?1 ORDER BY created_at ASC",
    )?;

    let mut rows = stmt.query(params![namespace])?;
    let mut result = Vec::new();
    loop {
        match rows.next()? {
            Some(row) => result.push(row_to_schedule(&row)?),
            None => break,
        }
    }
    Ok(result)
}

/// Convert a database row to a Schedule.
fn row_to_schedule(row: &Row) -> Result<Schedule, Box<dyn std::error::Error + Send + Sync>> {
    let schedule_id: String = row.get(0)?;
    let namespace: String = row.get(1)?;
    let spec_type: String = row.get(2)?;

    // Parse ScheduleSpec from DB columns
    let spec = if spec_type == "cron" {
        let cron_expressions: Option<String> = row.get(3)?;
        match cron_expressions {
            Some(json) => ScheduleSpec::Cron(serde_json::from_str(&json)?),
            None => ScheduleSpec::Cron(Vec::new()),
        }
    } else {
        let interval_seconds: Option<i64> = row.get(4)?;
        match interval_seconds {
            Some(secs) => ScheduleSpec::Interval(Duration::from_secs(secs as u64)),
            None => ScheduleSpec::Interval(Duration::ZERO),
        }
    };

    // Parse OverlapPolicy from DB column
    let overlap_policy_str: String = row.get(5)?;
    let policy = match overlap_policy_str.as_str() {
        "skip" => OverlapPolicy::Skip,
        "buffer" => OverlapPolicy::Buffer(1),
        "terminate" => OverlapPolicy::TerminateExisting,
        other => return Err(format!("Unknown overlap_policy: {}", other).into()),
    };

    // Parse WorkflowAction from DB columns
    let action_workflow_name: String = row.get(6)?;
    let action_task_queue: String = row.get(7)?;
    let action_args_json: String = row.get(8)?;
    let action_id: Option<String> = row.get(9)?;
    let timeouts_json: String = row.get(10)?;

    let args: BTreeMap<String, String> = serde_json::from_str(&action_args_json)?;
    let timeouts: Timeouts = serde_json::from_str(&timeouts_json)?;

    let action = WorkflowAction {
        workflow_name: action_workflow_name,
        task_queue: action_task_queue,
        args,
        id: action_id.unwrap_or_default(),
        timeouts,
    };

    // Parse timestamps
    let created_at_str: String = row.get(11)?;
    let updated_at_str: String = row.get(12)?;

    let created_at: DateTime<Utc> = parse_datetime(&created_at_str)?;
    let updated_at: DateTime<Utc> = parse_datetime(&updated_at_str)?;

    Ok(Schedule {
        schedule_id,
        namespace,
        spec,
        action,
        policy,
        created_at,
        updated_at,
    })
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
