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

#[cfg(test)]
mod tests {
    use super::*;
    use khronos_core::{OverlapPolicy, ScheduleSpec, Timeouts};
    use rusqlite::Connection;

    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        crate::schema::migrate(&conn).unwrap();
        conn
    }

    fn make_cron_schedule(id: &str) -> Schedule {
        let now = Utc::now();
        Schedule {
            schedule_id: id.to_string(),
            namespace: "default".to_string(),
            spec: ScheduleSpec::Cron(vec!["0 * * * *".to_string()]),
            action: WorkflowAction {
                workflow_name: "test_wf".to_string(),
                args: BTreeMap::new(),
                task_queue: "default".to_string(),
                id: "action-1".to_string(),
                timeouts: Timeouts::default(),
            },
            policy: OverlapPolicy::Skip,
            created_at: now,
            updated_at: now,
        }
    }

    fn make_interval_schedule(id: &str) -> Schedule {
        let now = Utc::now();
        Schedule {
            schedule_id: id.to_string(),
            namespace: "default".to_string(),
            spec: ScheduleSpec::Interval(std::time::Duration::from_secs(300)),
            action: WorkflowAction {
                workflow_name: "interval_wf".to_string(),
                args: BTreeMap::new(),
                task_queue: "default".to_string(),
                id: "action-2".to_string(),
                timeouts: Timeouts::default(),
            },
            policy: OverlapPolicy::Buffer(5),
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn test_insert_and_get_cron_schedule() {
        let conn = test_conn();
        let schedule = make_cron_schedule("cron-1");
        insert_schedule(&conn, &schedule).unwrap();

        let result = get_schedule(&conn, "cron-1").unwrap().expect("Schedule should exist");
        assert_eq!(result.schedule_id, "cron-1");
        assert_eq!(result.namespace, "default");
        match result.spec {
            ScheduleSpec::Cron(exprs) => {
                assert_eq!(exprs.len(), 1);
                assert_eq!(exprs[0], "0 * * * *");
            }
            _ => panic!("Expected Cron spec"),
        }
    }

    #[test]
    fn test_insert_and_get_interval_schedule() {
        let conn = test_conn();
        let schedule = make_interval_schedule("interval-1");
        insert_schedule(&conn, &schedule).unwrap();

        let result = get_schedule(&conn, "interval-1").unwrap().expect("Schedule should exist");
        assert_eq!(result.schedule_id, "interval-1");
        match result.spec {
            ScheduleSpec::Interval(dur) => assert_eq!(dur.as_secs(), 300),
            _ => panic!("Expected Interval spec"),
        }
    }

    #[test]
    fn test_get_nonexistent_schedule() {
        let conn = test_conn();
        let result = get_schedule(&conn, "nonexistent").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_list_schedules_empty() {
        let conn = test_conn();
        let schedules = list_schedules(&conn, "default").unwrap();
        assert!(schedules.is_empty());
    }

    #[test]
    fn test_list_schedules_multiple() {
        let conn = test_conn();
        insert_schedule(&conn, &make_cron_schedule("cron-1")).unwrap();
        insert_schedule(&conn, &make_interval_schedule("interval-1")).unwrap();

        let schedules = list_schedules(&conn, "default").unwrap();
        assert_eq!(schedules.len(), 2);
    }

    #[test]
    fn test_list_schedules_namespace_filter() {
        let conn = test_conn();
        insert_schedule(&conn, &make_cron_schedule("cron-1")).unwrap();

        // Insert schedule in different namespace
        let mut ns_schedule = make_interval_schedule("interval-ns");
        ns_schedule.namespace = "other".to_string();
        insert_schedule(&conn, &ns_schedule).unwrap();

        assert_eq!(list_schedules(&conn, "default").unwrap().len(), 1);
        assert_eq!(list_schedules(&conn, "other").unwrap().len(), 1);
    }

    #[test]
    fn test_update_schedule_spec() {
        let conn = test_conn();
        let schedule = make_cron_schedule("update-test");
        insert_schedule(&conn, &schedule).unwrap();

        // Update to interval spec
        let mut updated = schedule.clone();
        updated.spec = ScheduleSpec::Interval(std::time::Duration::from_secs(600));
        update_schedule_spec(&conn, "update-test", "default", &updated).unwrap();

        let result = get_schedule(&conn, "update-test").unwrap().expect("Schedule should exist");
        match result.spec {
            ScheduleSpec::Interval(dur) => assert_eq!(dur.as_secs(), 600),
            _ => panic!("Expected Interval spec after update"),
        }
    }

    #[test]
    fn test_delete_schedule() {
        let conn = test_conn();
        insert_schedule(&conn, &make_cron_schedule("delete-test")).unwrap();

        assert!(get_schedule(&conn, "delete-test").unwrap().is_some());

        let rows = delete_schedule(&conn, "delete-test", "default").unwrap();
        assert_eq!(rows, 1);

        assert!(get_schedule(&conn, "delete-test").unwrap().is_none());
    }

    #[test]
    fn test_delete_nonexistent_schedule() {
        let conn = test_conn();
        let rows = delete_schedule(&conn, "nonexistent", "default").unwrap();
        assert_eq!(rows, 0);
    }

    #[test]
    fn test_schedule_with_args_round_trip() {
        let conn = test_conn();
        let mut args = BTreeMap::new();
        args.insert("key1".to_string(), "val1".to_string());
        args.insert("key2".to_string(), "val2".to_string());

        let schedule = Schedule {
            schedule_id: "args-test".to_string(),
            namespace: "default".to_string(),
            spec: ScheduleSpec::Cron(vec!["*/5 * * * *".to_string()]),
            action: WorkflowAction {
                workflow_name: "wf_with_args".to_string(),
                args: args.clone(),
                task_queue: "workers".to_string(),
                id: "action-args".to_string(),
                timeouts: Timeouts {
                    execution_timeout_secs: Some(3600),
                    run_timeout_secs: None,
                    task_timeout_secs: Some(300),
                },
            },
            policy: OverlapPolicy::TerminateExisting,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        insert_schedule(&conn, &schedule).unwrap();
        let result = get_schedule(&conn, "args-test").unwrap().expect("Schedule should exist");

        assert_eq!(result.action.args.get("key1").unwrap(), "val1");
        assert_eq!(result.action.args.get("key2").unwrap(), "val2");
        assert_eq!(result.action.timeouts.execution_timeout_secs, Some(3600));
    }
}
