//! Database schema and migration.

use rusqlite::Connection;

/// Run all schema migrations on the given connection.
pub fn migrate(conn: &Connection) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS namespaces (
            name TEXT PRIMARY KEY,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS schedules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            schedule_id TEXT NOT NULL UNIQUE,
            namespace TEXT NOT NULL DEFAULT 'default',
            spec_type TEXT NOT NULL CHECK(spec_type IN ('cron', 'interval')),
            cron_expressions TEXT,
            interval_seconds INTEGER,
            overlap_policy TEXT NOT NULL DEFAULT 'skip' CHECK(overlap_policy IN ('skip', 'buffer', 'terminate')),
            action_workflow_name TEXT NOT NULL,
            action_task_queue TEXT NOT NULL,
            action_args_json TEXT NOT NULL DEFAULT '{}',
            action_id TEXT,
            timeouts_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS workflows (
            run_id TEXT PRIMARY KEY,
            workflow_id TEXT NOT NULL,
            name TEXT NOT NULL,
            task_queue TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
            args_json TEXT NOT NULL DEFAULT '{}',
            result_json TEXT,
            execution_timeout_secs INTEGER,
            run_timeout_secs INTEGER,
            task_timeout_secs INTEGER,
            started_at TEXT,
            completed_at TEXT,
            namespace TEXT NOT NULL DEFAULT 'default'
        );

        CREATE TABLE IF NOT EXISTS workflow_steps (
            id TEXT PRIMARY KEY,
            workflow_run_id TEXT NOT NULL REFERENCES workflows(run_id),
            step_index INTEGER NOT NULL,
            activity_name TEXT NOT NULL,
            args_json TEXT NOT NULL DEFAULT '{}',
            retry_policy_json TEXT NOT NULL DEFAULT '{\"maximum_attempts\":1,\"initial_interval_secs\":1.0}',
            timeout_secs INTEGER NOT NULL DEFAULT 300,
            heartbeat_timeout_secs INTEGER,
            state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending', 'running', 'completed', 'failed', 'retried')),
            attempt INTEGER NOT NULL DEFAULT 0,
            result_json TEXT,
            next_retry_at TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS activities (
            id TEXT PRIMARY KEY,
            step_id TEXT NOT NULL REFERENCES workflow_steps(id),
            attempt INTEGER NOT NULL,
            state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending', 'running', 'completed', 'failed')),
            started_at TEXT,
            completed_at TEXT,
            last_heartbeat_at TEXT,
            result_json TEXT,
            error_message TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_workflows_state ON workflows(state, namespace);
        CREATE INDEX IF NOT EXISTS idx_workflow_steps_workflow ON workflow_steps(workflow_run_id, step_index);
        CREATE INDEX IF NOT EXISTS idx_workflow_steps_retry ON workflow_steps(next_retry_at) WHERE next_retry_at IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_activities_step ON activities(step_id);
        ",
    )?;

    Ok(())
}
