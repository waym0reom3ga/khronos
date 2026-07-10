//! Khronos database persistence layer.

pub mod activities;
pub mod schedules;
pub mod schema;
pub mod workflows;

use std::path::{Path, PathBuf};
use rusqlite::{Connection, OpenFlags};
use tracing::info;

/// SQLite-backed database for Khronos workflow orchestration.
#[derive(Clone)]
pub struct Database {
    path: PathBuf,
}

impl Database {
    /// Create a new database at the given path, initializing schema if needed.
    pub fn new(path: impl AsRef<Path>) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let path = path.as_ref().to_path_buf();
        // Create parent dirs if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let conn = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_FULL_MUTEX,
        )?;

        // Enable WAL mode for better concurrency
        conn.execute_batch("PRAGMA journal_mode = WAL;")?;
        conn.execute_batch("PRAGMA busy_timeout = 5000;")?;
        conn.execute_batch("PRAGMA foreign_keys = ON;")?;

        schema::migrate(&conn)?;

        info!("Database initialized at {:?}", path);
        Ok(Self { path })
    }

    /// Open a new connection to the database.
    pub fn connection(&self) -> Connection {
        let conn = Connection::open_with_flags(
            &self.path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_FULL_MUTEX,
        )
        .expect("Failed to open database connection");
        conn.execute_batch("PRAGMA journal_mode = WAL;").ok();
        conn.execute_batch("PRAGMA busy_timeout = 5000;").ok();
        conn.execute_batch("PRAGMA foreign_keys = ON;").ok();
        conn
    }
}
