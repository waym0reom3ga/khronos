//! Activity handler trait and implementations.

use std::path::PathBuf;
use async_trait::async_trait;
use tracing::info;

/// Black-box activity handler interface.
#[async_trait]
pub trait ActivityHandler: Send + Sync {
    /// Execute an activity task and return the result string.
    async fn execute(&self, task: &crate::ActivityTask) -> Result<String, String>;
}

// ——— Shell script handler ———

/// Executes a shell script via `bash -c`.
pub struct ScriptHandler {
    pub script_path: PathBuf,
    pub workdir: PathBuf,
}

#[async_trait]
impl ActivityHandler for ScriptHandler {
    async fn execute(&self, _task: &crate::ActivityTask) -> Result<String, String> {
        info!(script = ?self.script_path, "executing shell script");

        let output = tokio::process::Command::new("bash")
            .arg("-c")
            .arg(self.script_path.to_string_lossy().as_ref())
            .current_dir(&self.workdir)
            .output()
            .await
            .map_err(|e| format!("Failed to spawn: {}", e))?;

        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();

        if output.status.success() {
            Ok(if stdout.is_empty() { stderr } else { stdout })
        } else {
            Err(format!("Exit code {:?}: {}", output.status.code(), stderr))
        }
    }
}

// ——— Python script handler ———

/// Executes a Python script via `python3`.
pub struct PythonHandler {
    pub script_path: PathBuf,
    pub workdir: PathBuf,
}

#[async_trait]
impl ActivityHandler for PythonHandler {
    async fn execute(&self, _task: &crate::ActivityTask) -> Result<String, String> {
        info!(script = ?self.script_path, "executing python script");

        let output = tokio::process::Command::new("python3")
            .arg(&self.script_path)
            .current_dir(&self.workdir)
            .output()
            .await
            .map_err(|e| format!("Failed to spawn: {}", e))?;

        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();

        if output.status.success() {
            Ok(if stdout.is_empty() { stderr } else { stdout })
        } else {
            Err(format!("Exit code {:?}: {}", output.status.code(), stderr))
        }
    }
}

// ——— Dummy handler for testing ———

/// Always succeeds with a fixed message. Useful for end-to-end verification.
pub struct DummyHandler;

#[async_trait]
impl ActivityHandler for DummyHandler {
    async fn execute(&self, task: &crate::ActivityTask) -> Result<String, String> {
        info!(activity_id = %task.activity_id, "dummy handler executed");
        Ok(format!("Dummy result for activity {}", task.name))
    }
}
