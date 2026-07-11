//! Activity handler registry — maps activity names to execution logic.

use std::collections::HashMap;
use std::path::PathBuf;
use async_trait::async_trait;
use tracing::info;

#[async_trait]
pub trait ActivityHandler: Send + Sync {
    async fn execute(&self, task: &crate::ActivityTask) -> Result<String, Box<dyn std::error::Error + Send + Sync>>;
}

/// Script-based handler — executes a shell command/script.
struct ScriptHandler {
    script_path: PathBuf,
    workdir: PathBuf,
}

#[async_trait]
impl ActivityHandler for ScriptHandler {
    async fn execute(&self, _task: &crate::ActivityTask) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        info!(script = ?self.script_path, "Executing script");
        
        let output = tokio::process::Command::new("bash")
            .arg("-c")
            .arg(self.script_path.to_string_lossy().as_ref())
            .current_dir(&self.workdir)
            .output()
            .await?;

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        
        if !output.status.success() {
            return Err(format!("Script failed with exit code {:?}: {}", output.status.code(), stderr).into());
        }

        Ok(if stdout.is_empty() { stderr } else { stdout })
    }
}

/// Python script handler — runs a Python file.
struct PythonScriptHandler {
    python_path: PathBuf,
    workdir: PathBuf,
}

#[async_trait]
impl ActivityHandler for PythonScriptHandler {
    async fn execute(&self, _task: &crate::ActivityTask) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        info!(script = ?self.python_path, "Executing Python script");
        
        let output = tokio::process::Command::new("python3")
            .arg(&self.python_path)
            .current_dir(&self.workdir)
            .output()
            .await?;

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        
        if !output.status.success() {
            return Err(format!("Python script failed with exit code {:?}: {}", output.status.code(), stderr).into());
        }

        Ok(if stdout.is_empty() { stderr } else { stdout })
    }
}

/// Register all known activity handlers.
pub fn register_default_handlers(registry: &mut HashMap<String, Box<dyn ActivityHandler>>, workdir: &PathBuf) {
    
    // Memory condensation — Python script
    registry.insert(
        "lycus-memory-condenser".to_string(),
        Box::new(PythonScriptHandler {
            python_path: PathBuf::from("/home/waymore/scripts/memory_condenser.py"),
            workdir: workdir.clone(),
        }),
    );

    // Cron failure notifier — shell script  
    registry.insert(
        "lycus-cron-notifier".to_string(),
        Box::new(ScriptHandler {
            script_path: PathBuf::from("/home/waymore/scripts/cron_notifier.sh"),
            workdir: workdir.clone(),
        }),
    );

    // SearXNG health check — shell script (stub for now)
    registry.insert(
        "lycus-searxng-healthcheck".to_string(),
        Box::new(ScriptHandler {
            script_path: PathBuf::from("/home/waymore/scripts/searxng_health_check.sh"),
            workdir: workdir.clone(),
        }),
    );

    // SearXNG 429 error reactive check
    registry.insert(
        "lycus-searxng-error-reactive".to_string(),
        Box::new(ScriptHandler {
            script_path: PathBuf::from("/home/waymore/scripts/searxng_error_check.sh"),
            workdir: workdir.clone(),
        }),
    );

    // Math pipeline stages (stubs — need agent execution)
    registry.insert(
        "lycus-arxiv-factory".to_string(),
        Box::new(ScriptHandler {
            script_path: PathBuf::from("/home/waymore/scripts/arxiv_factory.sh"),
            workdir: workdir.clone(),
        }),
    );

    registry.insert(
        "lycus-mathnexus-extract".to_string(),
        Box::new(PythonScriptHandler {
            python_path: PathBuf::from("/home/waymore/Documents/AI_researched_math/mathNEXUS/extract_mindmap.py"),
            workdir: workdir.clone(),
        }),
    );

    registry.insert(
        "lycus-mathlab-agent".to_string(),
        Box::new(PythonScriptHandler {
            python_path: PathBuf::from("/home/waymore/Documents/AI_researched_math/mathLaboratory/laboratory_agent.py"),
            workdir: workdir.clone(),
        }),
    );
}
