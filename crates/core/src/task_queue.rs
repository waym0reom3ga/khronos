use chrono::DateTime;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A registered worker and the activity types it can handle.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegisteredWorker {
    /// Unique identifier for this worker instance.
    pub worker_id: String,
    /// Activity types this worker is capable of executing.
    pub activity_types: Vec<String>,
    /// Timestamp of the last heartbeat from this worker.
    pub last_heartbeat: DateTime<chrono::Utc>,
}

/// In-memory task queue for managing registered workers and their capabilities.
///
/// This is the type layer only; persistence is handled by the `khronos-db` crate.
#[derive(Clone, Debug)]
pub struct TaskQueue {
    /// Name of this task queue.
    pub name: String,
    /// Registered workers keyed by worker_id.
    pub workers: HashMap<String, RegisteredWorker>,
}

impl TaskQueue {
    /// Create a new empty task queue with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            workers: HashMap::new(),
        }
    }

    /// Register a worker with the activity types it can handle.
    pub fn register_worker(&mut self, worker_id: String, activity_types: Vec<String>) {
        let now = chrono::Utc::now();
        self.workers.insert(
            worker_id.clone(),
            RegisteredWorker {
                worker_id,
                activity_types,
                last_heartbeat: now,
            },
        );
    }

    /// Remove a registered worker by ID.
    pub fn unregister_worker(&mut self, worker_id: &str) -> Option<RegisteredWorker> {
        self.workers.remove(worker_id)
    }

    /// Update the heartbeat timestamp for an existing worker.
    pub fn update_heartbeat(&mut self, worker_id: &str) -> bool {
        if let Some(worker) = self.workers.get_mut(worker_id) {
            worker.last_heartbeat = chrono::Utc::now();
            true
        } else {
            false
        }
    }

    /// Check whether any registered worker can handle the given activity type.
    pub fn has_handler(&self, activity_type: &str) -> bool {
        self.workers.values().any(|w| w.activity_types.contains(&activity_type.to_string()))
    }

    /// Get all workers that can handle a specific activity type.
    pub fn get_handlers<'a>(
        &'a self,
        activity_type: &str,
    ) -> impl Iterator<Item = &'a RegisteredWorker> {
        self.workers.values().filter(|w| w.activity_types.contains(&activity_type.to_string()))
    }

    /// Get a registered worker by ID.
    pub fn get_worker(&self, worker_id: &str) -> Option<&RegisteredWorker> {
        self.workers.get(worker_id)
    }

    /// Return the number of registered workers.
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_unregister_worker() {
        let mut queue = TaskQueue::new("default");
        assert_eq!(queue.worker_count(), 0);

        queue.register_worker("w1".into(), vec!["activity_a".into()]);
        assert_eq!(queue.worker_count(), 1);
        assert!(queue.has_handler("activity_a"));

        let removed = queue.unregister_worker("w1");
        assert!(removed.is_some());
        assert_eq!(queue.worker_count(), 0);
    }

    #[test]
    fn test_update_heartbeat() {
        let mut queue = TaskQueue::new("default");
        queue.register_worker("w1".into(), vec!["activity_a".into()]);

        assert!(queue.update_heartbeat("w1"));
        assert!(!queue.update_heartbeat("nonexistent"));
    }

    #[test]
    fn test_get_handlers() {
        let mut queue = TaskQueue::new("default");
        queue.register_worker("w1".into(), vec!["activity_a".into(), "activity_b".into()]);
        queue.register_worker("w2".into(), vec!["activity_a".into()]);

        let handlers: Vec<_> = queue.get_handlers("activity_a").collect();
        assert_eq!(handlers.len(), 2);

        let handlers: Vec<_> = queue.get_handlers("activity_b").collect();
        assert_eq!(handlers.len(), 1);
    }
}
