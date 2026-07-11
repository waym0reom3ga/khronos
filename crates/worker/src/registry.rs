//! Activity registry — maps activity names to handler instances.

use std::collections::HashMap;
use crate::handler::ActivityHandler;

/// Thread-safe activity registry wrapped in Arc for shared access.
pub struct ActivityRegistry {
    handlers: HashMap<String, Box<dyn ActivityHandler>>,
}

impl ActivityRegistry {
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    /// Register a handler for an activity type name.
    pub fn register(&mut self, name: &str, handler: impl ActivityHandler + 'static) {
        self.handlers.insert(name.to_string(), Box::new(handler));
    }

    /// Look up a handler by activity type name.
    pub fn get(&self, name: &str) -> Option<&dyn ActivityHandler> {
        self.handlers.get(name).map(|b| b.as_ref())
    }

    /// Check if an activity type is registered.
    pub fn has(&self, name: &str) -> bool {
        self.handlers.contains_key(name)
    }

    /// Number of registered handlers.
    pub fn len(&self) -> usize {
        self.handlers.len()
    }
}
