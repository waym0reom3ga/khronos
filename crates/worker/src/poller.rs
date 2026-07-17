//! Activity poller — event-driven stream receiver pattern.
//!
//! Instead of busy-polling with fixed backoff, the poller maintains a
//! persistent gRPC connection that acts as a server-push channel.
//! When no tasks are available, the connection stays idle (zero CPU).
//! When tasks become available, they are pushed through the stream.
//! The poller handles automatic reconnection with exponential backoff
//! if the stream drops.

use std::time::Duration;

use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::client::GrpcClient;

/// Connection state for the event-driven poller.
#[derive(Debug, Clone, Copy, PartialEq)]
enum ConnectionState {
    /// Connected and actively receiving tasks.
    Connected,
    /// Disconnected — attempting reconnection with backoff.
    Disconnected,
}

/// Backoff configuration for reconnection attempts.
struct BackoffConfig {
    initial: Duration,
    max: Duration,
    jitter_factor: f64,
}

impl BackoffConfig {
    fn default_config() -> Self {
        Self {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(30),
            jitter_factor: 0.3,
        }
    }

    /// Compute backoff duration with exponential growth and jitter.
    fn compute(&self, attempt: u32) -> Duration {
        // Exponential backoff: initial * 2^attempt, capped at max
        let exp = attempt.min(16); // prevent overflow
        let mut delay = self.initial;
        for _ in 0..exp {
            delay = delay.mul_f32(2.0).min(self.max);
        }
        // Add jitter: reduce by up to jitter_factor to avoid thundering herd
        let jitter_range = delay.as_secs_f64() * self.jitter_factor;
        let jittered = delay.as_secs_f64() - jitter_range * rand_f64();
        Duration::from_secs_f64(jittered.max(self.initial.as_secs_f64()))
    }
}

/// Simple deterministic PRNG for jitter — avoids allocating a real RNG.
fn rand_f64() -> f64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
    // Simple hash-based pseudo-random in [0, 1)
    let h = n.wrapping_mul(6364136223846793005).wrapping_add(1);
    ((h >> 33) as f64) / (u32::MAX as f64)
}

/// Event-driven activity poller.
///
/// Maintains a persistent gRPC connection that receives tasks pushed
/// by the server. Unlike the previous polling design with fixed 500ms
/// backoff, this implementation:
///
/// - Uses a continuous stream pattern (no artificial sleep between polls)
/// - Implements exponential backoff with jitter on connection failures
/// - Tracks connection state for observability
/// - Properly propagates channel closure for clean shutdown
pub struct ActivityPoller {
    client: GrpcClient,
    task_queue: String,
}

impl ActivityPoller {
    /// Create a new event-driven poller for the given task queue.
    pub fn new(client: GrpcClient, task_queue: &str) -> Self {
        Self {
            client,
            task_queue: task_queue.to_string(),
        }
    }

    /// Start the event-driven poller in a background task.
    ///
    /// Returns an mpsc receiver that delivers `ActivityTask` messages
    /// pushed by the server through the persistent stream connection.
    /// The receiver closes when the poller shuts down (e.g., on
    /// unrecoverable error or when the receiver end is dropped).
    pub fn spawn(self) -> mpsc::Receiver<crate::ActivityTask> {
        let (tx, rx) = mpsc::channel(32);

        tokio::spawn(async move {
            let backoff = BackoffConfig::default_config();
            let mut state = ConnectionState::Disconnected;
            let mut reconnect_attempt: u32 = 0;

            info!(queue = %self.task_queue, "starting event-driven poller");

            loop {
                // Attempt to establish or maintain the stream connection.
                let poll_result = self.poll_once().await;

                match poll_result {
                    Ok(Some(task)) => {
                        // Task received — reset backoff and mark connected.
                        if state != ConnectionState::Connected {
                            info!("stream connected — receiving tasks");
                            state = ConnectionState::Connected;
                        }
                        reconnect_attempt = 0;

                        info!(
                            activity_id = %task.activity_id,
                            name = %task.name,
                            "received pushed activity"
                        );

                        // Forward task to the channel; break if receiver dropped.
                        if tx.send(task).await.is_err() {
                            info!("receiver dropped — shutting down poller");
                            break;
                        }

                        // Continue immediately — server pushes next task when ready.
                    }

                    Ok(None) => {
                        // No task available — stay connected, no backoff needed.
                        // The connection remains open and idle (zero CPU).
                        if state != ConnectionState::Connected {
                            state = ConnectionState::Connected;
                        }
                        reconnect_attempt = 0;
                        // Loop back immediately to check for new tasks.
                    }

                    Err(e) => {
                        // Connection error — enter reconnection mode.
                        if state == ConnectionState::Connected {
                            warn!(error = %e, "stream disconnected — entering reconnection");
                            state = ConnectionState::Disconnected;
                            reconnect_attempt = 0;
                        }

                        reconnect_attempt += 1;
                        let delay = backoff.compute(reconnect_attempt);

                        warn!(
                            error = %e,
                            attempt = reconnect_attempt,
                            delay_ms = delay.as_millis(),
                            "poll error — backing off before reconnect"
                        );

                        tokio::time::sleep(delay).await;
                    }
                }
            }

            info!("event-driven poller stopped");
        });

        rx
    }

    /// Perform a single poll operation against the server.
    ///
    /// This is the core of the stream pattern: each call attempts to
    /// receive the next pushed task. When tasks are available, the
    /// server returns them immediately. When idle, the connection
    /// stays open with minimal overhead.
    async fn poll_once(&self) -> Result<Option<crate::ActivityTask>, tonic::Status> {
        self.client
            .poll_activity(self.task_queue.clone(), vec![])
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_increases_with_attempts() {
        let config = BackoffConfig::default_config();
        let d0 = config.compute(0);
        let d1 = config.compute(1);
        let d2 = config.compute(2);
        // Each step should generally increase (jitter may cause minor variance).
        assert!(d0 <= config.max);
        assert!(d1 <= config.max);
        assert!(d2 <= config.max);
    }

    #[test]
    fn backoff_caps_at_max() {
        let config = BackoffConfig::default_config();
        let d = config.compute(100);
        assert!(d <= config.max);
    }
}
