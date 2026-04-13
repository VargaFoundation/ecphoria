//! Data tiering — background task for retention enforcement and TTL cleanup.
//!
//! Runs periodically to:
//! - Delete episodic events older than `default_retention_days`
//! - Clean up expired state entries (TTL)
//! - (Future) Move cold data to S3

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

/// Background data lifecycle manager.
///
/// Periodically enforces retention policies and cleans up expired state.
pub struct TieringManager {
    interval: Duration,
    shutdown_rx: watch::Receiver<bool>,
}

/// Handle to stop the tiering background task.
pub struct TieringHandle {
    shutdown_tx: watch::Sender<bool>,
}

impl TieringHandle {
    /// Signal the background task to stop.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

impl TieringManager {
    /// Create a new tiering manager with the given interval.
    pub fn new(interval_secs: u64) -> (Self, TieringHandle) {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mgr = Self {
            interval: Duration::from_secs(if interval_secs == 0 {
                3600
            } else {
                interval_secs
            }),
            shutdown_rx,
        };
        let handle = TieringHandle { shutdown_tx };
        (mgr, handle)
    }

    /// Run the background tiering loop.
    ///
    /// This should be spawned as a tokio task. It runs until shutdown is signaled.
    pub async fn run(mut self, engine: Arc<crate::StrataEngine>) {
        tracing::info!(
            interval_secs = self.interval.as_secs(),
            "tiering manager started"
        );

        loop {
            tokio::select! {
                _ = tokio::time::sleep(self.interval) => {
                    self.run_pass(&engine).await;
                }
                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        tracing::info!("tiering manager shutting down");
                        break;
                    }
                }
            }
        }
    }

    /// Run a single maintenance pass.
    async fn run_pass(&self, engine: &crate::StrataEngine) {
        // 1. Enforce episodic retention
        match engine.enforce_retention().await {
            Ok(deleted) if deleted > 0 => {
                tracing::info!(deleted, "retention: deleted old episodic events");
                metrics::counter!("strata_retention_events_deleted_total").increment(deleted);
            }
            Err(e) => tracing::warn!(error = %e, "retention pass failed"),
            _ => {}
        }

        // 2. Clean up expired state entries (TTL)
        // Access state store through the engine's public API
        // The engine doesn't expose cleanup_expired directly, so we call it
        // via the state store. For now, log that we'd do it.
        // TODO: expose cleanup_expired on engine when state store access is public
        tracing::debug!("tiering pass complete");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_tiering_manager() {
        let (mgr, handle) = TieringManager::new(60);
        assert_eq!(mgr.interval.as_secs(), 60);
        handle.shutdown();
    }

    #[test]
    fn default_interval() {
        let (mgr, _handle) = TieringManager::new(0);
        assert_eq!(mgr.interval.as_secs(), 3600);
    }

    #[tokio::test]
    async fn shutdown_stops_loop() {
        let (mgr, handle) = TieringManager::new(1);
        let engine = Arc::new(
            crate::StrataEngine::new(crate::CoreConfig::default())
                .await
                .unwrap(),
        );

        let task = tokio::spawn(mgr.run(engine));

        // Give it a moment then shutdown
        tokio::time::sleep(Duration::from_millis(100)).await;
        handle.shutdown();

        // Should complete quickly
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("tiering task should stop on shutdown")
            .expect("task should not panic");
    }
}
