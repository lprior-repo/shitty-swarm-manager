#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct HealthMetrics {
    pub total_operations: u64,
    pub successful_operations: u64,
    pub failed_operations: u64,
    pub in_progress: u64,
}

impl HealthMetrics {
    #[must_use]
    pub const fn new(total: u64, success: u64, failed: u64, in_progress: u64) -> Self {
        Self {
            total_operations: total,
            successful_operations: success,
            failed_operations: failed,
            in_progress,
        }
    }

    #[must_use]
    pub fn success_rate(&self) -> u8 {
        if self.total_operations == 0 {
            return 100;
        }
        let rate = (self.successful_operations as f64 / self.total_operations as f64) * 100.0;
        rate.clamp(0.0, 100.0) as u8
    }

    #[must_use]
    pub const fn record_success(&self) -> Self {
        Self::new(
            self.total_operations + 1,
            self.successful_operations + 1,
            self.failed_operations,
            self.in_progress.saturating_sub(1),
        )
    }

    #[must_use]
    pub const fn record_failure(&self) -> Self {
        Self::new(
            self.total_operations + 1,
            self.successful_operations,
            self.failed_operations + 1,
            self.in_progress.saturating_sub(1),
        )
    }

    #[must_use]
    pub const fn start_operation(&self) -> Self {
        Self::new(
            self.total_operations,
            self.successful_operations,
            self.failed_operations,
            self.in_progress + 1,
        )
    }

    #[must_use]
    pub fn is_critical(&self, threshold: u8) -> bool {
        self.total_operations >= 10 && self.success_rate() < threshold
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BehavioralFingerprint {
    pub agent_id: String,
    pub current_bead_id: Option<i64>,
    pub current_stage: String,
    pub consecutive_failures: u32,
    pub secs_since_progress: u64,
    pub retry_count: u32,
    pub computed_at: DateTime<Utc>,
}

impl BehavioralFingerprint {
    #[must_use]
    pub fn new(
        agent_id: String,
        current_bead_id: Option<i64>,
        current_stage: String,
        consecutive_failures: u32,
        secs_since_progress: u64,
        retry_count: u32,
    ) -> Self {
        Self {
            agent_id,
            current_bead_id,
            current_stage,
            consecutive_failures,
            secs_since_progress,
            retry_count,
            computed_at: Utc::now(),
        }
    }

    #[must_use]
    pub const fn is_stuck(&self, max_idle_secs: u64, max_failures: u32) -> bool {
        self.secs_since_progress > max_idle_secs || self.consecutive_failures > max_failures
    }

    #[must_use]
    pub const fn is_retry_loop(&self, max_retries: u32) -> bool {
        self.retry_count > max_retries
    }

    #[must_use]
    pub const fn health_status(&self) -> AgentHealthStatus {
        if self.is_stuck(300, 5) {
            AgentHealthStatus::Stuck
        } else if self.is_retry_loop(10) {
            AgentHealthStatus::RetryLoop
        } else if self.consecutive_failures > 0 {
            AgentHealthStatus::Degraded
        } else {
            AgentHealthStatus::Healthy
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AgentHealthStatus {
    Healthy,
    Degraded,
    Stuck,
    RetryLoop,
}

impl AgentHealthStatus {
    #[must_use]
    pub const fn needs_intervention(&self) -> bool {
        matches!(self, Self::Stuck | Self::RetryLoop)
    }

    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Stuck => "stuck",
            Self::RetryLoop => "retry_loop",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_metrics_success_rate_calculates_correctly() {
        let metrics = HealthMetrics::new(100, 80, 20, 5);
        assert_eq!(metrics.success_rate(), 80);

        let no_data = HealthMetrics::default();
        assert_eq!(no_data.success_rate(), 100);
    }

    #[test]
    fn health_metrics_is_critical_detects_low_success_rate() {
        let critical = HealthMetrics::new(100, 30, 70, 0);
        assert!(critical.is_critical(50));

        let healthy = HealthMetrics::new(100, 80, 20, 0);
        assert!(!healthy.is_critical(50));
    }

    #[test]
    fn health_metrics_is_critical_requires_minimum_operations() {
        let few_ops = HealthMetrics::new(5, 1, 4, 0);
        assert!(!few_ops.is_critical(50));
    }

    #[test]
    fn health_metrics_record_operations_immutably() {
        let metrics = HealthMetrics::new(10, 8, 2, 1);

        let after_success = metrics.record_success();
        assert_eq!(after_success.total_operations, 11);
        assert_eq!(after_success.successful_operations, 9);

        let after_failure = metrics.record_failure();
        assert_eq!(after_failure.total_operations, 11);
        assert_eq!(after_failure.failed_operations, 3);

        let after_start = metrics.start_operation();
        assert_eq!(after_start.in_progress, 2);
    }

    #[test]
    fn behavioral_fingerprint_is_stuck_detects_idle_and_failures() {
        let stuck_by_idle = BehavioralFingerprint::new(
            "agent-1".to_string(),
            Some(123),
            "implement".to_string(),
            0,
            600,
            0,
        );
        assert!(stuck_by_idle.is_stuck(300, 5));

        let stuck_by_failures = BehavioralFingerprint::new(
            "agent-2".to_string(),
            Some(124),
            "implement".to_string(),
            10,
            60,
            0,
        );
        assert!(stuck_by_failures.is_stuck(300, 5));

        let healthy = BehavioralFingerprint::new(
            "agent-3".to_string(),
            Some(125),
            "contract".to_string(),
            0,
            60,
            0,
        );
        assert!(!healthy.is_stuck(300, 5));
    }

    #[test]
    fn behavioral_fingerprint_is_retry_loop_detects_excessive_retries() {
        let retry_loop = BehavioralFingerprint::new(
            "agent-1".to_string(),
            Some(123),
            "implement".to_string(),
            0,
            60,
            15,
        );
        assert!(retry_loop.is_retry_loop(10));

        let normal = BehavioralFingerprint::new(
            "agent-2".to_string(),
            Some(124),
            "implement".to_string(),
            0,
            60,
            5,
        );
        assert!(!normal.is_retry_loop(10));
    }

    #[test]
    fn behavioral_fingerprint_health_status_classifies_correctly() {
        let healthy = BehavioralFingerprint::new(
            "agent-1".to_string(),
            Some(123),
            "implement".to_string(),
            0,
            60,
            0,
        );
        assert_eq!(healthy.health_status(), AgentHealthStatus::Healthy);

        let degraded = BehavioralFingerprint::new(
            "agent-2".to_string(),
            Some(124),
            "implement".to_string(),
            3,
            60,
            0,
        );
        assert_eq!(degraded.health_status(), AgentHealthStatus::Degraded);

        let stuck = BehavioralFingerprint::new(
            "agent-3".to_string(),
            Some(125),
            "implement".to_string(),
            0,
            600,
            0,
        );
        assert_eq!(stuck.health_status(), AgentHealthStatus::Stuck);

        let retry_loop = BehavioralFingerprint::new(
            "agent-4".to_string(),
            Some(126),
            "implement".to_string(),
            0,
            60,
            15,
        );
        assert_eq!(retry_loop.health_status(), AgentHealthStatus::RetryLoop);
    }

    #[test]
    fn agent_health_status_needs_intervention_identifies_problems() {
        assert!(!AgentHealthStatus::Healthy.needs_intervention());
        assert!(!AgentHealthStatus::Degraded.needs_intervention());
        assert!(AgentHealthStatus::Stuck.needs_intervention());
        assert!(AgentHealthStatus::RetryLoop.needs_intervention());
    }
}
