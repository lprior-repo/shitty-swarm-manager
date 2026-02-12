//! Health and circuit breaker types for blast-radius containment.
//!
//! This module provides types for tracking agent health, circuit breaker
//! state, and detecting stuck/zombie agents.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Circuit breaker state for protecting the swarm from cascade failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CircuitState {
    /// Circuit is closed - operations proceed normally.
    Closed,
    /// Circuit is open - operations are blocked.
    Open,
    /// Circuit is half-open - testing if operations can resume.
    HalfOpen,
}

impl CircuitState {
    /// Check if operations should be allowed.
    #[must_use]
    pub const fn allows_operations(&self) -> bool {
        matches!(self, Self::Closed | Self::HalfOpen)
    }

    /// Get string representation.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Open => "open",
            Self::HalfOpen => "half_open",
        }
    }
}

impl Default for CircuitState {
    fn default() -> Self {
        Self::Closed
    }
}

impl TryFrom<&str> for CircuitState {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, String> {
        match value {
            "closed" => Ok(Self::Closed),
            "open" => Ok(Self::Open),
            "half_open" | "half-open" => Ok(Self::HalfOpen),
            _ => Err(format!("Unknown circuit state: {value}")),
        }
    }
}

/// Configuration for circuit breaker behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CircuitConfig {
    /// Number of failures before opening the circuit.
    pub failure_threshold: u32,
    /// Number of successes in half-open state before closing.
    pub success_threshold: u32,
    /// Seconds to wait before transitioning from open to half-open.
    pub reset_timeout_secs: u64,
    /// Window size for counting failures (in seconds).
    pub window_secs: u64,
}

impl CircuitConfig {
    /// Create a new circuit breaker configuration.
    #[must_use]
    pub const fn new(
        failure_threshold: u32,
        success_threshold: u32,
        reset_timeout_secs: u64,
        window_secs: u64,
    ) -> Self {
        Self {
            failure_threshold,
            success_threshold,
            reset_timeout_secs,
            window_secs,
        }
    }
}

impl Default for CircuitConfig {
    fn default() -> Self {
        Self::new(5, 3, 60, 300)
    }
}

/// Health metrics for an agent or the swarm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct HealthMetrics {
    /// Total operations attempted.
    pub total_operations: u64,
    /// Successful operations.
    pub successful_operations: u64,
    /// Failed operations.
    pub failed_operations: u64,
    /// Currently in-progress operations.
    pub in_progress: u64,
}

impl HealthMetrics {
    /// Create new health metrics.
    #[must_use]
    pub const fn new(total: u64, success: u64, failed: u64, in_progress: u64) -> Self {
        Self {
            total_operations: total,
            successful_operations: success,
            failed_operations: failed,
            in_progress: in_progress,
        }
    }

    /// Calculate success rate (0-100).
    #[must_use]
    pub fn success_rate(&self) -> u8 {
        if self.total_operations == 0 {
            return 100;
        }
        let rate = (self.successful_operations as f64 / self.total_operations as f64) * 100.0;
        rate.clamp(0.0, 100.0) as u8
    }

    /// Record a successful operation.
    #[must_use]
    pub const fn record_success(&self) -> Self {
        Self::new(
            self.total_operations + 1,
            self.successful_operations + 1,
            self.failed_operations,
            self.in_progress.saturating_sub(1),
        )
    }

    /// Record a failed operation.
    #[must_use]
    pub const fn record_failure(&self) -> Self {
        Self::new(
            self.total_operations + 1,
            self.successful_operations,
            self.failed_operations + 1,
            self.in_progress.saturating_sub(1),
        )
    }

    /// Start a new operation.
    #[must_use]
    pub const fn start_operation(&self) -> Self {
        Self::new(
            self.total_operations,
            self.successful_operations,
            self.failed_operations,
            self.in_progress + 1,
        )
    }

    /// Check if health is critical (low success rate).
    #[must_use]
    pub fn is_critical(&self, threshold: u8) -> bool {
        self.total_operations >= 10 && self.success_rate() < threshold
    }
}

/// Behavioral fingerprint for detecting stuck agents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BehavioralFingerprint {
    /// Agent identifier.
    pub agent_id: String,
    /// Current bead being processed.
    pub current_bead_id: Option<i64>,
    /// Current stage.
    pub current_stage: String,
    /// Number of consecutive failures.
    pub consecutive_failures: u32,
    /// Time since last progress (in seconds).
    pub secs_since_progress: u64,
    /// Number of retries attempted.
    pub retry_count: u32,
    /// When the fingerprint was computed.
    pub computed_at: DateTime<Utc>,
}

impl BehavioralFingerprint {
    /// Create a new behavioral fingerprint.
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

    /// Check if the agent appears stuck.
    #[must_use]
    pub const fn is_stuck(&self, max_idle_secs: u64, max_failures: u32) -> bool {
        self.secs_since_progress > max_idle_secs || self.consecutive_failures > max_failures
    }

    /// Check if the agent is retrying excessively.
    #[must_use]
    pub const fn is_retry_loop(&self, max_retries: u32) -> bool {
        self.retry_count > max_retries
    }

    /// Get health status string.
    #[must_use]
    pub fn health_status(&self) -> AgentHealthStatus {
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

/// Health status classification for agents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AgentHealthStatus {
    /// Agent is operating normally.
    Healthy,
    /// Agent has some failures but is progressing.
    Degraded,
    /// Agent appears stuck (no progress for extended period).
    Stuck,
    /// Agent is in a retry loop.
    RetryLoop,
}

impl AgentHealthStatus {
    /// Check if the agent needs intervention.
    #[must_use]
    pub const fn needs_intervention(&self) -> bool {
        matches!(self, Self::Stuck | Self::RetryLoop)
    }

    /// Get string representation.
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

/// Circuit breaker state record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerRecord {
    /// Unique identifier.
    pub id: i64,
    /// Scope of this circuit breaker (swarm name or "global").
    pub scope: String,
    /// Current state.
    pub state: CircuitState,
    /// Number of consecutive failures.
    pub failure_count: u32,
    /// Number of consecutive successes (for half-open state).
    pub success_count: u32,
    /// When the circuit was last opened.
    pub opened_at: Option<DateTime<Utc>>,
    /// When the circuit state was last updated.
    pub updated_at: DateTime<Utc>,
    /// Configuration for this circuit breaker.
    pub config: CircuitConfig,
}

impl CircuitBreakerRecord {
    /// Create a new circuit breaker in closed state.
    #[must_use]
    pub fn new(scope: String, config: CircuitConfig) -> Self {
        Self {
            id: 0,
            scope,
            state: CircuitState::Closed,
            failure_count: 0,
            success_count: 0,
            opened_at: None,
            updated_at: Utc::now(),
            config,
        }
    }

    /// Check if the circuit should open based on failures.
    #[must_use]
    pub fn should_open(&self) -> bool {
        matches!(self.state, CircuitState::Closed)
            && self.failure_count >= self.config.failure_threshold
    }

    /// Check if the circuit should close based on successes.
    #[must_use]
    pub fn should_close(&self) -> bool {
        matches!(self.state, CircuitState::HalfOpen)
            && self.success_count >= self.config.success_threshold
    }

    /// Record a failure.
    #[must_use]
    pub fn record_failure(mut self) -> Self {
        self.failure_count += 1;
        self.success_count = 0;
        self.updated_at = Utc::now();

        if self.should_open() {
            self.state = CircuitState::Open;
            self.opened_at = Some(Utc::now());
        }

        self
    }

    /// Record a success.
    #[must_use]
    pub fn record_success(mut self) -> Self {
        self.failure_count = 0;
        self.success_count += 1;
        self.updated_at = Utc::now();

        if self.should_close() {
            self.state = CircuitState::Closed;
            self.opened_at = None;
        }

        self
    }

    /// Try to transition from open to half-open.
    #[must_use]
    pub fn try_half_open(mut self) -> Self {
        if self.state == CircuitState::Open {
            if let Some(opened_at) = self.opened_at {
                let elapsed = (Utc::now() - opened_at).num_seconds() as u64;
                if elapsed >= self.config.reset_timeout_secs {
                    self.state = CircuitState::HalfOpen;
                    self.success_count = 0;
                    self.updated_at = Utc::now();
                }
            }
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_state_allows_operations() {
        assert!(CircuitState::Closed.allows_operations());
        assert!(CircuitState::HalfOpen.allows_operations());
        assert!(!CircuitState::Open.allows_operations());
    }

    #[test]
    fn test_health_metrics_success_rate() {
        let metrics = HealthMetrics::new(100, 80, 20, 5);
        assert_eq!(metrics.success_rate(), 80);

        let no_data = HealthMetrics::default();
        assert_eq!(no_data.success_rate(), 100);
    }

    #[test]
    fn test_health_metrics_is_critical() {
        let critical = HealthMetrics::new(100, 30, 70, 0);
        assert!(critical.is_critical(50));

        let healthy = HealthMetrics::new(100, 80, 20, 0);
        assert!(!healthy.is_critical(50));
    }

    #[test]
    fn test_behavioral_fingerprint_is_stuck() {
        let stuck = BehavioralFingerprint::new(
            "agent-1".to_string(),
            Some(123),
            "implement".to_string(),
            0,
            600, // 10 minutes
            0,
        );
        assert!(stuck.is_stuck(300, 5));

        let healthy = BehavioralFingerprint::new(
            "agent-2".to_string(),
            Some(124),
            "contract".to_string(),
            0,
            60,
            0,
        );
        assert!(!healthy.is_stuck(300, 5));
    }

    #[test]
    fn test_circuit_breaker_record_transitions() {
        let config = CircuitConfig::new(3, 2, 60, 300);
        let breaker = CircuitBreakerRecord::new("swarm-1".to_string(), config);

        // Record failures until open
        let breaker = breaker.record_failure().record_failure().record_failure();
        assert_eq!(breaker.state, CircuitState::Open);
        assert!(breaker.opened_at.is_some());
    }

    #[test]
    fn test_circuit_breaker_success_closes() {
        let config = CircuitConfig::new(2, 2, 0, 300);
        let breaker = CircuitBreakerRecord::new("swarm-1".to_string(), config)
            .record_failure()
            .record_failure();

        assert_eq!(breaker.state, CircuitState::Open);

        // Manually set to half-open for testing
        let mut breaker = breaker;
        breaker.state = CircuitState::HalfOpen;

        let breaker = breaker.record_success().record_success();
        assert_eq!(breaker.state, CircuitState::Closed);
    }
}
