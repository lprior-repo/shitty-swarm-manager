#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum CircuitState {
    #[default]
    Closed,
    Open,
    HalfOpen,
}

impl CircuitState {
    #[must_use]
    pub const fn allows_operations(&self) -> bool {
        matches!(self, Self::Closed | Self::HalfOpen)
    }

    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Open => "open",
            Self::HalfOpen => "half_open",
        }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CircuitConfig {
    pub failure_threshold: u32,
    pub success_threshold: u32,
    pub reset_timeout_secs: u64,
    pub window_secs: u64,
}

impl CircuitConfig {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerRecord {
    pub id: i64,
    pub scope: String,
    pub state: CircuitState,
    pub failure_count: u32,
    pub success_count: u32,
    pub opened_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub config: CircuitConfig,
}

impl CircuitBreakerRecord {
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

    #[must_use]
    pub const fn should_open(&self) -> bool {
        matches!(self.state, CircuitState::Closed)
            && self.failure_count >= self.config.failure_threshold
    }

    #[must_use]
    pub const fn should_close(&self) -> bool {
        matches!(self.state, CircuitState::HalfOpen)
            && self.success_count >= self.config.success_threshold
    }

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

    #[must_use]
    pub fn try_half_open(mut self) -> Self {
        if self.state == CircuitState::Open {
            if let Some(opened_at) = self.opened_at {
                let elapsed = (Utc::now() - opened_at).num_seconds().cast_unsigned();
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
    fn circuit_state_allows_operations_when_closed_or_half_open() {
        assert!(CircuitState::Closed.allows_operations());
        assert!(CircuitState::HalfOpen.allows_operations());
        assert!(!CircuitState::Open.allows_operations());
    }

    #[test]
    fn circuit_state_roundtrip_preserves_values() {
        let cases = [
            (CircuitState::Closed, "closed"),
            (CircuitState::Open, "open"),
            (CircuitState::HalfOpen, "half_open"),
        ];

        for (state, expected) in cases {
            assert_eq!(state.as_str(), expected);
            assert_eq!(CircuitState::try_from(expected), Ok(state));
        }
    }

    #[test]
    fn circuit_state_accepts_both_hyphen_and_underscore() {
        assert_eq!(
            CircuitState::try_from("half_open"),
            Ok(CircuitState::HalfOpen)
        );
        assert_eq!(
            CircuitState::try_from("half-open"),
            Ok(CircuitState::HalfOpen)
        );
    }

    #[test]
    fn circuit_breaker_record_transitions_to_open_after_failures() {
        let config = CircuitConfig::new(3, 2, 60, 300);
        let breaker = CircuitBreakerRecord::new("swarm-1".to_string(), config);

        let breaker = breaker.record_failure().record_failure().record_failure();
        assert_eq!(breaker.state, CircuitState::Open);
        assert!(breaker.opened_at.is_some());
    }

    #[test]
    fn circuit_breaker_success_closes_after_threshold() {
        let config = CircuitConfig::new(2, 2, 0, 300);
        let mut breaker = CircuitBreakerRecord::new("swarm-1".to_string(), config)
            .record_failure()
            .record_failure();

        assert_eq!(breaker.state, CircuitState::Open);
        breaker.state = CircuitState::HalfOpen;

        let breaker = breaker.record_success().record_success();
        assert_eq!(breaker.state, CircuitState::Closed);
    }

    #[test]
    fn circuit_config_default_provides_sensible_defaults() {
        let config = CircuitConfig::default();
        assert_eq!(config.failure_threshold, 5);
        assert_eq!(config.success_threshold, 3);
        assert_eq!(config.reset_timeout_secs, 60);
        assert_eq!(config.window_secs, 300);
    }
}
