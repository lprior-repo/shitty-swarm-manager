//! Budget tracking types for cost/token management.
//!
//! This module provides types for tracking and enforcing token budgets
//! at both the bead and swarm level.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Budget limit configuration for a bead or swarm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BudgetLimit {
    /// Maximum input tokens allowed.
    pub max_input_tokens: u64,
    /// Maximum output tokens allowed.
    pub max_output_tokens: u64,
    /// Maximum total tokens (input + output).
    pub max_total_tokens: u64,
}

impl BudgetLimit {
    /// Create a new budget limit with the specified constraints.
    #[must_use]
    pub const fn new(max_input: u64, max_output: u64, max_total: u64) -> Self {
        Self {
            max_input_tokens: max_input,
            max_output_tokens: max_output,
            max_total_tokens: max_total,
        }
    }

    /// Default budget limit for standard beads.
    #[must_use]
    pub const fn default_bead() -> Self {
        Self::new(50_000, 20_000, 60_000)
    }

    /// Default budget limit for high-priority (P0) beads.
    #[must_use]
    pub const fn high_priority() -> Self {
        Self::new(100_000, 50_000, 120_000)
    }

    /// Check if the given usage exceeds this budget.
    #[must_use]
    pub const fn is_exceeded(&self, usage: &TokenUsage) -> bool {
        usage.input_tokens > self.max_input_tokens
            || usage.output_tokens > self.max_output_tokens
            || usage.total_tokens() > self.max_total_tokens
    }

    /// Calculate remaining budget given current usage.
    #[must_use]
    pub fn remaining(&self, usage: &TokenUsage) -> BudgetRemaining {
        BudgetRemaining {
            input: self.max_input_tokens.saturating_sub(usage.input_tokens),
            output: self.max_output_tokens.saturating_sub(usage.output_tokens),
            total: self.max_total_tokens.saturating_sub(usage.total_tokens()),
        }
    }
}

impl Default for BudgetLimit {
    fn default() -> Self {
        Self::default_bead()
    }
}

/// Current token usage metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TokenUsage {
    /// Input tokens consumed.
    pub input_tokens: u64,
    /// Output tokens generated.
    pub output_tokens: u64,
}

impl TokenUsage {
    /// Create a new token usage record.
    #[must_use]
    pub const fn new(input: u64, output: u64) -> Self {
        Self {
            input_tokens: input,
            output_tokens: output,
        }
    }

    /// Get total tokens (input + output).
    #[must_use]
    pub const fn total_tokens(&self) -> u64 {
        self.input_tokens + self.output_tokens
    }

    /// Add another usage to this one.
    #[must_use]
    pub const fn add(&self, other: &Self) -> Self {
        Self::new(
            self.input_tokens + other.input_tokens,
            self.output_tokens + other.output_tokens,
        )
    }
}

/// Remaining budget after accounting for usage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BudgetRemaining {
    /// Remaining input token budget.
    pub input: u64,
    /// Remaining output token budget.
    pub output: u64,
    /// Remaining total token budget.
    pub total: u64,
}

impl BudgetRemaining {
    /// Check if any budget dimension is exhausted.
    #[must_use]
    pub const fn is_exhausted(&self) -> bool {
        self.input == 0 || self.output == 0 || self.total == 0
    }
}

/// Budget status for a bead or swarm.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BudgetStatus {
    /// The budget limit.
    pub limit: BudgetLimit,
    /// Current usage.
    pub usage: TokenUsage,
    /// Whether the budget has been exceeded.
    pub exceeded: bool,
    /// When the budget was created.
    pub created_at: DateTime<Utc>,
    /// When the budget was last updated.
    pub updated_at: DateTime<Utc>,
}

impl BudgetStatus {
    /// Create a new budget status with the given limit.
    #[must_use]
    pub fn new(limit: BudgetLimit) -> Self {
        let now = Utc::now();
        Self {
            limit,
            usage: TokenUsage::default(),
            exceeded: false,
            created_at: now,
            updated_at: now,
        }
    }

    /// Record token usage and update status.
    #[must_use]
    pub fn record_usage(mut self, usage: &TokenUsage) -> Self {
        self.usage = self.usage.add(usage);
        self.exceeded = self.limit.is_exceeded(&self.usage);
        self.updated_at = Utc::now();
        self
    }

    /// Get remaining budget.
    #[must_use]
    pub fn remaining(&self) -> BudgetRemaining {
        self.limit.remaining(&self.usage)
    }
}

/// Budget record stored in the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BudgetRecord {
    /// Unique identifier for this budget record.
    pub id: i64,
    /// The bead this budget is for (None for swarm-level budget).
    pub bead_id: Option<i64>,
    /// Maximum input tokens.
    pub max_input_tokens: u64,
    /// Maximum output tokens.
    pub max_output_tokens: u64,
    /// Maximum total tokens.
    pub max_total_tokens: u64,
    /// Current input tokens used.
    pub used_input_tokens: u64,
    /// Current output tokens used.
    pub used_output_tokens: u64,
    /// Whether the budget has been exceeded.
    pub exceeded: bool,
    /// When the budget was created.
    pub created_at: DateTime<Utc>,
    /// When the budget was last updated.
    pub updated_at: DateTime<Utc>,
}

impl BudgetRecord {
    /// Convert to a BudgetStatus.
    #[must_use]
    pub fn to_status(&self) -> BudgetStatus {
        BudgetStatus {
            limit: BudgetLimit::new(
                self.max_input_tokens,
                self.max_output_tokens,
                self.max_total_tokens,
            ),
            usage: TokenUsage::new(self.used_input_tokens, self.used_output_tokens),
            exceeded: self.exceeded,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

/// Token usage record for granular tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenUsageRecord {
    /// Unique identifier for this usage record.
    pub id: i64,
    /// The budget this usage is associated with.
    pub budget_id: i64,
    /// The agent that consumed these tokens.
    pub agent_id: String,
    /// Input tokens in this usage.
    pub input_tokens: u64,
    /// Output tokens in this usage.
    pub output_tokens: u64,
    /// Description of what the tokens were used for.
    pub description: Option<String>,
    /// When this usage was recorded.
    pub recorded_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_budget_limit_default() {
        let limit = BudgetLimit::default_bead();
        assert_eq!(limit.max_input_tokens, 50_000);
        assert_eq!(limit.max_output_tokens, 20_000);
        assert_eq!(limit.max_total_tokens, 60_000);
    }

    #[test]
    fn test_budget_limit_exceeded() {
        let limit = BudgetLimit::new(100, 50, 120);
        let under = TokenUsage::new(50, 25);
        let over_input = TokenUsage::new(150, 25);
        let over_output = TokenUsage::new(50, 60);
        let over_total = TokenUsage::new(80, 50);

        assert!(!limit.is_exceeded(&under));
        assert!(limit.is_exceeded(&over_input));
        assert!(limit.is_exceeded(&over_output));
        assert!(limit.is_exceeded(&over_total));
    }

    #[test]
    fn test_token_usage_total() {
        let usage = TokenUsage::new(100, 50);
        assert_eq!(usage.total_tokens(), 150);
    }

    #[test]
    fn test_token_usage_add() {
        let a = TokenUsage::new(100, 50);
        let b = TokenUsage::new(50, 25);
        let sum = a.add(&b);
        assert_eq!(sum.input_tokens, 150);
        assert_eq!(sum.output_tokens, 75);
        assert_eq!(sum.total_tokens(), 225);
    }

    #[test]
    fn test_budget_remaining_exhausted() {
        let exhausted = BudgetRemaining {
            input: 0,
            output: 100,
            total: 100,
        };
        let not_exhausted = BudgetRemaining {
            input: 50,
            output: 50,
            total: 100,
        };

        assert!(exhausted.is_exhausted());
        assert!(!not_exhausted.is_exhausted());
    }

    #[test]
    fn test_budget_status_record_usage() {
        let limit = BudgetLimit::new(200, 100, 250);
        let status = BudgetStatus::new(limit);
        let usage = TokenUsage::new(150, 75);

        let updated = status.record_usage(&usage);

        assert_eq!(updated.usage.input_tokens, 150);
        assert_eq!(updated.usage.output_tokens, 75);
        assert!(!updated.exceeded);

        let more = TokenUsage::new(100, 50);
        let exceeded = updated.record_usage(&more);

        assert_eq!(exceeded.usage.input_tokens, 250);
        assert!(exceeded.exceeded);
    }
}
