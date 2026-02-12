// Placeholder module for canonical schema
pub struct CanonicalSchema;

impl Default for CanonicalSchema {
    fn default() -> Self {
        Self::new()
    }
}

impl CanonicalSchema {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

pub const CANONICAL_COORDINATOR_SCHEMA_PATH: &str = "schema.sql";
