// Placeholder module for contracts
pub struct Contract;

impl Default for Contract {
    fn default() -> Self {
        Self::new()
    }
}

impl Contract {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}
