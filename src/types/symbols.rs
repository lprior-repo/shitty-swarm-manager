//! Symbol tracking types for semantic drift detection.
//!
//! This module provides types for tracking type signatures, function
//! signatures, and detecting semantic drift between contract and
//! implementation.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A symbol kind in the codebase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SymbolKind {
    /// Function definition.
    Function,
    /// Struct definition.
    Struct,
    /// Enum definition.
    Enum,
    /// Trait definition.
    Trait,
    /// Type alias.
    TypeAlias,
    /// Constant.
    Constant,
    /// Module.
    Module,
}

impl SymbolKind {
    /// Get string representation.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Function => "function",
            Self::Struct => "struct",
            Self::Enum => "enum",
            Self::Trait => "trait",
            Self::TypeAlias => "type_alias",
            Self::Constant => "constant",
            Self::Module => "module",
        }
    }
}

impl TryFrom<&str> for SymbolKind {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, String> {
        match value {
            "function" => Ok(Self::Function),
            "struct" => Ok(Self::Struct),
            "enum" => Ok(Self::Enum),
            "trait" => Ok(Self::Trait),
            "type_alias" | "type-alias" => Ok(Self::TypeAlias),
            "constant" => Ok(Self::Constant),
            "module" => Ok(Self::Module),
            _ => Err(format!("Unknown symbol kind: {value}")),
        }
    }
}

/// A type signature representing a function or type definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeSignature {
    /// The signature string (e.g., "fn(x: i32) -> String").
    pub signature: String,
    /// Hash of the signature for quick comparison.
    pub hash: String,
}

impl TypeSignature {
    /// Create a new type signature.
    #[must_use]
    pub fn new(signature: String) -> Self {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        signature.hash(&mut hasher);
        let hash = format!("{:016x}", hasher.finish());

        Self { signature, hash }
    }

    /// Check if two signatures match.
    #[must_use]
    pub fn matches(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

/// A symbol being tracked for semantic drift.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrackedSymbol {
    /// Symbol name.
    pub name: String,
    /// Symbol kind.
    pub kind: SymbolKind,
    /// Module path.
    pub module_path: String,
    /// The contract signature (from design phase).
    pub contract_signature: Option<TypeSignature>,
    /// The implementation signature (from implementation phase).
    pub implementation_signature: Option<TypeSignature>,
    /// Whether drift has been detected.
    pub has_drift: bool,
    /// When this symbol was first tracked.
    pub created_at: DateTime<Utc>,
    /// When this symbol was last updated.
    pub updated_at: DateTime<Utc>,
}

impl TrackedSymbol {
    /// Create a new tracked symbol.
    #[must_use]
    pub fn new(name: String, kind: SymbolKind, module_path: String) -> Self {
        let now = Utc::now();
        Self {
            name,
            kind,
            module_path,
            contract_signature: None,
            implementation_signature: None,
            has_drift: false,
            created_at: now,
            updated_at: now,
        }
    }

    /// Set the contract signature.
    #[must_use]
    pub fn with_contract_signature(mut self, signature: String) -> Self {
        self.contract_signature = Some(TypeSignature::new(signature));
        self.updated_at = Utc::now();
        self.check_drift();
        self
    }

    /// Set the implementation signature.
    #[must_use]
    pub fn with_implementation_signature(mut self, signature: String) -> Self {
        self.implementation_signature = Some(TypeSignature::new(signature));
        self.updated_at = Utc::now();
        self.check_drift();
        self
    }

    /// Check for drift between contract and implementation.
    pub fn check_drift(&mut self) {
        self.has_drift = match (&self.contract_signature, &self.implementation_signature) {
            (Some(contract), Some(impl_sig)) => !contract.matches(impl_sig),
            _ => false,
        };
    }

    /// Get the fully qualified name.
    #[must_use]
    pub fn fully_qualified_name(&self) -> String {
        format!("{}::{}", self.module_path, self.name)
    }
}

/// Drift detection result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DriftReport {
    /// Symbols that have drifted.
    pub drifted_symbols: Vec<DriftedSymbol>,
    /// Total symbols checked.
    pub total_checked: usize,
    /// Whether any drift was detected.
    pub has_drift: bool,
    /// When this report was generated.
    pub generated_at: DateTime<Utc>,
}

impl DriftReport {
    /// Create an empty report.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            drifted_symbols: Vec::new(),
            total_checked: 0,
            has_drift: false,
            generated_at: Utc::now(),
        }
    }

    /// Create a report from tracked symbols.
    #[must_use]
    pub fn from_symbols(symbols: &[TrackedSymbol]) -> Self {
        let drifted: Vec<_> = symbols
            .iter()
            .filter(|s| s.has_drift)
            .map(|s| DriftedSymbol {
                name: s.name.clone(),
                kind: s.kind,
                module_path: s.module_path.clone(),
                contract_signature: s
                    .contract_signature
                    .as_ref()
                    .map(|ts| ts.signature.clone())
                    .unwrap_or_default(),
                implementation_signature: s
                    .implementation_signature
                    .as_ref()
                    .map(|ts| ts.signature.clone())
                    .unwrap_or_default(),
            })
            .collect();

        Self {
            has_drift: !drifted.is_empty(),
            total_checked: symbols.len(),
            drifted_symbols: drifted,
            generated_at: Utc::now(),
        }
    }
}

/// A symbol that has drifted from its contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DriftedSymbol {
    /// Symbol name.
    pub name: String,
    /// Symbol kind.
    pub kind: SymbolKind,
    /// Module path.
    pub module_path: String,
    /// The expected (contract) signature.
    pub contract_signature: String,
    /// The actual (implementation) signature.
    pub implementation_signature: String,
}

impl DriftedSymbol {
    /// Get a description of the drift.
    #[must_use]
    pub fn description(&self) -> String {
        format!(
            "{} {}::{}: expected '{}', found '{}'",
            self.kind.as_str(),
            self.module_path,
            self.name,
            self.contract_signature,
            self.implementation_signature
        )
    }
}

/// Symbol record stored in the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolRecord {
    /// Unique identifier.
    pub id: i64,
    /// Bead this symbol is associated with.
    pub bead_id: i64,
    /// Symbol name.
    pub name: String,
    /// Symbol kind.
    pub kind: String,
    /// Module path.
    pub module_path: String,
    /// Contract signature.
    pub contract_signature: Option<String>,
    /// Contract signature hash.
    pub contract_hash: Option<String>,
    /// Implementation signature.
    pub implementation_signature: Option<String>,
    /// Implementation signature hash.
    pub implementation_hash: Option<String>,
    /// Whether drift was detected.
    pub has_drift: bool,
    /// When the record was created.
    pub created_at: DateTime<Utc>,
    /// When the record was last updated.
    pub updated_at: DateTime<Utc>,
}

impl SymbolRecord {
    /// Convert to a `TrackedSymbol`.
    #[must_use]
    pub fn to_tracked_symbol(&self) -> TrackedSymbol {
        let kind = SymbolKind::try_from(self.kind.as_str()).unwrap_or(SymbolKind::Function);

        let mut symbol = TrackedSymbol::new(self.name.clone(), kind, self.module_path.clone());

        if let (Some(sig), Some(_hash)) = (&self.contract_signature, &self.contract_hash) {
            symbol = symbol.with_contract_signature(sig.clone());
        }

        if let (Some(sig), Some(_hash)) =
            (&self.implementation_signature, &self.implementation_hash)
        {
            symbol = symbol.with_implementation_signature(sig.clone());
        }

        symbol.has_drift = self.has_drift;
        symbol
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_signature_matches() {
        let sig1 = TypeSignature::new("fn(x: i32) -> String".to_string());
        let sig2 = TypeSignature::new("fn(x: i32) -> String".to_string());
        let sig3 = TypeSignature::new("fn(x: i64) -> String".to_string());

        assert!(sig1.matches(&sig2));
        assert!(!sig1.matches(&sig3));
    }

    #[test]
    fn test_tracked_symbol_drift_detection() {
        let symbol = TrackedSymbol::new(
            "process".to_string(),
            SymbolKind::Function,
            "myapp::handlers".to_string(),
        )
        .with_contract_signature("fn(i32) -> String".to_string())
        .with_implementation_signature("fn(i32) -> String".to_string());

        assert!(!symbol.has_drift);

        let drifted = TrackedSymbol::new(
            "process".to_string(),
            SymbolKind::Function,
            "myapp::handlers".to_string(),
        )
        .with_contract_signature("fn(i32) -> String".to_string())
        .with_implementation_signature("fn(i64) -> String".to_string());

        assert!(drifted.has_drift);
    }

    #[test]
    fn test_drift_report_from_symbols() {
        let symbols = vec![
            TrackedSymbol::new(
                "func_a".to_string(),
                SymbolKind::Function,
                "module".to_string(),
            )
            .with_contract_signature("fn() -> i32".to_string())
            .with_implementation_signature("fn() -> i32".to_string()),
            TrackedSymbol::new(
                "func_b".to_string(),
                SymbolKind::Function,
                "module".to_string(),
            )
            .with_contract_signature("fn() -> String".to_string())
            .with_implementation_signature("fn() -> i32".to_string()),
        ];

        let report = DriftReport::from_symbols(&symbols);

        assert_eq!(report.total_checked, 2);
        assert!(report.has_drift);
        assert_eq!(report.drifted_symbols.len(), 1);
        assert_eq!(report.drifted_symbols[0].name, "func_b");
    }

    #[test]
    fn test_drifted_symbol_description() {
        let drifted = DriftedSymbol {
            name: "my_func".to_string(),
            kind: SymbolKind::Function,
            module_path: "myapp::core".to_string(),
            contract_signature: "fn(i32) -> String".to_string(),
            implementation_signature: "fn(i64) -> String".to_string(),
        };

        let desc = drifted.description();
        assert!(desc.contains("my_func"));
        assert!(desc.contains("myapp::core"));
        assert!(desc.contains("fn(i32) -> String"));
        assert!(desc.contains("fn(i64) -> String"));
    }
}
