//! File manifest types for scope fencing and conflict detection.
//!
//! This module provides types for declaring which files an agent intends
//! to modify, enabling conflict detection and scope enforcement.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// A file that an agent intends to modify.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FileDeclaration {
    /// Relative path from repository root.
    pub path: String,
    /// Type of modification intended.
    pub modification_type: ModificationType,
    /// Optional description of why this file needs modification.
    pub reason: Option<String>,
}

impl FileDeclaration {
    /// Create a new file declaration.
    #[must_use]
    pub fn new(path: String, modification_type: ModificationType) -> Self {
        Self {
            path,
            modification_type,
            reason: None,
        }
    }

    /// Add a reason for the modification.
    #[must_use]
    pub fn with_reason(mut self, reason: String) -> Self {
        self.reason = Some(reason);
        self
    }
}

/// Type of file modification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ModificationType {
    /// Create a new file.
    Create,
    /// Modify an existing file.
    Modify,
    /// Delete a file.
    Delete,
    /// Rename/move a file.
    Rename,
}

impl ModificationType {
    /// Get string representation.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Modify => "modify",
            Self::Delete => "delete",
            Self::Rename => "rename",
        }
    }
}

impl TryFrom<&str> for ModificationType {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, String> {
        match value {
            "create" => Ok(Self::Create),
            "modify" => Ok(Self::Modify),
            "delete" => Ok(Self::Delete),
            "rename" => Ok(Self::Rename),
            _ => Err(format!("Unknown modification type: {value}")),
        }
    }
}

/// A manifest declaring all files an agent intends to modify.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileManifest {
    /// The agent that owns this manifest.
    pub agent_id: String,
    /// The bead this manifest is for.
    pub bead_id: i64,
    /// Files the agent intends to modify.
    pub files: Vec<FileDeclaration>,
    /// Directories that are in scope.
    pub scope_directories: Vec<String>,
    /// When this manifest was created.
    pub created_at: DateTime<Utc>,
}

impl FileManifest {
    /// Create a new file manifest.
    #[must_use]
    pub fn new(agent_id: String, bead_id: i64) -> Self {
        Self {
            agent_id,
            bead_id,
            files: Vec::new(),
            scope_directories: Vec::new(),
            created_at: Utc::now(),
        }
    }

    /// Add a file to the manifest.
    #[must_use]
    pub fn with_file(mut self, file: FileDeclaration) -> Self {
        self.files.push(file);
        self
    }

    /// Add a scope directory.
    #[must_use]
    pub fn with_scope_directory(mut self, dir: String) -> Self {
        self.scope_directories.push(dir);
        self
    }

    /// Get all file paths in this manifest.
    #[must_use]
    pub fn file_paths(&self) -> HashSet<&str> {
        self.files.iter().map(|f| f.path.as_str()).collect()
    }

    /// Check if a path is in scope.
    #[must_use]
    pub fn is_in_scope(&self, path: &str) -> bool {
        // Check if any scope directory matches
        for dir in &self.scope_directories {
            if path.starts_with(dir) || path == dir {
                return true;
            }
        }
        // If no scope directories defined, allow all
        self.scope_directories.is_empty()
    }

    /// Validate that all files are within scope.
    #[must_use]
    pub fn validate_scope(&self) -> ScopeValidation {
        let violations: Vec<_> = self
            .files
            .iter()
            .filter(|f| !self.is_in_scope(&f.path))
            .map(|f| ScopeViolation {
                path: f.path.clone(),
                reason: ViolationReason::OutOfScope,
            })
            .collect();

        ScopeValidation {
            is_valid: violations.is_empty(),
            violations,
        }
    }
}

/// Result of scope validation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeValidation {
    /// Whether all files are within scope.
    pub is_valid: bool,
    /// Any violations found.
    pub violations: Vec<ScopeViolation>,
}

/// A scope violation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeViolation {
    /// The violating path.
    pub path: String,
    /// Reason for the violation.
    pub reason: ViolationReason,
}

/// Reason for a scope violation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ViolationReason {
    /// File is outside declared scope directories.
    OutOfScope,
    /// File is in a protected directory.
    Protected,
    /// File is already claimed by another agent.
    Conflict,
}

impl ViolationReason {
    /// Get string representation.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::OutOfScope => "out_of_scope",
            Self::Protected => "protected",
            Self::Conflict => "conflict",
        }
    }
}

/// File conflict detection result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConflictReport {
    /// Conflicts detected.
    pub conflicts: Vec<FileConflict>,
    /// Whether any conflicts exist.
    pub has_conflicts: bool,
    /// When this report was generated.
    pub generated_at: DateTime<Utc>,
}

impl ConflictReport {
    /// Create an empty conflict report.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            conflicts: Vec::new(),
            has_conflicts: false,
            generated_at: Utc::now(),
        }
    }

    /// Create a report from conflicts.
    #[must_use]
    pub fn from_conflicts(conflicts: Vec<FileConflict>) -> Self {
        let has_conflicts = !conflicts.is_empty();
        Self {
            conflicts,
            has_conflicts,
            generated_at: Utc::now(),
        }
    }
}

/// A file conflict between agents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileConflict {
    /// The conflicting file path.
    pub path: String,
    /// Agents that want to modify this file.
    pub conflicting_agents: Vec<String>,
    /// Beads involved in the conflict.
    pub involved_beads: Vec<i64>,
    /// Types of modifications requested.
    pub modification_types: Vec<ModificationType>,
}

impl FileConflict {
    /// Get a description of the conflict.
    #[must_use]
    pub fn description(&self) -> String {
        format!(
            "File '{}' contested by agents [{}] for beads [{}]",
            self.path,
            self.conflicting_agents.join(", "),
            self.involved_beads
                .iter()
                .map(|b| b.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

/// Check for conflicts between manifests.
#[must_use]
pub fn detect_conflicts(manifests: &[FileManifest]) -> ConflictReport {
    let mut file_claims: HashMap<String, Vec<&FileManifest>> = HashMap::new();

    // Build a map of file -> manifests claiming it
    for manifest in manifests {
        for file in &manifest.files {
            file_claims
                .entry(file.path.clone())
                .or_default()
                .push(manifest);
        }
    }

    // Find conflicts (files claimed by more than one manifest)
    let conflicts: Vec<_> = file_claims
        .iter()
        .filter(|(_, claimants)| claimants.len() > 1)
        .map(|(path, claimants)| FileConflict {
            path: path.clone(),
            conflicting_agents: claimants.iter().map(|m| m.agent_id.clone()).collect(),
            involved_beads: claimants.iter().map(|m| m.bead_id).collect(),
            modification_types: claimants
                .iter()
                .flat_map(|m| m.files.iter().filter(|f| f.path == *path))
                .map(|f| f.modification_type)
                .collect(),
        })
        .collect();

    ConflictReport::from_conflicts(conflicts)
}

/// File claim record stored in the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileClaimRecord {
    /// Unique identifier.
    pub id: i64,
    /// The agent claiming this file.
    pub agent_id: String,
    /// The bead this claim is for.
    pub bead_id: i64,
    /// File path being claimed.
    pub file_path: String,
    /// Type of modification.
    pub modification_type: String,
    /// When the claim was made.
    pub claimed_at: DateTime<Utc>,
    /// When the claim expires (if applicable).
    pub expires_at: Option<DateTime<Utc>>,
    /// Whether the claim is still active.
    pub is_active: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_manifest_is_in_scope() {
        let manifest = FileManifest::new("agent-1".to_string(), 1)
            .with_scope_directory("src/handlers".to_string())
            .with_scope_directory("tests".to_string());

        assert!(manifest.is_in_scope("src/handlers/mod.rs"));
        assert!(manifest.is_in_scope("tests/integration.rs"));
        assert!(!manifest.is_in_scope("src/db/queries.rs"));
    }

    #[test]
    fn test_file_manifest_validate_scope() {
        let manifest = FileManifest::new("agent-1".to_string(), 1)
            .with_scope_directory("src/handlers".to_string())
            .with_file(FileDeclaration::new(
                "src/handlers/user.rs".to_string(),
                ModificationType::Modify,
            ))
            .with_file(FileDeclaration::new(
                "src/db/schema.rs".to_string(),
                ModificationType::Modify,
            ));

        let validation = manifest.validate_scope();

        assert!(!validation.is_valid);
        assert_eq!(validation.violations.len(), 1);
        assert_eq!(validation.violations[0].path, "src/db/schema.rs");
    }

    #[test]
    fn test_detect_conflicts() {
        let manifests = vec![
            FileManifest::new("agent-1".to_string(), 1).with_file(FileDeclaration::new(
                "src/common.rs".to_string(),
                ModificationType::Modify,
            )),
            FileManifest::new("agent-2".to_string(), 2).with_file(FileDeclaration::new(
                "src/common.rs".to_string(),
                ModificationType::Modify,
            )),
            FileManifest::new("agent-3".to_string(), 3).with_file(FileDeclaration::new(
                "src/unique.rs".to_string(),
                ModificationType::Create,
            )),
        ];

        let report = detect_conflicts(&manifests);

        assert!(report.has_conflicts);
        assert_eq!(report.conflicts.len(), 1);
        assert_eq!(report.conflicts[0].path, "src/common.rs");
        assert_eq!(report.conflicts[0].conflicting_agents.len(), 2);
    }

    #[test]
    fn test_file_conflict_description() {
        let conflict = FileConflict {
            path: "src/lib.rs".to_string(),
            conflicting_agents: vec!["agent-1".to_string(), "agent-2".to_string()],
            involved_beads: vec![1, 2],
            modification_types: vec![ModificationType::Modify, ModificationType::Modify],
        };

        let desc = conflict.description();
        assert!(desc.contains("src/lib.rs"));
        assert!(desc.contains("agent-1"));
        assert!(desc.contains("agent-2"));
    }
}
