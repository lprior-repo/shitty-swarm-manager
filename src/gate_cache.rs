use crate::error::{Result, SwarmError};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::sync::RwLock;
use tracing::debug;

#[derive(Debug, Clone)]
struct CacheEntry {
    fingerprint: String,
    success: bool,
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
}

pub struct GateExecutionCache {
    entries: RwLock<HashMap<String, CacheEntry>>,
    source_dir: PathBuf,
    extensions: Vec<&'static str>,
}

impl GateExecutionCache {
    /// Creates a new gate execution cache for the given source directory.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::ConfigError` if the source directory does not exist.
    pub fn new<P: AsRef<Path>>(source_dir: P) -> Result<Self> {
        let source_dir = source_dir.as_ref().to_path_buf();
        if !source_dir.exists() {
            return Err(SwarmError::ConfigError(format!(
                "Source directory does not exist: {}",
                source_dir.display()
            )));
        }

        Ok(Self {
            entries: RwLock::new(HashMap::new()),
            source_dir,
            extensions: vec!["rs", "toml", "yaml", "yml", "json"],
        })
    }

    async fn calculate_fingerprint(&self) -> Result<String> {
        let mut files = self.collect_source_files(&self.source_dir).await?;
        files.sort_by(|a, b| a.to_string_lossy().cmp(&b.to_string_lossy()));

        let mut hasher = Sha256::new();
        for path in files {
            match tokio::fs::read(&path).await {
                Ok(contents) => {
                    let rel = path.strip_prefix(&self.source_dir).map_or_else(
                        |_| path.to_string_lossy().into_owned(),
                        |p| p.to_string_lossy().into_owned(),
                    );
                    hasher.update(rel.as_bytes());
                    hasher.update(&contents);
                }
                Err(err) => {
                    debug!("Skipping unreadable file {}: {}", path.display(), err);
                }
            }
        }

        Ok(format!("{:x}", hasher.finalize()))
    }

    async fn collect_source_files(&self, root: &Path) -> Result<Vec<PathBuf>> {
        let mut stack = vec![root.to_path_buf()];
        let mut files = Vec::new();

        while let Some(dir) = stack.pop() {
            let mut entries = tokio::fs::read_dir(&dir)
                .await
                .map_err(SwarmError::IoError)?;

            while let Some(entry) = entries.next_entry().await.map_err(SwarmError::IoError)? {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.is_file() && self.path_has_allowed_extension(&path) {
                    files.push(path);
                }
            }
        }

        Ok(files)
    }

    fn path_has_allowed_extension(&self, path: &Path) -> bool {
        path.extension()
            .is_some_and(|ext| ext.to_str().is_some_and(|e| self.extensions.contains(&e)))
    }

    pub async fn get(&self, task_name: &str) -> Option<(bool, Option<i32>, String, String)> {
        let current_fingerprint = self.calculate_fingerprint().await.ok()?;
        let entries = self.entries.read().await;
        entries.get(task_name).and_then(|entry| {
            if entry.fingerprint == current_fingerprint {
                Some((
                    entry.success,
                    entry.exit_code,
                    entry.stdout.clone(),
                    entry.stderr.clone(),
                ))
            } else {
                None
            }
        })
    }

    /// Stores a gate execution result in the cache.
    ///
    /// # Errors
    ///
    /// Returns `SwarmError::DatabaseError` if the fingerprint calculation fails.
    pub async fn put(
        &self,
        task_name: String,
        success: bool,
        exit_code: Option<i32>,
        stdout: String,
        stderr: String,
    ) -> Result<()> {
        let fingerprint = self.calculate_fingerprint().await?;
        let entry = CacheEntry {
            fingerprint,
            success,
            exit_code,
            stdout,
            stderr,
        };

        self.entries.write().await.insert(task_name.clone(), entry);
        debug!("Cached result for task {}", task_name);
        Ok(())
    }

    pub async fn clear(&self) {
        self.entries.write().await.clear();
    }

    pub async fn clear_task(&self, task_name: &str) {
        self.entries.write().await.remove(task_name);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cache_miss_when_no_entry() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache = GateExecutionCache::new(temp_dir.path()).unwrap();
        assert!(cache.get(":quick").await.is_none());
    }

    #[tokio::test]
    async fn cache_put_and_get_round_trip() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache = GateExecutionCache::new(temp_dir.path()).unwrap();

        let put_result = cache
            .put(
                ":quick".to_string(),
                true,
                Some(0),
                "stdout".to_string(),
                "stderr".to_string(),
            )
            .await;
        assert!(put_result.is_ok());

        let result = cache.get(":quick").await;
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn cache_invalidates_on_source_change() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.rs");
        tokio::fs::write(&test_file, "content1").await.unwrap();

        let cache = GateExecutionCache::new(temp_dir.path()).unwrap();
        let put_result = cache
            .put(
                ":quick".to_string(),
                true,
                Some(0),
                "stdout".to_string(),
                "stderr".to_string(),
            )
            .await;
        assert!(put_result.is_ok());

        tokio::fs::write(&test_file, "content2").await.unwrap();
        assert!(cache.get(":quick").await.is_none());
    }
}
