use assert_cmd::Command;
use serde_json::Value;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct ScenarioResult {
    pub output: Value,
}

#[derive(Debug, Clone)]
pub struct ProtocolScenarioHarness {
    binary_path: PathBuf,
}

impl ProtocolScenarioHarness {
    #[must_use]
    pub fn new() -> Self {
        Self {
            binary_path: PathBuf::from(assert_cmd::cargo::cargo_bin!("swarm")),
        }
    }

    pub fn run_protocol(&self, input: &str) -> Result<ScenarioResult, String> {
        let assert = Command::new(&self.binary_path)
            .write_stdin(format!("{input}\n"))
            .assert()
            .success();

        let raw = String::from_utf8_lossy(&assert.get_output().stdout)
            .trim()
            .to_string();
        serde_json::from_str::<Value>(&raw)
            .map(|output| ScenarioResult { output })
            .map_err(|err| format!("expected JSON response envelope, got '{raw}': {err}"))
    }
}

impl Default for ProtocolScenarioHarness {
    fn default() -> Self {
        Self::new()
    }
}

pub fn assert_protocol_envelope(output: &Value) -> Result<(), String> {
    match (output.get("ok"), output.get("t"), output.get("ms")) {
        (Some(_), Some(timestamp), Some(duration))
            if timestamp.is_number() && duration.is_number() =>
        {
            Ok(())
        }
        _ => Err(format!(
            "missing or invalid protocol envelope fields in response: {output}"
        )),
    }
}

#[allow(dead_code)]
pub fn assert_contract_test_is_decoupled(relative_path: &str) -> Result<(), String> {
    let source = read_contract_test_source(relative_path)?;
    let forbidden = [
        (
            "schema.sql",
            "contract tests must not assert persistence schema text",
        ),
        (
            "sqlx::query",
            "contract tests must assert protocol/service behavior, not SQL statements",
        ),
        (
            "sqlx::query_as",
            "contract tests must assert protocol/service behavior, not SQL statements",
        ),
        (
            "sqlx::query_scalar",
            "contract tests must assert protocol/service behavior, not SQL statements",
        ),
        (
            "sqlx::raw_sql",
            "contract tests must assert protocol/service behavior, not SQL statements",
        ),
        (
            "use super::",
            "contract tests must not couple to private module internals",
        ),
    ];

    forbidden
        .iter()
        .find(|(snippet, _)| source.contains(snippet))
        .map_or(Ok(()), |(snippet, reason)| {
            Err(format!(
                "{relative_path} contains forbidden internal coupling snippet '{snippet}': {reason}"
            ))
        })
}

#[allow(dead_code)]
fn read_contract_test_source(relative_path: &str) -> Result<String, String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(relative_path);
    std::fs::read_to_string(&path).map_err(|err| {
        format!(
            "failed to read contract test source {}: {err}",
            path.display()
        )
    })
}
