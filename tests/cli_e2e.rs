use assert_cmd::Command;
use serde_json::Value;
use std::path::PathBuf;

fn swarm_command() -> Command {
    Command::new(assert_cmd::cargo::cargo_bin!("swarm"))
}

fn parse_json_output(raw: &[u8]) -> Value {
    let text = String::from_utf8_lossy(raw).trim().to_string();
    serde_json::from_str::<Value>(&text)
        .unwrap_or_else(|e| panic!("expected JSON output, got '{}': {}", text, e))
}

#[test]
fn init_defaults_to_json_output() {
    let assert = swarm_command().arg("init").assert().success();

    let output = assert.get_output();
    let json = parse_json_output(&output.stdout);

    assert_eq!(json["command"], "init");
    assert_eq!(json["status"], "ok");
    assert_eq!(json["payload"]["message"], "Swarm CLI ready");
}

#[test]
fn init_supports_text_output_override() {
    let assert = swarm_command()
        .args(["init", "--output", "text"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout).to_string();
    assert!(stdout.contains("Swarm CLI ready"));
    assert!(!stdout.contains("\"status\":\"ok\""));
}

#[test]
fn spawn_prompts_errors_are_structured_json_by_default() {
    let temp_dir =
        tempfile::tempdir().unwrap_or_else(|e| panic!("failed to create tempdir: {}", e));
    let missing_template: PathBuf = temp_dir.path().join("missing_template.md");
    let out_dir: PathBuf = temp_dir.path().join("generated");

    let assert = swarm_command()
        .args([
            "spawn-prompts",
            "--template",
            missing_template.to_string_lossy().as_ref(),
            "--out-dir",
            out_dir.to_string_lossy().as_ref(),
            "--count",
            "1",
        ])
        .assert()
        .failure()
        .code(2);

    let stderr_json = parse_json_output(&assert.get_output().stderr);
    assert_eq!(stderr_json["status"], "error");
    assert_eq!(stderr_json["error"]["kind"], "config_error");
    assert_eq!(stderr_json["error"]["exit_code"], 2);
    assert!(stderr_json["error"]["message"]
        .as_str()
        .map_or(false, |msg| msg.contains("Failed to read template")));
}
