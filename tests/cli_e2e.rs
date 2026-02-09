use assert_cmd::Command;
use serde_json::Value;

fn swarm_command() -> Command {
    Command::new(assert_cmd::cargo::cargo_bin!("swarm"))
}

fn parse_line_json(raw: &[u8]) -> Value {
    let text = String::from_utf8_lossy(raw).trim().to_string();
    serde_json::from_str::<Value>(&text)
        .unwrap_or_else(|err| panic!("expected JSON output, got '{}': {}", text, err))
}

#[test]
fn help_command_returns_protocol_envelope() {
    let assert = swarm_command()
        .write_stdin("{\"cmd\":\"?\",\"rid\":\"r-1\"}\n")
        .assert()
        .success();

    let json = parse_line_json(&assert.get_output().stdout);
    assert_eq!(json["ok"], true);
    assert_eq!(json["rid"], "r-1");
    assert!(json["t"].is_i64());
    assert!(json["ms"].is_i64());
    assert!(json["d"]["commands"].is_object());
    assert!(json["state"]["total"].is_number());
}

#[test]
fn invalid_command_returns_structured_error() {
    let assert = swarm_command()
        .write_stdin("{\"cmd\":\"nope\"}\n")
        .assert()
        .success();

    let json = parse_line_json(&assert.get_output().stdout);
    assert_eq!(json["ok"], false);
    assert_eq!(json["err"]["code"], "INVALID");
    assert!(json["fix"].is_string());
}

#[test]
fn dry_run_lock_uses_standard_dry_shape() {
    let assert = swarm_command()
        .write_stdin("{\"cmd\":\"lock\",\"resource\":\"res_abc\",\"agent\":\"agent-1\",\"ttl_ms\":30000,\"dry\":true}\n")
        .assert()
        .success();

    let json = parse_line_json(&assert.get_output().stdout);
    assert_eq!(json["ok"], true);
    assert_eq!(json["d"]["dry"], true);
    assert!(json["d"]["would_do"].is_array());
    assert!(json["d"]["estimated_ms"].is_number());
}

#[test]
fn batch_partial_success_reports_summary() {
    let payload = "{\"cmd\":\"batch\",\"ops\":[{\"cmd\":\"?\"},{\"cmd\":\"definitely-invalid\"}],\"dry\":false}\n";
    let assert = swarm_command().write_stdin(payload).assert().success();
    let json = parse_line_json(&assert.get_output().stdout);

    assert_eq!(json["ok"], true);
    assert_eq!(json["d"]["summary"]["total"], 2);
    assert_eq!(json["d"]["summary"]["pass"], 1);
    assert_eq!(json["d"]["summary"]["fail"], 1);
}
