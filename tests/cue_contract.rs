use assert_cmd::Command;
use std::process::Command as ProcessCommand;
use tempfile::NamedTempFile;

#[test]
fn protocol_response_validates_against_cue_schema_when_cue_is_available() -> Result<(), String> {
    let cue_available = ProcessCommand::new("bash")
        .arg("-lc")
        .arg("command -v cue")
        .output()
        .map(|output| output.status.success())
        .unwrap_or_else(|_| false);

    if !cue_available {
        return Ok(());
    }

    let assert = Command::new(assert_cmd::cargo::cargo_bin!("swarm"))
        .write_stdin("{\"cmd\":\"?\"}\n")
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout)
        .trim()
        .to_string();
    let mut response_file =
        NamedTempFile::new().map_err(|err| format!("failed to create temp file: {}", err))?;
    std::io::Write::write_all(&mut response_file, stdout.as_bytes())
        .map_err(|err| format!("failed to write temp response file: {}", err))?;

    let vet = ProcessCommand::new("bash")
        .arg("-lc")
        .arg(format!(
            "cue vet -d '#Response' - ai_cli_protocol.cue < '{}'",
            response_file.path().to_string_lossy()
        ))
        .output()
        .map_err(|err| format!("failed to run cue vet: {}", err))?;

    if vet.status.success() {
        Ok(())
    } else {
        Err(format!(
            "cue vet failed: {}",
            String::from_utf8_lossy(&vet.stderr)
        ))
    }
}
