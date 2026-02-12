#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

mod cli;

use std::env;

use cli::{cli_command_to_request, parse_cli_args, CliAction, CliError};
use serde_json::json;
use swarm::protocol_envelope::ProtocolEnvelope;
use swarm::protocol_runtime;
use swarm::SwarmError;

const VERSION: &str = env!("CARGO_PKG_VERSION");

const HELP_DATA: &str = r#"{
  "n": "swarm",
  "desc": "PostgreSQL-based agent swarm coordination",
  "v": "0.2.0",
  "proto": "v1",
  "fmt": "jsonl",
  "usage": "echo '{\"cmd\":\"<cmd>\"}' | swarm",
  "cmds": [
    ["doctor", "Health check | NEXT: fix failures before proceeding"],
    ["status", "Swarm state | NEXT: if idle>0 & pending>0, run claim-next"],
    ["init", "Full bootstrap (bootstrap+init-db+register) | NEXT: doctor"],
    ["bootstrap", "Repo structure | NEXT: init-db"],
    ["init-db", "Database schema | NEXT: register if seed_agents not set"],
    ["init-local-db", "Local Docker DB | NEXT: init-db with new URL"],
    ["register", "Seed agents | NEXT: status to verify"],
    ["next", "Top bead rec (preview) | NEXT: claim-next to reserve"],
    ["claim-next", "Claim top bead | NEXT: agent with returned agent_id"],
    ["assign", "Explicit assign | NEXT: agent with assigned agent_id"],
    ["agent", "Run pipeline | NEXT: monitor --view progress"],
    ["run-once", "Single cycle | NEXT: status to see result"],
    ["smoke", "Smoke test | NEXT: fix errors before spawn-prompts"],
    ["monitor", "View state | VIEWS: active,progress,failures,messages"],
    ["release", "Free agent | NEXT: status to confirm"],
    ["artifacts", "Get bead outputs | NEXT: parse content by artifact_type"],
    ["resume", "Resumable beads | NEXT: resume-context for details"],
    ["resume-context", "Deep context | NEXT: continue from current_stage"],
    ["qa", "QA checks | NEXT: if fail, check artifacts for details"],
    ["state", "Full dump | USE: debugging complex issues"],
    ["history", "Event log | NEXT: filter by bead_id if needed"],
    ["lock", "Acquire lock | NEXT: proceed if ok:true"],
    ["unlock", "Release lock | NEXT: state to verify"],
    ["agents", "List agents | NEXT: find idle before assign"],
    ["broadcast", "Send message | NEXT: monitor --view messages"],
    ["load-profile", "Simulate load | NEXT: monitor during run"],
    ["spawn-prompts", "Generate prompts | NEXT: launch agents with files"],
    ["prompt", "Get prompt text | NEXT: use for agent config"],
    ["batch", "Multi-command | NOTE: use ops key, stops on first fail"],
    ["?", "This help | SEE: examples for patterns"]
  ],
  "workflows": {
    "fresh_start": ["doctor", "init", "doctor", "status"],
    "single_agent": ["claim-next", "agent --id N", "monitor --view progress"],
    "parallel_launch": ["register --count 12", "spawn-prompts --count 12"],
    "recovery": ["resume", "resume-context --bead-id X"],
    "debug": ["status", "history", "artifacts --bead-id X"]
  },
  "examples": [
    {"desc": "Quick start", "cmd": "echo '{\"cmd\":\"init\"}' | swarm"},
    {"desc": "Health check", "cmd": "echo '{\"cmd\":\"doctor\"}' | swarm"},
    {"desc": "Assign bead", "cmd": "echo '{\"cmd\":\"assign\",\"bead_id\":\"bd-abc123\",\"agent_id\":1}' | swarm"},
    {"desc": "Dry run", "cmd": "echo '{\"cmd\":\"agent\",\"id\":1,\"dry\":true}' | swarm"},
    {"desc": "Batch (use ops)", "cmd": "echo '{\"cmd\":\"batch\",\"ops\":[{\"cmd\":\"doctor\"},{\"cmd\":\"status\"}]}' | swarm"},
    {"desc": "Monitor progress", "cmd": "echo '{\"cmd\":\"monitor\",\"view\":\"progress\"}' | swarm"},
    {"desc": "Get artifacts", "cmd": "echo '{\"cmd\":\"artifacts\",\"bead_id\":\"bd-abc\"}' | swarm"}
  ],
  "batch_input": {
    "required": "ops",
    "not": "cmds",
    "example": "echo '{\"cmd\":\"batch\",\"ops\":[{\"cmd\":\"doctor\"},{\"cmd\":\"status\"}]}' | swarm"
  },
  "resp": {
    "ok": "bool - success",
    "d": "object - data",
    "err": "object - error",
    "t": "number - timestamp",
    "state": "object - current state"
  }
}"#;

fn handle_cli_action(
    action: &CliAction,
    _unknown_arg: Option<&str>,
) -> (Option<String>, i32, bool) {
    match action {
        CliAction::ShowHelp => {
            let help_json: serde_json::Value = serde_json::from_str(HELP_DATA).unwrap_or_default();
            let envelope = ProtocolEnvelope::success(None, help_json);
            (
                Some(serde_json::to_string(&envelope).unwrap_or_default()),
                0,
                false,
            )
        }
        CliAction::ShowVersion => {
            let version_data = json!({
                "n": "swarm",
                "v": VERSION,
                "proto": "v1"
            });
            let envelope = ProtocolEnvelope::success(None, version_data);
            (
                Some(serde_json::to_string(&envelope).unwrap_or_default()),
                0,
                false,
            )
        }
        CliAction::RunProtocol => (None, 0, true),
        CliAction::Command(cmd) => {
            let json = cli_command_to_request(cmd.clone());
            (Some(json), 0, false)
        }
    }
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();

    tracing_subscriber::fmt::init();

    let args: Vec<String> = env::args().skip(1).collect();

    let action = match parse_cli_args(&args) {
        Ok(a) => a,
        Err(err) => {
            eprintln!("Error: {err}");
            if let CliError::UnknownCommand { cmd } = &err {
                let suggestions = cli::suggest_commands(cmd);
                if let Some(suggestion) = suggestions.first() {
                    eprintln!("Did you mean: {suggestion}?");
                }
            }
            std::process::exit(1);
        }
    };

    let (input_or_output, code, is_loop) = handle_cli_action(&action, None);

    if is_loop {
        let exit_code = match run().await {
            Ok(()) => 0,
            Err(err) => {
                let envelope =
                    ProtocolEnvelope::error(None, err.code().to_string(), err.to_string());
                println!("{}", serde_json::to_string(&envelope).unwrap_or_default());
                err.exit_code()
            }
        };
        std::process::exit(exit_code);
    }

    if let Some(msg) = input_or_output {
        if code != 0 || matches!(action, CliAction::ShowHelp | CliAction::ShowVersion) {
            println!("{msg}");
            std::process::exit(code);
        }

        let exit_code = match protocol_runtime::process_protocol_line(&msg).await {
            Ok(()) => 0,
            Err(err) => {
                let envelope =
                    ProtocolEnvelope::error(None, err.code().to_string(), err.to_string());
                println!("{}", serde_json::to_string(&envelope).unwrap_or_default());
                err.exit_code()
            }
        };
        std::process::exit(exit_code);
    }
}

async fn run() -> std::result::Result<(), SwarmError> {
    protocol_runtime::run_protocol_loop().await
}
