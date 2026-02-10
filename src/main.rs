mod agent_runtime;
mod agent_runtime_support;
mod config;
mod protocol_runtime;

use serde_json::json;
use std::env;
use swarm::protocol_envelope::ProtocolEnvelope;
use swarm::SwarmError;

const VERSION: &str = env!("CARGO_PKG_VERSION");

const HELP_DATA: &str = r#"{
  "tool": "swarm",
  "description": "PostgreSQL-based agent swarm coordination for AI-native workflows",
  "version": "0.2.0",
  "protocol": "v1",
  "ai_expectations": {
    "input_format": "JSONL - single line JSON objects via stdin",
    "output_format": "JSONL - parse by keys, not string matching",
    "workflow": [
      "1. Run 'swarm doctor' to verify environment",
      "2. Check 'swarm status' for current state", 
      "3. Use 'dry':true flag before destructive operations",
      "4. Parse 'ok' field to determine success/failure",
      "5. Check 'err.msg' for actionable error details",
      "6. Follow 'next' field suggestions for workflow continuation"
    ],
    "rules": [
      "Always parse JSON responses programmatically",
      "Never assume command success - check 'ok' field",
      "Use dry-run mode for unfamiliar operations",
      "Handle database connection errors gracefully",
      "Follow the state machine transitions exactly"
    ]
  },
  "usage": "echo '{\"cmd\":\"<command>\"}' | swarm",
  "commands": [
    {"cmd": "doctor", "desc": "Check environment health - REQUIRED first step", "ai_priority": "high"},
    {"cmd": "status", "desc": "Show swarm state", "ai_priority": "high"},
    {"cmd": "agent", "desc": "Run single agent pipeline", "args": {"id": "number (1-12)", "dry": "boolean (optional, use for testing)"}, "ai_priority": "high"},
    {"cmd": "smoke", "desc": "Run smoke test for agent", "args": {"id": "number"}, "ai_priority": "medium"},
    {"cmd": "register", "desc": "Register repository agents", "args": {"count": "number"}, "ai_priority": "medium"},
    {"cmd": "release", "desc": "Release agent claim", "args": {"agent_id": "number"}, "ai_priority": "medium"},
    {"cmd": "monitor", "desc": "Read monitor view", "args": {"view": "active|progress|failures|messages", "watch_ms": "number (optional)"}, "ai_priority": "high"},
    {"cmd": "init-db", "desc": "Initialize database schema", "ai_priority": "low"},
    {"cmd": "init-local-db", "desc": "Initialize local database", "ai_priority": "low"},
    {"cmd": "spawn-prompts", "desc": "Generate agent prompt files", "args": {"count": "number"}, "ai_priority": "low"},
    {"cmd": "batch", "desc": "Execute multiple commands", "ai_priority": "medium"},
    {"cmd": "bootstrap", "desc": "Bootstrap swarm environment", "ai_priority": "low"},
    {"cmd": "?", "desc": "Return this API metadata", "ai_priority": "high"}
  ],
  "examples": [
    {
      "desc": "Health check - ALWAYS run this first",
      "cmd": "echo '{\"cmd\":\"doctor\"}' | swarm"
    },
    {
      "desc": "Dry run before executing agent",
      "cmd": "echo '{\"cmd\":\"agent\",\"id\":1,\"dry\":true}' | swarm"
    },
    {
      "desc": "Execute agent for real",
      "cmd": "echo '{\"cmd\":\"agent\",\"id\":1}' | swarm"
    },
    {
      "desc": "Monitor active agents",
      "cmd": "echo '{\"cmd\":\"monitor\",\"view\":\"active\"}' | swarm"
    }
  ],
  "response_schema": {
    "ok": "boolean - true if command succeeded",
    "d": "object - response data (when ok=true)",
    "err": "object - error details (when ok=false)",
    "err.code": "string - error code for programmatic handling",
    "err.msg": "string - human-readable error message",
    "next": "string - suggested next command",
    "t": "number - timestamp in milliseconds",
    "state": "object - current swarm state"
  },
  "documentation": "https://github.com/lewisprior/shitty-swarm-manager"
}"#;

#[derive(Debug, Clone, Copy)]
enum CliAction {
    ShowHelp,
    ShowVersion,
    RunProtocol,
    ExitWithError(i32),
}

fn parse_cli_args(args: &[String]) -> CliAction {
    match args.first().map(String::as_str) {
        None | Some("--") => CliAction::RunProtocol,
        Some("-h") | Some("--help") => CliAction::ShowHelp,
        Some("-v") | Some("--version") => CliAction::ShowVersion,
        Some(_) => CliAction::ExitWithError(1),
    }
}

fn handle_cli_action(action: CliAction, unknown_arg: Option<&str>) -> (Option<String>, i32) {
    match action {
        CliAction::ShowHelp => {
            let help_json: serde_json::Value = serde_json::from_str(HELP_DATA).unwrap_or_default();
            let envelope = ProtocolEnvelope::success(None, help_json);
            (
                Some(serde_json::to_string(&envelope).unwrap_or_default()),
                0,
            )
        }
        CliAction::ShowVersion => {
            let version_data = json!({
                "tool": "swarm",
                "version": VERSION,
                "protocol": "v1"
            });
            let envelope = ProtocolEnvelope::success(None, version_data);
            (
                Some(serde_json::to_string(&envelope).unwrap_or_default()),
                0,
            )
        }
        CliAction::RunProtocol => (None, 0),
        CliAction::ExitWithError(code) => {
            let error_msg = unknown_arg.map_or_else(
                || "Invalid CLI usage".to_string(),
                |arg| {
                    format!(
                        "Unknown argument: {}. Use --help for usage information",
                        arg
                    )
                },
            );
            let envelope = ProtocolEnvelope::error(None, "CLI_ERROR".to_string(), error_msg);
            (
                Some(serde_json::to_string(&envelope).unwrap_or_default()),
                code,
            )
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = env::args().skip(1).collect();
    let action = parse_cli_args(&args);
    let unknown_arg = args
        .first()
        .filter(|_| matches!(action, CliAction::ExitWithError(_)));

    let (output, code) = handle_cli_action(action, unknown_arg.map(String::as_str));

    if let Some(msg) = output {
        println!("{}", msg);
        std::process::exit(code);
    }

    let exit_code = match run().await {
        Ok(()) => 0,
        Err(err) => {
            let envelope = ProtocolEnvelope::error(None, err.code().to_string(), err.to_string());
            println!("{}", serde_json::to_string(&envelope).unwrap_or_default());
            err.exit_code()
        }
    };

    std::process::exit(exit_code);
}

async fn run() -> std::result::Result<(), SwarmError> {
    protocol_runtime::run_protocol_loop().await
}
