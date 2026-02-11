#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

mod agent_runtime;
mod agent_runtime_support;
mod config;
mod protocol_runtime;

use serde_json::json;
use std::env;
use swarm::protocol_envelope::ProtocolEnvelope;
use swarm::CliError;
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
    ["init", "Initialize swarm (bootstrap + init-db + register)"],
    ["doctor", "Environment health check"],
    ["status", "Show swarm state"],
    ["resume", "Show resumable context projections"],
    ["resume-context", "Show deep resume context payload"],
    ["resume-context", "Show deep resume context payload"],
    ["artifacts", "Retrieve bead artifacts"],
    ["agent", "Run single agent"],
    ["monitor", "View agents/progress"],
    ["register", "Register agents"],
    ["release", "Release agent claim"],
    ["prompt", "Return agent/skill prompt"],
    ["smoke", "Run smoke test"],
    ["init-db", "Initialize database"],
    ["bootstrap", "Bootstrap repo"],
    ["batch", "Execute multiple commands"],
    ["state", "Full coordinator state"],
    ["?", "This help"]
  ],
  "examples": [
    {"desc": "Quick start", "cmd": "echo '{\"cmd\":\"init\"}' | swarm"},
    {"desc": "Health check", "cmd": "echo '{\"cmd\":\"doctor\"}' | swarm"},
    {"desc": "Dry run", "cmd": "echo '{\"cmd\":\"agent\",\"id\":1,\"dry\":true}' | swarm"}
  ],
  "resp": {
    "ok": "bool - success",
    "d": "object - data",
    "err": "object - error",
    "t": "number - timestamp",
    "state": "object - current state"
  }
}"#;

#[derive(Debug, Clone)]
pub enum CliCommand {
    Doctor,
    Help,
    Status,
    Resume,
    ResumeContext,
    ResumeContext,
    Artifacts {
        bead_id: String,
        artifact_type: Option<String>,
    },
    Agent {
        id: u32,
        dry: Option<bool>,
    },
    Init {
        dry: Option<bool>,
        database_url: Option<String>,
        schema: Option<String>,
        seed_agents: Option<u32>,
    },
    Register {
        count: Option<u32>,
        dry: Option<bool>,
    },
    Release {
        agent_id: u32,
        dry: Option<bool>,
    },
    Monitor {
        view: Option<String>,
        watch_ms: Option<u64>,
    },
    InitDb {
        url: Option<String>,
        schema: Option<String>,
        seed_agents: Option<u32>,
        dry: Option<bool>,
    },
    InitLocalDb {
        container_name: Option<String>,
        port: Option<u16>,
        user: Option<String>,
        database: Option<String>,
        schema: Option<String>,
        seed_agents: Option<u32>,
        dry: Option<bool>,
    },
    Bootstrap {
        dry: Option<bool>,
    },
    SpawnPrompts {
        template: Option<String>,
        out_dir: Option<String>,
        count: Option<u32>,
        dry: Option<bool>,
    },
    Prompt {
        id: u32,
        skill: Option<String>,
    },
    Smoke {
        id: u32,
        dry: Option<bool>,
    },
    Batch {
        dry: Option<bool>,
    },
    State,
    History {
        limit: Option<i64>,
    },
    Lock {
        resource: String,
        agent: String,
        ttl_ms: i64,
        dry: Option<bool>,
    },
    Unlock {
        resource: String,
        agent: String,
        dry: Option<bool>,
    },
    Agents,
    Broadcast {
        msg: String,
        from: String,
        dry: Option<bool>,
    },
    LoadProfile {
        agents: Option<u32>,
        rounds: Option<u32>,
        timeout_ms: Option<u64>,
        dry: Option<bool>,
    },
    Json(String),
}

#[derive(Debug, Clone)]
enum CliAction {
    ShowHelp,
    ShowVersion,
    RunProtocol,
    Command(CliCommand),
}

#[allow(clippy::too_many_lines)]
fn parse_cli_args(args: &[String]) -> Result<CliAction, CliError> {
    match args.first().map(String::as_str) {
        None | Some("--") => Ok(CliAction::RunProtocol),
        Some("-h" | "--help") => Ok(CliAction::ShowHelp),
        Some("-v" | "--version") => Ok(CliAction::ShowVersion),
        Some("--json") => {
            if args.len() < 2 {
                Err(CliError::MissingRequiredArg {
                    arg_name: "command".to_string(),
                    usage: "--json <command>".to_string(),
                })
            } else {
                Ok(CliAction::Command(CliCommand::Json(args[1].clone())))
            }
        }

        // Commands with no args
        Some("doctor") => Ok(CliAction::Command(CliCommand::Doctor)),
        Some("status") => Ok(CliAction::Command(CliCommand::Status)),
        Some("resume") => Ok(CliAction::Command(CliCommand::Resume)),
        Some("resume-context") => Ok(CliAction::Command(CliCommand::ResumeContext)),
        Some("artifacts") => {
            let bead_id = parse_required_arg::<String>(args, "bead_id")?;
            let artifact_type = parse_optional_arg::<String>(args, "artifact_type")?;
            Ok(CliAction::Command(CliCommand::Artifacts { bead_id, artifact_type }))
        }
        Some("resume-context") => Ok(CliAction::Command(CliCommand::ResumeContext)),
        Some("?" | "help") => Ok(CliAction::Command(CliCommand::Help)),
        Some("state") => Ok(CliAction::Command(CliCommand::State)),
        Some("agents") => Ok(CliAction::Command(CliCommand::Agents)),
        Some("batch") => {
            let dry = parse_optional_arg(args, "dry")?;
            Ok(CliAction::Command(CliCommand::Batch { dry }))
        }

        // Commands with required args
        Some("agent") => {
            let id = parse_required_arg(args, "id")?;
            let dry = parse_optional_arg(args, "dry")?;
            Ok(CliAction::Command(CliCommand::Agent { id, dry }))
        }

        Some("init") => {
            let dry = parse_optional_arg(args, "dry")?;
            let database_url = parse_optional_arg(args, "database_url")?;
            let schema = parse_optional_arg(args, "schema")?;
            let seed_agents = parse_optional_arg(args, "seed_agents")?;
            Ok(CliAction::Command(CliCommand::Init {
                dry,
                database_url,
                schema,
                seed_agents,
            }))
        }

        Some("register") => {
            let count = parse_optional_arg(args, "count")?;
            let dry = parse_optional_arg(args, "dry")?;
            Ok(CliAction::Command(CliCommand::Register { count, dry }))
        }

        Some("release") => {
            let agent_id = parse_required_arg(args, "agent_id")?;
            let dry = parse_optional_arg(args, "dry")?;
            Ok(CliAction::Command(CliCommand::Release { agent_id, dry }))
        }

        Some("monitor") => {
            let view = parse_optional_arg(args, "view")?;
            let watch_ms = parse_optional_arg(args, "watch_ms")?;
            Ok(CliAction::Command(CliCommand::Monitor { view, watch_ms }))
        }

        Some("init-db") => {
            let url = parse_optional_arg(args, "url")?;
            let schema = parse_optional_arg(args, "schema")?;
            let seed_agents = parse_optional_arg(args, "seed_agents")?;
            let dry = parse_optional_arg(args, "dry")?;
            Ok(CliAction::Command(CliCommand::InitDb {
                url,
                schema,
                seed_agents,
                dry,
            }))
        }

        Some("init-local-db") => {
            let container_name = parse_optional_arg(args, "container_name")?;
            let port = parse_optional_arg(args, "port")?;
            let user = parse_optional_arg(args, "user")?;
            let database = parse_optional_arg(args, "database")?;
            let schema = parse_optional_arg(args, "schema")?;
            let seed_agents = parse_optional_arg(args, "seed_agents")?;
            let dry = parse_optional_arg(args, "dry")?;
            Ok(CliAction::Command(CliCommand::InitLocalDb {
                container_name,
                port,
                user,
                database,
                schema,
                seed_agents,
                dry,
            }))
        }

        Some("bootstrap") => {
            let dry = parse_optional_arg(args, "dry")?;
            Ok(CliAction::Command(CliCommand::Bootstrap { dry }))
        }

        Some("spawn-prompts") => {
            let template = parse_optional_arg(args, "template")?;
            let out_dir = parse_optional_arg(args, "out_dir")?;
            let count = parse_optional_arg(args, "count")?;
            let dry = parse_optional_arg(args, "dry")?;
            Ok(CliAction::Command(CliCommand::SpawnPrompts {
                template,
                out_dir,
                count,
                dry,
            }))
        }

        Some("prompt") => {
            let id = parse_optional_arg(args, "id")?.map_or(1, |v: u32| v);
            let skill = parse_optional_arg(args, "skill")?;
            Ok(CliAction::Command(CliCommand::Prompt { id, skill }))
        }

        Some("smoke") => {
            let id = parse_optional_arg(args, "id")?.map_or(1, |v: u32| v);
            let dry = parse_optional_arg(args, "dry")?;
            Ok(CliAction::Command(CliCommand::Smoke { id, dry }))
        }

        Some("history") => {
            let limit = parse_optional_arg(args, "limit")?;
            Ok(CliAction::Command(CliCommand::History { limit }))
        }

        Some("lock") => {
            let resource = parse_required_arg(args, "resource")?;
            let agent = parse_required_arg(args, "agent")?;
            let ttl_ms = parse_required_arg(args, "ttl_ms")?;
            let dry = parse_optional_arg(args, "dry")?;
            Ok(CliAction::Command(CliCommand::Lock {
                resource,
                agent,
                ttl_ms,
                dry,
            }))
        }

        Some("unlock") => {
            let resource = parse_required_arg(args, "resource")?;
            let agent = parse_required_arg(args, "agent")?;
            let dry = parse_optional_arg(args, "dry")?;
            Ok(CliAction::Command(CliCommand::Unlock {
                resource,
                agent,
                dry,
            }))
        }

        Some("broadcast") => {
            let msg = parse_required_arg(args, "msg")?;
            let from = parse_required_arg(args, "from")?;
            let dry = parse_optional_arg(args, "dry")?;
            Ok(CliAction::Command(CliCommand::Broadcast { msg, from, dry }))
        }

        Some("load-profile") => {
            let agents = parse_optional_arg(args, "agents")?;
            let rounds = parse_optional_arg(args, "rounds")?;
            let timeout_ms = parse_optional_arg(args, "timeout_ms")?;
            let dry = parse_optional_arg(args, "dry")?;
            Ok(CliAction::Command(CliCommand::LoadProfile {
                agents,
                rounds,
                timeout_ms,
                dry,
            }))
        }

        Some(cmd) => Err(CliError::UnknownCommand {
            command: cmd.to_string(),
            suggestions: suggest_commands(cmd),
        }),
    }
}

#[allow(clippy::too_many_lines)]
fn cli_command_to_request(cmd: CliCommand) -> String {
    let (cmd_name, dry, args) = match cmd {
        CliCommand::Doctor => ("doctor".to_string(), None, serde_json::Map::new()),

        CliCommand::Help => ("?".to_string(), None, serde_json::Map::new()),

        CliCommand::Status => ("status".to_string(), None, serde_json::Map::new()),

        CliCommand::Resume => ("resume".to_string(), None, serde_json::Map::new()),
        CliCommand::Artifacts {
            bead_id,
            artifact_type,
        } => {
            let mut args = serde_json::Map::new();
            args.insert("bead_id".to_string(), json!(bead_id));
            if let Some(kind) = artifact_type {
                args.insert("artifact_type".to_string(), json!(kind));
            }
            ("artifacts".to_string(), None, args)
        }
        CliCommand::ResumeContext => ("resume-context".to_string(), None, serde_json::Map::new()),

        CliCommand::Agent { id, dry } => {
            let mut args = serde_json::Map::new();
            args.insert("id".to_string(), json!(id));
            ("agent".to_string(), dry, args)
        }

        CliCommand::Init {
            dry,
            database_url,
            schema,
            seed_agents,
        } => {
            let mut args = serde_json::Map::new();
            if let Some(url) = database_url {
                args.insert("database_url".to_string(), json!(url));
            }
            if let Some(schema_path) = schema {
                args.insert("schema".to_string(), json!(schema_path));
            }
            if let Some(seeds) = seed_agents {
                args.insert("seed_agents".to_string(), json!(seeds));
            }
            ("init".to_string(), dry, args)
        }

        CliCommand::Register { count, dry } => {
            let mut args = serde_json::Map::new();
            if let Some(cnt) = count {
                args.insert("count".to_string(), json!(cnt));
            }
            ("register".to_string(), dry, args)
        }

        CliCommand::Release { agent_id, dry } => {
            let mut args = serde_json::Map::new();
            args.insert("agent_id".to_string(), json!(agent_id));
            ("release".to_string(), dry, args)
        }

        CliCommand::Monitor { view, watch_ms } => {
            let mut args = serde_json::Map::new();
            if let Some(v) = view {
                args.insert("view".to_string(), json!(v));
            }
            if let Some(w) = watch_ms {
                args.insert("watch_ms".to_string(), json!(w));
            }
            ("monitor".to_string(), None, args)
        }

        CliCommand::InitDb {
            url,
            schema,
            seed_agents,
            dry,
        } => {
            let mut args = serde_json::Map::new();
            if let Some(u) = url {
                args.insert("url".to_string(), json!(u));
            }
            if let Some(schema_path) = schema {
                args.insert("schema".to_string(), json!(schema_path));
            }
            if let Some(seeds) = seed_agents {
                args.insert("seed_agents".to_string(), json!(seeds));
            }
            ("init-db".to_string(), dry, args)
        }

        CliCommand::InitLocalDb {
            container_name,
            port,
            user,
            database,
            schema,
            seed_agents,
            dry,
        } => {
            let mut args = serde_json::Map::new();
            if let Some(name) = container_name {
                args.insert("container_name".to_string(), json!(name));
            }
            if let Some(p) = port {
                args.insert("port".to_string(), json!(p));
            }
            if let Some(u) = user {
                args.insert("user".to_string(), json!(u));
            }
            if let Some(db) = database {
                args.insert("database".to_string(), json!(db));
            }
            if let Some(schema_path) = schema {
                args.insert("schema".to_string(), json!(schema_path));
            }
            if let Some(seeds) = seed_agents {
                args.insert("seed_agents".to_string(), json!(seeds));
            }
            ("init-local-db".to_string(), dry, args)
        }

        CliCommand::Bootstrap { dry } => ("bootstrap".to_string(), dry, serde_json::Map::new()),

        CliCommand::SpawnPrompts {
            template,
            out_dir,
            count,
            dry,
        } => {
            let mut args = serde_json::Map::new();
            if let Some(t) = template {
                args.insert("template".to_string(), json!(t));
            }
            if let Some(dir) = out_dir {
                args.insert("out_dir".to_string(), json!(dir));
            }
            if let Some(c) = count {
                args.insert("count".to_string(), json!(c));
            }
            ("spawn-prompts".to_string(), dry, args)
        }

        CliCommand::Prompt { id, skill } => {
            let mut args = serde_json::Map::new();
            args.insert("id".to_string(), json!(id));
            if let Some(s) = skill {
                args.insert("skill".to_string(), json!(s));
            }
            ("prompt".to_string(), None, args)
        }

        CliCommand::Smoke { id, dry } => {
            let mut args = serde_json::Map::new();
            args.insert("id".to_string(), json!(id));
            ("smoke".to_string(), dry, args)
        }

        CliCommand::Batch { dry } => ("batch".to_string(), dry, serde_json::Map::new()),

        CliCommand::State => ("state".to_string(), None, serde_json::Map::new()),

        CliCommand::History { limit } => {
            let mut args = serde_json::Map::new();
            if let Some(l) = limit {
                args.insert("limit".to_string(), json!(l));
            }
            ("history".to_string(), None, args)
        }

        CliCommand::Lock {
            resource,
            agent,
            ttl_ms,
            dry,
        } => {
            let mut args = serde_json::Map::new();
            args.insert("resource".to_string(), json!(resource));
            args.insert("agent".to_string(), json!(agent));
            args.insert("ttl_ms".to_string(), json!(ttl_ms));
            ("lock".to_string(), dry, args)
        }

        CliCommand::Unlock {
            resource,
            agent,
            dry,
        } => {
            let mut args = serde_json::Map::new();
            args.insert("resource".to_string(), json!(resource));
            args.insert("agent".to_string(), json!(agent));
            ("unlock".to_string(), dry, args)
        }

        CliCommand::Agents => ("agents".to_string(), None, serde_json::Map::new()),

        CliCommand::Broadcast { msg, from, dry } => {
            let mut args = serde_json::Map::new();
            args.insert("msg".to_string(), json!(msg));
            args.insert("from".to_string(), json!(from));
            ("broadcast".to_string(), dry, args)
        }

        CliCommand::LoadProfile {
            agents,
            rounds,
            timeout_ms,
            dry,
        } => {
            let mut args = serde_json::Map::new();
            if let Some(a) = agents {
                args.insert("agents".to_string(), json!(a));
            }
            if let Some(r) = rounds {
                args.insert("rounds".to_string(), json!(r));
            }
            if let Some(t) = timeout_ms {
                args.insert("timeout_ms".to_string(), json!(t));
            }
            ("load-profile".to_string(), dry, args)
        }

        CliCommand::Json(cmd) => (cmd, None, serde_json::Map::new()),
    };

    let request = protocol_runtime::ProtocolRequest {
        cmd: cmd_name,
        rid: None,
        dry,
        args,
    };
    serde_json::to_string(&request).unwrap_or_default()
}

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

fn parse_required_arg<T>(args: &[String], name: &str) -> Result<T, CliError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let flag = format!("--{}", name.replace('_', "-"));
    args.iter()
        .position(|a| a.as_str() == flag)
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse::<T>().ok())
        .ok_or_else(|| CliError::MissingRequiredArg {
            arg_name: name.to_string(),
            usage: format!("--{} <value>", name.replace('_', "-")),
        })
}

fn parse_optional_arg<T>(args: &[String], name: &str) -> Result<Option<T>, CliError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let flag = format!("--{}", name.replace('_', "-"));
    args.iter()
        .position(|a| a.as_str() == flag)
        .and_then(|i| args.get(i + 1))
        .map(|v| {
            v.parse::<T>().map_err(|e| CliError::InvalidArgValue {
                arg_name: name.to_string(),
                value: format!("{e}"),
                expected: std::any::type_name::<T>().to_string(),
            })
        })
        .transpose()
}

fn suggest_commands(typo: &str) -> Vec<String> {
    const VALID_COMMANDS: &[&str] = &[
        "doctor",
        "help",
        "status",
        "resume",
        "resume-context",
        "resume-context",
        "artifacts",
        "agent",
        "init",
        "register",
        "release",
        "monitor",
        "init-db",
        "init-local-db",
        "bootstrap",
        "spawn-prompts",
        "prompt",
        "smoke",
        "batch",
        "state",
        "history",
        "lock",
        "unlock",
        "agents",
        "broadcast",
        "load-profile",
    ];

    VALID_COMMANDS
        .iter()
        .map(|cmd| (cmd, strsim::levenshtein(typo, cmd)))
        .filter(|(_, dist)| *dist <= 3)
        .min_by_key(|(_, dist)| *dist)
        .map(|(cmd, _)| vec![cmd.to_string()])
        .unwrap_or_default()
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
            if let CliError::UnknownCommand {
                command: _,
                suggestions,
            } = &err
            {
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
