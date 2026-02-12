#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use super::action::CliAction;
use super::commands::CliCommand;

#[derive(Debug, Clone, thiserror::Error)]
pub enum CliError {
    #[error("Missing required argument: {}", arg)]
    MissingRequiredArg { arg: String },
    #[error("Unknown command: {}", cmd)]
    UnknownCommand { cmd: String },
    #[error("Invalid type for {}", arg)]
    InvalidArgType { arg: String },
    #[error("Invalid argument value for {}: {}", arg, error)]
    InvalidArgValue { arg: String, error: String },
}

pub fn parse_cli_args(args: &[String]) -> Result<CliAction, CliError> {
    if args
        .get(1)
        .is_some_and(|arg| matches!(arg.as_str(), "-h" | "--help"))
    {
        return Ok(CliAction::ShowHelp);
    }

    match args.first().map(String::as_str) {
        None | Some("--") => Ok(CliAction::RunProtocol),
        Some("-h" | "--help") => Ok(CliAction::ShowHelp),
        Some("-v" | "--version") => Ok(CliAction::ShowVersion),
        Some("--json") => {
            if args.len() < 2 {
                Err(CliError::MissingRequiredArg {
                    arg: "command".to_string(),
                })
            } else {
                Ok(CliAction::Command(CliCommand::Json(args[1].clone())))
            }
        }
        Some("doctor") => Ok(CliAction::Command(CliCommand::Doctor)),
        Some("status") => Ok(CliAction::Command(CliCommand::Status)),
        Some("next") => Ok(CliAction::Command(CliCommand::Next {
            dry: parse_optional_arg(args, "dry")?,
        })),
        Some("claim-next") => Ok(CliAction::Command(CliCommand::ClaimNext {
            dry: parse_optional_arg(args, "dry")?,
        })),
        Some("assign") => {
            let bead_id = parse_required_arg(args, "bead_id")?;
            let agent_id = parse_required_arg(args, "agent_id")?;
            let dry = parse_optional_arg(args, "dry")?;
            Ok(CliAction::Command(CliCommand::Assign {
                bead_id,
                agent_id,
                dry,
            }))
        }
        Some("run-once") => {
            let id = parse_optional_arg(args, "id")?;
            let dry = parse_optional_arg(args, "dry")?;
            Ok(CliAction::Command(CliCommand::RunOnce { id, dry }))
        }
        Some("qa") => {
            let target = parse_optional_arg(args, "target")?;
            let id = parse_optional_arg(args, "id")?;
            let dry = parse_optional_arg(args, "dry")?;
            Ok(CliAction::Command(CliCommand::Qa { target, id, dry }))
        }
        Some("resume") => Ok(CliAction::Command(CliCommand::Resume)),
        Some("resume-context") => {
            let bead_id = parse_optional_arg(args, "bead_id")?;
            Ok(CliAction::Command(CliCommand::ResumeContext { bead_id }))
        }
        Some("artifacts") => {
            let bead_id = parse_required_arg::<String>(args, "bead_id")?;
            let artifact_type = parse_optional_arg::<String>(args, "artifact_type")?;
            Ok(CliAction::Command(CliCommand::Artifacts {
                bead_id,
                artifact_type,
            }))
        }
        Some("?" | "help") => Ok(CliAction::Command(CliCommand::Help)),
        Some("state") => Ok(CliAction::Command(CliCommand::State)),
        Some("agents") => Ok(CliAction::Command(CliCommand::Agents)),
        Some("batch") => Ok(CliAction::Command(CliCommand::Batch {
            dry: parse_optional_arg(args, "dry")?,
        })),
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
        Some("bootstrap") => Ok(CliAction::Command(CliCommand::Bootstrap {
            dry: parse_optional_arg(args, "dry")?,
        })),
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
            cmd: cmd.to_string(),
        }),
    }
}

fn parse_required_arg<T>(args: &[String], name: &str) -> Result<T, CliError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let flag = format!("--{}", name.replace('_', "-"));
    let Some(position) = args.iter().position(|a| a.as_str() == flag) else {
        return Err(CliError::MissingRequiredArg {
            arg: name.to_string(),
        });
    };

    let Some(raw_value) = args.get(position + 1) else {
        return Err(CliError::MissingRequiredArg {
            arg: name.to_string(),
        });
    };

    if raw_value.starts_with("--") {
        return Err(CliError::MissingRequiredArg {
            arg: name.to_string(),
        });
    }

    raw_value
        .parse::<T>()
        .map_err(|_| CliError::InvalidArgType {
            arg: name.to_string(),
        })
}

fn parse_optional_arg<T>(args: &[String], name: &str) -> Result<Option<T>, CliError>
where
    T: std::str::FromStr + 'static,
    T::Err: std::fmt::Display,
{
    let flag = format!("--{}", name.replace('_', "-"));
    let position = args.iter().position(|a| a.as_str() == flag);

    match position {
        None => Ok(None),
        Some(i) => {
            let maybe_value = args.get(i + 1);
            let treat_as_boolean_flag = std::any::TypeId::of::<T>()
                == std::any::TypeId::of::<bool>()
                && maybe_value.is_none_or(|v| v.starts_with("--"));

            if treat_as_boolean_flag {
                return "true"
                    .parse::<T>()
                    .map(Some)
                    .map_err(|e| CliError::InvalidArgValue {
                        arg: name.to_string(),
                        error: format!("{e}"),
                    });
            }

            maybe_value
                .map(|v| {
                    if v.starts_with("--") {
                        return Err(CliError::MissingRequiredArg {
                            arg: name.to_string(),
                        });
                    }
                    v.parse::<T>().map_err(|e| CliError::InvalidArgValue {
                        arg: name.to_string(),
                        error: format!("{e}"),
                    })
                })
                .transpose()
        }
    }
}
