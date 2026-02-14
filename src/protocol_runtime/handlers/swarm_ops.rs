#![allow(clippy::too_many_lines)]

use super::super::{
    dry_flag, dry_run_success, handle_register, load_schema_sql, mask_database_url,
    minimal_state_for_request, resolve_database_url_for_init, CommandSuccess, ParseInput,
    ProtocolRequest, EMBEDDED_COORDINATOR_SCHEMA_REF,
};
use crate::protocol_envelope::ProtocolEnvelope;
use crate::{code, SwarmDb, SwarmError};
use serde_json::{json, Map, Value};
use std::path::PathBuf;
use tokio::fs;
use tokio::process::Command;

pub(in crate::protocol_runtime) async fn handle_bootstrap(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let repo_root: PathBuf = current_repo_root().await?;
    let swarm_dir = repo_root.join(".swarm");
    let config_path = swarm_dir.join("config.toml");
    let ignore_path = swarm_dir.join(".swarmignore");

    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![
                json!({"step": 1, "action": "create_dir", "target": swarm_dir.display().to_string()}),
                json!({"step": 2, "action": "write_config", "target": config_path.display().to_string()}),
            ],
            "swarm doctor",
        ));
    }

    fs::create_dir_all(&swarm_dir)
        .await
        .map_err(SwarmError::IoError)
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    let mut actions = Vec::new();
    if !config_path.exists() {
        fs::write(
            &config_path,
            "database_url = \"postgresql://shitty_swarm_manager@localhost:5437/shitty_swarm_manager_db\"\nrust_contract_cmd = \"br show {bead_id}\"\nimplement_cmd = \"jj status\"\nqa_enforcer_cmd = \"moon run :quick\"\nred_queen_cmd = \"moon run :test\"\n",
        )
        .await
        .map_err(SwarmError::IoError)
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
        actions.push("created_config");
    }
    if !ignore_path.exists() {
        fs::write(&ignore_path, "*.log\n.cache/\ntemp/\n")
            .await
            .map_err(SwarmError::IoError)
            .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
        actions.push("created_swarmignore");
    }

    Ok(CommandSuccess {
        data: json!({
            "repo_root": repo_root.display().to_string(),
            "swarm_dir": swarm_dir.display().to_string(),
            "actions_taken": actions,
            "idempotent": true,
        }),
        next: "swarm doctor".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

#[allow(clippy::too_many_lines)]
pub(in crate::protocol_runtime) async fn handle_init(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let seed_agents = request
        .args
        .get("seed_agents")
        .and_then(Value::as_u64)
        .map_or(12, |value| value) as u32;
    let db_url = request
        .args
        .get("database_url")
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string);
    let schema = request
        .args
        .get("schema")
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string);

    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![
                json!({"step": 1, "action": "bootstrap", "target": "repository"}),
                json!({"step": 2, "action": "init_db", "target": db_url.as_ref().map_or_else(|| "auto-discover".to_string(), |url| mask_database_url(url))}),
                json!({"step": 3, "action": "register", "target": seed_agents}),
            ],
            "swarm doctor",
        ));
    }

    let mut steps = Vec::new();
    let mut errors = Vec::new();

    match handle_bootstrap(request).await {
        Ok(success) => {
            steps
                .push(json!({"step": 1, "action": "bootstrap", "status": "ok", "d": success.data}));
        }
        Err(e) => {
            errors.push(json!({"step": 1, "action": "bootstrap", "err": e.err}));
        }
    }

    let init_db_request = ProtocolRequest {
        cmd: "init-db".to_string(),
        rid: request.rid.clone(),
        dry: Some(false),
        args: {
            let mut args =
                Map::from_iter(vec![("seed_agents".to_string(), Value::from(seed_agents))]);
            if let Some(url) = db_url.clone() {
                args.insert("url".to_string(), Value::String(url));
            }
            if let Some(schema_value) = schema.clone() {
                args.insert("schema".to_string(), Value::String(schema_value));
            }
            args
        },
    };
    match handle_init_db(&init_db_request).await {
        Ok(success) => {
            steps.push(json!({"step": 2, "action": "init_db", "status": "ok", "d": success.data}));
        }
        Err(e) => {
            errors.push(json!({"step": 2, "action": "init_db", "err": e.err}));
        }
    }

    let register_request = ProtocolRequest {
        cmd: "register".to_string(),
        rid: request.rid.clone(),
        dry: Some(false),
        args: Map::from_iter(vec![("count".to_string(), Value::from(seed_agents))]),
    };
    match handle_register(&register_request).await {
        Ok(success) => {
            steps.push(json!({"step": 3, "action": "register", "status": "ok", "d": success.data}));
        }
        Err(e) => {
            errors.push(json!({"step": 3, "action": "register", "err": e.err}));
        }
    }

    if errors.is_empty() {
        Ok(CommandSuccess {
            data: json!({
                "initialized": true,
                "steps": steps,
                "database_url": db_url.as_ref().map_or_else(|| "auto-discover".to_string(), |url| mask_database_url(url)),
                "seed_agents": seed_agents,
            }),
            next: "swarm doctor".to_string(),
            state: minimal_state_for_request(request).await,
        })
    } else {
        Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INTERNAL.to_string(),
                format!("Init completed with {} errors", errors.len()),
            )
            .with_fix("Review error details and retry failed steps manually".to_string())
            .with_ctx(json!({"errors": errors, "completed_steps": steps})),
        ))
    }
}

pub(in crate::protocol_runtime) async fn handle_init_db(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let input = crate::InitDbInput::parse_input(request).map_err(|error| {
        Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                error.to_string(),
            )
            .with_fix("echo '{\"cmd\":\"init-db\",\"seed_agents\":12}' | swarm".to_string())
            .with_ctx(json!({"error": error.to_string()})),
        )
    })?;

    let schema = input
        .schema
        .as_deref()
        .map(PathBuf::from)
        .map(|value| value.display().to_string());
    let seed_agents = input.seed_agents.map_or(12, |value| value);

    if dry_flag(request) {
        let dry_database_target = input.url.as_deref().map_or_else(
            || "auto-discover-on-execution".to_string(),
            mask_database_url,
        );
        return Ok(dry_run_success(
            request,
            vec![
                json!({"step": 1, "action": "connect_db", "target": dry_database_target}),
                json!({"step": 2, "action": "apply_schema", "target": schema.clone().unwrap_or_else(|| EMBEDDED_COORDINATOR_SCHEMA_REF.to_string())}),
                json!({"step": 3, "action": "seed_agents", "target": seed_agents}),
            ],
            "swarm state",
        ));
    }

    let url = resolve_database_url_for_init(request).await?;

    let (schema_sql, schema_ref) = load_schema_sql(request.rid.clone(), schema.as_deref()).await?;
    let db: SwarmDb = SwarmDb::new(&url)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
    db.initialize_schema_from_sql(&schema_sql)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
    db.update_config(seed_agents)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;
    db.seed_idle_agents(seed_agents)
        .await
        .map_err(|e| to_protocol_failure(e, request.rid.clone()))?;

    Ok(CommandSuccess {
        data: json!({
            "database_url": mask_database_url(&url),
            "schema": schema_ref,
            "seed_agents": seed_agents
        }),
        next: "swarm state".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

#[allow(clippy::too_many_lines)]
pub(in crate::protocol_runtime) async fn handle_init_local_db(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let container_name = request
        .args
        .get("container_name")
        .and_then(Value::as_str)
        .map_or("shitty-swarm-manager-db", |value| value)
        .to_string();
    let port = request
        .args
        .get("port")
        .and_then(Value::as_u64)
        .map_or(5437, |value| value) as u16;
    let user = request
        .args
        .get("user")
        .and_then(Value::as_str)
        .map_or("shitty_swarm_manager", |value| value)
        .to_string();
    let database = request
        .args
        .get("database")
        .and_then(Value::as_str)
        .map_or("shitty_swarm_manager_db", |value| value)
        .to_string();
    let schema = request
        .args
        .get("schema")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .map(|value| value.display().to_string());
    let seed_agents = request
        .args
        .get("seed_agents")
        .and_then(Value::as_u64)
        .map_or(12, |value| value) as u32;

    if dry_flag(request) {
        return Ok(dry_run_success(
            request,
            vec![
                json!({"step": 1, "action": "docker_start_or_run", "target": container_name.clone()}),
                json!({"step": 2, "action": "init_db", "target": schema.clone().unwrap_or_else(|| EMBEDDED_COORDINATOR_SCHEMA_REF.to_string())}),
            ],
            "swarm state",
        ));
    }

    let port_mapping = format!("{port}:5432");
    let start_result = Command::new("docker")
        .args(["start", container_name.as_str()])
        .output()
        .await;

    let container_started = start_result
        .as_ref()
        .is_ok_and(|output| output.status.success());

    if !container_started {
        let run_result = Command::new("docker")
            .args([
                "run",
                "-d",
                "--name",
                container_name.as_str(),
                "-p",
                port_mapping.as_str(),
                "-e",
                format!("POSTGRES_USER={user}").as_str(),
                "-e",
                "POSTGRES_HOST_AUTH_METHOD=trust",
                "-e",
                format!("POSTGRES_DB={database}").as_str(),
                "postgres:16",
            ])
            .output()
            .await;

        if let Err(e) = run_result.as_ref() {
            return Err(Box::new(
                ProtocolEnvelope::error(
                    request.rid.clone(),
                    code::INTERNAL.to_string(),
                    format!("Failed to run docker container: {e}"),
                )
                .with_fix("Ensure docker is running and container name is available".to_string()),
            ));
        }

        if let Ok(output) = &run_result {
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                return Err(Box::new(
                    ProtocolEnvelope::error(
                        request.rid.clone(),
                        code::INTERNAL.to_string(),
                        format!("Docker run failed: {stderr}"),
                    )
                    .with_fix(
                        "Check docker logs, ensure port is available and container name is unique"
                            .to_string(),
                    ),
                ));
            }
        }
    }

    let mut retry_count = 0;
    let max_retries = 10;
    let mut last_error = String::new();
    while retry_count < max_retries {
        let ready_check = Command::new("docker")
            .args(["exec", container_name.as_str(), "pg_isready", "-U", &user])
            .output()
            .await;

        match ready_check {
            Ok(check) if check.status.success() => break,
            Ok(check) => {
                last_error = String::from_utf8_lossy(&check.stderr).trim().to_string();
            }
            Err(e) => {
                last_error = e.to_string();
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        retry_count += 1;
    }

    if retry_count >= max_retries {
        return Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INTERNAL.to_string(),
                format!("Database container not ready after {max_retries}s: {last_error}"),
            )
            .with_fix(
                "Check docker logs for the container, verify postgres is starting correctly"
                    .to_string(),
            ),
        ));
    }

    let url = format!("postgresql://{user}@localhost:{port}/{database}");

    let bootstrap_request = ProtocolRequest {
        cmd: "bootstrap".to_string(),
        rid: request.rid.clone(),
        dry: Some(false),
        args: Map::new(),
    };
    let _ = handle_bootstrap(&bootstrap_request).await?;

    let mut init_args = Map::from_iter(vec![
        ("url".to_string(), Value::String(url.clone())),
        ("seed_agents".to_string(), Value::from(seed_agents)),
    ]);
    if let Some(schema_value) = schema {
        init_args.insert("schema".to_string(), Value::String(schema_value));
    }

    let init_request = ProtocolRequest {
        cmd: "init-db".to_string(),
        rid: request.rid.clone(),
        dry: Some(false),
        args: init_args,
    };
    let _ = handle_init_db(&init_request).await?;

    Ok(CommandSuccess {
        data: json!({
            "container": container_name,
            "database_url": mask_database_url(&url),
            "seed_agents": seed_agents
        }),
        next: "swarm state".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

fn to_protocol_failure(error: SwarmError, rid: Option<String>) -> Box<ProtocolEnvelope> {
    super::super::helpers::to_protocol_failure(error, rid)
}

async fn current_repo_root() -> std::result::Result<PathBuf, Box<ProtocolEnvelope>> {
    Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .await
        .map_err(SwarmError::IoError)
        .map_err(|e| to_protocol_failure(e, None))
        .and_then(|output| {
            if output.status.success() {
                Ok(PathBuf::from(
                    String::from_utf8_lossy(&output.stdout).trim().to_string(),
                ))
            } else {
                Err(Box::new(
                    ProtocolEnvelope::error(
                        None,
                        code::INVALID.to_string(),
                        "Not in git repository".to_string(),
                    )
                    .with_fix("Run bootstrap from repository root".to_string()),
                ))
            }
        })
}
