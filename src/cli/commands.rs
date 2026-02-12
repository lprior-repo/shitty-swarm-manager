#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use crate::protocol_runtime::ProtocolRequest;
use serde_json::{json, Map};

#[derive(Debug, Clone)]
pub enum CliCommand {
    Doctor,
    Help,
    Status,
    Next {
        dry: Option<bool>,
    },
    ClaimNext {
        dry: Option<bool>,
    },
    Assign {
        bead_id: String,
        agent_id: u32,
        dry: Option<bool>,
    },
    RunOnce {
        id: Option<u32>,
        dry: Option<bool>,
    },
    Qa {
        target: Option<String>,
        id: Option<u32>,
        dry: Option<bool>,
    },
    Resume,
    ResumeContext {
        bead_id: Option<String>,
    },
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

#[allow(clippy::too_many_lines)]
pub fn cli_command_to_request(cmd: CliCommand) -> String {
    let (cmd_name, dry, args) = match cmd {
        CliCommand::Doctor => ("doctor".to_string(), None, Map::new()),
        CliCommand::Help => ("?".to_string(), None, Map::new()),
        CliCommand::Status => ("status".to_string(), None, Map::new()),
        CliCommand::Next { dry } => ("next".to_string(), dry, Map::new()),
        CliCommand::ClaimNext { dry } => ("claim-next".to_string(), dry, Map::new()),
        CliCommand::Assign {
            bead_id,
            agent_id,
            dry,
        } => {
            let mut args = Map::new();
            args.insert("bead_id".to_string(), json!(bead_id));
            args.insert("agent_id".to_string(), json!(agent_id));
            ("assign".to_string(), dry, args)
        }
        CliCommand::RunOnce { id, dry } => {
            let mut args = Map::new();
            if let Some(agent_id) = id {
                args.insert("id".to_string(), json!(agent_id));
            }
            ("run-once".to_string(), dry, args)
        }
        CliCommand::Qa { target, id, dry } => {
            let mut args = Map::new();
            if let Some(value) = target {
                args.insert("target".to_string(), json!(value));
            }
            if let Some(agent_id) = id {
                args.insert("id".to_string(), json!(agent_id));
            }
            ("qa".to_string(), dry, args)
        }
        CliCommand::Resume => ("resume".to_string(), None, Map::new()),
        CliCommand::Artifacts {
            bead_id,
            artifact_type,
        } => {
            let mut args = Map::new();
            args.insert("bead_id".to_string(), json!(bead_id));
            if let Some(kind) = artifact_type {
                args.insert("artifact_type".to_string(), json!(kind));
            }
            ("artifacts".to_string(), None, args)
        }
        CliCommand::ResumeContext { bead_id } => {
            let mut args = Map::new();
            if let Some(id) = bead_id {
                args.insert("bead_id".to_string(), json!(id));
            }
            ("resume-context".to_string(), None, args)
        }
        CliCommand::Agent { id, dry } => {
            let mut args = Map::new();
            args.insert("id".to_string(), json!(id));
            ("agent".to_string(), dry, args)
        }
        CliCommand::Init {
            dry,
            database_url,
            schema,
            seed_agents,
        } => {
            let mut args = Map::new();
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
            let mut args = Map::new();
            if let Some(cnt) = count {
                args.insert("count".to_string(), json!(cnt));
            }
            ("register".to_string(), dry, args)
        }
        CliCommand::Release { agent_id, dry } => {
            let mut args = Map::new();
            args.insert("agent_id".to_string(), json!(agent_id));
            ("release".to_string(), dry, args)
        }
        CliCommand::Monitor { view, watch_ms } => {
            let mut args = Map::new();
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
            let mut args = Map::new();
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
            let mut args = Map::new();
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
        CliCommand::Bootstrap { dry } => ("bootstrap".to_string(), dry, Map::new()),
        CliCommand::SpawnPrompts {
            template,
            out_dir,
            count,
            dry,
        } => {
            let mut args = Map::new();
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
            let mut args = Map::new();
            args.insert("id".to_string(), json!(id));
            if let Some(s) = skill {
                args.insert("skill".to_string(), json!(s));
            }
            ("prompt".to_string(), None, args)
        }
        CliCommand::Smoke { id, dry } => {
            let mut args = Map::new();
            args.insert("id".to_string(), json!(id));
            ("smoke".to_string(), dry, args)
        }
        CliCommand::Batch { dry } => ("batch".to_string(), dry, Map::new()),
        CliCommand::State => ("state".to_string(), None, Map::new()),
        CliCommand::History { limit } => {
            let mut args = Map::new();
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
            let mut args = Map::new();
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
            let mut args = Map::new();
            args.insert("resource".to_string(), json!(resource));
            args.insert("agent".to_string(), json!(agent));
            ("unlock".to_string(), dry, args)
        }
        CliCommand::Agents => ("agents".to_string(), None, Map::new()),
        CliCommand::Broadcast { msg, from, dry } => {
            let mut args = Map::new();
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
            let mut args = Map::new();
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
        CliCommand::Json(cmd) => (cmd, None, Map::new()),
    };

    let request = ProtocolRequest {
        cmd: cmd_name,
        rid: None,
        dry,
        args,
    };
    serde_json::to_string(&request).unwrap_or_default()
}
