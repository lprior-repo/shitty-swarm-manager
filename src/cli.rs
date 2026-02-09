use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "swarm")]
#[command(about = "Shitty Swarm Manager - Simple PostgreSQL-based agent coordination")]
#[command(version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    #[arg(short, long, global = true)]
    pub config: Option<PathBuf>,

    #[arg(long, global = true)]
    pub database_url: Option<String>,

    #[arg(long, global = true)]
    pub database_url_pass: Option<String>,

    #[arg(long, global = true, value_enum, default_value = "json")]
    pub output: OutputFormat,

    #[arg(long, global = true, default_value_t = false)]
    pub claude_mode: bool,
}

#[derive(Subcommand)]
pub enum Commands {
    Init,
    Register {
        #[arg(default_value = "12")]
        count: u32,
    },
    Agent {
        #[arg(short, long)]
        id: u32,
    },
    Status {
        #[arg(short, long)]
        all: bool,
    },
    Ps {
        #[arg(short, long)]
        all: bool,
    },
    Dashboard {
        #[arg(short, long, default_value = "1000")]
        refresh: u64,
    },
    Release {
        #[arg(short, long)]
        agent_id: u32,
    },
    InitDb {
        #[arg(short, long)]
        url: Option<String>,
        #[arg(long, default_value = "crates/swarm-coordinator/schema.sql")]
        schema: PathBuf,
        #[arg(long, default_value = "12")]
        seed_agents: u32,
    },
    Monitor {
        #[arg(long, default_value = "active")]
        view: MonitorView,
        #[arg(long, default_value = "0")]
        watch_ms: u64,
    },
    SpawnPrompts {
        #[arg(long, default_value = ".agents/agent_prompt.md")]
        template: PathBuf,
        #[arg(long, default_value = ".agents/generated")]
        out_dir: PathBuf,
        #[arg(long, default_value = "12")]
        count: u32,
    },
    Smoke {
        #[arg(short, long, default_value = "1")]
        id: u32,
    },

    LoadProfile {
        #[arg(long, default_value = "90")]
        agents: u32,

        #[arg(long, default_value = "5")]
        rounds: u32,

        #[arg(long, default_value = "1500")]
        timeout_ms: u64,
    },
}

#[derive(Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum OutputFormat {
    Text,
    Json,
}

#[derive(Clone, Debug, ValueEnum)]
pub enum MonitorView {
    Active,
    Progress,
    Failures,
    Messages,
}
