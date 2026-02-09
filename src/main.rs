use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing::{info, warn, error};

use swarm::{SwarmDb, RepoId, AgentId, Result};

#[derive(Parser)]
#[command(name = "swarm")]
#[command(about = "Shitty Swarm Manager - Simple PostgreSQL-based agent coordination")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Path to config file (default: .swarm/config.toml)
    #[arg(short, long, global = true)]
    config: Option<PathBuf>,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize swarm in current repository
    Init,

    /// Register agents for this repository
    Register {
        /// Number of agents to register
        #[arg(default_value = "12")]
        count: u32,
    },

    /// Run a single agent (claims and processes a bead)
    Agent {
        /// Agent ID number
        #[arg(short, long)]
        id: u32,
    },

    /// Show swarm status
    Status {
        /// Show all repos, not just current
        #[arg(short, long)]
        all: bool,
    },

    /// List active agents
    Ps {
        /// Show all repos
        #[arg(short, long)]
        all: bool,
    },

    /// Start monitoring dashboard
    Dashboard {
        /// Refresh interval in milliseconds
        #[arg(short, long, default_value = "1000")]
        refresh: u64,
    },

    /// Release a stuck agent
    Release {
        /// Agent ID to release
        #[arg(short, long)]
        agent_id: u32,
    },

    /// Initialize database schema
    InitDb {
        /// Database URL
        #[arg(short, long, default_value = "postgresql://swarm:swarm@localhost:5432/swarm_db")]
        url: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Init => {
            println!("🐝 Initializing Shitty Swarm Manager...");
            println!("   Run './init.sh' in your repository root");
            Ok(())
        }

        Commands::Register { count } => {
            let config = load_config(cli.config).await?;
            let db = SwarmDb::new(&config.database_url).await?;
            let repo_id = RepoId::from_current_dir()
                .ok_or_else(|| swarm::SwarmError::ConfigError("Not in a git repo".to_string()))?;

            // Register repo if not exists
            let repo_name = std::env::current_dir()
                .ok()
                .and_then(|p| p.file_name().map(|n| n.to_string_lossy().to_string()))
                .unwrap_or_else(|| "unknown".to_string());
            
            let repo_path = std::env::current_dir()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|_| ".".to_string());

            db.register_repo(&repo_id, &repo_name, &repo_path).await?;

            // Register agents
            for i in 1..=count {
                let agent_id = AgentId::new(repo_id.clone(), i);
                db.register_agent(&agent_id).await?;
            }

            info!("Registered {} agents for repo: {}", count, repo_id);
            println!("✅ Registered {} agents for {}", count, repo_id);
            
            Ok(())
        }

        Commands::Agent { id } => {
            let config = load_config(cli.config).await?;
            let db = SwarmDb::new(&config.database_url).await?;
            let repo_id = RepoId::from_current_dir()
                .ok_or_else(|| swarm::SwarmError::ConfigError("Not in a git repo".to_string()))?;
            
            let agent_id = AgentId::new(repo_id.clone(), id);
            
            info!("Starting agent {}", agent_id);
            run_agent(&db, &agent_id).await?;
            
            Ok(())
        }

        Commands::Status { all } => {
            let config = load_config(cli.config).await?;
            let db = SwarmDb::new(&config.database_url).await?;

            if all {
                let repos = db.list_repos().await?;
                println!("\n📊 Swarm Status (All Repos)\n");
                for (repo_id, name) in repos {
                    match db.get_progress(&repo_id).await {
                        Ok(progress) => {
                            println!("  {} ({}) ", name, repo_id);
                            println!("    Working: {} | Idle: {} | Done: {} | Errors: {}",
                                progress.working, progress.idle, progress.completed, progress.errors);
                        }
                        Err(e) => {
                            warn!("Failed to get progress for {}: {}", repo_id, e);
                        }
                    }
                }
            } else {
                let repo_id = RepoId::from_current_dir()
                    .ok_or_else(|| swarm::SwarmError::ConfigError("Not in a git repo".to_string()))?;
                
                let progress = db.get_progress(&repo_id).await?;
                println!("\n📊 Swarm Status for {}\n", repo_id);
                println!("  Total Agents:    {}", progress.total_agents);
                println!("  Working:         {}", progress.working);
                println!("  Idle:            {}", progress.idle);
                println!("  Waiting:         {}", progress.waiting);
                println!("  Done:            {}", progress.completed);
                println!("  Errors:          {}", progress.errors);
            }
            
            Ok(())
        }

        Commands::Ps { all } => {
            let config = load_config(cli.config).await?;
            let db = SwarmDb::new(&config.database_url).await?;

            let agents = db.get_all_active_agents().await?;
            
            if agents.is_empty() {
                println!("\n🦗 No active agents\n");
            } else {
                println!("\n🐝 Active Agents\n");
                println!("{:<20} {:<8} {:<20} {:<10}", "REPO", "AGENT", "BEAD", "STATUS");
                println!("{}", "-".repeat(60));
                
                for (repo_id, agent_id, bead_id, status) in agents {
                    if all || repo_id == RepoId::from_current_dir().unwrap_or_else(|| RepoId::new("")) {
                        let bead = bead_id.unwrap_or_else(|| "-".to_string());
                        println!("{:<20} {:<8} {:<20} {:<10}", 
                            repo_id.value().chars().take(20).collect::<String>(),
                            agent_id,
                            bead.chars().take(20).collect::<String>(),
                            status
                        );
                    }
                }
            }
            
            Ok(())
        }

        Commands::Dashboard { refresh } => {
            println!("\n📊 Dashboard (refresh: {}ms)", refresh);
            println!("   Run: watch -n 1 'swarm status --all'");
            println!("   Or:  swarm ps --all\n");
            Ok(())
        }

        Commands::Release { agent_id } => {
            let config = load_config(cli.config).await?;
            let db = SwarmDb::new(&config.database_url).await?;
            let repo_id = RepoId::from_current_dir()
                .ok_or_else(|| swarm::SwarmError::ConfigError("Not in a git repo".to_string()))?;
            
            let agent = AgentId::new(repo_id.clone(), agent_id);
            
            // TODO: Implement release logic
            println!("🔄 Releasing agent {}...", agent);
            
            Ok(())
        }

        Commands::InitDb { url } => {
            println!("🗄️  Initializing database schema...");
            println!("   URL: {}", url);
            
            let db = SwarmDb::new(&url).await?;
            
            // Schema should be loaded manually with:
            // psql -h localhost -U swarm -d swarm_db -f schema.sql
            println!("   Run: psql -h localhost -U swarm -d swarm_db -f schema.sql");
            
            Ok(())
        }
    }
}

#[derive(Debug)]
struct Config {
    database_url: String,
    max_agents: u32,
}

async fn load_config(path: Option<PathBuf>) -> Result<Config> {
    let config_path = path.unwrap_or_else(|| PathBuf::from(".swarm/config.toml"));
    
    if !config_path.exists() {
        return Ok(Config {
            database_url: "postgresql://swarm:swarm@localhost:5432/swarm_db".to_string(),
            max_agents: 12,
        });
    }

    let content = tokio::fs::read_to_string(&config_path).await
        .map_err(|e| swarm::SwarmError::ConfigError(format!("Failed to read config: {}", e)))?;

    // Simple TOML parsing - extract database URL
    let mut database_url = None;
    let mut max_agents = 12u32;

    for line in content.lines() {
        let line = line.trim();
        if line.starts_with("host") {
            // Parse host = "..."
        } else if line.starts_with("max_agents") {
            if let Some(val) = line.split('=').nth(1) {
                max_agents = val.trim().parse().unwrap_or(12);
            }
        }
    }

    // Build connection string from config
    let db_url = database_url.unwrap_or_else(|| {
        "postgresql://swarm:swarm@localhost:5432/swarm_db".to_string()
    });

    Ok(Config {
        database_url: db_url,
        max_agents,
    })
}

async fn run_agent(db: &SwarmDb, agent_id: &AgentId) -> Result<()> {
    use swarm::{Stage, StageResult};
    use std::time::Instant;

    loop {
        // Get current state
        let state = db.get_agent_state(agent_id).await?;
        
        match state {
            Some(state) => {
                match state.status {
                    swarm::AgentStatus::Idle => {
                        // Try to claim a bead
                        info!("Agent {} looking for work...", agent_id);
                        // TODO: Query SQLite for available beads, then claim
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    }
                    swarm::AgentStatus::Working => {
                        // Continue current stage
                        if let Some(stage) = state.current_stage {
                            if let Some(bead_id) = state.bead_id {
                                info!("Agent {} working on {} (stage: {})", 
                                    agent_id, bead_id, stage);
                                
                                let start = Instant::now();
                                db.record_stage_started(agent_id, &bead_id, stage, 
                                    state.implementation_attempt).await?;
                                
                                // TODO: Execute actual stage work here
                                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                                
                                let duration = start.elapsed().as_millis() as u64;
                                let result = StageResult::Passed; // Placeholder
                                
                                db.record_stage_complete(agent_id, &bead_id, stage,
                                    state.implementation_attempt, result, duration).await?;
                            }
                        }
                    }
                    swarm::AgentStatus::Done => {
                        info!("Agent {} completed work", agent_id);
                        break;
                    }
                    _ => {
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }
                }
            }
            None => {
                error!("Agent {} not registered", agent_id);
                break;
            }
        }
    }

    Ok(())
}
