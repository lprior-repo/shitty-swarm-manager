use chrono::Utc;
use deadpool_postgres::{Client, Pool, PoolConfig, Runtime, Manager, ManagerConfig, RecyclingMethod};
use tokio_postgres::{Config as PgConfig, NoTls};
use tracing::{debug, error, info, warn};

use crate::types::*;
use crate::error::{Result, SwarmError};

/// Database connection pool and operations
#[derive(Clone)]
pub struct SwarmDb {
    pool: Pool,
}

impl SwarmDb {
    /// Create new database connection pool
    pub async fn new(database_url: &str) -> Result<Self> {
        let config = database_url.parse::<PgConfig>()
            .map_err(|e| SwarmError::ConfigError(format!("Invalid database URL: {}", e)))?;

        let manager_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };

        let manager = Manager::from_config(config, NoTls, manager_config);

        let pool_config = PoolConfig::new(10);

        let pool = Pool::builder(manager)
            .config(pool_config)
            .runtime(Runtime::Tokio1)
            .build()
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to create pool: {}", e)))?;

        // Test connection
        let _ = pool.get().await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to connect: {}", e)))?;

        info!("Connected to PostgreSQL swarm database");

        Ok(Self { pool })
    }

    /// Get a client from the pool
    async fn client(&self) -> Result<Client> {
        self.pool.get().await
            .map_err(|e| SwarmError::DatabaseError(format!("Failed to get client: {}", e)))
    }

    /// Register a new repository
    pub async fn register_repo(&self, repo_id: &RepoId, name: &str, path: &str) -> Result<()> {
        let client = self.client().await?;

        let git_remote = std::process::Command::new("git")
            .args(["remote", "get-url", "origin"])
            .current_dir(path)
            .output()
            .ok()
            .and_then(|o| {
                if o.status.success() {
                    String::from_utf8(o.stdout).ok()
                } else {
                    None
                }
            });

        client.execute(
            "INSERT INTO repos (repo_id, name, path, git_remote) 
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (repo_id) DO UPDATE SET 
                last_active_at = NOW()",
            &[&repo_id.value(), &name, &path, &git_remote],
        ).await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to register repo: {}", e)))?;

        // Initialize config if not exists
        client.execute(
            "INSERT INTO swarm_config (repo_id) VALUES ($1) ON CONFLICT DO NOTHING",
            &[&repo_id.value()],
        ).await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to init config: {}", e)))?;

        info!("Registered repo: {}", repo_id);
        Ok(())
    }

    /// Register an agent for a repo
    pub async fn register_agent(&self, agent_id: &AgentId) -> Result<bool> {
        let client = self.client().await?;

        let rows = client.execute(
            "INSERT INTO agent_state (repo_id, agent_id, status) 
             VALUES ($1, $2, 'idle')
             ON CONFLICT (repo_id, agent_id) DO NOTHING",
            &[&agent_id.repo_id().value(), &(agent_id.number() as i32)],
        ).await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to register agent: {}", e)))?;

        Ok(rows > 0)
    }

    /// Claim a bead for an agent
    pub async fn claim_bead(&self, agent_id: &AgentId, bead_id: &BeadId) -> Result<bool> {
        let client = self.client().await?;

        // Use the claim_bead function
        let row = client.query_one(
            "SELECT claim_bead($1, $2, $3)",
            &[&agent_id.repo_id().value(), &(agent_id.number() as i32), &bead_id.value()],
        ).await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to claim bead: {}", e)))?;

        let success: bool = row.get(0);

        if success {
            info!(
                "Agent {} claimed bead {}",
                agent_id, bead_id
            );
        }

        Ok(success)
    }

    /// Get current agent state
    pub async fn get_agent_state(&self, agent_id: &AgentId) -> Result<Option<AgentState>> {
        let client = self.client().await?;

        let row = client.query_opt(
            "SELECT bead_id, current_stage, stage_started_at, status, 
                    last_update, implementation_attempt, feedback
             FROM agent_state 
             WHERE repo_id = $1 AND agent_id = $2",
            &[&agent_id.repo_id().value(), &(agent_id.number() as i32)],
        ).await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to get agent state: {}", e)))?;

        match row {
            Some(row) => {
                let bead_id: Option<String> = row.get(0);
                let stage_str: Option<String> = row.get(1);
                let stage_started_at = row.get(2);
                let status_str: String = row.get(3);
                let last_update = row.get(4);
                let implementation_attempt: i32 = row.get(5);
                let feedback: Option<String> = row.get(6);

                let bead_id = bead_id.map(BeadId::new);
                let current_stage = stage_str.and_then(|s| Stage::try_from(s.as_str()).ok());
                let status = AgentStatus::try_from(status_str.as_str())
                    .map_err(|e| SwarmError::DatabaseError(e))?;

                Ok(Some(AgentState {
                    agent_id: agent_id.clone(),
                    bead_id,
                    current_stage,
                    stage_started_at,
                    status,
                    last_update,
                    implementation_attempt: implementation_attempt as u32,
                    feedback,
                }))
            }
            None => Ok(None),
        }
    }

    /// Record stage started
    pub async fn record_stage_started(
        &self,
        agent_id: &AgentId,
        bead_id: &BeadId,
        stage: Stage,
        attempt: u32,
    ) -> Result<()> {
        let client = self.client().await?;

        client.execute(
            "INSERT INTO stage_history 
             (agent_id, repo_id, bead_id, stage, attempt_number, status)
             VALUES ($1, $2, $3, $4, $5, 'started')",
            &[
                &(agent_id.number() as i32),
                &agent_id.repo_id().value(),
                &bead_id.value(),
                &stage.as_str(),
                &(attempt as i32),
            ],
        ).await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to record stage start: {}", e)))?;

        Ok(())
    }

    /// Record stage completed
    pub async fn record_stage_complete(
        &self,
        agent_id: &AgentId,
        bead_id: &BeadId,
        stage: Stage,
        attempt: u32,
        result: StageResult,
        duration_ms: u64,
    ) -> Result<()> {
        let client = self.client().await?;

        client.execute(
            "SELECT record_stage_complete($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            &[
                &agent_id.repo_id().value(),
                &(agent_id.number() as i32),
                &bead_id.value(),
                &stage.as_str(),
                &(attempt as i32),
                &result.as_str(),
                &result.message(),
                &result.message(),
                &(duration_ms as i32),
            ],
        ).await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to record stage complete: {}", e)))?;

        debug!(
            "Agent {} completed stage {} for bead {}: {:?}",
            agent_id, stage, bead_id, result
        );

        Ok(())
    }

    /// Get available agents (idle or failed with retries left)
    pub async fn get_available_agents(&self, repo_id: &RepoId) -> Result<Vec<AvailableAgent>> {
        let client = self.client().await?;

        let rows = client.query(
            "SELECT agent_id, status, implementation_attempt, 
                    max_implementation_attempts, max_agents
             FROM v_available_agents 
             WHERE repo_id = $1",
            &[&repo_id.value()],
        ).await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to get available agents: {}", e)))?;

        let mut agents = Vec::new();
        for row in rows {
            let agent_id: i32 = row.get(0);
            let status_str: String = row.get(1);
            let attempt: i32 = row.get(2);
            let max_attempts: i32 = row.get(3);
            let max_agents: i32 = row.get(4);

            let status = AgentStatus::try_from(status_str.as_str())
                .map_err(|e| SwarmError::DatabaseError(e))?;

            agents.push(AvailableAgent {
                repo_id: repo_id.clone(),
                agent_id: agent_id as u32,
                status,
                implementation_attempt: attempt as u32,
                max_implementation_attempts: max_attempts as u32,
                max_agents: max_agents as u32,
            });
        }

        Ok(agents)
    }

    /// Get progress summary for a repo
    pub async fn get_progress(&self, repo_id: &RepoId) -> Result<ProgressSummary> {
        let client = self.client().await?;

        let row = client.query_one(
            "SELECT completed, working, waiting, errors, idle, total_agents
             FROM v_swarm_progress 
             WHERE repo_id = $1",
            &[&repo_id.value()],
        ).await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to get progress: {}", e)))?;

        Ok(ProgressSummary {
            completed: row.get::<_, i64>(0) as u64,
            working: row.get::<_, i64>(1) as u64,
            waiting: row.get::<_, i64>(2) as u64,
            errors: row.get::<_, i64>(3) as u64,
            idle: row.get::<_, i64>(4) as u64,
            total_agents: row.get::<_, i64>(5) as u64,
        })
    }

    /// Get swarm config
    pub async fn get_config(&self, repo_id: &RepoId) -> Result<SwarmConfig> {
        let client = self.client().await?;

        let row = client.query_one(
            "SELECT max_agents, max_implementation_attempts, claim_label,
                    swarm_started_at, swarm_status
             FROM swarm_config 
             WHERE repo_id = $1",
            &[&repo_id.value()],
        ).await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to get config: {}", e)))?;

        let max_agents: i32 = row.get(0);
        let max_attempts: i32 = row.get(1);
        let claim_label: String = row.get(2);
        let started_at = row.get(3);
        let status_str: String = row.get(4);

        let swarm_status = SwarmStatus::try_from(status_str.as_str())
            .map_err(|e| SwarmError::DatabaseError(e))?;

        Ok(SwarmConfig {
            repo_id: repo_id.clone(),
            max_agents: max_agents as u32,
            max_implementation_attempts: max_attempts as u32,
            claim_label,
            swarm_started_at: started_at,
            swarm_status,
        })
    }

    /// Update swarm status
    pub async fn set_swarm_status(&self, repo_id: &RepoId, status: SwarmStatus) -> Result<()> {
        let client = self.client().await?;

        client.execute(
            "UPDATE swarm_config 
             SET swarm_status = $1 
             WHERE repo_id = $2",
            &[&status.as_str(), &repo_id.value()],
        ).await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to update status: {}", e)))?;

        Ok(())
    }

    /// Start swarm (set status to running and timestamp)
    pub async fn start_swarm(&self, repo_id: &RepoId) -> Result<()> {
        let client = self.client().await?;

        client.execute(
            "UPDATE swarm_config 
             SET swarm_status = 'running', swarm_started_at = NOW()
             WHERE repo_id = $1",
            &[&repo_id.value()],
        ).await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to start swarm: {}", e)))?;

        info!("Started swarm for repo: {}", repo_id);
        Ok(())
    }

    /// List all repos
    pub async fn list_repos(&self) -> Result<Vec<(RepoId, String)>> {
        let client = self.client().await?;

        let rows = client.query(
            "SELECT repo_id, name FROM repos ORDER BY last_active_at DESC",
            &[],
        ).await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to list repos: {}", e)))?;

        let mut repos = Vec::new();
        for row in rows {
            let id: String = row.get(0);
            let name: String = row.get(1);
            repos.push((RepoId::new(id), name));
        }

        Ok(repos)
    }

    /// Get active agents across all repos
    pub async fn get_all_active_agents(&self) -> Result<Vec<(RepoId, u32, Option<String>, String)>> {
        let client = self.client().await?;

        let rows = client.query(
            "SELECT repo_id, agent_id, bead_id, status
             FROM agent_state
             WHERE status IN ('working', 'waiting', 'error')
             ORDER BY last_update DESC",
            &[],
        ).await
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to get active agents: {}", e)))?;

        let mut agents = Vec::new();
        for row in rows {
            let repo_id: String = row.get(0);
            let agent_id: i32 = row.get(1);
            let bead_id: Option<String> = row.get(2);
            let status: String = row.get(3);
            agents.push((RepoId::new(repo_id), agent_id as u32, bead_id, status));
        }

        Ok(agents)
    }
}
