-- Shitty Swarm Manager Schema
-- Simple but powerful PostgreSQL-based agent coordination
-- Supports unlimited agents across multiple repos

-- ============================================================
-- Extensions
-- ============================================================
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- Table: repos
-- Tracks registered repositories
-- ============================================================
CREATE TABLE IF NOT EXISTS repos (
    repo_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    path TEXT NOT NULL,
    git_remote TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_active_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_repos_name ON repos(name);

-- ============================================================
-- Table: bead_claims
-- Tracks which beads are claimed by which agents
-- ============================================================
CREATE TABLE IF NOT EXISTS bead_claims (
    bead_id TEXT PRIMARY KEY,
    repo_id TEXT NOT NULL REFERENCES repos(repo_id),
    claimed_by INTEGER NOT NULL,
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status TEXT NOT NULL DEFAULT 'in_progress' CHECK (status IN ('in_progress', 'completed', 'blocked'))
);

CREATE INDEX IF NOT EXISTS idx_bead_claims_repo ON bead_claims(repo_id);
CREATE INDEX IF NOT EXISTS idx_bead_claims_agent ON bead_claims(repo_id, claimed_by);
CREATE INDEX IF NOT EXISTS idx_bead_claims_status ON bead_claims(status);

-- ============================================================
-- Table: agent_state
-- Current state of each agent (unlimited agents, repo-scoped)
-- ============================================================
CREATE TABLE IF NOT EXISTS agent_state (
    agent_id INTEGER NOT NULL,
    repo_id TEXT NOT NULL REFERENCES repos(repo_id),
    bead_id TEXT REFERENCES bead_claims(bead_id),
    current_stage TEXT CHECK (current_stage IN ('contract', 'implement', 'test', 'qa', 'done')),
    stage_started_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'idle' CHECK (status IN ('idle', 'working', 'waiting', 'error', 'done')),
    last_update TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    implementation_attempt INTEGER NOT NULL DEFAULT 0,
    feedback TEXT,
    PRIMARY KEY (repo_id, agent_id)
);

CREATE INDEX IF NOT EXISTS idx_agent_state_stage ON agent_state(current_stage);
CREATE INDEX IF NOT EXISTS idx_agent_state_status ON agent_state(status);
CREATE INDEX IF NOT EXISTS idx_agent_state_repo ON agent_state(repo_id);
CREATE INDEX IF NOT EXISTS idx_agent_state_update ON agent_state(last_update DESC);

-- ============================================================
-- Table: stage_history
-- Complete audit log of all stage executions
-- ============================================================
CREATE TABLE IF NOT EXISTS stage_history (
    id BIGSERIAL PRIMARY KEY,
    agent_id INTEGER NOT NULL,
    repo_id TEXT NOT NULL REFERENCES repos(repo_id),
    bead_id TEXT NOT NULL,
    stage TEXT NOT NULL CHECK (stage IN ('contract', 'implement', 'test', 'qa')),
    attempt_number INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('started', 'passed', 'failed', 'error')),
    result TEXT,
    feedback TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    duration_ms INTEGER
);

CREATE INDEX IF NOT EXISTS idx_stage_history_agent ON stage_history(repo_id, agent_id);
CREATE INDEX IF NOT EXISTS idx_stage_history_bead ON stage_history(bead_id);
CREATE INDEX IF NOT EXISTS idx_stage_history_stage ON stage_history(stage);
CREATE INDEX IF NOT EXISTS idx_stage_history_time ON stage_history(started_at DESC);

-- ============================================================
-- Table: swarm_config
-- Configuration per repo
-- ============================================================
CREATE TABLE IF NOT EXISTS swarm_config (
    repo_id TEXT PRIMARY KEY REFERENCES repos(repo_id),
    max_agents INTEGER NOT NULL DEFAULT 12,
    max_implementation_attempts INTEGER NOT NULL DEFAULT 3,
    claim_label TEXT NOT NULL DEFAULT 'p0',
    swarm_started_at TIMESTAMPTZ,
    swarm_status TEXT NOT NULL DEFAULT 'initializing' CHECK (swarm_status IN ('initializing', 'running', 'paused', 'complete', 'error'))
);

-- ============================================================
-- Functions: Core Operations
-- ============================================================

-- Claim a bead for an agent (transactional, lock-free)
CREATE OR REPLACE FUNCTION claim_bead(
    p_repo_id TEXT,
    p_agent_id INTEGER,
    p_bead_id TEXT
) RETURNS BOOLEAN AS $$
BEGIN
    -- Check if bead is already claimed
    IF EXISTS (
        SELECT 1 FROM bead_claims 
        WHERE bead_id = p_bead_id AND status = 'in_progress'
    ) THEN
        RETURN FALSE;
    END IF;

    -- Claim the bead
    INSERT INTO bead_claims (bead_id, repo_id, claimed_by, status)
    VALUES (p_bead_id, p_repo_id, p_agent_id, 'in_progress');

    -- Update agent state
    UPDATE agent_state
    SET bead_id = p_bead_id,
        current_stage = 'contract',
        stage_started_at = NOW(),
        status = 'working',
        last_update = NOW()
    WHERE repo_id = p_repo_id AND agent_id = p_agent_id;

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- Record stage completion
CREATE OR REPLACE FUNCTION record_stage_complete(
    p_repo_id TEXT,
    p_agent_id INTEGER,
    p_bead_id TEXT,
    p_stage TEXT,
    p_attempt INTEGER,
    p_status TEXT,
    p_result TEXT DEFAULT NULL,
    p_feedback TEXT DEFAULT NULL,
    p_duration_ms INTEGER DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    -- Update stage history
    UPDATE stage_history
    SET status = p_status,
        result = p_result,
        feedback = p_feedback,
        completed_at = NOW(),
        duration_ms = p_duration_ms
    WHERE repo_id = p_repo_id 
      AND agent_id = p_agent_id 
      AND bead_id = p_bead_id
      AND stage = p_stage
      AND attempt_number = p_attempt;

    -- Update agent state based on result
    IF p_status = 'failed' OR p_status = 'error' THEN
        UPDATE agent_state
        SET status = 'waiting',
            feedback = p_feedback,
            implementation_attempt = implementation_attempt + 1,
            last_update = NOW()
        WHERE repo_id = p_repo_id AND agent_id = p_agent_id;
    ELSIF p_stage = 'qa' AND p_status = 'passed' THEN
        UPDATE agent_state
        SET status = 'done',
            current_stage = 'done',
            last_update = NOW()
        WHERE repo_id = p_repo_id AND agent_id = p_agent_id;
        
        UPDATE bead_claims
        SET status = 'completed'
        WHERE bead_id = p_bead_id;
    ELSE
        -- Move to next stage
        UPDATE agent_state
        SET current_stage = CASE p_stage
            WHEN 'contract' THEN 'implement'
            WHEN 'implement' THEN 'test'
            WHEN 'test' THEN 'qa'
            ELSE 'done'
        END,
        stage_started_at = NOW(),
        last_update = NOW()
        WHERE repo_id = p_repo_id AND agent_id = p_agent_id;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Register a new agent
CREATE OR REPLACE FUNCTION register_agent(
    p_repo_id TEXT,
    p_agent_id INTEGER
) RETURNS BOOLEAN AS $$
BEGIN
    INSERT INTO agent_state (repo_id, agent_id, status)
    VALUES (p_repo_id, p_agent_id, 'idle')
    ON CONFLICT (repo_id, agent_id) DO NOTHING;
    
    RETURN FOUND;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- Views: Monitoring
-- ============================================================

-- Active agents with their current work
CREATE OR REPLACE VIEW v_active_agents AS
SELECT
    a.repo_id,
    r.name as repo_name,
    a.agent_id,
    a.bead_id,
    a.current_stage,
    a.status,
    a.implementation_attempt,
    a.last_update,
    b.claimed_at,
    EXTRACT(EPOCH FROM (NOW() - b.claimed_at)) * 1000 as time_elapsed_ms
FROM agent_state a
JOIN repos r ON a.repo_id = r.repo_id
LEFT JOIN bead_claims b ON a.bead_id = b.bead_id
WHERE a.status IN ('working', 'waiting', 'error');

-- Swarm progress summary per repo
CREATE OR REPLACE VIEW v_swarm_progress AS
SELECT
    repo_id,
    COUNT(*) FILTER (WHERE status = 'done')::BIGINT as completed,
    COUNT(*) FILTER (WHERE status = 'working')::BIGINT as working,
    COUNT(*) FILTER (WHERE status = 'waiting')::BIGINT as waiting,
    COUNT(*) FILTER (WHERE status = 'error')::BIGINT as errors,
    COUNT(*) FILTER (WHERE status = 'idle')::BIGINT as idle,
    COUNT(*)::BIGINT as total_agents
FROM agent_state
GROUP BY repo_id;

-- Latest failed stages requiring feedback
CREATE OR REPLACE VIEW v_feedback_required AS
SELECT DISTINCT ON (repo_id, agent_id, bead_id)
    repo_id,
    agent_id,
    bead_id,
    stage,
    attempt_number,
    feedback,
    completed_at
FROM stage_history
WHERE status = 'failed'
ORDER BY repo_id, agent_id, bead_id, completed_at DESC;

-- Agents needing work (idle or failed with retries left)
CREATE OR REPLACE VIEW v_available_agents AS
SELECT 
    a.repo_id,
    a.agent_id,
    a.status,
    a.implementation_attempt,
    c.max_implementation_attempts,
    c.max_agents
FROM agent_state a
JOIN swarm_config c ON a.repo_id = c.repo_id
WHERE a.status = 'idle'
   OR (a.status = 'waiting' AND a.implementation_attempt < c.max_implementation_attempts);

-- ============================================================
-- Triggers: Auto-update timestamps
-- ============================================================

CREATE OR REPLACE FUNCTION update_last_update()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_update = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_agent_state_update
    BEFORE UPDATE ON agent_state
    FOR EACH ROW
    EXECUTE FUNCTION update_last_update();

CREATE OR REPLACE FUNCTION update_repo_last_active()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE repos SET last_active_at = NOW() WHERE repo_id = NEW.repo_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_repo_active_agent
    AFTER UPDATE ON agent_state
    FOR EACH ROW
    EXECUTE FUNCTION update_repo_last_active();
