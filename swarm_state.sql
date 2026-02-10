-- Swarm State Database for Parallel Bead Processing
-- SQLite schema for tracking agent progress, bead claims, and stage history

-- Enable foreign keys
PRAGMA foreign_keys = ON;

-- ============================================================
-- Table: bead_claims
-- Tracks which beads are claimed by which agents
-- ============================================================
CREATE TABLE IF NOT EXISTS bead_claims (
    bead_id TEXT PRIMARY KEY,
    claimed_by INTEGER NOT NULL,  -- Agent ID
    claimed_at TEXT NOT NULL,     -- ISO 8601 timestamp
    status TEXT NOT NULL DEFAULT 'in_progress',  -- in_progress, completed, blocked
    UNIQUE(bead_id)
);

CREATE INDEX IF NOT EXISTS idx_bead_claims_agent ON bead_claims(claimed_by);
CREATE INDEX IF NOT EXISTS idx_bead_claims_status ON bead_claims(status);

-- ============================================================
-- Table: agent_state
-- Current state of each agent
-- ============================================================
CREATE TABLE IF NOT EXISTS agent_state (
    agent_id INTEGER PRIMARY KEY,  -- Unique agent ID
    bead_id TEXT,                  -- Currently assigned bead
    current_stage TEXT,            -- rust-contract, implement, qa, red-queen, done
    stage_started_at TEXT,         -- When current stage began
    status TEXT NOT NULL DEFAULT 'idle',  -- idle, working, waiting, error, done
    last_update TEXT NOT NULL,     -- Last state change timestamp
    implementation_attempt INTEGER DEFAULT 0,  -- Counter for implement retries
    feedback TEXT,                 -- Error feedback from QA/Red Queen
    FOREIGN KEY (bead_id) REFERENCES bead_claims(bead_id)
);

CREATE INDEX IF NOT EXISTS idx_agent_state_stage ON agent_state(current_stage);
CREATE INDEX IF NOT EXISTS idx_agent_state_status ON agent_state(status);

-- ============================================================
-- Table: stage_history
-- Complete audit log of all stage executions
-- ============================================================
CREATE TABLE IF NOT EXISTS stage_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id INTEGER NOT NULL,
    bead_id TEXT NOT NULL,
    stage TEXT NOT NULL,           -- rust-contract, implement, qa, red-queen
    attempt_number INTEGER NOT NULL,  -- For tracking retries
    status TEXT NOT NULL,          -- started, passed, failed, error
    result TEXT,                   -- Success details or error message
    feedback TEXT,                 -- Detailed feedback for next stage
    started_at TEXT NOT NULL,
    completed_at TEXT,
    duration_ms INTEGER,           -- Stage duration in milliseconds
    FOREIGN KEY (agent_id) REFERENCES agent_state(agent_id),
    FOREIGN KEY (bead_id) REFERENCES bead_claims(bead_id)
);

CREATE INDEX IF NOT EXISTS idx_stage_history_agent ON stage_history(agent_id);
CREATE INDEX IF NOT EXISTS idx_stage_history_bead ON stage_history(bead_id);
CREATE INDEX IF NOT EXISTS idx_stage_history_stage ON stage_history(stage);
CREATE INDEX IF NOT EXISTS idx_stage_history_time ON stage_history(started_at DESC);

-- ============================================================
-- Table: pipeline_config
-- Configuration for swarm execution
-- ============================================================
CREATE TABLE IF NOT EXISTS pipeline_config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Insert default configuration
INSERT OR REPLACE INTO pipeline_config (key, value) VALUES
    ('max_agents', '10'),
    ('max_implementation_attempts', '3'),
    ('claim_label', 'p0'),
    ('swarm_started_at', datetime('now')),
    ('swarm_status', 'initializing');

-- ============================================================
-- Useful Queries
-- ============================================================

-- Get next unclaimed P0 bead
-- SELECT id FROM beads WHERE status = 'pending' AND priority = 'p0'
--   AND id NOT IN (SELECT bead_id FROM bead_claims) LIMIT 1;

-- Get all agent statuses
-- SELECT agent_id, bead_id, current_stage, status, implementation_attempt
--   FROM agent_state ORDER BY agent_id;

-- Get failure feedback for an agent
-- SELECT feedback FROM agent_state WHERE agent_id = ?;

-- Get stage history for a bead
-- SELECT stage, attempt_number, status, feedback, started_at, completed_at
--   FROM stage_history WHERE bead_id = ? ORDER BY started_at;

-- Check if swarm is complete
-- SELECT COUNT(*) FROM agent_state WHERE status != 'done';

-- ============================================================
-- Views for common queries
-- ============================================================

-- View: Active agents with their current beads
CREATE VIEW IF NOT EXISTS v_active_agents AS
SELECT
    a.agent_id,
    a.bead_id,
    a.current_stage,
    a.status,
    a.implementation_attempt,
    b.claimed_at,
    datetime('now') - datetime(b.claimed_at) as time_elapsed
FROM agent_state a
JOIN bead_claims b ON a.bead_id = b.bead_id
WHERE a.status IN ('working', 'waiting', 'error')
ORDER BY a.agent_id;

-- View: Swarm progress summary
CREATE VIEW IF NOT EXISTS v_swarm_progress AS
SELECT
    COUNT(CASE WHEN status = 'done' THEN 1 END) as completed,
    COUNT(CASE WHEN status = 'working' THEN 1 END) as working,
    COUNT(CASE WHEN status = 'waiting' THEN 1 END) as waiting,
    COUNT(CASE WHEN status = 'error' THEN 1 END) as errors,
    COUNT(CASE WHEN status = 'idle' THEN 1 END) as idle,
    COUNT(*) as total_agents
FROM agent_state;

-- View: Failed stages requiring feedback
CREATE VIEW IF NOT EXISTS v_feedback_required AS
SELECT
    agent_id,
    bead_id,
    stage,
    attempt_number,
    feedback,
    completed_at
FROM stage_history
WHERE status = 'failed'
  AND id IN (
    SELECT MAX(id)
    FROM stage_history
    GROUP BY agent_id, bead_id
  )
ORDER BY completed_at DESC;
