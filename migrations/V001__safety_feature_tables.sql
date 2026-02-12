-- V001__safety_feature_tables.sql
-- Migration for swarm safety features: budgets, file claims, health, symbols

-- ============================================================================
-- BUDGET TRACKING TABLES
-- ============================================================================

-- Budget limits and usage for beads and swarm
CREATE TABLE IF NOT EXISTS budgets (
    id BIGSERIAL PRIMARY KEY,
    bead_id BIGINT,  -- NULL for swarm-level budget
    max_input_tokens BIGINT NOT NULL DEFAULT 50000,
    max_output_tokens BIGINT NOT NULL DEFAULT 20000,
    max_total_tokens BIGINT NOT NULL DEFAULT 60000,
    used_input_tokens BIGINT NOT NULL DEFAULT 0,
    used_output_tokens BIGINT NOT NULL DEFAULT 0,
    exceeded BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT budgets_bead_id_unique UNIQUE (bead_id)
);

-- Granular token usage records
CREATE TABLE IF NOT EXISTS token_usage_log (
    id BIGSERIAL PRIMARY KEY,
    budget_id BIGINT NOT NULL REFERENCES budgets(id) ON DELETE CASCADE,
    agent_id TEXT NOT NULL,
    input_tokens BIGINT NOT NULL,
    output_tokens BIGINT NOT NULL,
    description TEXT,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_token_usage_budget_id ON token_usage_log(budget_id);
CREATE INDEX idx_token_usage_agent_id ON token_usage_log(agent_id);
CREATE INDEX idx_token_usage_recorded_at ON token_usage_log(recorded_at);

-- ============================================================================
-- FILE CLAIMS TABLES
-- ============================================================================

-- File scope claims for conflict detection
CREATE TABLE IF NOT EXISTS file_claims (
    id BIGSERIAL PRIMARY KEY,
    agent_id TEXT NOT NULL,
    bead_id BIGINT NOT NULL,
    file_path TEXT NOT NULL,
    modification_type TEXT NOT NULL CHECK (modification_type IN ('create', 'modify', 'delete', 'rename')),
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT file_claims_unique UNIQUE (file_path, bead_id) WHERE is_active = TRUE
);

CREATE INDEX idx_file_claims_agent_id ON file_claims(agent_id);
CREATE INDEX idx_file_claims_bead_id ON file_claims(bead_id);
CREATE INDEX idx_file_claims_file_path ON file_claims(file_path);
CREATE INDEX idx_file_claims_active ON file_claims(is_active) WHERE is_active = TRUE;

-- File manifests (stored as artifacts)
-- Reuses existing artifacts table with artifact_type = 'file_manifest'

-- ============================================================================
-- CIRCUIT BREAKER & HEALTH TABLES
-- ============================================================================

-- Circuit breaker state
CREATE TABLE IF NOT EXISTS circuit_breakers (
    id BIGSERIAL PRIMARY KEY,
    scope TEXT NOT NULL UNIQUE,  -- 'global' or swarm name
    state TEXT NOT NULL DEFAULT 'closed' CHECK (state IN ('closed', 'open', 'half_open')),
    failure_count INTEGER NOT NULL DEFAULT 0,
    success_count INTEGER NOT NULL DEFAULT 0,
    failure_threshold INTEGER NOT NULL DEFAULT 5,
    success_threshold INTEGER NOT NULL DEFAULT 3,
    reset_timeout_secs BIGINT NOT NULL DEFAULT 60,
    window_secs BIGINT NOT NULL DEFAULT 300,
    opened_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Health metrics per agent
CREATE TABLE IF NOT EXISTS agent_health (
    id BIGSERIAL PRIMARY KEY,
    agent_id TEXT NOT NULL UNIQUE,
    total_operations BIGINT NOT NULL DEFAULT 0,
    successful_operations BIGINT NOT NULL DEFAULT 0,
    failed_operations BIGINT NOT NULL DEFAULT 0,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    last_success_at TIMESTAMPTZ,
    last_failure_at TIMESTAMPTZ,
    last_progress_at TIMESTAMPTZ,
    current_bead_id BIGINT,
    current_stage TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    health_status TEXT NOT NULL DEFAULT 'healthy' CHECK (health_status IN ('healthy', 'degraded', 'stuck', 'retry_loop')),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_agent_health_status ON agent_health(health_status);
CREATE INDEX idx_agent_health_last_progress ON agent_health(last_progress_at);

-- Health events log
CREATE TABLE IF NOT EXISTS health_events (
    id BIGSERIAL PRIMARY KEY,
    agent_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_data JSONB,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_health_events_agent_id ON health_events(agent_id);
CREATE INDEX idx_health_events_type ON health_events(event_type);
CREATE INDEX idx_health_events_occurred_at ON health_events(occurred_at);

-- ============================================================================
-- SYMBOL TRACKING TABLES
-- ============================================================================

-- Symbol registry for semantic drift detection
CREATE TABLE IF NOT EXISTS tracked_symbols (
    id BIGSERIAL PRIMARY KEY,
    bead_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('function', 'struct', 'enum', 'trait', 'type_alias', 'constant', 'module')),
    module_path TEXT NOT NULL,
    contract_signature TEXT,
    contract_hash TEXT,
    implementation_signature TEXT,
    implementation_hash TEXT,
    has_drift BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT tracked_symbols_unique UNIQUE (bead_id, name, module_path)
);

CREATE INDEX idx_tracked_symbols_bead_id ON tracked_symbols(bead_id);
CREATE INDEX idx_tracked_symbols_name ON tracked_symbols(name);
CREATE INDEX idx_tracked_symbols_drift ON tracked_symbols(has_drift) WHERE has_drift = TRUE;

-- ============================================================================
-- CONTROL TABLES (Kill Switch)
-- ============================================================================

-- Swarm control state (halt/pause)
CREATE TABLE IF NOT EXISTS swarm_control (
    id BIGSERIAL PRIMARY KEY,
    scope TEXT NOT NULL UNIQUE,  -- 'global' or swarm name
    is_halted BOOLEAN NOT NULL DEFAULT FALSE,
    is_paused BOOLEAN NOT NULL DEFAULT FALSE,
    halt_reason TEXT,
    paused_reason TEXT,
    halted_at TIMESTAMPTZ,
    paused_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Insert default control records
INSERT INTO swarm_control (scope, is_halted, is_paused) VALUES ('global', FALSE, FALSE)
ON CONFLICT (scope) DO NOTHING;

INSERT INTO circuit_breakers (scope, state) VALUES ('global', 'closed')
ON CONFLICT (scope) DO NOTHING;

-- ============================================================================
-- TRUNK FRESHNESS TABLES
-- ============================================================================

-- Trunk freshness tracking
CREATE TABLE IF NOT EXISTS trunk_freshness (
    id BIGSERIAL PRIMARY KEY,
    agent_id TEXT NOT NULL,
    bead_id BIGINT NOT NULL,
    trunk_commit_sha TEXT NOT NULL,
    trunk_commit_at TIMESTAMPTZ NOT NULL,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_stale BOOLEAN NOT NULL DEFAULT FALSE,
    commits_behind INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT trunk_freshness_unique UNIQUE (bead_id)
);

CREATE INDEX idx_trunk_freshness_stale ON trunk_freshness(is_stale) WHERE is_stale = TRUE;
