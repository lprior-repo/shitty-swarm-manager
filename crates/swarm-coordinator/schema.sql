-- Swarm coordinator schema for high-concurrency agents.

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS bead_backlog (
    bead_id TEXT PRIMARY KEY,
    priority TEXT NOT NULL DEFAULT 'p0',
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'in_progress', 'completed', 'blocked')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bead_claims (
    bead_id TEXT PRIMARY KEY,
    claimed_by INTEGER NOT NULL CHECK (claimed_by >= 1),
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status TEXT NOT NULL DEFAULT 'in_progress' CHECK (status IN ('in_progress', 'completed', 'blocked'))
);

ALTER TABLE bead_claims ALTER COLUMN claimed_by TYPE INTEGER;
ALTER TABLE bead_claims DROP CONSTRAINT IF EXISTS bead_claims_claimed_by_check;
ALTER TABLE bead_claims ADD CONSTRAINT bead_claims_claimed_by_check CHECK (claimed_by >= 1);

CREATE TABLE IF NOT EXISTS agent_state (
    agent_id INTEGER PRIMARY KEY CHECK (agent_id >= 1),
    bead_id TEXT REFERENCES bead_claims(bead_id),
    current_stage TEXT CHECK (current_stage IN ('rust-contract', 'implement', 'qa-enforcer', 'red-queen', 'done')),
    stage_started_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'idle' CHECK (status IN ('idle', 'working', 'waiting', 'error', 'done')),
    last_update TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    implementation_attempt INTEGER NOT NULL DEFAULT 0 CHECK (implementation_attempt >= 0),
    feedback TEXT
);

ALTER TABLE agent_state ALTER COLUMN agent_id TYPE INTEGER;
ALTER TABLE agent_state DROP CONSTRAINT IF EXISTS agent_state_agent_id_check;
ALTER TABLE agent_state ADD CONSTRAINT agent_state_agent_id_check CHECK (agent_id >= 1);

CREATE TABLE IF NOT EXISTS stage_history (
    id BIGSERIAL PRIMARY KEY,
    agent_id INTEGER NOT NULL CHECK (agent_id >= 1),
    bead_id TEXT NOT NULL,
    stage TEXT NOT NULL CHECK (stage IN ('rust-contract', 'implement', 'qa-enforcer', 'red-queen')),
    attempt_number INTEGER NOT NULL CHECK (attempt_number >= 1),
    status TEXT NOT NULL CHECK (status IN ('started', 'passed', 'failed', 'error')),
    result TEXT,
    feedback TEXT,
    transcript TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    duration_ms INTEGER CHECK (duration_ms IS NULL OR duration_ms >= 0)
);

ALTER TABLE stage_history ALTER COLUMN agent_id TYPE INTEGER;
ALTER TABLE stage_history DROP CONSTRAINT IF EXISTS stage_history_agent_id_check;
ALTER TABLE stage_history ADD CONSTRAINT stage_history_agent_id_check CHECK (agent_id >= 1);

CREATE TABLE IF NOT EXISTS stage_artifacts (
    id BIGSERIAL PRIMARY KEY,
    stage_history_id BIGINT NOT NULL REFERENCES stage_history(id) ON DELETE CASCADE,
    artifact_type TEXT NOT NULL CHECK (artifact_type IN (
        'contract_document',
        'requirements',
        'system_context',
        'invariants',
        'data_flow',
        'implementation_plan',
        'acceptance_criteria',
        'error_handling',
        'test_scenarios',
        'validation_gates',
        'success_metrics',
        'implementation_code',
        'modified_files',
        'implementation_notes',
        'test_output',
        'test_results',
        'coverage_report',
        'validation_report',
        'failure_details',
        'adversarial_report',
        'regression_report',
        'quality_gate_report',
        'stage_log',
        'skill_invocation',
        'error_message',
        'feedback'
    )),
    content TEXT NOT NULL,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    content_hash TEXT
);

CREATE TABLE IF NOT EXISTS agent_messages (
    id BIGSERIAL PRIMARY KEY,
    from_repo_id TEXT NOT NULL,
    from_agent_id INTEGER NOT NULL CHECK (from_agent_id >= 1),
    to_repo_id TEXT,
    to_agent_id INTEGER CHECK (to_agent_id IS NULL OR to_agent_id >= 1),
    bead_id TEXT REFERENCES bead_claims(bead_id),
    message_type TEXT NOT NULL CHECK (message_type IN (
        'contract_ready',
        'implementation_ready',
        'qa_complete',
        'qa_failed',
        'red_queen_failed',
        'implementation_retry',
        'artifact_available',
        'stage_complete',
        'stage_failed',
        'blocking_issue',
        'coordination'
    )),
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    read_at TIMESTAMPTZ,
    read BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS agent_run_logs (
    id BIGSERIAL PRIMARY KEY,
    agent_id INTEGER NOT NULL CHECK (agent_id >= 1),
    bead_id TEXT,
    stage TEXT,
    log_content TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE agent_run_logs ALTER COLUMN agent_id TYPE INTEGER;
ALTER TABLE agent_run_logs DROP CONSTRAINT IF EXISTS agent_run_logs_agent_id_check;
ALTER TABLE agent_run_logs ADD CONSTRAINT agent_run_logs_agent_id_check CHECK (agent_id >= 1);

CREATE TABLE IF NOT EXISTS swarm_config (
    id BOOLEAN PRIMARY KEY DEFAULT TRUE,
    max_agents INTEGER NOT NULL DEFAULT 12,
    max_implementation_attempts INTEGER NOT NULL DEFAULT 3,
    claim_label TEXT NOT NULL DEFAULT 'p0',
    swarm_started_at TIMESTAMPTZ,
    swarm_status TEXT NOT NULL DEFAULT 'initializing' CHECK (swarm_status IN ('initializing', 'running', 'paused', 'complete', 'error')),
    CHECK (id)
);

INSERT INTO swarm_config (id)
VALUES (TRUE)
ON CONFLICT (id) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_bead_backlog_claim ON bead_backlog(status, priority, created_at);
CREATE INDEX IF NOT EXISTS idx_bead_claims_status ON bead_claims(status, claimed_at);
CREATE INDEX IF NOT EXISTS idx_agent_state_status ON agent_state(status, last_update DESC);
CREATE INDEX IF NOT EXISTS idx_stage_history_lookup ON stage_history(bead_id, stage, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_stage_history_failed ON stage_history(status, completed_at DESC);
CREATE INDEX IF NOT EXISTS idx_stage_artifacts_history ON stage_artifacts(stage_history_id);
CREATE INDEX IF NOT EXISTS idx_stage_artifacts_type ON stage_artifacts(artifact_type);
CREATE INDEX IF NOT EXISTS idx_stage_artifacts_hash ON stage_artifacts(content_hash);
CREATE INDEX IF NOT EXISTS idx_agent_messages_to ON agent_messages(to_repo_id, to_agent_id, read);
CREATE INDEX IF NOT EXISTS idx_agent_messages_from ON agent_messages(from_repo_id, from_agent_id);
CREATE INDEX IF NOT EXISTS idx_agent_messages_bead ON agent_messages(bead_id);
CREATE INDEX IF NOT EXISTS idx_agent_messages_unread ON agent_messages(to_repo_id, to_agent_id) WHERE read = FALSE;

CREATE OR REPLACE FUNCTION set_agent_last_update()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_update = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_agent_last_update ON agent_state;
CREATE TRIGGER trg_agent_last_update
BEFORE UPDATE ON agent_state
FOR EACH ROW
EXECUTE FUNCTION set_agent_last_update();

CREATE OR REPLACE FUNCTION claim_next_p0_bead(p_agent_id INTEGER)
RETURNS TEXT AS $$
DECLARE
    v_bead_id TEXT;
BEGIN
    SELECT bead_id INTO v_bead_id
    FROM bead_backlog
    WHERE status = 'pending' AND priority = 'p0'
    ORDER BY created_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1;

    IF v_bead_id IS NULL THEN
        RETURN NULL;
    END IF;

    UPDATE bead_backlog
    SET status = 'in_progress'
    WHERE bead_id = v_bead_id;

    INSERT INTO bead_claims (bead_id, claimed_by, status)
    VALUES (v_bead_id, p_agent_id, 'in_progress')
    ON CONFLICT (bead_id) DO NOTHING;

    UPDATE agent_state
    SET bead_id = v_bead_id,
        current_stage = 'rust-contract',
        stage_started_at = NOW(),
        status = 'working'
    WHERE agent_id = p_agent_id;

    RETURN v_bead_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION store_stage_artifact(
    p_stage_history_id BIGINT,
    p_artifact_type TEXT,
    p_content TEXT,
    p_metadata JSONB DEFAULT NULL
) RETURNS BIGINT AS $$
DECLARE
    v_content_hash TEXT;
    v_existing_id BIGINT;
    v_new_id BIGINT;
BEGIN
    v_content_hash := encode(digest(p_content, 'sha256'), 'hex');

    SELECT id INTO v_existing_id
    FROM stage_artifacts
    WHERE stage_history_id = p_stage_history_id
      AND artifact_type = p_artifact_type
      AND content_hash = v_content_hash
    LIMIT 1;

    IF v_existing_id IS NOT NULL THEN
        RETURN v_existing_id;
    END IF;

    INSERT INTO stage_artifacts (stage_history_id, artifact_type, content, metadata, content_hash)
    VALUES (p_stage_history_id, p_artifact_type, p_content, p_metadata, v_content_hash)
    RETURNING id INTO v_new_id;

    RETURN v_new_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION send_agent_message(
    p_from_repo_id TEXT,
    p_from_agent_id INTEGER,
    p_to_repo_id TEXT DEFAULT NULL,
    p_to_agent_id INTEGER DEFAULT NULL,
    p_bead_id TEXT DEFAULT NULL,
    p_message_type TEXT DEFAULT 'coordination',
    p_subject TEXT DEFAULT '',
    p_body TEXT DEFAULT '',
    p_metadata JSONB DEFAULT NULL
) RETURNS BIGINT AS $$
DECLARE
    v_message_id BIGINT;
BEGIN
    INSERT INTO agent_messages (
        from_repo_id,
        from_agent_id,
        to_repo_id,
        to_agent_id,
        bead_id,
        message_type,
        subject,
        body,
        metadata
    )
    VALUES (
        p_from_repo_id,
        p_from_agent_id,
        p_to_repo_id,
        p_to_agent_id,
        p_bead_id,
        p_message_type,
        p_subject,
        p_body,
        p_metadata
    )
    RETURNING id INTO v_message_id;

    RETURN v_message_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_unread_messages(
    p_repo_id TEXT,
    p_agent_id INTEGER,
    p_bead_id TEXT DEFAULT NULL
) RETURNS TABLE (
    id BIGINT,
    from_repo_id TEXT,
    from_agent_id INTEGER,
    to_repo_id TEXT,
    to_agent_id INTEGER,
    bead_id TEXT,
    message_type TEXT,
    subject TEXT,
    body TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ,
    read_at TIMESTAMPTZ,
    read BOOLEAN
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        am.id,
        am.from_repo_id,
        am.from_agent_id,
        am.to_repo_id,
        am.to_agent_id,
        am.bead_id,
        am.message_type,
        am.subject,
        am.body,
        am.metadata,
        am.created_at,
        am.read_at,
        am.read
    FROM agent_messages am
    WHERE am.to_repo_id = p_repo_id
      AND am.to_agent_id = p_agent_id
      AND am.read = FALSE
      AND (p_bead_id IS NULL OR am.bead_id = p_bead_id)
    ORDER BY am.created_at ASC;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION mark_messages_read(
    p_repo_id TEXT,
    p_agent_id INTEGER,
    p_message_ids BIGINT[]
) RETURNS VOID AS $$
BEGIN
    UPDATE agent_messages
    SET read = TRUE,
        read_at = NOW()
    WHERE id = ANY(p_message_ids)
      AND to_repo_id = p_repo_id
      AND to_agent_id = p_agent_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE VIEW v_active_agents AS
SELECT
    a.agent_id,
    a.bead_id,
    a.current_stage,
    a.status,
    a.implementation_attempt,
    a.feedback,
    a.stage_started_at,
    a.last_update
FROM agent_state a
WHERE a.status IN ('working', 'waiting', 'error');

CREATE OR REPLACE VIEW v_swarm_progress AS
SELECT
    COUNT(*) FILTER (WHERE status = 'done') AS done_agents,
    COUNT(*) FILTER (WHERE status = 'working') AS working_agents,
    COUNT(*) FILTER (WHERE status = 'waiting') AS waiting_agents,
    COUNT(*) FILTER (WHERE status = 'error') AS error_agents,
    COUNT(*) FILTER (WHERE status = 'idle') AS idle_agents,
    COUNT(*) AS total_agents,
    (SELECT COUNT(*) FROM bead_claims WHERE status = 'completed') AS completed_beads,
    (SELECT COUNT(*) FROM bead_claims WHERE status = 'in_progress') AS in_progress_beads,
    (SELECT COUNT(*) FROM bead_claims WHERE status = 'blocked') AS blocked_beads
FROM agent_state;

CREATE OR REPLACE VIEW v_feedback_required AS
SELECT DISTINCT ON (bead_id, stage)
    bead_id,
    agent_id,
    stage,
    attempt_number,
    feedback,
    completed_at
FROM stage_history
WHERE status IN ('failed', 'error')
ORDER BY bead_id, stage, completed_at DESC;

CREATE OR REPLACE VIEW v_bead_artifacts AS
SELECT
    sh.bead_id,
    sh.stage,
    sh.attempt_number,
    sa.artifact_type,
    sa.content,
    sa.metadata,
    sa.created_at
FROM stage_artifacts sa
JOIN stage_history sh ON sa.stage_history_id = sh.id
ORDER BY sh.started_at, sa.artifact_type;

CREATE OR REPLACE VIEW v_contract_artifacts AS
SELECT
    sh.bead_id,
    sa.artifact_type,
    sa.content,
    sa.created_at
FROM stage_artifacts sa
JOIN stage_history sh ON sa.stage_history_id = sh.id
WHERE sh.stage = 'rust-contract'
  AND sa.artifact_type IN ('contract_document', 'requirements', 'implementation_plan', 'acceptance_criteria');

CREATE OR REPLACE VIEW v_unread_messages AS
SELECT
    am.id,
    am.from_repo_id,
    am.from_agent_id,
    am.to_repo_id,
    am.to_agent_id,
    am.bead_id,
    am.message_type,
    am.subject,
    am.body,
    am.metadata,
    am.created_at,
    am.read_at,
    am.read
FROM agent_messages am
WHERE am.read = FALSE
ORDER BY am.created_at ASC;

CREATE OR REPLACE VIEW v_available_agents AS
SELECT
    a.agent_id,
    a.status,
    a.implementation_attempt,
    c.max_implementation_attempts,
    c.max_agents
FROM agent_state a
CROSS JOIN swarm_config c
WHERE a.status = 'idle'
   OR (a.status = 'waiting' AND a.implementation_attempt < c.max_implementation_attempts);

CREATE OR REPLACE VIEW v_resume_context AS
SELECT
    a.agent_id,
    a.bead_id,
    a.current_stage,
    a.implementation_attempt,
    a.feedback,
    a.status,
    a.last_update
FROM agent_state a;

COMMIT;
