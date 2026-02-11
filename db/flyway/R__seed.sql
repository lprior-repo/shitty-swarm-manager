INSERT INTO repos (repo_id, name, path)
VALUES
    ('local', 'local', '.'),
    ('sandbox', 'sandbox', '.')
ON CONFLICT (repo_id) DO UPDATE
SET
    name = EXCLUDED.name,
    path = EXCLUDED.path;

INSERT INTO agent_state (repo_id, agent_id, status, implementation_attempt)
SELECT
    'local',
    agent_id,
    'idle',
    0
FROM generate_series(1, 12) AS agent_id
ON CONFLICT (repo_id, agent_id) DO UPDATE
SET
    status = EXCLUDED.status,
    bead_id = NULL,
    current_stage = NULL,
    stage_started_at = NULL,
    implementation_attempt = 0,
    feedback = NULL,
    last_update = NOW();

INSERT INTO agent_state (repo_id, agent_id, status, implementation_attempt)
SELECT
    'sandbox',
    agent_id,
    'idle',
    0
FROM generate_series(1, 12) AS agent_id
ON CONFLICT (repo_id, agent_id) DO UPDATE
SET
    status = EXCLUDED.status,
    bead_id = NULL,
    current_stage = NULL,
    stage_started_at = NULL,
    implementation_attempt = 0,
    feedback = NULL,
    last_update = NOW();
