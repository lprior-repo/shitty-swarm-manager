-- Basic schema for swarm coordinator
-- This is a placeholder schema for testing purposes

CREATE TABLE IF NOT EXISTS repositories (
    repo_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    path TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS agents (
    agent_id TEXT PRIMARY KEY,
    repo_id TEXT NOT NULL REFERENCES repositories(repo_id),
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS beads (
    bead_id TEXT PRIMARY KEY,
    repo_id TEXT NOT NULL REFERENCES repositories(repo_id),
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stage_history (
    id SERIAL PRIMARY KEY,
    repo_id TEXT NOT NULL REFERENCES repositories(repo_id),
    bead_id TEXT NOT NULL REFERENCES beads(bead_id),
    stage TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    result TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);