// Input Contracts - What each command accepts
// Output Contracts - What each command returns

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// ============================================================================
// DOCTOR COMMAND
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct DoctorInput {
    pub json: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct DoctorOutput {
    pub v: &'static str,
    pub h: bool,
    pub p: i64,
    pub f: i64,
    pub c: Vec<CheckResult>,
}

#[derive(Debug, Serialize)]
pub struct CheckResult {
    pub n: String, // command name
    pub ok: bool,  // is available
}

// ============================================================================
// HELP COMMAND
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct HelpInput {
    pub short: Option<bool>,
    pub s: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct HelpOutput {
    pub n: &'static str, // "swarm"
    pub v: &'static str, // version
    pub cmds: Vec<CommandHelp>,
}

#[derive(Debug, Serialize)]
pub struct CommandHelp(pub &'static str, pub &'static str); // (cmd, desc)

// ============================================================================
// STATUS COMMAND
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct StatusInput {
    // No args - reads from current state
}

#[derive(Debug, Serialize)]
pub struct StatusOutput {
    pub working: u32,
    pub idle: u32,
    pub waiting: u32,
    pub done: u32,
    pub errors: u32,
    pub total: u32,
}

// ============================================================================
// AGENT COMMAND
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct AgentInput {
    pub id: u32,
    pub dry: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct AgentOutput {
    pub agent_id: u32,
    pub status: &'static str,
}

// ============================================================================
// INIT COMMAND (new)
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct InitInput {
    pub dry: Option<bool>,
    pub database_url: Option<String>,
    pub schema: Option<String>,
    pub seed_agents: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct InitOutput {
    pub initialized: bool,
    pub steps: Vec<InitStep>,
}

#[derive(Debug, Serialize)]
pub struct InitStep {
    pub step: i64,
    pub action: String,
    pub status: String,
}

// ============================================================================
// REGISTER COMMAND
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct RegisterInput {
    pub count: Option<u32>,
    pub dry: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct RegisterOutput {
    pub repo: String,
    pub count: u32,
}

// ============================================================================
// RELEASE COMMAND
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct ReleaseInput {
    pub agent_id: u32,
    pub dry: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct ReleaseOutput {
    pub agent_id: u32,
    pub released_bead: Option<String>,
}

// ============================================================================
// MONITOR COMMAND
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct MonitorInput {
    pub view: Option<String>,
    pub watch_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct MonitorOutput {
    pub view: String,
    pub rows: Vec<Value>,
}

#[derive(Debug, Serialize)]
pub struct Value {
    #[serde(flatten)]
    pub data: BTreeMap<String, serde_json::Value>,
}

// ============================================================================
// INIT-DB COMMAND
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct InitDbInput {
    pub url: Option<String>,
    pub schema: Option<String>,
    pub seed_agents: Option<u32>,
    pub dry: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct InitDbOutput {
    pub database_url: String,
    pub schema: String,
    pub seed_agents: u32,
}

// ============================================================================
// INIT-LOCAL-DB COMMAND
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct InitLocalDbInput {
    pub container_name: Option<String>,
    pub port: Option<u16>,
    pub user: Option<String>,
    pub database: Option<String>,
    pub schema: Option<String>,
    pub seed_agents: Option<u32>,
    pub dry: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct InitLocalDbOutput {
    pub container: String,
    pub database_url: String,
    pub seed_agents: u32,
}

// ============================================================================
// BOOTSTRAP COMMAND
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct BootstrapInput {
    pub dry: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct BootstrapOutput {
    pub repo_root: String,
    pub swarm_dir: String,
    pub actions_taken: Vec<String>,
    pub idempotent: bool,
}

// ============================================================================
// SPAWN-PROMPTS COMMAND
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct SpawnPromptsInput {
    pub template: Option<String>,
    pub out_dir: Option<String>,
    pub count: Option<u32>,
    pub dry: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct SpawnPromptsOutput {
    pub count: u32,
    pub out_dir: String,
    pub template: String,
}

// ============================================================================
// PROMPT COMMAND
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct PromptInput {
    pub id: u32,
    pub skill: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct PromptOutput {
    pub agent_id: u32,
    pub prompt: String,
}

// ============================================================================
// SMOKE COMMAND
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct SmokeInput {
    pub id: u32,
    pub dry: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct SmokeOutput {
    pub agent_id: u32,
    pub status: &'static str,
}

// ============================================================================
// BATCH COMMAND
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct BatchInput {
    pub ops: Vec<serde_json::Value>, // Array of ProtocolRequest
    pub dry: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct BatchOutput {
    pub items: Vec<BatchItem>,
    pub summary: BatchSummary,
}

#[derive(Debug, Serialize)]
pub struct BatchItem {
    pub seq: i64,
    pub cmd: String,
    pub ok: bool,
    pub ms: u64,
}

#[derive(Debug, Serialize)]
pub struct BatchSummary {
    pub total: i64,
    pub pass: i64,
    pub fail: i64,
}

// ============================================================================
// STATE COMMAND
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct StateInput {}

#[derive(Debug, Serialize)]
pub struct StateOutput {
    pub initialized: bool,
    pub resources: Vec<Value>,
    pub health: Value,
    pub config: Value,
    pub warnings: Vec<String>,
}

// ============================================================================
// HISTORY COMMAND
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct HistoryInput {
    pub limit: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct HistoryOutput {
    pub actions: Vec<HistoryAction>,
    pub total: i64,
    pub aggregates: HistoryAggregates,
}

#[derive(Debug, Serialize)]
pub struct HistoryAction {
    pub seq: i64,
    pub t: i64,
    pub cmd: String,
    pub args: serde_json::Value,
    pub ok: bool,
    pub ms: u64,
    pub error_code: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct HistoryAggregates {
    pub success_rate: f64,
    pub avg_duration_ms: f64,
    pub common_sequences: Vec<serde_json::Value>,
    pub error_frequency: BTreeMap<String, i64>,
}

// ============================================================================
// LOCK COMMAND
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct LockInput {
    pub resource: String,
    pub agent: String,
    pub ttl_ms: i64,
    pub dry: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct LockOutput {
    pub locked: bool,
    pub until: i64,
}

// ============================================================================
// UNLOCK COMMAND
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct UnlockInput {
    pub resource: String,
    pub agent: String,
    pub dry: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct UnlockOutput {
    pub unlocked: bool,
}

// ============================================================================
// AGENTS COMMAND
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct AgentsInput {}

#[derive(Debug, Serialize)]
pub struct AgentsOutput {
    pub agents: Vec<AgentInfo>,
}

#[derive(Debug, Serialize)]
pub struct AgentInfo {
    pub id: String,
    pub resource: String,
    pub since: i64,
}

// ============================================================================
// BROADCAST COMMAND
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct BroadcastInput {
    pub msg: String,
    pub from: String,
    pub dry: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct BroadcastOutput {
    pub delivered_to: usize,
}

// ============================================================================
// LOAD-PROFILE COMMAND
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct LoadProfileInput {
    pub agents: Option<u32>,
    pub rounds: Option<u32>,
    pub timeout_ms: Option<u64>,
    pub dry: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct LoadProfileOutput {
    pub agents: u32,
    pub rounds: u32,
    pub timeouts: u64,
    pub errors: u64,
    pub successful_claims: u64,
    pub empty_claims: u64,
}

// ============================================================================
// CLI ERROR TYPES
// ============================================================================

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error("Unknown command: {command}")]
    UnknownCommand {
        command: String,
        suggestions: Vec<String>,
    },

    #[error("Missing required argument: {arg_name}")]
    MissingRequiredArg { arg_name: String, usage: String },

    #[error("Invalid value for {arg_name}: {value}")]
    InvalidArgValue {
        arg_name: String,
        value: String,
        expected: String,
    },

    #[error("Invalid type for {arg_name}: got {got}, expected {expected}")]
    InvalidArgType {
        arg_name: String,
        got: String,
        expected: String,
    },
}
