//! Stage execution implementations using functional Rust patterns.
//!
//! This module provides zero-panic, zero-unwrap implementations for each
//! pipeline stage, replacing shell commands with proper Rust code.

use crate::gate_cache::GateExecutionCache;
use crate::skill_execution::store_skill_artifacts;
use crate::types::Stage;
use crate::{AgentId, BeadId, SwarmDb};

mod contract_stage;
mod gate_stage;
mod implement_stage;
mod output_mapping;

#[cfg(test)]
mod tests_gate_stage;
#[cfg(test)]
mod tests_gate_stage_execution;
#[cfg(test)]
mod tests_gate_stage_red_queen;
#[cfg(test)]
mod tests_implement;
#[cfg(test)]
mod tests_implement_helpers;
#[cfg(test)]
mod tests_output_and_gate;

use contract_stage::execute_rust_contract_stage;
use gate_stage::{execute_qa_stage, execute_red_queen_stage};
use implement_stage::execute_implement_stage;
use output_mapping::{error_output, output_to_stage_result, success_output};

/// Execute a stage and return the result.
///
/// This is the main entry point for stage execution, replacing shell commands
/// with proper Rust implementations.
pub async fn execute_stage_rust(
    db: &SwarmDb,
    stage: Stage,
    bead_id: &BeadId,
    agent_id: &AgentId,
    stage_history_id: i64,
    cache: Option<&GateExecutionCache>,
) -> crate::types::StageResult {
    if stage == Stage::Done {
        return crate::types::StageResult::Passed;
    }

    let stage_output = match stage {
        Stage::RustContract => Ok(execute_rust_contract_stage(bead_id, agent_id)),
        Stage::Implement => execute_implement_stage(bead_id, agent_id, db).await,
        Stage::QaEnforcer => execute_qa_stage(bead_id, agent_id, db, cache).await,
        Stage::RedQueen => execute_red_queen_stage(bead_id, agent_id, db, cache).await,
        Stage::Done => Ok(success_output(
            "Done stage does not produce artifacts".to_string(),
        )),
    };

    match stage_output {
        Ok(output) => {
            let result = output_to_stage_result(&output);
            if let Err(err) = store_skill_artifacts(db, stage_history_id, stage, &output).await {
                return crate::types::StageResult::Error(format!(
                    "Failed to store stage artifacts: {err}"
                ));
            }
            result
        }
        Err(err) => {
            let output = error_output(err.to_string());
            if let Err(store_err) =
                store_skill_artifacts(db, stage_history_id, stage, &output).await
            {
                return crate::types::StageResult::Error(format!(
                    "Stage execution failed ({err}) and artifact storage failed ({store_err})"
                ));
            }
            crate::types::StageResult::Error(err.to_string())
        }
    }
}
