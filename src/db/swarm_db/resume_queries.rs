use crate::db::SwarmDb;
use crate::error::{Result, SwarmError};
use crate::types::{
    AgentStatus, BeadId, DeepResumeContextContract, RepoId, ResumeContextProjection, Stage,
};

impl SwarmDb {
    /// # Errors
    /// Returns an error if the database operation fails.
    pub async fn get_resume_context_projections(
        &self,
        repo_id: &RepoId,
    ) -> Result<Vec<ResumeContextProjection>> {
        let rows = sqlx::query_as::<_, (i32, String, String, Option<String>, i32, Option<String>)>(
            "SELECT agent_id, bead_id, status, current_stage, implementation_attempt, feedback
             FROM agent_state
             WHERE repo_id = $1 AND bead_id IS NOT NULL
             ORDER BY agent_id ASC",
        )
        .bind(repo_id.value())
        .fetch_all(self.pool())
        .await
        .map_err(|error| {
            SwarmError::DatabaseError(format!(
                "Failed to load resume context projections: {error}"
            ))
        })?;

        rows.into_iter()
            .map(
                |(agent_id, bead_id, status, current_stage, implementation_attempt, feedback)| {
                    let status = AgentStatus::try_from(status.as_str())
                        .map_err(SwarmError::DatabaseError)?;
                    let current_stage = current_stage
                        .map(|value| Stage::try_from(value.as_str()))
                        .transpose()
                        .map_err(SwarmError::DatabaseError)?;
                    Ok(ResumeContextProjection {
                        agent_id: agent_id.max(0).cast_unsigned(),
                        bead_id: BeadId::new(bead_id),
                        status,
                        current_stage,
                        implementation_attempt: implementation_attempt.max(0).cast_unsigned(),
                        feedback,
                        attempts: Vec::new(),
                        artifacts: Vec::new(),
                    })
                },
            )
            .collect()
    }

    /// # Errors
    /// Returns an error if the database operation fails.
    pub async fn get_deep_resume_contexts(
        &self,
        repo_id: &RepoId,
    ) -> Result<Vec<DeepResumeContextContract>> {
        self.get_resume_context_projections(repo_id)
            .await
            .map(|contexts| {
                contexts
                    .into_iter()
                    .map(|projection| DeepResumeContextContract {
                        agent_id: projection.agent_id,
                        bead_id: projection.bead_id.value().to_string(),
                        status: projection.status.as_str().to_string(),
                        current_stage: projection
                            .current_stage
                            .map(|stage| stage.as_str().to_string()),
                        implementation_attempt: projection.implementation_attempt,
                        feedback: projection.feedback,
                        attempts: projection
                            .attempts
                            .into_iter()
                            .map(|attempt| crate::ResumeStageAttemptContract {
                                stage: attempt.stage.as_str().to_string(),
                                attempt_number: attempt.attempt_number,
                                status: attempt.status,
                                feedback: attempt.feedback,
                                started_at: attempt.started_at,
                                completed_at: attempt.completed_at,
                            })
                            .collect::<Vec<_>>(),
                        diagnostics: None,
                        artifacts: Vec::new(),
                    })
                    .collect::<Vec<_>>()
            })
    }
}
