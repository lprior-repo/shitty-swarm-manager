use crate::db::mappers::to_u32_i32;
use crate::db::SwarmDb;
use crate::error::{Result, SwarmError};
use crate::types::{
    AgentStatus, ArtifactType, BeadId, DeepResumeContextContract, FailureDiagnostics,
    RepoId, ResumeArtifactDetailContract, ResumeArtifactSummary, ResumeContextProjection,
    ResumeStageAttempt, ResumeStageAttemptContract, Stage,
};
use std::collections::HashMap;

use super::types::{ExecutionEventRow, ResumeArtifactDetailRow, ResumeContextAggregateRow};

impl SwarmDb {
    pub async fn get_resume_context_projections(
        &self,
        repo_id: &RepoId,
    ) -> Result<Vec<ResumeContextProjection>> {
        let artifact_types = resume_artifact_type_names();
        let contexts = sqlx::query_as::<_, ResumeContextAggregateRow>(
            "SELECT
                a.agent_id,
                a.bead_id,
                a.current_stage,
                a.implementation_attempt,
                a.feedback,
                a.status,
                COALESCE(
                    (
                        SELECT json_agg(
                            json_build_object(
                                'stage', sh.stage,
                                'attempt_number', sh.attempt_number,
                                'status', sh.status,
                                'feedback', sh.feedback,
                                'started_at', sh.started_at,
                                'completed_at', sh.completed_at
                            )
                            ORDER BY sh.attempt_number ASC, sh.started_at ASC, sh.id ASC
                        )
                        FROM stage_history sh
                        JOIN bead_claims bc ON bc.bead_id = sh.bead_id
                        WHERE sh.bead_id = a.bead_id AND bc.repo_id = $1
                    ),
                    '[]'::json
                ) AS attempts_json,
                COALESCE(
                    (
                        SELECT json_agg(
                            json_build_object(
                                'artifact_type', latest.artifact_type,
                                'created_at', latest.created_at,
                                'content_hash', latest.content_hash,
                                'byte_length', latest.byte_length
                            )
                            ORDER BY latest.created_at ASC, latest.artifact_type ASC
                        )
                        FROM (
                            SELECT DISTINCT ON (sa.artifact_type)
                                sa.artifact_type,
                                sa.created_at,
                                sa.content_hash,
                                OCTET_LENGTH(sa.content)::BIGINT AS byte_length
                            FROM stage_artifacts sa
                            JOIN stage_history sh ON sh.id = sa.stage_history_id
                            JOIN bead_claims bc ON bc.bead_id = sh.bead_id
                            WHERE sh.bead_id = a.bead_id AND bc.repo_id = $1
                              AND sa.artifact_type = ANY($2::TEXT[])
                            ORDER BY sa.artifact_type, sa.created_at DESC, sa.id DESC
                        ) AS latest
                    ),
                    '[]'::json
                ) AS artifacts_json
             FROM agent_state a
             JOIN bead_claims bc ON bc.bead_id = a.bead_id
             WHERE a.bead_id IS NOT NULL
               AND a.status IN ('working', 'waiting', 'error')
               AND bc.repo_id = $1
             ORDER BY a.bead_id ASC, a.agent_id ASC",
        )
        .bind(repo_id.value())
        .bind(artifact_types)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            SwarmError::DatabaseError(format!("Failed to load resume context rows: {e}"))
        })?;

        let mut projections = Vec::with_capacity(contexts.len());
        for context in contexts {
            let attempts = parse_resume_attempts(context.attempts_json.clone())?;
            let artifacts = parse_resume_artifacts(context.artifacts_json.clone())?;

            let status = AgentStatus::try_from(context.status.as_str())
                .map_err(SwarmError::DatabaseError)?;

            let current_stage = context
                .current_stage
                .as_deref()
                .map(Stage::try_from)
                .transpose()
                .map_err(SwarmError::DatabaseError)?;

            projections.push(ResumeContextProjection {
                agent_id: to_u32_i32(context.agent_id),
                bead_id: BeadId::new(&context.bead_id),
                status,
                current_stage,
                implementation_attempt: to_u32_i32(context.implementation_attempt),
                feedback: context.feedback.clone(),
                attempts,
                artifacts,
            });
        }

        Ok(projections)
    }

    pub async fn get_deep_resume_contexts(
        &self,
        repo_id: &RepoId,
    ) -> Result<Vec<DeepResumeContextContract>> {
        let projections = self.get_resume_context_projections(repo_id).await?;
        if projections.is_empty() {
            return Ok(Vec::new());
        }

        let bead_ids = projections
            .iter()
            .map(|context| context.bead_id.value().to_string())
            .collect::<Vec<_>>();

        let diagnostics_map = self
            .get_latest_diagnostics_for_beads(repo_id, &bead_ids)
            .await?;
        let artifacts_map = self
            .get_latest_artifacts_for_beads(repo_id, &bead_ids)
            .await?;

        let contexts = projections
            .into_iter()
            .map(|projection| {
                let diagnostics = diagnostics_map.get(projection.bead_id.value()).cloned();
                let artifacts = artifacts_map
                    .get(projection.bead_id.value())
                    .cloned()
                    .unwrap_or_default();
                let attempts = projection
                    .attempts
                    .iter()
                    .map(|attempt| ResumeStageAttemptContract {
                        stage: attempt.stage.as_str().to_string(),
                        attempt_number: attempt.attempt_number,
                        status: attempt.status.clone(),
                        feedback: attempt.feedback.clone(),
                        started_at: attempt.started_at,
                        completed_at: attempt.completed_at,
                    })
                    .collect::<Vec<_>>();

                DeepResumeContextContract {
                    agent_id: projection.agent_id,
                    bead_id: projection.bead_id.value().to_string(),
                    status: projection.status.as_str().to_string(),
                    current_stage: projection
                        .current_stage
                        .map(|stage| stage.as_str().to_string()),
                    implementation_attempt: projection.implementation_attempt,
                    feedback: projection.feedback.clone(),
                    attempts,
                    diagnostics,
                    artifacts,
                }
            })
            .collect::<Vec<_>>();

        Ok(contexts)
    }

    async fn get_latest_diagnostics_for_beads(
        &self,
        repo_id: &RepoId,
        bead_ids: &[String],
    ) -> Result<HashMap<String, FailureDiagnostics>> {
        if bead_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let rows = sqlx::query_as::<_, ExecutionEventRow>(
            "SELECT DISTINCT ON (bead_id)
                seq,
                schema_version,
                event_type,
                entity_id,
                bead_id,
                agent_id,
                stage,
                causation_id,
                diagnostics_category,
                diagnostics_retryable,
                diagnostics_next_command,
                diagnostics_detail,
                payload,
                created_at
             FROM execution_events
             WHERE bead_id = ANY($1::TEXT[])
               AND bead_id IN (SELECT bc.bead_id FROM bead_claims bc WHERE bc.repo_id = $2)
               AND diagnostics_category IS NOT NULL
             ORDER BY bead_id, seq DESC",
        )
        .bind(bead_ids)
        .bind(repo_id.value())
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            SwarmError::DatabaseError(format!(
                "Failed to load diagnostics for resume context: {e}"
            ))
        })?;

        let mut map = HashMap::new();
        for row in rows {
            if let Some(bead_id) = row.bead_id.clone() {
                if let Some(diagnostics) = diagnostics_from_row(&row) {
                    map.insert(bead_id, diagnostics);
                }
            }
        }

        Ok(map)
    }

    async fn get_latest_artifacts_for_beads(
        &self,
        repo_id: &RepoId,
        bead_ids: &[String],
    ) -> Result<HashMap<String, Vec<ResumeArtifactDetailContract>>> {
        if bead_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let artifact_types = resume_artifact_type_names();
        let rows = sqlx::query_as::<_, ResumeArtifactDetailRow>(
            "SELECT DISTINCT ON (sh.bead_id, sa.artifact_type)
                sh.bead_id,
                sa.artifact_type,
                sa.content,
                sa.metadata,
                sa.created_at,
                sa.content_hash
             FROM stage_artifacts sa
             JOIN stage_history sh ON sh.id = sa.stage_history_id
             WHERE sh.bead_id = ANY($1::TEXT[])
               AND sh.bead_id IN (SELECT bc.bead_id FROM bead_claims bc WHERE bc.repo_id = $2)
               AND sa.artifact_type = ANY($3::TEXT[])
             ORDER BY sh.bead_id, sa.artifact_type, sa.created_at DESC, sa.id DESC",
        )
        .bind(bead_ids)
        .bind(repo_id.value())
        .bind(artifact_types)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            SwarmError::DatabaseError(format!(
                "Failed to load artifact contents for resume context: {e}"
            ))
        })?;

        let mut map: HashMap<String, Vec<ResumeArtifactDetailContract>> = HashMap::new();
        for row in rows {
            let bead_id = row.bead_id;
            let content = row.content;
            let byte_length = content.len() as u64;
            let detail = ResumeArtifactDetailContract {
                artifact_type: row.artifact_type,
                created_at: row.created_at,
                content,
                metadata: row.metadata,
                content_hash: row.content_hash,
                byte_length,
            };
            map.entry(bead_id).or_default().push(detail);
        }

        Ok(map)
    }
}

pub(crate) fn diagnostics_from_row(row: &ExecutionEventRow) -> Option<FailureDiagnostics> {
    row.diagnostics_category
        .as_ref()
        .zip(row.diagnostics_retryable)
        .zip(row.diagnostics_next_command.as_ref())
        .map(|((category, retryable), next_command)| FailureDiagnostics {
            category: category.clone(),
            retryable,
            next_command: next_command.clone(),
            detail: row.diagnostics_detail.clone(),
        })
}

fn parse_resume_attempts(json: serde_json::Value) -> Result<Vec<ResumeStageAttempt>> {
    use super::types::ResumeStageAttemptJson;
    
    serde_json::from_value::<Vec<ResumeStageAttemptJson>>(json)
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to decode resume attempts: {e}")))
        .and_then(|rows| {
            rows.into_iter()
                .map(|row| {
                    Stage::try_from(row.stage.as_str())
                        .map_err(SwarmError::DatabaseError)
                        .map(|stage| ResumeStageAttempt {
                            stage,
                            attempt_number: to_u32_i32(row.attempt_number),
                            status: row.status,
                            feedback: row.feedback,
                            started_at: row.started_at,
                            completed_at: row.completed_at,
                        })
                })
                .collect::<Result<Vec<_>>>()
        })
}

fn parse_resume_artifacts(json: serde_json::Value) -> Result<Vec<ResumeArtifactSummary>> {
    use super::types::ResumeArtifactSummaryJson;
    
    serde_json::from_value::<Vec<ResumeArtifactSummaryJson>>(json)
        .map_err(|e| SwarmError::DatabaseError(format!("Failed to decode resume artifacts: {e}")))
        .and_then(|rows| {
            rows.into_iter()
                .map(|row| {
                    ArtifactType::try_from(row.artifact_type.as_str())
                        .map_err(SwarmError::DatabaseError)
                        .map(|artifact_type| ResumeArtifactSummary {
                            artifact_type,
                            created_at: row.created_at,
                            content_hash: row.content_hash,
                            byte_length: row.byte_length.max(0).cast_unsigned(),
                        })
                })
                .collect::<Result<Vec<_>>>()
        })
}

#[must_use]
pub(crate) fn resume_artifact_type_names() -> Vec<String> {
    [
        ArtifactType::ContractDocument,
        ArtifactType::ImplementationCode,
        ArtifactType::FailureDetails,
        ArtifactType::ErrorMessage,
        ArtifactType::Feedback,
        ArtifactType::ValidationReport,
        ArtifactType::TestResults,
        ArtifactType::StageLog,
        ArtifactType::RetryPacket,
    ]
    .iter()
    .map(|value| value.as_str().to_string())
    .collect::<Vec<_>>()
}

pub(crate) fn repo_id_from_context() -> RepoId {
    RepoId::from_current_dir().unwrap_or_else(|| RepoId::new("local"))
}

#[cfg(test)]
mod tests {
    use super::{parse_resume_artifacts, parse_resume_attempts, resume_artifact_type_names};
    use crate::types::{ArtifactType, Stage};
    use chrono::Utc;
    use serde_json::json;

    #[test]
    fn parse_resume_attempts_decodes_stored_history() {
        let now = Utc::now();
        let attempts_json = json!([
            {
                "stage": "rust-contract",
                "attempt_number": 1,
                "status": "passed",
                "feedback": null,
                "started_at": now,
                "completed_at": now,
            },
            {
                "stage": "implement",
                "attempt_number": 2,
                "status": "failed",
                "feedback": "missing artifact",
                "started_at": now,
                "completed_at": null,
            }
        ]);

        let attempts = parse_resume_attempts(attempts_json).expect("parse resume attempts");

        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0].stage, Stage::RustContract);
        assert_eq!(attempts[0].attempt_number, 1);
        assert_eq!(attempts[1].stage, Stage::Implement);
        assert_eq!(attempts[1].status, "failed");
        assert_eq!(attempts[1].feedback.as_deref(), Some("missing artifact"));
        assert!(attempts[1].completed_at.is_none());
    }

    #[test]
    fn parse_resume_artifacts_clamps_negative_byte_lengths_and_maps_types() {
        let now = Utc::now();
        let artifacts_json = json!([
            {
                "artifact_type": "contract_document",
                "created_at": now,
                "content_hash": "doc-hash",
                "byte_length": 256,
            },
            {
                "artifact_type": "failure_details",
                "created_at": now,
                "content_hash": null,
                "byte_length": -10,
            }
        ]);

        let artifacts = parse_resume_artifacts(artifacts_json).expect("parse resume artifacts");

        assert_eq!(artifacts.len(), 2);
        assert_eq!(artifacts[0].artifact_type, ArtifactType::ContractDocument);
        assert_eq!(artifacts[1].artifact_type, ArtifactType::FailureDetails);
        assert_eq!(artifacts[1].byte_length, 0);
        assert_eq!(artifacts[0].content_hash.as_deref(), Some("doc-hash"));
    }

    #[test]
    fn resume_artifact_query_types_include_contract_and_implementation() {
        let query_types = resume_artifact_type_names();
        assert!(
            query_types.contains(&"contract_document".to_string()),
            "contract artifact missing from query type list"
        );
        assert!(
            query_types.contains(&"implementation_code".to_string()),
            "implementation artifact missing from query type list"
        );
        assert!(
            query_types.contains(&"retry_packet".to_string()),
            "retry packet artifact missing from resume query type list"
        );
        assert!(
            !query_types.contains(&"requirements".to_string()),
            "non-actionable requirements artifact leaked into resume query"
        );
    }
}
