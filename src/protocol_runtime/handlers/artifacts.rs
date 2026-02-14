use super::super::{
    db_from_request, minimal_state_for_request, repo_id_from_request, to_protocol_failure,
    CommandSuccess, ProtocolRequest,
};
use crate::protocol_envelope::ProtocolEnvelope;
use crate::{code, ArtifactType, BeadId, StageArtifact, SwarmDb};
use serde_json::{json, Value};
use std::future::Future;
use std::pin::Pin;

type ArtifactPortFuture<'a, T> = Pin<Box<dyn Future<Output = crate::Result<T>> + Send + 'a>>;

trait ArtifactQueryPort {
    fn bead_artifacts<'a>(
        &'a self,
        request: &'a ArtifactQuery,
    ) -> ArtifactPortFuture<'a, Vec<StageArtifact>>;
}

trait ArtifactHandlerPorts: ArtifactQueryPort {}

impl<T> ArtifactHandlerPorts for T where T: ArtifactQueryPort {}

#[derive(Debug, Clone)]
struct ArtifactQuery {
    repo_id: crate::RepoId,
    bead_id: BeadId,
    artifact_type: Option<ArtifactType>,
}

impl ArtifactQuery {
    const fn new(repo_id: crate::RepoId, bead_id: BeadId, artifact_type: Option<ArtifactType>) -> Self {
        Self {
            repo_id,
            bead_id,
            artifact_type,
        }
    }
}

struct SwarmDbArtifactPort {
    db: SwarmDb,
}

impl SwarmDbArtifactPort {
    const fn new(db: SwarmDb) -> Self {
        Self { db }
    }
}

impl ArtifactQueryPort for SwarmDbArtifactPort {
    fn bead_artifacts<'a>(
        &'a self,
        request: &'a ArtifactQuery,
    ) -> ArtifactPortFuture<'a, Vec<StageArtifact>> {
        Box::pin(async move {
            self.db
                .get_bead_artifacts(&request.repo_id, &request.bead_id, request.artifact_type)
                .await
        })
    }
}

#[allow(clippy::future_not_send)]
async fn fetch_artifacts<P: ArtifactHandlerPorts>(
    ports: &P,
    query: &ArtifactQuery,
) -> crate::Result<Vec<StageArtifact>> {
    ports.bead_artifacts(query).await
}

pub(in crate::protocol_runtime) async fn handle_artifacts(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let bead_id = parse_artifact_bead_id(request)?;
    let artifact_type = parse_artifact_type(request)?;
    let db: SwarmDb = db_from_request(request).await?;
    let query = ArtifactQuery::new(
        repo_id_from_request(request),
        bead_id.clone(),
        artifact_type,
    );
    let ports = SwarmDbArtifactPort::new(db);
    let artifacts = fetch_artifacts(&ports, &query)
        .await
        .map_err(|error| to_protocol_failure(error, request.rid.clone()))?;
    let artifact_payload = artifacts.iter().map(artifact_to_json).collect::<Vec<_>>();

    Ok(CommandSuccess {
        data: json!({
            "bead_id": bead_id.value(),
            "artifact_count": artifact_payload.len(),
            "artifacts": artifact_payload,
        }),
        next: "swarm monitor --view progress".to_string(),
        state: minimal_state_for_request(request).await,
    })
}

fn parse_artifact_bead_id(
    request: &ProtocolRequest,
) -> std::result::Result<BeadId, Box<ProtocolEnvelope>> {
    let bead_id_str = request
        .args
        .get("bead_id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            Box::new(
                ProtocolEnvelope::error(
                    request.rid.clone(),
                    code::INVALID.to_string(),
                    "Missing required field: bead_id".to_string(),
                )
                .with_fix(
                    "Include `bead_id` in the request. Example: {\"cmd\":\"artifacts\",\"bead_id\":\"<bead>\"}".to_string(),
                )
                .with_ctx(json!({"bead_id": "required"})),
            )
        })?;

    if bead_id_str.trim().is_empty() {
        return Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                "bead_id cannot be empty".to_string(),
            )
            .with_fix(
                "Provide a non-empty bead_id. Example: {\"cmd\":\"artifacts\",\"bead_id\":\"swm-abc123\"}".to_string(),
            )
            .with_ctx(json!({"bead_id": bead_id_str})),
        ));
    }

    if bead_id_str.len() > 255 {
        return Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                "bead_id exceeds maximum length of 255 characters".to_string(),
            )
            .with_fix(format!(
                "Provide a bead_id with 255 or fewer characters. Current length: {}",
                bead_id_str.len()
            ))
            .with_ctx(json!({"bead_id": bead_id_str, "length": bead_id_str.len()})),
        ));
    }

    Ok(BeadId::new(bead_id_str))
}

fn parse_artifact_type(
    request: &ProtocolRequest,
) -> std::result::Result<Option<ArtifactType>, Box<ProtocolEnvelope>> {
    let Some(raw_artifact_type) = request.args.get("artifact_type") else {
        return Ok(None);
    };

    let Some(raw_artifact_type) = raw_artifact_type.as_str() else {
        return Err(Box::new(
            ProtocolEnvelope::error(
                request.rid.clone(),
                code::INVALID.to_string(),
                "artifact_type must be a string".to_string(),
            )
            .with_fix(format!(
                "Use artifact_type from: {}",
                ArtifactType::names().join(", ")
            ))
            .with_ctx(json!({"artifact_type": request.args.get("artifact_type")})),
        ));
    };

    let candidate = raw_artifact_type.trim();
    if candidate.is_empty() {
        return Ok(None);
    }

    ArtifactType::try_from(candidate)
        .map(Some)
        .map_err(|error| {
            Box::new(
                ProtocolEnvelope::error(request.rid.clone(), code::INVALID.to_string(), error)
                    .with_fix(format!(
                        "Use artifact_type from: {}",
                        ArtifactType::names().join(", ")
                    ))
                    .with_ctx(json!({"artifact_type": candidate})),
            )
        })
}

fn artifact_to_json(artifact: &StageArtifact) -> Value {
    json!({
        "id": artifact.id,
        "stage_history_id": artifact.stage_history_id,
        "artifact_type": artifact.artifact_type.as_str(),
        "content": artifact.content.clone(),
        "metadata": artifact.metadata.clone(),
        "created_at": artifact.created_at.to_rfc3339(),
        "content_hash": artifact.content_hash.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{map::Map, Value};

    fn request_with_args(entries: &[(&str, &str)]) -> ProtocolRequest {
        let args = entries
            .iter()
            .map(|(key, value)| (key.to_string(), Value::String(value.to_string())))
            .collect::<Map<_, _>>();
        ProtocolRequest {
            cmd: "artifacts".to_string(),
            rid: None,
            dry: None,
            args,
        }
    }

    #[test]
    fn given_valid_bead_id_when_parsing_then_value_is_returned() {
        let request = request_with_args(&[("bead_id", "bead-42")]);

        let bead_id = parse_artifact_bead_id(&request);

        assert_eq!(bead_id.ok().as_ref().map(BeadId::value), Some("bead-42"));
    }

    #[test]
    fn given_missing_bead_id_when_parsing_then_invalid_envelope_is_returned() {
        let request = request_with_args(&[]);

        let error = parse_artifact_bead_id(&request).err();

        assert_eq!(
            error
                .as_ref()
                .and_then(|envelope| envelope.err.as_ref())
                .map(|err| err.code.as_str()),
            Some("INVALID")
        );
        assert!(error
            .and_then(|envelope| envelope.fix)
            .is_some_and(|fix| fix.contains("bead_id")));
    }

    #[test]
    fn given_unknown_artifact_type_when_parsing_then_invalid_envelope_is_returned() {
        let request = request_with_args(&[("bead_id", "bead-42"), ("artifact_type", "unknown")]);

        let error = parse_artifact_type(&request).err();

        assert_eq!(
            error
                .as_ref()
                .and_then(|envelope| envelope.err.as_ref())
                .map(|err| err.code.as_str()),
            Some("INVALID")
        );
    }

    #[test]
    fn given_boolean_artifact_type_when_parsing_then_type_error_is_returned() {
        let mut request = request_with_args(&[("bead_id", "bead-42")]);
        request
            .args
            .insert("artifact_type".to_string(), Value::Bool(true));

        let error = parse_artifact_type(&request).err();

        assert_eq!(
            error
                .as_ref()
                .and_then(|envelope| envelope.err.as_ref())
                .map(|err| err.msg.as_str()),
            Some("artifact_type must be a string")
        );
    }
}
