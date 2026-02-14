use super::parsing;
use super::ProtocolRequest;
use crate::config::database_url_candidates_for_cli;
use crate::protocol_envelope::ProtocolEnvelope;
use crate::{code, RepoId, SwarmDb};
use serde_json::{json, Value};

pub(super) async fn db_from_request(
    request: &ProtocolRequest,
    default_timeout_ms: u64,
    min_timeout_ms: u64,
    max_timeout_ms: u64,
) -> std::result::Result<SwarmDb, Box<ProtocolEnvelope>> {
    let explicit_database_url = request
        .args
        .get("database_url")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let candidates =
        compose_database_url_candidates(explicit_database_url, database_url_candidates_for_cli());
    let timeout_ms = parsing::request_connect_timeout_ms(
        request,
        default_timeout_ms,
        min_timeout_ms,
        max_timeout_ms,
    )?;
    connect_using_candidates(candidates, timeout_ms, request.rid.clone()).await
}

pub(super) async fn resolve_database_url_for_init(
    request: &ProtocolRequest,
    default_timeout_ms: u64,
    min_timeout_ms: u64,
    max_timeout_ms: u64,
) -> std::result::Result<String, Box<ProtocolEnvelope>> {
    if let Some(url) = request
        .args
        .get("url")
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string)
    {
        return Ok(url);
    }

    if let Some(url) = request
        .args
        .get("database_url")
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string)
    {
        return Ok(url);
    }

    let candidates = database_url_candidates_for_cli();
    let timeout_ms = parsing::request_connect_timeout_ms(
        request,
        default_timeout_ms,
        min_timeout_ms,
        max_timeout_ms,
    )?;
    let (connected, failures) = try_connect_candidates(&candidates, timeout_ms).await;
    if let Some((_db, connected_url)) = connected {
        return Ok(connected_url);
    }

    let masked = candidates
        .iter()
        .map(|candidate| mask_database_url(candidate))
        .collect::<Vec<_>>();

    Err(Box::new(
        ProtocolEnvelope::error(
            request.rid.clone(),
            code::INTERNAL.to_string(),
            "Unable to resolve a reachable database URL for init-db".to_string(),
        )
        .with_fix("Pass --url <database_url> or run 'swarm init-local-db'".to_string())
        .with_ctx(json!({"tried": masked, "errors": failures})),
    ))
}

pub(super) async fn connect_using_candidates(
    candidates: Vec<String>,
    timeout_ms: u64,
    rid: Option<String>,
) -> std::result::Result<SwarmDb, Box<ProtocolEnvelope>> {
    let (connected, failures) = try_connect_candidates(&candidates, timeout_ms).await;
    if let Some((db, _connected_url)) = connected {
        return Ok(db);
    }

    let masked = candidates
        .iter()
        .map(|candidate| mask_database_url(candidate))
        .collect::<Vec<_>>();

    Err(Box::new(
        ProtocolEnvelope::error(
            rid,
            code::INTERNAL.to_string(),
            "Unable to connect to any configured database URL".to_string(),
        )
        .with_fix(
            "Set DATABASE_URL, verify postgres is reachable, or run 'swarm init-local-db'"
                .to_string(),
        )
        .with_ctx(json!({"tried": masked, "errors": failures})),
    ))
}

pub(super) async fn try_connect_candidates(
    candidates: &[String],
    timeout_ms: u64,
) -> (Option<(SwarmDb, String)>, Vec<String>) {
    let mut failures = Vec::new();

    for candidate in candidates {
        match SwarmDb::new_with_timeout(candidate, Some(timeout_ms)).await {
            Ok(db) => return (Some((db, candidate.clone())), failures),
            Err(err) => failures.push(format!("{}: {}", mask_database_url(candidate), err)),
        }
    }

    (None, failures)
}

pub(super) fn compose_database_url_candidates(
    explicit_database_url: Option<&str>,
    discovered_candidates: Vec<String>,
) -> Vec<String> {
    let mut candidates = Vec::new();

    if let Some(explicit) = explicit_database_url {
        let trimmed = explicit.trim();
        if !trimmed.is_empty() {
            candidates.push(trimmed.to_string());
        }
    }

    for candidate in discovered_candidates {
        if !candidates.iter().any(|existing| existing == &candidate) {
            candidates.push(candidate);
        }
    }

    candidates
}

pub(super) fn repo_id_from_request(request: &ProtocolRequest) -> RepoId {
    request
        .args
        .get("repo_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(RepoId::new)
        .or_else(RepoId::from_current_dir)
        .unwrap_or_else(|| RepoId::new("local"))
}

pub(super) fn mask_database_url(url: &str) -> String {
    match url::Url::parse(url) {
        Ok(mut parsed) => {
            if parsed.password().is_some() {
                let _ = parsed.set_password(Some("********"));
            }
            parsed.to_string()
        }
        Err(_) => "<invalid-database-url>".to_string(),
    }
}

#[must_use]
pub fn mask_database_url_public(url: &str) -> String {
    mask_database_url(url)
}
