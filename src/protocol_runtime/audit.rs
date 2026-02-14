use crate::config::database_url_candidates_for_cli;
use crate::SwarmError;

#[allow(clippy::too_many_arguments)]
/// # Errors
/// Returns an error if the database connection or operation fails.
pub async fn audit_request(
    cmd: &str,
    rid: Option<&str>,
    args: serde_json::Value,
    ok: bool,
    ms: u64,
    error_code: Option<&str>,
    candidates: &[String],
    timeout_ms: u64,
) -> std::result::Result<(), SwarmError> {
    let (connected, _failures) =
        super::db_resolution::try_connect_candidates(candidates, timeout_ms).await;
    match connected {
        Some((db, _used_url)) => {
            db.record_command_audit(cmd, rid, args, ok, ms, error_code)
                .await
        }
        None => Err(SwarmError::DatabaseError(
            "Audit database connection failed: no candidates succeeded".to_string(),
        )),
    }
}

pub fn mask_passwords_in_args(args: &mut serde_json::Value) {
    if let Some(obj) = args.as_object_mut() {
        mask_url_password(obj, "database_url");
        mask_url_password(obj, "url");
    }
}

fn mask_url_password(obj: &mut serde_json::Map<String, serde_json::Value>, key: &str) {
    if let Some(url_val) = obj.get_mut(key) {
        if let Some(url_str) = url_val.as_str() {
            if let Ok(mut url) = url::Url::parse(url_str) {
                if url.password().is_some() {
                    let _ = url.set_password(Some("********"));
                    *url_val = serde_json::json!(url.to_string());
                }
            }
        }
    }
}

#[must_use]
pub fn compose_database_url_candidates(
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

pub fn database_url_candidates_with_explicit(explicit: Option<&str>) -> Vec<String> {
    compose_database_url_candidates(explicit, database_url_candidates_for_cli())
}
