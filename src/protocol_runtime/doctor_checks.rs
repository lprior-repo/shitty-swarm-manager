use super::ProtocolRequest;
use serde_json::json;
use tokio::process::Command;

pub async fn check_command(command: &str) -> serde_json::Value {
    match Command::new("bash")
        .arg("-lc")
        .arg(format!("command -v {command}"))
        .output()
        .await
    {
        Ok(output) => {
            if output.status.success() {
                json!({"name": command, "ok": true})
            } else {
                json!({"name": command, "ok": false, "fix": format!("Install '{}' and ensure it is on PATH.", command)})
            }
        }
        Err(_) => json!({
            "name": command,
            "ok": false,
            "fix": format!("Install '{}' and ensure it is on PATH.", command),
        }),
    }
}

pub async fn check_database_connectivity(request: &ProtocolRequest) -> serde_json::Value {
    check_database_connectivity_with_timeout(request, super::DEFAULT_DB_CONNECT_TIMEOUT_MS).await
}

pub async fn check_database_connectivity_with_timeout(
    request: &ProtocolRequest,
    timeout_ms: u64,
) -> serde_json::Value {
    let explicit_database_url = request
        .args
        .get("database_url")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let candidates = super::audit::database_url_candidates_with_explicit(explicit_database_url);
    let (connected, failures) =
        super::db_resolution::try_connect_candidates(&candidates, timeout_ms).await;

    match connected {
        Some((_db, connected_url)) => {
            let source = if explicit_database_url == Some(connected_url.as_str()) {
                "request.database_url"
            } else {
                "discovered"
            };
            json!({"name": "database", "ok": true, "url": super::db_resolution::mask_database_url(&connected_url), "source": source})
        }
        None => json!({
            "name": "database",
            "ok": false,
            "source": if explicit_database_url.is_some() { "request.database_url+fallback" } else { "discovered" },
            "fix": if explicit_database_url.is_some() {
                "Check request.database_url, set DATABASE_URL, verify postgres is reachable, or run 'swarm init-local-db'"
            } else {
                "Set DATABASE_URL, verify postgres is reachable, or run 'swarm init-local-db'"
            },
            "errors": failures,
        }),
    }
}
