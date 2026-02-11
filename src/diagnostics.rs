/// Classify a failure message into a normalized diagnostics category.
#[must_use]
pub fn classify_failure_category(message: &str) -> &'static str {
    let lowered = message.to_ascii_lowercase();
    if lowered.contains("timeout") {
        "timeout"
    } else if lowered.contains("syntax") || lowered.contains("compile") {
        "compile_error"
    } else if lowered.contains("test") || lowered.contains("assert") {
        "test_failure"
    } else {
        "stage_failure"
    }
}

/// Redact sensitive tokens (API keys, passwords, etc.) from a message.
#[must_use]
pub fn redact_sensitive(message: &str) -> String {
    message
        .split_whitespace()
        .map(redact_token)
        .collect::<Vec<_>>()
        .join(" ")
}

#[must_use]
fn redact_token(token: &str) -> String {
    token.split_once('=').map_or_else(
        || token.to_string(),
        |(key, _)| {
            let normalized = key.to_ascii_lowercase();
            if ["token", "password", "secret", "api_key", "database_url"]
                .iter()
                .any(|sensitive| normalized.contains(sensitive))
            {
                format!("{key}=<redacted>")
            } else {
                token.to_string()
            }
        },
    )
}
