use crate::protocol_envelope::ProtocolEnvelope;
use crate::{code, SwarmError};
use serde_json::json;
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// # Errors
/// Returns an error if stdin reading or stdout writing fails.
pub async fn run_protocol_loop() -> std::result::Result<(), SwarmError> {
    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    let mut processed_non_empty_line = false;

    while let Some(line) = lines.next_line().await.map_err(SwarmError::IoError)? {
        if line.trim().is_empty() {
            continue;
        }

        processed_non_empty_line = true;
        super::process_protocol_line(&line).await?;
    }

    if !processed_non_empty_line {
        emit_no_input_envelope().await?;
    }

    Ok(())
}

async fn emit_no_input_envelope() -> std::result::Result<(), SwarmError> {
    let mut stdout = tokio::io::stdout();
    let envelope = ProtocolEnvelope::error(
        None,
        code::INVALID.to_string(),
        "No input received on stdin".to_string(),
    )
    .with_fix(
        "Provide one JSON command per line. Example: echo '{\"cmd\":\"doctor\"}' | swarm"
            .to_string(),
    )
    .with_ctx(json!({"stdin": "empty"}))
    .with_ms(0);

    let response_text = serde_json::to_string(&envelope).map_err(SwarmError::SerializationError)?;
    stdout
        .write_all(response_text.as_bytes())
        .await
        .map_err(SwarmError::IoError)?;
    stdout.write_all(b"\n").await.map_err(SwarmError::IoError)
}

pub fn elapsed_ms(start: Instant) -> u64 {
    let ms = start.elapsed().as_millis();
    u64::try_from(ms).map_or(u64::MAX, |value| value)
}
