use crate::protocol_envelope::ProtocolEnvelope;
use crate::SwarmError;
use serde_json::{json, Value};
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;

pub const MAX_EXTERNAL_OUTPUT_CAPTURE_BYTES: usize = 1_048_576;
const DEFAULT_EXTERNAL_COMMAND_TIMEOUT_MS: u64 = 15_000;

#[derive(Debug, Clone)]
pub struct StreamCapture {
    pub bytes: Vec<u8>,
    pub truncated: bool,
}

pub async fn capture_stream_limited<R>(
    mut stream: R,
    max_bytes: usize,
) -> std::result::Result<StreamCapture, SwarmError>
where
    R: AsyncRead + Unpin,
{
    let mut bytes = Vec::new();
    let mut truncated = false;
    let mut chunk = [0_u8; 8_192];

    loop {
        let read = stream.read(&mut chunk).await.map_err(SwarmError::IoError)?;
        if read == 0 {
            break;
        }

        let remaining = max_bytes.saturating_sub(bytes.len());
        if remaining == 0 {
            truncated = true;
            continue;
        }

        let to_copy = remaining.min(read);
        bytes.extend_from_slice(&chunk[..to_copy]);
        if to_copy < read {
            truncated = true;
        }
    }

    Ok(StreamCapture { bytes, truncated })
}

pub async fn run_external_json_command(
    program: &str,
    args: &[&str],
    rid: Option<String>,
    fix: &str,
) -> std::result::Result<Value, Box<ProtocolEnvelope>> {
    run_external_json_command_with_timeout(
        program,
        args,
        rid,
        fix,
        DEFAULT_EXTERNAL_COMMAND_TIMEOUT_MS,
    )
    .await
}

pub async fn run_external_json_command_with_timeout(
    program: &str,
    args: &[&str],
    rid: Option<String>,
    fix: &str,
    timeout_ms: u64,
) -> std::result::Result<Value, Box<ProtocolEnvelope>> {
    let mut child = Command::new(program)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| {
            Box::new(
                ProtocolEnvelope::error(
                    rid.clone(),
                    crate::code::INTERNAL.to_string(),
                    format!("Failed to execute {program}: {err}"),
                )
                .with_fix(fix.to_string()),
            )
        })?;

    let stdout = child.stdout.take().ok_or_else(|| {
        Box::new(
            ProtocolEnvelope::error(
                rid.clone(),
                crate::code::INTERNAL.to_string(),
                format!("Failed to capture {program} stdout"),
            )
            .with_fix(fix.to_string()),
        )
    })?;

    let stderr = child.stderr.take().ok_or_else(|| {
        Box::new(
            ProtocolEnvelope::error(
                rid.clone(),
                crate::code::INTERNAL.to_string(),
                format!("Failed to capture {program} stderr"),
            )
            .with_fix(fix.to_string()),
        )
    })?;

    let stdout_task = tokio::spawn(async move {
        capture_stream_limited(stdout, MAX_EXTERNAL_OUTPUT_CAPTURE_BYTES).await
    });
    let stderr_task = tokio::spawn(async move {
        capture_stream_limited(stderr, MAX_EXTERNAL_OUTPUT_CAPTURE_BYTES).await
    });

    let status = if let Ok(wait_result) =
        tokio::time::timeout(Duration::from_millis(timeout_ms), child.wait()).await
    {
        wait_result.map_err(SwarmError::IoError).map_err(|err| {
            Box::new(
                ProtocolEnvelope::error(
                    rid.clone(),
                    crate::code::INTERNAL.to_string(),
                    format!("Failed to wait for {program}: {err}"),
                )
                .with_fix(fix.to_string()),
            )
        })?
    } else {
        let _ = child.kill().await;
        return Err(Box::new(
            ProtocolEnvelope::error(
                rid,
                crate::code::INTERNAL.to_string(),
                format!("{program} command timed out"),
            )
            .with_fix(fix.to_string())
            .with_ctx(json!({"program": program, "args": args, "timeout_ms": timeout_ms})),
        ));
    };

    let stdout_capture = stdout_task.await.map_err(|err| {
        Box::new(
            ProtocolEnvelope::error(
                rid.clone(),
                crate::code::INTERNAL.to_string(),
                format!("Failed to read {program} stdout: {err}"),
            )
            .with_fix(fix.to_string()),
        )
    })?;

    let stderr_capture = stderr_task.await.map_err(|err| {
        Box::new(
            ProtocolEnvelope::error(
                rid.clone(),
                crate::code::INTERNAL.to_string(),
                format!("Failed to read {program} stderr: {err}"),
            )
            .with_fix(fix.to_string()),
        )
    })?;

    let stdout_capture = stdout_capture.map_err(|err| {
        Box::new(
            ProtocolEnvelope::error(
                rid.clone(),
                crate::code::INTERNAL.to_string(),
                format!("Failed to read {program} stdout: {err}"),
            )
            .with_fix(fix.to_string()),
        )
    })?;

    let stderr_capture = stderr_capture.map_err(|err| {
        Box::new(
            ProtocolEnvelope::error(
                rid.clone(),
                crate::code::INTERNAL.to_string(),
                format!("Failed to read {program} stderr: {err}"),
            )
            .with_fix(fix.to_string()),
        )
    })?;

    if !status.success() {
        let exit_code = status.code().map_or(1, |code| code);
        let stderr = String::from_utf8_lossy(&stderr_capture.bytes)
            .trim()
            .to_string();
        return Err(Box::new(
            ProtocolEnvelope::error(
                rid,
                crate::code::INTERNAL.to_string(),
                if stderr.is_empty() {
                    format!("{program} command failed")
                } else {
                    format!("{program} command failed: {stderr}")
                },
            )
            .with_fix(fix.to_string())
            .with_ctx(json!({
                "program": program,
                "exit_code": exit_code,
                "stderr": stderr,
                "stderr_truncated": stderr_capture.truncated,
            })),
        ));
    }

    let raw = String::from_utf8_lossy(&stdout_capture.bytes)
        .trim()
        .to_string();
    serde_json::from_str::<Value>(&raw).map_err(|err| {
        Box::new(
            ProtocolEnvelope::error(
                rid,
                crate::code::INVALID.to_string(),
                format!("{program} returned non-JSON output: {err}"),
            )
            .with_fix(fix.to_string())
            .with_ctx(json!({"raw": raw, "stdout_truncated": stdout_capture.truncated})),
        )
    })
}

pub async fn run_external_json_command_with_ms(
    program: &str,
    args: &[&str],
    rid: Option<String>,
    fix: &str,
) -> std::result::Result<(Value, u64), Box<ProtocolEnvelope>> {
    let start = Instant::now();
    run_external_json_command(program, args, rid, fix)
        .await
        .map(|value| (value, super::elapsed_ms(start)))
}
