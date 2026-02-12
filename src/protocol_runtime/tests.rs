#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use super::{
    bounded_history_limit, capture_stream_limited, compose_database_url_candidates,
    parse_database_connect_timeout_ms, ParseInput, ProtocolRequest, DEFAULT_DB_CONNECT_TIMEOUT_MS,
    MAX_DB_CONNECT_TIMEOUT_MS, MAX_HISTORY_LIMIT, MIN_DB_CONNECT_TIMEOUT_MS,
};
use serde_json::{json, Map, Value};
use tokio::io::{AsyncWriteExt, DuplexStream};

fn make_request(cmd: &str, args: Map<String, Value>) -> ProtocolRequest {
    ProtocolRequest {
        cmd: cmd.to_string(),
        rid: None,
        dry: None,
        args,
    }
}

#[test]
fn given_explicit_database_url_when_composing_candidates_then_it_is_first_and_deduped() {
    let candidates = compose_database_url_candidates(
        Some("postgres://explicit/db"),
        vec![
            "postgres://explicit/db".to_string(),
            "postgres://env/db".to_string(),
        ],
    );

    assert_eq!(
        candidates,
        vec![
            "postgres://explicit/db".to_string(),
            "postgres://env/db".to_string(),
        ]
    );
}

#[test]
fn given_invalid_connect_timeout_when_parsed_then_default_is_used() {
    assert_eq!(
        parse_database_connect_timeout_ms(Some("invalid")),
        DEFAULT_DB_CONNECT_TIMEOUT_MS
    );
}

#[test]
fn given_out_of_range_connect_timeout_when_parsed_then_bounds_are_enforced() {
    assert_eq!(
        parse_database_connect_timeout_ms(Some("1")),
        MIN_DB_CONNECT_TIMEOUT_MS
    );
    assert_eq!(
        parse_database_connect_timeout_ms(Some("999999")),
        MAX_DB_CONNECT_TIMEOUT_MS
    );
}

#[test]
fn given_large_history_limit_when_bounded_then_limit_is_capped() {
    assert_eq!(bounded_history_limit(Some(50_000)), MAX_HISTORY_LIMIT);
}

#[test]
fn given_negative_agent_id_when_parsing_agent_input_then_parse_error_is_returned() {
    let mut args = Map::new();
    args.insert("id".to_string(), json!(-1));
    let request = make_request("agent", args);

    let result = crate::AgentInput::parse_input(&request);

    assert!(result.is_err());
}

#[test]
fn given_zero_register_count_when_parsing_register_input_then_parse_error_is_returned() {
    let mut args = Map::new();
    args.insert("count".to_string(), json!(0));
    let request = make_request("register", args);

    let result = crate::RegisterInput::parse_input(&request);

    assert!(result.is_err());
}

#[test]
fn given_missing_lock_resource_when_parsing_lock_input_then_parse_error_is_returned() {
    let mut args = Map::new();
    args.insert("agent".to_string(), json!("agent-1"));
    args.insert("ttl_ms".to_string(), json!(30_000));
    let request = make_request("lock", args);

    let result = crate::LockInput::parse_input(&request);

    assert!(result.is_err());
}

async fn write_all(mut writer: DuplexStream, bytes: Vec<u8>) -> std::io::Result<()> {
    writer.write_all(&bytes).await?;
    writer.shutdown().await
}

#[tokio::test]
async fn given_stream_under_limit_when_captured_then_payload_is_complete() {
    let (writer, reader) = tokio::io::duplex(64);
    let payload = b"hello-stream".to_vec();
    let writer_task = tokio::spawn(write_all(writer, payload.clone()));

    let captured = capture_stream_limited(reader, 1024).await;
    let writer_result = writer_task.await;

    assert!(captured.is_ok());
    assert!(writer_result.is_ok());
    let capture = captured.expect("stream should capture");
    assert_eq!(capture.bytes, payload);
    assert!(!capture.truncated);
}

#[tokio::test]
async fn given_stream_over_limit_when_captured_then_payload_is_truncated() {
    let (writer, reader) = tokio::io::duplex(32);
    let payload = b"abcdefghijklmnopqrstuvwxyz".to_vec();
    let writer_task = tokio::spawn(write_all(writer, payload));

    let captured = capture_stream_limited(reader, 10).await;
    let writer_result = writer_task.await;

    assert!(captured.is_ok());
    assert!(writer_result.is_ok());
    let capture = captured.expect("stream should capture");
    assert_eq!(capture.bytes, b"abcdefghij".to_vec());
    assert!(capture.truncated);
}
