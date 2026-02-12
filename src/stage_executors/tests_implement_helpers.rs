#![allow(clippy::panic)]

use super::implement_stage::{append_section, format_retry_packet};

#[test]
fn given_valid_json_retry_packet_when_formatting_then_pretty_json_is_returned() {
    let payload = r#"{"attempt":1,"failure_reason":"test failed","remaining_attempts":2}"#;

    let formatted = format_retry_packet(payload);

    assert_eq!(
        formatted,
        "{\n  \"attempt\": 1,\n  \"failure_reason\": \"test failed\",\n  \"remaining_attempts\": 2\n}"
    );
}

#[test]
fn given_invalid_json_retry_packet_when_formatting_then_original_payload_is_preserved() {
    let payload = "not-json";

    let formatted = format_retry_packet(payload);

    assert_eq!(formatted, payload);
}

#[test]
fn given_whitespace_only_section_content_when_appending_then_no_section_is_added() {
    let mut sections = vec!["## Contract Document\ncontract".to_string()];

    append_section(&mut sections, "Retry Packet", Some("   \n\t  "));

    assert_eq!(sections, vec!["## Contract Document\ncontract".to_string()]);
}

#[test]
fn given_section_content_with_surrounding_whitespace_when_appending_then_content_is_trimmed() {
    let mut sections = Vec::new();

    append_section(
        &mut sections,
        "Failure Details",
        Some("\n  deterministic failure details\n\n"),
    );

    assert_eq!(
        sections,
        vec!["## Failure Details\ndeterministic failure details".to_string()]
    );
}

#[test]
fn given_contract_and_retry_related_sections_when_composing_then_sections_are_ordered_and_delimited(
) {
    let contract_context = "contract content";
    let retry_packet = format_retry_packet(r#"{"attempt":1,"failure_reason":"flaky test"}"#);
    let failure_details = "assertion failed at line 41";
    let test_results = "1 failed; 12 passed";
    let test_output = "stderr: assertion mismatch";

    let mut context_sections = Vec::new();
    context_sections.push(format!("## Contract Document\n{}", contract_context.trim()));
    append_section(&mut context_sections, "Retry Packet", Some(&retry_packet));
    append_section(
        &mut context_sections,
        "Failure Details",
        Some(failure_details),
    );
    append_section(&mut context_sections, "Test Results", Some(test_results));
    append_section(&mut context_sections, "Test Output", Some(test_output));

    let aggregated_context = context_sections.join("\n\n");

    assert_eq!(
        aggregated_context,
        "## Contract Document\ncontract content\n\n## Retry Packet\n{\n  \"attempt\": 1,\n  \"failure_reason\": \"flaky test\"\n}\n\n## Failure Details\nassertion failed at line 41\n\n## Test Results\n1 failed; 12 passed\n\n## Test Output\nstderr: assertion mismatch"
    );
}
