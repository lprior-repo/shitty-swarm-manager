use crate::BeadId;
use serde::Deserialize;
use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
};

#[must_use]
pub fn contract_document_and_artifacts(bead_id: &BeadId) -> (String, HashMap<String, String>) {
    let issue = load_bead_issue(bead_id.value());
    let bead_reference = issue
        .as_ref()
        .map(|issue| format!("{} — {}", bead_id.value(), issue.title))
        .unwrap_or_else(|| bead_id.value().to_string());
    let issue_goal = issue
        .as_ref()
        .map(|issue| format!("- Issue goal: {}\n", issue.title))
        .unwrap_or_default();
    let requirements = format!(
        "- Implement bead {bead_id}\n- Preserve deterministic stage transitions\n- Persist artifacts for downstream stages\n{issue_goal}"
    );
    let system_context =
        "Pipeline stages coordinate through persisted artifacts and agent messages".to_string();
    let invariants =
        "- No unwrap/expect/panic paths\n- Stage artifacts are persisted before stage completion"
            .to_string();
    let data_flow =
        "rust-contract -> implement -> qa-enforcer -> red-queen via typed artifacts".to_string();
    let implementation_plan =
        "1. Generate contract\n2. Implement from contract\n3. Run quick gate\n4. Run adversarial gate".to_string();
    let acceptance_criteria =
        "- StageResult matches execution outcome\n- Required artifacts exist for each stage"
            .to_string();
    let error_handling =
        "All fallible operations return Result and propagate contextual SwarmError values"
            .to_string();
    let test_scenarios =
        "Given/When/Then scenarios validate success, failure, and missing-artifact paths"
            .to_string();
    let validation_gates = "moon run :quick then moon run :test".to_string();
    let success_metrics =
        "Successful artifact handoff between stages and readable full message payloads".to_string();
    let metadata_section = issue
        .as_ref()
        .map(render_issue_metadata)
        .unwrap_or_else(|| "Bead metadata unavailable; refer to backlog.".to_string());
    let issue_description_block = issue.as_ref().map(|issue| issue.description.clone());
    let ai_hints_block = issue
        .as_ref()
        .and_then(|issue| parse_ai_hints(&issue.description));

    let mut contract_document = format!(
            r"# Contract for {bead_reference}

## Goal
Deliver the bead with explicit behavior boundaries and failure semantics.

## Bead Metadata
{metadata_section}

## Requirements
{requirements}

## Given-When-Then Scenarios
- Given valid bead context, when implementation runs, then behavior is deterministic and side effects are explicit.
- Given dependency failure, when stage logic runs, then errors propagate via Result with no panic path.
- Given stage artifacts, when downstream stages execute, then required artifacts are discoverable and typed.

## Implementation Plan
1. Load required artifacts from persistence layer.
2. Transform data through pure functions where possible.
3. Isolate shell/process boundaries into thin async adapters.
4. Persist typed artifacts for downstream stages.

## Acceptance Criteria
- No unwrap, expect, panic, todo, or unimplemented paths.
- Stage status reflects command outcome with actionable feedback.
- Artifacts are persisted for each stage before completion.

## System Context
{system_context}

## Invariants
{invariants}

## Data Flow
{data_flow}

## Error Handling
{error_handling}

## Test Scenarios
{test_scenarios}

## Validation Gates
{validation_gates}

## Success Metrics
{success_metrics}

"#
        );

    if let Some(description) = &issue_description_block {
        contract_document.push_str("## Backlog Description\n```cue\n");
        contract_document.push_str(description.trim());
        contract_document.push_str("\n```\n\n");
    }

    if let Some(ai_hints) = &ai_hints_block {
        contract_document.push_str("## AI Hints\n");
        contract_document.push_str(&render_ai_hints(ai_hints));
        contract_document.push('\n');
    }

    let mut artifacts = vec![
        ("requirements".to_string(), requirements.clone()),
        ("system_context".to_string(), system_context),
        ("invariants".to_string(), invariants),
        ("data_flow".to_string(), data_flow),
        ("implementation_plan".to_string(), implementation_plan),
        ("acceptance_criteria".to_string(), acceptance_criteria),
        ("error_handling".to_string(), error_handling),
        ("test_scenarios".to_string(), test_scenarios),
        ("validation_gates".to_string(), validation_gates),
        ("success_metrics".to_string(), success_metrics),
    ];

    if issue.is_some() {
        artifacts.push(("issue_metadata".to_string(), metadata_section.clone()));
        if let Some(description) = issue_description_block.clone() {
            artifacts.push(("issue_description".to_string(), description));
        }
        if let Some(ai_hints) = ai_hints_block {
            artifacts.push(("ai_hints".to_string(), render_ai_hints(&ai_hints)));
        }
    }

    let artifacts = artifacts.into_iter().collect::<HashMap<_, _>>();

    (contract_document, artifacts)
}

#[must_use]
pub fn implementation_scaffold(bead_id: &BeadId, contract_context: &str) -> String {
    format!(
        r#"// Implementation scaffold for {bead_id}
// Contract context summary:
// {contract_context}

pub fn process_bead(input: &str) -> Result<String, String> {{
    if input.trim().is_empty() {{
        return Err("input cannot be empty".to_string());
    }}

    Ok(format!("processed: {{}}", input))
}}

#[cfg(test)]
mod implementation_contract_tests {{
    use super::process_bead;

    #[test]
    fn given_valid_input_when_processing_then_returns_processed_payload() {{
        let result = process_bead("bead");
        assert!(matches!(result, Ok(ref value) if value == "processed: bead"));
    }}

    #[test]
    fn given_empty_input_when_processing_then_returns_error() {{
        let result = process_bead("  ");
        assert!(result.is_err());
    }}
}}
"#
    )
}

#[derive(Debug, Deserialize)]
struct BeadIssue {
    id: String,
    title: String,
    description: String,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    priority: Option<i64>,
    #[serde(rename = "issue_type")]
    #[serde(default)]
    issue_type: Option<String>,
    #[serde(default)]
    effort_estimate: Option<String>,
    #[serde(default)]
    labels: Option<Vec<String>>,
}

#[derive(Debug)]
struct AiHints {
    do_: Vec<String>,
    do_not: Vec<String>,
    constitution: Vec<String>,
}

fn load_bead_issue(bead_id: &str) -> Option<BeadIssue> {
    let file = File::open(".beads/issues.jsonl").ok()?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        if let Ok(record) = line {
            if let Ok(issue) = serde_json::from_str::<BeadIssue>(&record) {
                if issue.id == bead_id {
                    return Some(issue);
                }
            }
        }
    }

    None
}

fn render_issue_metadata(issue: &BeadIssue) -> String {
    let mut lines = vec![
        format!("- ID: {}", issue.id),
        format!("- Title: {}", issue.title),
    ];

    if let Some(status) = &issue.status {
        lines.push(format!("- Status: {}", status));
    }

    if let Some(priority) = issue.priority {
        lines.push(format!("- Priority: {priority}"));
    }

    if let Some(issue_type) = &issue.issue_type {
        lines.push(format!("- Issue Type: {issue_type}"));
    }

    if let Some(effort) = &issue.effort_estimate {
        lines.push(format!("- Effort Estimate: {effort}"));
    }

    if let Some(labels) = &issue.labels {
        if !labels.is_empty() {
            lines.push(format!("- Labels: {}", labels.join(", ")));
        }
    }

    lines.join("\n")
}

fn parse_ai_hints(description: &str) -> Option<AiHints> {
    let ai_block = capture_braced_block(description, "ai_hints:")?;
    let do_list = parse_array_block(ai_block, "do:");
    let do_not = parse_array_block(ai_block, "do_not:");
    let constitution = parse_array_block(ai_block, "constitution:");

    if do_list.is_empty() && do_not.is_empty() && constitution.is_empty() {
        None
    } else {
        Some(AiHints {
            do_,
            do_not,
            constitution,
        })
    }
}

fn render_ai_hints(hints: &AiHints) -> String {
    let mut sections = Vec::new();

    if !hints.do_.is_empty() {
        sections.push(format!("### Do\n{}\n", bullet_list(&hints.do_)));
    }

    if !hints.do_not.is_empty() {
        sections.push(format!("### Do Not\n{}\n", bullet_list(&hints.do_not)));
    }

    if !hints.constitution.is_empty() {
        sections.push(format!(
            "### Constitution\n{}\n",
            bullet_list(&hints.constitution)
        ));
    }

    sections.join("\n")
}

fn parse_array_block(block: &str, key: &str) -> Vec<String> {
    capture_bracketed_block(block, key)
        .and_then(|snippet| serde_json::from_str::<Vec<String>>(snippet).ok())
        .unwrap_or_default()
}

fn capture_braced_block<'a>(source: &'a str, key: &str) -> Option<&'a str> {
    let key_idx = source.find(key)?;
    let open_idx = source[key_idx..].find('{')? + key_idx;
    let close_idx = find_matching_char(source, open_idx, '{', '}')?;
    Some(&source[open_idx..=close_idx])
}

fn capture_bracketed_block<'a>(source: &'a str, key: &str) -> Option<&'a str> {
    let key_idx = source.find(key)?;
    let open_idx = source[key_idx..].find('[')? + key_idx;
    let close_idx = find_matching_char(source, open_idx, '[', ']')?;
    Some(&source[open_idx..=close_idx])
}

fn find_matching_char(text: &str, start: usize, open: char, close: char) -> Option<usize> {
    let mut depth = 0;

    for (idx, ch) in text[start..].char_indices() {
        if ch == open {
            depth += 1;
        } else if ch == close {
            depth -= 1;
            if depth == 0 {
                return Some(start + idx);
            }
        }
    }

    None
}

fn bullet_list(items: &[String]) -> String {
    items
        .iter()
        .map(|item| format!("- {item}"))
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    const AI_HINTS_SAMPLE: &str = r#"
ai_hints: {
  do: [
    "Use functional patterns"
  ]
  do_not: [
    "Do NOT panic"
  ]
  constitution: [
    "Zero unwrap law"
  ]
}
"#;

    #[test]
    fn parse_ai_hints_extracts_lists() {
        let hints = parse_ai_hints(AI_HINTS_SAMPLE).expect("expected hints");
        assert_eq!(hints.do_, vec!["Use functional patterns".to_string()]);
        assert_eq!(hints.do_not, vec!["Do NOT panic".to_string()]);
        assert_eq!(hints.constitution, vec!["Zero unwrap law".to_string()]);
    }

    #[test]
    fn render_issue_metadata_formats_fields() {
        let issue = BeadIssue {
            id: "swm-3qw".to_string(),
            title: "contract-stage".to_string(),
            description: "desc".to_string(),
            status: Some("in_progress".to_string()),
            priority: Some(1),
            issue_type: Some("feature".to_string()),
            effort_estimate: Some("2hr".to_string()),
            labels: Some(vec!["planner-generated".to_string()]),
        };

        let metadata = render_issue_metadata(&issue);
        assert!(metadata.contains("- ID: swm-3qw"));
        assert!(metadata.contains("- Title: contract-stage"));
        assert!(metadata.contains("- Status: in_progress"));
        assert!(metadata.contains("- Priority: 1"));
        assert!(metadata.contains("- Issue Type: feature"));
        assert!(metadata.contains("- Effort Estimate: 2hr"));
        assert!(metadata.contains("- Labels: planner-generated"));
    }
}
