use std::fs;
use std::path::Path;

fn read_repo_file(relative_path: &str) -> Result<String, String> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative_path);
    fs::read_to_string(&path).map_err(|err| format!("failed to read {}: {err}", path.display()))
}

#[test]
fn runtime_prompt_uses_bead_as_canonical_work_unit() -> Result<(), String> {
    let prompt = swarm::prompts::AGENT_PROMPT_TEMPLATE;

    if !prompt.contains("moving a bead through a **state machine**") {
        return Err("agent prompt must describe bead lifecycle explicitly".to_string());
    }

    if prompt.contains("moving a task through a **state machine**") {
        return Err("agent prompt should not use deprecated 'task' term".to_string());
    }

    Ok(())
}

#[test]
fn glossary_publishes_required_canonical_terms() -> Result<(), String> {
    let glossary = read_repo_file("docs/UBIQUITOUS_LANGUAGE.md")?;
    let required_terms = [
        "**Bead**",
        "**Claim**",
        "**Attempt**",
        "**Transition**",
        "**Landing**",
    ];

    if let Some(term) = required_terms
        .iter()
        .find(|term| !glossary.contains(**term))
    {
        return Err(format!("glossary missing required canonical term {term}"));
    }

    Ok(())
}

#[test]
fn schema_declares_deprecated_aliases_for_work_unit_nouns() -> Result<(), String> {
    let schema = read_repo_file("crates/swarm-coordinator/schema.sql")?;

    if !schema.contains("Deprecated aliases: task, issue, work item") {
        return Err(
            "schema must document deprecated aliases for canonical bead terminology".to_string(),
        );
    }

    Ok(())
}
