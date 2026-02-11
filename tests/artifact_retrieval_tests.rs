#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

use anyhow::Result;
use sqlx::PgPool;
use std::str::FromStr;

use swarm::protocol::commands::ArtifactRetrievalRequest;
use swarm::types::artifacts::ArtifactType;

#[tokio::test]
async fn test_artifact_retrieval_happy_path() -> Result<()> {
    // Setup: Assuming a way to insert test artifacts
    let pool =
        PgPool::connect("postgresql://shitty_swarm_manager@localhost:5437/shitty_swarm_manager_db")
            .await?;
    let bead_id = "test-bead-1";

    let request = ArtifactRetrievalRequest {
        bead_id: bead_id.to_string(),
        artifact_type: None,
    };

    // Expect this to fail compilation to drive implementation
    let artifacts = swarm::protocol::artifact_retrieval(&pool, request)
        .await
        .expect("Artifacts retrieval should work");

    // Assertions to drive implementation
    assert!(
        !artifacts.is_empty(),
        "Should retrieve at least some artifacts"
    );

    Ok(())
}

#[tokio::test]
async fn test_artifact_retrieval_with_type_filter() -> Result<()> {
    let pool =
        PgPool::connect("postgresql://shitty_swarm_manager@localhost:5437/shitty_swarm_manager_db")
            .await?;
    let bead_id = "test-bead-1";

    let artifact_type = ArtifactType::from_str("ContractDocument").unwrap();

    let request = ArtifactRetrievalRequest {
        bead_id: bead_id.to_string(),
        artifact_type: Some(artifact_type),
    };

    // Expect this to fail compilation to drive implementation
    let artifacts = swarm::protocol::artifact_retrieval(&pool, request)
        .await
        .expect("Artifacts with type filter should work");

    // Assertions to drive implementation
    assert!(
        !artifacts.is_empty(),
        "Should retrieve at least some artifacts of specified type"
    );

    Ok(())
}
