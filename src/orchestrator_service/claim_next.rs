use super::ports::PortFuture;
use super::timing::elapsed_ms;
use crate::Result;
use serde_json::Value;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct ClaimNextResult {
    pub recommendation: Value,
    pub bead_id: String,
    pub claim: Value,
    pub bv_robot_next_ms: u64,
    pub br_update_ms: u64,
}

pub trait ClaimNextPorts {
    fn bv_robot_next(&self) -> PortFuture<'_, Value>;
    fn br_update_in_progress<'a>(&'a self, bead_id: &'a str) -> PortFuture<'a, Value>;
}

pub struct ClaimNextAppService<P> {
    ports: P,
}

impl<P> ClaimNextAppService<P>
where
    P: ClaimNextPorts + Sync,
{
    #[must_use]
    pub const fn new(ports: P) -> Self {
        Self { ports }
    }

    /// Execute one claim-next orchestration cycle through external ports.
    ///
    /// # Errors
    /// Returns an error when recommendation retrieval fails, the recommendation
    /// payload does not contain a bead id, or claim update fails.
    pub async fn execute<F>(&self, bead_id_from_recommendation: F) -> Result<ClaimNextResult>
    where
        F: Fn(&Value) -> Option<String>,
    {
        let recommendation_start = Instant::now();
        let recommendation_payload = self.ports.bv_robot_next().await?;
        let bv_robot_next_ms = elapsed_ms(recommendation_start);
        let recommendation = recommendation_payload
            .get("next")
            .cloned()
            .unwrap_or(recommendation_payload);
        let bead_id = bead_id_from_recommendation(&recommendation).ok_or_else(|| {
            crate::Error::ConfigError("missing bead id in recommendation".to_string())
        })?;

        let update_start = Instant::now();
        let claim = self.ports.br_update_in_progress(&bead_id).await?;
        let br_update_ms = elapsed_ms(update_start);

        Ok(ClaimNextResult {
            recommendation,
            bead_id,
            claim,
            bv_robot_next_ms,
            br_update_ms,
        })
    }
}
