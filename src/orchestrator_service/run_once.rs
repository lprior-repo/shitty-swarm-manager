use super::ports::PortFuture;
use super::timing::elapsed_ms;
use crate::Result;
use serde_json::Value;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct RunOnceResult {
    pub agent_id: u32,
    pub doctor: Value,
    pub status_before: Value,
    pub claim_next: Value,
    pub agent: Value,
    pub progress: Value,
    pub doctor_ms: u64,
    pub status_before_ms: u64,
    pub claim_next_ms: u64,
    pub agent_ms: u64,
    pub progress_ms: u64,
}

pub trait RunOncePorts {
    fn doctor(&self) -> PortFuture<'_, Value>;
    fn status(&self) -> PortFuture<'_, Value>;
    fn claim_next(&self) -> PortFuture<'_, Value>;
    fn run_agent(&self, agent_id: u32) -> PortFuture<'_, Value>;
    fn monitor_progress(&self) -> PortFuture<'_, Value>;
}

pub struct RunOnceAppService<P> {
    ports: P,
}

impl<P> RunOnceAppService<P>
where
    P: RunOncePorts + Sync,
{
    #[must_use]
    pub const fn new(ports: P) -> Self {
        Self { ports }
    }

    /// Execute one compact orchestration run-once sequence.
    ///
    /// # Errors
    /// Returns an error when any constituent command port fails.
    pub async fn execute(&self, agent_id: u32) -> Result<RunOnceResult> {
        let doctor_start = Instant::now();
        let doctor = self.ports.doctor().await?;
        let doctor_ms = elapsed_ms(doctor_start);

        let status_before_start = Instant::now();
        let status_before = self.ports.status().await?;
        let status_before_ms = elapsed_ms(status_before_start);

        let claim_start = Instant::now();
        let claim_next = self.ports.claim_next().await?;
        let claim_next_ms = elapsed_ms(claim_start);

        let agent_start = Instant::now();
        let agent = self.ports.run_agent(agent_id).await?;
        let agent_ms = elapsed_ms(agent_start);

        let progress_start = Instant::now();
        let progress = self.ports.monitor_progress().await?;
        let progress_ms = elapsed_ms(progress_start);

        Ok(RunOnceResult {
            agent_id,
            doctor,
            status_before,
            claim_next,
            agent,
            progress,
            doctor_ms,
            status_before_ms,
            claim_next_ms,
            agent_ms,
            progress_ms,
        })
    }
}
