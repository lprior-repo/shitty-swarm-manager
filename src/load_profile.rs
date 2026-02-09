use crate::cli::OutputFormat;
use crate::output::emit_output;
use futures_util::stream::{self, StreamExt};
use serde_json::json;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use swarm::{AgentId, RepoId, Result, SwarmDb};
use tokio::time::Duration;
use uuid::Uuid;

#[derive(Default, Clone)]
struct Metrics {
    successful_claims: u64,
    empty_claims: u64,
    timeout_count: u64,
    error_count: u64,
    latencies_ms: Vec<u128>,
}

#[derive(Clone)]
struct ProfilePlan {
    agents: u32,
    rounds: u32,
    timeout_ms: u64,
}

pub async fn run_load_profile(
    db: &SwarmDb,
    agents: u32,
    rounds: u32,
    timeout_ms: u64,
    output: &OutputFormat,
) -> Result<()> {
    let plan = ProfilePlan {
        agents,
        rounds,
        timeout_ms,
    };

    db.seed_idle_agents(plan.agents).await?;
    db.enqueue_backlog_batch(
        &format!("load-{}", Uuid::new_v4()),
        plan.agents * plan.rounds,
    )
    .await?;

    let in_flight = Arc::new(AtomicUsize::new(0));
    let max_in_flight = Arc::new(AtomicUsize::new(0));

    let totals = run_rounds_recursive(
        db,
        &plan,
        0,
        Metrics::default(),
        in_flight.clone(),
        max_in_flight.clone(),
    )
    .await?;

    let percentiles = compute_percentiles(&totals.latencies_ms);
    let recommendation = recommend_limits(
        plan.agents,
        totals.timeout_count,
        totals.error_count,
        percentiles.p95_ms,
    );

    let payload = json!({
        "agents": plan.agents,
        "rounds": plan.rounds,
        "timeouts": totals.timeout_count,
        "errors": totals.error_count,
        "successful_claims": totals.successful_claims,
        "empty_claims": totals.empty_claims,
        "latency_ms": {
            "p50": percentiles.p50_ms,
            "p95": percentiles.p95_ms,
            "p99": percentiles.p99_ms,
        },
        "max_in_flight": max_in_flight.load(Ordering::Relaxed) as u64,
        "recommended": {
            "swarm_db_max_connections": recommendation.recommended_max_connections,
            "agent_concurrency_cap": recommendation.recommended_agent_cap,
            "reason": recommendation.reason,
        },
        "message": format!(
            "load profile complete: p95={}ms, timeouts={}, errors={}",
            percentiles.p95_ms, totals.timeout_count, totals.error_count
        ),
    });

    emit_output(output, "load-profile", payload);
    Ok(())
}

fn run_rounds_recursive<'a>(
    db: &'a SwarmDb,
    plan: &'a ProfilePlan,
    round: u32,
    acc: Metrics,
    in_flight: Arc<AtomicUsize>,
    max_in_flight: Arc<AtomicUsize>,
) -> Pin<Box<dyn Future<Output = Result<Metrics>> + Send + 'a>> {
    Box::pin(async move {
        if round >= plan.rounds {
            Ok(acc)
        } else {
            let round_metrics = run_one_round(
                db,
                plan.agents,
                Duration::from_millis(plan.timeout_ms),
                in_flight.clone(),
                max_in_flight.clone(),
            )
            .await;
            run_rounds_recursive(
                db,
                plan,
                round + 1,
                merge_metrics(acc, round_metrics),
                in_flight,
                max_in_flight,
            )
            .await
        }
    })
}

async fn run_one_round(
    db: &SwarmDb,
    agents: u32,
    timeout: Duration,
    in_flight: Arc<AtomicUsize>,
    max_in_flight: Arc<AtomicUsize>,
) -> Metrics {
    stream::iter(1..=agents)
        .map(|agent_num| {
            let db = db.clone();
            let in_flight = in_flight.clone();
            let max_in_flight = max_in_flight.clone();
            async move {
                let now = Instant::now();
                let current = in_flight.fetch_add(1, Ordering::Relaxed) + 1;
                max_in_flight.fetch_max(current, Ordering::Relaxed);

                let outcome = tokio::time::timeout(
                    timeout,
                    db.claim_next_bead(&AgentId::new(RepoId::new("local"), agent_num)),
                )
                .await;

                let _ = in_flight.fetch_sub(1, Ordering::Relaxed);
                (now.elapsed().as_millis(), outcome)
            }
        })
        .buffer_unordered(agents as usize)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .fold(Metrics::default(), |acc, (latency_ms, outcome)| {
            classify_outcome(acc, latency_ms, outcome)
        })
}

fn classify_outcome(
    mut acc: Metrics,
    latency_ms: u128,
    outcome: std::result::Result<Result<Option<swarm::BeadId>>, tokio::time::error::Elapsed>,
) -> Metrics {
    match outcome {
        Ok(Ok(Some(_))) => {
            acc.successful_claims = acc.successful_claims.saturating_add(1);
            acc.latencies_ms.push(latency_ms);
            acc
        }
        Ok(Ok(None)) => {
            acc.empty_claims = acc.empty_claims.saturating_add(1);
            acc.latencies_ms.push(latency_ms);
            acc
        }
        Ok(Err(_)) => {
            acc.error_count = acc.error_count.saturating_add(1);
            acc
        }
        Err(_) => {
            acc.timeout_count = acc.timeout_count.saturating_add(1);
            acc
        }
    }
}

fn merge_metrics(left: Metrics, right: Metrics) -> Metrics {
    Metrics {
        successful_claims: left
            .successful_claims
            .saturating_add(right.successful_claims),
        empty_claims: left.empty_claims.saturating_add(right.empty_claims),
        timeout_count: left.timeout_count.saturating_add(right.timeout_count),
        error_count: left.error_count.saturating_add(right.error_count),
        latencies_ms: left
            .latencies_ms
            .into_iter()
            .chain(right.latencies_ms)
            .collect::<Vec<_>>(),
    }
}

#[derive(Debug, Clone)]
struct Percentiles {
    p50_ms: u128,
    p95_ms: u128,
    p99_ms: u128,
}

fn compute_percentiles(values: &[u128]) -> Percentiles {
    if values.is_empty() {
        return Percentiles {
            p50_ms: 0,
            p95_ms: 0,
            p99_ms: 0,
        };
    }

    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let len = sorted.len();
    let p50 = sorted[(len.saturating_sub(1)) * 50 / 100];
    let p95 = sorted[(len.saturating_sub(1)) * 95 / 100];
    let p99 = sorted[(len.saturating_sub(1)) * 99 / 100];

    Percentiles {
        p50_ms: p50,
        p95_ms: p95,
        p99_ms: p99,
    }
}

#[derive(Debug, Clone)]
struct Recommendation {
    recommended_max_connections: u32,
    recommended_agent_cap: u32,
    reason: String,
}

fn recommend_limits(agents: u32, timeouts: u64, errors: u64, p95_ms: u128) -> Recommendation {
    let degraded = timeouts > 0 || errors > 0 || p95_ms > 300;
    if degraded {
        Recommendation {
            recommended_max_connections: (agents / 6).max(8),
            recommended_agent_cap: (agents * 2 / 3).max(8),
            reason: "timeouts/errors/high p95 detected; reduce concurrency and pool pressure"
                .to_string(),
        }
    } else {
        Recommendation {
            recommended_max_connections: (agents / 4).max(8),
            recommended_agent_cap: agents,
            reason: "no pressure signals detected under test load".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{compute_percentiles, recommend_limits};

    #[test]
    fn percentiles_are_stable_for_known_distribution() {
        let p = compute_percentiles(&[10, 20, 30, 40, 50, 60, 70, 80, 90, 100]);
        assert_eq!(p.p50_ms, 50);
        assert_eq!(p.p95_ms, 90);
        assert_eq!(p.p99_ms, 90);
    }

    #[test]
    fn recommendation_throttles_when_signals_are_bad() {
        let rec = recommend_limits(90, 1, 0, 450);
        assert!(rec.recommended_agent_cap < 90);
        assert!(rec.recommended_max_connections >= 8);
    }
}
