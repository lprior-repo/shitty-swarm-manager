use super::super::{
    check_command, check_database_connectivity, minimal_state_for_request, CommandSuccess,
    ProtocolRequest,
};
use crate::protocol_envelope::ProtocolEnvelope;
use serde_json::{json, Value};
use std::time::Instant;

pub(in crate::protocol_runtime) async fn handle_doctor(
    request: &ProtocolRequest,
) -> std::result::Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let total_start = Instant::now();
    let moon_start = Instant::now();
    let moon = check_command("moon").await;
    let moon_ms = elapsed_ms(moon_start);
    let br_start = Instant::now();
    let br = check_command("br").await;
    let br_ms = elapsed_ms(br_start);
    let jj_start = Instant::now();
    let jj = check_command("jj").await;
    let jj_ms = elapsed_ms(jj_start);
    let zjj_start = Instant::now();
    let zjj = check_command("zjj").await;
    let zjj_ms = elapsed_ms(zjj_start);
    let psql_start = Instant::now();
    let psql = check_command("psql").await;
    let psql_ms = elapsed_ms(psql_start);
    let database_start = Instant::now();
    let database = check_database_connectivity(request).await;
    let database_ms = elapsed_ms(database_start);
    let mut checks = vec![moon, br, jj, zjj, psql];
    checks.push(database);
    let failed = checks
        .iter()
        .filter(|check| !check["ok"].as_bool().is_some_and(|value| value))
        .count() as i64;
    let passed = checks.len() as i64 - failed;
    let check_results: Vec<Value> = checks
        .iter()
        .map(|check| {
            json!({
                "n": check["name"],
                "ok": check["ok"]
            })
        })
        .collect();

    Ok(CommandSuccess {
        data: json!({
            "v": "v1",
            "h": failed == 0,
            "p": passed,
            "f": failed,
            "c": check_results,
            "timing": {
                "checks_ms": {
                    "moon": moon_ms,
                    "br": br_ms,
                    "jj": jj_ms,
                    "zjj": zjj_ms,
                    "psql": psql_ms,
                    "database": database_ms,
                },
                "total_ms": elapsed_ms(total_start),
            }
        }),
        next: if failed == 0 {
            "swarm state".to_string()
        } else {
            "swarm doctor".to_string()
        },
        state: minimal_state_for_request(request).await,
    })
}

fn elapsed_ms(start: Instant) -> u64 {
    let ms = start.elapsed().as_millis();
    u64::try_from(ms).map_or(u64::MAX, |value| value)
}
