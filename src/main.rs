mod agent_runtime;
mod cli;
mod commands;
mod config;
mod load_profile;
mod monitor;
mod output;

use swarm::{Result, SwarmError};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let code = match run().await {
        Ok(()) => 0,
        Err(err) => {
            eprintln!("Error: {}", err);
            output::map_error_to_exit_code(&err)
        }
    };

    std::process::exit(code);
}

async fn run() -> Result<()> {
    commands::run().await
}

#[allow(dead_code)]
fn _assert_error_type(_: &SwarmError) {}
