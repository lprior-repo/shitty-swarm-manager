mod agent_runtime;
mod agent_runtime_support;
mod config;
mod protocol_runtime;

use swarm::{Result, SwarmError};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let code = match run().await {
        Ok(()) => 0,
        Err(err) => {
            eprintln!("{}", err);
            map_error_to_exit_code(&err)
        }
    };

    std::process::exit(code);
}

async fn run() -> Result<()> {
    protocol_runtime::run_protocol_loop().await
}

fn map_error_to_exit_code(error: &SwarmError) -> i32 {
    match error {
        SwarmError::ConfigError(_) => 2,
        SwarmError::DatabaseError(_) => 3,
        SwarmError::AgentError(_) => 4,
        SwarmError::BeadError(_) => 5,
        SwarmError::StageError(_) => 6,
        SwarmError::IoError(_) => 7,
        SwarmError::SerializationError(_) => 8,
    }
}
