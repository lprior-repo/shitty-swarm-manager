mod agent_runtime;
mod agent_runtime_support;
mod cli;
mod commands;
mod commands_support;
mod config;
mod load_profile;
mod monitor;
mod output;

use swarm::{Result, SwarmError};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let output_format = output_format_from_args(std::env::args().collect());

    let code = match run().await {
        Ok(()) => 0,
        Err(err) => {
            output::emit_error(&output_format, &err);
            output::map_error_to_exit_code(&err)
        }
    };

    std::process::exit(code);
}

async fn run() -> Result<()> {
    commands::run().await
}

fn output_format_from_args(args: Vec<String>) -> cli::OutputFormat {
    args.iter()
        .enumerate()
        .find_map(|(idx, arg)| {
            if arg == "--output" {
                args.get(idx + 1).map(String::as_str)
            } else {
                arg.strip_prefix("--output=")
            }
        })
        .map_or(cli::OutputFormat::Json, |value| {
            if value.eq_ignore_ascii_case("text") {
                cli::OutputFormat::Text
            } else {
                cli::OutputFormat::Json
            }
        })
}

#[allow(dead_code)]
fn _assert_error_type(_: &SwarmError) {}

#[cfg(test)]
mod tests {
    use super::output_format_from_args;
    use crate::cli::OutputFormat;

    #[test]
    fn output_format_defaults_to_json() {
        let args = vec!["swarm".to_string(), "init".to_string()];
        assert_eq!(output_format_from_args(args), OutputFormat::Json);
    }

    #[test]
    fn output_format_honors_text_flag() {
        let args = vec![
            "swarm".to_string(),
            "init".to_string(),
            "--output".to_string(),
            "text".to_string(),
        ];
        assert_eq!(output_format_from_args(args), OutputFormat::Text);
    }

    #[test]
    fn output_format_honors_equals_syntax() {
        let args = vec![
            "swarm".to_string(),
            "init".to_string(),
            "--output=text".to_string(),
        ];
        assert_eq!(output_format_from_args(args), OutputFormat::Text);
    }
}
