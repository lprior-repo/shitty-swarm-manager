#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

#[cfg(test)]
mod bdd_tests {
    use crate::cli::{parse_cli_args, CliAction, CliCommand};

    fn given_cli_args(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn when_no_args_then_run_protocol() {
        let args = given_cli_args(&[]);
        let action = parse_cli_args(&args).expect("parse");

        assert!(matches!(action, CliAction::RunProtocol));
    }

    #[test]
    fn when_help_flag_then_show_help() {
        let args = given_cli_args(&["-h"]);
        let action = parse_cli_args(&args).expect("parse");

        assert!(matches!(action, CliAction::ShowHelp));
    }

    #[test]
    fn when_version_flag_then_show_version() {
        let args = given_cli_args(&["-v"]);
        let action = parse_cli_args(&args).expect("parse");

        assert!(matches!(action, CliAction::ShowVersion));
    }

    #[test]
    fn when_doctor_command_then_doctor_action() {
        let args = given_cli_args(&["doctor"]);
        let action = parse_cli_args(&args).expect("parse");

        assert!(matches!(action, CliAction::Command(CliCommand::Doctor)));
    }

    #[test]
    fn when_status_command_then_status_action() {
        let args = given_cli_args(&["status"]);
        let action = parse_cli_args(&args).expect("parse");

        assert!(matches!(action, CliAction::Command(CliCommand::Status)));
    }

    #[test]
    fn when_agent_command_with_id_then_agent_action_with_id() {
        let args = given_cli_args(&["agent", "--id", "5"]);
        let action = parse_cli_args(&args).expect("parse");

        match action {
            CliAction::Command(CliCommand::Agent { id, dry: _ }) => assert_eq!(id, 5),
            _ => panic!("Expected Agent command"),
        }
    }

    #[test]
    fn when_assign_command_then_assign_action_with_correct_values() {
        let args = given_cli_args(&["assign", "--bead-id", "bead-123", "--agent-id", "2"]);
        let action = parse_cli_args(&args).expect("parse");

        match action {
            CliAction::Command(CliCommand::Assign {
                bead_id,
                agent_id,
                dry: _,
            }) => {
                assert_eq!(bead_id, "bead-123");
                assert_eq!(agent_id, 2);
            }
            _ => panic!("Expected Assign command"),
        }
    }

    #[test]
    fn when_missing_required_arg_then_error() {
        let args = given_cli_args(&["assign", "--bead-id", "bead-123"]);
        let result = parse_cli_args(&args);

        assert!(result.is_err());
    }

    #[test]
    fn when_unknown_command_then_error() {
        let args = given_cli_args(&["unknown-command"]);
        let result = parse_cli_args(&args);

        assert!(result.is_err());
    }

    #[test]
    fn when_json_command_then_json_action() {
        let args = given_cli_args(&["--json", "{\"cmd\":\"doctor\"}"]);
        let action = parse_cli_args(&args).expect("parse");

        match action {
            CliAction::Command(CliCommand::Json(json)) => assert_eq!(json, "{\"cmd\":\"doctor\"}"),
            _ => panic!("Expected Json command"),
        }
    }

    #[test]
    fn when_agent_command_with_dry_flag_then_dry_is_true() {
        let args = given_cli_args(&["agent", "--id", "1", "--dry"]);
        let action = parse_cli_args(&args).expect("parse");

        match action {
            CliAction::Command(CliCommand::Agent { id: _, dry }) => assert_eq!(dry, Some(true)),
            _ => panic!("Expected Agent command"),
        }
    }
}
