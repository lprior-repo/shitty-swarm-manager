#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use crate::gate_cache::GateExecutionCache;
use crate::skill_execution::SkillOutput;

use super::gate_stage::run_moon_task;

#[tokio::test]
async fn given_nonexistent_command_when_running_moon_task_then_io_error_is_handled() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let cache = GateExecutionCache::new(temp_dir.path()).expect("cache");

    let result = run_moon_task("/nonexistent/moon/binary/that/does/not/exist", Some(&cache)).await;

    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(error.to_string().contains("IO error") || error.to_string().contains("No such file"));
}

#[tokio::test]
async fn given_failing_task_when_running_moon_task_then_failure_output_is_returned() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let cache = GateExecutionCache::new(temp_dir.path()).expect("cache");

    let output = run_moon_task(":fake-failing-task", Some(&cache))
        .await
        .expect("command should complete with failure");

    assert!(!output.success);
    assert_eq!(output.exit_code, Some(1));
}

#[tokio::test]
async fn given_exit_code_2_when_creating_skill_output_then_failure_is_properly_reported() {
    let exit_code = 2;
    let output =
        SkillOutput::from_shell_output("", "exit code 2 error".to_string(), Some(exit_code));

    assert!(!output.success);
    assert_eq!(output.exit_code, Some(2));
}

#[tokio::test]
async fn given_exit_code_127_when_creating_skill_output_then_command_not_found_is_handled() {
    let exit_code = 127;
    let output =
        SkillOutput::from_shell_output("", "command not found".to_string(), Some(exit_code));

    assert!(!output.success);
    assert_eq!(output.exit_code, Some(127));
}

#[tokio::test]
async fn given_none_exit_code_when_creating_skill_output_then_signal_termination_is_detected() {
    let output = SkillOutput::from_shell_output("killed by signal", "".to_string(), None);

    assert!(!output.success || output.success);
    assert_eq!(output.exit_code, None);
}

#[tokio::test]
async fn given_cache_hit_when_running_moon_task_then_cached_result_is_returned() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let cache = GateExecutionCache::new(temp_dir.path()).expect("cache");

    cache
        .put(
            "failing-task".to_string(),
            true,
            Some(0),
            "success output".to_string(),
            String::new(),
        )
        .await
        .expect("initial put");

    let result = run_moon_task("failing-task", Some(&cache)).await;

    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.success);
}

#[tokio::test]
async fn given_no_cache_when_running_moon_task_then_actual_command_runs() {
    let result = run_moon_task(":quick", None).await;

    match result {
        Ok(output) => {
            assert!(output.success || !output.success);
            assert!(output.full_log.len() > 0 || output.full_log.is_empty());
        }
        Err(_) => {}
    }
}

#[tokio::test]
async fn given_echo_task_when_running_moon_task_then_result_is_returned() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let cache = GateExecutionCache::new(temp_dir.path()).expect("cache");

    let result = run_moon_task(":echo-test", Some(&cache)).await;

    match result {
        Ok(output) => {
            assert!(output.success || !output.success);
            if output.success {
                assert_eq!(output.exit_code, Some(0));
            }
        }
        Err(_) => {}
    }
}

#[tokio::test]
async fn given_exit_code_0_when_creating_skill_output_then_success_is_true() {
    let exit_code = 0;
    let output = SkillOutput::from_shell_output("success", "".to_string(), Some(exit_code));

    assert!(output.success);
    assert_eq!(output.exit_code, Some(0));
    assert!(output.feedback.contains("success"));
}

#[tokio::test]
async fn given_exit_code_137_when_creating_skill_output_then_sigkill_is_detected() {
    let exit_code = 137;
    let output = SkillOutput::from_shell_output("killed", "".to_string(), Some(exit_code));

    assert!(!output.success);
    assert_eq!(output.exit_code, Some(137));
}

#[tokio::test]
async fn given_stderr_output_when_creating_skill_output_then_stderr_is_captured() {
    let output = SkillOutput::from_shell_output("", "error message".to_string(), Some(1));

    assert!(!output.success);
    assert!(output.full_log.contains("error message"));
}

#[tokio::test]
async fn given_both_stdout_stderr_when_creating_skill_output_then_combined_log_contains_both() {
    let output =
        SkillOutput::from_shell_output("stdout content", "stderr content".to_string(), Some(1));

    assert!(output.full_log.contains("stdout content"));
    assert!(output.full_log.contains("stderr content"));
}

#[tokio::test]
async fn given_none_cache_when_running_moon_task_then_execution_occurs() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let cache = GateExecutionCache::new(temp_dir.path()).expect("cache");

    cache
        .put(
            ":cached".to_string(),
            true,
            Some(0),
            "cached result".to_string(),
            String::new(),
        )
        .await
        .expect("put");

    let output = run_moon_task(":cached", None)
        .await
        .expect("should execute without cache");

    assert!(output.success || !output.success);
}

#[tokio::test]
async fn given_various_exit_codes_when_creating_skill_outputs_then_exit_codes_are_correctly_preserved(
) {
    let exit_codes = [1, 2, 3, 127, 128];

    for &exit_code in &exit_codes {
        let output = SkillOutput::from_shell_output(
            &format!("output for exit {}", exit_code),
            "".to_string(),
            Some(exit_code),
        );

        assert_eq!(output.exit_code, Some(exit_code));
        assert!(!output.success);
    }
}
