use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestResults {
    pub passed: u32,
    pub failed: u32,
    pub skipped: u32,
    pub total: u32,
    pub failures: Vec<TestFailure>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestFailure {
    pub name: String,
    pub file: Option<String>,
    pub line: Option<u32>,
    pub reason: String,
}

#[must_use]
pub fn parse_test_results(output: &str) -> TestResults {
    let result_line = output.lines().find(|line| line.contains("test result:"));
    let (passed, failed) = result_line
        .and_then(|line| {
            let normalized = line.replace([';', '.'], "");
            let words: Vec<&str> = normalized.split_whitespace().collect();
            let passed = words
                .iter()
                .position(|w| *w == "passed")
                .and_then(|idx| words.get(idx.saturating_sub(1)))
                .and_then(|s| s.parse::<u32>().ok());
            let failed = words
                .iter()
                .position(|w| *w == "failed")
                .and_then(|idx| words.get(idx.saturating_sub(1)))
                .and_then(|s| s.parse::<u32>().ok());
            match (passed, failed) {
                (Some(p), Some(f)) => Some((p, f)),
                _ => None,
            }
        })
        .map_or((0, 0), |(p, f)| (p, f));

    let failures = output
        .lines()
        .filter(|line| line.contains("FAILED") || line.contains("Error:"))
        .map(|line| TestFailure {
            name: line
                .split_whitespace()
                .nth(1)
                .map_or_else(|| String::from("unknown"), str::to_string),
            file: None,
            line: None,
            reason: "See test output".to_string(),
        })
        .collect::<Vec<_>>();

    TestResults {
        passed,
        failed,
        skipped: 0,
        total: passed.saturating_add(failed),
        failures,
    }
}

#[cfg(test)]
mod tests {
    use super::parse_test_results;

    #[test]
    fn parse_cargo_test_results() {
        let output = r"
running 3 tests
test tests::test_one ... ok
test tests::test_two ... FAILED
test tests::test_three ... ok

test result: ok. 2 passed; 1 failed; 0 skipped
";
        let results = parse_test_results(output);
        assert_eq!(results.passed, 2);
        assert_eq!(results.failed, 1);
        assert_eq!(results.total, 3);
    }
}
