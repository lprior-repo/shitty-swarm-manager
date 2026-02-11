# Martin Fowler Test Plan: Database Connection Timeout

## Happy Path Tests
- `test_connects_successfully_when_database_is_reachable_within_timeout`
- `test_uses_explicit_connect_timeout_when_provided_in_request`
- `test_falls_back_to_system_default_when_no_request_timeout_specified`
- `test_returns_successful_pool_when_connection_established_before_timeout`

## Error Path Tests (Adversarial)
- `test_fails_fast_when_connection_refused_immediately`
- `test_times_out_when_network_is_unreachable`
- `test_times_out_when_database_host_responds_but_port_is_closed`
- `test_times_out_when_dns_lookup_hangs`
- `test_returns_distinct_error_for_timeout_vs_auth_failure`
- `test_reports_actual_elapsed_time_in_timeout_error`
- `test_cleans_up_resources_after_timeout`

## Edge Case Tests (Boundary Exploration)
- `test_honors_minimum_timeout_of_100ms`
- `test_honors_maximum_timeout_of_30000ms`
- `test_clamps_negative_values_to_minimum`
- `test_clamps_excessive_values_to_maximum`
- `test_handles_zero_timeout_gracefully`
- `test_handles_null_timeout_gracefully`
- `test_handles_malformed_timeout_value_gracefully`
- `test_handles_floating_point_timeout_gracefully`
- `test_handles_string_timeout_value_gracefully`

## Contract Verification Tests
- `test_precondition_timeout_is_clamped_to_valid_range`
- `test_postcondition_connection_fails_within_timeout_plus_tolerance`
- `test_invariant_pool_options_always_has_connect_timeout_configured`
- `test_invariant_timeout_errors_are_distinguishable`
- `test_invariant_timing_metrics_accurate_for_failures`

## Given-When-Then Scenarios

### Scenario 1: Request timeout is respected for unreachable host
**Given**: A request with `connect_timeout_ms: 100` and `database_url: postgresql://localhost:1/notreal`  
**When**: The system attempts to establish a database connection  
**Then**:
- The connection attempt fails within 600ms (100ms + 500ms tolerance)
- The error indicates a timeout occurred
- The timing metric shows actual elapsed time ≈ 100ms

### Scenario 2: System default timeout is used when not specified
**Given**: A request with no `connect_timeout_ms` and unreachable database  
**When**: The system attempts to establish a database connection  
**Then**:
- The system default of 3000ms is used
- The connection attempt fails within 3500ms (3000ms + 500ms tolerance)

### Scenario 3: Invalid URL fails immediately without timeout
**Given**: A request with `database_url: not-a-valid-url`  
**When**: The system attempts to establish a database connection  
**Then**:
- The request fails immediately (< 50ms)
- The error indicates invalid URL format
- No network timeout occurs

### Scenario 4: Active refusal fails immediately
**Given**: A request to `postgresql://127.0.0.1:1/test` where port 1 is closed  
**When**: The system attempts to establish a database connection  
**Then**:
- The request fails within the timeout budget
- The error indicates connection refused or timeout
- The response time is bounded by the configured timeout

## Adversarial Regression Tests

### Test: Timeout should not drift
```rust
// Verifies that timeout is consistent across multiple attempts
for i in 0..100 {
    let start = Instant::now();
    let result = connect_with_timeout(UNREACHABLE_URL, 100).await;
    let elapsed = start.elapsed().as_millis();
    assert!(elapsed < 200, "Attempt {} exceeded timeout: {}ms", i, elapsed);
}
```

### Test: Concurrent timeout attempts
```rust
// Verifies that multiple concurrent connection attempts don't interfere
let futures = (0..10).map(|_| {
    connect_with_timeout(UNREACHABLE_URL, 100)
});
let results = join_all(futures).await;
for result in results {
    assert!(result.is_err());
    // Each should complete within timeout budget
}
```

### Test: Timeout with connection pooling
```rust
// Verifies that pool-level timeout doesn't accumulate across connections
let pool = create_pool_with_timeout(UNREACHABLE_URL, 100).await?;
// Pool creation should fail within timeout, not after multiple attempts
```

### Test: Resource exhaustion
```rust
// Verifies that timed-out attempts don't leak connections
for _ in 0..1000 {
    let _ = connect_with_timeout(UNREACHABLE_URL, 10).await;
}
// System should still be responsive
```
