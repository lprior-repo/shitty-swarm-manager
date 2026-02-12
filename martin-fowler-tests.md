# Martin Fowler Test Plan: Database Connection Timeout

## Contract

- Default timeout: 3000ms
- Min timeout: 100ms
- Max timeout: 30000ms

## Given-When-Then Scenarios

### Success
- **Given** reachable DB within timeout → **When** connect → **Then** success

### Timeout
- **Given** unreachable host + 100ms timeout → **When** connect → **Then** fail within 600ms

### Edge Cases
- Zero timeout → clamp to minimum
- Negative timeout → clamp to minimum
- Excessive timeout → clamp to maximum
- Invalid URL → fail immediately (no timeout)

## Invariants

- Connection fails within `timeout + tolerance`
- Timeout errors are distinguishable from auth failures
- No resource leaks on timeout
