# Contract Refactor Plan: Type-Safe CLI

## Goal

Replace runtime JSON parsing with compile-time type contracts.

## Status

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Contract types (24 commands) | ✅ DONE |
| 2 | CLI argument parser | Pending |
| 3 | ParseInput trait | Pending |
| 4 | Handler refactor | Pending |
| 5 | Tests | Pending |

## Target Pattern

```rust
// Before (runtime)
let id = request.args.get("id").and_then(Value::as_u64)?;

// After (compile-time)
let input: AgentInput = parse_input(request)?;
let id = input.id;  // Already validated as u32
```

## Benefits

- **Compile-time safety** - Wrong types caught at compile time
- **Better errors** - Clear messages at CLI boundary
- **Auto-completion** - Structs enable shell completion
- **Refactoring** - Change contract, compiler finds all uses

## Next Steps

1. Update `parse_cli_args()` in `main.rs` to use `Result<CliAction, CliError>`
2. Add `CliCommand` enum with all 24 command variants
3. Implement `ParseInput` trait for each handler
4. Refactor handlers one by one
