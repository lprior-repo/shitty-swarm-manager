# Contract Refactor Plan: Type-Safe CLI for Swarm

## Overview
Refactor the CLI to use compile-time type contracts instead of runtime JSON parsing.

## Current State ❌
```rust
// Loose runtime parsing
let id = request.args.get("id").and_then(Value::as_u64)
    .ok_or_else(|| Box::new(ProtocolEnvelope::error(..., "Missing id")))?
```

## Target State ✅
```rust
// Compile-time type safety
let input: AgentInput = parse_input(request)?;
let id = input.id;  // Already validated as u32
```

---

## Phase 1: Contract Types ✅ (DONE)
**File**: `src/contracts.rs`

All 24 commands have defined Input/Output contracts:
- Doctor, Help, Status, Agent, Init, Register, Release
- Monitor, InitDb, InitLocalDb, Bootstrap, SpawnPrompts
- Prompt, Smoke, Batch, State, History, Lock, Unlock
- Agents, Broadcast, LoadProfile

Plus `CliError` enum for helpful error messages.

---

## Phase 2: CLI Argument Parser

### 2.1 Update `parse_cli_args` in `main.rs`

**Before**:
```rust
fn parse_cli_args(args: &[String]) -> CliAction {
    match args.first().map(String::as_str) {
        Some("doctor") => CliAction::RunCommand("doctor".into(), vec![]),
        Some(cmd) => CliAction::RunCommand(cmd.to_string(), pairs),
    }
}
```

**After**:
```rust
fn parse_cli_args(args: &[String]) -> Result<CliAction, CliError> {
    match args.first().map(String::as_str) {
        None | Some("--") => Ok(CliAction::RunProtocol),
        Some("-h") | Some("--help") => Ok(CliAction::ShowHelp),
        Some("-v") | Some("--version") => Ok(CliAction::ShowVersion),

        // Commands with no args
        Some("doctor") => Ok(CliAction::Command(CliCommand::Doctor)),
        Some("status") => Ok(CliAction::Command(CliCommand::Status)),
        Some("?") => Ok(CliAction::Command(CliCommand::Help)),

        // Commands with required args
        Some("agent") => {
            let id = parse_required_arg(&args, "id")?;
            let dry = parse_optional_arg(&args, "dry")?;
            Ok(CliAction::Command(CliCommand::Agent { id, dry }))
        }

        Some("init") => {
            let dry = parse_optional_arg(&args, "dry")?;
            let database_url = parse_optional_arg(&args, "database_url")?;
            let schema = parse_optional_arg(&args, "schema")?;
            let seed_agents = parse_optional_arg(&args, "seed_agents")?;
            Ok(CliAction::Command(CliCommand::Init {
                dry,
                database_url,
                schema,
                seed_agents,
            }))
        }

        Some(cmd) => Err(CliError::UnknownCommand {
            command: cmd.to_string(),
            suggestions: suggest_commands(cmd),
        }),
    }
}
```

### 2.2 Define CliCommand Enum

```rust
#[derive(Debug, Clone)]
enum CliCommand {
    Doctor,
    Help { short: Option<bool> },
    Status,
    Agent { id: u32, dry: Option<bool> },
    Init {
        dry: Option<bool>,
        database_url: Option<String>,
        schema: Option<String>,
        seed_agents: Option<u32>,
    },
    Register { count: Option<u32>, dry: Option<bool> },
    Release { agent_id: u32, dry: Option<bool> },
    Monitor { view: Option<String>, watch_ms: Option<u64> },
    InitDb {
        url: Option<String>,
        schema: Option<String>,
        seed_agents: Option<u32>,
        dry: Option<bool>,
    },
    // ... all 24 commands
}
```

### 2.3 Helper Functions

```rust
fn parse_required_arg<T>(args: &[String], name: &str) -> Result<T, CliError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    args.iter()
        .position(|a| a == format!("--{}", name.replace('_', "-")))
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse::<T>().ok())
        .ok_or_else(|| CliError::MissingRequiredArg {
            arg_name: name.to_string(),
            usage: format!("--{} <value>", name.replace('_', "-")),
        })
}

fn parse_optional_arg<T>(args: &[String], name: &str) -> Result<Option<T>, CliError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    Ok(args
        .iter()
        .position(|a| a == format!("--{}", name.replace('_', "-")))
        .and_then(|i| args.get(i + 1))
        .map(|v| v.parse::<T>())
        .transpose()
        .map_err(|e| CliError::InvalidArgValue {
            arg_name: name.to_string(),
            value: format!("{:?}", e),
            expected: std::any::type_name::<T>().to_string(),
        })?)
}

fn suggest_commands(typo: &str) -> Vec<String> {
    const VALID_COMMANDS: &[&str] = &[
        "doctor", "help", "status", "agent", "init", "register",
        "release", "monitor", "init-db", "init-local-db", "bootstrap",
        "spawn-prompts", "prompt", "smoke", "batch", "state", "history",
        "lock", "unlock", "agents", "broadcast", "load-profile",
    ];

    VALID_COMMANDS
        .iter()
        .map(|cmd| (cmd, strsim::levenshtein(typo, cmd)))
        .filter(|(_, dist)| *dist <= 3)
        .min_by_key(|(_, dist)| *dist)
        .map(|(cmd, _)| vec![cmd.to_string()])
        .unwrap_or_default()
}
```

---

## Phase 3: Protocol Runtime Refactor

### 3.1 Add Parse Trait

```rust
trait ParseInput {
    type Input;
    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, Box<ProtocolEnvelope>>;
}

// Implement for all command handlers
impl ParseInput for DoctorInput {
    type Input = DoctorInput;

    fn parse_input(request: &ProtocolRequest) -> Result<Self::Input, Box<ProtocolEnvelope>> {
        Ok(DoctorInput {
            json: request.args.get("json").and_then(|v| v.as_bool()),
        })
    }
}
```

### 3.2 Update Handlers

**Before**:
```rust
async fn handle_agent(request: &ProtocolRequest) -> Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let id = request.args.get("id")
        .and_then(Value::as_u64)
        .ok_or_else(|| Box::new(ProtocolEnvelope::error(...)))? as u32;
    // ...
}
```

**After**:
```rust
async fn handle_agent(request: &ProtocolRequest) -> Result<CommandSuccess, Box<ProtocolEnvelope>> {
    let input: AgentInput = AgentInput::parse_input(request)?;
    let dry = input.dry.unwrap_or(false);

    if dry {
        return Ok(dry_run_success(
            request,
            vec![json!({"step": 1, "action": "run_agent", "target": input.id})],
            "swarm status",
        ));
    }

    // ... rest of logic with input.id
}
```

---

## Phase 4: Execution Flow

```
CLI Input
    ↓
parse_cli_args() → Result<CliAction, CliError>
    ↓                    (fails fast at CLI boundary)
CliAction::Command
    ↓
Convert to ProtocolRequest
    ↓
process_protocol_line()
    ↓
handle_<cmd>()
    ↓
ParseInput trait → Result<Input, Box<ProtocolEnvelope>>
    ↓
Execute business logic
    ↓
Return Output contract
    ↓
Serialize to JSONL
```

---

## Phase 5: Error Messages

### Before ❌
```json
{"ok":false,"err":{"msg":"Missing id"}}
```

### After ✅
```bash
$ swarm agent
Error: Missing required argument: id
Usage: swarm agent --id <number>

$ swarm agnet
Error: Unknown command: 'agnet'
Did you mean: agent?
```

---

## Implementation Order

1. ✅ **Contract types** - DONE
2. **CLI parser** - `main.rs` parse_cli_args
3. **ParseInput trait** - Generic parsing logic
4. **Refactor handlers** - Update one by one
5. **Tests** - Verify each command
6. **Documentation** - Update help text

---

## Benefits

✅ **Compile-time safety** - Wrong types caught at compile time
✅ **Better errors** - Clear messages at CLI boundary
✅ **Auto-completion** - Structs enable shell completion
✅ **Documentation** - Contracts serve as API spec
✅ **Refactoring** - Change contract, compiler finds all uses
✅ **Testing** - Easy to construct test inputs
