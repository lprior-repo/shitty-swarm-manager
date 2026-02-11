# CLAUDE.md - Project Instructions for Claude Code

## CRITICAL: Use Codanna for Code Exploration

**MANDATORY: codanna MCP server is your PRIMARY code exploration tool.** Use it for ALL searching, symbol lookup, impact analysis, and understanding codebase structure.

### Why Codanna?

- **Token efficient**: Returns structured context, not raw file dumps
- **Instant results**: Pre-indexed codebase, no grep lag
- **Complete picture**: Shows callers, callees, types, dependencies in ONE call
- **Symbol-level precision**: Knows about functions, structs, traits, not just text

### Codanna Status for This Project

✅ **Indexed and Ready**: 302 symbols across 15 files
- Location: `.codanna/index`
- Semantic search: Enabled (AllMiniLML6V2)
- Relationships: 452 resolved

### How to Use Codanna (Workflow)

```bash
# STEP 1: Start with semantic search (gets full context)
mcp__codanna__semantic_search_with_context "database operations" --limit 5

# STEP 2: Find exact symbols
mcp__codanna__find_symbol "AgentRuntime"

# STEP 3: Analyze impact (see complete dependency graph)
mcp__codanna__analyze_impact --symbol_id 12345

# STEP 4: Get specific details
mcp__codanna__get_calls --function_name "spawn_agent"
mcp__codanna__find_callers --function_name "handle_command"
```

### DO NOT Use These (Wasteful)

❌ `grep -r "pattern"` - Use `search_symbols` instead
❌ `find . -name "*.rs"` - Use `find_symbol` with type filter
❌ `sed -i 's/foo/bar/' file` - Use Edit tool instead
❌ `awk '{print $1}' file` - Use Read + jq/text processing instead
❌ `cat file.rs` to understand - Use `analyze_impact` first
❌ Multiple file reads for context - Use `semantic_search_with_context` once

### Token Efficiency

**Without codanna**:
```bash
grep -r "agent" src/       # 50K tokens of raw output
cat agent_runtime.rs        # 2K tokens
cat commands.rs             # 3K tokens
# Still don't know relationships
```

**With codanna**:
```bash
semantic_search_with_context "agent runtime" --limit 3
# Returns: 3 symbols + documentation + callers + callees + dependencies
# Total: ~2K tokens, COMPLETE understanding
```

### When to Read Files

Only use `Read` tool AFTER codanna for:
- Specific implementation details
- Verification of algorithm
- Checking exact error messages

**Order matters**: codanna → understand → read specifics

## Critical Rules

### NEVER Touch Clippy/Lint Configuration
**ABSOLUTE RULE: DO NOT MODIFY clippy or linting configuration files. EVER.**

This includes but is not limited to:
- `.clippy.toml`
- `clippy.toml`
- Any `#![allow(...)]` or `#![deny(...)]` attributes in `lib.rs` or `main.rs`
- Clippy-related sections in `Cargo.toml`

If clippy reports warnings or errors, fix the **code**, not the lint rules.

### Build System: Moon

This project uses Moon for builds:

```bash
# Development
moon run :build      # Release build
moon run :test       # Run tests
moon run :quick      # Format + lint check
moon run :fmt-fix    # Auto-fix formatting
moon run :check      # Fast type check
```

### Code Quality
- Zero unwraps: `unwrap()` and `expect()` are forbidden
- Zero panics: `panic!`, `todo!`, `unimplemented!` are forbidden
- All errors must use `Result<T, Error>` with proper propagation
- Use functional patterns: `map`, `and_then`, `?` operator

## Project Overview

**shitty-swarm-manager** is a PostgreSQL-based agent swarm coordination system.

### AI Mission & Guidelines
See **[MISSION.md](MISSION.md)** for the high-level mission statement, operating principles, and detailed pipeline guidance for autonomous agents.

### Project Structure
```
src/
├── main.rs              # Binary entry point
├── lib.rs               # Library exports
├── config.rs            # Configuration loading
├── agent_runtime.rs     # Agent execution logic
├── types.rs             # Type definitions
├── error.rs             # Error types
└── db/                  # Database layer
    ├── mod.rs           # Database module
    ├── mappers.rs       # Row/Entity mappers
    ├── read_ops.rs      # Read operations
    └── write_ops.rs     # Write operations
```

### Core Types
- **Agent**: Represents an autonomous agent in the swarm
- **Task**: Work units assigned to agents
- **AgentStatus**: Agent lifecycle states (idle, active, failed, etc.)
- **SwarmDb**: Database abstraction layer

### Protocol Commands

This CLI operates as a JSONL protocol. Each line is a JSON object with a `cmd` field.
See `ai_cli_protocol.cue` for the full schema and supported commands.

## Database

**PostgreSQL** is the primary data store:

- Default URL: `postgresql://oya:oya@localhost:5432/swarm_db`
- Schema: SQL migrations in `crates/swarm-coordinator/schema.sql`
- Access via `SwarmDb` type in `src/db/`

### Database Operations

- **Read**: `src/db/read_ops.rs` - Query agents, tasks, status
- **Write**: `src/db/write_ops.rs` - Create, update, delete operations
- **Mappers**: `src/db/mappers.rs` - Convert DB rows to runtime types

## Development Workflow

### Quick Iteration Loop
```bash
# Edit code...
moon run :check           # Fast type check
moon run :quick           # Format + lint check
```

### Before Committing
```bash
moon run :fmt-fix         # Auto-fix formatting
moon run :test            # Run tests
moon run :quick           # Format + lint check
```

### Running Tests
```bash
moon run :test            # All tests
```

## Key Dependencies

- **tokio**: Async runtime
- **sqlx**: Database queries with compile-time verification
- **tracing**: Structured logging
- **serde/serde_json**: Serialization
- **anyhow/thiserror**: Error handling
- **uuid**: Unique identifiers

## Session Completion

**When ending a work session**, ensure the following:

1. **Run quality gates** (if code changed):
   ```bash
   moon run :fmt-fix
   moon run :quick
   moon run :test
   ```

2. **Commit changes**:
   ```bash
   jj commit -m "description"
   br sync --flush-only
   git add .beads/
   git commit -m "sync beads"
   jj git fetch
   jj git push
   jj status
   ```

3. **Verify build**:
   ```bash
   moon run :build
   ```

**CRITICAL RULES:**
- Always use Moon for builds (not cargo)
- Zero panics/panics - use `Result<T, Error>` everywhere
- DO NOT modify clippy/lint configuration
- Test before committing
