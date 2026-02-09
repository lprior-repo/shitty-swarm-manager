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

### Build System: Cargo

This project uses standard Cargo for builds:

```bash
# Development
cargo build                   # Debug build
cargo build --release         # Release build
cargo test                    # Run tests
cargo clippy                  # Lint checks
cargo fmt                     # Format code

# Quick iteration
cargo check                   # Fast type check (no compilation)
cargo clippy -- -D warnings   # Strict linting
```

### Code Quality
- Zero unwraps: `unwrap()` and `expect()` are forbidden
- Zero panics: `panic!`, `todo!`, `unimplemented!` are forbidden
- All errors must use `Result<T, Error>` with proper propagation
- Use functional patterns: `map`, `and_then`, `?` operator

## Project Overview

**shitty-swarm-manager** is a PostgreSQL-based agent swarm coordination system.

### Project Structure
```
src/
├── main.rs              # Binary entry point
├── lib.rs               # Library exports
├── cli.rs               # CLI argument parsing (clap)
├── commands.rs          # Command handlers
├── config.rs            # Configuration loading
├── agent_runtime.rs     # Agent execution logic
├── monitor.rs           # Monitoring/dashboard
├── output.rs            # Output formatting
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

### CLI Commands

```bash
swarm init                          # Initialize workspace
swarm register [count]              # Register N agents (default: 12)
swarm agent -i <id>                 # Show agent details
swarm status [-a]                   # Show swarm status
swarm ps [-a]                       # List all agents
swarm dashboard [-r ms]             # Launch dashboard
swarm release -i <agent_id>         # Release an agent
swarm init-db [-u url] [-s schema]  # Initialize database
swarm monitor [-v view]             # Monitor swarm activity
swarm spawn-prompts [-n count]      # Generate agent prompts
swarm smoke -i <id>                 # Run smoke test
```

## Database

**PostgreSQL** is the primary data store:

- Default URL: `postgresql://oya:oya@localhost:5432/swarm_db`
- Schema: SQL migrations in `crates/swarm-coordinator/schema.sql`
- Access via `SwarmDb` type in `src/db/`

### Database Operations

- **Read**: `src/db/read_ops.rs` - Query agents, tasks, status
- **Write**: `src/db/write_ops.rs` - Create, update, delete operations
- **Mappers**: `src/db/mappers.rs` - Convert DB rows to domain types

## Development Workflow

### Quick Iteration Loop
```bash
# Edit code...
cargo check              # Fast type check
cargo clippy             # Lint checks
```

### Before Committing
```bash
cargo fmt                # Format code
cargo test               # Run tests
cargo clippy -- -D warnings  # Strict linting
```

### Running Tests
```bash
cargo test               # All tests
cargo test --test integration  # Integration tests only
```

## Key Dependencies

- **tokio**: Async runtime
- **sqlx**: Database queries with compile-time verification
- **clap**: CLI argument parsing
- **tracing**: Structured logging
- **serde/serde_json**: Serialization
- **anyhow/thiserror**: Error handling
- **uuid**: Unique identifiers

## Session Completion

**When ending a work session**, ensure the following:

1. **Run quality gates** (if code changed):
   ```bash
   cargo fmt
   cargo clippy -- -D warnings
   cargo test
   ```

2. **Commit changes**:
   ```bash
   git add .
   git commit -m "description"
   git push
   ```

3. **Verify build**:
   ```bash
   cargo build --release
   ```

**CRITICAL RULES:**
- Always use `cargo` for builds (not Moon or other tools)
- Zero panics/panics - use `Result<T, Error>` everywhere
- DO NOT modify clippy/lint configuration
- Test before committing
