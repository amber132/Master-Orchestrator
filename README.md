<div align="center">

# Master Orchestrator

**Let Claude Code and Codex collaborate on your large-scale coding tasks.**

Automatically decompose goals into DAGs, execute tasks in parallel across multiple AI agents, and converge on verified results.

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![GitHub Stars](https://img.shields.io/github/stars/amber132/Master-Orchestrator.svg)](https://github.com/amber132/Master-Orchestrator/stargazers)

[Quick Start](#quick-start) | [Why Master Orchestrator?](#why-master-orchestrator) | [How It Works](#how-it-works) | [Documentation](#documentation)

</div>

---

## The Problem

You're working on a task that's too big for a single AI agent call:

- "Add JWT auth to this Express app — with tests, middleware, and docs"
- "Refactor the payment module — 15 files across 3 packages"
- "Fix all failing tests after the database migration"

A single `claude` or `codex` session gets lost in context, misses dependencies, or produces inconsistent code across files. You end up manually coordinating multiple runs, checking outputs, and stitching results together.

## The Solution

Master Orchestrator turns one natural-language goal into a **directed acyclic graph (DAG)** of tasks, then executes them with the best AI agent for each phase:

```bash
mo do "Add JWT authentication to the Express app with middleware, routes, tests, and API docs"
```

It automatically:
1. **Decomposes** your goal into dependent sub-tasks (Claude excels here)
2. **Routes** each task to the optimal provider (Claude for reasoning, Codex for execution)
3. **Executes** independent tasks in parallel (up to 150 concurrent)
4. **Reviews** outputs with cross-agent verification
5. **Retries** failures with exponential backoff and error classification
6. **Converges** on a verified, consistent result

## Why Master Orchestrator?

| Scenario | Single Agent | Master Orchestrator |
|----------|-------------|-------------------|
| 5-file refactor | Context overflow, inconsistent edits | DAG decomposition, parallel execution |
| Bulk fixes (100+ files) | Sequential, slow, no retry | Simple mode: 16 parallel workers, auto-retry |
| Complex feature | Manual coordination of multiple runs | Automatic phase routing, convergence detection |
| Mixed tasks | Same model for reasoning and coding | Claude for planning, Codex for execution |

## Quick Start

### Install

```bash
git clone https://github.com/amber132/Master-Orchestrator.git
cd Master-Orchestrator
pip install -e ".[dev]"
```

Requires Python 3.11+ and at least one of `claude` or `codex` on your PATH.

### Your First Orchestrated Task

```bash
# Let the orchestrator auto-route providers
mo do "Add input validation to all POST endpoints in src/routes/"

# Force a specific provider
mo do --provider codex "Generate unit tests for the UserService class"

# Mix providers by phase — Claude plans, Codex executes, Claude reviews
mo do \
  --phase-provider decompose=claude \
  --phase-provider execute=codex \
  --phase-provider review=claude \
  "Refactor the payment module to support multiple currencies"
```

### Bulk Operations with Simple Mode

For hundreds of independent work items (linting, formatting, repetitive edits):

```bash
# Scan and execute from a task manifest
mo simple run --manifest tasks.jsonl

# Resume after interruption
mo simple resume

# Retry only failures
mo simple retry
```

Simple mode runs up to **16 parallel workers** with automatic retry, syntax validation, and crash recovery.

## How It Works

```
                    ┌─────────────────────────────────────────────┐
                    │              Your Goal (natural language)     │
                    └────────────────────┬────────────────────────┘
                                         │
                    ┌────────────────────▼────────────────────────┐
                    │         Decompose (Claude)                   │
                    │   Goal → DAG of dependent sub-tasks          │
                    └────────────────────┬────────────────────────┘
                                         │
              ┌──────────────────────────┼──────────────────────────┐
              │                          │                          │
    ┌─────────▼─────────┐    ┌──────────▼──────────┐    ┌─────────▼─────────┐
    │  Task A (Claude)   │    │  Task B (Codex)      │    │  Task C (Codex)    │
    │  "Write middleware"│    │  "Write routes"      │    │  "Write tests"     │
    └─────────┬─────────┘    └──────────┬──────────┘    └─────────┬─────────┘
              │                          │                          │
              └──────────────────────────┼──────────────────────────┘
                                         │
                    ┌────────────────────▼────────────────────────┐
                    │         Review (Claude)                      │
                    │   Cross-agent verification & quality gate     │
                    └────────────────────┬────────────────────────┘
                                         │
                    ┌────────────────────▼────────────────────────┐
                    │         Converge                             │
                    │   Merge results, run tests, verify output     │
                    └─────────────────────────────────────────────┘
```

### Provider Routing

Each phase of the pipeline can use a different AI agent:

| Phase | Default Provider | Why |
|-------|-----------------|-----|
| `decompose` | Claude | Better at planning and dependency analysis |
| `execute` | Codex | Faster for code generation |
| `review` | Claude | Better at reasoning about correctness |
| `discover` | Claude | Better at research and exploration |
| `simple` | Codex | Optimized for high-throughput bulk tasks |

Override at any level:

```bash
# Global override
mo do --provider claude "..."

# Per-phase override
mo do --phase-provider execute=codex --phase-provider review=claude "..."

# Config file (config.toml)
[routing.phase_defaults]
execute = "codex"
review = "claude"
```

### Error Recovery

The orchestrator classifies errors and applies appropriate strategies:

- **Rate limits** → exponential backoff with jitter
- **Context overflow** → automatic context compaction and retry
- **Transient failures** → up to 10 retries with 30s base backoff
- **Provider down** → fallback to alternate provider (auto mode only)
- **Task failure** → propagate to dependents, skip unreachable tasks

### Convergence Detection

The system monitors for:
- **Plateaus** — no progress across iterations → escalate or pivot strategy
- **Deterioration** — quality declining → rollback to last good state
- **Regression** — previously passing tests failing → halt and alert

## Architecture

```
master_orchestrator/
├── orchestrator.py      # Core DAG execution engine
├── autonomous.py        # Goal-driven autonomous controller
├── claude_cli.py        # Claude Code integration
├── codex_cli.py         # Codex CLI integration
├── simple_runtime.py    # High-throughput bulk execution
├── config.py            # TOML configuration management
├── store.py             # SQLite state persistence
├── scheduler.py         # DAG-aware task scheduling
├── convergence.py       # Quality convergence detection
├── error_classifier.py  # Intelligent error categorization
├── self_improve.py      # Self-improvement loop
└── cli.py               # Unified CLI surface
```

## Configuration

```toml
# config.toml
[orchestrator]
max_parallel = 150

[claude]
default_model = "sonnet"
default_timeout = 1800
max_budget_usd = 1000.0

[codex]
default_model = "gpt-5.4"
default_timeout = 1800
execution_security_mode = "restricted"

[routing]
default_provider = "auto"
auto_fallback = true

[routing.phase_defaults]
decompose = "claude"
execute = "codex"
review = "claude"
```

See [config.toml](./config.toml) for a complete example.

## Documentation

| Document | Description |
|----------|-------------|
| [USAGE.md](./USAGE.md) | Complete usage guide with examples |
| [CLI Reference](./docs/CLI_REFERENCE.md) | All commands and options |
| [Providers & Routing](./docs/providers-and-routing.md) | Provider configuration deep-dive |
| [Simple Mode](./docs/simple-mode.md) | High-throughput bulk execution |
| [Simple Validation](./docs/simple-validation.md) | Validation pipeline configuration |

## Use Cases

**Feature Development**
```bash
mo do "Add real-time notifications using WebSocket — include server, client, reconnection logic, and tests"
```

**Codebase Migration**
```bash
mo do --phase-provider execute=codex "Migrate all class components to functional components with hooks in src/components/"
```

**Bulk Fixes**
```bash
mo simple run --manifest lint-fixes.jsonl
```

**Self-Improvement**
```bash
mo improve -d ./my-project --discover
```

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for guidelines.

## License

[MIT](LICENSE)

---

<div align="center">

**If Master Orchestrator helps you ship faster, give it a star!**

[![Star History Chart](https://api.star-history.com/svg?repos=amber132/Master-Orchestrator&type=Date)](https://star-history.com/#amber132/Master-Orchestrator&Date)

</div>
