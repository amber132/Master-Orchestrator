# How I Made Claude Code and Codex Collaborate on Large Coding Tasks — Master Orchestrator

> This post introduces Master Orchestrator's design motivation, architecture, and real-world results. Suitable for Medium, Dev.to, or Hacker News "Show HN".

---

## The Problem: Single-Agent Ceiling

If you've used Claude Code or Codex for anything beyond trivial tasks, you've hit these walls:

**Context overflow.** A 15-file refactor — by file 8, the agent forgets what it changed in file 2. Imports break. Interfaces mismatch.

**Sequential bottleneck.** 100 independent lint fixes, each taking 30 seconds. That's 50 minutes for something embarrassingly parallel.

**Wrong tool for the job.** Planning needs strong reasoning (Claude). Bulk code generation needs speed (Codex). Using one model for everything compromises both.

**Manual orchestration.** You become the scheduler — run agent 1, check output, feed to agent 2, check again. That's not automation.

## The Idea: DAG-Based Multi-Agent Orchestration

The core insight: **decompose a goal into a directed acyclic graph (DAG) of sub-tasks, then auto-schedule execution across the best agent for each phase.**

```
Goal: "Add JWT auth to Express app"
        │
        ▼
   ┌─────────────┐
   │ Decompose    │  Claude (strong reasoning)
   │ (plan tasks) │
   └──────┬──────┘
          │
    ┌─────┼─────┬─────────┐
    ▼     ▼     ▼         ▼
  Mid-  Utils  Routes   Tests
  dle- (codex) (codex)  (codex)  ← parallel
  ware
    │     │     │         │
    └─────┼─────┘         │
          ▼               │
       Review             │  Claude (strong analysis)
          │               │
          └───────┬───────┘
                  ▼
            Integration
```

- **Planning** → Claude (better reasoning)
- **Execution** → Codex (faster generation)
- **Review** → Claude (better at finding issues)
- **Independent tasks** → automatic parallelism

## Architecture

### Provider Routing

Each pipeline phase binds to a different AI provider:

```toml
[routing.phase_defaults]
decompose = "claude"
execute = "codex"
review = "claude"
simple = "codex"
```

Override per-task or per-phase:

```bash
mo do --provider codex "implement pagination"
mo do --phase-provider execute=codex --phase-provider review=claude "refactor payments"
```

### DAG Scheduler

The scheduler maintains a task dependency graph:

1. Find all nodes with zero in-degree (no incomplete dependencies)
2. Execute them in parallel (up to 150 concurrent)
3. On completion, update the graph and unlock downstream tasks
4. Repeat until all tasks complete or fail

### Error Classification

Not all errors deserve the same retry strategy:

| Error | Strategy |
|-------|----------|
| Rate limit | Exponential backoff with jitter |
| Context overflow | Auto-compact context, retry with shorter prompt |
| Transient | Up to 10 retries, 30s base backoff |
| Provider down | Fallback to alternate provider (auto mode) |
| Task failure | Propagate to dependents, skip unreachable |

### Simple Mode: Bulk Execution Engine

For "100 files need the same small fix" tasks:

- 16 parallel workers
- Automatic retry on failures
- Syntax validation
- Crash recovery (`simple resume` after power loss)

```bash
mo simple run --manifest fixes.jsonl
mo simple resume  # continue after interruption
mo simple retry   # retry only failures
```

## Real-World Results

**JWT auth feature** — 6 tasks, 4 parallel, completed in 4m 23s with 0 failures.

**Bulk lint fix** — 147 files, 16 workers, 8 minutes (vs. 40+ minutes sequential).

**Cross-module refactor** — 15 files across 3 packages. DAG ensured correct ordering. Claude review caught 3 edge cases that Codex missed.

## Tech Stack

- Python 3.11+ (asyncio for concurrent scheduling)
- Pydantic (data models, config validation)
- SQLite (lightweight state persistence)
- TOML (human-friendly configuration)

## Try It

```bash
git clone https://github.com/amber132/Master-Orchestrator.git
cd Master-Orchestrator
pip install -e ".[dev]"
mo do "your goal here"
```

Requires Python 3.11+ and at least one of `claude` or `codex` on PATH.

---

*Master Orchestrator is open source under MIT. Contributions welcome.*
