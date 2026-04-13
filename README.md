# Master Orchestrator

`master-orchestrator` is an isolated third project that merges the Claude-oriented orchestration baseline with Codex execution support. It provides one neutral control plane that can run Claude, Codex, or mix both providers inside the same task flow.

## Setup

```bash
cd <repo>
python -m venv .venv
.venv/Scripts/activate
pip install -e ".[dev]"
```

Python `3.11+` is required. Runtime execution expects whichever provider you choose to be installed on `PATH`:

- `claude` for Claude-backed runs
- `codex` for Codex-backed runs

The default Codex model is `gpt-5.4`. This avoids an unnecessary first-hop fallback on machines where `gpt-5.4-pro` is not available.

## Quick Start

```bash
# Verify the repository
python -m pytest -q

# Show the unified CLI surface
python -m master_orchestrator --help

# Auto route providers by phase defaults
mo do "修复登录接口"

# Force one provider for the whole run
mo do --provider codex "实现分页接口"

# Mix providers by phase
mo do --phase-provider execute=codex --phase-provider review=claude "重构支付模块"
```

## Project Layout

- `master_orchestrator/`: primary package
- `claude_orchestrator/`: compatibility package alias
- `codex_orchestrator/`: compatibility package alias
- `tests/`: merged regression suite
- `docs/`: CLI reference, simple-mode docs, routing guide
- `workflows/`: example DAG and workflow definitions

## Runtime Artifacts

This project writes local runtime state inside the new repo only. Common artifacts are:

- `orchestrator_state.db`
- `task_cache.db`
- `orchestrator_runs/`
- `simple_runs/`
- `audit_logs/`
- `*.jsonl`

## Command Surface

Preferred commands:

- `mo do`
- `mo runs`
- `mo improve`

Compatibility remains available inside the same command surface:

- legacy hidden commands: `run`, `resume`, `retry-failed`, `status`, `visualize`, `auto`, `self-improve`, `simple`
- provider aliases: `master-orchestrator claude ...` and `master-orchestrator codex ...`

## Release Notes

This GitHub export intentionally excludes local runtime state, internal planning notes, and private project-specific workflows.
