# Flow Matrix

This document is the executable truth source for end-to-end feature coverage in `master_orchestrator`.

## Current Matrix

| Flow ID | Surface | Real Provider | Purpose |
|---|---|---:|---|
| `import_aliases` | import/compat | no | Verify `master_orchestrator`, `claude_orchestrator`, `codex_orchestrator` all import |
| `help_master_module` | entrypoint | no | Verify `python -m master_orchestrator --help` |
| `help_master_script` | entrypoint | no | Verify installed `master-orchestrator --help` |
| `help_mo_script` | entrypoint | no | Verify installed `mo --help` |
| `help_claude_compat` | compat CLI | no | Verify `python -m claude_orchestrator.cli --help` |
| `help_codex_compat` | compat CLI | no | Verify `python -m codex_orchestrator.cli --help` |
| `simple_codex_real` | simple | yes | Real Codex simple smoke on sandbox target |
| `simple_claude_real` | simple | yes | Real Claude simple smoke on sandbox target |
| `runs_status_after_simple` | runs | no | Verify `runs` can inspect a real simple run |
| `operation_dag_run` | run/operation | no | Verify a real operation DAG can run to completion |
| `runs_graph_after_operation` | runs/visualize | no | Verify DAG graph output against a real operation run fixture |
| `runs_retry_after_operation` | runs/retry | no | Verify retry-failed can recover a real failing operation DAG fixture |
| `auto_default_real` | auto | yes | Real default-provider auto smoke |
| `auto_claude_real` | auto | yes | Real Claude-forced auto smoke |
| `auto_codex_real` | auto | yes | Real Codex-forced auto smoke |
| `auto_mixed_real` | auto | yes | Real mixed phase-provider auto smoke |
| `improve_real` | improve | yes | Real minimal improve smoke |

## Tiers

- `required`: 发布门禁默认只看这一层。以本地可重复、无需真实 provider 的流程为主。
- `nightly`: 真实 provider smoke 和高成本流程，默认参与报告，但不阻断 required gate。

`scripts/run_flow_matrix.py` 现在会：

- 继续执行所选的全部 flow
- 在 JSON 报告里写入 `required_*` / `nightly_*` 汇总字段
- 仅当 `required` 层存在 `failed` 或 `blocked` 时返回非零退出码

## Execution

Run the full matrix:

```bash
python scripts/run_flow_matrix.py
```

Run a subset:

```bash
python scripts/run_flow_matrix.py --flow simple_codex_real --flow operation_dag_run
```

The runner writes:

- JSON summary: `audit_logs/flow-matrix.json`
- Per-flow logs: `audit_logs/flow-matrix-artifacts/`

## Repair Loop

When a flow fails:

1. Take the first failing or blocked required flow.
2. Add or tighten the focused regression test for that flow.
3. Fix only the owning subsystem.
4. Re-run focused tests.
5. Re-run the failing flow through `scripts/run_flow_matrix.py --flow <id>`.
6. Only then continue to the next failing flow.
