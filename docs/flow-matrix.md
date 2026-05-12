# Flow Matrix 流程矩阵

本文档是 `master_orchestrator` 端到端能力覆盖的可执行事实来源。

## 当前矩阵

| Flow ID | 入口 | 真实 Provider | 用途 |
| --- | --- | ---: | --- |
| `import_aliases` | import/兼容 | 否 | 验证 `master_orchestrator`、`claude_orchestrator`、`codex_orchestrator` 都可以导入 |
| `help_master_module` | 入口 | 否 | 验证 `python -m master_orchestrator --help` |
| `help_master_script` | 入口 | 否 | 验证安装后的 `master-orchestrator --help` |
| `help_mo_script` | 入口 | 否 | 验证安装后的 `mo --help` |
| `help_claude_compat` | 兼容 CLI | 否 | 验证 `python -m claude_orchestrator.cli --help` |
| `help_codex_compat` | 兼容 CLI | 否 | 验证 `python -m codex_orchestrator.cli --help` |
| `simple_codex_real` | simple | 是 | 在沙盒目标上跑真实 Codex simple smoke |
| `simple_claude_real` | simple | 是 | 在沙盒目标上跑真实 Claude simple smoke |
| `runs_status_after_simple` | runs | 否 | 验证 `runs` 可以检查真实 simple run |
| `operation_dag_run` | run/operation | 否 | 验证真实 operation DAG 可以完整跑通 |
| `runs_graph_after_operation` | runs/visualize | 否 | 基于真实 operation run fixture 验证 DAG 图输出 |
| `runs_retry_after_operation` | runs/retry | 否 | 验证 `retry-failed` 可以恢复失败的 operation DAG fixture |
| `auto_default_real` | auto | 是 | 真实默认 provider auto smoke |
| `auto_claude_real` | auto | 是 | 强制 Claude 的真实 auto smoke |
| `auto_codex_real` | auto | 是 | 强制 Codex 的真实 auto smoke |
| `auto_mixed_real` | auto | 是 | 混合 phase provider 的真实 auto smoke |
| `improve_real` | improve | 是 | 最小真实 improve smoke |

## 分层

- `required`：发布门禁默认只看这一层，优先覆盖本地可重复、无需真实 provider 的流程。
- `nightly`：真实 provider smoke 和高成本流程，默认参与报告，但不阻断 required gate。

`scripts/run_flow_matrix.py` 现在会：

- 继续执行所选的全部 flow
- 在 JSON 报告里写入 `required_*` / `nightly_*` 汇总字段
- 仅当 `required` 层存在 `failed` 或 `blocked` 时返回非零退出码

## 执行

运行完整矩阵：

```bash
python scripts/run_flow_matrix.py
```

只运行子集：

```bash
python scripts/run_flow_matrix.py --flow simple_codex_real --flow operation_dag_run
```

runner 会写出：

- JSON 汇总：`audit_logs/flow-matrix.json`
- 单个 flow 日志：`audit_logs/flow-matrix-artifacts/`

## 修复循环

当某个 flow 失败时：

1. 先处理第一个失败或 blocked 的 required flow。
2. 为该 flow 新增或收紧聚焦回归测试。
3. 只修复它所属的子系统。
4. 重新运行聚焦测试。
5. 使用 `scripts/run_flow_matrix.py --flow <id>` 重跑失败 flow。
6. 通过后再处理下一个失败 flow。
