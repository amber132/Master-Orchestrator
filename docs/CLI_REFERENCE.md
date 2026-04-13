# Master Orchestrator — CLI 参考

## 全局形式

```bash
master-orchestrator [-c CONFIG] <command> [options]
```

主命令别名：

```bash
mo ...
```

---

## 推荐入口

| 命令 | 说明 |
|------|------|
| `do` | 统一执行入口 |
| `runs` | 统一运行管理 |
| `improve` | 自我改进入口 |

---

## Provider 控制参数

适用于 `do`、`auto`、`self-improve`，以及 provider 相关流程：

| 参数 | 说明 |
|------|------|
| `--provider {auto,claude,codex}` | 指定整个任务的 provider |
| `--phase-provider PHASE=PROVIDER` | 覆盖某个 phase 的 provider，可重复传入 |

示例：

```bash
mo do --provider codex "实现分页接口"
mo do --phase-provider execute=codex --phase-provider review=claude "重构支付模块"
master-orchestrator codex do "实现分页接口"
```

---

## `do`

```bash
master-orchestrator do <goal|@doc|dag> [options]
```

路由规则：

- DAG 文件输入映射到 `run` / `resume`
- simple 输入映射到 `simple run`
- 普通目标文本映射到 `auto`

---

## `runs`

```bash
master-orchestrator runs [dag] [--resume|--retry|--graph] [options]
```

映射关系：

- 默认：`status`
- `--resume`：`resume`
- `--retry`：`retry-failed`
- `--graph`：`visualize`

---

## `improve`

```bash
master-orchestrator improve -d <project> [options]
```

说明：

- `improve` 是 `self-improve` 的推荐入口
- 可结合 `--provider` 或 phase provider 覆盖做混合执行
- `--plan-file` 可直接加载结构化改进计划并把它们作为本轮提案种子
- `--monitor-required` 会在每轮后追加 required flow gate
- `--monitor-flow FLOW_ID` 可追加特定 flow-matrix smoke 作为持续监控

---

## 兼容命令

以下命令保留用于兼容历史脚本：

- `run`
- `resume`
- `retry-failed`
- `status`
- `visualize`
- `auto`
- `self-improve`
- `simple`

这些命令默认在帮助里隐藏，但仍然可执行。

### `simple` 子命令

当前公开的 `simple` 子命令包括：

- `simple run`
- `simple scan`
- `simple resume`
- `simple retry`
- `simple status`
- `simple manifest`
- `simple cancel`
- `simple reconcile`
