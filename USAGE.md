# Master Orchestrator — 使用手册

## 安装

```bash
cd <repo>
python -m venv .venv
.venv/Scripts/activate
pip install -e ".[dev]"
```

安装后可使用 `master-orchestrator`，也可使用短别名 `mo`。

---

## 全局选项

```bash
master-orchestrator [-c CONFIG] <command> [options]
```

| 选项 | 说明 |
|------|------|
| `-c`, `--config` | 配置文件路径（默认自动查找 `config.toml`） |

---

## 推荐命令

| 命令 | 用途 |
|------|------|
| `mo do ...` | 统一执行入口。自动识别自然语言目标、`@文档`、DAG 文件和 simple 输入 |
| `mo runs ...` | 统一运行管理。查看状态、恢复运行、重试失败任务、查看 DAG 图 |
| `mo improve ...` | 自我改进流程 |

常用示例：

```bash
# 默认按 phase 路由 provider
mo do "修复 login API 返回 500 错误"

# 显式指定整个任务使用 codex
mo do --provider codex "实现分页接口"

# 按阶段混用 provider
mo do --phase-provider execute=codex --phase-provider review=claude "重构支付模块"

# 文档驱动任务
mo do @specs/auth.md

# 查看最近运行状态
mo runs

# 自我改进
mo improve -d D:/myproject --discover
```

兼容性说明：

- 旧命令 `run`、`resume`、`retry-failed`、`status`、`visualize`、`auto`、`self-improve`、`simple` 仍然可用
- 旧 provider 名称不再注册为全局命令；如需显式切 provider，使用 `--provider` 或 `master-orchestrator claude|codex ...`

---

## Provider 选择

### 1. 整体指定

```bash
mo do --provider claude "分析并修复缓存抖动"
mo do --provider codex "实现导出接口"
```

### 2. 分阶段指定

```bash
mo do \
  --phase-provider decompose=claude \
  --phase-provider execute=codex \
  --phase-provider review=claude \
  "重构支付模块"
```

支持的 phase 至少包括：

- `decompose`
- `execute`
- `review`
- `discover`
- `self_improve`

### 3. provider 子命令别名

```bash
master-orchestrator codex do "实现分页接口"
master-orchestrator claude improve -d D:/myproject
```

---

## 兼容命令一览

| 命令 | 用途 |
|------|------|
| `run` | 执行 DAG 工作流 |
| `resume` | 恢复上次中断的 DAG 运行 |
| `retry-failed` | 重试失败任务并继续 |
| `status` | 查看运行状态 |
| `visualize` | 打印 DAG 结构 |
| `auto` | 自主目标驱动执行 |
| `self-improve` | 自我迭代改进 |
| `simple` | simple work-item 模式 |

---

## 配置要点

配置文件同时支持三组核心配置：

- `[claude]`：Claude CLI 默认模型、CLI 路径、超时、预算
- `[codex]`：Codex CLI 默认模型、CLI 路径、超时、预算
- `[routing]`：默认 provider、auto fallback、phase 默认路由

完整示例见 [config.toml](./config.toml)。

---

## 进一步阅读

- [CLI_REFERENCE.md](./docs/CLI_REFERENCE.md)
- [providers-and-routing.md](./docs/providers-and-routing.md)
- [simple-mode.md](./docs/simple-mode.md)
