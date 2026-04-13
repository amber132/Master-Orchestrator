# Providers And Routing

`master-orchestrator` 在一个运行时内同时支持 Claude 和 Codex 两个 provider。

## 核心规则

- `TaskNode.provider` 取值为 `auto`、`claude`、`codex`
- 旧写法 `type = "claude_cli"` / `type = "codex_cli"` 仍可用，但内部会归一成 `type = "agent_cli"`
- `provider_used` 会落到 `TaskResult`、任务尝试和 simple 尝试的持久化记录里
- `codex.execution_security_mode` 控制 Codex CLI 的执行边界：
  - `restricted`：保留正常审批/沙箱边界
  - `trusted_local`：仅在显式信任的本地环境下启用危险绕过模式

## 路由优先级

从高到低：

1. CLI 显式 `--provider`
2. 任务或 workflow 显式 `provider`
3. `--phase-provider phase=provider`
4. `[routing.phase_defaults]`
5. `auto` 启发式

## 默认 phase 路由

```toml
[routing.phase_defaults]
decompose = "claude"
review = "claude"
discover = "claude"
execute = "codex"
simple = "codex"
self_improve = "claude"
requirement = "claude"
```

## Fallback 规则

- 仅 `provider = "auto"` 时允许跨 provider fallback
- 显式指定 `claude` 或 `codex` 时，失败即返回失败，不自动偷偷切 provider
