<div align="center">

# Master Orchestrator

**让 Claude Code 和 Codex 协作处理大型编码任务。**

自动把目标拆成 DAG 任务图，跨多个 AI Agent 并行执行，并收敛到可验证的交付结果。

[![许可证：MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![GitHub Stars](https://img.shields.io/github/stars/amber132/Master-Orchestrator.svg)](https://github.com/amber132/Master-Orchestrator/stargazers)

[快速开始](#快速开始) | [为什么需要 Master Orchestrator](#为什么需要-master-orchestrator) | [工作原理](#工作原理) | [文档](#文档)

</div>

---

## 问题

你正在处理一个单次 AI Agent 调用很难稳住的大任务：

- "给这个 Express 应用加 JWT 鉴权，包括测试、中间件和文档"
- "重构支付模块，涉及 3 个包、15 个文件"
- "数据库迁移后，把所有失败测试修好"

单个 `claude` 或 `codex` 会遇到上下文漂移、依赖遗漏、跨文件改动不一致等问题。最后你只能自己反复启动多个会话、检查输出、拼接结果。

## 方案

Master Orchestrator 把一个自然语言目标转换成**有向无环图（DAG）**任务，再按阶段选择最合适的 AI Agent 执行：

```bash
mo do "给 Express 应用添加 JWT 鉴权，包括中间件、路由、测试和 API 文档"
```

它会自动完成：

1. **任务分解**：把目标拆成有依赖关系的子任务，规划阶段默认交给 Claude
2. **阶段路由**：按任务类型选择 provider，例如 Claude 负责推理，Codex 负责执行
3. **并行执行**：没有依赖冲突的任务并行跑，最高支持 150 并发
4. **交叉审查**：用另一个 Agent 做结果验证和质量门禁
5. **失败重试**：按错误类型分类，使用指数退避重试
6. **结果收敛**：合并输出、跑测试、确认最终交付一致

## 为什么需要 Master Orchestrator

| 场景 | 单 Agent | Master Orchestrator |
| --- | --- | --- |
| 5 个文件的重构 | 容易上下文溢出，改动不一致 | DAG 分解，并行执行 |
| 100+ 文件批量修复 | 串行、慢、失败后靠手工重跑 | Simple 模式：16 并发、自动重试 |
| 复杂功能开发 | 人工协调多个会话 | 自动阶段路由，检测收敛状态 |
| 混合型任务 | 同一个模型同时负责规划和写代码 | Claude 做规划，Codex 做执行 |

## 快速开始

### 安装

```bash
git clone https://github.com/amber132/Master-Orchestrator.git
cd Master-Orchestrator
pip install -e ".[dev]"
```

需要 Python 3.11+，并且本机 PATH 中至少能使用 `claude` 或 `codex` 其中之一。

### 第一个编排任务

```bash
# 让编排器自动选择 provider
mo do "给 src/routes/ 下所有 POST 接口补充输入校验"

# 强制使用指定 provider
mo do --provider codex "给 UserService 类生成单元测试"

# 按阶段混用 provider：Claude 规划，Codex 执行，Claude 审查
mo do \
  --phase-provider decompose=claude \
  --phase-provider execute=codex \
  --phase-provider review=claude \
  "重构支付模块，让它支持多币种"
```

### 用 Simple 模式处理批量任务

适合几百个独立工作项，例如 lint 修复、格式化、规则化小改动：

```bash
# 从任务清单扫描并执行
mo simple run --manifest tasks.jsonl

# 中断后继续
mo simple resume

# 只重试失败项
mo simple retry
```

Simple 模式默认最多运行 **16 个并发 worker**，内置自动重试、语法验证和崩溃恢复。

## 工作原理

```
                    ┌─────────────────────────────────────────────┐
                    │            你的目标（自然语言）              │
                    └────────────────────┬────────────────────────┘
                                         │
                    ┌────────────────────▼────────────────────────┐
                    │            分解（Claude）                    │
                    │       目标 → 有依赖关系的 DAG 子任务          │
                    └────────────────────┬────────────────────────┘
                                         │
              ┌──────────────────────────┼──────────────────────────┐
              │                          │                          │
    ┌─────────▼─────────┐    ┌──────────▼──────────┐    ┌─────────▼─────────┐
    │  任务 A (Claude)   │    │  任务 B (Codex)      │    │  任务 C (Codex)    │
    │  "写中间件"        │    │  "写路由"            │    │  "写测试"          │
    └─────────┬─────────┘    └──────────┬──────────┘    └─────────┬─────────┘
              │                          │                          │
              └──────────────────────────┼──────────────────────────┘
                                         │
                    ┌────────────────────▼────────────────────────┐
                    │            审查（Claude）                    │
                    │       跨 Agent 验证与质量门禁                 │
                    └────────────────────┬────────────────────────┘
                                         │
                    ┌────────────────────▼────────────────────────┐
                    │            收敛                              │
                    │       合并结果、运行测试、验证输出            │
                    └─────────────────────────────────────────────┘
```

### Provider 路由

流水线的每个阶段都可以使用不同 AI Agent：

| 阶段 | 默认 Provider | 原因 |
| --- | --- | --- |
| `decompose` | Claude | 更适合规划和依赖分析 |
| `execute` | Codex | 更适合高吞吐代码生成 |
| `review` | Claude | 更适合推理正确性和发现问题 |
| `discover` | Claude | 更适合调研和探索 |
| `simple` | Codex | 面向批量任务做了吞吐优化 |

可以在不同层级覆盖默认路由：

```bash
# 全局指定
mo do --provider claude "..."

# 按阶段指定
mo do --phase-provider execute=codex --phase-provider review=claude "..."

# 配置文件 config.toml
[routing.phase_defaults]
execute = "codex"
review = "claude"
```

### 错误恢复

编排器会识别错误类型，并选择对应策略：

- **限流**：指数退避并加入抖动
- **上下文溢出**：自动压缩上下文后重试
- **临时失败**：最多重试 10 次，默认 30 秒基础退避
- **Provider 不可用**：在 auto 模式下切换到备用 provider
- **任务失败**：向依赖任务传播状态，跳过不可达任务

### 收敛检测

系统会持续检查：

- **停滞**：多轮没有进展时升级或切换策略
- **劣化**：质量下降时回滚到上一个可用状态
- **回归**：原本通过的测试失败时暂停并提示

## 架构

```
master_orchestrator/
├── orchestrator.py      # DAG 执行核心
├── autonomous.py        # 目标驱动的自主控制器
├── claude_cli.py        # Claude Code 集成
├── codex_cli.py         # Codex CLI 集成
├── simple_runtime.py    # 高吞吐批量执行
├── config.py            # TOML 配置管理
├── store.py             # SQLite 状态持久化
├── scheduler.py         # 感知 DAG 依赖的任务调度
├── convergence.py       # 质量收敛检测
├── error_classifier.py  # 错误分类
├── self_improve.py      # 自我改进循环
└── cli.py               # 统一 CLI 入口
```

## 配置

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

完整示例见 [config.toml](./config.toml)。

## 文档

| 文档 | 说明 |
| --- | --- |
| [USAGE.md](./USAGE.md) | 使用指南和示例 |
| [CLI 参考](./docs/CLI_REFERENCE.md) | 全部命令和参数 |
| [Provider 与路由](./docs/providers-and-routing.md) | Provider 配置与路由机制 |
| [Simple 模式](./docs/simple-mode.md) | 高吞吐批量执行模式 |
| [Simple 验证](./docs/simple-validation.md) | Simple 模式验证管线配置 |

## 使用场景

**功能开发**

```bash
mo do "用 WebSocket 添加实时通知，包括服务端、客户端、断线重连逻辑和测试"
```

**代码迁移**

```bash
mo do --phase-provider execute=codex "把 src/components/ 下所有 class component 迁移为 hooks 写法"
```

**批量修复**

```bash
mo simple run --manifest lint-fixes.jsonl
```

**自我改进**

```bash
mo improve -d ./my-project --discover
```

## 参与贡献

贡献指南见 [CONTRIBUTING.md](./CONTRIBUTING.md)。

## 许可证

[MIT](LICENSE)

---

<div align="center">

**如果 Master Orchestrator 帮你更快交付，欢迎点一个星标。**

[![Star History Chart](https://api.star-history.com/svg?repos=amber132/Master-Orchestrator&type=Date)](https://star-history.com/#amber132/Master-Orchestrator&Date)

</div>
