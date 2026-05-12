# 我如何让 Claude Code 和 Codex 协作完成大型编码任务 - Master Orchestrator

> 这篇文章介绍 Master Orchestrator 的设计动机、架构思路和实际效果。原来的英文草稿已转为中文，方便面向中文社区发布。

---

## 问题：单 Agent 的天花板

如果你用 Claude Code 或 Codex 处理过稍微复杂一点的任务，大概率会遇到这些问题：

**上下文溢出。** 一个涉及 15 个文件的重构，跑到第 8 个文件时，Agent 已经忘了第 2 个文件改过什么，最后 import 断裂、接口不一致。

**串行瓶颈。** 100 个独立 lint 修复，每个 30 秒，串行跑完就是 50 分钟。明明这些任务可以并行。

**工具错配。** 规划需要强推理能力，批量代码生成需要速度和吞吐。让同一个模型处理所有阶段，两边都会妥协。

**人工编排。** 你变成调度器：启动第一个 Agent，检查输出，再喂给第二个，再检查。这不是自动化。

## 思路：基于 DAG 的多 Agent 编排

核心思路很直接：**把一个目标拆成有向无环图（DAG）子任务，再自动按阶段调度给最合适的 Agent。**

```
目标："给 Express 应用添加 JWT 鉴权"
        │
        ▼
   ┌─────────────┐
   │ 分解任务     │  Claude（推理强）
   │ 生成计划     │
   └──────┬──────┘
          │
    ┌─────┼─────┬─────────┐
    ▼     ▼     ▼         ▼
  中间件  工具   路由      测试
 (Codex) (Codex) (Codex) (Codex)  ← 并行
    │     │     │         │
    └─────┼─────┘         │
          ▼               │
        审查              │  Claude（分析强）
          │               │
          └───────┬───────┘
                  ▼
                集成
```

- **规划**：Claude 更适合依赖分析和任务拆解
- **执行**：Codex 更适合快速生成和批量改动
- **审查**：Claude 更适合推理正确性和发现遗漏
- **独立任务**：自动并行执行

## 架构

### Provider 路由

每个流水线阶段都可以绑定不同 AI provider：

```toml
[routing.phase_defaults]
decompose = "claude"
execute = "codex"
review = "claude"
simple = "codex"
```

也可以按任务或阶段覆盖：

```bash
mo do --provider codex "实现分页接口"
mo do --phase-provider execute=codex --phase-provider review=claude "重构支付模块"
```

### DAG 调度器

调度器维护任务依赖图：

1. 找出入度为 0 的节点，也就是没有未完成依赖的任务。
2. 并行执行这些任务，最高支持 150 并发。
3. 任务完成后更新图，解锁下游任务。
4. 重复执行，直到所有任务完成或失败。

### 错误分类

不是所有错误都应该用同一种重试策略：

| 错误 | 策略 |
| --- | --- |
| 限流 | 带抖动的指数退避 |
| 上下文溢出 | 自动压缩上下文，缩短 prompt 后重试 |
| 临时失败 | 最多重试 10 次，默认 30 秒基础退避 |
| Provider 不可用 | auto 模式下切换备用 provider |
| 任务失败 | 向依赖任务传播失败，跳过不可达任务 |

### Simple 模式：批量执行引擎

对于"100 个文件都要做同一个小改动"这类任务：

- 16 个并发 worker
- 失败自动重试
- 语法验证
- 崩溃恢复，断电后可用 `simple resume` 继续

```bash
mo simple run --manifest fixes.jsonl
mo simple resume  # 中断后继续
mo simple retry   # 只重试失败项
```

## 实际效果

**JWT 鉴权功能**：6 个任务，4 个并行，用时 4 分 23 秒，失败 0 个。

**批量 lint 修复**：147 个文件，16 个 worker，8 分钟完成，串行预计超过 40 分钟。

**跨模块重构**：3 个包、15 个文件。DAG 确保执行顺序正确，Claude 审查发现了 3 个 Codex 执行时遗漏的边界情况。

## 技术栈

- Python 3.11+，使用 asyncio 做并发调度
- Pydantic，负责数据模型和配置校验
- SQLite，负责轻量状态持久化
- TOML，提供可读的配置文件

## 试用

```bash
git clone https://github.com/amber132/Master-Orchestrator.git
cd Master-Orchestrator
pip install -e ".[dev]"
mo do "你的目标"
```

需要 Python 3.11+，并且 PATH 中至少能使用 `claude` 或 `codex`。

---

*Master Orchestrator 基于 MIT 开源，欢迎贡献。*
