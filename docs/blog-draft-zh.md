# 我如何让 Claude Code 和 Codex 协作完成大型编码任务 — Master Orchestrator 的设计与实践

> 这篇文章介绍 Master Orchestrator 的设计动机、架构思路和实际使用效果。可发布到掘金、知乎、V2EX 等平台。

---

## 背景：单 Agent 的天花板

用 Claude Code 或 Codex 做过大型重构的人大概都遇到过这些场景：

1. **上下文溢出**：一个涉及 15 个文件的重构，跑到第 8 个文件时 agent 已经忘了前面改了什么，导致 import 不一致、接口不匹配。

2. **串行瓶颈**：100 个独立的 lint 修复，每个要 30 秒，串行跑完要 50 分钟。明明可以并行，但单 agent 做不到。

3. **工具错配**：规划任务需要强推理能力（Claude 擅长），批量代码生成需要速度（Codex 擅长）。用一个模型干所有事，两头都亏。

4. **手动编排**：你成了人肉调度器 —— 跑完第一个 agent，检查输出，喂给下一个，再检查，再喂。这叫什么自动化？

## 思路：把编排逻辑从人脑搬到代码

核心想法很简单：**把一个大目标拆成有依赖关系的子任务图（DAG），然后自动调度执行。**

```
目标: "给 Express 项目加 JWT 认证"
        │
        ▼
   ┌─────────────┐
   │  分解 (Claude) │  ← 规划能力强
   └──────┬──────┘
          │
    ┌─────┼─────┬─────────┐
    ▼     ▼     ▼         ▼
  中间件  工具函数  路由     测试
 (Codex) (Codex) (Codex) (Codex)  ← 并行执行
    │     │     │         │
    └─────┼─────┘         │
          ▼               │
      代码审查 (Claude)    │  ← 审查能力强
          │               │
          └───────┬───────┘
                  ▼
            集成验证
```

这样一来：
- **规划**用 Claude（推理更强）
- **执行**用 Codex（速度更快）
- **审查**回到 Claude（更擅长找问题）
- **独立任务**自动并行，有依赖的自动等待

## 架构设计

### Provider 路由

每个执行阶段可以绑定不同的 AI provider：

```toml
[routing.phase_defaults]
decompose = "claude"    # 规划阶段用 Claude
execute = "codex"       # 执行阶段用 Codex
review = "claude"       # 审查阶段用 Claude
simple = "codex"        # 批量任务用 Codex
```

也可以在命令行覆盖：

```bash
# 整个任务用 Codex
mo do --provider codex "实现分页接口"

# 按阶段混用
mo do --phase-provider execute=codex --phase-provider review=claude "重构支付模块"
```

### DAG 调度器

调度器维护一个任务依赖图，核心逻辑：

1. 找出所有入度为 0 的节点（没有未完成的依赖）
2. 并行执行这些节点（最多 150 个并发）
3. 任务完成后更新依赖图，解锁下游任务
4. 重复直到所有任务完成或失败

### 错误分类与恢复

不是所有错误都应该重试。系统会分类处理：

| 错误类型 | 策略 |
|---------|------|
| Rate limit | 指数退避 + 抖动，最多等 5 分钟 |
| Context overflow | 自动压缩上下文，缩减 prompt 重试 |
| 瞬态错误 | 最多重试 10 次，30 秒基础退避 |
| Provider 宕机 | 自动模式下切换到备用 provider |
| 任务失败 | 传播到下游依赖，跳过不可达任务 |

### 收敛检测

自主执行模式下，系统持续监控：

- **高原信号**：连续多轮没有进展 → 提升策略或切换方向
- **退化信号**：质量指标下降 → 回滚到上一个好的状态
- **回归信号**：之前通过的测试开始失败 → 立即停止并告警

### Simple 模式：批量执行引擎

对于"100 个文件都需要做同样的小改动"这类任务，DAG 模式太重了。Simple 模式提供：

- 最多 16 个并行 worker
- 自动重试失败项
- 语法检查验证
- 崩溃恢复（断电后 `simple resume` 继续）

```bash
# 从 JSONL 清单批量执行
mo simple run --manifest lint-fixes.jsonl

# 中断后恢复
mo simple resume

# 只重试失败的
mo simple retry
```

## 实际使用效果

### 场景 1：给 Express 项目加 JWT 认证

```bash
mo do "Add JWT authentication to the Express app with middleware, routes, tests, and API docs"
```

系统自动分解为 6 个任务，其中 4 个并行执行。总耗时 4 分 23 秒，0 失败。

### 场景 2：批量修复 lint 错误

147 个文件需要修复，Simple 模式用 16 个 worker 并行跑，8 分钟完成（串行估计要 40+ 分钟）。

### 场景 3：跨模块重构

支付模块重构涉及 3 个包、15 个文件。DAG 确保了正确的执行顺序，Claude 审查发现了 3 个 Codex 执行时遗漏的边界情况。

## 技术栈选择

- **Python 3.11+**：asyncio 原生支持，并发调度的核心
- **Pydantic**：数据模型和配置验证
- **SQLite**：轻量级状态持久化，无需额外数据库
- **TOML**：人类友好的配置格式

## 与现有方案的区别

| 维度 | 单 Agent | Multi-Agent 框架 | Master Orchestrator |
|------|---------|-----------------|-------------------|
| 定位 | 工具调用 | 通用 Agent 协作 | 编码任务专用编排 |
| 并行 | 无 | 有限 | 150 并发 |
| Provider | 单一 | 通常单一 | 按阶段混用 |
| 错误恢复 | 手动 | 基础重试 | 智能分类 + 退避 |
| 批量模式 | 无 | 无 | Simple 模式 |
| 收敛检测 | 无 | 无 | 有 |

## 开源与未来

Master Orchestrator 已经开源在 GitHub：[amber132/Master-Orchestrator](https://github.com/amber132/Master-Orchestrator)

接下来的计划：
- 支持更多 AI provider（如 Gemini Code Assist）
- Web UI 仪表盘
- DAG 可视化
- 社区贡献的验证器和模板

如果你也在用 AI 做大型编码任务，欢迎试用和反馈。

---

*标签：#AI编程 #Claude #Codex #自动化 #编排器 #开源*
