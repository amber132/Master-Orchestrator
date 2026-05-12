# Simple 模式

`simple` 模式服务“海量独立 work-item，高吞吐，强覆盖，轻推理”的任务。

适合：

- 源码注释补全
- 规则化小改动
- 大量独立文件修补
- 从 CSV / JSONL 驱动的批量执行

不适合：

- 需要阶段分解、策略迭代、AI review 的复杂任务
- 跨文件强依赖的重构
- 依赖闭环收敛判断的问题

核心流程：

1. 摄取 work-item
2. 分桶并调度
3. 通过 `codex exec` 执行
4. 运行轻量验证管线
5. 重试失败项
6. 输出 manifest 和覆盖率汇总

运行控制：

- `simple cancel`：正式停机，优先终止匹配进程，再把未完成 item 收口为 `blocked`
- `simple reconcile`：修复断电/崩溃后的残留活跃态，默认把残留 item 重置为 `ready`
- `simple resume`：继续上次 run
- `simple retry`：仅重试 `failed/blocked` 项

默认补完策略：

- 前台 `simple run` 本身就会应用 completion policy，不需要额外的公开 `simple watch` 子命令
- 当 run 进入 `partial_success` / `failed` 时，会自动继续重试失败集
- 默认退出条件不是固定小波次，而是：
  - 失败集清零
  - 或连续多轮没有净进展
  - 或连续多轮失败集合完全一致
- `cancelled` 默认视为人工终止，不会自动复活

说明：

- 外部 watchdog 逻辑仍存在于代码库中，但不是当前跨平台公开 CLI 面的一部分

推荐配置：

```toml
[simple]
completion_until_clean_enabled = true
completion_max_retry_waves = 0
completion_max_stagnant_waves = 4
completion_max_identical_failure_waves = 3
completion_retry_cancelled_runs = false
retry_feedback_in_prompt_enabled = true
```
