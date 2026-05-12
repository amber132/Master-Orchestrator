# Simple 任务文件

`simple --task-file` 支持 `jsonl` 和 `csv`。

标准字段：

- `target`
- `instruction`
- `bucket`
- `priority`
- `timeout_seconds`
- `max_attempts`
- `verify_commands`
- `require_patterns`
- `metadata`

`verify_commands` 和 `require_patterns` 可以是：

- JSON 数组
- 多行文本

`metadata` 推荐使用 JSON 对象。
