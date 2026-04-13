"""YAML 格式的 DAG 加载器。

提供比 TOML 更友好的任务定义格式，支持：
- 自然缩进
- 列表和字典的直观语法
- 与 CI/CD 工具的兼容性（GitHub Actions 等）
"""
from __future__ import annotations

from pathlib import Path

import yaml

from .dag_loader import _parse_task, _parse_retry_policy, _validate_path
from .exceptions import DAGLoadError
from .model import DAG, RetryPolicy
from .validator import validate_dag


def load_yaml(path: str | Path) -> DAG:
    """从 YAML 文件加载 DAG 定义。

    YAML 格式示例::

        name: my-workflow
        max_parallel: 10
        tasks:
          task_id:
            prompt: "任务描述"
            depends_on: [other_task]
            model: sonnet
    """
    p = _validate_path(Path(path))

    try:
        with open(p, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise DAGLoadError(f"Failed to parse YAML: {e}") from e
    except OSError as e:
        raise DAGLoadError(f"Failed to read YAML file: {e}") from e

    if not isinstance(raw, dict):
        raise DAGLoadError("YAML root must be a mapping")

    name = raw.get("name", p.stem)
    max_parallel = raw.get("max_parallel", 30)
    tasks_raw = raw.get("tasks", {})

    if not tasks_raw:
        raise DAGLoadError("No tasks defined in YAML DAG")

    # 解析默认重试策略
    default_retry_raw = raw.get("retry", {})
    default_retry = (
        _parse_retry_policy(default_retry_raw)
        if default_retry_raw
        else RetryPolicy()
    )

    dag = DAG(name=name, max_parallel=max_parallel)

    for task_id, task_data in tasks_raw.items():
        if not isinstance(task_data, dict):
            raise DAGLoadError(f"Task '{task_id}' must be a mapping")
        dag.tasks[task_id] = _parse_task(task_id, task_data, default_retry)

    # 使用 validator 模块进行验证（strict=True 会抛出异常）
    validate_dag(dag, strict=True)
    return dag
