"""DAG definition loader — TOML files and Python DSL."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import tomllib
from pathlib import Path

from .exceptions import DAGLoadError, DAGValidationError
from .llm_schema import LLMTaskSchema, LLMWorkflowSchema, llm_schema_to_dag
from .schema_version import CURRENT_VERSION, migrate_dag
from .model import (
    DAG,
    ErrorPolicy,
    FieldTransform,
    LifecycleHooks,
    LinkMapping,
    LoopConfig,
    RetryPolicy,
    TaskNode,
)
from .provider_router import normalize_task_executor
from .validator import validate_dag


def _validate_path(path: Path, check_extension: str | None = None) -> Path:
    """验证路径安全性，防止路径遍历攻击

    Args:
        path: 要验证的路径
        check_extension: 如果提供，验证文件扩展名

    Returns:
        解析后的绝对路径

    Raises:
        DAGLoadError: 路径无效或不安全
    """
    try:
        # 解析为绝对路径（解析符号链接）
        resolved = path.resolve()

        # 确保文件存在
        if not resolved.exists():
            raise DAGLoadError(f"DAG file not found: {path}")

        # 确保是常规文件（不是目录或特殊文件）
        if not resolved.is_file():
            raise DAGLoadError(f"Path is not a regular file: {path}")

        # 验证文件扩展名
        if check_extension and resolved.suffix != check_extension:
            raise DAGLoadError(f"Expected {check_extension} file, got: {resolved.suffix}")

        return resolved
    except DAGLoadError:
        raise
    except Exception as e:
        raise DAGLoadError(f"Invalid or inaccessible path: {path}") from e


def _parse_retry_policy(raw: dict) -> RetryPolicy:
    return RetryPolicy(
        max_attempts=raw.get("max_attempts", 3),
        backoff_base=raw.get("backoff_base", 30.0),
        backoff_multiplier=raw.get("backoff_multiplier", 2.0),
    )


def _parse_task(task_id: str, raw: dict, default_retry: RetryPolicy) -> TaskNode:
    retry_raw = raw.get("retry", raw.get("retry_policy"))
    retry = _parse_retry_policy(retry_raw) if retry_raw else default_retry

    # 解析循环配置
    loop_raw = raw.get("loop")
    loop = None
    if loop_raw:
        loop = LoopConfig(
            max_iterations=loop_raw.get("max_iterations", 5),
            until_condition=loop_raw.get("until_condition", ""),
            retry_on_failure=loop_raw.get("retry_on_failure", True),
        )

    # 解析字段转换规则
    transform_raw = raw.get("transform")
    transform = None
    if transform_raw:
        transform = [
            FieldTransform(
                source_path=t["source_path"],
                target_key=t["target_key"],
                default=t.get("default"),
            )
            for t in transform_raw
        ]

    # 解析错误处理策略
    error_policy_raw = raw.get("error_policy")
    error_policy = None
    if error_policy_raw:
        error_policy = ErrorPolicy(
            on_error=error_policy_raw.get("on_error", "fail-fast"),
            error_handler=error_policy_raw.get("error_handler"),
            classify_errors=error_policy_raw.get("classify_errors", True),
        )

    # 解析任务间数据链接
    links_raw = raw.get("links", [])
    links = [
        LinkMapping(
            upstream_task=link["upstream_task"],
            output_path=link["output_path"],
            input_key=link["input_key"],
        )
        for link in links_raw
    ]

    raw_type = raw.get('type', 'agent_cli')
    raw_provider = raw.get('provider', 'auto')
    node = TaskNode(
        id=task_id,
        prompt_template=raw.get("prompt", ""),
        depends_on=raw.get("depends_on", []),
        timeout=raw.get("timeout", 1800),
        retry_policy=retry,
        model=raw.get("model"),
        complexity=raw.get("complexity"),
        output_format=raw.get("output_format", "json"),
        output_schema=raw.get("output_schema"),
        working_dir=raw.get("working_dir"),
        allowed_tools=raw.get("allowed_tools"),
        system_prompt=raw.get("system_prompt"),
        max_budget_usd=raw.get("max_budget_usd"),
        max_turns=raw.get("max_turns"),
        condition=raw.get("condition"),
        tags=raw.get("tags", []),
        preload_skills=raw.get("preload_skills"),
        validation_gate=raw.get("validation_gate"),
        color=raw.get("color"),
        loop=loop,
        transform=transform,
        error_policy=error_policy,
        links=links,
        priority=raw.get('priority', 0),
        is_sequential=raw.get('is_sequential', False),
        task_type=raw.get('task_type', 'io'),
        concurrency_group=raw.get('concurrency_group'),
        is_read_only=raw.get('is_read_only', raw.get('read_only', False)),
        is_critical=raw.get('is_critical', False),
        lane=raw.get('lane', 'main'),
        idempotent=raw.get('idempotent', False),
        provider=raw_provider,
        type=raw_type,
        executor_config=raw.get('executor_config'),
        env_overrides=raw.get('env_overrides'),
        extra_args=raw.get('extra_args'),
        ephemeral=raw.get('ephemeral', False),
    )
    return normalize_task_executor(node)


def load_toml(path: str | Path) -> DAG:
    """Load a DAG definition from a TOML file."""
    p = _validate_path(Path(path), check_extension=".toml")

    try:
        with open(p, "rb") as f:
            raw = tomllib.load(f)
    except Exception as e:
        raise DAGLoadError(f"Failed to parse TOML: {e}") from e

    meta = raw.get("meta", {})

    # 读取 [dag] 段（优先）或 [meta] 段（向后兼容）
    dag_section = raw.get("dag", meta)

    # 读取 schema_version 并进行迁移
    file_version = dag_section.get("schema_version", "1.0.0")

    # 执行 schema 版本迁移（在创建 DAG 对象之前对原始字典进行迁移）
    if file_version != CURRENT_VERSION:
        raw = migrate_dag(raw, from_version=file_version)
        # 重新获取 dag_section（因为 migrate_dag 可能修改了结构）
        meta = raw.get("meta", {})
        dag_section = raw.get("dag", meta)

    dag = DAG(
        name=dag_section.get("name", p.stem),
        max_parallel=dag_section.get("max_parallel", 30),
        schema_version=CURRENT_VERSION,  # 迁移后总是使用当前版本
    )

    # 解析生命周期钩子

    # 解析生命周期钩子
    hooks_raw = raw.get("hooks", meta.get("hooks"))
    if hooks_raw and isinstance(hooks_raw, dict):
        dag.hooks = LifecycleHooks(
            on_task_start=hooks_raw.get("on_task_start"),
            on_task_complete=hooks_raw.get("on_task_complete"),
            on_task_fail=hooks_raw.get("on_task_fail"),
        )

    default_retry_raw = meta.get("retry", {})
    default_retry = _parse_retry_policy(default_retry_raw) if default_retry_raw else RetryPolicy()

    tasks_section = raw.get("tasks", {})
    if not tasks_section:
        raise DAGLoadError("No [tasks] section found in DAG file")

    for task_id, task_raw in tasks_section.items():
        if not isinstance(task_raw, dict):
            raise DAGLoadError(f"Task '{task_id}' must be a table")
        dag.tasks[task_id] = _parse_task(task_id, task_raw, default_retry)

    # 使用新的 validator 模块进行验证（strict=True 会抛出异常）
    validate_dag(dag, strict=True)

    return dag


def load_python(path: str | Path) -> DAG:
    """Load a DAG from a Python file.

    The file can define either:
    1. A module-level `dag` variable of type DAG (legacy)
    2. A `build()` function that returns a DAG (recommended)

    WARNING: This function executes arbitrary Python code. Only load DAG files from trusted sources.
    """
    p = _validate_path(Path(path), check_extension=".py")

    spec = importlib.util.spec_from_file_location("_dag_module", str(p))
    if spec is None or spec.loader is None:
        raise DAGLoadError(f"Cannot load Python module from {p}")

    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as e:
        raise DAGLoadError(f"Failed to execute DAG Python file: {e}") from e

    # 优先尝试 build() 函数（新方式）
    build_func = getattr(module, "build", None)
    if callable(build_func):
        try:
            dag = build_func()
        except Exception as e:
            raise DAGLoadError(f"Failed to execute build() function: {e}") from e

        if not isinstance(dag, DAG):
            raise DAGLoadError(f"build() function must return a DAG instance, got {type(dag)}")
    else:
        # 回退到模块级 dag 变量（旧方式）
        dag = getattr(module, "dag", None)
        if not isinstance(dag, DAG):
            raise DAGLoadError("Python DAG file must define either a 'build()' function or a module-level 'dag' variable of type DAG")

    # 使用 validator 进行验证

    # 设置 schema_version（如果未设置）
    if not hasattr(dag, "schema_version") or dag.schema_version is None:
        dag.schema_version = "1.0.0"  # Python DAG 默认为旧版本

    # 执行 schema 版本迁移
    if dag.schema_version != CURRENT_VERSION:
        dag.schema_version = CURRENT_VERSION

    try:
        validate_dag(dag, strict=True)
    except Exception as e:
        raise DAGValidationError(f"DAG validation failed: {e}") from e

    return dag


def load_llm_json(path: str | Path) -> DAG:
    """Load a DAG definition from an LLM-friendly JSON file."""
    p = _validate_path(Path(path), check_extension=".json")

    try:
        with open(p, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except json.JSONDecodeError as e:
        raise DAGLoadError(f"Failed to parse JSON: {e}") from e
    except Exception as e:
        raise DAGLoadError(f"Failed to read JSON file: {e}") from e

    # 解析为 LLMWorkflowSchema
    try:
        # 解析 tasks 列表
        tasks_raw = raw.get("tasks", [])
        tasks = []
        for i, task in enumerate(tasks_raw):
            try:
                tasks.append(LLMTaskSchema(
                    id=task["id"],
                    instruction=task["instruction"],
                    inputs=task.get("inputs", []),
                    outputs=task.get("outputs", []),
                    condition=task.get("condition", ""),
                    loop=task.get("loop"),
                ))
            except KeyError as e:
                raise DAGLoadError(f"Task #{i} (id: {task.get('id', 'unknown')}) missing required field: {e}") from e

        schema = LLMWorkflowSchema(
            name=raw.get("name", p.stem),
            description=raw.get("description", ""),
            tasks=tasks,
        )
    except KeyError as e:
        raise DAGLoadError(f"Missing required field in JSON: {e}") from e
    except Exception as e:
        raise DAGLoadError(f"Failed to parse LLM schema: {e}") from e

    # 转换为 DAG
    dag = llm_schema_to_dag(schema)

    # 使用新的 validator 模块进行验证（strict=True 会抛出异常）
    validate_dag(dag, strict=True)

    return dag


def load_dag(path: str | Path) -> DAG:
    """Auto-detect format and load a DAG definition."""
    p = Path(path)
    suffix = p.suffix.lower()

    if suffix == ".py":
        return load_python(p)
    elif suffix == ".toml":
        return load_toml(p)
    elif suffix in (".yaml", ".yml"):
        # lazy import，避免未安装 pyyaml 时 crash
        from .yaml_loader import load_yaml
        return load_yaml(p)
    elif suffix == ".json":
        return load_llm_json(p)
    else:
        raise DAGLoadError(f"Unsupported DAG file format: {suffix} (use .toml, .py, .yaml, or .json)")


def dag_hash(path: str | Path) -> str:
    """Compute a hash of the DAG file for change detection.

    Uses streaming read to avoid memory exhaustion on large files.
    """
    p = Path(path)

    # 检查文件大小，防止资源耗尽
    try:
        file_size = p.stat().st_size
    except OSError as e:
        raise DAGLoadError(f"Cannot access file: {path}") from e

    max_size = 100 * 1024 * 1024  # 100MB
    if file_size > max_size:
        raise DAGLoadError(f"DAG file too large: {file_size} bytes (max: {max_size})")

    # 使用流式读取计算哈希
    hasher = hashlib.sha256()
    try:
        with open(p, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                hasher.update(chunk)
    except OSError as e:
        raise DAGLoadError(f"Failed to read file for hashing: {path}") from e

    return hasher.hexdigest()[:16]
