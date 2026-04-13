"""LLM-friendly workflow schema for DAG representation.

This module provides a simplified schema for representing DAG workflows
in a format that's easier for LLMs to understand and generate.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any

from master_orchestrator.model import DAG, TaskNode, LoopConfig


@dataclass
class LLMTaskSchema:
    """LLM-friendly task representation."""
    id: str
    instruction: str
    inputs: list[str] = field(default_factory=list)
    outputs: list[str] = field(default_factory=list)
    condition: str = ''
    loop: dict[str, Any] | None = None


@dataclass
class LLMWorkflowSchema:
    """LLM-friendly workflow representation."""
    name: str
    description: str
    tasks: list[LLMTaskSchema] = field(default_factory=list)


def dag_to_llm_schema(dag: DAG) -> LLMWorkflowSchema:
    """Convert a DAG to LLM-friendly schema.

    Args:
        dag: The DAG to convert

    Returns:
        LLMWorkflowSchema representation of the DAG
    """
    # 构建反向依赖图，用于推导 outputs
    reverse_deps: dict[str, list[str]] = {}
    for task_id, node in dag.tasks.items():
        for dep in node.depends_on:
            if dep not in reverse_deps:
                reverse_deps[dep] = []
            reverse_deps[dep].append(task_id)

    # 转换每个任务
    llm_tasks: list[LLMTaskSchema] = []
    for task_id, node in dag.tasks.items():
        # 转换 loop 配置为 dict
        loop_dict: dict[str, Any] | None = None
        if node.loop:
            loop_dict = {
                'max_iterations': node.loop.max_iterations,
                'until_condition': node.loop.until_condition,
                'retry_on_failure': node.loop.retry_on_failure,
            }

        llm_task = LLMTaskSchema(
            id=node.id,
            instruction=node.prompt_template,
            inputs=node.depends_on.copy(),
            outputs=reverse_deps.get(task_id, []),
            condition=node.condition or '',
            loop=loop_dict,
        )
        llm_tasks.append(llm_task)

    # 生成描述
    description = f"Workflow with {len(dag.tasks)} tasks, max {dag.max_parallel} parallel"

    return LLMWorkflowSchema(
        name=dag.name,
        description=description,
        tasks=llm_tasks,
    )


def llm_schema_to_dag(schema: LLMWorkflowSchema) -> DAG:
    """Convert LLM-friendly schema to DAG.

    Args:
        schema: The LLM schema to convert

    Returns:
        DAG representation of the schema
    """
    dag = DAG(name=schema.name)

    for llm_task in schema.tasks:
        # 转换 loop dict 为 LoopConfig
        loop_config: LoopConfig | None = None
        if llm_task.loop:
            loop_config = LoopConfig(
                max_iterations=llm_task.loop.get('max_iterations', 5),
                until_condition=llm_task.loop.get('until_condition', ''),
                retry_on_failure=llm_task.loop.get('retry_on_failure', True),
            )

        # 创建 TaskNode
        task_node = TaskNode(
            id=llm_task.id,
            prompt_template=llm_task.instruction,
            depends_on=llm_task.inputs.copy(),
            condition=llm_task.condition if llm_task.condition else None,
            loop=loop_config,
        )

        dag.tasks[llm_task.id] = task_node

    return dag
