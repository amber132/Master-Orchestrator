"""Pydantic v2 验证器模块，为 TaskNode 和 DAG 提供严格的数据验证。"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from master_orchestrator.exceptions import DAGValidationError
from master_orchestrator.model import DAG, TaskNode


class TaskNodeValidator(BaseModel):
    """TaskNode 的 Pydantic v2 验证模型"""
    
    id: str = Field(..., min_length=1, description="任务唯一标识符")
    prompt_template: str = Field(..., min_length=1, description="Prompt 模板，不能为空")
    timeout: int = Field(default=1800, gt=0, description="超时时间（秒），必须为正整数")
    model: str | None = Field(default=None, description="模型名称")
    depends_on: list[str] = Field(default_factory=list, description="依赖的任务 ID 列表")
    
    @field_validator('model')
    @classmethod
    def validate_model(cls, v: str | None) -> str | None:
        """验证模型名称必须是 opus/sonnet/haiku 之一"""
        if v is not None:
            allowed_models = {'opus', 'sonnet', 'haiku'}
            if v not in allowed_models:
                raise ValueError(f"model 必须是 {allowed_models} 之一，当前值: {v}")
        return v
    
    @field_validator('prompt_template')
    @classmethod
    def validate_prompt_not_empty(cls, v: str) -> str:
        """验证 prompt_template 不能为空字符串"""
        if not v or not v.strip():
            raise ValueError("prompt_template 不能为空字符串")
        return v
    
    @field_validator('depends_on')
    @classmethod
    def validate_depends_on(cls, v: list[str]) -> list[str]:
        """验证 depends_on 列表中的任务 ID 不能为空"""
        for dep_id in v:
            if not dep_id or not dep_id.strip():
                raise ValueError(f"depends_on 中包含空任务 ID")
        return v


def validate_task_node(task: TaskNode) -> list[str]:
    """
    验证单个 TaskNode 的字段合法性。
    
    Args:
        task: 要验证的 TaskNode 实例
        
    Returns:
        错误消息列表，空列表表示验证通过
    """
    errors: list[str] = []
    
    try:
        TaskNodeValidator(
            id=task.id,
            prompt_template=task.prompt_template,
            timeout=task.timeout,
            model=task.model,
            depends_on=task.depends_on,
        )
    except Exception as e:
        errors.append(f"任务 '{task.id}' 验证失败: {str(e)}")
    
    return errors


def validate_dag(dag: DAG, strict: bool = True) -> list[str]:
    """
    验证 DAG 的完整性，包括：
    1. 所有任务的字段合法性（使用 Pydantic 验证）
    2. 所有 depends_on 引用的任务 ID 必须存在
    3. 不存在循环依赖
    
    Args:
        dag: 要验证的 DAG 实例
        strict: 是否启用严格模式（验证所有 TaskNode 字段）
        
    Returns:
        错误消息列表，空列表表示验证通过
        
    Raises:
        DAGValidationError: 当 strict=True 且发现错误时抛出异常
    """
    errors: list[str] = []
    
    # 1. 验证所有任务的字段合法性
    for task_id, task in dag.tasks.items():
        task_errors = validate_task_node(task)
        errors.extend(task_errors)
    
    # 2. 验证所有任务 ID 引用的合法性
    all_task_ids = set(dag.tasks.keys())
    for task_id, task in dag.tasks.items():
        for dep_id in task.depends_on:
            if dep_id not in all_task_ids:
                errors.append(f"任务 '{task_id}' 依赖的任务 '{dep_id}' 不存在")
    
    # 3. 检测循环依赖（使用 DFS）
    visited: set[str] = set()
    rec_stack: set[str] = set()
    
    def _detect_cycle(task_id: str, path: list[str]) -> bool:
        """DFS 检测循环依赖"""
        visited.add(task_id)
        rec_stack.add(task_id)
        path.append(task_id)
        
        if task_id in dag.tasks:
            for dep_id in dag.tasks[task_id].depends_on:
                if dep_id not in all_task_ids:
                    # 跳过不存在的依赖（已在上面报告）
                    continue
                    
                if dep_id in rec_stack:
                    # 发现循环
                    cycle_start = path.index(dep_id)
                    cycle_path = " -> ".join(path[cycle_start:] + [dep_id])
                    errors.append(f"检测到循环依赖: {cycle_path}")
                    return True
                    
                if dep_id not in visited:
                    if _detect_cycle(dep_id, path.copy()):
                        return True
        
        rec_stack.remove(task_id)
        return False
    
    # 对所有未访问的任务执行 DFS
    for task_id in dag.tasks:
        if task_id not in visited:
            _detect_cycle(task_id, [])
    
    # 如果启用严格模式且发现错误，抛出异常
    if strict and errors:
        error_msg = "DAG 验证失败:\n" + "\n".join(f"  - {e}" for e in errors)
        raise DAGValidationError(error_msg)
    
    return errors


def validate_dag_safe(dag: DAG) -> tuple[bool, list[str]]:
    """
    安全的 DAG 验证函数，不抛出异常。
    
    Args:
        dag: 要验证的 DAG 实例
        
    Returns:
        (is_valid, errors) 元组
        - is_valid: 是否验证通过
        - errors: 错误消息列表
    """
    try:
        errors = validate_dag(dag, strict=False)
        return (len(errors) == 0, errors)
    except Exception as e:
        return (False, [f"验证过程发生异常: {str(e)}"])
