"""Schema version management and migration for DAG definitions."""

from __future__ import annotations
from typing import Any

CURRENT_VERSION = '2.0.0'

# 默认字段值定义
DAG_DEFAULTS = {
    'version': CURRENT_VERSION,
    'max_parallel': 10,
    'timeout': 3600,
    'model': 'sonnet',
    'retry_policy': {
        'max_attempts': 3,
        'backoff_base': 30.0,
        'backoff_multiplier': 2.0,
        'jitter': True,
        'max_delay': 300.0
    },
    'error_handling': {
        'on_error': 'fail',
        'continue_on_error': False
    },
    'context': {},
    'output_dir': './outputs',
    'tags': [],
    'metadata': {},
    'lifecycle_hooks': {},
    'resource_limits': {},
    'priority': 0,
    'lane': 'main'
}

TASK_DEFAULTS = {
    'model': None,  # 继承 DAG 的 model
    'timeout': None,  # 继承 DAG 的 timeout
    'depends_on': [],
    'retry_policy': None,  # 继承 DAG 的 retry_policy
    'error_handling': None,  # 继承 DAG 的 error_handling
    'context': {},
    'output_format': 'text',
    'cache_key': None,
    'idempotent': False,
    'priority': 0,
    'lane': 'main',
    'resource_requirements': {},
    'lifecycle_hooks': {},
    'validation_rules': [],
    'tags': [],
    'metadata': {}
}


def get_version(dag_dict: dict[str, Any]) -> str:
    """
    从 DAG 字典中提取版本号。
    
    Args:
        dag_dict: DAG 定义字典
        
    Returns:
        版本号字符串，如果未指定则返回 None
    """
    # 尝试从顶层 'version' 字段获取
    if 'version' in dag_dict:
        return dag_dict['version']
    
    # 尝试从 'dag' 子字典获取（TOML 格式）
    if 'dag' in dag_dict and isinstance(dag_dict['dag'], dict):
        if 'version' in dag_dict['dag']:
            return dag_dict['dag']['version']
    
    # 未找到版本号
    return None


def migrate_dag(dag_dict: dict[str, Any], from_version: str | None = None) -> dict[str, Any]:
    """
    将 DAG 定义从旧版本迁移到当前版本。
    
    Args:
        dag_dict: 原始 DAG 定义字典
        from_version: 源版本号，如果为 None 则自动检测
        
    Returns:
        迁移后的 DAG 定义字典
    """
    # 自动检测版本
    if from_version is None:
        from_version = get_version(dag_dict)
    
    # 已经是最新版本，无需迁移
    if from_version == CURRENT_VERSION:
        return dag_dict
    
    # 创建副本避免修改原始数据
    migrated = dag_dict.copy()
    
    # 从 None 或 '1.0.0' 迁移到 '2.0.0'
    if from_version is None or from_version == '1.0.0':
        migrated = _migrate_to_2_0_0(migrated)
    
    return migrated


def _migrate_to_2_0_0(dag_dict: dict[str, Any]) -> dict[str, Any]:
    """
    迁移到 2.0.0 版本：添加缺失的字段默认值。
    
    新增字段：
    - DAG 级别: lifecycle_hooks, resource_limits, priority, lane
    - Task 级别: cache_key, idempotent, priority, lane, resource_requirements,
                 lifecycle_hooks, validation_rules, tags, metadata
    """
    migrated = dag_dict.copy()
    
    # 处理 TOML 格式（有 'dag' 子字典）
    if 'dag' in migrated and isinstance(migrated['dag'], dict):
        dag_section = migrated['dag'].copy()
        
        # 添加 DAG 级别的缺失字段
        for key, default_value in DAG_DEFAULTS.items():
            if key not in dag_section:
                dag_section[key] = default_value
        
        # 更新版本号
        dag_section['version'] = CURRENT_VERSION
        
        migrated['dag'] = dag_section
        
        # 处理任务定义
        if 'tasks' in migrated and isinstance(migrated['tasks'], dict):
            tasks_section = {}
            for task_id, task_def in migrated['tasks'].items():
                if isinstance(task_def, dict):
                    task_copy = task_def.copy()
                    for key, default_value in TASK_DEFAULTS.items():
                        if key not in task_copy:
                            task_copy[key] = default_value
                    tasks_section[task_id] = task_copy
                else:
                    tasks_section[task_id] = task_def
            migrated['tasks'] = tasks_section
    
    # 处理 Python dict 格式（扁平结构）
    else:
        # 添加 DAG 级别的缺失字段
        for key, default_value in DAG_DEFAULTS.items():
            if key not in migrated:
                migrated[key] = default_value
        
        # 更新版本号
        migrated['version'] = CURRENT_VERSION
        
        # 处理任务定义
        if 'tasks' in migrated and isinstance(migrated['tasks'], dict):
            tasks_dict = {}
            for task_id, task_def in migrated['tasks'].items():
                if isinstance(task_def, dict):
                    task_copy = task_def.copy()
                    for key, default_value in TASK_DEFAULTS.items():
                        if key not in task_copy:
                            task_copy[key] = default_value
                    tasks_dict[task_id] = task_copy
                else:
                    tasks_dict[task_id] = task_def
            migrated['tasks'] = tasks_dict
    
    return migrated


def validate_version(version: str) -> bool:
    """
    验证版本号格式是否有效（语义化版本）。
    
    Args:
        version: 版本号字符串
        
    Returns:
        True 如果格式有效，否则 False
    """
    if not version:
        return False
    
    parts = version.split('.')
    if len(parts) != 3:
        return False
    
    try:
        for part in parts:
            int(part)
        return True
    except ValueError:
        return False
