"""Field transformation utilities for extracting and mapping task outputs."""

from typing import Any

from master_orchestrator.model import TaskNode
from master_orchestrator.exceptions import FieldTransformError


def apply_transforms(task: TaskNode, outputs: dict) -> dict[str, Any]:
    """Apply field transformations from upstream task outputs.

    遍历任务的 transform 列表，从 outputs 字典中按点分路径提取值，
    返回 {target_key: value} 映射。路径不存在时使用 default，
    仍失败则抛出 FieldTransformError。

    Args:
        task: 包含转换规则的任务节点
        outputs: 所有任务输出的字典，以任务 ID 为键

    Returns:
        目标键到提取值的映射字典

    Raises:
        FieldTransformError: 当必需字段无法提取且没有默认值时

    Examples:
        >>> task = TaskNode(
        ...     id="process",
        ...     prompt_template="...",
        ...     transform=[
        ...         FieldTransform(source_path="scan.output.files", target_key="input_files"),
        ...         FieldTransform(source_path="config.timeout", target_key="max_time", default=300),
        ...     ]
        ... )
        >>> outputs = {
        ...     "scan": {"output": {"files": ["a.py", "b.py"]}},
        ...     "config": {}
        ... }
        >>> apply_transforms(task, outputs)
        {'input_files': ['a.py', 'b.py'], 'max_time': 300}
    """
    if not task.transform:
        return {}

    result: dict[str, Any] = {}

    for transform in task.transform:
        try:
            # 按点分路径提取值
            value = _extract_by_path(outputs, transform.source_path)
            result[transform.target_key] = value
        except (KeyError, TypeError) as e:
            # 路径不存在，尝试使用默认值
            if transform.default is not None:
                result[transform.target_key] = transform.default
            else:
                # 没有默认值，抛出异常
                raise FieldTransformError(
                    f"Failed to extract '{transform.source_path}' for target '{transform.target_key}' "
                    f"and no default value provided",
                    context={
                        'task_id': task.id,
                        'source_path': transform.source_path,
                        'target_key': transform.target_key,
                        'original_error': str(e),
                        'available_keys': list(outputs.keys()),
                    }
                ) from e

    return result


def _extract_by_path(data: dict, path: str) -> Any:
    """从嵌套字典中按点分路径提取值。

    Args:
        data: 要提取的字典
        path: 点分路径，如 'scan.output.files'

    Returns:
        提取的值

    Raises:
        KeyError: 如果路径中的任何键不存在
        TypeError: 如果中间值不是字典类型

    Examples:
        >>> data = {"scan": {"output": {"files": ["a.py"]}}}
        >>> _extract_by_path(data, "scan.output.files")
        ['a.py']
    """
    parts = path.split('.')
    current = data

    for i, part in enumerate(parts):
        if not isinstance(current, dict):
            raise TypeError(
                f"Cannot access key '{part}' on non-dict value at path segment {i}: "
                f"expected dict, got {type(current).__name__}"
            )
        current = current[part]

    return current
