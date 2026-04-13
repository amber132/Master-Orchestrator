"""任务验证模块：提供单任务自检和任务间交叉验证能力"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

from master_orchestrator.model import TaskNode, TaskResult, TaskStatus


@dataclass
class ValidationResult:
    """验证结果"""
    passed: bool
    level: str  # 'intra-task' 或 'inter-task'
    issues: list[str] = field(default_factory=list)

    def __bool__(self) -> bool:
        return self.passed


class IntraTaskValidator:
    """单任务自检验证器"""

    # 常见错误标记模式
    ERROR_PATTERNS = [
        r'(?i)\berror\b.*occurred',
        r'(?i)exception.*raised',
        r'(?i)failed\s+to\s+\w+',
        r'(?i)could\s+not\s+\w+',
        r'(?i)unable\s+to\s+\w+',
        r'(?i)traceback.*most\s+recent\s+call',
        r'(?i)fatal\s+error',
        r'(?i)critical\s+error',
    ]

    @classmethod
    def validate(cls, task_node: TaskNode, result: TaskResult) -> ValidationResult:
        """
        单任务自检验证

        Args:
            task_node: 任务节点配置
            result: 任务执行结果

        Returns:
            ValidationResult: 验证结果
        """
        issues: list[str] = []

        # 1. 检查任务是否成功完成
        if result.status != TaskStatus.SUCCESS:
            issues.append(f"任务状态为 {result.status.value}，非 SUCCESS")
            return ValidationResult(passed=False, level='intra-task', issues=issues)

        # 2. 检查输出非空
        if not result.output or result.output.strip() == '':
            issues.append("任务输出为空")

        # 3. 检查是否有明显的错误标记
        if result.output:
            for pattern in cls.ERROR_PATTERNS:
                if re.search(pattern, result.output):
                    issues.append(f"输出中包含错误标记: {pattern}")
                    break

        # 4. 检查 error 字段
        if result.error:
            issues.append(f"任务包含错误信息: {result.error[:200]}")

        # 5. 检查格式是否符合 output_schema
        if task_node.output_schema and task_node.output_format == 'json':
            schema_issues = cls._validate_schema(result, task_node.output_schema)
            issues.extend(schema_issues)

        # 6. 检查 parsed_output（如果期望 JSON 输出）
        if task_node.output_format == 'json' and result.parsed_output is None:
            if result.output and result.output.strip():
                issues.append("期望 JSON 输出但 parsed_output 为 None")

        passed = len(issues) == 0
        return ValidationResult(passed=passed, level='intra-task', issues=issues)

    @staticmethod
    def _validate_schema(result: TaskResult, schema: dict[str, Any]) -> list[str]:
        """
        验证输出是否符合 schema

        Args:
            result: 任务结果
            schema: 期望的输出 schema

        Returns:
            list[str]: 发现的问题列表
        """
        issues: list[str] = []

        # 如果没有 parsed_output，尝试解析 output
        data = result.parsed_output
        if data is None and result.output:
            try:
                data = json.loads(result.output)
            except json.JSONDecodeError:
                issues.append("输出不是有效的 JSON 格式")
                return issues

        if data is None:
            issues.append("无法获取解析后的输出数据")
            return issues

        # 简单的 schema 验证：检查必需字段
        if 'required' in schema and isinstance(schema['required'], list):
            if not isinstance(data, dict):
                issues.append(f"期望输出为字典类型，实际为 {type(data).__name__}")
                return issues

            for field in schema['required']:
                if field not in data:
                    issues.append(f"缺少必需字段: {field}")

        # 检查字段类型（如果 schema 中定义了 properties）
        if 'properties' in schema and isinstance(data, dict):
            for field, field_schema in schema['properties'].items():
                if field in data and 'type' in field_schema:
                    expected_type = field_schema['type']
                    actual_value = data[field]

                    # 简单的类型检查
                    type_valid = IntraTaskValidator._check_type(actual_value, expected_type)
                    if not type_valid:
                        issues.append(
                            f"字段 '{field}' 类型不匹配: 期望 {expected_type}, "
                            f"实际 {type(actual_value).__name__}"
                        )

        return issues

    @staticmethod
    def _check_type(value: Any, expected_type: str) -> bool:
        """检查值是否符合期望类型"""
        type_map = {
            'string': str,
            'number': (int, float),
            'integer': int,
            'boolean': bool,
            'array': list,
            'object': dict,
            'null': type(None),
        }

        expected_py_type = type_map.get(expected_type)
        if expected_py_type is None:
            return True  # 未知类型，跳过检查

        return isinstance(value, expected_py_type)


class InterTaskValidator:
    """任务间交叉验证器"""

    @classmethod
    def validate(
        cls,
        task_id: str,
        result: TaskResult,
        related_results: dict[str, TaskResult]
    ) -> ValidationResult:
        """
        任务间交叉验证

        Args:
            task_id: 当前任务 ID
            result: 当前任务结果
            related_results: 相关任务的结果字典 {task_id: TaskResult}

        Returns:
            ValidationResult: 验证结果
        """
        issues: list[str] = []

        # 1. 检查输出不矛盾（基于关键字段）
        contradiction_issues = cls._check_contradictions(task_id, result, related_results)
        issues.extend(contradiction_issues)

        # 2. 检查共享文件无冲突修改
        file_conflict_issues = cls._check_file_conflicts(task_id, result, related_results)
        issues.extend(file_conflict_issues)

        passed = len(issues) == 0
        return ValidationResult(passed=passed, level='inter-task', issues=issues)

    @staticmethod
    def _check_contradictions(
        task_id: str,
        result: TaskResult,
        related_results: dict[str, TaskResult]
    ) -> list[str]:
        """
        检查任务输出是否存在矛盾

        策略：
        - 如果多个任务输出包含相同的键（如 user_id, file_path 等），检查值是否一致
        - 如果输出中包含布尔判断（如 is_valid, should_proceed），检查是否冲突
        """
        issues: list[str] = []

        # 获取当前任务的 parsed_output
        current_data = result.parsed_output
        if not isinstance(current_data, dict):
            return issues  # 非字典类型，跳过矛盾检查

        # 遍历相关任务，查找共同字段
        for other_id, other_result in related_results.items():
            if other_id == task_id:
                continue

            other_data = other_result.parsed_output
            if not isinstance(other_data, dict):
                continue

            # 查找共同字段
            common_keys = set(current_data.keys()) & set(other_data.keys())
            for key in common_keys:
                current_val = current_data[key]
                other_val = other_data[key]

                # 如果值不同，且都不是复杂对象，报告矛盾
                if current_val != other_val:
                    if isinstance(current_val, (str, int, float, bool)) and \
                       isinstance(other_val, (str, int, float, bool)):
                        issues.append(
                            f"字段 '{key}' 在任务 {task_id} 和 {other_id} 中存在矛盾: "
                            f"{current_val} vs {other_val}"
                        )

        return issues

    @staticmethod
    def _check_file_conflicts(
        task_id: str,
        result: TaskResult,
        related_results: dict[str, TaskResult]
    ) -> list[str]:
        """
        检查共享文件是否存在冲突修改

        策略：
        - 从输出中提取文件路径（通过正则或 parsed_output 中的 files 字段）
        - 检查是否有多个任务修改了同一文件
        """
        issues: list[str] = []

        # 提取当前任务修改的文件
        current_files = InterTaskValidator._extract_modified_files(result)
        if not current_files:
            return issues

        # 检查其他任务是否也修改了相同文件
        for other_id, other_result in related_results.items():
            if other_id == task_id:
                continue

            other_files = InterTaskValidator._extract_modified_files(other_result)
            common_files = current_files & other_files

            if common_files:
                issues.append(
                    f"任务 {task_id} 和 {other_id} 同时修改了文件: "
                    f"{', '.join(sorted(common_files))}"
                )

        return issues

    @staticmethod
    def _extract_modified_files(result: TaskResult) -> set[str]:
        """
        从任务结果中提取修改的文件列表

        策略：
        1. 优先从 parsed_output 中提取 'files', 'modified_files', 'changed_files' 等字段
        2. 如果没有，尝试从 output 文本中用正则提取文件路径
        """
        files: set[str] = set()

        # 策略 1: 从 parsed_output 提取
        if isinstance(result.parsed_output, dict):
            for key in ['files', 'modified_files', 'changed_files', 'file_list']:
                if key in result.parsed_output:
                    file_list = result.parsed_output[key]
                    if isinstance(file_list, list):
                        files.update(str(f) for f in file_list if f)
                    elif isinstance(file_list, str):
                        files.add(file_list)

        # 策略 2: 从 output 文本中提取（正则匹配常见文件路径模式）
        if not files and result.output:
            # 匹配常见文件路径模式（简化版）
            file_patterns = [
                r'(?:modified|changed|updated|created|edited)\s+(?:file\s+)?[\'"]?([^\s\'"]+\.\w+)[\'"]?',
                r'(?:file|path):\s*[\'"]?([^\s\'"]+\.\w+)[\'"]?',
            ]

            for pattern in file_patterns:
                matches = re.findall(pattern, result.output, re.IGNORECASE)
                files.update(matches)

        return files
