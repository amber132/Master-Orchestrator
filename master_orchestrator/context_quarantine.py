"""
任务输出隔离模块

用于标记和管理可疑任务输出，防止污染上下文传播。
"""

import threading
from typing import Any, Optional


class ContextQuarantine:
    """
    任务输出隔离器

    线程安全地管理任务输出的隔离状态，防止可疑输出污染后续任务。
    """

    def __init__(self):
        """初始化隔离器"""
        self._quarantined: dict[str, str] = {}  # task_id -> reason
        self._lock = threading.Lock()

    def quarantine(self, task_id: str, reason: str) -> None:
        """
        标记任务输出为隔离状态

        Args:
            task_id: 任务 ID
            reason: 隔离原因
        """
        with self._lock:
            self._quarantined[task_id] = reason

    def is_quarantined(self, task_id: str) -> bool:
        """
        检查任务是否被隔离

        Args:
            task_id: 任务 ID

        Returns:
            True 如果任务被隔离，否则 False
        """
        with self._lock:
            return task_id in self._quarantined

    def get_safe_output(self, task_id: str, outputs: Any) -> Optional[Any]:
        """
        安全获取任务输出

        Args:
            task_id: 任务 ID
            outputs: 原始输出

        Returns:
            若任务被隔离返回 None，否则返回原输出
        """
        with self._lock:
            if task_id in self._quarantined:
                return None
            return outputs

    def release(self, task_id: str) -> None:
        """
        解除任务隔离

        Args:
            task_id: 任务 ID
        """
        with self._lock:
            self._quarantined.pop(task_id, None)

    def get_quarantine_reason(self, task_id: str) -> Optional[str]:
        """
        获取隔离原因

        Args:
            task_id: 任务 ID

        Returns:
            隔离原因，若未隔离返回 None
        """
        with self._lock:
            return self._quarantined.get(task_id)

    def get_all_quarantined(self) -> dict[str, str]:
        """
        获取所有被隔离的任务

        Returns:
            task_id -> reason 的字典副本
        """
        with self._lock:
            return self._quarantined.copy()

    def clear(self) -> None:
        """清空所有隔离记录"""
        with self._lock:
            self._quarantined.clear()
