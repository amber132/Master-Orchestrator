"""
并发组管理器

管理多个命名并发组，每组有独立的并发限制。
用于控制不同类型任务（如不同模型）的并发执行数量。
"""

import threading
from typing import Optional


class ConcurrencyGroupManager:
    """
    并发组管理器

    管理多个命名并发组，每组有独立的 max_concurrent 限制。
    未配置的组不受限制。

    示例:
        manager = ConcurrencyGroupManager({'opus': 3, 'sonnet': 20})

        if manager.acquire('opus'):
            try:
                # 执行 opus 任务
                pass
            finally:
                manager.release('opus')
    """

    def __init__(self, group_limits: Optional[dict[str, int]] = None):
        """
        初始化并发组管理器

        Args:
            group_limits: 各组的并发限制，如 {'opus': 3, 'sonnet': 20}
                         未指定的组不受限制
        """
        self._limits = group_limits or {}
        self._current = {group: 0 for group in self._limits}
        self._lock = threading.Lock()

    def acquire(self, group: str) -> bool:
        """
        非阻塞尝试获取槽位

        Args:
            group: 组名

        Returns:
            True 表示成功获取槽位，False 表示已达上限
            未配置的组总是返回 True
        """
        with self._lock:
            # 未配置的组不限制
            if group not in self._limits:
                return True

            # 检查是否达到上限
            if self._current[group] >= self._limits[group]:
                return False

            # 获取槽位
            self._current[group] += 1
            return True

    def release(self, group: str) -> None:
        """
        释放槽位

        Args:
            group: 组名
        """
        with self._lock:
            # 未配置的组无需释放
            if group not in self._limits:
                return

            # 释放槽位（防止负数）
            if self._current[group] > 0:
                self._current[group] -= 1

    def get_usage(self, group: str) -> tuple[int, int]:
        """
        获取组的使用情况

        Args:
            group: 组名

        Returns:
            (当前使用数, 最大限制)
            未配置的组返回 (0, 0)
        """
        with self._lock:
            if group not in self._limits:
                return (0, 0)
            return (self._current[group], self._limits[group])

    def get_all_usage(self) -> dict[str, tuple[int, int]]:
        """
        获取所有组的使用情况

        Returns:
            {组名: (当前使用数, 最大限制)}
        """
        with self._lock:
            return {
                group: (self._current[group], self._limits[group])
                for group in self._limits
            }
