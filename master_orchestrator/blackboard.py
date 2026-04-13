"""
Blackboard - 线程安全的共享知识空间

用于任务间共享事实、假设和中间结果。
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from threading import Lock
from typing import Any, Callable, Optional


class Category(str, Enum):
    """知识条目类别"""
    FACTS = "facts"
    HYPOTHESES = "hypotheses"
    INTERMEDIATE_RESULTS = "intermediate_results"


@dataclass
class Entry:
    """知识条目"""
    category: str
    key: str
    value: Any
    source_task: str
    timestamp: datetime = field(default_factory=datetime.now)


class Blackboard:
    """线程安全的共享知识空间

    用于任务间共享和查询知识条目，支持订阅通知。
    """

    def __init__(self):
        self._entries: list[Entry] = []
        self._lock = Lock()
        self._subscribers: dict[str, list[Callable[[Entry], None]]] = {
            Category.FACTS.value: [],
            Category.HYPOTHESES.value: [],
            Category.INTERMEDIATE_RESULTS.value: []
        }

    def post(self, category: str, key: str, value: Any, source_task: str) -> None:
        """发布知识条目

        Args:
            category: 类别，必须是 facts/hypotheses/intermediate_results 之一
            key: 条目键
            value: 条目值
            source_task: 来源任务ID

        Raises:
            ValueError: 如果 category 不是有效值
        """
        if category not in [c.value for c in Category]:
            raise ValueError(
                f"Invalid category '{category}'. Must be one of: "
                f"{', '.join(c.value for c in Category)}"
            )

        entry = Entry(
            category=category,
            key=key,
            value=value,
            source_task=source_task
        )

        with self._lock:
            self._entries.append(entry)
            # 内存防护：限制条目数量上限
            if len(self._entries) > 1000:
                self._entries = self._entries[-1000:]
            # 通知订阅者
            for callback in self._subscribers.get(category, []):
                try:
                    callback(entry)
                except Exception:
                    # 订阅者回调失败不应影响发布
                    pass

    def query(
        self,
        category: Optional[str] = None,
        key: Optional[str] = None
    ) -> list[Entry]:
        """查询知识条目

        Args:
            category: 可选，按类别过滤
            key: 可选，按键过滤

        Returns:
            匹配的条目列表
        """
        with self._lock:
            results = list(self._entries)

        if category is not None:
            results = [e for e in results if e.category == category]

        if key is not None:
            results = [e for e in results if e.key == key]

        return results

    def subscribe(self, category: str, callback: Callable[[Entry], None]) -> None:
        """订阅某类别的新条目通知

        Args:
            category: 要订阅的类别
            callback: 回调函数，接收 Entry 参数

        Raises:
            ValueError: 如果 category 不是有效值
        """
        if category not in [c.value for c in Category]:
            raise ValueError(
                f"Invalid category '{category}'. Must be one of: "
                f"{', '.join(c.value for c in Category)}"
            )

        with self._lock:
            self._subscribers[category].append(callback)

    def get_snapshot(self) -> dict:
        """获取当前全部知识的快照

        Returns:
            按类别组织的知识快照字典
        """
        with self._lock:
            snapshot = {
                Category.FACTS.value: [],
                Category.HYPOTHESES.value: [],
                Category.INTERMEDIATE_RESULTS.value: []
            }

            for entry in self._entries:
                snapshot[entry.category].append({
                    "key": entry.key,
                    "value": entry.value,
                    "source_task": entry.source_task,
                    "timestamp": entry.timestamp.isoformat()
                })

            return snapshot
