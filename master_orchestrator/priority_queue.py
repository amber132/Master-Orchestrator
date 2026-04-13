"""线程安全的优先级任务队列

使用 heapq 实现最大堆（通过负优先级），支持容量限制和 FIFO 保证。
"""

import heapq
import threading
from typing import Optional

from .model import TaskNode


class PriorityTaskQueue:
    """线程安全的优先级任务队列

    使用 heapq 实现最大堆（通过负优先级），支持容量限制。
    元素按 (-priority, insert_order) 排序，确保：
    1. 高优先级任务先出队
    2. 相同优先级时 FIFO

    示例:
        >>> queue = PriorityTaskQueue(capacity=100)
        >>> task1 = TaskNode(id="t1", prompt_template="...", priority=10)
        >>> task2 = TaskNode(id="t2", prompt_template="...", priority=5)
        >>> queue.push(task1)  # True
        >>> queue.push(task2)  # True
        >>> queue.peek().id    # "t1" (高优先级)
        >>> queue.pop().id     # "t1"
        >>> queue.size         # 1
    """

    def __init__(self, capacity: Optional[int] = None):
        """初始化优先级队列

        Args:
            capacity: 队列容量上限，None 表示无限制
        """
        self._heap: list[tuple[int, int, TaskNode]] = []
        self._lock = threading.Lock()
        self._counter = 0  # 插入顺序计数器，确保 FIFO
        self._capacity = capacity

    def push(self, task: TaskNode) -> bool:
        """入队任务

        Args:
            task: 要入队的任务节点

        Returns:
            True 表示成功入队，False 表示队列已满（拒绝策略）
        """
        with self._lock:
            if self._capacity is not None and len(self._heap) >= self._capacity:
                return False

            # 使用负优先级实现最大堆（高优先级先出）
            heapq.heappush(self._heap, (-task.priority, self._counter, task))
            self._counter += 1
            return True

    def pop(self) -> Optional[TaskNode]:
        """出队最高优先级任务

        Returns:
            最高优先级的任务节点，队列为空时返回 None
        """
        with self._lock:
            if not self._heap:
                return None
            _, _, task = heapq.heappop(self._heap)
            return task

    def peek(self) -> Optional[TaskNode]:
        """查看队首任务但不移除

        Returns:
            最高优先级的任务节点，队列为空时返回 None
        """
        with self._lock:
            if not self._heap:
                return None
            return self._heap[0][2]

    @property
    def size(self) -> int:
        """返回队列当前大小"""
        with self._lock:
            return len(self._heap)

    @property
    def empty(self) -> bool:
        """判断队列是否为空"""
        with self._lock:
            return len(self._heap) == 0

    def pop_lowest(self) -> Optional[TaskNode]:
        """出队最低优先级任务

        用于背压策略中的 DISCARD_OLDEST，移除优先级最低的任务腾出空间。

        Returns:
            最低优先级的任务节点，队列为空时返回 None
        """
        with self._lock:
            if not self._heap:
                return None

            # 找到优先级最低的元素（-priority 最大，即原始 priority 最小）
            max_item = max(self._heap, key=lambda x: x[0])
            self._heap.remove(max_item)
            heapq.heapify(self._heap)  # 重建堆

            return max_item[2]

    def clear(self) -> None:
        """清空队列"""
        with self._lock:
            self._heap.clear()
            self._counter = 0

    def __len__(self) -> int:
        """支持 len(queue) 语法"""
        return self.size

    def __bool__(self) -> bool:
        """支持 if queue: 语法（非空为 True）"""
        return not self.empty
