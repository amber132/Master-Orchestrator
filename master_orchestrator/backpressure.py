"""
背压策略模块

当任务队列满时，提供多种背压处理策略：
- CALLER_RUNS: 在调用线程同步执行任务
- ABORT: 抛出 QueueFullError 异常
- DISCARD: 静默丢弃任务并记录日志
- DISCARD_OLDEST: 弹出最低优先级任务腾出空间
"""

from __future__ import annotations

import logging
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable

from .exceptions import QueueFullError

if TYPE_CHECKING:
    from typing import Protocol

    class PriorityTaskQueue(Protocol):
        """优先级任务队列协议（用于类型检查）"""

        @property
        def size(self) -> int:
            """返回队列当前大小"""
            ...

        @property
        def _capacity(self) -> int | None:
            """返回队列容量上限"""
            ...

        def push(self, item: Any) -> bool:
            """将任务放入队列，返回是否成功"""
            ...

        def pop_lowest(self) -> Any:
            """获取并移除最低优先级的任务"""
            ...


logger = logging.getLogger(__name__)


class BackpressurePolicy(Enum):
    """背压策略枚举"""

    CALLER_RUNS = "caller_runs"  # 在调用线程同步执行
    ABORT = "abort"  # 抛出异常
    DISCARD = "discard"  # 静默丢弃
    DISCARD_OLDEST = "discard_oldest"  # 丢弃最低优先级任务


class BackpressureHandler:
    """背压处理器，根据策略处理队列满的情况"""

    def __init__(
        self,
        policy: BackpressurePolicy = BackpressurePolicy.CALLER_RUNS,
        queue: Any = None,  # 运行时类型为 PriorityTaskQueue
    ):
        """
        初始化背压处理器

        Args:
            policy: 背压策略，默认为 CALLER_RUNS
            queue: 优先级任务队列实例
        """
        self.policy = policy
        self.queue = queue

    def handle(self, task: Any, executor_fn: Callable[[Any], Any]) -> Any:
        """
        处理任务提交，当队列满时根据策略执行相应操作

        Args:
            task: 要提交的任务对象
            executor_fn: 执行任务的函数，接受 task 作为参数

        Returns:
            任务执行结果（仅 CALLER_RUNS 策略会返回结果）

        Raises:
            QueueFullError: 当策略为 ABORT 且队列已满时抛出
        """
        # 检查队列是否已满
        is_full = (
            self.queue is not None
            and hasattr(self.queue, '_capacity')
            and self.queue._capacity is not None
            and self.queue.size >= self.queue._capacity
        )

        if self.queue is None or not is_full:
            # 队列未满，正常放入
            if self.queue is not None:
                self.queue.push(task)
            return None

        # 队列已满，根据策略处理
        if self.policy == BackpressurePolicy.CALLER_RUNS:
            logger.warning(
                "Queue is full, executing task in caller thread (CALLER_RUNS policy)"
            )
            return executor_fn(task)

        elif self.policy == BackpressurePolicy.ABORT:
            logger.error("Queue is full, aborting task submission (ABORT policy)")
            raise QueueFullError(
                "Task queue is full and backpressure policy is ABORT",
                context={"task": str(task), "policy": self.policy.value}
            )

        elif self.policy == BackpressurePolicy.DISCARD:
            logger.warning(
                "Queue is full, discarding task (DISCARD policy): %s",
                task
            )
            return None

        elif self.policy == BackpressurePolicy.DISCARD_OLDEST:
            logger.warning(
                "Queue is full, discarding oldest task to make room (DISCARD_OLDEST policy)"
            )
            try:
                # 弹出最低优先级任务
                discarded = self.queue.pop_lowest()
                logger.info("Discarded lowest priority task: %s", discarded)

                # 放入新任务
                self.queue.push(task)
            except Exception as e:
                logger.error(
                    "Failed to discard oldest task: %s. Falling back to DISCARD policy.",
                    e
                )
                # 回退到 DISCARD 策略
                return None

        return None

    def __repr__(self) -> str:
        return f"BackpressureHandler(policy={self.policy.value})"
