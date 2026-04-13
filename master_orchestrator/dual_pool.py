"""
双线程池管理器模块

提供 IO 密集型和 CPU 密集型任务的独立线程池管理
"""

import logging
import os
import threading
from concurrent.futures import Future
from typing import Callable, Any, Dict, Literal

from .adaptive_pool import AdaptiveThreadPool

logger = logging.getLogger(__name__)

TaskType = Literal["io", "cpu"]


class DualPoolManager:
    """双线程池管理器，为 IO 和 CPU 任务提供独立的线程池"""

    def __init__(
        self,
        max_parallel: int,
        min_parallel: int = 1,
        queue_capacity: int = 1000,
        adjust_interval: int = 10,
        cpu_max_workers: int | None = None
    ):
        """
        初始化双线程池管理器

        Args:
            max_parallel: IO 池的最大工作线程数
            min_parallel: IO 池的最小工作线程数
            queue_capacity: 每个池的任务队列容量
            adjust_interval: 调整间隔（秒）
            cpu_max_workers: CPU 池的最大工作线程数，默认为 os.cpu_count() or 4
        """
        if max_parallel < 1:
            raise ValueError("max_parallel 必须至少为 1")
        if min_parallel < 1:
            raise ValueError("min_parallel 必须至少为 1")
        if min_parallel > max_parallel:
            raise ValueError("min_parallel 不能大于 max_parallel")

        # 确定 CPU 池的最大线程数
        if cpu_max_workers is None:
            cpu_max_workers = os.cpu_count() or 4
        if cpu_max_workers < 1:
            raise ValueError("cpu_max_workers 必须至少为 1")

        self.max_parallel = max_parallel
        self.min_parallel = min_parallel
        self.cpu_max_workers = cpu_max_workers
        self.queue_capacity = queue_capacity
        self.adjust_interval = adjust_interval

        # 线程安全锁（用于协调两个池的操作）
        self._lock = threading.Lock()
        self._shutdown = False

        # 创建 IO 池（大并发）
        self.io_pool = AdaptiveThreadPool(
            min_workers=min_parallel,
            max_workers=max_parallel,
            queue_capacity=queue_capacity,
            adjust_interval=adjust_interval
        )
        logger.info(
            f"IO pool initialized: min={min_parallel}, max={max_parallel}, "
            f"queue_capacity={queue_capacity}"
        )

        # 创建 CPU 池（小并发）
        cpu_min_workers = min(1, cpu_max_workers)
        self.cpu_pool = AdaptiveThreadPool(
            min_workers=cpu_min_workers,
            max_workers=cpu_max_workers,
            queue_capacity=queue_capacity,
            adjust_interval=adjust_interval
        )
        logger.info(
            f"CPU pool initialized: min={cpu_min_workers}, max={cpu_max_workers}, "
            f"queue_capacity={queue_capacity}"
        )

    def submit(
        self,
        task_type: TaskType,
        fn: Callable,
        *args,
        **kwargs
    ) -> Future:
        """
        提交任务到对应的线程池

        Args:
            task_type: 任务类型，'io' 或 'cpu'
            fn: 要执行的函数
            *args: 位置参数
            **kwargs: 关键字参数

        Returns:
            Future 对象，可用于获取任务结果

        Raises:
            ValueError: 如果 task_type 不是 'io' 或 'cpu'
            RuntimeError: 如果线程池已关闭
        """
        with self._lock:
            if self._shutdown:
                raise RuntimeError("DualPoolManager 已关闭，无法提交新任务")

        if task_type == "io":
            return self.io_pool.submit(fn, *args, **kwargs)
        elif task_type == "cpu":
            return self.cpu_pool.submit(fn, *args, **kwargs)
        else:
            raise ValueError(
                f"无效的 task_type: {task_type}，必须是 'io' 或 'cpu'"
            )

    def shutdown(self, wait: bool = True, cancel_futures: bool = False):
        """
        关闭双线程池

        Args:
            wait: 是否等待所有任务完成
            cancel_futures: 是否取消未开始的任务
        """
        with self._lock:
            if self._shutdown:
                logger.warning("DualPoolManager 已经关闭")
                return
            self._shutdown = True

        logger.info(
            f"Shutting down DualPoolManager (wait={wait}, cancel_futures={cancel_futures})"
        )

        # 关闭两个池
        self.io_pool.shutdown(wait=wait, cancel_futures=cancel_futures)
        self.cpu_pool.shutdown(wait=wait, cancel_futures=cancel_futures)

        logger.info("DualPoolManager shutdown complete")

    def stop_accepting(self) -> None:
        """停止接受新任务（用于优雅关闭）"""
        with self._lock:
            if self._shutdown:
                return

        logger.info("DualPoolManager stopping acceptance of new tasks")
        self.io_pool.stop_accepting()
        self.cpu_pool.stop_accepting()

    def get_active_count(self, task_type: TaskType) -> int:
        """
        获取指定池的当前活跃线程数

        Args:
            task_type: 任务类型，'io' 或 'cpu'

        Returns:
            活跃线程数

        Raises:
            ValueError: 如果 task_type 不是 'io' 或 'cpu'
        """
        if task_type == "io":
            return self.io_pool.get_active_count()
        elif task_type == "cpu":
            return self.cpu_pool.get_active_count()
        else:
            raise ValueError(
                f"无效的 task_type: {task_type}，必须是 'io' 或 'cpu'"
            )

    def get_pending_count(self, task_type: TaskType) -> int:
        """
        获取指定池的队列中待处理任务数

        Args:
            task_type: 任务类型，'io' 或 'cpu'

        Returns:
            待处理任务数

        Raises:
            ValueError: 如果 task_type 不是 'io' 或 'cpu'
        """
        if task_type == "io":
            return self.io_pool.get_pending_count()
        elif task_type == "cpu":
            return self.cpu_pool.get_pending_count()
        else:
            raise ValueError(
                f"无效的 task_type: {task_type}，必须是 'io' 或 'cpu'"
            )

    def get_total_active_count(self) -> int:
        """获取两个池的总活跃线程数"""
        return self.io_pool.get_active_count() + self.cpu_pool.get_active_count()

    def get_total_pending_count(self) -> int:
        """获取两个池的总待处理任务数"""
        return self.io_pool.get_pending_count() + self.cpu_pool.get_pending_count()

    @property
    def pending_count(self) -> int:
        """总待处理任务数（属性访问）"""
        return self.get_total_pending_count()

    def metrics(self) -> Dict[str, Any]:
        """
        获取双线程池的聚合指标

        Returns:
            包含以下键的字典：
            - io_pool: IO 池的详细指标
            - cpu_pool: CPU 池的详细指标
            - total_active_threads: 两个池的总活跃线程数
            - total_queue_size: 两个池的总队列大小
            - total_completed_count: 两个池的总完成任务数
            - total_current_workers: 两个池的总当前工作线程数
        """
        io_metrics = self.io_pool.metrics()
        cpu_metrics = self.cpu_pool.metrics()

        return {
            "io_pool": io_metrics,
            "cpu_pool": cpu_metrics,
            "total_active_threads": io_metrics["active_threads"] + cpu_metrics["active_threads"],
            "total_queue_size": io_metrics["queue_size"] + cpu_metrics["queue_size"],
            "total_completed_count": io_metrics["completed_count"] + cpu_metrics["completed_count"],
            "total_current_workers": io_metrics["current_workers"] + cpu_metrics["current_workers"]
        }

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出，自动关闭双线程池"""
        self.shutdown(wait=True)
        return False
