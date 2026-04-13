"""
自适应线程池模块

提供根据负载动态调整工作线程数量的线程池实现
"""

import logging
import threading
import queue
import time
from concurrent.futures import Future
from typing import Callable, Any, Dict

logger = logging.getLogger(__name__)


class AdaptiveThreadPool:
    """自适应线程池，根据负载动态调整工作线程数量"""

    def __init__(
        self,
        min_workers: int,
        max_workers: int,
        queue_capacity: int,
        adjust_interval: int = 10
    ):
        """
        初始化自适应线程池

        Args:
            min_workers: 最小工作线程数
            max_workers: 最大工作线程数
            queue_capacity: 任务队列容量
            adjust_interval: 调整间隔（秒）
        """
        if min_workers < 1:
            raise ValueError("min_workers 必须至少为 1")
        if max_workers < min_workers:
            raise ValueError("max_workers 不能小于 min_workers")
        if queue_capacity < 1:
            raise ValueError("queue_capacity 必须至少为 1")

        self.min_workers = min_workers
        self.max_workers = max_workers
        self.adjust_interval = adjust_interval

        # 任务队列
        self.task_queue = queue.Queue(maxsize=queue_capacity)

        # 状态变量
        self.current_workers = 0  # 当前工作线程数
        self.target_workers = min_workers  # 目标工作线程数
        self.active_count = 0  # 当前活跃线程数（正在执行任务）
        self.completed_count = 0  # 已完成任务数
        self.shutdown_flag = False  # 关闭标志
        self._accepting = True  # 是否接受新任务
        self._peak_active = 0  # 峰值活跃线程数
        self._total_duration = 0.0  # 任务总耗时（秒）
        self._worker_id_counter = 0  # 线程ID计数器（避免重复）

        # 线程安全锁
        self.lock = threading.Lock()
        self._workers: list[threading.Thread] = []

        # 启动初始工作线程
        for _ in range(min_workers):
            self._add_worker()

        # 启动后台调整线程
        self.adjuster_thread = threading.Thread(
            target=self._adjuster,
            daemon=True,
            name="AdaptivePoolAdjuster"
        )
        self.adjuster_thread.start()

    def _worker(self):
        """工作线程函数，从队列中取任务并执行"""
        # 注册线程
        with self.lock:
            self.current_workers += 1

        try:
            while not self.shutdown_flag:
                # 检查是否需要退出（缩容）
                with self.lock:
                    if self.current_workers > self.target_workers:
                        break

                try:
                    # 从队列中取任务，超时1秒避免阻塞
                    task, future = self.task_queue.get(timeout=1)

                    # 标记为活跃
                    with self.lock:
                        self.active_count += 1
                        self._peak_active = max(self._peak_active, self.active_count)

                    # 记录任务开始时间
                    start_time = time.time()

                    try:
                        # 执行任务
                        result = task()
                        future.set_result(result)
                    except Exception as e:
                        # 捕获异常并设置到 Future
                        future.set_exception(e)
                    finally:
                        # 记录任务耗时
                        duration = time.time() - start_time

                        # 更新状态
                        with self.lock:
                            self.active_count -= 1
                            self.completed_count += 1
                            self._total_duration += duration
                        self.task_queue.task_done()

                except queue.Empty:
                    # 队列为空，继续等待
                    continue

        finally:
            # 注销线程
            with self.lock:
                self.current_workers -= 1

    def _adjuster(self):
        """后台调整线程，定期检查并调整线程池大小"""
        while not self.shutdown_flag:
            time.sleep(self.adjust_interval)

            if self.shutdown_flag:
                break

            workers_to_add = 0
            with self.lock:
                current_workers = self.current_workers
                pending_tasks = self.task_queue.qsize()
                active_workers = self.active_count

                # 补充死亡的 worker（低于 min_workers 时无条件补充）
                if current_workers < self.min_workers:
                    workers_to_add = self.min_workers - current_workers
                    self.target_workers = max(self.target_workers, self.min_workers)
                # 扩容条件：pending_tasks/active_workers > 2
                elif pending_tasks > 0 and pending_tasks / max(active_workers, 1) > 2:
                    if current_workers < self.max_workers:
                        new_workers = max(1, int(current_workers * 0.25))
                        new_workers = min(new_workers, self.max_workers - current_workers)
                        self.target_workers = current_workers + new_workers
                        workers_to_add = new_workers
                # 缩容条件：active_workers 利用率 < 30%
                elif current_workers > 0 and active_workers / max(current_workers, 1) < 0.3:
                    if current_workers > self.min_workers:
                        remove_workers = max(1, int(current_workers * 0.25))
                        remove_workers = min(remove_workers, current_workers - self.min_workers)
                        self.target_workers = current_workers - remove_workers

            # 在锁外启动新线程（避免死锁）
            for _ in range(workers_to_add):
                self._add_worker()

    def _add_worker(self):
        """添加一个工作线程"""
        with self.lock:
            worker_id = self._worker_id_counter
            self._worker_id_counter += 1

        worker = threading.Thread(
            target=self._worker,
            daemon=True,
            name=f"orch-worker_{worker_id}"
        )
        worker.start()
        with self.lock:
            self._workers.append(worker)

    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        """
        提交任务到线程池

        Args:
            fn: 要执行的函数
            *args: 位置参数
            **kwargs: 关键字参数

        Returns:
            Future 对象，可用于获取任务结果

        Raises:
            RuntimeError: 如果线程池已关闭或不再接受新任务
        """
        if self.shutdown_flag or not self._accepting:
            raise RuntimeError("线程池已关闭，无法提交新任务")

        future = Future()
        def task(_fn=fn, _args=args, _kwargs=kwargs):
            return _fn(*_args, **_kwargs)
        self.task_queue.put((task, future))
        return future

    def shutdown(self, wait: bool = True, cancel_futures: bool = False):
        """
        关闭线程池

        Args:
            wait: 是否等待所有任务完成
            cancel_futures: 是否取消未开始的任务（清空队列）
        """
        self.shutdown_flag = True

        if cancel_futures:
            # 清空队列中未开始的任务
            discarded = 0
            while not self.task_queue.empty():
                try:
                    task, future = self.task_queue.get_nowait()
                    future.cancel()
                    self.task_queue.task_done()
                    discarded += 1
                except queue.Empty:
                    break
            if discarded > 0:
                logger.info(f"Cancelled {discarded} pending tasks")

        if wait:
            # 等待队列中的任务完成
            self.task_queue.join()
            # 等待工作线程退出
            for w in self._workers:
                w.join(timeout=5)

        # 等待调整线程结束
        if self.adjuster_thread.is_alive():
            self.adjuster_thread.join(timeout=self.adjust_interval + 1)

    def stop_accepting(self) -> None:
        """停止接受新任务（用于优雅关闭）"""
        with self.lock:
            self._accepting = False

    def get_active_count(self) -> int:
        """获取当前活跃线程数"""
        with self.lock:
            return self.active_count

    def get_pending_count(self) -> int:
        """获取队列中待处理任务数"""
        return self.task_queue.qsize()

    @property
    def pending_count(self) -> int:
        """队列中待处理任务数（属性访问）"""
        return self.task_queue.qsize()

    def metrics(self) -> Dict[str, Any]:
        """
        获取线程池指标

        Returns:
            包含以下键的字典：
            - current_workers: 当前工作线程数
            - active_threads: 当前活跃线程数（正在执行任务）
            - queue_size: 队列中待处理任务数
            - completed_count: 已完成任务总数
            - max_workers: 最大工作线程数
            - peak_active: 峰值活跃线程数
            - avg_task_duration: 平均任务耗时（秒）
        """
        with self.lock:
            completed = self.completed_count
            avg_duration = self._total_duration / max(completed, 1)
            return {
                "current_workers": self.current_workers,
                "active_threads": self.active_count,
                "queue_size": self.task_queue.qsize(),
                "completed_count": completed,
                "max_workers": self.max_workers,
                "peak_active": self._peak_active,
                "avg_task_duration": avg_duration
            }

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出，自动关闭线程池"""
        self.shutdown(wait=True)
        return False
