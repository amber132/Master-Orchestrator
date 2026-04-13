"""
线程池监控模块

提供线程池运行时指标采集、饥饿检测、瓶颈检测等监控能力。
"""

import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from master_orchestrator.notification import get_notifier

if TYPE_CHECKING:
    from master_orchestrator.adaptive_pool import AdaptiveThreadPool


@dataclass
class ThreadPoolMetrics:
    """线程池指标快照"""
    active_threads: int  # 当前活跃线程数
    queue_size: int  # 队列中等待的任务数
    completed_count: int  # 已完成任务总数
    utilization: float  # 线程池利用率 (0.0-1.0)
    peak_active: int  # 历史峰值活跃线程数
    avg_task_duration: float  # 平均任务执行时长（秒）


@dataclass
class _StarvationState:
    """饥饿状态追踪"""
    is_starving: bool = False
    start_time: Optional[float] = None
    duration: float = 0.0


class ThreadMonitor:
    """
    线程池监控器

    后台守护线程定期采集线程池指标，检测饥饿和瓶颈。
    """

    def __init__(
        self,
        pool: "AdaptiveThreadPool",
        interval: int = 30,
        starvation_threshold: int = 300
    ):
        """
        初始化监控器

        Args:
            pool: 要监控的自适应线程池
            interval: 指标采集间隔（秒）
            starvation_threshold: 饥饿检测阈值（秒），队列持续非空且所有线程忙超过此时长触发告警
        """
        self.pool = pool
        self.interval = interval
        self.starvation_threshold = starvation_threshold

        # 监控状态
        self._running = False
        self._stop_event = threading.Event()
        self._monitor_thread: Optional[threading.Thread] = None

        # 指标和告警（线程安全）
        self._lock = threading.Lock()
        self._latest_metrics: Optional[ThreadPoolMetrics] = None
        self._alerts: list[str] = []

        # 饥饿检测状态
        self._starvation_state = _StarvationState()

        # 任务完成停滞检测
        self._last_completion_time: Optional[float] = None
        self._last_completed_count: int = 0

    def start(self) -> None:
        """启动监控线程"""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._stop_event.clear()

        # 在锁外创建和启动线程，避免持有锁时执行耗时操作
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            daemon=True,
            name="ThreadMonitor"
        )
        self._monitor_thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        """
        停止监控线程

        Args:
            timeout: 等待线程退出的超时时间（秒）
        """
        with self._lock:
            if not self._running:
                return
            self._running = False

        # 在锁外执行停止操作
        self._stop_event.set()

        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=timeout)

    def get_metrics(self) -> Optional[ThreadPoolMetrics]:
        """
        获取最新的指标快照

        Returns:
            最新的指标数据，如果监控未启动则返回 None
        """
        with self._lock:
            return self._latest_metrics

    def get_alerts(self) -> list[str]:
        """
        获取当前所有告警

        Returns:
            告警消息列表
        """
        with self._lock:
            return self._alerts.copy()

    def _monitor_loop(self) -> None:
        """监控主循环（在后台线程中运行）"""
        while not self._stop_event.wait(timeout=self.interval):
            try:
                self._collect_and_analyze()
            except Exception as e:
                # 监控线程不应因异常而崩溃
                with self._lock:
                    self._alerts.append(f"监控异常: {e}")

    def _collect_and_analyze(self) -> None:
        """采集指标并执行分析"""
        # 从线程池获取原始指标
        raw_metrics = self.pool.metrics()

        # 计算利用率
        utilization = self._calculate_utilization(raw_metrics)

        # 构建指标快照
        metrics = ThreadPoolMetrics(
            active_threads=raw_metrics.get("active_threads", 0),
            queue_size=raw_metrics.get("queue_size", 0),
            completed_count=raw_metrics.get("completed_count", 0),
            utilization=utilization,
            peak_active=raw_metrics.get("peak_active", 0),
            avg_task_duration=raw_metrics.get("avg_task_duration", 0.0)
        )

        # 更新指标和告警（在锁内统一处理所有共享状态）
        with self._lock:
            # 更新任务完成时间戳（修复：移到锁内）
            self._update_completion_time(metrics.completed_count)
            
            self._latest_metrics = metrics

            # 检测饥饿
            self._check_starvation(metrics)

            # 检测瓶颈
            self._check_bottleneck(metrics)

    def _calculate_utilization(self, raw_metrics: dict) -> float:
        """
        计算线程池利用率

        Args:
            raw_metrics: 线程池返回的原始指标字典

        Returns:
            利用率 (0.0-1.0)
        """
        active = raw_metrics.get("active_threads", 0)
        max_workers = raw_metrics.get("max_workers", 1)

        if max_workers == 0:
            return 0.0

        return min(1.0, active / max_workers)

    def _update_completion_time(self, completed_count: int) -> None:
        """
        更新任务完成时间戳
        
        注意：此方法必须在持有 self._lock 的情况下调用

        Args:
            completed_count: 当前已完成任务总数
        """
        current_time = time.time()

        # 如果完成数增加，更新时间戳
        if completed_count > self._last_completed_count:
            self._last_completion_time = current_time
            self._last_completed_count = completed_count
        elif self._last_completion_time is None:
            # 首次采集，初始化时间戳
            self._last_completion_time = current_time
            self._last_completed_count = completed_count

    def _check_starvation(self, metrics: ThreadPoolMetrics) -> None:
        """
        检测饥饿：所有线程忙且队列非空超过阈值，或任务完成停滞
        
        注意：此方法必须在持有 self._lock 的情况下调用

        Args:
            metrics: 当前指标快照
        """
        current_time = time.time()

        # 检测1：所有线程忙且队列非空（原有逻辑）
        is_starving = (metrics.utilization >= 1.0 and metrics.queue_size > 0)

        if is_starving:
            if not self._starvation_state.is_starving:
                # 进入饥饿状态
                self._starvation_state.is_starving = True
                self._starvation_state.start_time = current_time
                self._starvation_state.duration = 0.0
            else:
                # 更新饥饿持续时长
                if self._starvation_state.start_time:
                    self._starvation_state.duration = current_time - self._starvation_state.start_time

                    # 超过阈值触发告警
                    if self._starvation_state.duration >= self.starvation_threshold:
                        detail = (
                            f"线程池饥饿: 所有线程忙且队列积压 {metrics.queue_size} 个任务，"
                            f"已持续 {self._starvation_state.duration:.1f} 秒"
                        )
                        self._alerts.append(detail)
                        if len(self._alerts) > 100:
                            self._alerts = self._alerts[-100:]
                        try:
                            get_notifier().warning("线程池饥饿", detail)
                        except Exception:
                            pass
        else:
            # 退出饥饿状态
            if self._starvation_state.is_starving:
                self._starvation_state.is_starving = False
                self._starvation_state.start_time = None
                self._starvation_state.duration = 0.0

        # 检测2：任务完成停滞（新增逻辑）
        if self._last_completion_time is not None and metrics.queue_size > 0:
            stall_duration = current_time - self._last_completion_time

            if stall_duration > self.starvation_threshold:
                detail = (
                    f"任务完成停滞: 已 {stall_duration:.1f} 秒无任务完成，"
                    f"队列积压 {metrics.queue_size} 个任务"
                )
                self._alerts.append(detail)
                if len(self._alerts) > 100:
                    self._alerts = self._alerts[-100:]
                try:
                    get_notifier().warning("任务完成停滞", detail)
                except Exception:
                    pass

    def _check_bottleneck(self, metrics: ThreadPoolMetrics) -> None:
        """
        检测瓶颈：队列利用率过高
        
        注意：此方法必须在持有 self._lock 的情况下调用

        Args:
            metrics: 当前指标快照
        """
        # 从线程池获取真实队列容量
        queue_capacity = None
        if hasattr(self.pool, 'task_queue') and hasattr(self.pool.task_queue, 'maxsize'):
            queue_capacity = self.pool.task_queue.maxsize

        if queue_capacity and queue_capacity > 0:
            queue_utilization = metrics.queue_size / queue_capacity

            if queue_utilization > 0.8:
                detail = (
                    f"队列瓶颈: 队列利用率 {queue_utilization:.1%}，"
                    f"当前积压 {metrics.queue_size}/{queue_capacity} 个任务"
                )
                self._alerts.append(detail)
                if len(self._alerts) > 100:
                    self._alerts = self._alerts[-100:]
                try:
                    get_notifier().warning("队列瓶颈", detail)
                except Exception:
                    pass
