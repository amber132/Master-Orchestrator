"""Monitoring: logging setup, progress panel, ETA estimation."""

from __future__ import annotations

import json
import logging
import logging.handlers
import sys
import threading
from datetime import datetime, timedelta
from pathlib import Path

from typing import Any

from .log_context import get_run_id, get_task_id
from .log_level_watcher import LogLevelWatcher
from .model import TaskStatus

logger = logging.getLogger("claude_orchestrator")

# 日志轮转配置
_LOG_MAX_BYTES = 50 * 1024 * 1024  # 50 MB
_LOG_BACKUP_COUNT = 5  # 保留 5 个备份

_log_level_watcher: LogLevelWatcher | None = None

_watcher_lock = threading.Lock()


class SafeStreamHandler(logging.StreamHandler):
    """Avoid crashing logging when a previously captured stream gets closed."""

    def emit(self, record: logging.LogRecord) -> None:
        msg = self.format(record)
        try:
            stream = self.stream
            stream.write(msg + self.terminator)
            self.flush()
        except ValueError:
            fallback = sys.stderr
            if fallback is self.stream:
                return
            try:
                self.setStream(fallback)
                self.stream.write(msg + self.terminator)
                self.flush()
            except ValueError:
                return
        except Exception:
            self.handleError(record)


class JsonLinesFormatter(logging.Formatter):
    """将日志记录格式化为 JSON Lines 格式。"""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "thread_name": threading.current_thread().name,
            "run_id": get_run_id(),
            "task_id": get_task_id(),
        }
        if record.exc_info and record.exc_info[0]:
            entry["exception"] = str(record.exc_info[1])
        return json.dumps(entry, ensure_ascii=False)


def setup_logging(log_file: str | Path | None = None, level: int = logging.INFO) -> None:
    """Configure dual-channel logging: console (human) + JSON Lines file (machine).

    日志文件使用 RotatingFileHandler，单文件最大 50MB，保留 5 个备份。
    """
    root = logging.getLogger("claude_orchestrator")
    root.setLevel(level)
    # 清除前先关闭所有处理器，防止资源泄漏
    for handler in root.handlers:
        handler.close()
    root.handlers.clear()

    # Console handler
    console = SafeStreamHandler(sys.stderr)
    console.setLevel(level)
    fmt = logging.Formatter("[%(asctime)s] %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
    console.setFormatter(fmt)
    root.addHandler(console)

    # JSON Lines file handler（带轮转）
    if log_file:
        rotating = logging.handlers.RotatingFileHandler(
            str(log_file),
            maxBytes=_LOG_MAX_BYTES,
            backupCount=_LOG_BACKUP_COUNT,
            encoding="utf-8",
        )
        rotating.setFormatter(JsonLinesFormatter())
        root.addHandler(rotating)

    # 启动动态日志级别监控
    # 使用线程锁保护 watcher 初始化，防止竞态条件
    global _log_level_watcher
    with _watcher_lock:
        if _log_level_watcher is None:
            _log_level_watcher = LogLevelWatcher()
            _log_level_watcher.start()

def shutdown_logging() -> None:
    """停止日志监控线程并清理资源。"""
    global _log_level_watcher
    with _watcher_lock:
        if _log_level_watcher is not None:
            _log_level_watcher.stop()
            _log_level_watcher = None
    
    # 关闭所有日志处理器
    root = logging.getLogger("claude_orchestrator")
    for handler in root.handlers:
        handler.close()
    root.handlers.clear()


class ProgressMonitor:
    """Background thread that periodically prints a progress panel.

    Can operate in two modes:
    - Pull mode (preferred): pass scheduler + budget_tracker, monitor pulls live state
    - Push mode (legacy): call update() to push state from outside
    """

    def __init__(
        self,
        dag_name: str,
        total_tasks: int,
        interval: int = 60,
        scheduler: Any = None,
        budget_tracker: Any = None,
        thread_monitor: Any = None,
    ):
        self._dag_name = dag_name
        self._total = total_tasks
        self._interval = interval
        self._started_at = datetime.now()
        self._lock = threading.Lock()
        self._status_counts: dict[str, int] = {}
        self._cost: float = 0.0
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        # Pull mode refs
        self._scheduler = scheduler
        self._budget_tracker = budget_tracker
        self._thread_monitor = thread_monitor

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True, name="progress-monitor")
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)

    def update(self, status_counts: dict[str, int], cost: float) -> None:
        with self._lock:
            self._status_counts = dict(status_counts)
            self._cost = cost

    def _run(self) -> None:
        while not self._stop_event.wait(self._interval):
            self._print_panel()

    def _get_live_state(self) -> tuple[dict[str, int], float, dict[str, Any]]:
        """Get the latest state, preferring pull mode over cached push data."""
        thread_metrics = {}
        if self._thread_monitor is not None:
            metrics = self._thread_monitor.get_metrics()
            if metrics:
                thread_metrics = {
                    "active_threads": metrics.active_threads,
                    "queue_size": metrics.queue_size,
                    "utilization": metrics.utilization,
                    "peak_active": metrics.peak_active,
                    "completed_count": metrics.completed_count,
                    "avg_task_duration": metrics.avg_task_duration,
                }

        if self._scheduler is not None:
            counts = self._scheduler.summary()
            cost = self._budget_tracker.spent if self._budget_tracker else 0.0
            return counts, cost, thread_metrics
        with self._lock:
            return dict(self._status_counts), self._cost, thread_metrics

    def _print_panel(self) -> None:
        counts, cost, thread_metrics = self._get_live_state()

        done = counts.get(TaskStatus.SUCCESS.value, 0)
        failed = counts.get(TaskStatus.FAILED.value, 0)
        running = counts.get(TaskStatus.RUNNING.value, 0)
        pending = counts.get(TaskStatus.PENDING.value, 0)
        skipped = counts.get(TaskStatus.SKIPPED.value, 0)
        cancelled = counts.get(TaskStatus.CANCELLED.value, 0)

        elapsed = datetime.now() - self._started_at
        elapsed_str = _format_duration(elapsed)

        # ETA estimation based on average task completion rate
        eta_str = "?"
        if done > 0:
            remaining_tasks = pending + running
            avg_per_task = elapsed.total_seconds() / done
            eta_seconds = avg_per_task * remaining_tasks
            eta_str = _format_duration(timedelta(seconds=eta_seconds))

        width = 43
        lines = [
            "+" + "=" * width + "+",
            f"|  DAG: {self._dag_name:<{width - 8}}|",
            f"|  Total: {self._total} | Done: {done} | Failed: {failed:<{width - 30}}|",
            f"|  Running: {running} | Pending: {pending:<{width - 25}}|",
        ]
        if skipped or cancelled:
            lines.append(f"|  Skipped: {skipped} | Cancelled: {cancelled:<{width - 28}}|")

        # Thread pool metrics (if available)
        if thread_metrics:
            active = thread_metrics.get("active_threads", 0)
            queue = thread_metrics.get("queue_size", 0)
            util = thread_metrics.get("utilization", 0.0)
            peak = thread_metrics.get("peak_active", 0)
            lines.append(f"|  Threads: {active} | Queue: {queue} | Util: {util:.1%} | Peak: {peak:<{width - 45}}|")

        lines.extend([
            f"|  Elapsed: {elapsed_str} | ETA: {eta_str:<{width - 22}}|",
            f"|  Cost: ${cost:<{width - 10}.2f}|",
            "+" + "=" * width + "+",
        ])

        panel = "\n".join(lines)
        sys.stderr.write("\n" + panel + "\n\n")
        sys.stderr.flush()

    def print_final(self) -> None:
        """Print the final summary panel."""
        self._print_panel()


def _format_duration(td: timedelta) -> str:
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}h {minutes}m"
    if minutes > 0:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"
