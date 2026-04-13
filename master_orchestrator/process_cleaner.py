"""
进程清理器 - 管理和清理 Claude CLI 进程

功能：
1. 注册和追踪任务关联的进程
2. 清理超时或僵尸进程
3. 跨平台支持（Windows/Linux）
4. 线程安全
"""

import os
import platform
import subprocess
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Optional, Set

import logging

logger = logging.getLogger(__name__)


@dataclass
class ProcessInfo:
    """进程信息"""
    pid: int
    task_id: str
    registered_at: float  # 注册时间戳
    last_activity: float  # 最后活动时间戳


class ProcessCleaner:
    """
    进程清理器 - 管理 Claude CLI 进程的生命周期

    线程安全，支持跨平台进程清理
    """

    def __init__(self, inactive_timeout: int = 300):
        """
        初始化进程清理器

        Args:
            inactive_timeout: 进程无活动超时时间（秒），默认 5 分钟
        """
        self._lock = threading.Lock()
        self._processes: Dict[int, ProcessInfo] = {}  # pid -> ProcessInfo
        self._inactive_timeout = inactive_timeout
        self._is_windows = platform.system() == "Windows"

    def register_pid(self, pid: int, task_id: str) -> None:
        """
        注册进程

        Args:
            pid: 进程 ID
            task_id: 关联的任务 ID
        """
        with self._lock:
            now = time.time()
            self._processes[pid] = ProcessInfo(
                pid=pid,
                task_id=task_id,
                registered_at=now,
                last_activity=now
            )
            logger.debug(f"注册进程 PID={pid}, task_id={task_id}")

    def unregister_pid(self, pid: int) -> None:
        """
        注销进程

        Args:
            pid: 进程 ID
        """
        with self._lock:
            if pid in self._processes:
                task_id = self._processes[pid].task_id
                del self._processes[pid]
                logger.debug(f"注销进程 PID={pid}, task_id={task_id}")

    def update_activity(self, pid: int) -> None:
        """
        更新进程活动时间（心跳）

        Args:
            pid: 进程 ID
        """
        with self._lock:
            if pid in self._processes:
                self._processes[pid].last_activity = time.time()

    def get_registered_pids(self) -> Set[int]:
        """获取所有已注册的进程 ID"""
        with self._lock:
            return set(self._processes.keys())

    def get_inactive_pids(self, timeout: Optional[int] = None) -> Set[int]:
        """
        获取超时未活动的进程 ID

        Args:
            timeout: 超时时间（秒），None 则使用默认值

        Returns:
            超时进程的 PID 集合
        """
        timeout = timeout or self._inactive_timeout
        now = time.time()
        inactive_pids = set()

        with self._lock:
            for pid, info in self._processes.items():
                if now - info.last_activity > timeout:
                    inactive_pids.add(pid)
                    logger.warning(
                        f"进程 PID={pid} (task_id={info.task_id}) "
                        f"已无活动 {int(now - info.last_activity)} 秒"
                    )

        return inactive_pids

    def cleanup_by_pids(self, pids: Set[int]) -> Dict[int, bool]:
        """
        强制清理指定的进程列表

        Args:
            pids: 要清理的进程 ID 集合

        Returns:
            {pid: success} 字典，记录每个进程的清理结果
        """
        results = {}

        for pid in pids:
            try:
                success = self._kill_process(pid)
                results[pid] = success

                if success:
                    logger.info(f"成功清理进程 PID={pid}")
                    self.unregister_pid(pid)
                else:
                    logger.warning(f"清理进程 PID={pid} 失败")

            except Exception as e:
                logger.error(f"清理进程 PID={pid} 时发生异常: {e}")
                results[pid] = False

        return results

    def cleanup_suspended_processes(self, max_idle_seconds: Optional[int] = None) -> int:
        """
        清理已注册但超时未活动的进程

        安全策略：只清理本实例显式注册的进程，绝不扫描系统全局进程。
        避免误杀用户的交互式 Claude Code 会话或其他无关的 claude 进程。

        Args:
            max_idle_seconds: 最大空闲时间（秒），None 则使用默认值

        Returns:
            清理的进程数量
        """
        max_idle_seconds = max_idle_seconds or self._inactive_timeout

        # 只清理已注册但超时的进程
        inactive_registered = self.get_inactive_pids(max_idle_seconds)

        if not inactive_registered:
            logger.debug("没有需要清理的进程")
            return 0

        logger.info(
            f"准备清理 {len(inactive_registered)} 个超时注册进程"
        )

        # 执行清理
        results = self.cleanup_by_pids(inactive_registered)
        success_count = sum(1 for success in results.values() if success)

        logger.info(f"清理完成: 成功 {success_count}/{len(inactive_registered)}")
        return success_count

    def cleanup_all(self) -> int:
        """
        清理所有已注册的进程

        Returns:
            清理的进程数量
        """
        pids = self.get_registered_pids()

        if not pids:
            logger.debug("没有已注册的进程需要清理")
            return 0

        logger.info(f"准备清理所有已注册的 {len(pids)} 个进程")
        results = self.cleanup_by_pids(pids)
        success_count = sum(1 for success in results.values() if success)

        logger.info(f"清理完成: 成功 {success_count}/{len(pids)}")
        return success_count

    # ── 已移除：系统全局扫描方法（_scan_claude_processes 等） ──
    # 原因：无法区分编排器启动的子进程和用户的交互式 Claude Code 会话，
    # 会误杀用户正在使用的 Claude 进程。现在只清理显式注册的超时进程。

    def _kill_process(self, pid: int) -> bool:
        """
        跨平台 kill 进程

        Args:
            pid: 进程 ID

        Returns:
            是否成功
        """
        try:
            if self._is_windows:
                return self._kill_process_windows(pid)
            else:
                return self._kill_process_unix(pid)
        except Exception as e:
            logger.error(f"Kill 进程 PID={pid} 失败: {e}")
            return False

    def _kill_process_windows(self, pid: int) -> bool:
        """Windows 平台精确 kill 进程（不使用 /T 杀进程树）。

        学习 Claude Code 的 AbortController 模式：精确杀单个 PID，
        不用 taskkill /T 避免误杀同进程树下的其他 claude 实例。
        Job Object 作为最终安全网。
        """
        try:
            # 精确杀单个 PID（不杀进程树）
            result = subprocess.run(
                ["taskkill", "/F", "/PID", str(pid)],
                capture_output=True,
                text=True,
                timeout=10
            )

            # taskkill 返回 0 表示成功，128 表示进程不存在（也算成功）
            return result.returncode in (0, 128)

        except subprocess.TimeoutExpired:
            logger.error(f"taskkill PID={pid} 超时")
            return False
        except Exception as e:
            logger.error(f"Windows kill 进程 PID={pid} 失败: {e}")
            return False

    def _kill_process_unix(self, pid: int) -> bool:
        """Unix/Linux 平台 kill 进程"""
        try:
            # 先尝试 SIGTERM（优雅退出）
            os.kill(pid, 15)  # SIGTERM
            time.sleep(0.5)

            # 检查进程是否还存在
            try:
                os.kill(pid, 0)  # 信号 0 只检查进程是否存在
                # 进程还在，使用 SIGKILL 强制终止
                os.kill(pid, 9)  # SIGKILL
                time.sleep(0.2)
            except ProcessLookupError:
                # 进程已不存在，SIGTERM 成功
                pass

            return True

        except ProcessLookupError:
            # 进程不存在，算作成功
            return True
        except PermissionError:
            logger.error(f"没有权限 kill 进程 PID={pid}")
            return False
        except Exception as e:
            logger.error(f"Unix kill 进程 PID={pid} 失败: {e}")
            return False

    def get_stats(self) -> Dict:
        """
        获取进程清理器统计信息

        Returns:
            统计信息字典
        """
        with self._lock:
            now = time.time()
            inactive_count = 0

            for info in self._processes.values():
                if now - info.last_activity > self._inactive_timeout:
                    inactive_count += 1

            return {
                "total_registered": len(self._processes),
                "inactive_count": inactive_count,
                "inactive_timeout": self._inactive_timeout,
                "platform": "Windows" if self._is_windows else "Unix/Linux"
            }


# 全局单例
_global_cleaner: Optional[ProcessCleaner] = None
_cleaner_lock = threading.Lock()


def get_process_cleaner(inactive_timeout: int = 300) -> ProcessCleaner:
    """
    获取全局进程清理器单例

    Args:
        inactive_timeout: 进程无活动超时时间（秒）

    Returns:
        ProcessCleaner 实例
    """
    global _global_cleaner

    with _cleaner_lock:
        if _global_cleaner is None:
            _global_cleaner = ProcessCleaner(inactive_timeout=inactive_timeout)
        return _global_cleaner
