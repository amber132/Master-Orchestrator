"""
优雅关闭管理器

提供三阶段关闭流程：
1. stop_accepting() - 禁止新任务提交
2. await_completion() - 等待已提交任务完成
3. force_terminate() - 强制关闭并清理孤儿进程
"""

import logging
import os
import signal
import subprocess
import sys
import threading
import time
from typing import TYPE_CHECKING, Set, Optional

if TYPE_CHECKING:
    from master_orchestrator.adaptive_pool import AdaptiveThreadPool

logger = logging.getLogger(__name__)


class GracefulShutdownManager:
    """
    优雅关闭管理器

    管理线程池的三阶段关闭流程，跟踪子进程并在必要时强制终止。
    """

    def __init__(self, pool: "AdaptiveThreadPool", timeout: int = 30, store=None, run_id: str | None = None):
        """
        初始化优雅关闭管理器

        Args:
            pool: 自适应线程池实例
            timeout: 等待任务完成的超时时间（秒）
            store: 可选的 Store 实例，用于持久化被丢弃的任务状态
            run_id: 可选的运行 ID，配合 store 使用
        """
        self.pool = pool
        self.timeout = timeout
        self._store = store
        self._run_id = run_id
        self._accepting = True
        self._active_pids: Set[int] = set()
        self._lock = threading.Lock()
        self._shutdown_initiated = False

        logger.info(f"GracefulShutdownManager 初始化完成，超时设置: {timeout}秒")

    def register_subprocess(self, pid: int) -> None:
        """
        注册活跃子进程 PID

        Args:
            pid: 子进程 PID
        """
        with self._lock:
            self._active_pids.add(pid)
            logger.debug(f"注册子进程 PID: {pid}，当前活跃进程数: {len(self._active_pids)}")

    def unregister_subprocess(self, pid: int) -> None:
        """
        注销子进程 PID

        Args:
            pid: 子进程 PID
        """
        with self._lock:
            self._active_pids.discard(pid)
            logger.debug(f"注销子进程 PID: {pid}，当前活跃进程数: {len(self._active_pids)}")

    def stop_accepting(self) -> None:
        """
        阶段 1: 停止接受新任务

        设置标志位，禁止新任务提交到线程池。
        """
        with self._lock:
            if self._shutdown_initiated:
                logger.warning("关闭流程已启动，忽略重复的 stop_accepting 调用")
                return

            self._accepting = False
            self._shutdown_initiated = True

        logger.info("【阶段 1】停止接受新任务 - 已设置标志位，禁止新提交")

        # 通知线程池停止接受新任务
        if hasattr(self.pool, 'stop_accepting'):
            self.pool.stop_accepting()

    def is_accepting(self) -> bool:
        """
        检查是否仍在接受新任务

        Returns:
            True 如果仍在接受新任务，否则 False
        """
        with self._lock:
            return self._accepting

    def await_completion(self, timeout: Optional[int] = None) -> bool:
        """
        阶段 2: 等待已提交任务完成

        Args:
            timeout: 等待超时时间（秒），None 使用构造时的默认值

        Returns:
            True 如果所有任务在超时前完成，否则 False
        """
        wait_timeout = timeout if timeout is not None else self.timeout
        logger.info(f"【阶段 2】等待已提交任务完成 - 超时: {wait_timeout}秒")

        start_time = time.time()
        check_interval = 0.5  # 每 0.5 秒检查一次

        while True:
            elapsed = time.time() - start_time

            # 检查线程池状态
            active_tasks = self._get_active_task_count()

            if active_tasks == 0:
                logger.info(f"【阶段 2】所有任务已完成，耗时: {elapsed:.2f}秒")
                return True

            if elapsed >= wait_timeout:
                logger.warning(
                    f"【阶段 2】等待超时 ({wait_timeout}秒)，"
                    f"仍有 {active_tasks} 个任务未完成"
                )
                return False

            # 定期输出进度
            if int(elapsed) % 5 == 0 and elapsed > 0:
                logger.info(
                    f"【阶段 2】等待中... 剩余任务: {active_tasks}，"
                    f"已等待: {elapsed:.1f}秒 / {wait_timeout}秒"
                )

            time.sleep(check_interval)

    def force_terminate(self) -> int:
        """
        阶段 3: 强制终止

        强制关闭线程池并终止所有孤儿子进程。

        Returns:
            被丢弃的任务数量
        """
        logger.info("【阶段 3】强制终止 - 开始清理资源")

        # 获取待处理任务数
        discarded_count = self._get_pending_task_count()

        # 强制关闭线程池
        if hasattr(self.pool, 'shutdown'):
            try:
                # AdaptiveThreadPool 支持 cancel_futures 参数
                self.pool.shutdown(wait=False, cancel_futures=True)
                logger.info("线程池已强制关闭（取消未开始的任务）")
            except Exception as e:
                logger.error(f"关闭线程池时出错: {e}")

        # 调用 process_cleaner 清理挂起进程
        try:
            from master_orchestrator.process_cleaner import get_process_cleaner
            get_process_cleaner().cleanup_suspended_processes()
        except Exception:
            pass

        # 终止孤儿子进程
        orphan_count = self._kill_orphan_processes()

        logger.warning(
            f"【阶段 3】强制终止完成 - "
            f"丢弃任务数: {discarded_count}，终止孤儿进程数: {orphan_count}"
        )

        # 持久化被丢弃的任务状态（无论 discarded_count 是否 > 0，
        # 都需要检查 RUNNING 状态的任务——它们可能正在执行但未在 pending 队列中）
        if self._store and self._run_id:
            try:
                from .model import TaskStatus
                # 将所有 RUNNING 状态的任务重置为 PENDING，以便下次 resume 时重新执行
                reset_count = self._store.reset_running_tasks(self._run_id)
                if reset_count > 0:
                    logger.info("已将 %d 个运行中的任务重置为 PENDING（支持断点续传）", reset_count)
            except Exception as e:
                logger.error("持久化被丢弃任务状态失败: %s", e)

        return discarded_count

    def shutdown(self, timeout: Optional[int] = None) -> dict:
        """
        执行完整的三阶段关闭流程

        Args:
            timeout: 等待超时时间（秒），None 使用构造时的默认值

        Returns:
            关闭结果字典，包含：
            - success: 是否成功优雅关闭（所有任务完成）
            - discarded_tasks: 被丢弃的任务数
            - orphan_processes: 被终止的孤儿进程数
            - elapsed_time: 总耗时（秒）
        """
        logger.info("=" * 60)
        logger.info("开始优雅关闭流程")
        logger.info("=" * 60)

        start_time = time.time()

        # 阶段 1: 停止接受新任务
        self.stop_accepting()

        # Drain 屏障：等待 0.5 秒，确保正在执行的 get_ready_tasks() 完成
        time.sleep(0.5)

        # 阶段 2: 等待任务完成
        completed = self.await_completion(timeout)

        # 阶段 3: 强制终止（如果需要）
        discarded_tasks = 0
        if not completed:
            discarded_tasks = self.force_terminate()
        else:
            # 即使任务完成，也要清理可能的孤儿进程
            orphan_count = self._kill_orphan_processes()
            if orphan_count > 0:
                logger.warning(f"清理了 {orphan_count} 个孤儿进程")

        elapsed = time.time() - start_time

        result = {
            "success": completed,
            "discarded_tasks": discarded_tasks,
            "orphan_processes": len(self._active_pids),
            "elapsed_time": elapsed
        }

        logger.info("=" * 60)
        logger.info(f"优雅关闭流程完成 - 耗时: {elapsed:.2f}秒")
        logger.info(f"结果: {result}")
        logger.info("=" * 60)

        return result

    def _get_active_task_count(self) -> int:
        """
        获取活跃任务数量（活跃 + 待处理）

        Returns:
            活跃任务数
        """
        active = 0
        pending = 0

        if hasattr(self.pool, 'get_active_count'):
            active = self.pool.get_active_count()
        elif hasattr(self.pool, '_threads'):
            # ThreadPoolExecutor 兼容
            active = sum(1 for t in self.pool._threads if t.is_alive())

        if hasattr(self.pool, 'get_pending_count'):
            pending = self.pool.get_pending_count()
        elif hasattr(self.pool, '_work_queue'):
            pending = self.pool._work_queue.qsize()

        return active + pending

    def _get_pending_task_count(self) -> int:
        """
        获取待处理任务数量

        Returns:
            待处理任务数
        """
        if hasattr(self.pool, 'get_pending_count'):
            return self.pool.get_pending_count()
        elif hasattr(self.pool, '_work_queue'):
            return self.pool._work_queue.qsize()
        else:
            logger.warning("无法获取待处理任务数，假设为 0")
            return 0

    def _kill_orphan_processes(self) -> int:
        """
        终止所有孤儿子进程

        Returns:
            被终止的进程数量
        """
        with self._lock:
            orphan_pids = list(self._active_pids)

        if not orphan_pids:
            logger.debug("没有孤儿进程需要清理")
            return 0

        logger.warning(f"发现 {len(orphan_pids)} 个孤儿进程，开始终止: {orphan_pids}")

        killed_count = 0
        for pid in orphan_pids:
            try:
                if sys.platform == "win32":
                    # Windows: 用 taskkill 替代 os.kill（SIGTERM 在 Windows 上不可靠）
                    logger.info(f"Windows: 发送 taskkill 到进程 {pid}")
                    result = subprocess.run(["taskkill", "/PID", str(pid)], capture_output=True, timeout=10)
                    if result.returncode != 0:
                        # 优雅终止失败，尝试强制终止
                        result = subprocess.run(["taskkill", "/F", "/PID", str(pid)], capture_output=True, timeout=10)
                    if result.returncode == 0:
                        killed_count += 1
                        self.unregister_subprocess(pid)
                        logger.info(f"成功终止进程 {pid}")
                    else:
                        logger.warning(f"无法终止进程 {pid} (exit={result.returncode})")
                else:
                    # Unix: 检查进程是否存在
                    os.kill(pid, 0)

                    # 先尝试优雅终止 (SIGTERM)
                    logger.info(f"发送 SIGTERM 到进程 {pid}")
                    os.kill(pid, signal.SIGTERM)

                    # 等待 2 秒
                    time.sleep(2)

                    # 检查是否还存活
                    try:
                        os.kill(pid, 0)
                        # 仍然存活，强制终止 (SIGKILL)
                        logger.warning(f"进程 {pid} 未响应 SIGTERM，发送 SIGKILL")
                        os.kill(pid, signal.SIGKILL)
                    except OSError:
                        # 进程已终止
                        pass

                    killed_count += 1
                    self.unregister_subprocess(pid)
                    logger.info(f"成功终止进程 {pid}")

            except OSError as e:
                if e.errno == 3:  # No such process
                    logger.debug(f"进程 {pid} 已不存在")
                    self.unregister_subprocess(pid)
                elif e.errno == 1:  # Operation not permitted
                    logger.error(f"无权限终止进程 {pid}")
                else:
                    logger.error(f"终止进程 {pid} 时出错: {e}")
            except Exception as e:
                logger.error(f"处理进程 {pid} 时发生未知错误: {e}")

        return killed_count
