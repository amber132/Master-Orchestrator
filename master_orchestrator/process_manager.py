"""子进程生命周期管理。

全局进程注册表、环境变量隔离、进程终止。从 claude_cli.py 提取。
"""
from __future__ import annotations

import atexit
import logging
import os
import signal
import subprocess
import threading
import uuid

logger = logging.getLogger(__name__)


# ── 全局子进程注册表 & 僵尸进程清理 ──

_active_procs: dict[int, subprocess.Popen] = {}  # pid -> Popen
_active_procs_lock = threading.Lock()

# 环境变量隔离：这些键来自桌面版 Claude，会导致嵌套会话冲突
_STRIP_DESKTOP_CLAUDE_ENV_KEYS = frozenset({
    "CLAUDECODE",
    "CLAUDE_THREAD_ID",
    "CLAUDE_INTERNAL_ORIGINATOR_OVERRIDE",
    "CLAUDE_SHELL",
})

# 编排器会话标识：注入到所有子进程环境变量中，用于区分编排器启动的和用户手动启动的 claude 进程
_ORCHESTRATOR_SESSION_ID = f"orch-{uuid.uuid4().hex[:12]}"


def get_session_id() -> str:
    """获取当前编排器会话 ID。"""
    return _ORCHESTRATOR_SESSION_ID


def _register_proc(proc: subprocess.Popen) -> bool:
    """注册子进程到全局跟踪表。返回 True 表示成功，False 表示失败。"""
    if proc.pid is None:
        return False
    with _active_procs_lock:
        _active_procs[proc.pid] = proc
    return True


def _unregister_proc(proc: subprocess.Popen) -> None:
    """从全局跟踪表移除子进程。"""
    with _active_procs_lock:
        _active_procs.pop(proc.pid, None)
    # 同步从 ProcessCleaner 注销
    try:
        from .process_cleaner import get_process_cleaner
        get_process_cleaner().unregister_pid(proc.pid)
    except Exception:
        pass
    # 顺便清理其他已完成的进程引用
    _gc_finished_procs()


def _cleanup_all_procs() -> None:
    """atexit 钩子：强杀所有残留子进程，防止僵尸进程。

    学习 Claude Code 的双层清理模式：
    1. 先尝试逐个精确终止已知 PID
    2. 最终由 Job Object 兜底（OS 在句柄关闭时自动清理）
    """
    with _active_procs_lock:
        remaining = list(_active_procs.values())
    for proc in remaining:
        if proc.poll() is None:
            logger.warning("atexit: 清理残留子进程 PID=%d", proc.pid)
            _kill_process_tree(proc, task_id="atexit-cleanup")
    # 从 ProcessCleaner 注销所有残留进程
    try:
        from .process_cleaner import get_process_cleaner
        cleaner = get_process_cleaner()
        for proc in remaining:
            cleaner.unregister_pid(proc.pid)
    except Exception:
        pass
    # 最终：关闭 Job Object 句柄，OS 自动终止所有未清理的子进程
    try:
        from .win32_job import get_global_job
        job = get_global_job()
        job.close()
    except Exception:
        pass


def _gc_finished_procs() -> None:
    """清理已完成但未注销的进程引用，防止内存泄漏。"""
    with _active_procs_lock:
        finished = [pid for pid, proc in _active_procs.items() if proc.poll() is not None]
        for pid in finished:
            del _active_procs[pid]
        if finished:
            logger.debug("GC 清理了 %d 个已完成的进程引用", len(finished))


atexit.register(_cleanup_all_procs)


def _build_subprocess_env(overrides: dict[str, str] | None = None) -> dict[str, str]:
    """构建子进程环境变量，隔离桌面版 Claude 环境变量防止冲突。

    Args:
        overrides: 额外的环境变量覆盖。

    Returns:
        清理后的环境变量字典。
    """
    env = {k: v for k, v in os.environ.items()}
    isolate_claude_home = bool(overrides and overrides.get("CLAUDE_CONFIG_DIR"))
    override_keys = set(overrides or {})
    for key in list(env):
        if key in _STRIP_DESKTOP_CLAUDE_ENV_KEYS:
            env.pop(key, None)
            continue
        if isolate_claude_home and key.startswith("CLAUDE_") and key not in override_keys:
            env.pop(key, None)
    if overrides:
        env.update(overrides)
    # 注入编排器会话标识，用于区分编排器子进程和用户手动启动的 claude 进程
    env["CLAUDE_ORCHESTRATOR_SESSION_ID"] = _ORCHESTRATOR_SESSION_ID
    return env


def _kill_process_tree(proc: subprocess.Popen, task_id: str = "") -> None:
    """安全终止子进程，学习 Claude Code 的精确 PID 模式。

    优先精确杀单个 PID（不杀进程树），避免误杀其他 claude 进程。
    只在精确杀失败时才回退到进程树杀。
    Windows Job Object 作为最终安全网（编排器崩溃也能自动清理）。
    """
    pid = proc.pid
    try:
        if os.name == "nt":
            # 第一优先：精确杀单个 PID（不杀进程树）
            result = subprocess.run(
                ["taskkill", "/F", "/PID", str(pid)],
                capture_output=True, timeout=10,
            )
            if result.returncode == 0:
                logger.info("[%s] 已终止进程 (taskkill /F /PID %d)", task_id, pid)
            else:
                # 精确杀失败，回退到进程树杀
                subprocess.run(
                    ["taskkill", "/F", "/T", "/PID", str(pid)],
                    capture_output=True, timeout=10,
                )
                logger.info("[%s] 已终止进程树 (taskkill /T /PID %d)", task_id, pid)
        else:
            # Unix: 先 SIGTERM，再 SIGKILL，最后进程组
            try:
                os.kill(pid, signal.SIGTERM)
                import time
                time.sleep(0.5)
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                pgid = os.getpgid(pid)
                os.killpg(pgid, signal.SIGKILL)
                logger.info("[%s] 已杀死进程组 (PGID=%d)", task_id, pgid)
            except ProcessLookupError:
                pass
    except Exception as e:
        logger.warning("[%s] 进程终止失败，回退到 proc.kill(): %s", task_id, e)
        try:
            proc.kill()
        except OSError:
            pass
