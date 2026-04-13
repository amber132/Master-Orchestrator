"""Windows Job Object 封装 — 编排器子进程编组与自动清理。

学习 Claude Code 的 AbortController 式生命周期管理：
- 所有 claude -p 子进程分配到同一个 Job Object
- 设置 JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE，编排器退出时 OS 自动终止所有关联进程
- 不依赖 taskkill /T（进程树方式不可靠），用 Job Object 作为最终安全网
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Optional

logger = logging.getLogger(__name__)

# Windows API 常量
_JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000
_JOB_OBJECT_BASIC_LIMIT_INFORMATION = 2
_PROCESS_TERMINATE = 0x0001

# ctypes 类型前向声明
if os.name == "nt":
    import ctypes
    import ctypes.wintypes

    class _IO_COUNTERS(ctypes.Structure):
        """Windows IO_COUNTERS 结构体。"""
        _fields_ = [
            ("ReadOperationCount", ctypes.c_uint64),
            ("WriteOperationCount", ctypes.c_uint64),
            ("OtherOperationCount", ctypes.c_uint64),
            ("ReadTransferCount", ctypes.c_uint64),
            ("WriteTransferCount", ctypes.c_uint64),
            ("OtherTransferCount", ctypes.c_uint64),
        ]

    class _JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
        """Windows JOBOBJECT_BASIC_LIMIT_INFORMATION 结构体（64-bit）。"""
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_int64),
            ("PerJobUserTimeLimit", ctypes.c_int64),
            ("LimitFlags", ctypes.c_uint32),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", ctypes.c_uint32),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", ctypes.c_uint32),
            ("SchedulingClass", ctypes.c_uint32),
        ]

    class _JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
        """Windows JOBOBJECT_EXTENDED_LIMIT_INFORMATION 结构体。

        设置 KILL_ON_JOB_CLOSE 必须使用 Extended 版本。
        """
        _fields_ = [
            ("BasicLimitInformation", _JOBOBJECT_BASIC_LIMIT_INFORMATION),
            ("IoInfo", _IO_COUNTERS),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]


class Win32JobObject:
    """Windows Job Object 封装，支持将子进程编组并设置自动清理。

    用法：
        job = Win32JobObject()
        job.create()  # 创建 Job 并设置 KILL_ON_JOB_CLOSE
        job.assign_process(proc_handle)  # 将子进程加入 Job
        # 编排器退出时 Job 句柄被 GC，OS 自动终止所有子进程
    """

    def __init__(self) -> None:
        self._handle: Optional[int] = None
        self._lock = threading.Lock()
        self._assigned_pids: set[int] = set()

    @property
    def handle(self) -> Optional[int]:
        return self._handle

    @property
    def assigned_pids(self) -> set[int]:
        with self._lock:
            return set(self._assigned_pids)

    def create(self) -> bool:
        """创建 Job Object 并设置 KILL_ON_JOB_CLOSE。"""
        if os.name != "nt":
            logger.debug("非 Windows 平台，跳过 Job Object 创建")
            return False

        try:
            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]

            # 必须设置正确的 restype/argtypes，否则 64-bit 下句柄会被截断为 32-bit int
            kernel32.CreateJobObjectW.restype = ctypes.c_void_p
            kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, ctypes.c_wchar_p]
            kernel32.SetInformationJobObject.restype = ctypes.c_int
            kernel32.SetInformationJobObject.argtypes = [ctypes.c_void_p, ctypes.c_uint, ctypes.c_void_p, ctypes.c_uint]
            kernel32.AssignProcessToJobObject.restype = ctypes.c_int
            kernel32.AssignProcessToJobObject.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
            kernel32.OpenProcess.restype = ctypes.c_void_p
            kernel32.OpenProcess.argtypes = [ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32]
            kernel32.CloseHandle.restype = ctypes.c_int
            kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
            kernel32.TerminateProcess.restype = ctypes.c_int
            kernel32.TerminateProcess.argtypes = [ctypes.c_void_p, ctypes.c_uint32]

            # 创建匿名 Job Object
            job_handle = kernel32.CreateJobObjectW(None, None)
            if not job_handle:
                logger.warning("CreateJobObjectW 失败: %d", ctypes.get_last_error())
                return False

            # 设置 KILL_ON_JOB_CLOSE：Job 句柄关闭时自动终止所有子进程
            info = _JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
            info.BasicLimitInformation.LimitFlags = _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE

            # JobObjectExtendedLimitInformation = 9
            result = kernel32.SetInformationJobObject(
                job_handle,
                9,  # JobObjectExtendedLimitInformation
                ctypes.byref(info),
                ctypes.sizeof(info),
            )
            if not result:
                logger.warning("SetInformationJobObject 失败: %d", ctypes.get_last_error())
                kernel32.CloseHandle(job_handle)
                return False

            self._handle = job_handle
            logger.info("Job Object 创建成功 (handle=%d)，KILL_ON_JOB_CLOSE 已启用", job_handle)
            return True

        except Exception as e:
            logger.warning("Job Object 创建异常: %s", e)
            return False

    def assign_process(self, proc) -> bool:
        """将子进程分配到 Job Object。

        Args:
            proc: subprocess.Popen 实例

        Returns:
            是否成功分配
        """
        if os.name != "nt" or not self._handle:
            return False

        pid = proc.pid
        if pid is None:
            return False

        try:
            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]

            # 通过 PID 打开进程句柄
            proc_handle = kernel32.OpenProcess(_PROCESS_TERMINATE, False, pid)
            if not proc_handle:
                logger.warning("OpenProcess(%d) 失败: %d", pid, ctypes.get_last_error())
                return False

            # 分配到 Job
            result = kernel32.AssignProcessToJobObject(self._handle, proc_handle)
            kernel32.CloseHandle(proc_handle)

            if result:
                with self._lock:
                    self._assigned_pids.add(pid)
                logger.debug("进程 PID=%d 已分配到 Job Object", pid)
            else:
                # Windows 8 之前：进程已在其他 Job 中会失败
                logger.warning("AssignProcessToJobObject(PID=%d) 失败: %d", pid, ctypes.get_last_error())

            return bool(result)

        except Exception as e:
            logger.warning("分配进程到 Job 异常 (PID=%d): %s", pid, e)
            return False

    def terminate_all(self) -> int:
        """主动终止 Job 中所有进程（不等待 GC）。

        Returns:
            成功终止的进程数
        """
        if os.name != "nt" or not self._handle:
            return 0

        with self._lock:
            pids = list(self._assigned_pids)
            self._assigned_pids.clear()

        count = 0
        for pid in pids:
            try:
                kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
                proc_handle = kernel32.OpenProcess(_PROCESS_TERMINATE, False, pid)
                if proc_handle:
                    kernel32.TerminateProcess(proc_handle, 1)
                    kernel32.CloseHandle(proc_handle)
                    count += 1
            except Exception:
                pass

        logger.info("Job Object 主动终止 %d/%d 个进程", count, len(pids))
        return count

    def close(self) -> None:
        """关闭 Job Object 句柄（OS 会自动终止所有关联进程）。"""
        if self._handle:
            try:
                ctypes.windll.kernel32.CloseHandle(self._handle)  # type: ignore[attr-defined]
                logger.info("Job Object 句柄已关闭，关联进程将被 OS 终止")
            except Exception as e:
                logger.warning("关闭 Job Object 句柄异常: %s", e)
            finally:
                self._handle = None
                with self._lock:
                    self._assigned_pids.clear()

    def __del__(self) -> None:
        if self._handle:
            self.close()


# ── 全局单例 ──

_global_job: Optional[Win32JobObject] = None
_job_lock = threading.Lock()


def get_global_job() -> Win32JobObject:
    """获取全局 Job Object 单例（线程安全）。

    首次调用时自动创建并设置 KILL_ON_JOB_CLOSE。
    非平台（非 Windows）返回空壳实例（assign_process 为 no-op）。
    """
    global _global_job
    with _job_lock:
        if _global_job is None:
            _global_job = Win32JobObject()
            _global_job.create()
        return _global_job
