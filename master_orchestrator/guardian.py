#!/usr/bin/env python3
"""Process guardian wrapper for unattended execution.

Features:
- Crash auto-restart with exponential backoff
- Health check via heartbeat file
- Configurable max restart attempts
- Clean shutdown on SIGTERM/SIGINT
- PID file management for external monitoring

Usage:
    # Direct execution
    python -m claude_orchestrator.guardian auto --goal "..." --dir ./project

    # As Windows service via nssm
    nssm install ClaudeOrchestrator python -m claude_orchestrator.guardian auto --goal "..."
"""

from __future__ import annotations

import atexit
import logging
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# ── 默认配置 ──
MAX_RESTARTS = 10          # 最大连续重启次数
BACKOFF_BASE = 2           # 退避基数（秒）
BACKOFF_MAX = 300          # 最大退避时间（秒）
BACKOFF_RESET_AFTER = 600  # 成功运行超过此秒数后重置退避计数
HEARTBEAT_INTERVAL = 30    # 心跳文件更新间隔（秒）
PID_DIR = Path.home() / ".claude" / "guardian"


class Guardian:
    """进程守护器：监控子进程，崩溃时自动重启。"""

    def __init__(
        self,
        args: list[str],
        max_restarts: int = MAX_RESTARTS,
        pid_dir: Path = PID_DIR,
    ):
        self._args = args
        self._max_restarts = max_restarts
        self._pid_dir = pid_dir
        self._proc: subprocess.Popen | None = None
        self._shutdown = False
        self._restart_count = 0
        self._lock_fd = None

        # 子进程资源级监控配置
        self._mem_limit_mb = 4096  # 子进程内存上限 4GB
        self._cpu_check_interval = 60  # 资源检查间隔（秒）
        self._resource_kill = False  # 标记本次退出是否因资源超限主动终止

        self._pid_dir.mkdir(parents=True, exist_ok=True)
        self._pid_file = self._pid_dir / "guardian.pid"
        self._heartbeat_file = self._pid_dir / "heartbeat"

    def run(self) -> int:
        """主循环：启动子进程，监控，崩溃时重启。"""
        self._write_pid()
        atexit.register(self._cleanup_pid)
        self._register_signals()

        logger.info("Guardian 启动，监控命令: %s", " ".join(self._args))

        while not self._shutdown and self._restart_count <= self._max_restarts:
            # Guardian 主循环心跳：即使子进程未运行，也定期更新心跳文件
            self._update_heartbeat("supervising", restart_count=self._restart_count)

            start_time = time.monotonic()
            exit_code = self._run_once()
            elapsed = time.monotonic() - start_time

            if self._shutdown:
                logger.info("Guardian 收到关闭信号，退出")
                return exit_code

            if exit_code == 0:
                logger.info("子进程正常退出 (code=0)，Guardian 结束")
                return 0

            # 资源超限主动终止：不计入崩溃重启次数，直接重启
            if self._resource_kill:
                logger.info(
                    "子进程因资源超限被主动终止 (code=%d)，立即重启（不计入崩溃计数）",
                    exit_code,
                )
                self._resource_kill = False
                # 短暂等待让系统回收资源
                self._sleep_interruptible(5)
                continue

            self._restart_count += 1

            # 如果运行时间足够长，重置退避计数
            if elapsed > BACKOFF_RESET_AFTER:
                logger.info("子进程运行了 %.0f 秒后崩溃，重置退避计数", elapsed)
                self._restart_count = 1

            if self._restart_count > self._max_restarts:
                logger.error(
                    "连续重启次数超过上限 (%d)，Guardian 放弃",
                    self._max_restarts,
                )
                # 发送告警
                try:
                    from .notification import get_notifier
                    get_notifier().critical(
                        "Guardian 放弃重启",
                        restarts=self._restart_count,
                        max_restarts=self._max_restarts,
                        last_exit_code=exit_code,
                    )
                except Exception:
                    pass
                return exit_code

            backoff = min(BACKOFF_BASE ** self._restart_count, BACKOFF_MAX)
            logger.warning(
                "子进程异常退出 (code=%d)，第 %d/%d 次重启，等待 %ds...",
                exit_code, self._restart_count, self._max_restarts, backoff,
            )
            self._sleep_interruptible(backoff)

            # Guardian 自身内存检查
            try:
                import psutil
                rss_mb = psutil.Process().memory_info().rss / (1024 * 1024)
                if rss_mb > 500:
                    logger.warning("Guardian 自身内存过高: %.0fMB", rss_mb)
            except Exception:
                pass

        return 1

    def _monitor_resources(self) -> tuple[float, float] | None:
        """监控子进程的 RSS 内存和 CPU 使用率。

        使用 psutil 获取子进程资源信息。psutil 是可选依赖，
        import 失败时直接返回 None，降级为只监控退出码。

        注意：cpu_percent(interval=None) 首次调用总是返回 0.0（psutil 限制），
        这是正常行为，后续调用会返回自上次调用以来的 CPU 百分比。

        Returns:
            (rss_mb, cpu_percent) 或 None（psutil 不可用/进程已退出）
        """
        try:
            import psutil
        except ImportError:
            return None

        if self._proc is None or self._proc.poll() is not None:
            return None

        try:
            p = psutil.Process(self._proc.pid)
            rss_mb = p.memory_info().rss / (1024 * 1024)
            # interval=None 返回自上次调用以来的 CPU 百分比（非阻塞）
            cpu_percent = p.cpu_percent(interval=None)
            return (rss_mb, cpu_percent)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            # 进程已退出或无权限访问，返回 None
            return None
        except Exception as e:
            logger.debug("监控子进程资源时异常: %s", e)
            return None

    def _run_once(self) -> int:
        """启动并等待子进程完成，返回退出码。"""
        # 认证预检：确保 Claude CLI 可用
        try:
            clean_env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}
            precheck = subprocess.run(
                ["claude", "--version"],
                capture_output=True,
                text=True,
                timeout=15,
                shell=(os.name == "nt"),
                env=clean_env,
            )
            if precheck.returncode != 0:
                logger.warning("认证预检失败 (exit=%d)，等待 30s 后重试...", precheck.returncode)
                self._sleep_interruptible(30)
                if self._shutdown:
                    return -1
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as e:
            logger.warning("认证预检异常: %s，等待 30s 后重试...", e)
            self._sleep_interruptible(30)
            if self._shutdown:
                return -1

        cmd = [sys.executable, "-m", "claude_orchestrator.cli"] + self._args
        logger.info("启动子进程: %s", " ".join(cmd))

        # 修复问题2: 捕获子进程启动异常
        try:
            self._proc = subprocess.Popen(cmd)
        except OSError as e:
            logger.error("启动子进程失败: %s", e)
            return -1
        self._update_heartbeat("running", pid=self._proc.pid)

        # 重置资源超限标志
        self._resource_kill = False

        try:
            last_resource_check = time.monotonic()

            while self._proc.poll() is None:
                self._update_heartbeat("running", pid=self._proc.pid)

                # 检测子进程心跳
                child_heartbeat_file = Path.home() / ".claude" / "guardian" / "child_heartbeat"
                # 修复问题4: TOCTOU漏洞 - 直接try-except stat()
                try:
                    last_beat = child_heartbeat_file.stat().st_mtime
                    if time.time() - last_beat > 300:  # heartbeat_timeout_seconds
                        logger.critical("子进程心跳超时 (%.0fs)，强制终止", time.time() - last_beat)
                        try:
                            from master_orchestrator.notification import get_notifier
                            get_notifier().critical("子进程心跳超时", f"最后心跳 {time.time() - last_beat:.0f}s 前")
                        except Exception:
                            pass
                        if self._proc and self._proc.poll() is None:
                            # 修复问题1: 竞态条件 - 用try-except包裹kill操作
                            try:
                                self._proc.kill()
                            except OSError as e:
                                logger.warning("终止进程失败: %s", e)
                        return -2  # 特殊退出码触发重启
                except FileNotFoundError:
                    # 心跳文件不存在，跳过检查
                    pass
                except OSError as e:
                    logger.warning("读取子进程心跳文件失败: %s", e)

                # 子进程资源级监控：每 _cpu_check_interval 秒检查一次
                now = time.monotonic()
                if now - last_resource_check >= self._cpu_check_interval:
                    last_resource_check = now
                    metrics = self._monitor_resources()
                    if metrics is not None:
                        rss_mb, cpu_pct = metrics
                        logger.debug(
                            "子进程资源: RSS=%.0fMB, CPU=%.1f%%",
                            rss_mb, cpu_pct,
                        )
                        if rss_mb > self._mem_limit_mb:
                            logger.critical(
                                "子进程内存超限: RSS=%.0fMB > 上限 %dMB，主动终止",
                                rss_mb, self._mem_limit_mb,
                            )
                            # 发送资源超限通知
                            try:
                                from master_orchestrator.notification import get_notifier
                                get_notifier().critical(
                                    "子进程内存超限",
                                    rss_mb=f"{rss_mb:.0f}",
                                    limit_mb=self._mem_limit_mb,
                                    pid=self._proc.pid,
                                )
                            except Exception:
                                pass
                            # 先 terminate 给子进程优雅退出机会
                            self._resource_kill = True
                            if self._proc and self._proc.poll() is None:
                                try:
                                    self._proc.terminate()
                                    try:
                                        self._proc.wait(timeout=10)
                                    except subprocess.TimeoutExpired:
                                        logger.warning("资源超限 terminate 后 10s 未退出，强制 kill")
                                        try:
                                            self._proc.kill()
                                            self._proc.wait(timeout=5)
                                        except OSError as e:
                                            logger.warning("强制终止进程失败: %s", e)
                                except OSError as e:
                                    logger.warning("终止进程失败: %s", e)
                            exit_code = self._proc.returncode if self._proc.returncode is not None else -3
                            self._update_heartbeat("resource_killed", exit_code=exit_code, rss_mb=f"{rss_mb:.0f}")
                            return exit_code

                # 每隔 HEARTBEAT_INTERVAL 秒检查一次
                for _ in range(HEARTBEAT_INTERVAL):
                    if self._proc.poll() is not None or self._shutdown:
                        break
                    time.sleep(1)

            # 确保子进程已完全退出并获取 returncode
            if self._proc.poll() is None:
                # shutdown 信号已发 terminate，等待子进程退出
                try:
                    self._proc.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    logger.warning("子进程 terminate 后 30s 未退出，强制 kill")

                    # 修复问题1: 竞态条件 - 用try-except包裹kill操作
                    try:
                        self._proc.kill()
                        try:
                            self._proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            logger.error("子进程 kill 后 5s 仍未退出，可能为僵尸进程")
                    except OSError as e:
                        logger.warning("强制终止进程失败: %s", e)

            exit_code = self._proc.returncode
            self._update_heartbeat("exited", exit_code=exit_code)
            return exit_code

        except Exception as e:
            logger.error("监控子进程时异常: %s", e)
            # 修复问题1: 竞态条件 - 用try-except包裹terminate/kill操作
            if self._proc and self._proc.poll() is None:
                try:
                    self._proc.terminate()
                    try:
                        self._proc.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        try:
                            self._proc.kill()
                        except OSError as kill_err:
                            logger.warning("强制终止进程失败: %s", kill_err)
                except OSError as term_err:
                    logger.warning("终止进程失败: %s", term_err)
            return -1
        finally:
            self._proc = None

    def _register_signals(self) -> None:
        """注册信号处理器。"""
        def _handler(signum: int, frame: object) -> None:
            logger.info("Guardian 收到信号 %d，开始优雅关闭...", signum)
            self._shutdown = True

            # 修复问题1: 竞态条件 - 用try-except包裹terminate操作
            if self._proc and self._proc.poll() is None:
                try:
                    self._proc.terminate()
                except OSError as e:
                    logger.warning("信号处理器中终止进程失败: %s", e)

        signal.signal(signal.SIGINT, _handler)
        if os.name != "nt":
            signal.signal(signal.SIGTERM, _handler)
        else:
            try:
                signal.signal(signal.SIGBREAK, _handler)  # type: ignore[attr-defined]
            except (AttributeError, OSError):
                pass

    def _sleep_interruptible(self, seconds: float) -> None:
        """可中断的 sleep。"""
        end = time.monotonic() + seconds
        while not self._shutdown:
            remaining = end - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(1.0, remaining))

    def _write_pid(self) -> None:
        """写入 PID 文件并加文件锁，防止多实例运行。"""
        try:
            self._lock_fd = open(self._pid_file, 'w')
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(self._lock_fd.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._lock_fd.write(str(os.getpid()))
            self._lock_fd.flush()
        except (OSError, IOError) as e:
            logger.error("另一个 Guardian 实例已在运行: %s", e)
            sys.exit(1)

    def _cleanup_pid(self) -> None:
        """清理 PID 文件并释放文件锁。"""
        try:
            if self._lock_fd:
                self._lock_fd.close()
                self._lock_fd = None
            self._pid_file.unlink(missing_ok=True)
        except OSError:
            pass

    def _update_heartbeat(self, status: str, **extra: object) -> None:
        """更新心跳文件，供外部监控读取。"""
        try:
            data = {
                "guardian_pid": os.getpid(),
                "status": status,
                "timestamp": datetime.now().isoformat(),
                **extra,
            }
            self._heartbeat_file.write_text(
                "\n".join(f"{k}={v}" for k, v in data.items())
            )
        except OSError:
            pass


def main() -> int:
    """Guardian CLI 入口。"""
    import argparse

    parser = argparse.ArgumentParser(description="Guardian 进程守护器", add_help=False)
    parser.add_argument("--log-file", default=None, help="日志文件路径（启用轮转）")
    known, remaining = parser.parse_known_args()

    from master_orchestrator.monitor import setup_logging
    setup_logging(log_file=known.log_file)

    if not remaining:
        print("Usage: python -m claude_orchestrator.guardian <orchestrator args...>", file=sys.stderr)
        print("Example: python -m claude_orchestrator.guardian auto --goal 'my goal' --dir ./project", file=sys.stderr)
        return 1

    guardian = Guardian(args=remaining)
    return guardian.run()


if __name__ == "__main__":
    sys.exit(main())
