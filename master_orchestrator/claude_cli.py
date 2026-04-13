"""Claude Code CLI subprocess wrapper with streaming progress."""

from __future__ import annotations

import atexit
import json
import logging
import os
import shutil
import signal
import subprocess
import tempfile
import threading
import time
import uuid
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from .config import ClaudeConfig, LimitsConfig
from .audit_log import AuditLogger
from .error_classifier import looks_like_auth_error, looks_like_network_error, looks_like_rate_limit_error
from .execution_lease import ExecutionLease, ExecutionLeaseManager
from .exceptions import BudgetExhaustedError
from .log_context import set_task_id
from .metrics import MetricsCollector, TaskMetrics
from .model import TaskNode, TaskResult, TaskStatus
from .win32_job import get_global_job

logger = logging.getLogger(__name__)


class CostAssertionError(RuntimeError):
    """成本断言失败：存在 token 使用但成本为零。"""
    pass


# ── 基础 system prompt：注入到所有子 agent，防止文件写入死循环 ──

_BASE_SYSTEM_PROMPT = (
    "# 文件写入规范（最高优先级，违反会导致任务无限循环）\n"
    "Bash 工具在传递命令时会处理反斜杠转义（\\\\ → \\），即使单引号 heredoc 也无法避免。\n"
    "写文件方法按可靠性排序：\n"
    "1. Write/Edit 工具（100% 可靠）— 永远首选，无转义问题\n"
    "2. base64 编解码（100% 可靠）— 仅当 Write 工具不可用时使用：echo '<base64>' | base64 -d > file\n"
    "3. Heredoc/printf/python -c/node -e — 仅限不含 \\\\、${}、\\n\\t 字面量的简单内容，否则必定出错\n"
    "4. 绝对禁止：通过 Bash 调用 pwsh -Command 写文件（双重转义 100% 失败）\n"
    "规则：优先用 Write/Edit，遇到转义困难立即切换 base64，禁止反复尝试不同 shell 转义方案。\n"
    "大文件拆分：超过 200 行的文件，先 Write 前 200 行，再用 Edit 分批追加（每次不超过 200 行）。\n"
)

# ── 全局子进程注册表 & 僵尸进程清理 ──

_active_procs: dict[int, subprocess.Popen] = {}  # pid -> Popen
_active_procs_lock = threading.Lock()


@dataclass
class _ExecutionLeaseBinding:
    manager: ExecutionLeaseManager
    run_id: str
    wait_poll_interval_seconds: float = 0.2
    log_interval_seconds: float = 5.0

_execution_lease_stack: list[_ExecutionLeaseBinding] = []
_execution_lease_stack_lock = threading.Lock()

# 环境变量隔离：这些键来自桌面版 Claude，会导致嵌套会话冲突
_STRIP_DESKTOP_CLAUDE_ENV_KEYS = frozenset({
    "CLAUDECODE",
    "CLAUDE_THREAD_ID",
    "CLAUDE_INTERNAL_ORIGINATOR_OVERRIDE",
    "CLAUDE_SHELL",
})

# 编排器会话标识：注入到所有子进程环境变量中，用于区分编排器启动的和用户手动启动的 claude 进程
_ORCHESTRATOR_SESSION_ID = f"orch-{uuid.uuid4().hex[:12]}"

# 导出会话 ID 供外部模块（如 process_cleaner）使用
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


@contextmanager
def bind_execution_lease_scope(
    manager: ExecutionLeaseManager,
    run_id: str,
    *,
    wait_poll_interval_seconds: float = 0.2,
    log_interval_seconds: float = 5.0,
):
    """绑定执行租约管理器到当前线程的上下文栈。

    用于在任务执行期间通过栈式绑定获取租约，避免将 manager 传递到每个函数。
    """
    binding = _ExecutionLeaseBinding(
        manager=manager,
        run_id=run_id,
        wait_poll_interval_seconds=wait_poll_interval_seconds,
        log_interval_seconds=log_interval_seconds,
    )
    with _execution_lease_stack_lock:
        _execution_lease_stack.append(binding)
    try:
        yield binding
    finally:
        with _execution_lease_stack_lock:
            # 确保只弹出自己（防止并发栈操作错位）
            if _execution_lease_stack and _execution_lease_stack[-1] is binding:
                _execution_lease_stack.pop()
            else:
                try:
                    _execution_lease_stack.remove(binding)
                except ValueError:
                    pass


def _current_execution_lease_binding() -> _ExecutionLeaseBinding | None:
    """获取当前线程栈顶的执行租约绑定。"""
    with _execution_lease_stack_lock:
        if not _execution_lease_stack:
            return None
        return _execution_lease_stack[-1]


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


def verify_cli_available(
    *,
    max_retries: int = 3,
    timeout_schedule: tuple[int, ...] = (10, 15, 20),
    retry_delay_base: float = 2.0,
) -> str:
    """验证 Claude CLI 是否可用（`claude --version`）。

    通过 shutil.which 定位可执行文件，再运行 ``claude --version`` 确认
    CLI 能正常响应。失败时抛出 :class:`ClaudeCLIError`，包含可操作的错误信息。

    Args:
        max_retries: 最大重试次数（FileNotFoundError 除外，直接抛出）。
        timeout_schedule: 每次重试的超时秒数，长度应 >= max_retries。
        retry_delay_base: 重试间隔基数，实际等待 = retry_delay_base * (attempt+1)。

    Returns:
        CLI 版本字符串（stdout 第一行，已 strip）。

    Raises:
        ClaudeCLIError: CLI 不可用、认证失败或调用超时。
    """
    from .exceptions import ClaudeCLIError

    # 1. 检查 claude 是否在 PATH 中
    claude_path = shutil.which("claude")
    if not claude_path:
        raise ClaudeCLIError(
            "Claude CLI 未找到。请确保 'claude' 命令在 PATH 中。"
            " 安装方式: npm install -g @anthropic-ai/claude-code"
        )

    # 2. 运行 claude --version（含重试）
    env = _build_subprocess_env()
    for attempt in range(max_retries):
        timeout = timeout_schedule[min(attempt, len(timeout_schedule) - 1)]
        try:
            result = subprocess.run(
                [claude_path, "--version"],
                capture_output=True,
                text=True,
                timeout=timeout,
                shell=(os.name == "nt"),
                env=env,
            )
        except subprocess.TimeoutExpired:
            if attempt < max_retries - 1:
                logger.warning(
                    "Claude CLI 验证超时 (%ds)，第 %d/%d 次重试...",
                    timeout, attempt + 1, max_retries,
                )
                time.sleep(retry_delay_base * (attempt + 1))
                continue
            raise ClaudeCLIError(
                f"Claude CLI 验证超时 ({timeout}s，已重试 {max_retries} 次)。"
                " 可能是 CLI 卡住或网络问题，请手动运行 'claude --version' 确认"
            )
        except FileNotFoundError:
            # shutil.which 找到但实际执行失败（极罕见，如符号链接断裂）
            raise ClaudeCLIError(
                f"Claude CLI 路径无效: {claude_path}。"
                " 请确认符号链接或安装路径正确"
            )
        except Exception as exc:
            if attempt < max_retries - 1:
                logger.warning(
                    "Claude CLI 验证异常: %s，第 %d/%d 次重试...",
                    exc, attempt + 1, max_retries,
                )
                time.sleep(retry_delay_base * (attempt + 1))
                continue
            raise ClaudeCLIError(
                f"Claude CLI 验证异常: {exc}。"
                " 请确认 CLI 安装正确并已认证"
            )

        # 非零退出码 → 认证失败或 CLI 损坏
        if result.returncode != 0:
            if attempt < max_retries - 1:
                logger.warning(
                    "Claude CLI 退出码 %d (stderr=%s)，第 %d/%d 次重试...",
                    result.returncode, result.stderr.strip()[:120],
                    attempt + 1, max_retries,
                )
                time.sleep(retry_delay_base * (attempt + 1))
                continue
            raise ClaudeCLIError(
                f"Claude CLI 不可用 (exit_code={result.returncode})。"
                " 请运行 'claude login' 完成认证，或检查 CLI 安装是否完整"
            )

        # 成功
        version = result.stdout.strip().split("\n")[0]
        logger.info("[OK] Claude CLI 校验通过: %s (path=%s)", version, claude_path)
        return version

    # 理论上不可达（循环内总会 return 或 raise）
    raise ClaudeCLIError("Claude CLI 验证失败（已耗尽重试次数）")


@contextmanager
def _execution_lease_session(task: TaskNode):
    """为单个任务获取和释放执行租约的上下文管理器。

    如果栈中没有绑定管理器，直接透传（无租约模式）。
    """
    binding = _current_execution_lease_binding()
    if binding is None or binding.manager is None:
        yield
        return

    manager = binding.manager
    run_id = binding.run_id
    lease = None
    try:
        # 等待获取租约（长轮询）
        wait_start = time.monotonic()
        last_log = wait_start
        while lease is None:
            lease = manager.acquire(run_id, task.id, timeout_seconds=0.0)
            if lease is not None:
                break
            now = time.monotonic()
            if now - last_log >= binding.log_interval_seconds:
                logger.info(
                    "[%s] 等待执行租约 (run=%s, 活跃=%d, 已等 %ds)...",
                    task.id, run_id, manager.active_count(), int(now - wait_start),
                )
                last_log = now
            time.sleep(binding.wait_poll_interval_seconds)
        logger.debug("[%s] 获取执行租约成功: %s", task.id, lease.lease_id[:8] if lease else "N/A")
        yield
    finally:
        if lease is not None:
            manager.release(lease)
            logger.debug("[%s] 释放执行租约: %s", task.id, lease.lease_id[:8])

# Heartbeat: warn if no output for this many seconds
_HEARTBEAT_INTERVAL = 120

# 按模型设置合理的 max_turns 默认值，防止过度探索
# opus 更聪明，允许更多轮次；sonnet 容易陷入探索循环，需要限制
_DEFAULT_MAX_TURNS = {
    "opus": 200,
    "sonnet": 200,
    "haiku": 80,
}

# 停滞检测：连续只读调用阈值（Write/Edit 以外的工具调用视为只读）
_STAGNATION_WARNING_THRESHOLD = 20   # 超过 20 次连续只读发出警告
_STAGNATION_CRITICAL_THRESHOLD = 40  # 超过 40 次连续只读强制终止（减少剩余轮次）

# 网络健康探针配置
_NETWORK_PROBE_TIMEOUT = 5  # 单次探测超时秒数
_NETWORK_PROBE_MAX_WAIT = 60  # 最长等待网络恢复秒数（1 分钟，避免长时间阻塞重试）
_NETWORK_PROBE_INTERVAL = 30  # 探测间隔秒数（默认值，实际由分级策略覆盖）
_NETWORK_PROBE_ENDPOINTS = [
    "https://api.anthropic.com",
    "https://1.1.1.1",
]

# 分级等待策略阈值（秒）
_PROBE_TIER1_LIMIT = 300   # 前 5 分钟：每 10 秒探测
_PROBE_TIER1_INTERVAL = 10
_PROBE_TIER2_LIMIT = 1800  # 5-30 分钟：每 30 秒探测
_PROBE_TIER2_INTERVAL = 30
_PROBE_TIER3_INTERVAL = 60  # 30-60 分钟：每 60 秒探测

# 通知触发阈值（秒）
_PROBE_NOTIFY_WARNING = 120   # 等待超过 2 分钟发送 WARNING
_PROBE_NOTIFY_CRITICAL = 600  # 等待超过 10 分钟发送 CRITICAL


def _probe_single_endpoint(endpoint: str) -> tuple[bool, float]:
    """探测单个端点的可达性和延迟。

    自动检测本地代理（环境变量 > 常见本地代理端口）。

    Args:
        endpoint: 要探测的 URL。

    Returns:
        (reachable, latency_ms) — reachable 为 True 表示可达，
        latency_ms 为往返延迟毫秒数（不可达时为 -1）。
    """
    import urllib.request
    import urllib.error

    # 构建代理配置：优先环境变量，其次尝试常见本地代理端口
    proxy_url = (
        os.environ.get("HTTPS_PROXY")
        or os.environ.get("https_proxy")
        or os.environ.get("HTTP_PROXY")
        or os.environ.get("http_proxy")
    )
    if not proxy_url:
        # 尝试常见本地代理端口
        import socket
        for port in (7897, 7890, 1080, 8080):
            try:
                s = socket.create_connection(("127.0.0.1", port), timeout=1)
                s.close()
                proxy_url = f"http://127.0.0.1:{port}"
                break
            except OSError:
                continue

    start = time.monotonic()
    try:
        if proxy_url:
            proxy_handler = urllib.request.ProxyHandler({
                "http": proxy_url,
                "https": proxy_url,
            })
        else:
            proxy_handler = urllib.request.ProxyHandler()
        opener = urllib.request.build_opener(proxy_handler)
        req = urllib.request.Request(endpoint, method="HEAD")
        opener.open(req, timeout=_NETWORK_PROBE_TIMEOUT)
        latency_ms = (time.monotonic() - start) * 1000
        return True, latency_ms
    except Exception:
        return False, -1.0


def _wait_for_network(task_id: str = "") -> bool:
    """等待网络恢复。

    在重试前调用，避免在网络不通时浪费重试次数。
    先快速探测一次，如果通则立即返回。
    如果不通，使用分级等待策略循环探测直到恢复或超时：
      - 前 5 分钟：每 10 秒探测一次（快速恢复场景）
      - 5-30 分钟：每 30 秒探测一次（中等故障）
      - 30-60 分钟：每 60 秒探测一次（长时间宕机）

    在关键时间点通过 NotificationManager 发送告警：
      - 等待超过 2 分钟：WARNING
      - 等待超过 10 分钟：CRITICAL
      - 网络恢复时：恢复通知

    Returns:
        True 表示网络可用，False 表示等待超时仍不可用。
    """
    from .notification import get_notifier

    def _probe_once() -> bool:
        """探测所有端点，任一可达即返回 True。"""
        for endpoint in _NETWORK_PROBE_ENDPOINTS:
            reachable, _ = _probe_single_endpoint(endpoint)
            if reachable:
                return True
        return False

    def _get_interval(elapsed: float) -> float:
        """根据已等待时间返回当前探测间隔（分级策略）。"""
        if elapsed < _PROBE_TIER1_LIMIT:
            return _PROBE_TIER1_INTERVAL
        elif elapsed < _PROBE_TIER2_LIMIT:
            return _PROBE_TIER2_INTERVAL
        else:
            return _PROBE_TIER3_INTERVAL

    # 快速探测：网络正常则立即返回
    if _probe_once():
        return True

    # 网络不通，进入等待循环
    notifier = get_notifier()
    logger.warning(
        "[%s] 网络不可达，进入分级等待 (最长 %ds)...", task_id, _NETWORK_PROBE_MAX_WAIT
    )

    start_time = time.monotonic()
    warning_sent = False   # 是否已发送 WARNING 通知
    critical_sent = False  # 是否已发送 CRITICAL 通知
    alert_title = "网络中断"

    while True:
        waited = time.monotonic() - start_time
        if waited >= _NETWORK_PROBE_MAX_WAIT:
            break

        interval = _get_interval(waited)
        # 确保不会 sleep 超过剩余等待时间
        remaining = _NETWORK_PROBE_MAX_WAIT - waited
        time.sleep(min(interval, remaining))
        waited = time.monotonic() - start_time

        # 发送分级通知（仅在首次越过阈值时触发）
        if not warning_sent and waited >= _PROBE_NOTIFY_WARNING:
            logger.warning("[%s] 网络中断已超过 %d 秒，发送 WARNING 通知", task_id, int(waited))
            notifier.warning(
                alert_title,
                task_id=task_id,
                waited_seconds=int(waited),
                message=f"网络已中断超过 {int(waited)} 秒，正在等待恢复",
            )
            warning_sent = True

        if not critical_sent and waited >= _PROBE_NOTIFY_CRITICAL:
            logger.error("[%s] 网络中断已超过 %d 秒，发送 CRITICAL 通知", task_id, int(waited))
            notifier.critical(
                alert_title,
                task_id=task_id,
                waited_seconds=int(waited),
                message=f"网络已中断超过 {int(waited)} 秒，可能需要人工介入",
            )
            critical_sent = True

        # 探测网络
        if _probe_once():
            logger.info("[%s] 网络已恢复 (等待了 %ds)", task_id, int(waited))
            # 如果之前发过告警，发送恢复通知
            if warning_sent or critical_sent:
                notifier.resolve(alert_title)
            return True

        # 记录当前等待状态（使用当前层级对应的日志级别）
        if waited >= _PROBE_NOTIFY_CRITICAL:
            logger.error("[%s] 网络仍不可达，已等待 %ds / %ds", task_id, int(waited), _NETWORK_PROBE_MAX_WAIT)
        elif waited >= _PROBE_NOTIFY_WARNING:
            logger.warning("[%s] 网络仍不可达，已等待 %ds / %ds", task_id, int(waited), _NETWORK_PROBE_MAX_WAIT)
        else:
            logger.debug("[%s] 网络仍不可达，已等待 %ds / %ds", task_id, int(waited), _NETWORK_PROBE_MAX_WAIT)

    # 超时，发送最终通知
    logger.error("[%s] 网络等待超时 (%ds)，放弃", task_id, _NETWORK_PROBE_MAX_WAIT)
    if not critical_sent:
        notifier.critical(
            alert_title,
            task_id=task_id,
            waited_seconds=int(waited),
            message=f"网络等待超时 ({_NETWORK_PROBE_MAX_WAIT}s)，任务将失败",
        )
    return False


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


@dataclass
class CLIResponse:
    raw: str
    result: str
    is_error: bool
    cost_usd: float
    model: str
    token_input: int = 0
    token_output: int = 0
    cli_duration_ms: float = 0.0


@dataclass
class StreamProgress:
    """Tracks progress from stream-json events."""
    tool_uses: int = 0
    text_chunks: int = 0
    last_tool: str = ""
    last_text_preview: str = ""
    result_event: dict | None = None
    all_events: list[dict] = field(default_factory=list)
    on_progress: Callable | None = None
    _max_events: int = 500  # 防止内存无限增长
    token_input: int = 0
    token_output: int = 0
    cache_read_tokens: int = 0
    cache_creation_tokens: int = 0
    cli_duration_ms: float = 0.0
    turn_started: int = 0
    turn_completed: int = 0
    max_turns_exceeded: bool = False
    json_parse_errors: int = 0  # 流事件 JSON 解析失败次数
    json_parse_error_samples: list[str] = field(default_factory=list)  # 前 N 个解析错误详情，用于 TaskResult.error 诊断
    # 停滞检测
    consecutive_read_only_calls: int = 0  # 连续只读工具调用次数（Write/Edit 重置为 0）
    stagnation_warning_sent: bool = False  # 20 次阈值警告是否已发送
    stagnation_killed: bool = False  # 40 次阈值是否已强制终止

    def append_event(self, event: dict) -> None:
        """追加事件，超过上限时丢弃最早的事件。"""
        self.all_events.append(event)
        if len(self.all_events) > self._max_events:
            # 保留最后 _max_events 个事件
            self.all_events = self.all_events[-self._max_events:]


def _task_result_metrics(
    *,
    pid: int = 0,
    progress: StreamProgress | None = None,
    response: CLIResponse | None = None,
) -> dict[str, object]:
    if progress is None and response is None and not pid:
        return {}

    metrics: dict[str, object] = {}
    if pid:
        metrics["pid"] = pid
    if response is not None:
        metrics["token_input"] = response.token_input
        metrics["token_output"] = response.token_output
        metrics["cli_duration_ms"] = response.cli_duration_ms
    elif progress is not None:
        metrics["token_input"] = progress.token_input
        metrics["token_output"] = progress.token_output
        metrics["cli_duration_ms"] = progress.cli_duration_ms

    if progress is not None:
        metrics["tool_uses"] = progress.tool_uses
        metrics["turn_started"] = progress.turn_started
        metrics["turn_completed"] = progress.turn_completed
        metrics["max_turns_exceeded"] = progress.max_turns_exceeded
        metrics["cache_read_tokens"] = progress.cache_read_tokens
        metrics["cache_creation_tokens"] = progress.cache_creation_tokens
    return metrics


def _build_task_result(
    task_id: str,
    status: TaskStatus,
    *,
    started_at: datetime,
    finished_at: datetime,
    model_used: str,
    output: str | None = None,
    parsed_output: object = None,
    error: str | None = None,
    cost_usd: float = 0.0,
    duration_seconds: float = 0.0,
    pid: int = 0,
    progress: StreamProgress | None = None,
    response: CLIResponse | None = None,
) -> TaskResult:
    return TaskResult(
        task_id=task_id,
        status=status,
        output=output,
        parsed_output=parsed_output,
        error=error,
        cost_usd=cost_usd,
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        model_used=model_used,
        **_task_result_metrics(pid=pid, progress=progress, response=response),
    )


@dataclass
class ModelTokenUsage:
    """单个模型的 token 使用统计"""
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_creation_tokens: int = 0
    cost_usd: float = 0.0
    request_count: int = 0


class BudgetTracker:
    """Thread-safe global budget tracker.

    不再拦截预算超限，仅记录花费。预算检查由编排器上层统一管理。
    支持按模型分组的 token 使用统计，区分 cache_read 和 cache_creation。
    """

    def __init__(
        self,
        max_budget_usd: float,
        persist_path: str | None = None,
        enforcement_mode: str = "accounting",
    ):
        self._max = max_budget_usd
        self._spent = 0.0
        self._lock = threading.Lock()
        self._model_usage: dict[str, ModelTokenUsage] = {}
        # 持久化：每次 record_usage 后原子写入 JSON 文件
        self._persist_path = Path(persist_path) if persist_path else None
        self._entries: list[dict] = []  # 每条记录的详情
        self._enforcement_mode = enforcement_mode
        if self._enforcement_mode not in {"accounting", "hard_limit"}:
            raise ValueError(
                f"Unsupported enforcement_mode {self._enforcement_mode!r}; "
                "expected 'accounting' or 'hard_limit'"
            )

    @property
    def spent(self) -> float:
        with self._lock:
            return self._spent

    @spent.setter
    def spent(self, value: float) -> None:
        with self._lock:
            self._spent = value

    @property
    def limit(self) -> float:
        return self._max

    @property
    def remaining(self) -> float:
        if self._enforcement_mode == "hard_limit" and self._max > 0:
            with self._lock:
                return max(0.0, self._max - self._spent)
        return float("inf")

    @property
    def model_usage(self) -> dict[str, ModelTokenUsage]:
        """按模型分组的 token 使用统计（返回副本）。"""
        with self._lock:
            return dict(self._model_usage)

    def check_and_add(self, cost: float) -> None:
        """记录花费；hard_limit 模式下超限会抛 BudgetExhaustedError。"""
        with self._lock:
            normalized_cost = max(0.0, cost)
            self._ensure_can_charge_unlocked(normalized_cost)
            prev = self._spent
            self._spent += normalized_cost
            logger.debug("BudgetTracker._spend: cost=%.6f, spent %.6f -> %.6f", normalized_cost, prev, self._spent)

    def can_afford(self, estimated_cost: float = 0.0) -> bool:
        """hard_limit 模式下返回是否仍能支付；accounting 模式下始终为 True。"""
        if self._enforcement_mode != "hard_limit" or self._max <= 0:
            return True
        with self._lock:
            return self._spent + max(0.0, estimated_cost) <= self._max

    def add_spent(self, cost: float) -> None:
        """原子地增加已花费金额（不检查预算上限）。"""
        with self._lock:
            prev = self._spent
            self._spent += cost
            logger.debug("BudgetTracker.add_spent: cost=%.6f, spent %.6f -> %.6f", cost, prev, self._spent)

    def add_cost(self, amount: float) -> None:
        """添加成本到已花费金额（用于断点续传恢复预算）。"""
        with self._lock:
            prev = self._spent
            self._spent += amount
            logger.debug("BudgetTracker.add_cost: amount=%.6f, spent %.6f -> %.6f", amount, prev, self._spent)

    def get_total_cost(self) -> float:
        """返回所有线程累加的总花费（线程安全）。"""
        with self._lock:
            return self._spent

    def get_cost_by_model(self) -> dict[str, float]:
        """返回按模型分组的成本映射（线程安全）。"""
        with self._lock:
            return {model: round(u.cost_usd, 6) for model, u in self._model_usage.items()}

    @property
    def total_cost(self) -> float:
        """总花费（属性访问方式，等价于 get_total_cost）。"""
        with self._lock:
            return self._spent

    @property
    def remaining_budget(self) -> float:
        """剩余预算。"""
        with self._lock:
            return max(0.0, self._max - self._spent)

    def record_usage(self, model: str, input_tokens: int = 0, output_tokens: int = 0,
                     cache_read_tokens: int = 0, cache_creation_tokens: int = 0,
                     cost_usd: float = 0.0) -> None:
        """记录模型级别的 token 使用（含 cache token 区分），并持久化到文件。"""
        with self._lock:
            self._ensure_can_charge_unlocked(max(0.0, cost_usd))
            if model not in self._model_usage:
                self._model_usage[model] = ModelTokenUsage()
            usage = self._model_usage[model]
            usage.input_tokens += input_tokens
            usage.output_tokens += output_tokens
            usage.cache_read_tokens += cache_read_tokens
            usage.cache_creation_tokens += cache_creation_tokens
            usage.cost_usd += cost_usd
            usage.request_count += 1
            prev_spent = self._spent
            self._spent += cost_usd
            logger.debug(
                "BudgetTracker.record_usage: model=%s, cost_usd=%.6f, "
                "spent %.6f -> %.6f, tokens in=%d/out=%d",
                model, cost_usd, prev_spent, self._spent,
                input_tokens, output_tokens,
            )
            logger.info(
                'record_usage: model=%s, cost_usd=%.6f, new_total_spent=%.4f',
                model, cost_usd, self._spent,
            )
            # 追加条目并持久化
            self._entries.append({
                "timestamp": datetime.now().isoformat(),
                "model": model,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cache_read_tokens": cache_read_tokens,
                "cache_creation_tokens": cache_creation_tokens,
                "cost_usd": round(cost_usd, 6),
            })
            self._persist_to_file_unlocked()

    def _ensure_can_charge_unlocked(self, delta_cost: float) -> None:
        if self._enforcement_mode != "hard_limit":
            return
        if self._max <= 0:
            return
        projected = self._spent + delta_cost
        if projected <= self._max:
            return
        raise BudgetExhaustedError(
            f"Budget limit exceeded: projected=${projected:.4f} > limit=${self._max:.4f}"
        )

    @property
    def total_request_count(self) -> int:
        """全局累计 CLI 请求次数（所有模型的 request_count 之和）。"""
        with self._lock:
            return sum(u.request_count for u in self._model_usage.values())

    def cache_hit_rate(self) -> float:
        """全局缓存命中率：cache_read / (input + cache_read)。"""
        with self._lock:
            total_input = sum(u.input_tokens for u in self._model_usage.values())
            total_cache = sum(u.cache_read_tokens for u in self._model_usage.values())
            denominator = total_input + total_cache
            return total_cache / denominator if denominator > 0 else 0.0

    def summary(self) -> dict:
        """完整的预算摘要，包含按模型分组的 token 详情。"""
        with self._lock:
            return {
                "total_spent_usd": self._spent,
                "max_budget_usd": self._max,
                "remaining_usd": max(0, self._max - self._spent),
                "cache_hit_rate": self._cache_hit_rate_unlocked(),
                "models": {
                    model: {
                        "input_tokens": u.input_tokens,
                        "output_tokens": u.output_tokens,
                        "cache_read_tokens": u.cache_read_tokens,
                        "cache_creation_tokens": u.cache_creation_tokens,
                        "cost_usd": round(u.cost_usd, 6),
                        "request_count": u.request_count,
                    }
                    for model, u in self._model_usage.items()
                }
            }

    def _cache_hit_rate_unlocked(self) -> float:
        """不加锁版本的缓存命中率，仅供内部 summary() 使用。"""
        total_input = sum(u.input_tokens for u in self._model_usage.values())
        total_cache = sum(u.cache_read_tokens for u in self._model_usage.values())
        denominator = total_input + total_cache
        return total_cache / denominator if denominator > 0 else 0.0

    def _persist_to_file_unlocked(self) -> None:
        """原子写入 budget_tracker.json（调用方已持有锁）。

        格式：{total_cost, last_update, entries[]}
        使用 tmp + rename 实现原子写入，防止写入中断导致文件损坏。
        """
        if self._persist_path is None:
            return
        try:
            data = {
                "total_cost": round(self._spent, 6),
                "last_update": datetime.now().isoformat(),
                "entries": self._entries,
            }
            # 确保父目录存在
            self._persist_path.parent.mkdir(parents=True, exist_ok=True)
            # 原子写入：先写临时文件，再 rename
            tmp_fd, tmp_path = tempfile.mkstemp(
                dir=str(self._persist_path.parent),
                suffix=".tmp",
            )
            try:
                with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                # Windows 下 rename 需要目标文件不存在（或用 os.replace 覆盖）
                os.replace(tmp_path, str(self._persist_path))
            except Exception:
                # 写入失败时清理临时文件
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
            logger.debug(
                "BudgetTracker 持久化: total_cost=%.4f, entries=%d -> %s",
                self._spent, len(self._entries), self._persist_path,
            )
        except Exception as e:
            # 持久化失败不应阻塞主流程，仅记录警告
            logger.warning("BudgetTracker 持久化失败: %s", e)


def _build_model_fallback_candidates(model: str) -> list[str]:
    """为模型降级构建候选列表。

    如果模型名以 "-pro" 结尾，尝试去掉 "-pro" 作为降级候选。
    """
    requested = str(model or "").strip()
    if not requested:
        return []
    candidates: list[str] = []
    if requested.endswith("-pro"):
        candidates.append(requested[:-4])
    return [c for c in candidates if c and c != requested]


def _should_use_model_fallback(result: TaskResult, current_model: str) -> bool:
    """判断是否应该尝试降级模型。

    仅当当前模型以 "-pro" 结尾、任务失败、且错误信息包含限流或网络错误时才降级。
    """
    if not current_model.endswith("-pro"):
        return False
    if result.status == TaskStatus.SUCCESS:
        return False
    text = "\n".join(part for part in [result.error or "", result.output or ""] if part)
    if not text.strip():
        return False
    return looks_like_rate_limit_error(text) or looks_like_network_error(text)


def _parse_cli_output(raw: str) -> CLIResponse:
    """Parse the JSON envelope from claude -p --output-format json."""
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        # JSON 解析失败：记录警告
        # 注意：CLI 可能返回非 JSON 的纯文本（如错误信息），此时 is_error 由调用方判定
        logger.warning(
            "CLI 输出 JSON 解析失败: %s | 原始输出前200字符: %s",
            e, raw[:200],
        )
        # 如果原始输出为空，标记为错误（CLI 未产生有效输出）
        if not raw.strip():
            return CLIResponse(
                raw=raw, result="[PARSE_ERROR] CLI 输出为空且非 JSON",
                is_error=True, cost_usd=0.0, model="",
            )
        return CLIResponse(raw=raw, result=raw.strip(), is_error=False, cost_usd=0.0, model="")

    if isinstance(data, dict) and data.get("type") == "result":
        # 从 modelUsage 提取模型名（CLI 不再在顶层输出 model 字段）
        model_name = data.get("model", "")
        if not model_name:
            model_usage = data.get("modelUsage", {})
            if model_usage:
                model_name = next(iter(model_usage), "")
        return CLIResponse(
            raw=raw,
            result=data.get("result", ""),
            is_error=data.get("is_error", False),
            cost_usd=_extract_cost_usd(data),
            model=model_name,
        )

    return CLIResponse(raw=raw, result=raw.strip(), is_error=False, cost_usd=0.0, model="")


# 模型定价表：每百万 token 的美元价格 (input, output, cache_read, cache_creation)
_MODEL_PRICING_PER_MILLION: dict[str, tuple[float, float, float, float]] = {
    "claude-opus-4": (15.0, 75.0, 1.5, 18.75),
    "claude-sonnet-4": (3.0, 15.0, 0.3, 3.75),
    "claude-3-5-sonnet": (3.0, 15.0, 0.3, 3.75),
    "claude-3-5-haiku": (0.80, 4.0, 0.08, 1.0),
    "claude-3-opus": (15.0, 75.0, 1.5, 18.75),
    "claude-3-sonnet": (3.0, 15.0, 0.3, 3.75),
    "claude-3-haiku": (0.25, 1.25, 0.025, 0.3125),
}

# 未知模型默认定价（Sonnet 级别）
_DEFAULT_PRICING = (3.0, 15.0, 0.3, 3.75)


def _get_model_pricing(model_name: str) -> tuple[float, float, float, float]:
    """根据模型名获取定价 (input, output, cache_read, cache_creation 每百万 token)。"""
    if not model_name:
        return _DEFAULT_PRICING
    name_lower = model_name.lower()
    for key, pricing in _MODEL_PRICING_PER_MILLION.items():
        if key in name_lower:
            return pricing
    return _DEFAULT_PRICING


def _estimate_cost_from_tokens(event: dict) -> float:
    """当 CLI 不报告成本时，从 token 使用量估算成本。

    支持两种字段命名风格:
    - snake_case: input_tokens, output_tokens, cache_read_input_tokens, cache_creation_input_tokens
    - camelCase: inputTokens, outputTokens, cacheReadInputTokens, cacheCreationInputTokens
      （Claude CLI stream-json 格式使用的命名）

    同时从 modelUsage 中汇总所有模型的 token 计数（CLI 2025+ 版本的主要 token 数据来源）。
    """
    usage = event.get("usage", {})
    if not isinstance(usage, dict):
        usage = {}

    # 从 usage 对象或顶层提取 token 计数，同时兼容 snake_case 和 camelCase
    input_tokens = (
        event.get("input_tokens", 0)
        or event.get("inputTokens", 0)
        or usage.get("input_tokens", 0)
        or usage.get("inputTokens", 0)
        or event.get("token_count_input", 0)
        or 0
    )
    output_tokens = (
        event.get("output_tokens", 0)
        or event.get("outputTokens", 0)
        or usage.get("output_tokens", 0)
        or usage.get("outputTokens", 0)
        or event.get("token_count_output", 0)
        or 0
    )
    cache_read = (
        usage.get("cache_read_input_tokens", 0)
        or usage.get("cacheReadInputTokens", 0)
        or event.get("cache_read_input_tokens", 0)
        or event.get("cacheReadInputTokens", 0)
        or 0
    )
    cache_creation = (
        usage.get("cache_creation_input_tokens", 0)
        or usage.get("cacheCreationInputTokens", 0)
        or event.get("cache_creation_input_tokens", 0)
        or event.get("cacheCreationInputTokens", 0)
        or 0
    )

    # 从 modelUsage 汇总所有模型的 token 计数（CLI 2025+ 版本）
    # 如果前面的提取都没找到 token 数据，尝试从 modelUsage 中提取
    if not (input_tokens or output_tokens or cache_read or cache_creation):
        model_usage = event.get("modelUsage", {})
        if isinstance(model_usage, dict) and model_usage:
            for _model_key, model_data in model_usage.items():
                if not isinstance(model_data, dict):
                    continue
                input_tokens += (
                    model_data.get("input_tokens", 0)
                    or model_data.get("inputTokens", 0)
                    or 0
                )
                output_tokens += (
                    model_data.get("output_tokens", 0)
                    or model_data.get("outputTokens", 0)
                    or 0
                )
                cache_read += (
                    model_data.get("cache_read_input_tokens", 0)
                    or model_data.get("cacheReadInputTokens", 0)
                    or 0
                )
                cache_creation += (
                    model_data.get("cache_creation_input_tokens", 0)
                    or model_data.get("cacheCreationInputTokens", 0)
                    or 0
                )

    if not (input_tokens or output_tokens or cache_read or cache_creation):
        return 0.0
    # 推断模型名
    model_name = event.get("model", "")
    if not model_name:
        model_usage = event.get("modelUsage", {})
        if isinstance(model_usage, dict) and model_usage:
            model_name = next(iter(model_usage), "")
    inp_price, out_price, cache_read_price, cache_creation_price = _get_model_pricing(model_name)
    cost = (
        input_tokens * inp_price / 1_000_000
        + output_tokens * out_price / 1_000_000
        + cache_read * cache_read_price / 1_000_000
        + cache_creation * cache_creation_price / 1_000_000
    )
    if cost > 0:
        logger.info(
            "Token-based cost estimation: model=%s, tokens=%d/%d (cache: %d read, %d created) -> $%.6f",
            model_name or "unknown", input_tokens, output_tokens, cache_read, cache_creation, cost,
        )
    return cost


def _recursive_find_cost(obj: object, max_depth: int = 4, _depth: int = 0) -> float:
    """递归扫描所有嵌套 key，查找含 cost/usd 的数值字段。

    匹配规则：key 名（小写）包含 'cost' 或以 '_usd' 结尾 或等于 'usd'。
    跳过已知的非成本字段（如 session_id 等含 cost 字样的非数值字段）。
    遇到第一个有效值即返回（不累加，避免重复计算）。
    """
    if _depth > max_depth or not isinstance(obj, dict):
        return 0.0
    # 已知的非成本 key 前缀，避免误匹配
    _skip_prefixes = ("session_", "request_", "response_")
    cost_indicators = ("cost",)
    usd_indicators = ("_usd", "usd")
    # camelCase 变体：CostUSD, totalCost, costUsd 等
    camel_cost_indicators = ("Cost",)
    for key, val in obj.items():
        if not isinstance(key, str):
            continue
        kl = key.lower()
        # 检查是否是成本相关字段（支持 snake_case 和 camelCase）
        is_cost_key = (
            any(ci in kl for ci in cost_indicators)
            or any(kl.endswith(ui) for ui in usd_indicators)
            or kl == "usd"
            # camelCase 匹配：key 中包含 Cost（大写 C），如 totalCost、costUSD
            or any(ci in key for ci in camel_cost_indicators)
        )
        if is_cost_key and not any(kl.startswith(sp) for sp in _skip_prefixes):
            # 尝试转为浮点数
            if isinstance(val, (int, float)) and val > 0:
                logger.debug("_recursive_find_cost 命中 key='%s' val=%s depth=%d", key, val, _depth)
                return float(val)
            if isinstance(val, str):
                try:
                    fv = float(val)
                    if fv > 0:
                        logger.debug("_recursive_find_cost 命中 key='%s' val='%s' depth=%d", key, val, _depth)
                        return fv
                except (ValueError, TypeError):
                    pass
        # 递归深入嵌套 dict
        if isinstance(val, dict):
            found = _recursive_find_cost(val, max_depth, _depth + 1)
            if found > 0:
                return found
    return 0.0


def _try_float(val: object) -> float:
    """安全地将值转为 float，跳过 dict/list 等非数值类型。

    Claude CLI 有时会在 cost_usd 字段返回 dict（如嵌套对象），
    裸 float() 会抛 TypeError，此函数安全地返回 0.0。
    """
    if isinstance(val, (dict, list, tuple, set)):
        return 0.0
    try:
        return float(val)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0


def _extract_cost_usd(event: dict) -> float:
    """从 CLI result 事件中提取成本。

    提取策略（按优先级）:
    - 策略 1-9: 从 CLI 输出的显式成本字段提取
    - 策略 10: 从 token 使用量 + 模型定价估算成本（最终 fallback）

    所有 float() 转换均通过 _try_float() 安全处理，
    防止 cost_usd 字段为 dict 时 TypeError 崩溃。
    """
    # 调试：记录 event 的顶层 key 列表和嵌套结构，用于诊断 cost 字段名
    top_keys = sorted(event.keys())
    nested_info = {}
    for key in ("usage", "modelUsage", "cost", "costs"):
        val = event.get(key)
        if isinstance(val, dict):
            nested_info[key] = sorted(val.keys())
        elif val is not None:
            nested_info[key] = type(val).__name__
    logger.debug(
        "_extract_cost_usd 诊断: top_keys=%s, nested=%s",
        top_keys, nested_info,
    )
    cost = 0.0
    # 1. 顶层 total_cost_usd（最可靠，明确含单位后缀）
    val = event.get("total_cost_usd", 0.0)
    if val and _try_float(val) > 0:
        cost = _try_float(val)
    # 2. 顶层 cost_usd（标准字段）
    elif _try_float(event.get("cost_usd", 0.0)) > 0:
        cost = _try_float(event.get("cost_usd", 0.0))
    # 3. 顶层 total_cost（无 _usd 后缀变体）
    elif _try_float(event.get("total_cost", 0.0)) > 0:
        cost = _try_float(event.get("total_cost", 0.0))
    # 4. 顶层 costUSD（camelCase 变体）
    elif _try_float(event.get("costUSD", 0.0)) > 0:
        cost = _try_float(event.get("costUSD", 0.0))
    # 5. 拆分成本：input_cost_usd + output_cost_usd
    elif _try_float(event.get("input_cost_usd", 0.0)) > 0 or _try_float(event.get("output_cost_usd", 0.0)) > 0:
        cost = _try_float(event.get("input_cost_usd", 0.0)) + _try_float(event.get("output_cost_usd", 0.0))
    # 6. 拆分成本：input_cost + output_cost（无 _usd 后缀）
    elif _try_float(event.get("input_cost", 0.0)) > 0 or _try_float(event.get("output_cost", 0.0)) > 0:
        cost = _try_float(event.get("input_cost", 0.0)) + _try_float(event.get("output_cost", 0.0))
    # 7. 顶层 cost（最短字段名，最后兜底）
    elif _try_float(event.get("cost", 0.0)) > 0:
        cost = _try_float(event.get("cost", 0.0))
    else:
        # 8. 嵌套在 usage 对象中
        usage = event.get("usage", {})
        if isinstance(usage, dict):
            usage_cost = (
                _try_float(usage.get("cost_usd", 0.0))
                or _try_float(usage.get("costUSD", 0.0))
                or _try_float(usage.get("cost", 0.0))
            )
            if usage_cost:
                cost = usage_cost
        # 9. 从 modelUsage 提取（汇总所有模型的 cost，同时支持 camelCase 和 snake_case）
        if cost == 0.0:
            model_usage = event.get("modelUsage", {})
            if model_usage:
                total = sum(
                    _try_float(
                        m.get("costUSD", 0.0)
                        or m.get("cost_usd", 0.0)
                        or m.get("totalCostUSD", 0.0)
                        or m.get("total_cost_usd", 0.0)
                        or m.get("cost", 0.0)
                    )
                    for m in model_usage.values()
                    if isinstance(m, dict)
                )
                if total > 0:
                    cost = total
        # 9.5 递归兜底：遍历 event 所有嵌套 key 查找含 cost/usd 的字段
        # 防止 CLI 格式变更导致新字段名无法被上述策略匹配
        if cost == 0.0:
            cost = _recursive_find_cost(event, max_depth=4)
        # 9.7 从 modelUsage 的 token 计数估算成本（CLI 2025+ 版本可能只有 token 没有直接 cost 字段）
        if cost == 0.0:
            model_usage = event.get("modelUsage", {})
            if isinstance(model_usage, dict) and model_usage:
                est_total = 0.0
                for model_key, model_data in model_usage.items():
                    if not isinstance(model_data, dict):
                        continue
                    inp = model_data.get("inputTokens", 0) or model_data.get("input_tokens", 0) or 0
                    out = model_data.get("outputTokens", 0) or model_data.get("output_tokens", 0) or 0
                    cr = model_data.get("cacheReadInputTokens", 0) or model_data.get("cache_read_input_tokens", 0) or 0
                    cc = model_data.get("cacheCreationInputTokens", 0) or model_data.get("cache_creation_input_tokens", 0) or 0
                    if inp or out or cr or cc:
                        inp_p, out_p, cr_p, cc_p = _get_model_pricing(model_key)
                        est = inp * inp_p / 1_000_000 + out * out_p / 1_000_000 + cr * cr_p / 1_000_000 + cc * cc_p / 1_000_000
                        est_total += est
                        logger.debug(
                            "modelUsage token 估算: model=%s, tokens=%d/%d/%d/%d -> $%.6f",
                            model_key, inp, out, cr, cc, est,
                        )
                if est_total > 0:
                    cost = est_total
                    logger.info("从 modelUsage token 计数估算总成本: $%.6f", cost)
    # 10. 最终 fallback：从 token 使用量估算成本（已增强 camelCase 支持）
    if cost == 0.0:
        cost = _estimate_cost_from_tokens(event)
    logger.debug(f'extracted cost={cost} from event keys={list(event.keys())}')
    return cost


def _parse_stream_event(line: str, task_id: str, progress: StreamProgress) -> None:
    """Parse a single stream-json event line and log progress."""
    line = line.strip()
    if not line:
        return

    try:
        event = json.loads(line)
    except json.JSONDecodeError as e:
        # 记录解析失败，帮助诊断 CLI 输出异常问题
        progress.json_parse_errors += 1
        # 保留前 3 个解析错误详情，供 TaskResult.error 使用
        if len(progress.json_parse_error_samples) < 3:
            progress.json_parse_error_samples.append(f"{e} | 原始行: {line[:150]}")
        logger.warning(
            "[%s] 流事件 JSON 解析失败 (#%d): %s | 原始行: %s",
            task_id, progress.json_parse_errors, e, line[:200],
        )
        return

    progress.append_event(event)
    event_type = event.get("type", "")

    if event_type == "assistant":
        # Assistant turn with content blocks — 计数 turn
        progress.turn_started += 1
        message = event.get("message", {})
        content = message.get("content", [])
        for block in content:
            block_type = block.get("type", "")
            if block_type == "tool_use":
                tool_name = block.get("name", "unknown")
                tool_input = block.get("input", {})
                progress.tool_uses += 1
                progress.last_tool = tool_name
                # 停滞检测：Write/Edit 重置计数器，其他工具递增
                if tool_name in ("Write", "Edit"):
                    if progress.consecutive_read_only_calls > 0:
                        logger.info(
                            "[%s] 停滞检测重置: 写入工具 %s（之前连续 %d 次只读）",
                            task_id, tool_name, progress.consecutive_read_only_calls,
                        )
                    progress.consecutive_read_only_calls = 0
                else:
                    progress.consecutive_read_only_calls += 1
                # Log tool usage with brief input summary
                input_preview = str(tool_input)[:120]
                logger.info(
                    "[%s] 🔧 工具调用 #%d: %s | %s",
                    task_id, progress.tool_uses, tool_name, input_preview,
                )
            elif block_type == "text":
                text = block.get("text", "")
                if text.strip():
                    progress.text_chunks += 1
                    progress.last_text_preview = text[:100]
                    # Only log substantial text (not tiny fragments)
                    if len(text.strip()) > 20:
                        logger.info(
                            "[%s] 📝 文本输出 #%d: %s",
                            task_id, progress.text_chunks, text[:150].replace("\n", " "),
                        )
        # 从 assistant 事件的 message.usage 中累加 token（result 事件可能不存在）
        # 仅在 progress 计数仍为 0 时填充，避免与 result 事件的更精确值冲突
        a_usage = message.get("usage", {}) if isinstance(message, dict) else {}
        if isinstance(a_usage, dict):
            if not progress.token_input:
                progress.token_input = a_usage.get("input_tokens", 0) or a_usage.get("inputTokens", 0) or 0
            if not progress.token_output:
                progress.token_output = a_usage.get("output_tokens", 0) or a_usage.get("outputTokens", 0) or 0
            if not progress.cache_read_tokens:
                progress.cache_read_tokens = a_usage.get("cache_read_input_tokens", 0) or a_usage.get("cacheReadInputTokens", 0) or 0
            if not progress.cache_creation_tokens:
                progress.cache_creation_tokens = a_usage.get("cache_creation_input_tokens", 0) or a_usage.get("cacheCreationInputTokens", 0) or 0

    elif event_type == "item.completed":
        # 兼容旧版 stream-json：agent_message 文本承载在 item.completed 中。
        item = event.get("item", {})
        if item.get("type") in {"agent_message", "assistant_message"}:
            text = item.get("text", "")
            if text.strip():
                progress.text_chunks += 1
                progress.last_text_preview = text[:100]

    elif event_type == "tool_result":
        # Tool execution result
        tool_name = event.get("tool_name", "")
        is_error = event.get("is_error", False)
        content = event.get("content", "")
        status = "❌ 失败" if is_error else "✅ 完成"
        content_preview = str(content)[:100].replace("\n", " ") if content else ""
        logger.info("[%s] %s %s | %s", task_id, status, tool_name, content_preview)

    elif event_type == "result":
        # Final result event — 计数完成的 turn
        progress.turn_completed += 1
        progress.result_event = event
        # 调试：记录完整 result event 以诊断 cost 字段缺失问题
        logger.debug(
            "[%s] result event 完整内容:\n%s",
            task_id, json.dumps(event, indent=2, ensure_ascii=False),
        )
        # 成本字段：CLI 使用 total_cost_usd（顶层），旧版可能用 cost_usd
        cost = _extract_cost_usd(event)
        is_error = event.get("is_error", False)
        logger.info(
            "[%s] 🏁 任务结束 | 工具调用: %d 次 | 费用: $%.4f | 错误: %s",
            task_id, progress.tool_uses, cost, is_error,
        )
        # 解析 token 使用量和 CLI duration
        # Claude CLI 将 token 嵌套在 usage 对象中，同时支持顶层字段做兼容
        # 同时兼容 snake_case（input_tokens）和 camelCase（inputTokens）命名
        # 重要：仅在提取到非零值时更新 progress，避免 result 事件无 token 数据时
        # 覆盖从 assistant 事件中已收集的非零 token 值（这是 cost 始终为 0 的根因）
        usage = event.get("usage", {})
        _inp = (
            event.get("token_count_input", 0)
            or event.get("input_tokens", 0)
            or event.get("inputTokens", 0)
            or usage.get("input_tokens", 0)
            or usage.get("inputTokens", 0)
        )
        _out = (
            event.get("token_count_output", 0)
            or event.get("output_tokens", 0)
            or event.get("outputTokens", 0)
            or usage.get("output_tokens", 0)
            or usage.get("outputTokens", 0)
        )
        if _inp:
            progress.token_input = _inp
        if _out:
            progress.token_output = _out
        # 从 modelUsage 提取 token（CLI 2025+ 版本可能将 token 放在此处而非顶层）
        model_usage = event.get("modelUsage", {})
        if isinstance(model_usage, dict) and model_usage:
            if not progress.token_input or not progress.token_output:
                for _mk, _md in model_usage.items():
                    if not isinstance(_md, dict):
                        continue
                    if not progress.token_input:
                        _v = _md.get("input_tokens", 0) or _md.get("inputTokens", 0) or 0
                        if _v:
                            progress.token_input = _v
                    if not progress.token_output:
                        _v = _md.get("output_tokens", 0) or _md.get("outputTokens", 0) or 0
                        if _v:
                            progress.token_output = _v
            # cache tokens 也从 modelUsage 提取
            if not progress.cache_read_tokens or not progress.cache_creation_tokens:
                for _mk, _md in model_usage.items():
                    if not isinstance(_md, dict):
                        continue
                    if not progress.cache_read_tokens:
                        _v = _md.get("cache_read_input_tokens", 0) or _md.get("cacheReadInputTokens", 0) or 0
                        if _v:
                            progress.cache_read_tokens = _v
                    if not progress.cache_creation_tokens:
                        _v = _md.get("cache_creation_input_tokens", 0) or _md.get("cacheCreationInputTokens", 0) or 0
                        if _v:
                            progress.cache_creation_tokens = _v

    elif event_type == "turn.completed":
        # 兼容旧版 stream-json：turn.completed 携带 usage，但不一定有 result 事件。
        progress.turn_completed += 1
        usage = event.get("usage", {})
        if isinstance(usage, dict):
            progress.token_input = usage.get("input_tokens", 0) or usage.get("inputTokens", 0) or progress.token_input
            progress.token_output = usage.get("output_tokens", 0) or usage.get("outputTokens", 0) or progress.token_output
            progress.cache_read_tokens = (
                usage.get("cache_read_input_tokens", 0)
                or usage.get("cacheReadInputTokens", 0)
                or progress.cache_read_tokens
            )
            progress.cache_creation_tokens = (
                usage.get("cache_creation_input_tokens", 0)
                or usage.get("cacheCreationInputTokens", 0)
                or progress.cache_creation_tokens
            )
        progress.cli_duration_ms = event.get("duration_ms", 0.0) or event.get("duration", 0.0)
        # 提取 cache token（Anthropic API 特有字段，嵌套在 usage 中）
        _cr = (
            event.get("cache_read_input_tokens", 0)
            or event.get("cacheReadInputTokens", 0)
            or usage.get("cache_read_input_tokens", 0)
            or usage.get("cacheReadInputTokens", 0)
            or 0
        )
        _cc = (
            event.get("cache_creation_input_tokens", 0)
            or event.get("cacheCreationInputTokens", 0)
            or usage.get("cache_creation_input_tokens", 0)
            or usage.get("cacheCreationInputTokens", 0)
            or 0
        )
        if _cr:
            progress.cache_read_tokens = _cr
        if _cc:
            progress.cache_creation_tokens = _cc

    elif event_type == "system":
        # System messages (e.g., init, model info)
        msg = event.get("message", "") or event.get("subtype", "")
        if msg:
            logger.info("[%s] ⚙️ 系统: %s", task_id, str(msg)[:150])

    # Call on_progress callback if provided
    if progress.on_progress is not None:
        progress.on_progress(event_type, event)


def _build_response_from_stream(progress: StreamProgress) -> CLIResponse:
    """Build a CLIResponse from collected stream events."""
    if progress.result_event:
        evt = progress.result_event
        # 从 modelUsage 提取模型名（CLI 不再在顶层输出 model 字段）
        model_name = evt.get("model", "")
        if not model_name:
            model_usage = evt.get("modelUsage", {})
            if model_usage:
                model_name = next(iter(model_usage), "")
        extracted_cost = _extract_cost_usd(evt)
        # 最终兜底：result 事件不含 cost/token 数据时，用 progress 中从 assistant 事件
        # 收集的 token 数据估算成本（修复 cost 始终为 0 的根因）
        if extracted_cost == 0.0 and (progress.token_input or progress.token_output
                                      or progress.cache_read_tokens or progress.cache_creation_tokens):
            synthetic = {
                "input_tokens": progress.token_input,
                "output_tokens": progress.token_output,
                "cache_read_input_tokens": progress.cache_read_tokens,
                "cache_creation_input_tokens": progress.cache_creation_tokens,
                "model": model_name,
            }
            estimated = _estimate_cost_from_tokens(synthetic)
            if estimated > 0:
                logger.info(
                    "result_event cost=0，用 progress token 估算: model=%s, tokens=%d/%d -> $%.6f",
                    model_name or "unknown", progress.token_input, progress.token_output, estimated,
                )
                extracted_cost = estimated
        return CLIResponse(
            raw=json.dumps(evt, ensure_ascii=False),
            result=evt.get("result", ""),
            is_error=evt.get("is_error", False),
            cost_usd=extracted_cost,
            model=model_name,
            token_input=progress.token_input,
            token_output=progress.token_output,
            cli_duration_ms=progress.cli_duration_ms,
        )

    # No result event found — assemble text from assistant messages
    text_parts = []
    for evt in progress.all_events:
        if evt.get("type") == "assistant":
            for block in evt.get("message", {}).get("content", []):
                if block.get("type") == "text":
                    text_parts.append(block.get("text", ""))
        elif evt.get("type") == "item.completed":
            item = evt.get("item", {})
            if item.get("type") in {"agent_message", "assistant_message"}:
                text_parts.append(item.get("text", ""))

    combined = "".join(text_parts)

    # 如果没有任何输出且存在 JSON 解析错误，标记为错误（防止空响应伪装成功）
    if not combined.strip() and progress.json_parse_errors > 0:
        logger.error(
            "流式响应无 result 事件且 JSON 解析失败 %d 次，总事件数 %d，疑似输出解析全部失败",
            progress.json_parse_errors, len(progress.all_events),
        )
        return CLIResponse(
            raw="",
            result=f"[PARSE_ERROR] 流式响应无 result 事件，{progress.json_parse_errors} 个事件 JSON 解析失败",
            is_error=True,
            cost_usd=0.0,
            model="",
            token_input=progress.token_input,
            token_output=progress.token_output,
            cli_duration_ms=progress.cli_duration_ms,
        )

    if not combined.strip() and len(progress.all_events) == 0:
        # 完全没有任何事件 — CLI 输出为空，可能是进程异常
        logger.error("流式响应完全为空：0 个事件，0 个解析错误")
        return CLIResponse(
            raw="",
            result="[EMPTY_RESPONSE] CLI 输出完全为空，未收到任何流事件",
            is_error=True,
            cost_usd=0.0,
            model="",
            token_input=progress.token_input,
            token_output=progress.token_output,
            cli_duration_ms=progress.cli_duration_ms,
        )

    # result_event 为 None 的回退路径：尝试从已收集的事件中提取成本
    # 遍历 all_events 查找含 cost/usage 数据的事件（优先 result，其次 assistant）
    fallback_cost = 0.0
    fallback_model = ""
    for evt in reversed(progress.all_events):
        evt_type = evt.get("type", "")
        # 尝试从任意事件中提取成本
        c = _extract_cost_usd(evt)
        if c > 0 and fallback_cost == 0.0:
            fallback_cost = c
            logger.info("_build_response_from_stream 回退路径从 %s 事件提取到成本: $%.4f", evt_type, c)
        # 尝试提取模型名
        if not fallback_model:
            m = evt.get("model", "")
            if not m:
                mu = evt.get("modelUsage", {})
                if isinstance(mu, dict) and mu:
                    m = next(iter(mu), "")
            if m:
                fallback_model = m
        # 两个都找到了就提前退出
        if fallback_cost > 0 and fallback_model:
            break

    # 如果从事件中也没提取到成本，用 progress 中的 token 计数估算
    if fallback_cost == 0.0 and (progress.token_input or progress.token_output):
        synthetic_event = {
            "input_tokens": progress.token_input,
            "output_tokens": progress.token_output,
            "cache_read_input_tokens": progress.cache_read_tokens,
            "cache_creation_input_tokens": progress.cache_creation_tokens,
        }
        fallback_cost = _estimate_cost_from_tokens(synthetic_event)
        if fallback_cost > 0:
            logger.info("_build_response_from_stream 回退路径 token 估算成本: $%.6f", fallback_cost)

    return CLIResponse(
        raw=combined,
        result=combined,
        is_error=False,
        cost_usd=fallback_cost,
        model=fallback_model,
        token_input=progress.token_input,
        token_output=progress.token_output,
        cli_duration_ms=progress.cli_duration_ms,
    )


def run_claude_task(
    task: TaskNode,
    prompt: str,
    claude_config: ClaudeConfig,
    limits: LimitsConfig,
    budget_tracker: BudgetTracker | None = None,
    working_dir: str | None = None,
    on_progress: Callable[[str, dict], None] | None = None,
    audit_logger: AuditLogger | None = None,
    rate_limiter: "RateLimiter | None" = None,
) -> TaskResult:
    """Execute a single task via claude -p --output-format stream-json."""
    set_task_id(task.id)
    try:
        return _run_claude_task_inner(
            task, prompt, claude_config, limits, budget_tracker, working_dir, on_progress,
            audit_logger, rate_limiter,
        )
    finally:
        set_task_id("")


def _run_claude_task_inner(
    task: TaskNode,
    prompt: str,
    claude_config: ClaudeConfig,
    limits: LimitsConfig,
    budget_tracker: BudgetTracker | None = None,
    working_dir: str | None = None,
    on_progress: Callable[[str, dict], None] | None = None,
    audit_logger: AuditLogger | None = None,
    rate_limiter: "RateLimiter | None" = None,
) -> TaskResult:
    """Execute a single task via claude -p --output-format stream-json."""
    started_at = datetime.now()

    # Rate limiting: acquire token before task execution
    model = task.model or claude_config.default_model
    if rate_limiter is not None:
        logger.info("[%s] Waiting for rate limit token (model=%s)...", task.id, model)
        rate_limiter.acquire(model)
        logger.info("[%s] Rate limit token acquired", task.id)


    # 记录 prompt 审计日志
    if audit_logger is not None:
        audit_logger.log_prompt(task.id, prompt, source_type="orchestrator", model=task.model or claude_config.default_model)


    # Check budget before starting — 无 tracker 时记录警告但不阻塞
    if budget_tracker is None:
        logger.warning("run_claude_task 调用时未提供 budget_tracker，预算不受控 (task=%s)", task.id)

    # 构建模型降级候选列表
    fallback_models = _build_model_fallback_candidates(model)
    models_to_try = [model] + fallback_models

    # 通过执行租约保护整个任务执行过程
    with _execution_lease_session(task):
        for attempt_model in models_to_try:
            result = _execute_claude_subprocess(
                task, prompt, claude_config, limits, budget_tracker,
                working_dir, on_progress, audit_logger, rate_limiter,
                started_at, attempt_model,
            )
            # 模型降级判断：仅在失败且错误类型匹配时尝试降级
            if _should_use_model_fallback(result, attempt_model) and fallback_models:
                logger.warning(
                    "[%s] 模型 %s 失败且符合降级条件，尝试降级到 %s",
                    task.id, attempt_model, fallback_models,
                )
                continue
            return result

        # 所有模型都尝试完毕，返回最后一个结果
        return result


def _execute_claude_subprocess(
    task: TaskNode,
    prompt: str,
    claude_config: ClaudeConfig,
    limits: LimitsConfig,
    budget_tracker: BudgetTracker | None = None,
    working_dir: str | None = None,
    on_progress: Callable[[str, dict], None] | None = None,
    audit_logger: AuditLogger | None = None,
    rate_limiter: "RateLimiter | None" = None,
    started_at: datetime | None = None,
    model: str = "",
) -> TaskResult:
    """执行 Claude 子进程并收集结果。被 _run_claude_task_inner 调用，支持模型降级循环。"""
    if started_at is None:
        started_at = datetime.now()

    # Build command — use stream-json for real-time progress
    # Note: --verbose is required when using stream-json with --print
    cmd = [claude_config.cli_path, "-p", "--verbose", "--output-format", "stream-json"]

    cmd.extend(["--model", model])

    if task.max_budget_usd is not None:
        cmd.extend(["--max-budget-usd", str(task.max_budget_usd)])

    if task.max_turns is not None:
        cmd.extend(["--max-turns", str(task.max_turns)])
    else:
        # 未指定时按模型设默认值，防止 sonnet 等模型过度探索
        model_key = model.lower().split("-")[0] if model else "sonnet"
        default_turns = _DEFAULT_MAX_TURNS.get(model_key, 80)
        cmd.extend(["--max-turns", str(default_turns)])

    if task.allowed_tools:
        # --tools 控制可用工具集，--allowedTools 仅控制权限（会被 --dangerously-skip-permissions 绕过）
        cmd.extend(["--tools", ",".join(task.allowed_tools)])

    # 始终注入基础 system prompt（防止文件写入死循环），再拼接角色 system prompt
    effective_system_prompt = _BASE_SYSTEM_PROMPT
    if task.system_prompt:
        effective_system_prompt = f"{_BASE_SYSTEM_PROMPT}\n{task.system_prompt}"
    cmd.extend(["--system-prompt", effective_system_prompt])

    # Must add --dangerously-skip-permissions for unattended execution
    cmd.append("--dangerously-skip-permissions")

    # 构建子进程环境变量：隔离桌面版 Claude 环境变量防止嵌套会话冲突
    env = _build_subprocess_env()

    timeout = task.timeout or claude_config.default_timeout
    cwd = task.working_dir or working_dir

    # 防御性创建工作目录，兼容测试环境和首次运行时的空目录。
    if cwd and not os.path.isdir(cwd):
        try:
            os.makedirs(cwd, exist_ok=True)
        except OSError:
            logger.error(
                "Task '%s' aborted: working directory does not exist: %s",
                task.id, cwd,
            )
            if audit_logger is not None:
                audit_logger.log_result(task.id, success=False, error_category="execution_error")
            return _build_task_result(
                task.id,
                TaskStatus.FAILED,
                error=f"working directory does not exist: {cwd}",
                started_at=started_at,
                finished_at=datetime.now(),
                model_used=model,
            )

    logger.info("Running task '%s' with model=%s timeout=%ds (stream)", task.id, model, timeout)
    logger.debug("Command: %s", " ".join(cmd))

    try:
        # 不使用 shell=True（即使 Windows 下），避免 cmd.exe 中间层导致进程树不可控
        # 学习 Claude Code：直接启动可执行文件，通过 Job Object 管理生命周期
        creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            cwd=cwd,
            text=True,
            encoding="utf-8",
            errors="replace",
            shell=False,
            start_new_session=(os.name != "nt"),
            creationflags=creation_flags,
        )
    except FileNotFoundError:
        # 记录执行结果审计日志
        if audit_logger is not None:
            audit_logger.log_result(task.id, success=False, error_category="execution_error")

        return _build_task_result(
            task.id,
            TaskStatus.FAILED,
            error=f"Claude CLI not found at '{claude_config.cli_path}'",
            started_at=started_at,
            finished_at=datetime.now(),
            model_used=model,
        )
    except PermissionError as e:
        # CLI 二进制文件无执行权限
        logger.error("[%s] CLI 无执行权限: %s", task.id, e)
        if audit_logger is not None:
            audit_logger.log_result(task.id, success=False, error_category="execution_error")

        return _build_task_result(
            task.id,
            TaskStatus.FAILED,
            error=f"Permission denied for CLI at '{claude_config.cli_path}': {e}",
            started_at=started_at,
            finished_at=datetime.now(),
            model_used=model,
        )

    pid = proc.pid
    _register_proc(proc)

    # Windows Job Object 编组：确保编排器崩溃/退出时 OS 自动清理子进程
    get_global_job().assign_process(proc)
    # 同步注册到 ProcessCleaner，供编排器周期性清理使用
    try:
        from .process_cleaner import get_process_cleaner
        get_process_cleaner().register_pid(pid, task.id)
    except Exception:
        pass
    if pid is None:
        # 进程注册失败（pid 为 None），立即清理
        try:
            proc.kill()
            proc.wait(timeout=5)
        except Exception:
            pass
        # 记录执行结果审计日志
        if audit_logger is not None:
            audit_logger.log_result(task.id, success=False, error_category="execution_error")

        return _build_task_result(
            task.id,
            TaskStatus.FAILED,
            error="Failed to register process (pid is None)",
            started_at=started_at,
            finished_at=datetime.now(),
            model_used=model,
        )

    # --- Streaming read with progress parsing and heartbeat ---
    progress = StreamProgress(on_progress=on_progress)
    stderr_chunks: list[str] = []
    last_activity = time.monotonic()
    activity_lock = threading.Lock()

    def _update_activity():
        nonlocal last_activity
        with activity_lock:
            last_activity = time.monotonic()

    def _read_stdout():
        """Read stdout line by line, parse stream-json events."""
        assert proc.stdout is not None
        try:
            for line in proc.stdout:
                _update_activity()
                try:
                    _parse_stream_event(line, task.id, progress)
                except Exception as e:
                    # 优雅降级：解析失败时跳过该行继续处理，但记录警告便于诊断
                    logger.warning("[%s] Stream event parse error: %s | line: %s", task.id, e, line[:200])
                # max_turns 强制终止检测
                if (
                    task.max_turns is not None
                    and task.max_turns > 0
                    and progress.turn_started > task.max_turns
                    and not progress.max_turns_exceeded
                ):
                    progress.max_turns_exceeded = True
                    logger.error(
                        "[%s] 超出最大轮次限制: started=%d max_turns=%d，终止进程",
                        task.id, progress.turn_started, task.max_turns,
                    )
                    _kill_process_tree(proc, task.id)
                    break
                # 停滞检测：连续只读调用过多时干预
                if (
                    progress.consecutive_read_only_calls >= _STAGNATION_CRITICAL_THRESHOLD
                    and not progress.stagnation_killed
                    and not progress.max_turns_exceeded
                ):
                    progress.stagnation_killed = True
                    progress.max_turns_exceeded = True
                    logger.error(
                        "[%s] 停滞检测: 连续 %d 次只读调用无写入，强制终止进程（减少剩余轮次）",
                        task.id, progress.consecutive_read_only_calls,
                    )
                    _kill_process_tree(proc, task.id)
                    break
                elif (
                    progress.consecutive_read_only_calls >= _STAGNATION_WARNING_THRESHOLD
                    and not progress.stagnation_warning_sent
                ):
                    progress.stagnation_warning_sent = True
                    logger.warning(
                        "[%s] ⚠️ 停滞检测: 连续 %d 次只读调用无写入（Write/Edit），"
                        "请立即开始实施修改",
                        task.id, progress.consecutive_read_only_calls,
                    )
        except (ValueError, OSError, IOError) as e:
            # 断流检测：stdout 被强制关闭（进程被 kill）或发生 I/O 错误
            logger.warning("[%s] stdout 读取中断（可能因进程被强杀或断流）: %s", task.id, e)

    def _read_stderr():
        assert proc.stderr is not None
        for line in proc.stderr:
            stderr_chunks.append(line)
            # 限制缓冲区大小，最多保留最后 100 行
            if len(stderr_chunks) > 100:
                stderr_chunks.pop(0)
            _update_activity()
            stripped = line.strip()
            if stripped:
                logger.info("[%s|stderr] %s", task.id, stripped)

    def _heartbeat():
        consecutive_idle_count = 0
        # 动态阈值：至少允许 timeout 的 25% 时间无输出，下限 8 次（~960s）
        min_idle_time = max(timeout * 0.25, _HEARTBEAT_INTERVAL * 8)
        max_consecutive_idle = max(8, int(min_idle_time / _HEARTBEAT_INTERVAL))

        while proc.poll() is None:
            time.sleep(30)
            with activity_lock:
                idle = time.monotonic() - last_activity

            if idle >= _HEARTBEAT_INTERVAL and proc.poll() is None:
                consecutive_idle_count += 1
                elapsed = (datetime.now() - started_at).total_seconds()
                logger.warning(
                    "[%s] 无输出已 %ds (连续第 %d 次)，总耗时 %ds/%ds | 已调用 %d 次工具",
                    task.id, int(idle), consecutive_idle_count, int(elapsed), timeout, progress.tool_uses,
                )

                # 连续无输出达到阈值，强制杀死进程
                if consecutive_idle_count >= max_consecutive_idle:
                    logger.error(
                        "[%s] 连续 %d 次心跳无输出，强制杀死进程 (PID=%d)",
                        task.id, consecutive_idle_count, pid,
                    )
                    _kill_process_tree(proc, task.id)
                    break
            else:
                # 有活动，重置计数器
                if consecutive_idle_count > 0:
                    logger.info("[%s] 恢复输出，重置心跳计数器", task.id)
                    consecutive_idle_count = 0

    t_out = threading.Thread(target=_read_stdout, daemon=True, name=f"{task.id}-stdout")
    t_err = threading.Thread(target=_read_stderr, daemon=True, name=f"{task.id}-stderr")
    t_hb = threading.Thread(target=_heartbeat, daemon=True, name=f"{task.id}-heartbeat")

    # Send prompt via stdin, then close
    try:
        assert proc.stdin is not None
        # 预加载 Skills：在 prompt 前拼接 /skill_name 指令
        final_prompt = prompt
        if task.preload_skills:
            skill_commands = "\n".join(f"/{skill}" for skill in task.preload_skills)
            final_prompt = f"{skill_commands}\n\n{prompt}"
            logger.info("[%s] 预加载 Skills: %s", task.id, ", ".join(task.preload_skills))

        try:
            proc.stdin.write(final_prompt)
            proc.stdin.close()
        except (OSError, IOError) as e:
            # stdin 写入失败，进程可能已死亡或管道断开
            logger.error("[%s] stdin 写入失败，终止任务: %s", task.id, e)
            _kill_process_tree(proc, task.id)
            _unregister_proc(proc)
            # 记录执行结果审计日志
            if audit_logger is not None:
                audit_logger.log_result(task.id, success=False, error_category="execution_error")

            return _build_task_result(
                task.id,
                TaskStatus.FAILED,
                error=f"Failed to write to stdin: {e}",
                started_at=started_at,
                finished_at=datetime.now(),
                model_used=model,
                pid=pid,
                progress=progress,
            )
    except OSError as e:
        # 外层 except 保留以防万一
        logger.error("[%s] stdin 处理异常: %s", task.id, e)
        _kill_process_tree(proc, task.id)
        _unregister_proc(proc)
        # 记录执行结果审计日志
        if audit_logger is not None:
            audit_logger.log_result(task.id, success=False, error_category="execution_error")

        return _build_task_result(
            task.id,
            TaskStatus.FAILED,
            error=f"stdin handling error: {e}",
            started_at=started_at,
            finished_at=datetime.now(),
            model_used=model,
            pid=pid,
            progress=progress,
        )

    t_out.start()
    t_err.start()
    t_hb.start()

    # Wait for process with timeout
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        _kill_process_tree(proc, task.id)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            logger.warning("[%s] 进程杀死后仍未退出 (PID=%d)", task.id, pid)
        # 注销进程统一在 finally 块处理
        finished_at = datetime.now()
        # 记录执行结果审计日志
        if audit_logger is not None:
            audit_logger.log_result(task.id, success=False, error_category="execution_error")

        # 超时时也记录已产生的成本（progress 中可能已有部分流事件数据）
        # 注意：仅使用 record_usage，它内部已包含 _spent += cost_usd，不需要再调 check_and_add
        timeout_cost = 0.0
        if progress.result_event:
            logger.info(
                'Cost extraction input: event_keys=%s, has_usage=%s, has_modelUsage=%s',
                list(progress.result_event.keys()), 'usage' in progress.result_event, 'modelUsage' in progress.result_event,
            )
            timeout_cost = _extract_cost_usd(progress.result_event)
            logger.info('Cost extraction result: extracted_cost=%.6f', timeout_cost)
        if budget_tracker and timeout_cost > 0:
            budget_tracker.record_usage(
                model=progress.result_event.get("model", "") or model if progress.result_event else model,
                input_tokens=progress.token_input,
                output_tokens=progress.token_output,
                cache_read_tokens=progress.cache_read_tokens,
                cache_creation_tokens=progress.cache_creation_tokens,
                cost_usd=timeout_cost,
            )
            logger.info("[%s] 超时但仍记录已产生成本: $%.4f", task.id, timeout_cost)
            logger.info('BudgetTracker after record: total_spent=%.4f, model=%s', budget_tracker.spent, model)
        elif budget_tracker:
            logger.warning(
                "[%s] 超时且无法提取成本数据（result_event=%s, model=%s, tokens=%d/%d/%d/%d）",
                task.id, bool(progress.result_event), model,
                progress.token_input, progress.token_output,
                progress.cache_read_tokens, progress.cache_creation_tokens,
            )

        # 构建超时错误消息，包含流级别 JSON 解析失败信息（如果有）
        timeout_error = (
            f"Task timed out after {timeout}s "
            f"(工具调用 {progress.tool_uses} 次, 最后工具: {progress.last_tool})"
        )
        if progress.json_parse_errors > 0:
            parse_info = f" [stream_json_parse_errors={progress.json_parse_errors}"
            if progress.json_parse_error_samples:
                samples = "; ".join(progress.json_parse_error_samples[:2])
                parse_info += f", samples=[{samples}]"
            parse_info += "]"
            timeout_error += parse_info

        return _build_task_result(
            task.id,
            TaskStatus.FAILED,
            error=timeout_error,
            cost_usd=timeout_cost,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=(finished_at - started_at).total_seconds(),
            model_used=model,
            pid=pid,
            progress=progress,
        )
    finally:
        # 统一注销进程，无论正常退出还是超时
        _unregister_proc(proc)

    # Let reader threads finish
    t_out.join(timeout=5)
    t_err.join(timeout=5)

    finished_at = datetime.now()
    duration = (finished_at - started_at).total_seconds()

    stderr = "".join(stderr_chunks)
    # 记录完整 stderr 内容到日志（debug 级别）
    if stderr.strip():
        logger.debug("[%s] Complete stderr output:\n%s", task.id, stderr)

    # Build response from stream events
    resp = _build_response_from_stream(progress)

    # 成本校验：usage 存在但 cost=0 时记录 WARNING，附带原始 usage 数据便于排查
    _has_usage = (
        resp.token_input > 0 or resp.token_output > 0
        or progress.cache_read_tokens > 0 or progress.cache_creation_tokens > 0
    )
    if _has_usage and resp.cost_usd == 0.0:
        # 收集原始 usage 数据用于诊断
        raw_usage = {}
        if progress.result_event:
            raw_usage["result_event.usage"] = progress.result_event.get("usage")
            raw_usage["result_event.modelUsage"] = progress.result_event.get("modelUsage")
        raw_usage["progress.tokens"] = {
            "input": progress.token_input,
            "output": progress.token_output,
            "cache_read": progress.cache_read_tokens,
            "cache_creation": progress.cache_creation_tokens,
        }
        raw_usage["resp.model"] = resp.model
        raw_usage["resp.cost_usd"] = resp.cost_usd
        raw_usage["resp.token_input"] = resp.token_input
        raw_usage["resp.token_output"] = resp.token_output
        logger.warning(
            "[%s] cost=0 但存在 token 使用数据: %s",
            task.id, json.dumps(raw_usage, ensure_ascii=False, default=str),
        )
        # 严格模式：环境变量启用时，cost=0 但有 usage 视为异常
        if os.environ.get('CLAUDE_ORCHESTRATOR_STRICT_COST', '').lower() in ('1', 'true'):
            raise CostAssertionError(
                f'[{task.id}] cost=0 但存在 token 使用数据 (model={resp.model}, '
                f'tokens={resp.token_input}/{resp.token_output})'
            )

    # 停滞检测：因连续只读调用过多被强制终止，返回明确的错误信息
    if progress.stagnation_killed:
        stagnation_error = (
            f"停滞终止: 连续 {progress.consecutive_read_only_calls} 次只读调用无写入（Write/Edit），"
            f"已强制减少剩余轮次 (工具调用: {progress.tool_uses}, 最后工具: {progress.last_tool})"
        )
        # 追加流级别 JSON 解析错误信息（如果有），防止诊断信息丢失
        if progress.json_parse_errors > 0:
            parse_info = f" [stream_json_parse_errors={progress.json_parse_errors}"
            if progress.json_parse_error_samples:
                samples = "; ".join(progress.json_parse_error_samples[:2])
                parse_info += f", samples=[{samples}]"
            parse_info += "]"
            stagnation_error += parse_info
        logger.error("[%s] %s", task.id, stagnation_error)
        # 记录已产生的成本
        if budget_tracker and resp.cost_usd > 0:
            logger.info(
                'Cost extraction input (stagnation): resp.cost_usd=%.6f, resp.model=%s',
                resp.cost_usd, resp.model,
            )
            budget_tracker.record_usage(
                model=resp.model or model,
                input_tokens=resp.token_input,
                output_tokens=resp.token_output,
                cache_read_tokens=progress.cache_read_tokens,
                cache_creation_tokens=progress.cache_creation_tokens,
                cost_usd=resp.cost_usd,
            )
            logger.info('BudgetTracker after record: total_spent=%.4f, model=%s', budget_tracker.spent, resp.model or model)
        elif budget_tracker:
            logger.warning(
                "[%s] 停滞终止且无法提取成本数据（cost_usd=%.6f, model=%s, tokens=%d/%d）",
                task.id, resp.cost_usd, resp.model or model,
                resp.token_input, resp.token_output,
            )
        if audit_logger is not None:
            audit_logger.log_result(task.id, success=False, error_category="stagnation")
        return _build_task_result(
            task.id,
            TaskStatus.FAILED,
            error=stagnation_error,
            output=resp.result,
            cost_usd=resp.cost_usd,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=duration,
            model_used=resp.model or model,
            pid=pid,
            progress=progress,
            response=resp,
        )

    # 检测空响应：CLI 正常退出但无有效输出（JSON 解析全失败或流为空）
    if not resp.result.strip() and resp.is_error:
        # _build_response_from_stream 已标记 is_error=True，走下方错误路径
        logger.error(
            "[%s] 空响应（is_error=True）：json_parse_errors=%d, events=%d, returncode=%d",
            task.id, progress.json_parse_errors, len(progress.all_events), proc.returncode,
        )

    # 额外防护：result 为空但 is_error 仍为 False 的异常情况
    if not resp.result.strip() and not resp.is_error and proc.returncode == 0:
        logger.warning(
            "[%s] CLI 正常退出但输出为空（json_parse_errors=%d, events=%d, tool_uses=%d），标记为 FAILED",
            task.id, progress.json_parse_errors, len(progress.all_events), progress.tool_uses,
        )
        # 空响应也可能产生了成本，记录到 BudgetTracker
        if budget_tracker and resp.cost_usd > 0:
            budget_tracker.record_usage(
                model=resp.model or model,
                input_tokens=resp.token_input,
                output_tokens=resp.token_output,
                cache_read_tokens=progress.cache_read_tokens,
                cache_creation_tokens=progress.cache_creation_tokens,
                cost_usd=resp.cost_usd,
            )
            logger.info("[%s] 空响应但记录已产生成本: $%.4f", task.id, resp.cost_usd)
        elif budget_tracker:
            logger.warning(
                "[%s] 空响应且无 cost 数据（model=%s, tokens=%d/%d），无法记录 usage",
                task.id, resp.model or model, resp.token_input, resp.token_output,
            )
        # 记录执行结果审计日志
        if audit_logger is not None:
            audit_logger.log_result(task.id, success=False, error_category="empty_response")
        return _build_task_result(
            task.id,
            TaskStatus.FAILED,
            error=f"CLI 正常退出但输出为空（json_parse_errors={progress.json_parse_errors}, "
                  f"stream_events={len(progress.all_events)}, tool_uses={progress.tool_uses}）",
            cost_usd=resp.cost_usd,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=duration,
            model_used=resp.model or model,
            pid=pid,
            progress=progress,
            response=resp,
        )

    # Check output size
    if len(resp.result.encode()) > limits.max_output_size_bytes:
        # 输出超限但 CLI 已完整执行，需记录已产生的成本到 BudgetTracker
        # 注意：仅使用 record_usage，它内部已包含 _spent += cost_usd，不需要再调 check_and_add
        if budget_tracker and resp.cost_usd > 0:
            budget_tracker.record_usage(
                model=resp.model or model,
                input_tokens=resp.token_input,
                output_tokens=resp.token_output,
                cache_read_tokens=progress.cache_read_tokens,
                cache_creation_tokens=progress.cache_creation_tokens,
                cost_usd=resp.cost_usd,
            )
            logger.info("[%s] 输出超限但仍记录已产生成本: $%.4f", task.id, resp.cost_usd)
            logger.info('BudgetTracker after record: total_spent=%.4f, model=%s', budget_tracker.spent, model)
        elif budget_tracker:
            logger.warning(
                "[%s] 输出超限且无法提取成本数据（cost_usd=%.6f, model=%s, tokens=%d/%d）",
                task.id, resp.cost_usd, resp.model or model,
                resp.token_input, resp.token_output,
            )

        # 记录执行结果审计日志
        if audit_logger is not None:
            audit_logger.log_result(task.id, success=False, error_category="execution_error")

        # 输出超限错误消息，包含流级别 JSON 解析失败信息（如果有）
        size_error = f"Output exceeds max size ({limits.max_output_size_bytes} bytes)"
        if progress.json_parse_errors > 0:
            parse_info = f" [stream_json_parse_errors={progress.json_parse_errors}"
            if progress.json_parse_error_samples:
                samples = "; ".join(progress.json_parse_error_samples[:2])
                parse_info += f", samples=[{samples}]"
            parse_info += "]"
            size_error += parse_info

        return _build_task_result(
            task.id,
            TaskStatus.FAILED,
            error=size_error,
            cost_usd=resp.cost_usd,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=duration,
            model_used=model,
            pid=pid,
            progress=progress,
            response=resp,
        )

    # 错误检查必须在成本记录之前，避免成功路径和错误路径双重调用 record_usage
    if proc.returncode != 0 or resp.is_error:
        error_msg = resp.result if resp.is_error else (stderr.strip() or f"Exit code {proc.returncode}")

        # 追加流级别 JSON 解析错误信息（如果有），帮助诊断 CLI 输出异常
        if progress.json_parse_errors > 0:
            parse_info = f" [stream_json_parse_errors={progress.json_parse_errors}"
            if progress.json_parse_error_samples:
                samples = "; ".join(progress.json_parse_error_samples[:2])
                parse_info += f", samples=[{samples}]"
            parse_info += "]"
            error_msg += parse_info

        # 429 限流检测：使用统一错误分类器
        combined_text = f"{error_msg} {stderr}"
        if looks_like_rate_limit_error(combined_text):
            if rate_limiter is not None:
                rate_limiter.report_429()
                logger.warning("[%s] 检测到 429 限流，已触发速率限制器退避", task.id)

        if looks_like_auth_error(combined_text):
            error_msg = f"AUTH_EXPIRED: {error_msg}"
            logger.warning("[%s] 检测到认证过期: %s", task.id, error_msg[:200])

        # 错误路径也需要记录成本——失败的任务同样消耗 token
        error_model = resp.model or model
        if budget_tracker:
            if resp.cost_usd > 0:
                budget_tracker.record_usage(
                    model=error_model,
                    input_tokens=resp.token_input,
                    output_tokens=resp.token_output,
                    cache_read_tokens=progress.cache_read_tokens,
                    cache_creation_tokens=progress.cache_creation_tokens,
                    cost_usd=resp.cost_usd,
                )
                logger.info("[%s] 错误路径记录成本: $%.4f (model=%s)", task.id, resp.cost_usd, error_model)
            elif resp.token_input or resp.token_output or progress.cache_read_tokens or progress.cache_creation_tokens:
                # cost_usd==0 但有 token 消耗（含 cache token），估算成本
                estimated = _estimate_cost_from_tokens({
                    "input_tokens": resp.token_input,
                    "output_tokens": resp.token_output,
                    "cache_read_input_tokens": progress.cache_read_tokens,
                    "cache_creation_input_tokens": progress.cache_creation_tokens,
                    "model": error_model,
                })
                if estimated > 0:
                    budget_tracker.record_usage(
                        model=error_model,
                        input_tokens=resp.token_input,
                        output_tokens=resp.token_output,
                        cache_read_tokens=progress.cache_read_tokens,
                        cache_creation_tokens=progress.cache_creation_tokens,
                        cost_usd=estimated,
                    )
                    logger.info("[%s] 错误路径 token 估算成本: $%.6f (model=%s)", task.id, estimated, error_model)
                else:
                    logger.warning(
                        "[%s] 错误路径: cost_usd=0，有 token 但无法估算成本"
                        "（model=%s, tokens=%d/%d）",
                        task.id, error_model, resp.token_input, resp.token_output,
                    )
            else:
                logger.warning(
                    "[%s] 错误路径: cost_usd=0 且无 token 使用数据（model=%s）",
                    task.id, error_model,
                )

        # 记录执行结果审计日志
        if audit_logger is not None:
            audit_logger.log_result(task.id, success=False, error_category="execution_error")

        return _build_task_result(
            task.id,
            TaskStatus.FAILED,
            error=error_msg,
            output=resp.result,
            cost_usd=resp.cost_usd,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=duration,
            model_used=resp.model or model,
            pid=pid,
            progress=progress,
            response=resp,
        )

    # Track cost — 成功路径（错误路径在上方 return 前已单独记录，此处避免双重计数）
    # 注意：仅使用 record_usage，它内部已包含 _spent += cost_usd，不需要再调 check_and_add
    effective_model = resp.model or model
    if budget_tracker:
        if resp.cost_usd > 0:
            # CLI 返回了有效成本，直接记录
            logger.info(
                'Cost extraction input (success): resp.cost_usd=%.6f, resp.model=%s',
                resp.cost_usd, resp.model,
            )
            budget_tracker.record_usage(
                model=effective_model,
                input_tokens=resp.token_input,
                output_tokens=resp.token_output,
                cache_read_tokens=progress.cache_read_tokens,
                cache_creation_tokens=progress.cache_creation_tokens,
                cost_usd=resp.cost_usd,
            )
            logger.info('BudgetTracker after record: total_spent=%.4f, model=%s', budget_tracker.spent, effective_model)
        elif resp.token_input or resp.token_output or progress.cache_read_tokens or progress.cache_creation_tokens:
            # cost_usd==0 但有 token 消耗：从 token 估算成本并记录
            estimated = _estimate_cost_from_tokens({
                "input_tokens": resp.token_input,
                "output_tokens": resp.token_output,
                "cache_read_input_tokens": progress.cache_read_tokens,
                "cache_creation_input_tokens": progress.cache_creation_tokens,
                "model": effective_model,
            })
            if estimated > 0:
                logger.info(
                    "[%s] CLI 未返回 cost_usd，从 token 估算成本: $%.6f (model=%s, tokens=%d/%d)",
                    task.id, estimated, effective_model, resp.token_input, resp.token_output,
                )
                budget_tracker.record_usage(
                    model=effective_model,
                    input_tokens=resp.token_input,
                    output_tokens=resp.token_output,
                    cache_read_tokens=progress.cache_read_tokens,
                    cache_creation_tokens=progress.cache_creation_tokens,
                    cost_usd=estimated,
                )
                logger.info('BudgetTracker after record: total_spent=%.4f, model=%s', budget_tracker.spent, effective_model)
            else:
                logger.warning(
                    "[%s] CLI 返回 cost_usd=0，有 token 消耗但无法估算成本"
                    "（model=%s, tokens=%d/%d, cache_read=%d, cache_creation=%d）",
                    task.id, effective_model, resp.token_input, resp.token_output,
                    progress.cache_read_tokens, progress.cache_creation_tokens,
                )
        else:
            # 无成本、无 token：可能是空响应或 CLI 未输出 usage
            logger.warning(
                "[%s] CLI 返回 cost_usd=0 且无 token 使用数据"
                "（model=%s），无法记录 usage",
                task.id, effective_model,
            )

    # Parse JSON output if requested
    parsed_output = None
    json_format_error = None
    if task.output_format == "json":
        try:
            parsed_output = json.loads(resp.result)
        except json.JSONDecodeError as e:
            # 期望 JSON 输出但解析失败：记录到 error 字段，保留原始文本作为 fallback
            json_format_error = f"JSON 输出格式解析失败: {e}"
            logger.warning(
                "[%s] %s | 输出前200字符: %s",
                task.id, json_format_error, resp.result[:200],
            )
            parsed_output = resp.result

    # 汇总流级别 JSON 解析错误到 TaskResult.error（非致命，任务仍为 SUCCESS）
    if progress.json_parse_errors > 0:
        stream_parse_warning = (
            f"[STREAM_PARSE_WARNING] 流事件 JSON 解析失败 {progress.json_parse_errors} 次"
        )
        if progress.json_parse_error_samples:
            samples = "; ".join(progress.json_parse_error_samples)
            stream_parse_warning += f" | 示例: [{samples}]"
        logger.warning("[%s] %s", task.id, stream_parse_warning)
        # 拼接到已有的 json_format_error 或创建新的错误信息
        if json_format_error:
            json_format_error = f"{json_format_error}; {stream_parse_warning}"
        else:
            json_format_error = stream_parse_warning



    # 记录执行结果审计日志
    if audit_logger is not None:
        audit_logger.log_result(task.id, success=True)

    # 报告速率限制器任务成功
    if rate_limiter is not None:
        rate_limiter.report_success()

    return _build_task_result(
        task.id,
        TaskStatus.SUCCESS,
        output=resp.result,
        error=json_format_error,
        parsed_output=parsed_output if parsed_output is not None else resp.result,
        cost_usd=resp.cost_usd,
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration,
        model_used=resp.model or model,
        pid=pid,
        progress=progress,
        response=resp,
    )
