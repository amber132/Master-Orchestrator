"""Codex CLI subprocess wrapper with streaming progress."""

from __future__ import annotations

import atexit
import json
import logging
import os
import platform
import re
import signal
import shutil
import subprocess
import threading
import time
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from .error_classifier import looks_like_auth_error, looks_like_network_error, looks_like_rate_limit_error
from .config import CodexConfig, LimitsConfig
from .audit_log import AuditLogger
from .execution_lease import ExecutionLease, ExecutionLeaseManager
from .exceptions import BudgetExhaustedError
from .log_context import set_task_id
from .metrics import MetricsCollector, TaskMetrics
from .model import TaskNode, TaskResult, TaskStatus

logger = logging.getLogger(__name__)

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
_HOST_PATH_CLS = type(Path(__file__))


@dataclass
class _ExecutionLeaseBinding:
    manager: ExecutionLeaseManager
    run_id: str
    wait_poll_interval_seconds: float = 0.2
    log_interval_seconds: float = 5.0


_execution_lease_stack: list[_ExecutionLeaseBinding] = []
_execution_lease_stack_lock = threading.Lock()
_STRIP_DESKTOP_CODEX_ENV_KEYS = frozenset({
    "CODEX_THREAD_ID",
    "CODEX_INTERNAL_ORIGINATOR_OVERRIDE",
    "CODEX_SHELL",
})


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
    # 顺便清理其他已完成的进程引用
    _gc_finished_procs()


def _cleanup_all_procs() -> None:
    """atexit 钩子：强杀所有残留子进程，防止僵尸进程。"""
    with _active_procs_lock:
        remaining = list(_active_procs.values())
    for proc in remaining:
        if proc.poll() is None:
            logger.warning("atexit: 清理残留子进程 PID=%d", proc.pid)
            _kill_process_tree(proc, task_id="atexit-cleanup")


def _gc_finished_procs() -> None:
    """清理已完成但未注销的进程引用，防止内存泄漏。"""
    with _active_procs_lock:
        finished = [pid for pid, proc in _active_procs.items() if proc.poll() is not None]
        for pid in finished:
            del _active_procs[pid]
        if finished:
            logger.debug("GC 清理了 %d 个已完成的进程引用", len(finished))


atexit.register(_cleanup_all_procs)


def _current_execution_lease_binding() -> _ExecutionLeaseBinding | None:
    with _execution_lease_stack_lock:
        if not _execution_lease_stack:
            return None
        return _execution_lease_stack[-1]


def _build_subprocess_env(overrides: dict[str, str] | None = None) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items()}
    isolate_codex_home = bool(overrides and overrides.get("CODEX_HOME"))
    override_keys = set(overrides or {})
    for key in list(env):
        if key in _STRIP_DESKTOP_CODEX_ENV_KEYS:
            env.pop(key, None)
            continue
        # When a task already has its own CODEX_HOME, inheriting any other CODEX_*
        # key leaks desktop-session state back into the supposedly isolated worker.
        if isolate_codex_home and key.startswith("CODEX_") and key not in override_keys:
            env.pop(key, None)
    if overrides:
        env.update(overrides)
    return env


def _resolve_windows_native_codex_cli_path(cli_path: str) -> str:
    if os.name != "nt":
        return cli_path

    resolved = shutil.which(cli_path) or cli_path
    candidate = _HOST_PATH_CLS(resolved).expanduser()
    if candidate.suffix.lower() == ".exe" and candidate.exists():
        return str(candidate)
    if candidate.suffix.lower() not in {".cmd", ".bat"} or not candidate.exists():
        return cli_path

    try:
        wrapper_text = candidate.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return str(candidate)

    package_root: Path | None = None
    match = re.search(r'CODEX_JS=([^"\r\n]+)', wrapper_text)
    if match:
        raw_js_path = match.group(1).strip()
        js_path = _HOST_PATH_CLS(raw_js_path).expanduser()
        if not js_path.is_absolute() and raw_js_path.startswith("%dp0%"):
            js_path = candidate.parent / raw_js_path.replace("%dp0%\\", "").replace("%dp0%/", "")
        package_root = js_path.parent.parent
    elif re.search(r'node_modules[\\/]+@openai[\\/]+codex[\\/]+bin[\\/]+codex\.js', wrapper_text, re.IGNORECASE):
        package_root = candidate.parent / "node_modules" / "@openai" / "codex"
    if package_root is None:
        return str(candidate)

    machine = platform.machine().lower()
    if "arm" in machine:
        package_dir = ("@openai", "codex-win32-arm64")
        target_triple = "aarch64-pc-windows-msvc"
    else:
        package_dir = ("@openai", "codex-win32-x64")
        target_triple = "x86_64-pc-windows-msvc"

    candidates = (
        package_root / "node_modules" / package_dir[0] / package_dir[1] / "vendor" / target_triple / "codex" / "codex.exe",
        package_root / "vendor" / target_triple / "codex" / "codex.exe",
    )
    for native_path in candidates:
        if native_path.exists():
            return str(native_path.resolve())
    return str(candidate)


@contextmanager
def bind_execution_lease_scope(
    manager: ExecutionLeaseManager | None,
    *,
    run_id: str,
    wait_poll_interval_seconds: float = 0.2,
    log_interval_seconds: float = 5.0,
):
    if manager is None or not manager.enabled:
        yield None
        return

    binding = _ExecutionLeaseBinding(
        manager=manager,
        run_id=run_id,
        wait_poll_interval_seconds=max(0.05, wait_poll_interval_seconds),
        log_interval_seconds=max(1.0, log_interval_seconds),
    )
    with _execution_lease_stack_lock:
        _execution_lease_stack.append(binding)
    try:
        yield binding
    finally:
        with _execution_lease_stack_lock:
            if _execution_lease_stack and _execution_lease_stack[-1] is binding:
                _execution_lease_stack.pop()
            elif binding in _execution_lease_stack:
                _execution_lease_stack.remove(binding)

# Heartbeat: warn if no output for this many seconds
_HEARTBEAT_INTERVAL = 120

# 按模型设置合理的 max_turns 默认值，防止过度探索
# opus 更聪明，允许更多轮次；sonnet 容易陷入探索循环，需要限制
_DEFAULT_MAX_TURNS = {
    "o3": 200,
    "gpt-5-codex": 50,
    "gpt-4.1-mini": 20,
}

_CODEX_MODEL_FALLBACKS: dict[str, tuple[str, ...]] = {
    "gpt-5.5": ("gpt-5.4", "gpt-5.3-codex", "gpt-5-codex"),
    "gpt-5.4": ("gpt-5.3-codex", "gpt-5-codex"),
    "gpt-5.4-mini": ("gpt-5.3-codex-spark", "gpt-5-codex-mini"),
}

# 网络健康探针配置
_NETWORK_PROBE_TIMEOUT = 5  # 单次探测超时秒数
_NETWORK_PROBE_MAX_WAIT = 60  # 最长等待网络恢复秒数（1 分钟，避免长时间阻塞重试）
_NETWORK_PROBE_INTERVAL = 30  # 探测间隔秒数（默认值，实际由分级策略覆盖）
_NETWORK_PROBE_ENDPOINTS = [
    "https://api.openai.com",
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
    """Kill a process and all its children (entire process tree).

    On Windows, uses 'taskkill /T /F' to kill the tree.
    On Unix, uses os.killpg to kill the process group.
    Falls back to proc.kill() if tree-kill fails.
    """
    pid = proc.pid
    try:
        if os.name == "nt":
            # Windows: taskkill /T kills the process tree, /F forces it
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(pid)],
                capture_output=True, timeout=10,
            )
            logger.info("[%s] 已杀死进程树 (taskkill /T /PID %d)", task_id, pid)
        else:
            # Unix: kill the entire process group
            try:
                pgid = os.getpgid(pid)
                os.killpg(pgid, signal.SIGKILL)
                logger.info("[%s] 已杀死进程组 (PGID=%d)", task_id, pgid)
            except ProcessLookupError:
                pass  # already dead
    except Exception as e:
        logger.warning("[%s] 进程树杀死失败，回退到 proc.kill(): %s", task_id, e)
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
    cli_duration_ms: float = 0.0
    turn_started: int = 0
    turn_completed: int = 0
    max_turns_exceeded: bool = False

    def append_event(self, event: dict) -> None:
        """追加事件，超过上限时丢弃最早的事件。"""
        self.all_events.append(event)
        if len(self.all_events) > self._max_events:
            # 保留最后 _max_events 个事件
            self.all_events = self.all_events[-self._max_events:]


class BudgetTracker:
    """Thread-safe cost tracker.

    仅记录花费，不再执行任何预算拦截。
    """

    def __init__(self, max_budget_usd: float, enforcement_mode: str = "accounting"):
        self._max = max(0.0, max_budget_usd)
        self._spent = 0.0
        self._lock = threading.Lock()
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

    def check_and_add(self, cost: float) -> None:
        with self._lock:
            normalized_cost = max(0.0, cost)
            self._ensure_can_charge_unlocked(normalized_cost)
            self._spent += normalized_cost

    def can_afford(self, estimated_cost: float = 0.0) -> bool:
        if self._enforcement_mode != "hard_limit" or self._max <= 0:
            return True
        with self._lock:
            return self._spent + max(0.0, estimated_cost) <= self._max

    def add_spent(self, cost: float) -> None:
        """原子地增加已花费金额。"""
        with self._lock:
            self._spent += cost

    def add_cost(self, amount: float) -> None:
        """添加成本到已花费金额（用于断点续传恢复花费统计）。"""
        with self._lock:
            self._spent += amount

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


def _parse_cli_output(raw: str) -> CLIResponse:
    """Parse the JSON envelope from codex exec --output-format json."""
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return CLIResponse(raw=raw, result=raw.strip(), is_error=False, cost_usd=0.0, model="")

    if isinstance(data, dict) and data.get("type") == "result":
        return CLIResponse(
            raw=raw,
            result=data.get("result", ""),
            is_error=data.get("is_error", False),
            cost_usd=data.get("cost_usd", 0.0),
            model=data.get("model", ""),
        )

    return CLIResponse(raw=raw, result=raw.strip(), is_error=False, cost_usd=0.0, model="")


def _parse_stream_event(line: str, task_id: str, progress: StreamProgress) -> None:
    """Parse a single JSONL event line from codex exec --json and log progress.

    Codex CLI (v0.1+) emits these event types:
      - thread.started          — session begins
      - turn.started            — model turn begins
      - item.started            — item streaming begins (command_execution)
      - item.completed          — item finished:
            item.type = "agent_message"       → assistant text
            item.type = "reasoning"           → chain-of-thought
            item.type = "command_execution"   → tool/shell call + output
      - turn.completed          — turn ends, carries usage{input_tokens, output_tokens}
    """
    line = line.strip()
    if not line:
        return

    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        return

    progress.append_event(event)
    event_type = event.get("type", "")

    # ── item.completed: the main payload carrier ──
    if event_type == "item.completed":
        item = event.get("item", {})
        item_type = item.get("type", "")

        if item_type == "agent_message":
            text = item.get("text", "")
            if text.strip():
                progress.text_chunks += 1
                progress.last_text_preview = text[:100]
                if len(text.strip()) > 20:
                    logger.info(
                        "[%s] 📝 文本输出 #%d: %s",
                        task_id, progress.text_chunks, text[:150].replace("\n", " "),
                    )

        elif item_type == "reasoning":
            text = item.get("text", "")
            if text.strip():
                logger.debug("[%s] 💭 推理: %s", task_id, text[:150].replace("\n", " "))

        elif item_type == "command_execution":
            cmd = item.get("command", "")
            status = item.get("status", "")
            exit_code = item.get("exit_code")
            output = item.get("aggregated_output", "")

            progress.tool_uses += 1
            progress.last_tool = "shell"

            if status == "completed":
                ok = exit_code == 0
                icon = "✅ 完成" if ok else "❌ 失败"
                output_preview = output[:100].replace("\n", " ") if output else ""
                logger.info(
                    "[%s] %s shell (exit=%s) | cmd: %s | %s",
                    task_id, icon, exit_code, cmd[:80], output_preview,
                )
            else:
                logger.info(
                    "[%s] 🔧 命令执行 #%d: %s",
                    task_id, progress.tool_uses, cmd[:120],
                )

    # ── item.started: command begins streaming ──
    elif event_type == "item.started":
        item = event.get("item", {})
        if item.get("type") == "command_execution":
            cmd = item.get("command", "")
            logger.info("[%s] 🔧 命令开始: %s", task_id, cmd[:120])

    # ── turn.completed: carries usage info ──
    elif event_type == "turn.completed":
        progress.result_event = event
        progress.turn_completed += 1
        usage = event.get("usage", {})
        progress.token_input = usage.get("input_tokens", 0)
        progress.token_output = usage.get("output_tokens", 0)
        logger.info(
            "[%s] 🏁 轮次结束 | 工具调用: %d 次 | tokens: %d in / %d out",
            task_id, progress.tool_uses, progress.token_input, progress.token_output,
        )

    # ── thread.started / turn.started ──
    elif event_type == "thread.started":
        thread_id = event.get("thread_id", "")
        logger.info("[%s] ⚙️ 线程启动: %s", task_id, thread_id)

    elif event_type == "turn.started":
        progress.turn_started += 1
        logger.debug("[%s] ⚙️ 轮次开始", task_id)

    if progress.on_progress is not None:
        progress.on_progress(event_type, event)


def _build_response_from_stream(progress: StreamProgress) -> CLIResponse:
    """Build a CLIResponse from collected stream events.

    Codex CLI format: assemble text from item.completed events where
    item.type == "agent_message". The turn.completed event carries usage.
    """
    text_parts: list[str] = []
    has_error = False

    for evt in progress.all_events:
        evt_type = evt.get("type", "")

        if evt_type == "item.completed":
            item = evt.get("item", {})
            if item.get("type") == "agent_message":
                text = item.get("text", "")
                if text:
                    text_parts.append(text)
            elif item.get("type") == "command_execution":
                exit_code = item.get("exit_code")
                if exit_code is not None and exit_code != 0:
                    has_error = True

    combined = "\n".join(text_parts) if text_parts else ""

    return CLIResponse(
        raw=combined,
        result=combined,
        is_error=has_error and not combined,
        cost_usd=0.0,
        model="",
        token_input=progress.token_input,
        token_output=progress.token_output,
        cli_duration_ms=progress.cli_duration_ms,
    )


@contextmanager
def _execution_lease_session(task: TaskNode):
    binding = _current_execution_lease_binding()
    if binding is None or not binding.manager.enabled:
        yield None
        return

    waited_started_at = time.monotonic()
    lease: ExecutionLease | None = None
    while lease is None:
        lease = binding.manager.acquire(
            binding.run_id,
            task.id,
            timeout_seconds=binding.log_interval_seconds,
            poll_interval=binding.wait_poll_interval_seconds,
        )
        if lease is None:
            waited_seconds = time.monotonic() - waited_started_at
            logger.info(
                "[%s] 等待真实执行槽位 %.1fs (active=%d/%d, run=%s)",
                task.id,
                waited_seconds,
                binding.manager.active_count(),
                binding.manager.max_leases,
                binding.run_id,
            )

    if (waited_seconds := (time.monotonic() - waited_started_at)) >= 0.5:
        logger.info(
            "[%s] 已获取真实执行槽位 (waited=%.1fs, active=%d/%d, run=%s)",
            task.id,
            waited_seconds,
            binding.manager.active_count(),
            binding.manager.max_leases,
            binding.run_id,
        )

    stop_event = threading.Event()
    lease_lock = threading.Lock()

    def _renew_loop() -> None:
        renew_every = max(5.0, binding.manager.ttl_seconds / 3)
        while not stop_event.wait(renew_every):
            with lease_lock:
                current = nonlocal_lease[0]
            renewed = binding.manager.renew(current)
            if renewed is None:
                logger.warning("[%s] 执行租约续期失败，可能已丢失执行槽位", task.id)
                return
            with lease_lock:
                nonlocal_lease[0] = renewed

    nonlocal_lease: list[ExecutionLease | None] = [lease]

    def _lease_ref() -> ExecutionLease | None:
        with lease_lock:
            return nonlocal_lease[0]

    renew_thread = threading.Thread(
        target=_renew_loop,
        name=f"{task.id}-lease-renew",
        daemon=True,
    )
    renew_thread.start()
    try:
        yield lease
    finally:
        stop_event.set()
        renew_thread.join(timeout=1)
        binding.manager.release(_lease_ref())


def run_codex_task(
    task: TaskNode,
    prompt: str,
    codex_config: CodexConfig,
    limits: LimitsConfig,
    budget_tracker: BudgetTracker | None = None,
    working_dir: str | None = None,
    on_progress: Callable[[str, dict], None] | None = None,
    audit_logger: AuditLogger | None = None,
    rate_limiter: "RateLimiter | None" = None,
) -> TaskResult:
    """Execute a single task via codex exec --output-format stream-json."""
    set_task_id(task.id)
    try:
        return _run_codex_task_inner(
            task, prompt, codex_config, limits, budget_tracker, working_dir, on_progress,
            audit_logger, rate_limiter,
        )
    finally:
        set_task_id("")


def _run_codex_task_inner(
    task: TaskNode,
    prompt: str,
    codex_config: CodexConfig,
    limits: LimitsConfig,
    budget_tracker: BudgetTracker | None = None,
    working_dir: str | None = None,
    on_progress: Callable[[str, dict], None] | None = None,
    audit_logger: AuditLogger | None = None,
    rate_limiter: "RateLimiter | None" = None,
) -> TaskResult:
    """Execute a single task via codex exec --output-format stream-json."""
    started_at = datetime.now()

    # Rate limiting: acquire token before task execution
    model = task.model or codex_config.default_model
    if rate_limiter is not None:
        logger.info("[%s] Waiting for rate limit token (model=%s)...", task.id, model)
        rate_limiter.acquire(model)
        logger.info("[%s] Rate limit token acquired", task.id)


    # 记录 prompt 审计日志
    if audit_logger is not None:
        audit_logger.log_prompt(task.id, prompt, source_type="orchestrator", model=task.model or codex_config.default_model)


    # Cost tracking setup — 无 tracker 时仅跳过花费统计，不阻塞执行
    if budget_tracker is None:
        logger.warning("run_codex_task 调用时未提供 budget_tracker，花费将不会计入统计 (task=%s)", task.id)

    cli_path = _resolve_windows_native_codex_cli_path(codex_config.cli_path)
    model = task.model or codex_config.default_model
    cwd = task.working_dir or working_dir

    # Prepare environment
    env = _build_subprocess_env(task.env_overrides)

    timeout = task.timeout or codex_config.default_timeout
    model_attempts = [model, *_build_model_fallback_candidates(model)]

    with _execution_lease_session(task):
        last_result: TaskResult | None = None
        for index, attempt_model in enumerate(model_attempts):
            cmd = _build_codex_exec_command(
                task=task,
                cli_path=cli_path,
                model=attempt_model,
                cwd=cwd,
                execution_security_mode=codex_config.execution_security_mode,
            )
            logger.info("Running task '%s' with model=%s timeout=%ds (stream)", task.id, attempt_model, timeout)
            logger.debug("Command: %s", " ".join(cmd))

            result = _run_codex_subprocess(
                task=task,
                prompt=prompt,
                limits=limits,
                budget_tracker=budget_tracker,
                started_at=started_at,
                model=attempt_model,
                cmd=cmd,
                env=env,
                cwd=cwd,
                timeout=timeout,
                on_progress=on_progress,
                audit_logger=audit_logger,
                rate_limiter=rate_limiter,
            )
            last_result = result
            if result.status == TaskStatus.SUCCESS:
                return result
            if index >= len(model_attempts) - 1:
                break
            if not _should_use_model_fallback(result, attempt_model):
                break
            logger.warning(
                "[%s] 检测到 %s 的瞬时执行故障，自动降级到 %s 重试",
                task.id,
                attempt_model,
                model_attempts[index + 1],
            )

        assert last_result is not None
        return last_result


def _build_codex_exec_command(
    *,
    task: TaskNode,
    cli_path: str,
    model: str,
    cwd: str | None,
    execution_security_mode: str,
) -> list[str]:
    """Build the codex exec command for one concrete model attempt."""
    cmd = [cli_path, "exec", "--model", model]

    # Codex exec --json produces newline-delimited JSON events.
    cmd.append("--json")

    # Sandbox: workspace-write allows file modifications in the project.
    cmd.extend(["--sandbox", "workspace-write"])

    # Skip git repo check for directories that may not be git repos.
    cmd.append("--skip-git-repo-check")

    if task.ephemeral:
        cmd.append("--ephemeral")

    if execution_security_mode == "trusted_local":
        # Restricted mode keeps Codex inside its normal approval/sandbox boundary.
        cmd.append("--dangerously-bypass-approvals-and-sandbox")

    if cwd:
        cmd.extend(["--cd", cwd])

    if task.extra_args:
        cmd.extend(task.extra_args)

    # Codex reads prompt from stdin when "-" is passed as the prompt arg.
    cmd.append("-")
    return cmd


def _build_model_fallback_candidates(model: str) -> list[str]:
    """Return ordered fallback models for transient provider failures."""
    requested = str(model or "").strip()
    if not requested:
        return []
    candidates: list[str] = []
    roots = [requested]
    if requested.endswith("-pro"):
        roots.insert(0, requested[:-4])

    for root in roots:
        if root != requested:
            candidates.append(root)
        candidates.extend(_CODEX_MODEL_FALLBACKS.get(root, ()))

    deduped: list[str] = []
    for candidate in candidates:
        if candidate and candidate != requested and candidate not in deduped:
            deduped.append(candidate)
    return deduped


def _should_use_model_fallback(result: TaskResult, current_model: str) -> bool:
    """Only fallback on provider/transient failures, never on auth or task logic errors."""
    if result.status == TaskStatus.SUCCESS:
        return False
    text = "\n".join(
        part for part in [result.error or "", result.output or ""] if part
    )
    if not text.strip():
        return False
    lower = text.lower()
    invalid_model = (
        "model_not_found" in lower
        or "invalid model" in lower
        or "\"code\":\"1211\"" in lower
        or '"code": "1211"' in lower
        or "模型不存在" in text
        or "模型代码" in text
    )
    if invalid_model:
        return True
    if current_model.endswith("-pro"):
        return looks_like_rate_limit_error(text) or looks_like_network_error(text)
    return False


def _run_codex_subprocess(
    *,
    task: TaskNode,
    prompt: str,
    limits: LimitsConfig,
    budget_tracker: BudgetTracker | None,
    started_at: datetime,
    model: str,
    cmd: list[str],
    env: dict[str, str],
    cwd: str | None,
    timeout: int,
    on_progress: Callable[[str, dict], None] | None,
    audit_logger: AuditLogger | None,
    rate_limiter: "RateLimiter | None",
) -> TaskResult:
    try:
        use_shell = os.name == "nt" and Path(cmd[0]).suffix.lower() in {".cmd", ".bat"}
        creation_flags = 0
        if os.name == "nt":
            creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP | getattr(subprocess, "CREATE_NO_WINDOW", 0)
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
            shell=use_shell,
            start_new_session=(os.name != "nt"),
            creationflags=creation_flags,
        )
    except PermissionError as exc:
        if audit_logger is not None:
            audit_logger.log_result(task.id, success=False, error_category="execution_error")

        return TaskResult(
            task_id=task.id,
            status=TaskStatus.FAILED,
            error=f"PermissionError: {exc}",
            started_at=started_at,
            finished_at=datetime.now(),
            model_used=model,
        )
    except FileNotFoundError:
        if audit_logger is not None:
            audit_logger.log_result(task.id, success=False, error_category="execution_error")

        return TaskResult(
            task_id=task.id,
            status=TaskStatus.FAILED,
            error=f"Codex CLI not found at '{cmd[0]}'",
            started_at=started_at,
            finished_at=datetime.now(),
            model_used=model,
        )

    pid = proc.pid
    if not _register_proc(proc):
        try:
            proc.kill()
            proc.wait(timeout=5)
        except Exception:
            pass
        if audit_logger is not None:
            audit_logger.log_result(task.id, success=False, error_category="execution_error")

        return TaskResult(
            task_id=task.id,
            status=TaskStatus.FAILED,
            error="Failed to register process (pid is None)",
            started_at=started_at,
            finished_at=datetime.now(),
            model_used=model,
        )

    progress = StreamProgress(on_progress=on_progress)
    stderr_chunks: list[str] = []
    last_activity = time.monotonic()
    activity_lock = threading.Lock()

    def _update_activity() -> None:
        nonlocal last_activity
        with activity_lock:
            last_activity = time.monotonic()

    def _read_stdout() -> None:
        assert proc.stdout is not None
        try:
            for line in proc.stdout:
                _update_activity()
                try:
                    _parse_stream_event(line, task.id, progress)
                    if (
                        task.max_turns is not None
                        and task.max_turns > 0
                        and progress.turn_started > task.max_turns
                        and not progress.max_turns_exceeded
                    ):
                        progress.max_turns_exceeded = True
                        logger.error(
                            "[%s] 超出最大轮次限制: started=%d max_turns=%d，终止进程",
                            task.id,
                            progress.turn_started,
                            task.max_turns,
                        )
                        _kill_process_tree(proc, task.id)
                        break
                except Exception as e:
                    logger.debug("[%s] Stream event parse error: %s | line: %s", task.id, e, line[:200])
        except (ValueError, OSError, IOError) as e:
            logger.warning("[%s] stdout 读取中断（可能因进程被强杀或断流）: %s", task.id, e)

    def _read_stderr() -> None:
        assert proc.stderr is not None
        for line in proc.stderr:
            stderr_chunks.append(line)
            if len(stderr_chunks) > 100:
                stderr_chunks.pop(0)
            _update_activity()
            stripped = line.strip()
            if stripped:
                logger.info("[%s|stderr] %s", task.id, stripped)

    def _heartbeat() -> None:
        consecutive_idle_count = 0
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

                if consecutive_idle_count >= max_consecutive_idle:
                    logger.error(
                        "[%s] 连续 %d 次心跳无输出，强制杀死进程 (PID=%d)",
                        task.id, consecutive_idle_count, pid,
                    )
                    _kill_process_tree(proc, task.id)
                    break
            elif consecutive_idle_count > 0:
                logger.info("[%s] 恢复输出，重置心跳计数器", task.id)
                consecutive_idle_count = 0

    t_out = threading.Thread(target=_read_stdout, daemon=True, name=f"{task.id}-stdout")
    t_err = threading.Thread(target=_read_stderr, daemon=True, name=f"{task.id}-stderr")
    t_hb = threading.Thread(target=_heartbeat, daemon=True, name=f"{task.id}-heartbeat")

    effective_system_prompt = _BASE_SYSTEM_PROMPT
    if task.system_prompt:
        effective_system_prompt = f"{_BASE_SYSTEM_PROMPT}\n{task.system_prompt}"

    try:
        assert proc.stdin is not None
        final_prompt = f"[System Instructions]\n{effective_system_prompt}\n\n[Task]\n{prompt}"
        if task.preload_skills:
            skill_commands = "\n".join(f"/{skill}" for skill in task.preload_skills)
            final_prompt = f"{skill_commands}\n\n{final_prompt}"
            logger.info("[%s] 预加载 Skills: %s", task.id, ", ".join(task.preload_skills))

        try:
            proc.stdin.write(final_prompt)
            proc.stdin.close()
        except (OSError, IOError) as e:
            logger.error("[%s] stdin 写入失败，终止任务: %s", task.id, e)
            _kill_process_tree(proc, task.id)
            _unregister_proc(proc)
            if audit_logger is not None:
                audit_logger.log_result(task.id, success=False, error_category="execution_error")

            return TaskResult(
                task_id=task.id,
                status=TaskStatus.FAILED,
                error=f"Failed to write to stdin: {e}",
                started_at=started_at,
                finished_at=datetime.now(),
                model_used=model,
                pid=pid,
            )
    except OSError as e:
        logger.error("[%s] stdin 处理异常: %s", task.id, e)
        _kill_process_tree(proc, task.id)
        _unregister_proc(proc)
        if audit_logger is not None:
            audit_logger.log_result(task.id, success=False, error_category="execution_error")

        return TaskResult(
            task_id=task.id,
            status=TaskStatus.FAILED,
            error=f"stdin handling error: {e}",
            started_at=started_at,
            finished_at=datetime.now(),
            model_used=model,
            pid=pid,
        )

    t_out.start()
    t_err.start()
    t_hb.start()

    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        _kill_process_tree(proc, task.id)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            logger.warning("[%s] 进程杀死后仍未退出 (PID=%d)", task.id, pid)
        finished_at = datetime.now()
        if audit_logger is not None:
            audit_logger.log_result(task.id, success=False, error_category="execution_error")

        return TaskResult(
            task_id=task.id,
            status=TaskStatus.FAILED,
            error=f"Task timed out after {timeout}s (工具调用 {progress.tool_uses} 次, 最后工具: {progress.last_tool})",
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=(finished_at - started_at).total_seconds(),
            model_used=model,
            pid=pid,
        )
    finally:
        _unregister_proc(proc)

    t_out.join(timeout=5)
    t_err.join(timeout=5)

    finished_at = datetime.now()
    duration = (finished_at - started_at).total_seconds()
    duration_ms = round(duration * 1000, 1)

    stderr = "".join(stderr_chunks)
    if stderr.strip():
        logger.debug("[%s] Complete stderr output:\n%s", task.id, stderr)

    resp = _build_response_from_stream(progress)
    if resp.cli_duration_ms <= 0:
        resp.cli_duration_ms = duration_ms

    if len(resp.result.encode()) > limits.max_output_size_bytes:
        if audit_logger is not None:
            audit_logger.log_result(task.id, success=False, error_category="execution_error")

        return TaskResult(
            task_id=task.id,
            status=TaskStatus.FAILED,
            error=f"Output exceeds max size ({limits.max_output_size_bytes} bytes)",
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=duration,
            model_used=model,
            pid=pid,
        )

    if budget_tracker and resp.cost_usd > 0:
        budget_tracker.check_and_add(resp.cost_usd)

    if proc.returncode == 0 and not resp.is_error:
        if progress.max_turns_exceeded:
            return TaskResult(
                task_id=task.id,
                status=TaskStatus.FAILED,
                error=f"TASK_MAX_TURNS_EXCEEDED: exceeded max_turns={task.max_turns}",
                output=resp.result,
                cost_usd=resp.cost_usd,
                started_at=started_at,
                finished_at=finished_at,
                duration_seconds=duration,
                model_used=resp.model or model,
                pid=pid,
                token_input=resp.token_input,
                token_output=resp.token_output,
                cli_duration_ms=resp.cli_duration_ms,
                tool_uses=progress.tool_uses,
                turn_started=progress.turn_started,
                turn_completed=progress.turn_completed,
                max_turns_exceeded=True,
            )
        if rate_limiter is not None:
            rate_limiter.report_success()
        return TaskResult(
            task_id=task.id,
            status=TaskStatus.SUCCESS,
            output=resp.result,
            cost_usd=resp.cost_usd,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=duration,
            model_used=resp.model or model,
            pid=pid,
            token_input=resp.token_input,
            token_output=resp.token_output,
            cli_duration_ms=resp.cli_duration_ms,
            tool_uses=progress.tool_uses,
            turn_started=progress.turn_started,
            turn_completed=progress.turn_completed,
        )

    if proc.returncode != 0 or resp.is_error:
        error_msg = resp.result if resp.is_error else (stderr.strip() or f"Exit code {proc.returncode}")
        if progress.max_turns_exceeded:
            error_msg = f"TASK_MAX_TURNS_EXCEEDED: exceeded max_turns={task.max_turns}"

        combined_text = f"{error_msg}\n{stderr}"
        combined_lower = combined_text.lower()
        if looks_like_rate_limit_error(combined_text):
            if rate_limiter is not None:
                rate_limiter.report_429()
                logger.warning("[%s] 检测到 429 限流，已触发速率限制器退避", task.id)

        if looks_like_auth_error(combined_text) and not error_msg.startswith("AUTH_EXPIRED:"):
            error_msg = f"AUTH_EXPIRED: {error_msg}"
            logger.warning("[%s] 检测到认证过期: %s", task.id, error_msg[:200])

        if audit_logger is not None:
            audit_logger.log_result(task.id, success=False, error_category="execution_error")

        return TaskResult(
            task_id=task.id,
            status=TaskStatus.FAILED,
            error=error_msg,
            output=resp.result,
            cost_usd=resp.cost_usd,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=duration,
            model_used=resp.model or model,
            pid=pid,
            token_input=resp.token_input,
            token_output=resp.token_output,
            cli_duration_ms=resp.cli_duration_ms,
            tool_uses=progress.tool_uses,
            turn_started=progress.turn_started,
            turn_completed=progress.turn_completed,
            max_turns_exceeded=progress.max_turns_exceeded,
        )

    parsed_output = None
    if task.output_format == "json":
        try:
            parsed_output = json.loads(resp.result)
        except json.JSONDecodeError:
            parsed_output = resp.result

    if audit_logger is not None:
        audit_logger.log_result(task.id, success=True)

    if rate_limiter is not None:
        rate_limiter.report_success()

    return TaskResult(
        task_id=task.id,
        status=TaskStatus.SUCCESS,
        output=resp.result,
        parsed_output=parsed_output if parsed_output is not None else resp.result,
        cost_usd=resp.cost_usd,
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration,
        model_used=resp.model or model,
        pid=pid,
        token_input=resp.token_input,
        token_output=resp.token_output,
        cli_duration_ms=resp.cli_duration_ms,
        tool_uses=progress.tool_uses,
        turn_started=progress.turn_started,
        turn_completed=progress.turn_completed,
        max_turns_exceeded=progress.max_turns_exceeded,
    )
