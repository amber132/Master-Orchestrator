"""Claude Code CLI subprocess wrapper with streaming progress."""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import threading
import time
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime

from .config import ClaudeConfig, LimitsConfig
from .audit_log import AuditLogger
from .error_classifier import looks_like_auth_error, looks_like_network_error, looks_like_rate_limit_error
from .execution_lease import ExecutionLeaseManager
from .log_context import set_task_id
from .model import TaskNode, TaskResult, TaskStatus
from .win32_job import get_global_job

# 从子模块 re-export，保持向后兼容
from .process_manager import (  # noqa: E402
    _active_procs,
    _active_procs_lock,
    _STRIP_DESKTOP_CLAUDE_ENV_KEYS,
    _ORCHESTRATOR_SESSION_ID,
    get_session_id,
    _register_proc,
    _unregister_proc,
    _cleanup_all_procs,
    _gc_finished_procs,
    _build_subprocess_env,
    _kill_process_tree,
)
from .network_health import (  # noqa: E402
    _NETWORK_PROBE_TIMEOUT,
    _NETWORK_PROBE_MAX_WAIT,
    _NETWORK_PROBE_INTERVAL,
    _NETWORK_PROBE_ENDPOINTS,
    _PROBE_TIER1_LIMIT,
    _PROBE_TIER1_INTERVAL,
    _PROBE_TIER2_LIMIT,
    _PROBE_TIER2_INTERVAL,
    _PROBE_TIER3_INTERVAL,
    _PROBE_NOTIFY_WARNING,
    _PROBE_NOTIFY_CRITICAL,
    _probe_single_endpoint,
    _wait_for_network,
)
from .cli_response import (  # noqa: E402
    CostAssertionError,
    CLIResponse,
    StreamProgress,
    _parse_cli_output,
    _parse_stream_event,
    _build_response_from_stream,
)

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


@dataclass
class _ExecutionLeaseBinding:
    manager: ExecutionLeaseManager
    run_id: str
    wait_poll_interval_seconds: float = 0.2
    log_interval_seconds: float = 5.0

_execution_lease_stack: list[_ExecutionLeaseBinding] = []
_execution_lease_stack_lock = threading.Lock()


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


# BudgetTracker 已提取到 budget_tracker.py，通过 re-export 保持向后兼容
from .budget_tracker import BudgetTracker, ModelTokenUsage  # noqa: E402


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


# 成本计算函数已提取到 cost_calculation.py，通过 re-export 保持向后兼容
from .cost_calculation import (  # noqa: E402
    _DEFAULT_PRICING,
    _MODEL_PRICING_PER_MILLION,
    _extract_cost_usd,
    _estimate_cost_from_tokens,
    _get_model_pricing,
    _recursive_find_cost,
    _try_float,
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
