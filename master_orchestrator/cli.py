"""CLI entry point: run, resume, status, retry-failed, visualize."""

from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import os
import shutil
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .model import RunStatus
    from .orchestrator import Orchestrator

logger = logging.getLogger(__name__)

# 零成本失败自动重试上限（超出后放弃，避免无限重试消耗时间）
ZERO_COST_MAX_RETRIES = 3

from .config import DiscoveryConfig, load_config
from .dag_loader import load_dag
from .improvement_plan import load_improvement_plan
from .provider_router import parse_phase_provider_overrides
from .store import Store


@dataclasses.dataclass
class _RetryDecision:
    """零成本失败的重试决策。"""
    should_retry: bool = False
    category: str = ""           # "preflight" | "decompose" | "transient" | ""
    goal_modifier: str = ""      # 重试时追加到 goal 前面的提示


def _quick_env_check() -> bool:
    """快速环境检查：验证 Claude CLI 可用性。"""
    import subprocess
    try:
        clean_env = {k: v for k, v in os.environ.items() if k != "CLAUDE_CLI"}
        result = subprocess.run(
            ['claude', '--version'],
            capture_output=True, text=True, timeout=15,
            shell=(sys.platform == 'win32'), env=clean_env,
        )
        if result.returncode == 0:
            logger.info('环境检查通过: %s', result.stdout.strip())
            return True
        else:
            logger.warning('环境检查失败: claude --version 返回 %d', result.returncode)
            return False
    except Exception as e:
        logger.warning('环境检查异常: %s', e)
        return False


def _resolve_preflight_provider(args: argparse.Namespace, config) -> str:
    provider = getattr(args, "provider", "auto")
    if provider in {"claude", "codex"}:
        return provider
    if getattr(args, "mode", "") == "simple":
        return config.routing.phase_defaults.get("simple", config.routing.default_provider)
    return "claude"


def _preflight_check(work_dir: Path, *, provider: str = "claude", config=None) -> None:
    """统一前置健康检查：验证目标 provider CLI 可调用且工作目录存在。

    任一检查失败立即抛出 RuntimeError，附带诊断信息帮助用户定位问题。
    """
    import subprocess
    provider_name = provider if provider in {"claude", "codex"} else "claude"
    cli_path = (
        getattr(getattr(config, provider_name, None), "cli_path", provider_name)
        if config is not None
        else provider_name
    )
    env_key = "CLAUDE_CLI_PATH" if provider_name == "claude" else "CODEX_CLI_PATH"

    # ── 检查 1：provider CLI 实际可调用（调用 <cli> --version） ──
    clean_env = {k: v for k, v in os.environ.items() if k != env_key}
    try:
        result = subprocess.run(
            [cli_path, '--version'],
            capture_output=True, text=True, timeout=15,
            shell=(sys.platform == 'win32'), env=clean_env,
        )
    except FileNotFoundError:
        raise RuntimeError(
            f"preflight 失败：{provider_name} CLI 未找到。"
            "并确保其在 PATH 中。"
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError(
            f"preflight 失败：{provider_name} --version 超时（15s）。"
            "可能是 CLI 启动挂起或网络问题。"
        )
    except Exception as exc:
        raise RuntimeError(
            f"preflight 失败：执行 {provider_name} --version 时发生异常: {exc}"
        ) from exc

    if result.returncode != 0:
        raise RuntimeError(
            f"preflight 失败：{provider_name} --version 返回非零退出码 {result.returncode}。"
            f"\nstdout: {result.stdout.strip()}"
            f"\nstderr: {result.stderr.strip()}"
        )

    version_info = result.stdout.strip()
    logger.info('preflight: %s CLI 可用 — %s', provider_name, version_info)

    # ── 检查 2：工作目录存在 ──
    if not work_dir.is_dir():
        raise RuntimeError(
            f"preflight 失败：工作目录不存在或不是目录: {work_dir}"
        )

    logger.info('preflight: 工作目录有效 — %s', work_dir)


def _classify_zero_cost_failure(state: object) -> _RetryDecision:
    """分析零成本失败的原因，返回定向重试决策。

    策略：
    - preflight 失败：重试前检查环境，确认 CLI 可用
    - decompose 失败（goal_parse_error）：简化目标后重试
    - 临时性失败（transient/auth_expired）：直接重试
    - 不可重试（user_cancelled/safe_stop/catastrophic_stop/env_missing/init_error/fatal_error）：跳过
    """
    from .auto_model import GoalStatus, PhaseStatus

    # 基本条件：必须 FAILED + 零成本 + 零迭代
    if getattr(state, "status", None) is not GoalStatus.FAILED:
        return _RetryDecision()
    if getattr(state, "total_cost_usd", 0.0) >= 0.001:
        return _RetryDecision()
    if getattr(state, "total_iterations", 1) != 0:
        return _RetryDecision()

    # 已有阶段开始执行的不重试
    phases = getattr(state, "phases", []) or []
    if any(
        getattr(phase, "status", PhaseStatus.PENDING)
        not in (PhaseStatus.PENDING, PhaseStatus.SKIPPED)
        for phase in phases
    ):
        return _RetryDecision()

    # 收集所有失败标记（来自 failure_categories 汇总和 diagnostics 条目）
    failure_categories = getattr(state, "failure_categories", {}) or {}
    diagnostics = getattr(state, "diagnostics", []) or []
    all_failure_keys = set(failure_categories.keys())
    diag_statuses = {getattr(entry, "exit_status", "") for entry in diagnostics}
    all_failure_keys.update(diag_statuses - {"ok", ""})

    # 定向重试：preflight 失败 → 检查环境后重试（优先于 non-retriable，
    # 因为 preflight 失败时 diagnostics 可能同时包含 env_missing）
    if "preflight_failure" in all_failure_keys:
        return _RetryDecision(
            should_retry=True,
            category="preflight",
        )

    # 不可重试类别：命中任意一个即跳过
    _NON_RETRIABLE = {
        "user_cancelled", "safe_stop", "catastrophic_stop",
        "env_missing", "init_error", "fatal_error", "unclassified_failure",
    }
    non_retriable_hits = all_failure_keys & _NON_RETRIABLE
    if non_retriable_hits:
        logger.info('零成本失败但属于不可重试类别: %s', non_retriable_hits)
        return _RetryDecision()

    # 定向重试：decompose 失败 → 简化 goal 后重试
    if "goal_parse_error" in all_failure_keys:
        return _RetryDecision(
            should_retry=True,
            category="decompose",
            goal_modifier="（简化模式）请将目标分解为不超过3个简单阶段，每个阶段只含一个明确动作。原始目标：",
        )

    # 临时性失败或认证过期：直接重试
    if "transient" in all_failure_keys or "auth_expired" in all_failure_keys:
        return _RetryDecision(
            should_retry=True,
            category="transient",
        )

    # 兜底：没有匹配到任何已知可重试类别，不重试
    logger.info('零成本失败但无已知可重试类别: %s', all_failure_keys)
    return _RetryDecision()


def _should_retry_zero_cost_failure(state: object) -> bool:
    """兼容 codex 分支测试的布尔接口。"""
    return _classify_zero_cost_failure(state).should_retry


def _get_project_dir(args: argparse.Namespace) -> str | None:
    """从 CLI 参数中提取项目目录（-d/--dir），用于项目级配置发现。"""
    return getattr(args, "dir", None) or getattr(args, "directory", None)


def _resolve_log_file(args: argparse.Namespace) -> str | None:
    """从 --log-file 或 --log-dir 解析出最终的日志文件路径。

    优先级：--log-file > --log-dir > None
    --log-dir 时自动生成带时间戳的文件名。
    """
    if getattr(args, "log_file", None):
        p = Path(args.log_file)
        p.parent.mkdir(parents=True, exist_ok=True)
        return str(p)

    log_dir = getattr(args, "log_dir", None)
    if log_dir:
        d = Path(log_dir)
        d.mkdir(parents=True, exist_ok=True)
        cmd = getattr(args, "command", "run")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        return str(d / f"claude_orchestrator_{cmd}_{ts}.jsonl")

    return None


def _add_log_args(parser: argparse.ArgumentParser) -> None:
    """给子命令统一添加 --log-file 和 --log-dir 参数。"""
    parser.add_argument("--log-file", default=None, help="Path to JSON Lines log file")
    parser.add_argument(
        "--log-dir", default=None,
        help="Directory for log output (auto-generates timestamped filename)",
    )


def _add_pool_args(parser: argparse.ArgumentParser) -> None:
    """给子命令添加 failover pool 相关参数。"""
    parser.add_argument("--pool-config", default=None, help="Path to failover_pool.toml")
    parser.add_argument("--pool-profile", default=None, help="Force a specific pool profile for this execution")


def _resolve_discovery_config(base: DiscoveryConfig, args: argparse.Namespace) -> DiscoveryConfig:
    """从 CLI 参数覆盖 DiscoveryConfig 的默认值。

    用 dataclasses.replace 创建副本，避免修改全局配置对象。
    """
    cfg = dataclasses.replace(
        base,
        enabled_providers=list(base.enabled_providers),
        disabled_providers=list(base.disabled_providers),
    )
    if getattr(args, "search_provider", None):
        cfg.enabled_providers = list(dict.fromkeys(args.search_provider))
    if getattr(args, "disable_search_provider", None):
        cfg.disabled_providers = list(dict.fromkeys(args.disable_search_provider))
    if getattr(args, "min_source_score", None) is not None:
        cfg.min_source_score = args.min_source_score
    if getattr(args, "max_hits_per_provider", None) is not None:
        cfg.max_hits_per_provider = args.max_hits_per_provider
    if getattr(args, "disable_discovery_research", False):
        cfg.research_enabled = False
    if getattr(args, "research_iterations", None) is not None:
        cfg.research_iterations = args.research_iterations
    if getattr(args, "research_probe_budget", None) is not None:
        cfg.research_probe_budget = args.research_probe_budget
    if getattr(args, "research_max_leads", None) is not None:
        cfg.research_max_leads = args.research_max_leads
    if getattr(args, "sogou_cookie_header", None):
        cfg.sogou_cookie_header = args.sogou_cookie_header
    if getattr(args, "sogou_cookie_file", None):
        cfg.sogou_cookie_file = args.sogou_cookie_file
    if getattr(args, "sogou_storage_state", None):
        cfg.sogou_storage_state_path = args.sogou_storage_state
    return cfg


def _confirm_execution_preview(preview_text: str) -> bool:
    print(preview_text)
    try:
        answer = input("\n确认执行以上计划？[y/N]: ").strip().lower()
    except EOFError:
        return False
    return answer in {"y", "yes"}


def _resolve_auto_config(args: argparse.Namespace, config) -> "AutoConfig":
    """从 CLI 参数和 config.toml 合并构造 AutoConfig。

    CLI 参数优先于 config.toml 中的 [auto] 配置节；
    未指定的参数回退到 config.toml 的默认值。
    """
    from .auto_model import AutoConfig, QualityGate

    quality_gate = QualityGate()
    if args.quality_gate is not None:
        quality_gate = QualityGate(commands=args.quality_gate, enabled=True)

    auto_defaults = config.auto
    explicit_provider = getattr(args, "provider", "auto")
    phase_overrides = parse_phase_provider_overrides(getattr(args, "phase_provider", []) or [])

    def _provider_for_phase(phase: str) -> str:
        if explicit_provider in {"claude", "codex"}:
            return explicit_provider
        if phase_overrides.get(phase) in {"claude", "codex"}:
            return phase_overrides[phase]
        phase_default = config.routing.phase_defaults.get(phase)
        if phase_default in {"claude", "codex"}:
            return phase_default
        return "claude"

    def _model_for_provider(provider: str) -> str:
        return config.codex.default_model if provider == "codex" else config.claude.default_model
    manual_overrides: set[str] = set()
    if args.max_phase_iterations is not None:
        manual_overrides.add("max_phase_iterations")
    if args.convergence_threshold is not None:
        manual_overrides.add("convergence_threshold")
    if args.convergence_window is not None:
        manual_overrides.add("convergence_window")
    if args.score_improvement_min is not None:
        manual_overrides.add("score_improvement_min")

    return AutoConfig(
        max_hours=args.max_hours if args.max_hours is not None else auto_defaults.max_hours,
        max_total_iterations=(
            args.max_iterations if args.max_iterations is not None else auto_defaults.max_total_iterations
        ),
        max_phase_iterations=(
            args.max_phase_iterations if args.max_phase_iterations is not None else auto_defaults.max_phase_iterations
        ),
        phase_parallelism=(
            args.phase_parallelism if args.phase_parallelism is not None else auto_defaults.phase_parallelism
        ),
        convergence_threshold=(
            args.convergence_threshold
            if args.convergence_threshold is not None
            else auto_defaults.convergence_threshold
        ),
        convergence_window=(
            args.convergence_window if args.convergence_window is not None else auto_defaults.convergence_window
        ),
        min_convergence_checks=auto_defaults.min_convergence_checks,
        score_improvement_min=(
            args.score_improvement_min
            if args.score_improvement_min is not None
            else auto_defaults.score_improvement_min
        ),
        decomposition_model=_model_for_provider(_provider_for_phase("decompose")),
        review_model=_model_for_provider(_provider_for_phase("review")),
        execution_model=_model_for_provider(_provider_for_phase("execute")),
        quality_gate=quality_gate,
        adaptive_tuning_enabled=(
            False if args.disable_adaptive_tuning else auto_defaults.adaptive_tuning_enabled
        ),
        manual_overrides=manual_overrides,
        max_execution_processes=(
            args.max_execution_processes
            if args.max_execution_processes is not None
            else auto_defaults.max_execution_processes
        ),
        execution_lease_db_path=auto_defaults.execution_lease_db_path,
        execution_lease_ttl_seconds=auto_defaults.execution_lease_ttl_seconds,
    )


def _normalize_quality_gate_commands(commands: list[str] | None) -> list[str] | None:
    """将 CLI quality-gate 输入规范化为可执行的 shell 命令字符串。

    用户经常不加引号传递单个命令，如 `--quality-gate mvnw.cmd test`。
    Argparse 会将其拆分为多个 token，但运行时期望每个列表元素是一条完整的 shell 命令。
    当所有 token 都不含空格时，将整个列表合并为一条命令；含空格的引号项保持不变。
    """
    if commands is None:
        return None

    normalized = [str(item).strip() for item in commands if str(item).strip()]
    if len(normalized) <= 1:
        return normalized
    if any(any(ch.isspace() for ch in item) for item in normalized):
        return normalized
    return [" ".join(normalized)]


def _resolve_self_improve_quality_gates(args: argparse.Namespace) -> list[str]:
    commands = list(_normalize_quality_gate_commands(getattr(args, "quality_gate", None)) or [])
    monitor_required = bool(getattr(args, "monitor_required", False))
    monitor_flows = [str(item).strip() for item in getattr(args, "monitor_flow", []) or [] if str(item).strip()]

    if monitor_required or monitor_flows:
        repo_root = Path.cwd().resolve()
        script_path = repo_root / "scripts" / "run_flow_matrix.py"
        venv_python = repo_root / ".venv" / "Scripts" / "python.exe"
        python_executable = venv_python if venv_python.exists() else Path(sys.executable).resolve()
        flow_parts = [
            str(python_executable),
            str(script_path),
            "--repo-root",
            "{workspace_dir}",
            "--python-executable",
            str(python_executable),
        ]
        for flow_id in monitor_flows:
            flow_parts.extend(["--flow", flow_id])
        commands.append(" ".join(flow_parts))

    return commands


def _resolve_self_improve_auto_config(args: argparse.Namespace, config) -> "AutoConfig":
    from .auto_model import AutoConfig

    explicit_provider = getattr(args, "provider", "auto")
    phase_overrides = parse_phase_provider_overrides(getattr(args, "phase_provider", []) or [])

    def _provider_for_phase(phase: str) -> str:
        if explicit_provider in {"claude", "codex"}:
            return explicit_provider
        if phase_overrides.get(phase) in {"claude", "codex"}:
            return phase_overrides[phase]
        if phase == "self_improve":
            phase = "discover"
        phase_default = getattr(config.routing, "phase_defaults", {}).get(phase)
        if phase_default in {"claude", "codex"}:
            return phase_default
        return "claude"

    def _model_for_provider(provider: str) -> str:
        return config.codex.default_model if provider == "codex" else config.claude.default_model

    return AutoConfig(
        max_hours=args.max_hours,
        max_total_iterations=args.max_iterations,
        decomposition_model=_model_for_provider(_provider_for_phase("decompose")),
        review_model=_model_for_provider(_provider_for_phase("review")),
        execution_model=_model_for_provider(_provider_for_phase("execute")),
    )

# ── SIGTERM 信号处理 ──

_shutdown_requested = False
_VISIBLE_COMMANDS = "{do,runs,improve}"
_DAG_INPUT_SUFFIXES = {".toml", ".py"}
_PROVIDER_COMMAND_CHOICES = ("do", "runs", "improve")


def _sigterm_handler(signum: int, frame: object) -> None:
    """SIGTERM handler: 设置关闭标志，让主循环优雅退出。
    
    注意：不在信号处理器中抛出异常，因为信号是异步的，
    在任意代码位置抛出异常可能导致资源泄漏或状态不一致。
    """
    global _shutdown_requested
    _shutdown_requested = True
    logger.warning("收到 SIGTERM 信号 (signum=%d)，设置关闭标志...", signum)
    # 不抛出异常，让主循环检查 _shutdown_requested 标志并优雅退出


def _add_auto_parser_args(
    parser: argparse.ArgumentParser,
    *,
    positional_name: str,
    positional_help: str,
) -> None:
    parser.add_argument(positional_name, nargs="?", default="", help=positional_help)
    parser.add_argument("-d", "--dir", default=".", help="Project working directory")
    parser.add_argument("--provider", choices=["auto", "claude", "codex"], default="auto", help="Preferred execution provider")
    parser.add_argument(
        "--phase-provider",
        action="append",
        default=[],
        metavar="PHASE=PROVIDER",
        help="Override provider for a phase, e.g. execute=codex",
    )
    parser.add_argument("--doc", nargs="*", default=[], help="Optional document paths that define the task")
    parser.add_argument(
        "--mode",
        choices=["auto", "simple", "surgical"],
        default="auto",
        help="Execution mode: auto=parallel DAG, simple=single task, surgical=iterate-fix-verify",
    )
    _add_pool_args(parser)
    parser.add_argument("-y", "--yes", action="store_true", help="Skip preview confirmation and start immediately")
    parser.add_argument("--max-hours", type=float, default=None, help="Max hours to run (default: from config, fallback 24)")
    parser.add_argument("--max-iterations", type=int, default=None, help="Max total iterations (default: from config, fallback 50)")
    parser.add_argument(
        "--max-phase-iterations", type=int, default=None,
        help="Max iterations per phase (default: adaptive by complexity)",
    )
    parser.add_argument(
        "--phase-parallelism", type=int, default=None,
        help="Max number of phases to execute in parallel (default: 8)",
    )
    parser.add_argument(
        "--convergence-threshold", type=float, default=None,
        help="Stop threshold for review score (default: adaptive by complexity)",
    )
    parser.add_argument(
        "--convergence-window", type=int, default=None,
        help="Plateau detection window (default: adaptive by complexity)",
    )
    parser.add_argument(
        "--score-improvement-min", type=float, default=None,
        help="Minimum score delta treated as meaningful improvement (default: adaptive by complexity)",
    )
    parser.add_argument(
        "--disable-adaptive-tuning", action="store_true",
        help="Keep manual convergence settings and skip complexity-based retuning",
    )
    parser.add_argument(
        "--max-execution-processes",
        type=int,
        default=None,
        help="Hard cap for real concurrent claude exec processes in auto mode (default: from config, 0 disables)",
    )
    _add_log_args(parser)
    parser.add_argument("--resume", action="store_true", help="Resume from saved goal_state.json")
    parser.add_argument("--runtime-dir", default=None, help="Reuse an existing runtime directory when resuming")
    parser.add_argument(
        "--quality-gate", nargs="*", default=None, metavar="CMD",
        help="Quality gate commands to run before AI review (e.g. 'pytest -x' 'ruff check .')",
    )
    parser.add_argument(
        "--gather", action="store_true",
        help="Enable requirement gathering before goal decomposition",
    )
    parser.add_argument(
        "--gather-mode", choices=["interactive", "file", "auto"], default=None,
        help="Gathering mode: interactive (CLI Q&A), file (read from JSON), auto (AI answers)",
    )
    parser.add_argument(
        "--gather-max-rounds", type=int, default=None,
        help="Max clarification rounds for requirement gathering (default: from config.toml)",
    )
    parser.add_argument(
        "--gather-file", default=None,
        help="Path to pre-filled answers JSON file (for --gather-mode file)",
    )
    parser.add_argument(
        "--skip-gather", action="store_true",
        help="Explicitly skip requirement gathering (overrides config.toml global setting)",
    )
    parser.add_argument(
        "--architecture-mode",
        choices=["auto", "off", "required"],
        default="auto",
        help="Architecture layer mode: auto trigger, off, or required",
    )
    parser.add_argument(
        "--architecture-pattern",
        default=None,
        help="Preferred architecture pattern for the architecture layer",
    )
    parser.add_argument(
        "--architecture-deliberation",
        choices=["deterministic", "advisory"],
        default="deterministic",
        help="Architecture council mode: deterministic synthesis or advisory multi-role board",
    )
    parser.add_argument("--files", nargs="+", action="append", default=[], help="Simple mode explicit files or directories")
    parser.add_argument("--glob", nargs="+", action="append", default=[], dest="globs", help="Simple mode glob patterns")
    parser.add_argument("--task-file", default=None, help="Simple mode CSV or JSONL task file")
    parser.add_argument("--prompt-file", default=None, help="Simple mode prompt file")
    parser.add_argument("--isolate", choices=["none", "copy", "worktree"], default=None, help="Simple mode isolation mode")


def _add_self_improve_parser_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("-d", "--dir", default=".", help="Project working directory (contains goal_state.json, db)")
    parser.add_argument("--provider", choices=["auto", "claude", "codex"], default="auto", help="Preferred execution provider")
    parser.add_argument(
        "--phase-provider",
        action="append",
        default=[],
        metavar="PHASE=PROVIDER",
        help="Override provider for a phase, e.g. review=claude",
    )
    parser.add_argument(
        "--source", nargs="*", default=[], metavar="URL_OR_FILE",
        help="External documents to scan for improvement ideas",
    )
    parser.add_argument("--approval-mode", choices=["interactive", "file"], default="interactive")
    parser.add_argument("--approval-file", default=None, help="Path for file-based approval")
    parser.add_argument("--plan-file", default=None, help="Path to a JSON improvement plan that seeds proposals")
    parser.add_argument("--plan-phase", default=None, help="Optional phase tag filter when loading --plan-file")
    parser.add_argument(
        "--quality-gate", nargs="*", default=None, metavar="CMD",
        help="Test commands to run before/after improvement",
    )
    parser.add_argument("--monitor-required", action="store_true", help="Run required flow-matrix gate after each self-improve round")
    parser.add_argument("--monitor-flow", action="append", default=[], metavar="FLOW_ID", help="Append one or more flow-matrix smoke flows to the monitoring gate")
    parser.add_argument("--max-hours", type=float, default=2.0)
    parser.add_argument("--max-iterations", type=int, default=10)
    _add_log_args(parser)
    parser.add_argument("--skip-introspection", action="store_true", help="Skip run history analysis")
    parser.add_argument("--skip-external", action="store_true", help="Skip external document scanning")
    parser.add_argument("--discover", action="store_true", help="Enable auto-discovery of relevant blog posts")
    parser.add_argument(
        "--smart-discover", action="store_true",
        help="Let AI analyze the project first, then decide search keywords and templates (implies --discover)",
    )
    parser.add_argument(
        "--keywords", nargs="*", default=[], metavar="KW",
        help="Extra keywords for auto-discovery search",
    )
    parser.add_argument(
        "--rss-feed", nargs="*", default=[], metavar="URL",
        help="Extra RSS/Atom feed URLs for auto-discovery",
    )
    parser.add_argument("--discover-max", type=int, default=20, help="Max URLs to discover (default: 20)")
    parser.add_argument(
        "--search-template", default="{keyword} best practices",
        help='Search query template, use {keyword} as placeholder (default: "{keyword} best practices")',
    )
    parser.add_argument(
        "--search-provider",
        nargs="+",
        default=None,
        metavar="NAME",
        help="Override enabled search providers, e.g. duckduckgo_html brave_web rss",
    )
    parser.add_argument(
        "--disable-search-provider",
        nargs="+",
        default=None,
        metavar="NAME",
        help="Disable one or more configured search providers",
    )
    parser.add_argument(
        "--min-source-score",
        type=float,
        default=None,
        help="Minimum trust score required before external scanning",
    )
    parser.add_argument(
        "--max-hits-per-provider",
        type=int,
        default=None,
        help="Max normalized hits to keep per provider query",
    )
    parser.add_argument(
        "--disable-discovery-research",
        action="store_true",
        help="Disable the anchor/contradiction-driven research loop inside discovery",
    )
    parser.add_argument(
        "--research-iterations",
        type=int,
        default=None,
        help="Number of contradiction-driven research rounds to run after the first discovery pass",
    )
    parser.add_argument(
        "--research-probe-budget",
        type=int,
        default=None,
        help="Maximum number of follow-up probe queries generated by discovery research",
    )
    parser.add_argument(
        "--research-max-leads",
        type=int,
        default=None,
        help="Maximum number of structured anchors to keep per discovery run",
    )
    parser.add_argument(
        "--sogou-cookie-header",
        default=None,
        help="Cookie header used when resolving sogou wechat article links",
    )
    parser.add_argument(
        "--sogou-cookie-file",
        default=None,
        help="Cookie file (JSON/Netscape) used when resolving sogou wechat article links",
    )
    parser.add_argument(
        "--sogou-storage-state",
        default=None,
        help="Playwright storage state JSON used when resolving sogou wechat article links",
    )
    parser.add_argument(
        "--rounds", type=int, default=1,
        help="Number of self-improve rounds to run (default: 1). Each round re-discovers and re-analyzes.",
    )
    parser.add_argument(
        "--auto-approve", action="store_true",
        help="Automatically approve all proposals without interactive confirmation",
    )
    parser.add_argument(
        "--round-delay", type=int, default=30,
        help="Seconds to wait between rounds (default: 30)",
    )


def _copy_args(args: argparse.Namespace, **updates) -> argparse.Namespace:
    copied = argparse.Namespace(**vars(args))
    for key, value in updates.items():
        setattr(copied, key, value)
    return copied


def _looks_like_dag_input(target: str, working_dir: str | None) -> bool:
    if not target:
        return False
    candidate = Path(target)
    if candidate.suffix.lower() not in _DAG_INPUT_SUFFIXES:
        return False
    if not candidate.is_absolute():
        candidate = Path(working_dir or ".").resolve() / candidate
    return candidate.exists() and candidate.is_file()


def _has_simple_inputs(args: argparse.Namespace) -> bool:
    return bool(
        getattr(args, "files", None)
        or getattr(args, "globs", None)
        or getattr(args, "task_file", None)
        or getattr(args, "prompt_file", None)
        or getattr(args, "isolate", None)
    )


def _resolve_command_aliases(args: argparse.Namespace) -> argparse.Namespace:
    command = getattr(args, "command", "")
    if command in {"claude", "codex"}:
        parser = _build_parser()
        nested = parser.parse_args([args.provider_command, *list(getattr(args, "provider_args", []) or [])])
        if getattr(nested, "provider", "auto") == "auto":
            nested = _copy_args(nested, provider=command)
        else:
            nested = _copy_args(nested, provider=getattr(nested, "provider"))
        return _resolve_command_aliases(nested)

    if command == "improve":
        return _copy_args(args, command="self-improve")

    if command == "runs":
        action_count = sum(
            bool(flag)
            for flag in (
                getattr(args, "resume", False),
                getattr(args, "retry", False),
                getattr(args, "graph", False),
            )
        )
        if action_count > 1:
            raise ValueError("runs 仅允许一个操作标记: --resume、--retry、--graph")
        if getattr(args, "as_json", False) and action_count:
            raise ValueError("runs --json 仅支持状态查询，不能与 --resume、--retry、--graph 组合")
        if getattr(args, "resume", False):
            if not getattr(args, "dag", ""):
                raise ValueError("runs --resume 需要提供 DAG 文件路径")
            return _copy_args(args, command="resume")
        if getattr(args, "retry", False):
            if not getattr(args, "dag", ""):
                raise ValueError("runs --retry 需要提供 DAG 文件路径")
            return _copy_args(args, command="retry-failed")
        if getattr(args, "graph", False):
            if not getattr(args, "dag", ""):
                raise ValueError("runs --graph 需要提供 DAG 文件路径")
            return _copy_args(args, command="visualize")
        return _copy_args(args, command="status")

    if command != "do":
        return args

    target = (getattr(args, "target", "") or "").strip()
    docs = list(getattr(args, "doc", []) or [])
    if target.startswith("@"):
        referenced_target = target[1:].strip()
        if _looks_like_dag_input(referenced_target, getattr(args, "dir", None)):
            target = referenced_target
        else:
            docs = [referenced_target, *docs]
            target = ""

    if getattr(args, "resume", False) and _has_simple_inputs(args):
        raise ValueError("do --resume 不能与 simple 输入参数组合")

    if _looks_like_dag_input(target, getattr(args, "dir", None)):
        if getattr(args, "resume", False):
            return _copy_args(args, command="resume", dag=target, doc=docs)
        return _copy_args(args, command="run", dag=target, doc=docs)

    if _has_simple_inputs(args):
        return _copy_args(
            args,
            command="simple",
            simple_command="run",
            instruction=target,
            goal=target,
            doc=docs,
        )

    return _copy_args(args, command="auto", goal=target, doc=docs)


def _prune_hidden_subcommands(subparsers: argparse._SubParsersAction) -> None:
    subparsers._choices_actions = [
        action
        for action in subparsers._choices_actions
        if getattr(action, "dest", "") in {"do", "runs", "improve"}
    ]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="master-orchestrator",
        description="Master Orchestrator - unify Claude Code and Codex CLI workflows",
        epilog="Preferred commands: do, runs, improve. Legacy commands remain available for compatibility.",
    )
    parser.add_argument("-c", "--config", default=None, help="Path to config.toml")
    sub = parser.add_subparsers(dest="command", required=True, metavar=_VISIBLE_COMMANDS)

    do_p = sub.add_parser("do", help="Unified execution entrypoint")
    _add_auto_parser_args(
        do_p,
        positional_name="target",
        positional_help="Goal text, @document, or DAG path",
    )
    do_p.add_argument("--run-id", default=None, help="Run ID to resume when dispatching to DAG resume")
    do_p.add_argument(
        "--rate-limit", type=int, default=None, metavar="N",
        help="Max requests per minute when dispatching to DAG run",
    )
    do_p.add_argument(
        "--error-policy", choices=["fail-fast", "continue-on-error", "skip-downstream"], default=None,
        help="Default error handling policy when dispatching to DAG run",
    )
    do_p.add_argument(
        "--enable-streaming", action="store_true",
        help="Enable streaming event logs when dispatching to DAG run",
    )

    runs_p = sub.add_parser("runs", help="Run management entrypoint")
    runs_p.add_argument("dag", nargs="?", default="", help="Optional DAG file for resume, retry, or graph views")
    runs_p.add_argument("-d", "--dir", default=None, help="Working directory for DAG resume or retry")
    _add_log_args(runs_p)
    _add_pool_args(runs_p)
    runs_p.add_argument("--run-id", default=None, help="Run ID (default: latest)")
    runs_p.add_argument("--json", action="store_true", dest="as_json", help="Output status as JSON")
    runs_p.add_argument("--resume", action="store_true", help="Resume a DAG run")
    runs_p.add_argument("--retry", action="store_true", help="Retry failed tasks in a DAG run")
    runs_p.add_argument("--graph", action="store_true", help="Show DAG structure")

    improve_p = sub.add_parser("improve", help="Project self-improvement workflow")
    _add_self_improve_parser_args(improve_p)

    claude_p = sub.add_parser("claude", help=argparse.SUPPRESS)
    claude_p.add_argument(
        "provider_command",
        choices=[
            "do", "runs", "improve", "simple",
            "run", "resume", "retry-failed", "status", "visualize", "auto", "self-improve",
        ],
    )
    claude_p.add_argument("provider_args", nargs=argparse.REMAINDER)

    codex_p = sub.add_parser("codex", help=argparse.SUPPRESS)
    codex_p.add_argument(
        "provider_command",
        choices=[
            "do", "runs", "improve", "simple",
            "run", "resume", "retry-failed", "status", "visualize", "auto", "self-improve",
        ],
    )
    codex_p.add_argument("provider_args", nargs=argparse.REMAINDER)

    # run
    run_p = sub.add_parser("run", help=argparse.SUPPRESS)
    run_p.add_argument("dag", help="Path to DAG file (.toml or .py)")
    run_p.add_argument("-d", "--dir", default=None, help="Working directory for tasks")
    _add_log_args(run_p)
    _add_pool_args(run_p)
    run_p.add_argument(
        "--rate-limit", type=int, default=None, metavar="N",
        help="Max requests per minute (overrides config.toml rate_limit.requests_per_minute)",
    )
    run_p.add_argument(
        "--error-policy", choices=["fail-fast", "continue-on-error", "skip-downstream"], default=None,
        help="Default error handling policy for tasks without explicit error_policy",
    )
    run_p.add_argument(
        "--enable-streaming", action="store_true",
        help="Enable streaming event logs to Store.stream_events table",
    )

    # resume
    resume_p = sub.add_parser("resume", help=argparse.SUPPRESS)
    resume_p.add_argument("dag", help="Path to DAG file (.toml or .py)")
    resume_p.add_argument("--run-id", default=None, help="Run ID to resume (default: latest)")
    resume_p.add_argument("-d", "--dir", default=None, help="Working directory for tasks")
    _add_log_args(resume_p)
    _add_pool_args(resume_p)

    # retry-failed
    retry_p = sub.add_parser("retry-failed", help=argparse.SUPPRESS)
    retry_p.add_argument("dag", help="Path to DAG file (.toml or .py)")
    retry_p.add_argument("--run-id", default=None, help="Run ID (default: latest)")
    retry_p.add_argument("-d", "--dir", default=None, help="Working directory for tasks")
    _add_log_args(retry_p)
    _add_pool_args(retry_p)

    # status
    status_p = sub.add_parser("status", help=argparse.SUPPRESS)
    status_p.add_argument("--run-id", default=None, help="Run ID (default: latest)")
    status_p.add_argument("--json", action="store_true", dest="as_json", help="Output as JSON")

    # visualize
    viz_p = sub.add_parser("visualize", help=argparse.SUPPRESS)
    viz_p.add_argument("dag", help="Path to DAG file (.toml or .py)")
    viz_p.add_argument("--run-id", default=None, help="Overlay status from a run")

    # auto
    auto_p = sub.add_parser("auto", help=argparse.SUPPRESS)
    _add_auto_parser_args(
        auto_p,
        positional_name="goal",
        positional_help="Goal description in natural language",
    )

    # self-improve
    si_p = sub.add_parser("self-improve", help=argparse.SUPPRESS)
    _add_self_improve_parser_args(si_p)

    from .simple_cli import add_simple_subcommands

    add_simple_subcommands(sub, _add_log_args, hidden=True)
    _prune_hidden_subcommands(sub)

    return parser


def _cmd_run(args: argparse.Namespace) -> int:
    from .dag_loader import dag_hash
    from .model import RunStatus
    from .notification import init_notifier, get_notifier
    from .orchestrator import Orchestrator
    from .startup import parallel_init

    # config 和 DAG 加载互不依赖，并行执行以加速启动
    init_results = parallel_init({
        "config": lambda: load_config(args.config, project_dir=_get_project_dir(args)),
        "dag": lambda: load_dag(args.dag),
    })
    config = init_results["config"]
    dag = init_results["dag"]
    init_notifier(config.notification)
    log_file = _resolve_log_file(args)

    # CLI 参数覆盖配置文件
    if args.rate_limit is not None:
        config.rate_limit.requests_per_minute = args.rate_limit

    with Store(config.checkpoint.db_path) as store:
        orch = Orchestrator(
            dag=dag, config=config, store=store,
            working_dir=args.dir, log_file=log_file,
            default_error_policy=args.error_policy,
            enable_streaming=args.enable_streaming,
        )
        info = orch.run(dag_hash=dag.content_hash())

    if info.status != RunStatus.COMPLETED:
        get_notifier().warning("DAG 运行失败", dag=dag.name, status=info.status.value)

    return 0 if info.status == RunStatus.COMPLETED else 1


def _cmd_resume(args: argparse.Namespace) -> int:
    from .model import RunStatus
    from .notification import init_notifier, get_notifier
    from .orchestrator import Orchestrator
    from .startup import parallel_init

    # config 和 DAG 加载互不依赖，并行执行以加速启动
    init_results = parallel_init({
        "config": lambda: load_config(args.config, project_dir=_get_project_dir(args)),
        "dag": lambda: load_dag(args.dag),
    })
    config = init_results["config"]
    dag = init_results["dag"]
    init_notifier(config.notification)
    log_file = _resolve_log_file(args)

    with Store(config.checkpoint.db_path) as store:
        run_id = args.run_id
        if not run_id:
            latest = store.get_latest_run(dag.name)
            if not latest:
                print("No previous run found to resume.", file=sys.stderr)
                return 1
            run_id = latest.run_id

        orch = Orchestrator(
            dag=dag, config=config, store=store,
            working_dir=args.dir, log_file=log_file,
        )
        info = orch.resume(run_id)

    if info.status != RunStatus.COMPLETED:
        get_notifier().warning("DAG resume 失败", dag=dag.name, run_id=run_id, status=info.status.value)

    return 0 if info.status == RunStatus.COMPLETED else 1


def _cmd_retry_failed(args: argparse.Namespace) -> int:
    from .model import RunStatus
    from .notification import init_notifier, get_notifier
    from .orchestrator import Orchestrator
    from .startup import parallel_init

    # config 和 DAG 加载互不依赖，并行执行以加速启动
    init_results = parallel_init({
        "config": lambda: load_config(args.config, project_dir=_get_project_dir(args)),
        "dag": lambda: load_dag(args.dag),
    })
    config = init_results["config"]
    dag = init_results["dag"]
    init_notifier(config.notification)
    log_file = _resolve_log_file(args)

    with Store(config.checkpoint.db_path) as store:
        run_id = args.run_id
        if not run_id:
            latest = store.get_latest_run(dag.name)
            if not latest:
                print("No previous run found.", file=sys.stderr)
                return 1
            run_id = latest.run_id

        orch = Orchestrator(
            dag=dag, config=config, store=store,
            working_dir=args.dir, log_file=log_file,
        )
        info = orch.retry_failed(run_id)

    if info.status != RunStatus.COMPLETED:
        get_notifier().warning("DAG retry-failed 失败", dag=dag.name, run_id=run_id, status=info.status.value)

    return 0 if info.status == RunStatus.COMPLETED else 1


def _cmd_status(args: argparse.Namespace) -> int:
    config = load_config(args.config, project_dir=_get_project_dir(args))

    with Store(config.checkpoint.db_path) as store:
        run_id = args.run_id
        if not run_id:
            info = store.get_latest_run()
            if not info:
                print("No runs found.", file=sys.stderr)
                return 1
        else:
            info = store.get_run(run_id)
            if not info:
                print(f"Run '{run_id}' not found.", file=sys.stderr)
                return 1

        simple_run = store.get_simple_run(info.run_id)
        if simple_run is not None:
            from .simple_status import render_simple_status_json, render_simple_status_text

            items = store.get_simple_items(simple_run.run_id)
            manifest = store.get_simple_manifest(simple_run.run_id)
            events = store.get_simple_events(simple_run.run_id, limit=20)
            if args.as_json:
                print(render_simple_status_json(simple_run, manifest, items, events))
            else:
                print(render_simple_status_text(simple_run, manifest, items, events))
            return 0

        results = store.get_all_task_results(info.run_id)

    if args.as_json:
        data = {
            "run_id": info.run_id,
            "dag_name": info.dag_name,
            "status": info.status.value,
            "started_at": info.started_at.isoformat(),
            "finished_at": info.finished_at.isoformat() if info.finished_at else None,
            "total_cost_usd": info.total_cost_usd,
            "tasks": {
                tid: {
                    "status": r.status.value,
                    "attempt": r.attempt,
                    "cost_usd": r.cost_usd,
                    "duration_seconds": r.duration_seconds,
                    "error": r.error,
                }
                for tid, r in results.items()
            },
        }
        print(json.dumps(data, indent=2))
    else:
        print(f"Run:     {info.run_id}")
        print(f"DAG:     {info.dag_name}")
        print(f"Status:  {info.status.value}")
        print(f"Started: {info.started_at}")
        if info.finished_at:
            print(f"Ended:   {info.finished_at}")
        print(f"Cost:    ${info.total_cost_usd:.4f}")
        print()

        # Count by status
        counts: dict[str, int] = {}
        for r in results.values():
            counts[r.status.value] = counts.get(r.status.value, 0) + 1
        print("Tasks:")
        for status, count in sorted(counts.items()):
            print(f"  {status}: {count}")
        print()

        # Task details
        for tid, r in sorted(results.items()):
            icon = {"success": "[OK]", "failed": "[X]", "running": "[~]", "pending": "[ ]",
                    "skipped": "[-]", "cancelled": "[-]"}.get(r.status.value, "[?]")
            line = f"  {icon} {tid}: {r.status.value}"
            if r.duration_seconds:
                line += f" ({r.duration_seconds:.1f}s)"
            if r.cost_usd:
                line += f" ${r.cost_usd:.4f}"
            if r.error:
                line += f" — {r.error[:80]}"
            print(line)

    return 0


def _cmd_auto(args: argparse.Namespace) -> int:
    args.quality_gate = _normalize_quality_gate_commands(args.quality_gate)

    if getattr(args, "mode", "auto") == "simple":
        disallowed = []
        for flag, active in (
            ("--doc", bool(args.doc)),
            ("--resume", bool(args.resume)),
            ("--quality-gate", args.quality_gate is not None),
            ("--gather", bool(args.gather)),
            ("--gather-mode", args.gather_mode is not None),
            ("--gather-max-rounds", args.gather_max_rounds is not None),
            ("--gather-file", args.gather_file is not None),
            ("--skip-gather", bool(args.skip_gather)),
            ("--max-phase-iterations", args.max_phase_iterations is not None),
            ("--phase-parallelism", args.phase_parallelism is not None),
            ("--convergence-threshold", args.convergence_threshold is not None),
            ("--convergence-window", args.convergence_window is not None),
            ("--score-improvement-min", args.score_improvement_min is not None),
            ("--disable-adaptive-tuning", bool(args.disable_adaptive_tuning)),
            ("--max-execution-processes", args.max_execution_processes is not None),
        ):
            if active:
                disallowed.append(flag)
        if disallowed:
            print(f"auto --mode simple 不允许与以下参数组合: {', '.join(disallowed)}", file=sys.stderr)
            return 1
        simple_args = argparse.Namespace(
            config=args.config,
            simple_command="run",
            instruction=args.goal,
            dir=args.dir,
            provider=getattr(args, "provider", "auto"),
            files=args.files,
            globs=args.globs,
            task_file=args.task_file,
            prompt_file=args.prompt_file,
            isolate=args.isolate,
            log_file=args.log_file,
            log_dir=args.log_dir,
            pool_config=getattr(args, "pool_config", None),
            pool_profile=getattr(args, "pool_profile", None),
        )
        from .simple_cli import run_simple_command
        return run_simple_command(simple_args)

    from .auto_model import GoalStatus, save_goal_state
    from .autonomous import AutonomousController
    from .model import ControllerConfig
    from .backup_gate import BackupGate, BackupGateError
    from .delivery_manifest import DeliveryManifest
    from .execution_preview import ExecutionPreview
    from .handoff_packager import HandoffPackager
    from .monitor import setup_logging
    from .notification import init_notifier, get_notifier
    from .repo_profile import RepoProfiler
    from .task_classifier import TaskClassifier
    from .task_intake import build_task_contract, normalize_request
    from .verification_planner import VerificationCommand, VerificationPlan, VerificationPlanner
    from .workspace_manager import WorkspaceManager

    config = load_config(args.config, project_dir=_get_project_dir(args))
    init_notifier(config.notification)
    auto_config = _resolve_auto_config(args, config)
    phase_provider_overrides = parse_phase_provider_overrides(getattr(args, "phase_provider", []) or [])
    explicit_provider = getattr(args, "provider", "auto")
    if explicit_provider in {"claude", "codex"}:
        config.routing.default_provider = explicit_provider
        for phase_name in list(config.routing.phase_defaults):
            config.routing.phase_defaults[phase_name] = explicit_provider
    if phase_provider_overrides:
        config.routing.phase_defaults.update(phase_provider_overrides)

    # ── 统一前置健康检查：claude CLI 可用 + 工作目录存在 ──
    work_dir = Path(args.dir).resolve() if args.dir else Path.cwd()
    try:
        _preflight_check(work_dir, provider=_resolve_preflight_provider(args, config), config=config)
    except RuntimeError as exc:
        print(f"前置健康检查失败: {exc}", file=sys.stderr)
        return 1

    repo_root = work_dir
    request = normalize_request(args.goal or "", args.doc, repo_root)
    if not request.goal and not request.document_paths:
        print("必须提供目标描述或 --doc 文档路径。", file=sys.stderr)
        return 1

    repo_profile = RepoProfiler().profile(repo_root)
    classification = TaskClassifier().classify(request, repo_profile)
    contract = build_task_contract(request, repo_profile, classification)
    contract.architecture_mode = args.architecture_mode
    contract.requires_architecture_council = args.architecture_mode == "required"
    if args.architecture_pattern:
        contract.metadata["preferred_architecture_pattern"] = args.architecture_pattern
    contract.metadata["architecture_deliberation_mode"] = args.architecture_deliberation
    verification_plan = VerificationPlanner().plan(contract, repo_profile)
    preview_commands = list(args.quality_gate) if args.quality_gate is not None else [
        item.command for item in verification_plan.commands
    ]
    preview = ExecutionPreview.from_contract(contract, preview_commands)

    if config.preview.show_summary:
        preview_text = preview.render_text()
        if not args.yes and config.preview.require_confirmation and not config.preview.auto_confirm:
            if not _confirm_execution_preview(preview_text):
                print("已取消执行。", file=sys.stderr)
                return 1
        else:
            print(preview_text)

    workspace_session = WorkspaceManager(config.workspace).create_session(repo_root, contract)
    config.checkpoint.db_path = str(workspace_session.layout.state / "orchestrator_state.db")

    log_file = _resolve_log_file(args) or str(workspace_session.layout.logs / "run.jsonl")
    setup_logging(log_file)

    backup_manifest = None
    try:
        backup_manifest = BackupGate(config.backup).run(contract, workspace_session.layout, repo_root)
    except BackupGateError as exc:
        get_notifier().critical("备份门禁失败", error=str(exc))
        print(f"备份门禁失败: {exc}", file=sys.stderr)
        return 1

    # 需求收集：--skip-gather 优先于 --gather 和 config.toml
    gather_enabled = not args.skip_gather and (args.gather or config.requirement.enabled)

    # CLI 参数优先于 config.toml，未指定时使用 config.toml 的值
    gather_mode = args.gather_mode or config.requirement.gather_mode
    gather_max_rounds = args.gather_max_rounds if args.gather_max_rounds is not None else config.requirement.max_rounds

    resolved_verification_plan = (
        VerificationPlan(
            commands=[
                VerificationCommand(name="manual", command=cmd, cwd=str(workspace_session.layout.workspace))
                for cmd in args.quality_gate
            ],
            summary=f"{len(args.quality_gate)} 项手动指定验证",
        )
        if args.quality_gate is not None
        else verification_plan
    )

    # state 在 try 之前初始化，确保 finally 块可引用
    state = None  # type: ignore[assignment]
    with Store(config.checkpoint.db_path) as store:
        try:
            controller = AutonomousController(ControllerConfig(
                goal=contract.normalized_goal,
                working_dir=str(workspace_session.layout.workspace),
                config=config,
                auto_config=auto_config,
                store=store,
                log_file=log_file,
                resume=args.resume,
                gather_enabled=gather_enabled,
                gather_mode=gather_mode,
                gather_max_rounds=gather_max_rounds,
                gather_file=args.gather_file,
                task_contract=contract,
                repo_profile=repo_profile,
                runtime_layout=workspace_session.layout,
                verification_plan=resolved_verification_plan,
                backup_manifest=backup_manifest,
                explicit_mode=getattr(args, 'mode', 'auto') or 'auto',
                preferred_provider=getattr(args, "provider", "auto"),
                phase_provider_overrides=phase_provider_overrides,
            ))
            state = controller.execute()

            # 零成本失败定向自动重试（最多 3 次，指数退避 [2, 4, 8] 秒）
            for _retry_idx in range(ZERO_COST_MAX_RETRIES):
                decision = _classify_zero_cost_failure(state)
                if not decision.should_retry:
                    break

                retry_goal = contract.normalized_goal

                if decision.category == "preflight":
                    logger.warning(
                        '零成本失败（preflight, %d/%d），检查环境后重试...',
                        _retry_idx + 1, ZERO_COST_MAX_RETRIES,
                    )
                    env_ok = _quick_env_check()
                    if not env_ok:
                        logger.warning('环境检查未通过，放弃重试')
                        break
                elif decision.category == "decompose":
                    logger.warning(
                        '零成本失败（decompose, %d/%d），简化目标后重试...',
                        _retry_idx + 1, ZERO_COST_MAX_RETRIES,
                    )
                    retry_goal = decision.goal_modifier + contract.normalized_goal
                elif decision.category == "transient":
                    logger.warning(
                        '零成本失败（transient, %d/%d），直接重试...',
                        _retry_idx + 1, ZERO_COST_MAX_RETRIES,
                    )
                else:
                    logger.warning(
                        '零成本失败（%d/%d），重试...',
                        _retry_idx + 1, ZERO_COST_MAX_RETRIES,
                    )

                backoff = 2 ** (_retry_idx + 1)  # 指数退避：2, 4, 8 秒
                logger.info('等待 %d 秒后重试...', backoff)
                time.sleep(backoff)

                controller = AutonomousController(ControllerConfig(
                    goal=retry_goal,
                    working_dir=str(workspace_session.layout.workspace),
                    config=config,
                    auto_config=auto_config,
                    store=store,
                    log_file=log_file,
                    resume=False,
                    gather_enabled=gather_enabled,
                    gather_mode=gather_mode,
                    gather_max_rounds=gather_max_rounds,
                    gather_file=args.gather_file,
                    task_contract=contract,
                    repo_profile=repo_profile,
                    runtime_layout=workspace_session.layout,
                    verification_plan=resolved_verification_plan,
                    backup_manifest=backup_manifest,
                ))
                # 重试前再次验证环境，避免在 CLI 不可用时白白创建 controller
                if not _quick_env_check():
                    print("重试前环境检查失败：claude CLI 不可用。放弃重试。", file=sys.stderr)
                    break

                state = controller.execute()
                logger.info(
                    '零成本重试完成（%s, %d/%d），状态: %s',
                    decision.category, _retry_idx + 1, ZERO_COST_MAX_RETRIES, state.status.value,
                )

                # 成功则退出重试循环
                if state.status in (GoalStatus.CONVERGED, GoalStatus.PARTIAL_SUCCESS, GoalStatus.SAFE_STOP):
                    break

            # 统一设置运行时元数据（无论是否经过重试）
            state.runtime_dir = str(workspace_session.layout.root)
            state.workspace_dir = str(workspace_session.layout.workspace)
            state.handoff_dir = str(workspace_session.layout.handoff)
            state.branch_names = workspace_session.branch_names
            if backup_manifest:
                state.backup_summary = backup_manifest.summary
        finally:
            # 最终 flush：将运行时元数据持久化，即使 execute() 抛异常也必须执行
            if state is not None:
                try:
                    save_goal_state(state, controller._state_path)
                    logger.info("运行时元数据已 flush 到 %s", controller._state_path)
                except Exception as _flush_err:
                    logger.warning("最终 flush 失败（非致命）: %s", _flush_err)

                # Store 状态同步：将 goal_state.json 的最终状态同步到 Store.latest_run
                try:
                    from .autonomous import _goal_status_to_run_status
                    from .model import RunStatus as _RunStatus
                    _mapped_status = _goal_status_to_run_status(state.status)
                    _latest_run = store.get_latest_run()
                    if _latest_run:
                        store.update_run_status(
                            _latest_run.run_id, _mapped_status,
                            cost=state.total_cost_usd,
                        )
                        logger.info(
                            "Store 状态已同步: run_id=%s, status=%s, cost=%.4f",
                            _latest_run.run_id, _mapped_status.value,
                            state.total_cost_usd,
                        )
                except Exception as _sync_err:
                    logger.warning("Store 状态同步失败（非致命）: %s", _sync_err)
            else:
                logger.warning("execute() 未返回有效 state，跳过最终 flush")

    delivery_manifest = DeliveryManifest(
        run_id=state.goal_id,
        status=("success" if state.status in (GoalStatus.CONVERGED, GoalStatus.PARTIAL_SUCCESS) else state.status.value),
        branch_names=workspace_session.branch_names,
        worktree_paths=[str(path) for path in workspace_session.worktree_paths],
        backup_summary=state.backup_summary or (backup_manifest.summary if backup_manifest else "未涉及数据"),
        verification_summary=resolved_verification_plan.summary,
        recommended_merge_order=workspace_session.branch_names,
        suggested_pr_titles=[f"{contract.task_type.value}: {contract.normalized_goal[:50]}"],
    )
    handoff_result = HandoffPackager().write(workspace_session.layout, state, delivery_manifest)
    print(f"本地交付已生成: {handoff_result.markdown_path}")

    success = state.status in (GoalStatus.CONVERGED, GoalStatus.PARTIAL_SUCCESS)
    if not success:
        get_notifier().critical(
            "自动模式执行失败",
            goal=contract.normalized_goal,
            status=state.status.value,
            iterations=state.total_iterations,
            cost_usd=state.total_cost_usd,
        )
    else:
        get_notifier().info(
            "自动模式执行完成",
            goal=contract.normalized_goal,
            status=state.status.value,
            cost_usd=state.total_cost_usd,
        )

    return 0 if success else 1


def _cmd_self_improve(args: argparse.Namespace) -> int:
    import time
    from pathlib import Path

    from .auto_model import AutoConfig
    from .monitor import setup_logging
    from .notification import init_notifier, get_notifier
    from .self_improve import SelfImproveController

    config = load_config(args.config, project_dir=_get_project_dir(args))
    quality_gate_commands = _resolve_self_improve_quality_gates(args)
    log_file = _resolve_log_file(args)
    setup_logging(log_file)
    init_notifier(config.notification)

    auto_config = _resolve_self_improve_auto_config(args, config)

    # 编排器源码目录（用于自身改进时扫描）
    self_dir = str(Path(__file__).parent.parent)
    # 目标项目目录：如果 -d 指定了外部项目，内省和关键词提取应扫描该项目
    target_dir = str(Path(args.dir).resolve()) if args.dir != "." else self_dir
    orchestrator_dir = target_dir

    # 审批模式：--auto-approve 覆盖为 auto
    approval_mode = "auto" if args.auto_approve else args.approval_mode

    # --smart-discover 隐含 --discover
    discover_mode = args.discover or args.smart_discover
    smart_discover = args.smart_discover
    discovery_config = _resolve_discovery_config(config.discovery, args)
    config.discovery = discovery_config

    total_rounds = max(1, args.rounds)
    final_status = "completed"
    seed_proposals = (
        load_improvement_plan(args.plan_file, phase_filter=args.plan_phase)
        if getattr(args, "plan_file", None)
        else []
    )
    preferred_provider = getattr(args, "provider", "auto")
    phase_provider_overrides = parse_phase_provider_overrides(getattr(args, "phase_provider", []) or [])
    failure_history: list[dict] = []  # 跨轮次累积失败上下文
    shared_rate_limiter_state: dict | None = None  # 跨轮次限流状态
    shared_goal_history: list[str] | None = None  # 跨轮次目标历史
    prev_failure_categories: dict[str, int] | None = None  # 前轮 failure_categories
    prev_round_summary: dict | None = None  # 前轮 goal_state 运行摘要（score_trend/iterations/phases）
    shared_goal_outcomes: list[dict] | None = None  # 跨轮次目标执行结果（summary + success），用于 stalled goal 检测

    for round_num in range(1, total_rounds + 1):
        if total_rounds > 1:
            print(f"\n{'=' * 60}", file=sys.stderr)
            print(f"  第 {round_num}/{total_rounds} 轮自我迭代", file=sys.stderr)
            print(f"{'=' * 60}\n", file=sys.stderr)

        state = None
        controller = None
        try:
            with Store(config.checkpoint.db_path) as store:
                controller = SelfImproveController(
                    config=config,
                    auto_config=auto_config,
                    working_dir=args.dir,
                    orchestrator_dir=orchestrator_dir,
                    external_sources=args.source,
                    approval_mode=approval_mode,
                    approval_file=args.approval_file,
                    quality_gate_commands=quality_gate_commands,
                    store=store,
                    log_file=log_file,
                    skip_introspection=args.skip_introspection,
                    skip_external=args.skip_external,
                    discover_mode=discover_mode,
                    discover_keywords=args.keywords,
                    discover_rss_feeds=args.rss_feed,
                    discover_max_results=args.discover_max,
                    discover_search_template=args.search_template,
                    smart_discover=smart_discover,
                    failure_history=failure_history,
                    rate_limiter_state=shared_rate_limiter_state,
                    prev_failure_categories=prev_failure_categories,
                    goal_history=shared_goal_history,
                    prev_round_summary=prev_round_summary,
                    goal_outcomes=shared_goal_outcomes,
                    seed_proposals=seed_proposals,
                    preferred_provider=preferred_provider,
                    phase_provider_overrides=phase_provider_overrides,
                )

                state = controller.execute()
                shared_rate_limiter_state = controller.get_rate_limiter_state()
                shared_goal_history = list(controller._goal_history)
                shared_goal_outcomes = list(controller.goal_outcomes)

                # 提取前轮 failure_categories 供下一轮注入 prompt
                if controller.last_goal_state and controller.last_goal_state.failure_categories:
                    prev_failure_categories = dict(controller.last_goal_state.failure_categories)
                else:
                    prev_failure_categories = None

                # 提取前轮 goal_state 运行摘要供下一轮注入 prompt
                prev_round_summary = _extract_round_summary(controller.last_goal_state)

                # Store 状态同步：将最终执行状态同步到 Store.latest_run
                try:
                    from .model import RunStatus as _RunStatus
                    from .autonomous import _goal_status_to_run_status
                    _si_run_status = _RunStatus.COMPLETED if state.status == "completed" else _RunStatus.FAILED
                    _si_cost = getattr(state, 'total_cost_usd', 0.0) or 0.0
                    # 如果有 GoalState，使用更精确的映射
                    if controller and controller.last_goal_state:
                        _si_run_status = _goal_status_to_run_status(controller.last_goal_state.status)
                        _si_cost = controller.last_goal_state.total_cost_usd
                    _latest_run = store.get_latest_run()
                    if _latest_run:
                        store.update_run_status(
                            _latest_run.run_id, _si_run_status,
                            cost=_si_cost,
                        )
                        logger.info(
                            "Store 状态已同步 (self-improve): run_id=%s, status=%s, cost=%.4f",
                            _latest_run.run_id, _si_run_status.value, _si_cost,
                        )
                except Exception as _sync_err:
                    logger.warning("Store 状态同步失败（非致命）: %s", _sync_err)
        except KeyboardInterrupt:
            raise  # 用户中断不吞掉
        except Exception as e:
            # 单轮异常不杀死整个进程，记录后继续下一轮
            import traceback
            print(f"\n第 {round_num} 轮发生未预期异常: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            get_notifier().critical(
                "self-improve 轮次异常崩溃",
                round=round_num,
                total_rounds=total_rounds,
                error=str(e),
            )
            final_status = "failed"
            failure_history.append({
                "round": round_num,
                "status": "crash",
                "failure_stage": "unhandled_exception",
                "error_detail": str(e),
                "hint": "进程内异常，已自动恢复并继续下一轮",
            })
            if round_num < total_rounds:
                delay = max(args.round_delay, 10)  # 异常后至少等 10 秒
                print(f"休息 {delay} 秒后开始第 {round_num + 1} 轮...", file=sys.stderr)
                time.sleep(delay)
            continue

        if state is None or state.status != "completed":
            final_status = "failed"
            # 提取失败诊断信息，传递给下一轮
            if controller is not None and state is not None:
                failure_info = _extract_failure_info(round_num, state, controller)
                failure_history.append(failure_info)

            if round_num < total_rounds:
                print(f"\n第 {round_num} 轮失败，将在下一轮重试", file=sys.stderr)
                delay = args.round_delay
                if delay > 0:
                    print(f"休息 {delay} 秒后开始第 {round_num + 1} 轮...", file=sys.stderr)
                    time.sleep(delay)
                continue
            else:
                print(f"\n第 {round_num} 轮失败，已无剩余轮次", file=sys.stderr)
                break

        final_status = "completed"
        # 成功后清空失败历史（连续成功不需要携带旧失败上下文）
        failure_history.clear()
        # 轮间休息（最后一轮不休息）
        if round_num < total_rounds:
            delay = args.round_delay
            print(f"\n第 {round_num} 轮完成，休息 {delay} 秒...", file=sys.stderr)
            time.sleep(delay)

    return 0 if final_status == "completed" else 1


def _extract_failure_info(
    round_num: int,
    state,
    controller,
) -> dict:
    """从失败的 SelfImproveController 中提取诊断信息，供下一轮使用。"""
    info: dict = {"round": round_num, "status": state.status}

    goal_state = controller.last_goal_state
    if goal_state is None:
        # 执行阶段之前就失败了（如发现阶段无提案）
        info["failure_stage"] = "pre_execution"
        info["error_detail"] = "未进入执行阶段"
        info["hint"] = "可能是发现阶段未产生有效提案，或审批阶段全部拒绝"
        return info

    # 从 GoalState.diagnostics 提取最近的失败条目
    failed_diags = [d for d in goal_state.diagnostics if d.exit_status != "ok"]
    if failed_diags:
        last_fail = failed_diags[-1]
        info["failure_stage"] = last_fail.stage
        info["error_detail"] = last_fail.error_detail
        # 根据失败类型生成修正建议
        if "goal_parse" in last_fail.exit_status or "json" in last_fail.error_detail.lower():
            info["hint"] = "目标分解时 JSON 解析失败，请确保输出严格 JSON 格式，不要包含多余文本"
        elif "max_turns" in last_fail.error_detail.lower():
            info["hint"] = "Claude 达到最大轮次限制，请简化目标或增加 max_turns"
        else:
            info["hint"] = f"失败分类: {last_fail.exit_status}"
    else:
        info["failure_stage"] = goal_state.status.value
        info["error_detail"] = "执行未收敛"
        info["hint"] = "目标执行未达到收敛状态，可能需要更多迭代或简化目标"

    return info


def _extract_round_summary(goal_state) -> dict | None:
    """从 GoalState 提取运行摘要（score_trend/iterations/phases），供下一轮 prompt 注入。

    返回 None 表示无可用 goal_state（如发现阶段之前就失败了）。
    """
    if goal_state is None:
        return None

    summary: dict = {}

    # 从 iteration_history 提取分数趋势
    if hasattr(goal_state, "iteration_history") and goal_state.iteration_history:
        scores = [r.score for r in goal_state.iteration_history if hasattr(r, "score")]
        if scores:
            summary["score_trend"] = scores

    # 总迭代次数
    if hasattr(goal_state, "total_iterations") and goal_state.total_iterations:
        summary["total_iterations"] = goal_state.total_iterations

    # 阶段状态摘要
    if hasattr(goal_state, "phases") and goal_state.phases:
        phases_summary = []
        for p in goal_state.phases:
            ph_info = {
                "name": p.name if hasattr(p, "name") else "?",
                "status": p.status.value if hasattr(p, "status") and hasattr(p.status, "value") else str(getattr(p, "status", "?")),
            }
            if hasattr(p, "best_score") and p.best_score:
                ph_info["best_score"] = p.best_score
            phases_summary.append(ph_info)
        if phases_summary:
            summary["phases"] = phases_summary

    # 失败分类统计
    if hasattr(goal_state, "failure_categories") and goal_state.failure_categories:
        summary["failure_categories"] = dict(goal_state.failure_categories)

    return summary if summary else None


def _cmd_visualize(args: argparse.Namespace) -> int:
    config = load_config(args.config, project_dir=_get_project_dir(args))
    dag = load_dag(args.dag)

    # Load run status overlay if requested
    task_statuses: dict[str, str] = {}
    if args.run_id:
        with Store(config.checkpoint.db_path) as store:
            results = store.get_all_task_results(args.run_id)
            task_statuses = {tid: r.status.value for tid, r in results.items()}

    # Print DAG as text tree
    print(f"DAG: {dag.name} ({len(dag.tasks)} tasks, max_parallel={dag.max_parallel})")
    print()

    # Topological order
    from .scheduler import Scheduler
    sched = Scheduler(dag)
    order = sched.topological_order()

    for tid in order:
        node = dag.tasks[tid]
        status = task_statuses.get(tid, "")
        status_str = f" [{status}]" if status else ""
        deps = f" <- {', '.join(node.depends_on)}" if node.depends_on else ""
        print(f"  {tid}{status_str}{deps}")

    return 0


def main() -> int:
    # 注册 SIGTERM 信号处理器，使外部守护进程能触发优雅关闭
    if os.name != "nt":
        # Unix: 直接注册 SIGTERM handler
        signal.signal(signal.SIGTERM, _sigterm_handler)
    else:
        # Windows: SIGTERM 等同于 TerminateProcess，无法捕获
        # 但可以注册 SIGBREAK (Ctrl+Break) 作为替代
        try:
            signal.signal(signal.SIGBREAK, _sigterm_handler)  # type: ignore[attr-defined]
        except (AttributeError, OSError):
            pass  # 某些 Windows 环境不支持 SIGBREAK

    parser = _build_parser()
    args = parser.parse_args()
    try:
        args = _resolve_command_aliases(args)
    except ValueError as exc:
        parser.error(str(exc))

    # Pool Supervisor 入口：当指定了 --pool-config 且不是子进程时，启动 supervisor 模式
    from .pool_supervisor import pool_supervisor_required, run_pool_supervisor
    if pool_supervisor_required(args):
        return run_pool_supervisor(args, sys.argv[1:])

    from .simple_cli import run_simple_command

    handlers = {
        "run": _cmd_run,
        "resume": _cmd_resume,
        "retry-failed": _cmd_retry_failed,
        "status": _cmd_status,
        "visualize": _cmd_visualize,
        "auto": _cmd_auto,
        "simple": run_simple_command,
        "self-improve": _cmd_self_improve,
        "do": _cmd_auto,
        "runs": _cmd_status,
        "improve": _cmd_self_improve,
    }

    handler = handlers.get(args.command)
    if not handler:
        parser.print_help()
        return 1

    try:
        return handler(args)
    except KeyboardInterrupt:
        logger.info("用户中断 (KeyboardInterrupt)")
        return 130
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        try:
            from .notification import get_notifier
            get_notifier().critical(
                "CLI 顶层未捕获异常",
                command=args.command,
                error=str(e),
            )
        except Exception:
            pass
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
