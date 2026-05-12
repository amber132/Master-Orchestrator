"""CLI integration for simple mode."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .config import load_config
from .monitor import setup_logging
from .notification import init_notifier
from .simple_control import SimpleRunController
from .simple_model import SimpleItemStatus, SimpleRunStatus
from .simple_runtime import SimpleTaskRunner
from .simple_scan import build_scan_report
from .simple_status import render_simple_status_json, render_simple_status_text
from .store import Store


def _localize_argparse(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """把 argparse 默认帮助文案本地化为中文。"""
    parser._positionals.title = "位置参数"
    parser._optionals.title = "选项"
    original_format_usage = parser.format_usage
    original_format_help = parser.format_help
    parser.format_usage = lambda: original_format_usage().replace("usage:", "用法:", 1)  # type: ignore[method-assign]
    parser.format_help = lambda: original_format_help().replace("usage:", "用法:", 1)  # type: ignore[method-assign]
    for action in parser._actions:
        if "-h" in action.option_strings and "--help" in action.option_strings:
            action.help = "显示帮助信息并退出"
    return parser


def add_simple_subcommands(
    subparsers: argparse._SubParsersAction,
    add_log_args,
    *,
    hidden: bool = False,
) -> None:
    simple_p = _localize_argparse(subparsers.add_parser(
        "simple",
        help=argparse.SUPPRESS if hidden else "高吞吐 work-item 执行模式",
    ))
    simple_sub = simple_p.add_subparsers(dest="simple_command", required=True)

    run_p = _localize_argparse(simple_sub.add_parser("run", help="运行 simple 模式任务"))
    _add_simple_run_args(run_p, add_log_args)

    scan_p = _localize_argparse(simple_sub.add_parser("scan", help="预览 simple 模式任务但不执行"))
    _add_simple_run_args(scan_p, add_log_args, include_instruction=False)
    scan_p.add_argument("instruction", nargs="?", default="", help="指令模板")
    scan_p.add_argument("--json", action="store_true", dest="as_json", help="以 JSON 输出扫描报告")

    resume_p = _localize_argparse(simple_sub.add_parser("resume", help="恢复 simple run"))
    resume_p.add_argument("--run-id", required=True, help="Simple run ID")
    add_log_args(resume_p)

    retry_p = _localize_argparse(simple_sub.add_parser("retry", help="重试 simple run 中失败的 item"))
    retry_p.add_argument("--run-id", required=True, help="Simple run ID")
    add_log_args(retry_p)

    cancel_p = _localize_argparse(simple_sub.add_parser("cancel", help="取消 simple run 并阻塞未完成 item"))
    cancel_p.add_argument("--run-id", required=True, help="Simple run ID")
    cancel_p.add_argument("--reason", default="", help="可选取消原因")
    cancel_p.add_argument("--no-kill-processes", action="store_false", dest="kill_processes", help="不要终止匹配的 runner/exec 进程")
    cancel_p.add_argument("--force-close", action="store_true", help="即使匹配进程仍存活，也把 run 标记为 cancelled")
    cancel_p.add_argument("--grace-seconds", type=float, default=5.0, help="强制结束匹配进程前的宽限秒数")
    cancel_p.add_argument("--kill-timeout-seconds", type=float, default=2.0, help="强制结束后等待的超时秒数")
    cancel_p.add_argument("--dry-run", action="store_true", help="只预览匹配进程和状态变更，不实际修改")
    cancel_p.add_argument("--json", action="store_true", dest="as_json", help="输出 JSON")
    add_log_args(cancel_p)

    reconcile_p = _localize_argparse(simple_sub.add_parser("reconcile", help="通过重置临时 item 恢复陈旧 simple run"))
    reconcile_p.add_argument("--run-id", required=True, help="Simple run ID")
    reconcile_p.add_argument(
        "--item-status",
        choices=[SimpleItemStatus.READY.value, SimpleItemStatus.BLOCKED.value, SimpleItemStatus.FAILED.value, SimpleItemStatus.SKIPPED.value],
        default=SimpleItemStatus.READY.value,
        help="reconcile 时应用到临时 item 的状态",
    )
    reconcile_p.add_argument(
        "--run-status",
        choices=[SimpleRunStatus.FAILED.value, SimpleRunStatus.CANCELLED.value],
        default=SimpleRunStatus.FAILED.value,
        help="reconcile 后的最终 run 状态",
    )
    reconcile_p.add_argument("--reason", default="", help="可选 reconcile 原因")
    reconcile_p.add_argument("--force", action="store_true", help="即使匹配进程仍存活也执行 reconcile")
    reconcile_p.add_argument("--dry-run", action="store_true", help="只预览 reconcile，不实际修改")
    reconcile_p.add_argument("--json", action="store_true", dest="as_json", help="输出 JSON")
    add_log_args(reconcile_p)

    status_p = _localize_argparse(simple_sub.add_parser("status", help="显示 simple run 状态"))
    status_p.add_argument("--run-id", default=None, help="Simple run ID（默认 latest）")
    status_p.add_argument("--json", action="store_true", dest="as_json", help="输出 JSON")

    manifest_p = _localize_argparse(simple_sub.add_parser("manifest", help="打印或导出 simple run manifest"))
    manifest_p.add_argument("--run-id", default=None, help="Simple run ID（默认 latest）")
    manifest_p.add_argument("--out", default=None, help="把 manifest JSON 写入文件")


def _add_simple_run_args(parser: argparse.ArgumentParser, add_log_args, *, include_instruction: bool = True) -> None:
    if include_instruction:
        parser.add_argument("instruction", nargs="?", default="", help="每个 work item 的指令模板")
    parser.add_argument("-d", "--dir", default=".", help="项目工作目录")
    parser.add_argument("--provider", choices=["auto", "claude", "codex"], default="auto", help="首选执行 provider")
    parser.add_argument("--files", nargs="+", action="append", default=[], help="显式指定文件或目录")
    parser.add_argument("--glob", nargs="+", action="append", default=[], dest="globs", help="相对于项目目录的 glob 匹配模式")
    parser.add_argument("--task-file", default=None, help="CSV 或 JSONL 任务文件")
    parser.add_argument("--prompt-file", default=None, help="从文件读取指令模板")
    parser.add_argument("--isolate", choices=["none", "copy", "worktree"], default=None, help="隔离模式")
    add_log_args(parser)


def normalize_multi_value_args(values: object) -> list[str]:
    if not values:
        return []
    if isinstance(values, str):
        return [values]
    normalized: list[str] = []
    for value in values:
        if not value:
            continue
        if isinstance(value, str):
            normalized.append(value)
            continue
        normalized.extend(str(item) for item in value if item)
    return normalized


def resolve_instruction(args: argparse.Namespace) -> str:
    instruction = getattr(args, "instruction", "") or ""
    if getattr(args, "prompt_file", None):
        instruction = Path(args.prompt_file).read_text(encoding="utf-8")
    return instruction.strip()


def _render_simple_control_result(payload: dict[str, object]) -> str:
    lines = [
        f"run_id: {payload['run_id']}",
        f"action: {payload['action']}",
        f"dry_run: {payload['dry_run']}",
    ]
    reason = str(payload.get("reason", "") or "").strip()
    if reason:
        lines.append(f"reason: {reason}")
    requested_item_status = payload.get("requested_item_status")
    if requested_item_status:
        lines.append(f"item_status: {requested_item_status}")
    requested_run_status = payload.get("requested_run_status")
    if requested_run_status:
        lines.append(f"run_status: {requested_run_status}")

    before_counts = payload.get("before_counts")
    if before_counts is not None:
        lines.append("before_counts:")
        lines.append(json.dumps(before_counts, ensure_ascii=False, indent=2))
    after_counts = payload.get("after_counts")
    if after_counts is not None:
        lines.append("after_counts:")
        lines.append(json.dumps(after_counts, ensure_ascii=False, indent=2))

    processes = payload.get("processes")
    if isinstance(processes, dict):
        runner_pids = processes.get("runner_pids", [])
        exec_pids = processes.get("exec_pids", [])
        lines.append(f"runner_pids: {runner_pids}")
        lines.append(f"exec_pids: {exec_pids}")

    termination = payload.get("termination")
    if isinstance(termination, dict):
        lines.append("termination:")
        lines.append(json.dumps(termination, ensure_ascii=False, indent=2))

    return "\n".join(lines)


def _print_simple_json_error(command: str, error: str) -> None:
    print(json.dumps({"command": command, "error": error}, ensure_ascii=False, indent=2))


def _resolve_simple_preflight_provider(args: argparse.Namespace, config) -> str:
    provider = getattr(args, "provider", "auto")
    if provider in {"claude", "codex"}:
        return provider
    return config.routing.phase_defaults.get("simple", config.routing.default_provider)


def _preflight_simple_provider(args: argparse.Namespace, config) -> None:
    provider = _resolve_simple_preflight_provider(args, config)
    if provider not in {"claude", "codex"}:
        return
    from .cli import _preflight_check
    _preflight_check(Path(getattr(args, "dir", ".")).resolve(), provider=provider, config=config)


def run_simple_command(args: argparse.Namespace) -> int:
    config = load_config(args.config, project_dir=getattr(args, "dir", None))
    provider = getattr(args, "provider", "auto")
    if provider in {"claude", "codex"}:
        config.routing.default_provider = provider
        config.routing.phase_defaults["simple"] = provider
    if args.simple_command == "run":
        _preflight_simple_provider(args, config)
    init_notifier(config.notification)
    log_file = None
    if hasattr(args, "log_file") or hasattr(args, "log_dir"):
        from .cli import _resolve_log_file
        log_file = _resolve_log_file(args)
    if log_file:
        setup_logging(log_file)

    with Store(config.checkpoint.db_path) as store:
        runner = SimpleTaskRunner(
            config,
            store,
            working_dir=getattr(args, "dir", "."),
            log_file=log_file,
            preferred_provider=provider,
        )
        try:
            if args.simple_command == "scan":
                instruction = resolve_instruction(args)
                load_result = runner.load_items(
                    instruction,
                    files=normalize_multi_value_args(args.files),
                    globs=normalize_multi_value_args(args.globs),
                    task_file=args.task_file,
                )
                report = build_scan_report(load_result)
                if args.as_json:
                    print(json.dumps(report.to_dict(), indent=2, ensure_ascii=False))
                else:
                    print(report.render_text())
                return 0

            if args.simple_command == "run":
                instruction = resolve_instruction(args)
                if not instruction:
                    print("simple run 需要 instruction 或 --prompt-file", file=sys.stderr)
                    return 1
                run, payload = runner.run(
                    instruction,
                    files=normalize_multi_value_args(args.files),
                    globs=normalize_multi_value_args(args.globs),
                    task_file=args.task_file,
                    isolation_mode=args.isolate,
                )
                print(render_simple_status_text(run, payload.get("manifest"), runner._simple_store.load_items(run.run_id), payload.get("recent_events", [])))
                return 0 if run.status.value in {"completed", "partial_success"} else 1

            if args.simple_command in {"resume", "retry"}:
                if args.simple_command == "resume":
                    run, payload = runner.resume(args.run_id, retry_failed=False)
                else:
                    run, payload = runner.resume(args.run_id, retry_failed=True)
                print(render_simple_status_text(run, payload.get("manifest"), runner._simple_store.load_items(run.run_id), payload.get("recent_events", [])))
                return 0 if run.status.value in {"completed", "partial_success"} else 1

            if args.simple_command == "status":
                run = store.get_simple_run(args.run_id) if args.run_id else store.get_latest_simple_run()
                if run is None:
                    if args.as_json:
                        _print_simple_json_error("status", "No simple runs found.")
                    else:
                        print("No simple runs found.", file=sys.stderr)
                    return 1
                payload = runner.status_payload(run.run_id)
                refreshed_run = store.get_simple_run(run.run_id)
                if refreshed_run is None:
                    if args.as_json:
                        _print_simple_json_error("status", "No simple runs found.")
                    else:
                        print("No simple runs found.", file=sys.stderr)
                    return 1
                items = store.get_simple_items(refreshed_run.run_id)
                manifest = store.get_simple_manifest(refreshed_run.run_id)
                events = store.get_simple_events(refreshed_run.run_id, limit=20)
                if args.as_json:
                    print(render_simple_status_json(refreshed_run, manifest, items, events))
                else:
                    print(render_simple_status_text(refreshed_run, payload.get("manifest"), items, payload.get("recent_events", [])))
                return 0

            if args.simple_command == "manifest":
                run = store.get_simple_run(args.run_id) if args.run_id else store.get_latest_simple_run()
                if run is None:
                    print("No simple runs found.", file=sys.stderr)
                    return 1
                manifest = store.get_simple_manifest(run.run_id)
                if manifest is None:
                    print(f"Simple run '{run.run_id}' has no manifest.", file=sys.stderr)
                    return 1
                payload = json.dumps(manifest, indent=2, ensure_ascii=False)
                if args.out:
                    Path(args.out).write_text(payload, encoding="utf-8")
                else:
                    print(payload)
                return 0

            if args.simple_command in {"cancel", "reconcile"}:
                controller = SimpleRunController(store, config_path=getattr(args, "config", None))
                if args.simple_command == "cancel":
                    payload = controller.cancel(
                        args.run_id,
                        reason=args.reason,
                        kill_processes=args.kill_processes,
                        force_close=args.force_close,
                        grace_seconds=args.grace_seconds,
                        kill_timeout_seconds=args.kill_timeout_seconds,
                        dry_run=args.dry_run,
                    )
                else:
                    payload = controller.reconcile(
                        args.run_id,
                        item_status=SimpleItemStatus(args.item_status),
                        run_status=SimpleRunStatus(args.run_status),
                        reason=args.reason,
                        force=args.force,
                        dry_run=args.dry_run,
                    )
                if args.as_json:
                    print(json.dumps(payload, indent=2, ensure_ascii=False))
                else:
                    print(_render_simple_control_result(payload))
                return 0
        except Exception as exc:
            if getattr(args, "as_json", False):
                _print_simple_json_error(args.simple_command, str(exc))
            else:
                print(f"simple {args.simple_command} failed: {exc}", file=sys.stderr)
            return 1

    print(f"Unsupported simple command: {args.simple_command}", file=sys.stderr)
    return 1
