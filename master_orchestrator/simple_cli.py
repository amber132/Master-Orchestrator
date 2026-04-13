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


def add_simple_subcommands(
    subparsers: argparse._SubParsersAction,
    add_log_args,
    *,
    hidden: bool = False,
) -> None:
    simple_p = subparsers.add_parser(
        "simple",
        help=argparse.SUPPRESS if hidden else "High-throughput work-item execution mode",
    )
    simple_sub = simple_p.add_subparsers(dest="simple_command", required=True)

    run_p = simple_sub.add_parser("run", help="Run simple mode tasks")
    _add_simple_run_args(run_p, add_log_args)

    scan_p = simple_sub.add_parser("scan", help="Preview simple mode tasks without executing")
    _add_simple_run_args(scan_p, add_log_args, include_instruction=False)
    scan_p.add_argument("instruction", nargs="?", default="", help="Instruction template")
    scan_p.add_argument("--json", action="store_true", dest="as_json", help="Output scan report as JSON")

    resume_p = simple_sub.add_parser("resume", help="Resume a simple run")
    resume_p.add_argument("--run-id", required=True, help="Simple run id")
    add_log_args(resume_p)

    retry_p = simple_sub.add_parser("retry", help="Retry failed items in a simple run")
    retry_p.add_argument("--run-id", required=True, help="Simple run id")
    add_log_args(retry_p)

    cancel_p = simple_sub.add_parser("cancel", help="Cancel a simple run and block unfinished items")
    cancel_p.add_argument("--run-id", required=True, help="Simple run id")
    cancel_p.add_argument("--reason", default="", help="Optional cancellation reason")
    cancel_p.add_argument("--no-kill-processes", action="store_false", dest="kill_processes", help="Do not terminate matched runner/exec processes")
    cancel_p.add_argument("--force-close", action="store_true", help="Mark the run cancelled even if matching processes are still alive")
    cancel_p.add_argument("--grace-seconds", type=float, default=5.0, help="Grace period before force-kill matched processes")
    cancel_p.add_argument("--kill-timeout-seconds", type=float, default=2.0, help="Timeout after force-kill before giving up")
    cancel_p.add_argument("--dry-run", action="store_true", help="Preview matching processes and state changes without modifying anything")
    cancel_p.add_argument("--json", action="store_true", dest="as_json", help="Output JSON")
    add_log_args(cancel_p)

    reconcile_p = simple_sub.add_parser("reconcile", help="Recover a stale simple run by resetting transient items")
    reconcile_p.add_argument("--run-id", required=True, help="Simple run id")
    reconcile_p.add_argument(
        "--item-status",
        choices=[SimpleItemStatus.READY.value, SimpleItemStatus.BLOCKED.value, SimpleItemStatus.FAILED.value, SimpleItemStatus.SKIPPED.value],
        default=SimpleItemStatus.READY.value,
        help="Status to apply to transient items while reconciling",
    )
    reconcile_p.add_argument(
        "--run-status",
        choices=[SimpleRunStatus.FAILED.value, SimpleRunStatus.CANCELLED.value],
        default=SimpleRunStatus.FAILED.value,
        help="Final run status after reconciliation",
    )
    reconcile_p.add_argument("--reason", default="", help="Optional reconciliation reason")
    reconcile_p.add_argument("--force", action="store_true", help="Reconcile even if matching processes are still alive")
    reconcile_p.add_argument("--dry-run", action="store_true", help="Preview reconciliation without modifying anything")
    reconcile_p.add_argument("--json", action="store_true", dest="as_json", help="Output JSON")
    add_log_args(reconcile_p)

    status_p = simple_sub.add_parser("status", help="Show simple run status")
    status_p.add_argument("--run-id", default=None, help="Simple run id (default: latest)")
    status_p.add_argument("--json", action="store_true", dest="as_json", help="Output JSON")

    manifest_p = simple_sub.add_parser("manifest", help="Print or export simple run manifest")
    manifest_p.add_argument("--run-id", default=None, help="Simple run id (default: latest)")
    manifest_p.add_argument("--out", default=None, help="Write manifest JSON to file")


def _add_simple_run_args(parser: argparse.ArgumentParser, add_log_args, *, include_instruction: bool = True) -> None:
    if include_instruction:
        parser.add_argument("instruction", nargs="?", default="", help="Instruction template for each work item")
    parser.add_argument("-d", "--dir", default=".", help="Project working directory")
    parser.add_argument("--provider", choices=["auto", "claude", "codex"], default="auto", help="Preferred execution provider")
    parser.add_argument("--files", nargs="+", action="append", default=[], help="Explicit files or directories")
    parser.add_argument("--glob", nargs="+", action="append", default=[], dest="globs", help="Glob patterns relative to project dir")
    parser.add_argument("--task-file", default=None, help="CSV or JSONL task file")
    parser.add_argument("--prompt-file", default=None, help="Read instruction template from file")
    parser.add_argument("--isolate", choices=["none", "copy", "worktree"], default=None, help="Isolation mode")
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


def run_simple_command(args: argparse.Namespace) -> int:
    config = load_config(args.config, project_dir=getattr(args, "dir", None))
    provider = getattr(args, "provider", "auto")
    if provider in {"claude", "codex"}:
        config.routing.default_provider = provider
        config.routing.phase_defaults["simple"] = provider
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
                    print("No simple runs found.", file=sys.stderr)
                    return 1
                payload = runner.status_payload(run.run_id)
                refreshed_run = store.get_simple_run(run.run_id)
                if refreshed_run is None:
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
            print(f"simple {args.simple_command} failed: {exc}", file=sys.stderr)
            return 1

    print(f"Unsupported simple command: {args.simple_command}", file=sys.stderr)
    return 1
