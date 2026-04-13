"""Generic shell-based operation executor for structured playbook actions."""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from .command_runtime import normalize_python_command
from .model import TaskNode, TaskResult, TaskStatus


_DANGEROUS_COMMAND_PATTERNS = (
    r"(?i)\bgit\s+reset\s+--hard\b",
    r"(?i)\bgit\s+checkout\s+--\b",
    r"(?i)\bgit\s+clean\s+-",
    r"(?i)\brm\s+-rf\b",
    r"(?i)\brmdir\b",
    r"(?i)\bdel\s+/",
    r"(?i)\bshutdown\b",
    r"(?i)\breboot\b",
    r"(?i)\bformat\b",
    r"(?i)\bmkfs\b",
    r"(?i)\bdocker(?:-compose|\s+compose)\s+down\b",
)
_TEMPLATE_PATTERN = re.compile(r"\{(working_dir|task_id|command_id|timestamp|output)\}")
_OUTPUT_SNIPPET_LIMIT = 1200


class OperationExecutor:
    """Execute curated shell operations and return structured evidence."""

    def execute(
        self,
        task: TaskNode,
        prompt: str,
        claude_config: Any = None,
        limits: Any = None,
        budget_tracker: Any = None,
        working_dir: str | None = None,
        on_progress: Any = None,
        audit_logger: Any = None,
        rate_limiter: Any = None,
        **kwargs: Any,
    ) -> TaskResult:
        started_at = datetime.now()
        started_perf = time.perf_counter()
        config = task.executor_config or {}
        raw_commands = config.get("commands")
        if not isinstance(raw_commands, list) or not raw_commands:
            return TaskResult(
                task_id=task.id,
                status=TaskStatus.FAILED,
                error="operation executor requires executor_config.commands",
                started_at=started_at,
                finished_at=datetime.now(),
                duration_seconds=time.perf_counter() - started_perf,
                model_used="operation",
            )

        rollback_refs = _dedupe(config.get("rollback_refs", []))
        cutover_gates = _dedupe(config.get("cutover_gates", []))
        mode = str(config.get("mode", "generic") or "generic")
        base_dir = Path(working_dir or os.getcwd()).resolve()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        evidence_refs: list[str] = []
        satisfied_gates: list[str] = []
        command_results: list[dict[str, Any]] = []
        failed_commands: list[dict[str, Any]] = []

        for index, raw_command in enumerate(raw_commands, start=1):
            if not isinstance(raw_command, dict):
                continue

            command_id = str(raw_command.get("id") or f"command_{index}")
            context = {
                "working_dir": str(base_dir),
                "task_id": task.id,
                "command_id": command_id,
                "timestamp": timestamp,
                "output": "",
            }
            output_file = _resolve_output_file(raw_command.get("output_file"), base_dir, context)
            if output_file is not None:
                output_file.parent.mkdir(parents=True, exist_ok=True)
                context["output"] = str(output_file)

            command_text = _render_template(str(raw_command.get("command", "") or ""), context).strip()
            if not command_text:
                continue

            is_safe, unsafe_reason = _is_safe_command(command_text)
            if not is_safe:
                return TaskResult(
                    task_id=task.id,
                    status=TaskStatus.FAILED,
                    error=f"unsafe operation command blocked: {unsafe_reason}",
                    started_at=started_at,
                    finished_at=datetime.now(),
                    duration_seconds=time.perf_counter() - started_perf,
                    model_used="operation",
                )

            rendered_command = normalize_python_command(command_text)
            cwd_value = _resolve_cwd(raw_command.get("cwd"), base_dir, context)
            timeout = int(raw_command.get("timeout", task.timeout or 900) or 900)

            if on_progress is not None:
                on_progress(
                    "operation_start",
                    {
                        "task_id": task.id,
                        "command_id": command_id,
                        "command": rendered_command,
                        "cwd": cwd_value,
                    },
                )

            command_started = time.perf_counter()
            try:
                proc = subprocess.run(
                    rendered_command,
                    shell=True,
                    cwd=cwd_value,
                    capture_output=True,
                    text=True,
                    timeout=timeout,
                    encoding="utf-8",
                    errors="replace",
                )
                passed = proc.returncode == 0
                stdout = proc.stdout
                stderr = proc.stderr
                exit_code = proc.returncode
                timed_out = False
            except subprocess.TimeoutExpired:
                passed = False
                stdout = ""
                stderr = f"timeout after {timeout}s"
                exit_code = -1
                timed_out = True

            duration_seconds = round(time.perf_counter() - command_started, 3)
            if passed:
                evidence_refs.extend(_dedupe(raw_command.get("evidence_refs", [])))
                satisfied_gates.extend(_dedupe(raw_command.get("satisfies_gates", [])))

            command_result = {
                "id": command_id,
                "name": str(raw_command.get("name") or command_id),
                "command": rendered_command,
                "cwd": cwd_value,
                "passed": passed,
                "exit_code": exit_code,
                "timed_out": timed_out,
                "duration_seconds": duration_seconds,
                "stdout": _truncate_output(stdout),
                "stderr": _truncate_output(stderr),
            }
            if output_file is not None:
                command_result["output_file"] = str(output_file)
            command_results.append(command_result)
            if not passed:
                failed_commands.append(command_result)

            if on_progress is not None:
                on_progress(
                    "operation_complete",
                    {
                        "task_id": task.id,
                        "command_id": command_id,
                        "passed": passed,
                        "exit_code": exit_code,
                    },
                )

        unmet_cutover_gates = [gate for gate in cutover_gates if gate not in satisfied_gates]
        parsed_output = {
            "EvidenceRefs": _dedupe(evidence_refs),
            "RollbackRefs": rollback_refs,
            "UnmetCutoverGates": unmet_cutover_gates,
            "operation": {
                "mode": mode,
                "task_id": task.id,
                "prompt": prompt,
                "configured_cutover_gates": cutover_gates,
                "satisfied_gates": _dedupe(satisfied_gates),
                "command_results": command_results,
            },
        }

        output = "\n".join(
            [
                f"EvidenceRefs: {', '.join(parsed_output['EvidenceRefs']) or 'none'}",
                f"RollbackRefs: {', '.join(parsed_output['RollbackRefs']) or 'none'}",
                f"UnmetCutoverGates: {', '.join(parsed_output['UnmetCutoverGates']) or 'none'}",
            ]
        )
        finished_at = datetime.now()
        if failed_commands:
            first_failure = failed_commands[0]
            failure_message = (
                f"operation command failed: {first_failure['id']} "
                f"(exit={first_failure['exit_code']})"
            )
            return TaskResult(
                task_id=task.id,
                status=TaskStatus.FAILED,
                output=output,
                parsed_output=parsed_output,
                error=failure_message,
                started_at=started_at,
                finished_at=finished_at,
                duration_seconds=(finished_at - started_at).total_seconds(),
                model_used="operation",
            )
        return TaskResult(
            task_id=task.id,
            status=TaskStatus.SUCCESS,
            output=output,
            parsed_output=parsed_output,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=(finished_at - started_at).total_seconds(),
            model_used="operation",
        )


def _resolve_output_file(template: Any, base_dir: Path, context: dict[str, str]) -> Path | None:
    if not template:
        return None
    rendered = _render_template(str(template), context).strip()
    path = Path(rendered)
    if not path.is_absolute():
        path = base_dir / path
    return path.resolve()


def _resolve_cwd(template: Any, base_dir: Path, context: dict[str, str]) -> str:
    if not template:
        return str(base_dir)
    rendered = _render_template(str(template), context).strip()
    if not rendered:
        return str(base_dir)
    path = Path(rendered)
    if not path.is_absolute():
        path = base_dir / path
    return str(path.resolve())


def _render_template(template: str, context: dict[str, str]) -> str:
    return _TEMPLATE_PATTERN.sub(lambda match: context.get(match.group(1), match.group(0)), template)


def _is_safe_command(command: str) -> tuple[bool, str]:
    stripped = command.strip()
    if not stripped:
        return False, "empty command"
    for pattern in _DANGEROUS_COMMAND_PATTERNS:
        if re.search(pattern, stripped):
            return False, stripped
    return True, ""


def _truncate_output(text: str) -> str:
    if len(text) <= _OUTPUT_SNIPPET_LIMIT:
        return text
    return text[:_OUTPUT_SNIPPET_LIMIT] + f"\n... [truncated {len(text) - _OUTPUT_SNIPPET_LIMIT} chars]"


def _dedupe(values: Any) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in values or []:
        normalized = str(item).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result
