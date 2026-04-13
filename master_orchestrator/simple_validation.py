"""Validation pipeline for simple mode."""

from __future__ import annotations

import re
import shlex
import subprocess
import sys
from pathlib import Path

from .config import Config
from .model import TaskResult, TaskStatus
from .simple_isolation import PreparedItemWorkspace, _file_hash
from .simple_model import SimpleErrorCategory, SimpleItemType
from .simple_model import ValidationReport, ValidationStageResult


def _run_check_command(command: str, cwd: Path, *, target: str, timeout: int) -> tuple[bool, str]:
    rendered = command.format(target=target, cwd=str(cwd), python=sys.executable)
    use_shell = sys.platform == "win32"
    proc = subprocess.run(
        rendered if use_shell else shlex.split(rendered),
        cwd=cwd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=False,
        shell=use_shell,
    )
    detail = (proc.stdout + "\n" + proc.stderr).strip()
    return proc.returncode == 0, detail


def classify_simple_failure(result: TaskResult, report: ValidationReport) -> str:
    error_text = f"{result.error or ''}\n{result.output or ''}".lower()
    failure_code = report.failure_code or report.failure_reason
    if result.max_turns_exceeded:
        return SimpleErrorCategory.MAX_TURNS_EXCEEDED.value
    if "timed out" in error_text:
        return SimpleErrorCategory.TIMEOUT.value
    if "filename too long" in error_text or "path too long" in error_text:
        return SimpleErrorCategory.PATH_BUDGET_EXCEEDED.value
    if "auth_expired" in error_text or "authentication" in error_text:
        return SimpleErrorCategory.AUTH_EXPIRED.value
    if "429" in error_text or "rate limit" in error_text or "too many requests" in error_text:
        return SimpleErrorCategory.RATE_LIMITED.value

    if failure_code in {"target_missing_after_exec", "target file missing after execution"} or report.target_exists_after is False:
        return SimpleErrorCategory.TARGET_MISSING_AFTER_EXEC.value
    if failure_code in {"target_path_mismatch", "executor changed non-target files without touching target"}:
        return SimpleErrorCategory.TARGET_PATH_MISMATCH.value
    if failure_code in {"unauthorized_side_files", "unauthorized files modified"}:
        return SimpleErrorCategory.UNAUTHORIZED_SIDE_FILES.value
    if failure_code in {"no_change", "target_not_modified", "target file not modified"}:
        if report.unauthorized_changes:
            return SimpleErrorCategory.WRONG_FILE_CHANGED.value
        return SimpleErrorCategory.NO_CHANGE.value
    if failure_code in {"syntax_error", "syntax check failed"}:
        return SimpleErrorCategory.SYNTAX_ERROR.value
    if failure_code in {"pattern_missing", "required pattern missing"}:
        return SimpleErrorCategory.PATTERN_MISSING.value
    if failure_code in {"verify_command_failed", "verify command failed"}:
        return SimpleErrorCategory.VERIFY_COMMAND_FAILED.value
    if failure_code in {"copyback_conflict", "copy-back mismatch"}:
        return SimpleErrorCategory.COPYBACK_CONFLICT.value
    if failure_code in {"resource_exhausted", "resource throttled"}:
        return SimpleErrorCategory.RESOURCE_EXHAUSTED.value
    if report.failure_code == "path_budget_exceeded":
        return SimpleErrorCategory.PATH_BUDGET_EXCEEDED.value
    return SimpleErrorCategory.NON_RETRYABLE_EXEC_ERROR.value if result.status == TaskStatus.FAILED else SimpleErrorCategory.UNKNOWN.value


class SimpleValidationPipeline:
    def __init__(self, config: Config):
        self._config = config

    def validate(
        self,
        prepared: PreparedItemWorkspace,
        result: TaskResult,
        changed_files: list[str],
        *,
        copyback_ok: bool = True,
        copyback_reason: str = "",
        reused_target_changed_files: list[str] | None = None,
    ) -> ValidationReport:
        report = ValidationReport(passed=True, changed_files=changed_files)
        target_rel = prepared.item.target.replace("\\", "/")
        normalized_changed = [path.replace("\\", "/") for path in changed_files]
        reused_changed = [path.replace("\\", "/") for path in (reused_target_changed_files or [])]
        is_directory_shard = prepared.item.item_type == SimpleItemType.DIRECTORY_SHARD
        target_exists_after = prepared.target_path.exists() if not is_directory_shard else prepared.target_path.exists()
        target_hash_after = _file_hash(prepared.target_path) if prepared.target_path.is_file() else ""
        if is_directory_shard:
            target_prefix = target_rel.rstrip("/") + "/"
            target_changed_files = [path for path in (normalized_changed + reused_changed) if path == target_rel or path.startswith(target_prefix)]
            target_touched = bool(target_changed_files)
        else:
            target_changed_files = [path for path in (normalized_changed + reused_changed) if path == target_rel]
            target_touched = bool(target_changed_files)
            if not target_touched:
                target_touched = target_hash_after != prepared.target_baseline_hash
                if target_touched:
                    target_changed_files = [target_rel]
        report.target_touched = target_touched
        report.target_exists_after = target_exists_after
        report.target_content_changed = bool(target_changed_files) if is_directory_shard else target_hash_after != prepared.target_baseline_hash
        report.target_changed_files = target_changed_files
        allowed = {path.replace("\\", "/") for path in prepared.item.validation_profile.allowed_side_files}
        unauthorized: list[str] = []
        if is_directory_shard:
            target_prefix = target_rel.rstrip("/") + "/"
            unauthorized = [
                path for path in normalized_changed
                if not (path == target_rel or path.startswith(target_prefix) or path in allowed)
            ]
        else:
            allowed.add(target_rel)
            unauthorized = [path for path in normalized_changed if path not in allowed]
        report.unauthorized_changes = unauthorized

        def fail(stage: str, code: str, reason: str, details: str = "") -> None:
            report.passed = False
            report.failure_code = code
            report.failure_reason = reason
            report.stage_results.append(ValidationStageResult(stage, False, details))

        if result.status != TaskStatus.SUCCESS:
            fail("exit_code", "execution_failed", "execution failed", result.error or "")
            return report
        report.stage_results.append(ValidationStageResult("exit_code", True))

        if not is_directory_shard and not target_exists_after:
            fail("target_exists", "target_missing_after_exec", "target file missing after execution")
            return report
        if not is_directory_shard:
            report.stage_results.append(ValidationStageResult("target_exists", True))

        if not target_touched:
            if unauthorized:
                fail(
                    "target_touched",
                    "target_path_mismatch",
                    "executor changed non-target files without touching target",
                    ", ".join(unauthorized[:5]),
                )
            else:
                fail("target_touched", "no_change", "target file not modified")
            return report
        report.stage_results.append(ValidationStageResult("target_touched", True))

        if not is_directory_shard and target_hash_after == prepared.target_baseline_hash:
            fail("content_changed", "no_change", "target file not modified")
            return report
        if is_directory_shard and not target_changed_files:
            fail("content_changed", "no_change", "target file not modified")
            return report
        report.stage_results.append(ValidationStageResult("content_changed", True))

        if unauthorized:
            fail("unauthorized_changes", "unauthorized_side_files", "unauthorized files modified", ", ".join(unauthorized[:5]))
            return report
        report.stage_results.append(ValidationStageResult("unauthorized_changes", True))

        syntax_targets = target_changed_files if is_directory_shard else [target_rel]
        syntax_executed = False
        for rel in syntax_targets:
            target_path = (prepared.cwd / rel).resolve()
            if not target_path.exists() or not target_path.is_file():
                continue
            checker = self._config.simple.syntax_checkers.get(target_path.suffix.lower())
            if not checker:
                report.warnings.append(f"未配置 {target_path.suffix or '<no-ext>'} 语法检查器")
                continue
            syntax_executed = True
            ok, detail = _run_check_command(
                checker,
                prepared.cwd,
                target=str(target_path),
                timeout=self._config.simple.verify_command_timeout_seconds,
            )
            report.syntax_ok = ok if report.syntax_ok is not False else report.syntax_ok
            if not ok:
                fail("syntax_check", "syntax_error", "syntax check failed", detail[:500])
                return report
        if syntax_executed:
            report.syntax_ok = True
            report.stage_results.append(ValidationStageResult("syntax_check", True))

        if self._config.simple.pattern_checks_enabled:
            pattern_text = ""
            try:
                if is_directory_shard:
                    parts = []
                    for rel in target_changed_files:
                        path = (prepared.cwd / rel).resolve()
                        if path.exists() and path.is_file():
                            parts.append(path.read_text(encoding="utf-8", errors="replace"))
                    pattern_text = "\n".join(parts)
                else:
                    pattern_text = prepared.target_path.read_text(encoding="utf-8", errors="replace")
            except OSError as exc:
                fail("pattern_check", "pattern_missing", "required pattern missing", str(exc))
                return report
            for pattern in prepared.item.validation_profile.require_patterns:
                matched = False
                matched = bool(re.search(pattern, pattern_text, re.MULTILINE))
                report.pattern_matches[pattern] = matched
                if not matched:
                    fail("pattern_check", "pattern_missing", "required pattern missing", pattern)
                    return report
            if prepared.item.validation_profile.require_patterns:
                report.stage_results.append(ValidationStageResult("pattern_check", True))

        for command in prepared.item.validation_profile.verify_commands:
            ok, detail = _run_check_command(
                command,
                prepared.cwd,
                target=str(prepared.target_path),
                timeout=self._config.simple.verify_command_timeout_seconds,
            )
            report.command_results.append({"command": command, "passed": ok, "details": detail[:1000]})
            if not ok:
                fail("verify_command", "verify_command_failed", "verify command failed", detail[:500])
                return report
        if prepared.item.validation_profile.verify_commands:
            report.stage_results.append(ValidationStageResult("verify_command", True))

        if not copyback_ok:
            fail("copyback", "copyback_conflict", "copy-back mismatch", copyback_reason)
            return report
        report.stage_results.append(ValidationStageResult("copyback", True))
        return report
