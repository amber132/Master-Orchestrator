"""Zero-touch supervision loop for simple mode."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .config import Config
from .simple_control import SimpleRunController
from .simple_loader import load_simple_work_items
from .simple_model import SimpleItemStatus, SimpleRun, SimpleRunStatus, SimpleWorkItem
from .simple_runtime import SimpleTaskRunner
from .simple_semantic_validation import looks_like_annotation_task, run_semantic_validator
from .store import Store

DEFAULT_SOURCE_EXTENSIONS = (
    ".py",
    ".pyi",
    ".js",
    ".ts",
    ".tsx",
    ".jsx",
    ".sh",
    ".ps1",
    ".yml",
    ".yaml",
    ".toml",
)
DEFAULT_VALIDATOR_EXTENSIONS = (
    ".py",
    ".pyi",
    ".js",
    ".ts",
    ".tsx",
    ".jsx",
    ".html",
    ".htm",
)
DEFAULT_EXCLUDE_PARTS = {
    ".git",
    ".hg",
    ".svn",
    ".venv",
    "venv",
    "node_modules",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".next",
    "dist",
    "build",
    "coverage",
}
ACTIVE_RUN_STATUSES = {
    SimpleRunStatus.QUEUED,
    SimpleRunStatus.SCANNING,
    SimpleRunStatus.READY,
    SimpleRunStatus.RUNNING,
    SimpleRunStatus.DRAINING,
}
_CJK_RANGES = (
    (0x3400, 0x4DBF),
    (0x4E00, 0x9FFF),
)


@dataclass
class SimpleSuperviseOptions:
    instruction: str
    source_repo: Path
    project_root: Path
    config_path: str | None
    prompt_file: Path
    files: list[str]
    globs: list[str]
    task_file: str | None
    isolate: str | None
    run_root: Path
    copy_dir: Path | None
    max_audit_cycles: int
    auto_scan: bool
    source_extensions: tuple[str, ...]
    audit_validators: tuple[str, ...]
    require_cjk_audit: bool
    validator_extensions: tuple[str, ...]


def _contains_cjk(text: str) -> bool:
    for char in text:
        codepoint = ord(char)
        for start, end in _CJK_RANGES:
            if start <= codepoint <= end:
                return True
    return False


def _stable_run_slug(source_repo: Path) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{source_repo.name.lower()}_{timestamp}"


def _normalize_multi_value_args(values: object) -> list[str]:
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


def _normalize_target(path: Path, repo_root: Path) -> str:
    return str(path.relative_to(repo_root)).replace("\\", "/")


def _scan_repo_targets(
    repo_root: Path,
    *,
    source_extensions: tuple[str, ...],
    exclude_parts: set[str] | None = None,
) -> list[str]:
    excluded = exclude_parts or DEFAULT_EXCLUDE_PARTS
    targets: list[str] = []
    for path in repo_root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(repo_root)
        if any(part in excluded for part in rel.parts):
            continue
        if path.suffix.lower() not in source_extensions:
            continue
        targets.append(str(rel).replace("\\", "/"))
    return sorted(set(targets))


def _item_to_task_row(item: SimpleWorkItem, default_instruction: str) -> dict[str, Any]:
    row: dict[str, Any] = {"target": item.target}
    if item.instruction.strip() and item.instruction.strip() != default_instruction.strip():
        row["instruction"] = item.instruction
    if item.bucket:
        row["bucket"] = item.bucket
    if item.priority:
        row["priority"] = item.priority
    if item.timeout_seconds:
        row["timeout_seconds"] = item.timeout_seconds
    if item.attempt_state.max_attempts:
        row["max_attempts"] = item.attempt_state.max_attempts
    if item.validation_profile.verify_commands:
        row["verify_commands"] = list(item.validation_profile.verify_commands)
    if item.validation_profile.require_patterns:
        row["require_patterns"] = list(item.validation_profile.require_patterns)
    if item.validation_profile.semantic_validators:
        row["semantic_validators"] = list(item.validation_profile.semantic_validators)
    if item.validation_profile.allowed_side_files:
        row["allowed_side_files"] = list(item.validation_profile.allowed_side_files)
    if item.metadata:
        row["metadata"] = dict(item.metadata)
    return row


def build_supervision_seed_rows(
    repo_root: Path,
    instruction: str,
    *,
    files: list[str],
    globs: list[str],
    task_file: str | None,
    bucket_strategy: str,
    default_timeout: int,
    default_max_attempts: int,
    validate_task_file_targets: bool,
    auto_scan: bool,
    source_extensions: tuple[str, ...],
) -> dict[str, dict[str, Any]]:
    rows_by_target: dict[str, dict[str, Any]] = {}
    if files or globs or task_file:
        load_result = load_simple_work_items(
            repo_root,
            instruction,
            files=files,
            globs=globs,
            task_file=task_file,
            bucket_strategy=bucket_strategy,
            default_timeout=default_timeout,
            default_max_attempts=default_max_attempts,
            validate_task_file_targets=validate_task_file_targets,
        )
        for item in load_result.items:
            rows_by_target.setdefault(item.target, _item_to_task_row(item, instruction))
    if auto_scan:
        for target in _scan_repo_targets(repo_root, source_extensions=source_extensions):
            rows_by_target.setdefault(target, {"target": target})
    return dict(sorted(rows_by_target.items(), key=lambda entry: entry[0]))


def audit_repo_targets(
    repo_root: Path,
    *,
    targets: list[str],
    require_cjk_audit: bool,
    audit_validators: tuple[str, ...],
    validator_extensions: tuple[str, ...],
) -> tuple[list[str], dict[str, Any]]:
    unresolved: set[str] = set()
    missing_targets: list[str] = []
    no_cjk_targets: list[str] = []
    validator_failures: list[dict[str, Any]] = []
    validator_exts = {suffix.lower() for suffix in validator_extensions}

    for target in targets:
        path = (repo_root / target).resolve()
        if not path.exists() or not path.is_file():
            missing_targets.append(target)
            unresolved.add(target)
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        if require_cjk_audit and not _contains_cjk(text):
            no_cjk_targets.append(target)
            unresolved.add(target)
        if audit_validators and path.suffix.lower() in validator_exts:
            target_issues: dict[str, list[str]] = {}
            for validator in audit_validators:
                ok, issues, _ = run_semantic_validator(validator, path, text)
                if not ok:
                    target_issues[validator] = issues[:20]
            if target_issues:
                validator_failures.append({"target": target, "issues": target_issues})
                unresolved.add(target)

    report = {
        "targets_total": len(targets),
        "unresolved_count": len(unresolved),
        "missing_target_count": len(missing_targets),
        "no_cjk_count": len(no_cjk_targets),
        "validator_failure_count": len(validator_failures),
        "missing_targets": missing_targets[:500],
        "no_cjk_targets": no_cjk_targets[:500],
        "validator_failures": validator_failures[:500],
    }
    return sorted(unresolved), report


def export_working_copy(source_repo: Path, destination: Path) -> dict[str, Any]:
    source_repo = source_repo.resolve()
    destination = destination.resolve()
    if destination.exists() and any(destination.iterdir()):
        return {"copied": False, "reused": True, "method": "reuse", "path": str(destination)}
    destination.mkdir(parents=True, exist_ok=True)

    if (source_repo / ".git").exists():
        archive = subprocess.Popen(
            ["git", "-C", str(source_repo), "archive", "--format=tar", "HEAD"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        extract = subprocess.run(
            ["tar", "-xf", "-", "-C", str(destination)],
            stdin=archive.stdout,
            capture_output=True,
            check=False,
        )
        if archive.stdout is not None:
            archive.stdout.close()
        archive_stderr = archive.communicate()[1]
        if archive.returncode == 0 and extract.returncode == 0:
            return {"copied": True, "reused": False, "method": "git-archive", "path": str(destination)}
        shutil.rmtree(destination, ignore_errors=True)
        destination.mkdir(parents=True, exist_ok=True)
        error_text = "\n".join(
            part.decode("utf-8", errors="ignore").strip()
            for part in (archive_stderr, extract.stderr)
            if part
        ).strip()
    else:
        error_text = ""

    shutil.copytree(
        source_repo,
        destination,
        dirs_exist_ok=True,
        ignore=shutil.ignore_patterns(
            "__pycache__",
            ".pytest_cache",
            ".mypy_cache",
            "node_modules",
            "dist",
            "build",
            ".next",
            "coverage",
        ),
    )
    result = {"copied": True, "reused": False, "method": "copytree", "path": str(destination)}
    if error_text:
        result["fallback_reason"] = error_text
    return result


class SimpleSupervisor:
    def __init__(self, config: Config, options: SimpleSuperviseOptions):
        self._config = config
        self._options = options
        self._paths = {
            "root": options.run_root,
            "logs": options.run_root / "logs",
            "task_files": options.run_root / "task_files",
            "audit": options.run_root / "audit",
        }
        for path in self._paths.values():
            path.mkdir(parents=True, exist_ok=True)

    def run(self) -> dict[str, Any]:
        repo_root, copy_result = self._resolve_repo_root()
        seed_rows = build_supervision_seed_rows(
            repo_root,
            self._options.instruction,
            files=self._options.files,
            globs=self._options.globs,
            task_file=self._options.task_file,
            bucket_strategy=self._config.simple.bucket_strategy,
            default_timeout=self._config.simple.default_timeout_seconds,
            default_max_attempts=self._config.simple.default_max_attempts,
            validate_task_file_targets=self._config.simple.validate_task_file_targets,
            auto_scan=self._options.auto_scan,
            source_extensions=self._options.source_extensions,
        )
        if not seed_rows:
            raise ValueError("simple supervise 未生成任何 work item")

        self._options.prompt_file.write_text(self._options.instruction, encoding="utf-8")
        self._write_json(self._paths["root"] / "supervise_state.json", {
            "repo_root": str(repo_root),
            "source_repo": str(self._options.source_repo),
            "copy_result": copy_result,
            "initial_targets": len(seed_rows),
            "instruction_file": str(self._options.prompt_file),
        })

        next_targets = sorted(seed_rows)
        final_summary: dict[str, Any] = {
            "status": "exhausted",
            "repo_root": str(repo_root),
            "source_repo": str(self._options.source_repo),
            "copy_result": copy_result,
            "initial_target_count": len(seed_rows),
            "cycles": [],
        }

        for cycle in range(1, self._options.max_audit_cycles + 1):
            cycle_task_file = self._paths["task_files"] / f"cycle_{cycle:02d}.jsonl"
            self._write_task_file(cycle_task_file, [seed_rows.get(target, {"target": target}) for target in next_targets])
            self._recover_stale_runs(repo_root)
            exit_code, run_log = self._launch_cycle(repo_root, cycle, cycle_task_file)
            run_summary = self._latest_run_summary(repo_root, exit_code)
            audit_targets = sorted(set(seed_rows) | set(_scan_repo_targets(repo_root, source_extensions=self._options.source_extensions)))
            unresolved, audit_report = audit_repo_targets(
                repo_root,
                targets=audit_targets,
                require_cjk_audit=self._options.require_cjk_audit,
                audit_validators=self._options.audit_validators,
                validator_extensions=self._options.validator_extensions,
            )
            audit_report.update({
                "cycle": cycle,
                "task_file": str(cycle_task_file),
                "log_file": str(run_log),
                "run_summary": run_summary,
            })
            self._write_json(self._paths["audit"] / f"cycle_{cycle:02d}.json", audit_report)
            final_summary["cycles"].append(audit_report)

            if not unresolved:
                final_summary.update({
                    "status": "completed",
                    "completed_cycle": cycle,
                    "final_run_id": run_summary.get("run_id", ""),
                    "final_unresolved_count": 0,
                })
                self._write_json(self._paths["root"] / "final_summary.json", final_summary)
                return final_summary

            next_targets = unresolved
            for target in unresolved:
                seed_rows.setdefault(target, {"target": target})

        final_summary.update({
            "status": "exhausted",
            "completed_cycle": self._options.max_audit_cycles,
            "final_unresolved_count": len(next_targets),
            "unresolved_targets": next_targets[:2000],
        })
        self._write_json(self._paths["root"] / "final_summary.json", final_summary)
        return final_summary

    def _resolve_repo_root(self) -> tuple[Path, dict[str, Any]]:
        if self._options.copy_dir is None:
            return self._options.source_repo.resolve(), {"copied": False, "reused": False, "method": "in-place"}
        copy_result = export_working_copy(self._options.source_repo, self._options.copy_dir)
        return self._options.copy_dir.resolve(), copy_result

    def _recover_stale_runs(self, repo_root: Path) -> None:
        with Store(self._config.checkpoint.db_path) as store:
            runner = SimpleTaskRunner(self._config, store, working_dir=str(repo_root))
            runner.recover_stale_runs()

    def _launch_cycle(self, repo_root: Path, cycle: int, task_file: Path) -> tuple[int, Path]:
        run_log = self._paths["logs"] / f"cycle_{cycle:02d}.run.log"
        cmd = [sys.executable, "-u", "-m", "claude_orchestrator.cli"]
        if self._options.config_path:
            cmd.extend(["-c", self._options.config_path])
        cmd.extend([
            "simple",
            "run",
            "-d",
            str(repo_root),
            "--task-file",
            str(task_file),
            "--prompt-file",
            str(self._options.prompt_file),
        ])
        if self._options.isolate:
            cmd.extend(["--isolate", self._options.isolate])
        with run_log.open("w", encoding="utf-8") as fh:
            process = subprocess.run(
                cmd,
                cwd=self._options.project_root,
                stdout=fh,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
        return process.returncode, run_log

    def _latest_run_summary(self, repo_root: Path, exit_code: int) -> dict[str, Any]:
        with Store(self._config.checkpoint.db_path) as store:
            run = store.get_latest_simple_run_for_working_dir(str(repo_root))
            if run is None:
                return {"exit_code": exit_code, "status": "missing"}
            if run.status in ACTIVE_RUN_STATUSES:
                controller = SimpleRunController(store, config_path=self._options.config_path)
                controller.reconcile(
                    run.run_id,
                    item_status=SimpleItemStatus.READY,
                    run_status=SimpleRunStatus.FAILED,
                    reason="simple supervise detected active run after child exit",
                    force=True,
                )
                run = store.get_simple_run(run.run_id) or run
            counts = store.get_simple_item_counts(run.run_id)
            return {
                "exit_code": exit_code,
                "run_id": run.run_id,
                "status": run.status.value,
                "counts": counts,
                "started_at": run.started_at.isoformat() if run.started_at else None,
                "finished_at": run.finished_at.isoformat() if run.finished_at else None,
            }

    @staticmethod
    def _write_json(path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    @staticmethod
    def _write_task_file(path: Path, rows: list[dict[str, Any]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def build_supervise_options(args, config: Config, *, project_root: Path) -> SimpleSuperviseOptions:
    instruction = str(getattr(args, "instruction", "") or "").strip()
    prompt_file_arg = getattr(args, "prompt_file", None)
    if not instruction and prompt_file_arg:
        instruction = Path(prompt_file_arg).read_text(encoding="utf-8").strip()
    if not instruction:
        raise ValueError("simple supervise 需要 instruction 或 --prompt-file")
    source_repo = Path(args.dir).resolve()
    run_root = Path(args.run_root).resolve() if args.run_root else (
        Path(config.simple.manifest_dir).resolve() / "supervise" / _stable_run_slug(source_repo)
    )
    prompt_file = run_root / "prompt.md"
    source_extensions = tuple(
        dict.fromkeys(
            suffix if suffix.startswith(".") else f".{suffix}"
            for suffix in (args.scan_ext or DEFAULT_SOURCE_EXTENSIONS)
        )
    )
    annotation_task = looks_like_annotation_task(
        instruction,
        {"annotation_task": True} if args.audit_validator else None,
    )
    explicit_validators = tuple(dict.fromkeys(args.audit_validator or []))
    audit_validators = explicit_validators
    if not audit_validators and annotation_task:
        defaults = tuple(dict.fromkeys(config.simple.default_semantic_validators))
        merged = list(defaults or ("zh_annotation_coverage",))
        if any(name in {"zh_annotation_coverage", "zh_annotation_quality"} for name in merged):
            merged.append("zh_annotation_quality")
        audit_validators = tuple(dict.fromkeys(merged))
    require_cjk_audit = annotation_task and not args.no_cjk_audit
    return SimpleSuperviseOptions(
        instruction=instruction,
        source_repo=source_repo,
        project_root=project_root.resolve(),
        config_path=args.config,
        prompt_file=prompt_file,
        files=_normalize_multi_value_args(getattr(args, "files", [])),
        globs=_normalize_multi_value_args(getattr(args, "globs", [])),
        task_file=args.task_file,
        isolate=args.isolate,
        run_root=run_root,
        copy_dir=Path(args.copy_dir).resolve() if args.copy_dir else None,
        max_audit_cycles=max(1, int(args.max_audit_cycles)),
        auto_scan=not args.no_auto_scan,
        source_extensions=source_extensions,
        audit_validators=audit_validators,
        require_cjk_audit=require_cjk_audit,
        validator_extensions=DEFAULT_VALIDATOR_EXTENSIONS,
    )


def run_simple_supervision(args, config: Config, *, project_root: Path) -> dict[str, Any]:
    options = build_supervise_options(args, config, project_root=project_root)
    supervisor = SimpleSupervisor(config, options)
    return supervisor.run()
