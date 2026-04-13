"""Task ingestion for simple mode."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .simple_model import (
    AttemptState,
    SimpleItemType,
    SimpleValidationProfile,
    SimpleWorkItem,
    stable_item_id,
)


@dataclass
class SimpleLoadResult:
    items: list[SimpleWorkItem]
    source_summary: dict[str, Any]
    warnings: list[str] = field(default_factory=list)


def _parse_listish(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value if str(v).strip()]
    text = str(value).strip()
    if not text:
        return []
    if text[:1] in {"[", "{"}:
        try:
            decoded = json.loads(text)
        except json.JSONDecodeError:
            decoded = None
        if isinstance(decoded, list):
            return [str(v) for v in decoded if str(v).strip()]
    return [part.strip() for part in text.replace(";;", "\n").splitlines() if part.strip()]


def _parse_metadata(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    text = str(value).strip()
    if not text:
        return {}
    try:
        decoded = json.loads(text)
    except json.JSONDecodeError:
        return {"raw": text}
    return decoded if isinstance(decoded, dict) else {"value": decoded}


def _bucket_for(path: Path, strategy: str, repo_root: Path) -> str:
    relative = path.relative_to(repo_root) if path.is_absolute() and path.is_relative_to(repo_root) else path
    if strategy == "none":
        return "default"
    if strategy == "extension":
        return relative.suffix or "<no-ext>"
    if strategy == "size":
        try:
            size = path.stat().st_size
        except OSError:
            return "unknown"
        if size < 5_000:
            return "small"
        if size < 50_000:
            return "medium"
        return "large"
    parent = str(relative.parent).replace("\\", "/").strip(".")
    return parent or "."


def _resolve_repo_target(repo_root: Path, raw_target: str) -> tuple[Path, str | None]:
    raw_path = Path(raw_target)
    path = (repo_root / raw_path).resolve() if not raw_path.is_absolute() else raw_path.resolve()
    try:
        path.relative_to(repo_root)
    except ValueError:
        return path, f"目标路径不在工作目录内，已跳过: {raw_target}"
    if not path.exists():
        return path, f"目标路径不存在，已跳过: {raw_target}"
    return path, None


def _make_item(
    repo_root: Path,
    target: Path,
    instruction: str,
    bucket_strategy: str,
    default_timeout: int,
    default_max_attempts: int,
    raw: dict[str, Any] | None = None,
) -> SimpleWorkItem:
    raw = raw or {}
    item_type = raw.get("item_type")
    if item_type:
        item_enum = SimpleItemType(str(item_type))
    elif target.is_dir():
        item_enum = SimpleItemType.DIRECTORY_SHARD
    elif raw:
        item_enum = SimpleItemType.EXTERNAL_TASK
    else:
        item_enum = SimpleItemType.FILE
    normalized_target = str(target.relative_to(repo_root) if target.is_absolute() and target.is_relative_to(repo_root) else target)
    bucket = str(raw.get("bucket") or _bucket_for(target, bucket_strategy, repo_root))
    timeout_seconds = int(raw.get("timeout_seconds") or default_timeout)
    max_attempts = int(raw.get("max_attempts") or default_max_attempts)
    verify_commands = _parse_listish(raw.get("verify_commands"))
    require_patterns = _parse_listish(raw.get("require_patterns"))
    allowed_side_files = _parse_listish(raw.get("allowed_side_files"))
    metadata = _parse_metadata(raw.get("metadata"))
    priority = int(raw.get("priority") or 0)
    return SimpleWorkItem(
        item_id=stable_item_id(normalized_target, instruction, item_enum.value),
        item_type=item_enum,
        target=normalized_target,
        bucket=bucket,
        priority=priority,
        instruction=instruction,
        attempt_state=AttemptState(max_attempts=max_attempts),
        validation_profile=SimpleValidationProfile(
            verify_commands=verify_commands,
            require_patterns=require_patterns,
            allowed_side_files=allowed_side_files,
        ),
        metadata=metadata,
        timeout_seconds=timeout_seconds,
    )


def _load_task_file(task_file: Path) -> list[dict[str, Any]]:
    if task_file.suffix.lower() == ".jsonl":
        rows = []
        with task_file.open("r", encoding="utf-8") as fh:
            for index, line in enumerate(fh, 1):
                text = line.strip()
                if not text:
                    continue
                data = json.loads(text)
                if not isinstance(data, dict):
                    raise ValueError(f"JSONL line {index} must be an object")
                rows.append(data)
        return rows
    if task_file.suffix.lower() == ".csv":
        with task_file.open("r", encoding="utf-8", newline="") as fh:
            return list(csv.DictReader(fh))
    raise ValueError(f"Unsupported task file format: {task_file.suffix}")


def load_simple_work_items(
    repo_root: Path,
    instruction: str,
    *,
    files: list[str] | None = None,
    globs: list[str] | None = None,
    task_file: str | None = None,
    bucket_strategy: str = "dir",
    default_timeout: int = 1800,
    default_max_attempts: int = 3,
    validate_task_file_targets: bool = False,
) -> SimpleLoadResult:
    repo_root = repo_root.resolve()
    items: list[SimpleWorkItem] = []
    warnings: list[str] = []
    source_summary = {
        "files": 0,
        "globs": 0,
        "task_file": 0,
        "buckets": {},
    }
    dedupe: set[tuple[str, str, str]] = set()

    def register(item: SimpleWorkItem, source_type: str) -> None:
        key = (item.target, item.instruction, item.item_type.value)
        if key in dedupe:
            return
        dedupe.add(key)
        items.append(item)
        source_summary[source_type] += 1
        buckets = source_summary["buckets"]
        buckets[item.bucket] = buckets.get(item.bucket, 0) + 1

    for raw_path in files or []:
        path, warning = _resolve_repo_target(repo_root, raw_path)
        if warning:
            warnings.append(warning)
            continue
        register(
            _make_item(
                repo_root,
                path,
                instruction,
                bucket_strategy,
                default_timeout,
                default_max_attempts,
            ),
            "files",
        )

    for pattern in globs or []:
        matched = False
        for path in repo_root.glob(pattern):
            matched = True
            register(
                _make_item(
                    repo_root,
                    path.resolve(),
                    instruction,
                    bucket_strategy,
                    default_timeout,
                    default_max_attempts,
                ),
                "globs",
            )
        if not matched:
            warnings.append(f"glob 未匹配到任何路径: {pattern}")

    if task_file:
        rows = _load_task_file(Path(task_file))
        for row in rows:
            target_text = str(row.get("target", "")).strip()
            if not target_text:
                warnings.append("task-file 中存在缺少 target 的条目，已跳过")
                continue
            raw_path = Path(target_text)
            path = (repo_root / raw_path).resolve() if not raw_path.is_absolute() else raw_path.resolve()
            # validate_task_file_targets 为 False 时信任 task_file 中的 target，不验证存在性
            if validate_task_file_targets:
                path, warning = _resolve_repo_target(repo_root, target_text)
                if warning:
                    warnings.append(warning)
                    continue
            else:
                # 仅检查路径是否在工作目录内（安全性），不检查文件是否存在
                try:
                    path.relative_to(repo_root)
                except ValueError:
                    warnings.append(f"目标路径不在工作目录内，已跳过: {target_text}")
                    continue
            instruction_text = str(row.get("instruction") or instruction).strip()
            register(
                _make_item(
                    repo_root,
                    path,
                    instruction_text,
                    bucket_strategy,
                    default_timeout,
                    default_max_attempts,
                    raw=row,
                ),
                "task_file",
            )

    items.sort(key=lambda item: (item.bucket, -item.priority, item.target, item.item_id))
    return SimpleLoadResult(items=items, source_summary=source_summary, warnings=warnings)
