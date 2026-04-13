"""Task intake and contract building utilities."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import re

from .repo_profile import RepoProfile
from .task_classifier import TaskClassification
from .task_contract import DataRisk, TaskContract, TaskInputType, TaskType

_MAX_DOC_LINES = 24
_MAX_DOC_CHARS = 1800
_MAX_DOC_CONTEXT_CHARS = 9000


@dataclass
class TaskIntakeRequest:
    goal: str
    document_paths: list[Path]
    repo_root: Path
    document_briefs: list[str] = field(default_factory=list)
    document_context: str = ""

    @property
    def input_type(self) -> TaskInputType:
        has_goal = bool(self.goal.strip())
        has_docs = bool(self.document_paths)
        if has_docs and self.goal.strip().startswith("根据文档执行任务:"):
            return TaskInputType.DOCUMENT_PATH
        if has_goal and has_docs:
            return TaskInputType.MIXED
        if has_docs:
            return TaskInputType.DOCUMENT_PATH
        return TaskInputType.NATURAL_LANGUAGE


def normalize_request(goal: str, document_paths: list[str] | None, repo_root: str | Path) -> TaskIntakeRequest:
    root = Path(repo_root).resolve()
    docs = [Path(p) for p in (document_paths or [])]
    if not goal.strip() and docs:
        normalized_goal = f"根据文档执行任务: {', '.join(str(_resolve_doc_path(root, doc)) for doc in docs)}"
    else:
        normalized_goal = goal
    resolved_docs = [_resolve_doc_path(root, doc) for doc in docs]

    if goal.strip() and not resolved_docs:
        possible = _resolve_doc_path(root, Path(goal.strip()))
        if possible.exists() and possible.is_file():
            resolved_docs.append(possible)
            normalized_goal = f"根据文档执行任务: {possible}"
            goal = ""

    final_goal = normalized_goal if normalized_goal.strip() else goal
    document_briefs = [_build_document_brief(path, root) for path in resolved_docs]
    document_context = _render_document_context(document_briefs)
    return TaskIntakeRequest(
        goal=final_goal,
        document_paths=resolved_docs,
        document_briefs=document_briefs,
        document_context=document_context,
        repo_root=root,
    )


def build_task_contract(
    request: TaskIntakeRequest,
    profile: RepoProfile,
    classification: TaskClassification,
) -> TaskContract:
    data_risk = _infer_data_risk(request, classification)
    goal = request.goal.strip() or f"根据文档执行任务: {', '.join(str(path) for path in request.document_paths)}"
    strict_refactor_mode = classification.task_type is TaskType.REFACTOR

    data_paths: list[str] = []
    metadata: dict[str, object] = {
        "repo_frameworks": list(profile.detected_frameworks),
        "package_managers": list(profile.package_managers),
        "backend_commands": list(profile.backend_commands),
        "frontend_commands": list(profile.frontend_commands),
    }
    if data_risk in (DataRisk.FILES, DataRisk.BOTH):
        data_paths.extend(profile.file_backup_paths)
        metadata["backup_metadata_paths"] = list(profile.metadata_backup_paths)
    if data_risk in (DataRisk.DATABASE, DataRisk.BOTH) and profile.database_backup_commands:
        metadata["database_backup_commands"] = list(profile.database_backup_commands)
    if request.document_paths:
        metadata["document_briefs"] = list(request.document_briefs)
        metadata["document_context"] = request.document_context

    return TaskContract(
        source_goal=goal,
        normalized_goal=goal,
        input_type=request.input_type,
        task_type=classification.task_type,
        data_risk=data_risk,
        affected_areas=classification.affected_areas,
        document_paths=[str(path) for path in request.document_paths],
        document_briefs=request.document_briefs,
        document_context=request.document_context,
        verification_focus=profile.verification_commands,
        inferred_from=classification.reasons,
        data_paths=data_paths,
        metadata=metadata,
        strict_refactor_mode=strict_refactor_mode,
        max_service_families_per_phase=(1 if strict_refactor_mode else 3),
        max_prod_files_per_iteration=(8 if strict_refactor_mode else 20),
        forbid_state_file_edits=strict_refactor_mode,
        require_guardrail_tests_before_service_moves=strict_refactor_mode,
        allowed_refactor_roots=_infer_allowed_refactor_roots(profile, classification),
    )


def _resolve_doc_path(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else (repo_root / path)


def _build_document_brief(path: Path, repo_root: Path) -> str:
    display_path = _display_doc_path(path, repo_root)
    if not path.exists():
        return f"文档: {display_path}\n状态: 文件不存在，执行前需要人工确认路径。"
    if path.is_dir():
        return f"文档: {display_path}\n状态: 这是目录而不是文件，执行前需要进入目录挑选具体文档。"

    try:
        raw_text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        raw_text = path.read_text(encoding="utf-8", errors="ignore")

    lines = [line.rstrip() for line in raw_text.splitlines()]
    title = _first_title(lines, path.name)
    selected = _select_key_lines(lines)
    excerpt = "\n".join(selected).strip()
    if len(excerpt) > _MAX_DOC_CHARS:
        excerpt = excerpt[: _MAX_DOC_CHARS - 1].rstrip() + "…"

    if not excerpt:
        excerpt = "（文档内容为空或无法提取有效摘要，执行前必须打开原文）"

    return (
        f"文档: {display_path}\n"
        f"标题: {title}\n"
        "关键摘录:\n"
        f"{excerpt}"
    )


def _render_document_context(document_briefs: list[str]) -> str:
    if not document_briefs:
        return ""

    prefix = (
        "以下是任务文档摘要。它只用于规划和分解，不能替代实际阅读原文。\n"
        "执行每个阶段前，必须打开并遵守原始文档；若中央宪章与项目仓库内 guide 冲突，以项目仓库内 guide 为准。\n\n"
    )
    context = prefix + "\n\n".join(document_briefs)
    if len(context) > _MAX_DOC_CONTEXT_CHARS:
        context = context[: _MAX_DOC_CONTEXT_CHARS - 1].rstrip() + "…"
    return context


def _display_doc_path(path: Path, repo_root: Path) -> str:
    try:
        return str(path.resolve().relative_to(repo_root.resolve())).replace("\\", "/")
    except ValueError:
        return str(path)


def _first_title(lines: list[str], fallback: str) -> str:
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("#"):
            return stripped.lstrip("#").strip() or fallback
    for line in lines:
        stripped = line.strip()
        if stripped:
            return stripped[:120]
    return fallback


def _select_key_lines(lines: list[str]) -> list[str]:
    selected: list[str] = []
    important_pattern = re.compile(r"^(#{1,6}\s+|[-*]\s+|\d+\.\s+|`[^`]+`|See also:|目标|范围|优先级|Phase|Batch)")

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if important_pattern.search(stripped):
            selected.append(stripped)
        if len(selected) >= _MAX_DOC_LINES:
            return selected

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        selected.append(stripped)
        if len(selected) >= min(_MAX_DOC_LINES, 12):
            break
    return selected


def _infer_data_risk(request: TaskIntakeRequest, classification: TaskClassification) -> DataRisk:
    text = " ".join([request.goal, *[str(path) for path in request.document_paths]]).lower()
    db_tokens = ("数据库", "database", "sql", "迁移", "schema", "数据")
    file_tokens = (
        "upload",
        "uploads",
        "上传",
        "附件",
        "图片",
        "视频",
        "素材",
        "storage",
        "oss",
        "对象存储",
        "备份文件",
        "恢复文件",
    )
    touches_db = any(token in text for token in db_tokens) or "database" in classification.affected_areas
    touches_files = any(token in text for token in file_tokens) or "storage" in classification.affected_areas

    if touches_db and touches_files:
        return DataRisk.BOTH
    if touches_db:
        return DataRisk.DATABASE
    if touches_files:
        return DataRisk.FILES
    return DataRisk.NONE


def _infer_allowed_refactor_roots(profile: RepoProfile, classification: TaskClassification) -> list[str]:
    if classification.task_type is not TaskType.REFACTOR:
        return []

    roots: list[str] = []
    if "backend" in classification.affected_areas and profile.has_backend:
        roots.extend(_java_backend_roots(profile.root, profile.backend_dir or profile.root))
    if "frontend" in classification.affected_areas and profile.has_frontend:
        roots.extend(_frontend_roots(profile.root, profile.frontend_dir or profile.root))
    if "database" in classification.affected_areas:
        roots.extend([
            "src/main/resources",
            "backend/src/main/resources",
        ])

    return list(dict.fromkeys(root.replace('\\', '/').rstrip('/') for root in roots if root))


def _java_backend_roots(repo_root: Path, backend_dir: Path) -> list[str]:
    prefix = _relative_prefix(repo_root, backend_dir)
    return [
        _join_prefix(prefix, "src/main"),
        _join_prefix(prefix, "src/test"),
        _join_prefix(prefix, "pom.xml"),
        _join_prefix(prefix, "mvnw"),
        _join_prefix(prefix, "mvnw.cmd"),
        _join_prefix(prefix, ".mvn"),
    ]


def _frontend_roots(repo_root: Path, frontend_dir: Path) -> list[str]:
    prefix = _relative_prefix(repo_root, frontend_dir)
    return [
        _join_prefix(prefix, "src"),
        _join_prefix(prefix, "package.json"),
        _join_prefix(prefix, "vite.config.ts"),
        _join_prefix(prefix, "vite.config.js"),
    ]


def _relative_prefix(repo_root: Path, target: Path) -> str:
    try:
        rel = target.resolve().relative_to(repo_root.resolve())
    except ValueError:
        return ""
    return "" if str(rel) == "." else str(rel).replace('\\', '/')


def _join_prefix(prefix: str, leaf: str) -> str:
    return leaf if not prefix else f"{prefix}/{leaf}"
