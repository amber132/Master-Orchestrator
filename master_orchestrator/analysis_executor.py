"""Deterministic executor for lightweight phase analysis and scoping tasks."""

from __future__ import annotations

import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from .model import TaskNode, TaskResult, TaskStatus


class AnalysisExecutor:
    """Produce bounded text guidance for analyze_/scope_ tasks without invoking Codex."""

    def execute(
        self,
        task: TaskNode,
        prompt: str,
        claude_config: Any,
        limits: Any,
        budget_tracker: Any,
        working_dir: str | None,
        on_progress: Any,
        audit_logger: Any = None,
        rate_limiter: Any = None,
    ) -> TaskResult:
        started_at = datetime.now()
        started_perf = time.perf_counter()
        repo_root = Path(working_dir or ".").resolve()
        summary = _build_analysis_summary(task.id, prompt, repo_root)
        finished_at = datetime.now()
        return TaskResult(
            task_id=task.id,
            status=TaskStatus.SUCCESS,
            output=summary,
            parsed_output=summary,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=time.perf_counter() - started_perf,
            model_used="analysis",
        )


def _build_analysis_summary(task_id: str, prompt: str, repo_root: Path) -> str:
    phase_kind = "scope" if task_id.startswith("scope_") else "analysis"
    focus = _select_focus_family(task_id, prompt, repo_root)
    focus_files = _collect_focus_files(repo_root, focus["patterns"])
    application_props = repo_root / "src" / "main" / "resources" / "application.properties"
    props_text = _safe_read(application_props)
    uses_local_mysql = "spring.datasource.url=jdbc:mysql://localhost" in props_text.lower()
    has_context_loads = any("springboottest" in _safe_read(path).lower() for path in repo_root.rglob("*Tests.java"))

    lines = [
        f"Phase{phase_kind.title()}: {focus['name']}",
        "Goal: keep API semantics stable, limit this iteration to one service family, and avoid persistence changes.",
    ]
    if focus_files:
        lines.append("FocusFiles: " + ", ".join(focus_files[:6]))
    if uses_local_mysql:
        lines.append("RiskSignal: application.properties binds a local MySQL datasource, so contextLoads is likely coupled to external DB availability.")
    if has_context_loads:
        lines.append("RiskSignal: a Spring Boot contextLoads test exists and should be isolated before any boundary refactor widens the blast radius.")

    if phase_kind == "analysis":
        lines.extend(
            [
                "Plan:",
                "1. Stabilize test startup with a test-only datasource or equivalent isolation that does not change production persistence behavior.",
                f"2. Constrain the first structural refactor to the {focus['label']} family and add a single facade/adapter entry.",
                "3. Preserve existing controller endpoints and repository contracts for this wave.",
            ]
        )
    else:
        lines.extend(
            [
                "InScope:",
                f"- Limit code changes to the {focus['label']} controller/service path plus test-startup wiring.",
                "- Keep production schema, repositories, and endpoint paths unchanged.",
                "OutOfScope:",
                "- Physical microservice deployment split.",
                "- Database/schema migration or persistence replacement.",
                "- New authentication capabilities that do not already exist in code.",
            ]
        )

    return "\n".join(lines)


def _select_focus_family(task_id: str, prompt: str, repo_root: Path) -> dict[str, Any]:
    text = f"{task_id}\n{prompt}".lower()
    candidates = [
        {
            "name": "Borrowing Transaction Slice",
            "label": "borrowing-transaction",
            "keywords": ("transaction", "borrow", "issuebook", "returnbook", "lending"),
            "patterns": ("Transaction", "Card", "Book"),
        },
        {
            "name": "Catalog Slice",
            "label": "catalog",
            "keywords": ("catalog", "book", "author"),
            "patterns": ("Book", "Author"),
        },
        {
            "name": "Identity Slice",
            "label": "auth-identity",
            "keywords": ("auth", "authentication", "identity", "security", "login", "student"),
            "patterns": ("Student", "Card"),
        },
    ]
    for candidate in candidates:
        if any(keyword in text for keyword in candidate["keywords"]):
            return candidate

    for candidate in candidates:
        if _collect_focus_files(repo_root, candidate["patterns"]):
            return candidate

    return {
        "name": "Minimal Backend Slice",
        "label": "backend-core",
        "patterns": ("Controller", "Service"),
    }


def _collect_focus_files(repo_root: Path, patterns: tuple[str, ...]) -> list[str]:
    matches: list[str] = []
    src_root = repo_root / "src"
    if not src_root.exists():
        return matches

    for path in src_root.rglob("*.java"):
        name = path.stem
        if any(pattern.lower() in name.lower() for pattern in patterns):
            matches.append(path.relative_to(repo_root).as_posix())
    unique = []
    seen: set[str] = set()
    for item in matches:
        if item in seen:
            continue
        seen.add(item)
        unique.append(item)
    return unique


def _safe_read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
