"""Git worktree based isolated workspace management."""

from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)
_COPY_WORKSPACE_IGNORE_DIRS = {
    ".git",
    ".pytest_cache",
    "__pycache__",
    ".venv",
    "audit_logs",
    "orchestrator_runs",
    "simple_runs",
}
_COPY_WORKSPACE_IGNORE_FILES = {
    "orchestrator_state.db",
    "orchestrator_state.db-shm",
    "orchestrator_state.db-wal",
    "task_cache.db",
    "metrics.jsonl",
}

from .config import WorkspaceConfig
from .runtime_layout import RuntimeLayout
from .task_contract import TaskContract


@dataclass
class WorkspaceSession:
    source_repo: Path
    layout: RuntimeLayout
    branch_names: list[str]
    worktree_paths: list[Path]

    @property
    def primary_branch(self) -> str:
        return self.branch_names[0] if self.branch_names else ""


class WorkspaceManager:
    def __init__(self, config: WorkspaceConfig):
        self._config = config

    def create_session(self, source_repo: str | Path, contract: TaskContract) -> WorkspaceSession:
        repo_root = self._git_root(Path(source_repo).resolve())
        slug = self._slugify(contract.normalized_goal or contract.task_type.value)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_root = Path(self._config.root_dir).resolve() / repo_root.name / f"{timestamp}_{slug}"
        if not self._config.enabled:
            layout = RuntimeLayout(
                root=run_root,
                workspace=repo_root,
                state=run_root / "state",
                logs=run_root / "logs",
                cache=run_root / "cache",
                evidence=run_root / "evidence",
                backups=run_root / "backups",
                handoff=run_root / "handoff",
            )
            for path in (layout.root, layout.state, layout.logs, layout.cache, layout.evidence, layout.backups, layout.handoff):
                path.mkdir(parents=True, exist_ok=True)
            return WorkspaceSession(
                source_repo=repo_root,
                layout=layout,
                branch_names=[],
                worktree_paths=[],
            )

        layout = RuntimeLayout.create(run_root)

        if not self._has_head_commit(repo_root):
            logger.warning("仓库 %s 尚无初始提交，降级为复制工作区模式", repo_root)
            self._copy_repo_to_workspace(repo_root, layout.workspace)
            return WorkspaceSession(
                source_repo=repo_root,
                layout=layout,
                branch_names=[],
                worktree_paths=[layout.workspace],
            )

        branch = self._unique_branch_name(repo_root, f"{self._config.branch_prefix}/{contract.task_type.value}/{slug}")
        self._run_git(["worktree", "add", "-b", branch, str(layout.workspace), "HEAD"], cwd=repo_root)
        # git worktree 只包含已跟踪文件，同步未跟踪文件确保 workspace 完整
        self._sync_untracked_files(repo_root, layout.workspace.resolve())
        return WorkspaceSession(
            source_repo=repo_root,
            layout=layout,
            branch_names=[branch],
            worktree_paths=[layout.workspace],
        )

    def _git_root(self, repo: Path) -> Path:
        proc = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=repo,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        return Path(proc.stdout.strip())

    def _unique_branch_name(self, repo_root: Path, base: str) -> str:
        branch = base
        counter = 1
        while self._branch_exists(repo_root, branch):
            counter += 1
            branch = f"{base}-{counter}"
        return branch

    def _branch_exists(self, repo_root: Path, branch: str) -> bool:
        proc = subprocess.run(
            ["git", "branch", "--list", branch],
            cwd=repo_root,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=True,
        )
        return bool(proc.stdout.strip())

    def _run_git(self, args: list[str], cwd: Path) -> None:
        subprocess.run(
            ["git", "-c", "core.longpaths=true", *args],
            cwd=cwd,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )

    def _has_head_commit(self, repo_root: Path) -> bool:
        proc = subprocess.run(
            ["git", "rev-parse", "--verify", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        return proc.returncode == 0 and bool(proc.stdout.strip())

    def _copy_repo_to_workspace(self, source_repo: Path, workspace: Path) -> None:
        for current_root, dirnames, filenames in os.walk(source_repo):
            current_path = Path(current_root)
            rel_root = current_path.relative_to(source_repo)
            dirnames[:] = [name for name in dirnames if name not in _COPY_WORKSPACE_IGNORE_DIRS]
            target_root = workspace / rel_root
            target_root.mkdir(parents=True, exist_ok=True)

            for filename in filenames:
                if filename in _COPY_WORKSPACE_IGNORE_FILES:
                    continue
                src = current_path / filename
                if not src.is_file():
                    continue
                dst = target_root / filename
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dst)

    def _sync_untracked_files(self, source_repo: Path, workspace: Path) -> None:
        """将源仓库中未跟踪的文件同步到 workspace（git worktree 不包含未跟踪文件）。"""
        proc = subprocess.run(
            ["git", "ls-files", "--others", "--exclude-standard"],
            cwd=source_repo,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=True,
        )
        untracked = proc.stdout.strip()
        if not untracked:
            return
        count = 0
        for rel_path in untracked.splitlines():
            rel_path = rel_path.strip()
            if not rel_path:
                continue
            src = source_repo / rel_path
            dst = workspace / rel_path
            if src.is_file():
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, dst)
                count += 1
        if count:
            logger.info("同步 %d 个未跟踪文件到 workspace", count)

    def _slugify(self, text: str) -> str:
        # 只保留 ASCII 字母数字和下划线/连字符，避免中文路径导致 Windows MAX_PATH 问题
        cleaned = re.sub(r"[^a-zA-Z0-9_-]+", "-", text.strip())
        cleaned = re.sub(r"-+", "-", cleaned).strip("-")
        return cleaned[:32] or "task"
