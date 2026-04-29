"""Isolation helpers for simple mode."""

from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import TYPE_CHECKING

from .simple_config import SimpleConfig
from .simple_model import SimpleIsolationMode, SimpleItemType, SimpleWorkItem

if TYPE_CHECKING:
    from .simple_runtime import SimpleRuntimeLayout


def _file_hash(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_bytes(path: Path) -> bytes | None:
    if not path.exists() or not path.is_file():
        return None
    return path.read_bytes()


def _scan_tree_hashes(root: Path) -> dict[str, str]:
    if not root.exists():
        return {}
    hashes: dict[str, str] = {}
    for path in root.rglob("*"):
        if path.is_file():
            hashes[str(path.relative_to(root)).replace("\\", "/")] = _file_hash(path)
    return hashes


def _git_repo_root(path: Path) -> Path | None:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=path,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=True,
        )
    except Exception:
        return None
    return Path(proc.stdout.strip()) if proc.stdout.strip() else None


@dataclass
class PreparedItemWorkspace:
    item: SimpleWorkItem
    requested_mode: str
    effective_mode: str
    cwd: Path
    target_path: Path
    source_target_path: Path
    git_root: Path | None
    source_baseline_hash: str
    target_baseline_hash: str
    source_baseline_bytes: bytes | None = None
    source_file_backups: dict[str, bytes | None] = field(default_factory=dict)
    workspace_git_root: Path | None = None
    track_repo_status: bool = True
    baseline_git_changes: set[str] = field(default_factory=set)
    baseline_tree_hashes: dict[str, str] = field(default_factory=dict)
    ignored_repo_paths: set[str] = field(default_factory=set)
    can_git_restore: bool = False
    warnings: list[str] = field(default_factory=list)

    def _is_ignored_repo_path(self, rel: str) -> bool:
        normalized = rel.replace("\\", "/")
        return any(
            normalized == ignored or normalized.startswith(f"{ignored}/")
            for ignored in self.ignored_repo_paths
        )

    def collect_changed_files(self) -> list[str]:
        if self.effective_mode == SimpleIsolationMode.COPY.value:
            current = _scan_tree_hashes(self.cwd)
            keys = set(current) | set(self.baseline_tree_hashes)
            return sorted(key for key in keys if current.get(key, "") != self.baseline_tree_hashes.get(key, ""))
        repo_root = (self.workspace_git_root or _git_repo_root(self.cwd)) if self.track_repo_status else None
        if not repo_root:
            candidates = {
                path.replace("\\", "/")
                for path in self.source_file_backups
                if not self._is_ignored_repo_path(path)
            }
            if self.item.item_type == SimpleItemType.DIRECTORY_SHARD:
                root = (self.cwd / self.item.target).resolve()
                if root.exists() and root.is_dir():
                    for path in root.rglob("*"):
                        if path.is_file():
                            rel = str(path.relative_to(self.cwd)).replace("\\", "/")
                            if not self._is_ignored_repo_path(rel):
                                candidates.add(rel)
            else:
                target = self.item.target.replace("\\", "/")
                if not self._is_ignored_repo_path(target):
                    candidates.add(target)
            changed: list[str] = []
            for rel in sorted(candidates):
                baseline = self.source_file_backups.get(rel)
                current = _read_bytes((self.cwd / rel).resolve())
                if current != baseline:
                    changed.append(rel)
            return changed
        proc = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        current: set[str] = set()
        for line in proc.stdout.splitlines():
            if len(line) >= 4:
                rel = line[3:].strip().replace("\\", "/")
                if self._is_ignored_repo_path(rel):
                    continue
                current.add(rel)
        return sorted(current - self.baseline_git_changes)


class SimpleIsolationManager:
    def __init__(
        self,
        repo_root: Path,
        layout: SimpleRuntimeLayout,
        config: SimpleConfig,
        run_id: str,
        mode: str,
        ignored_repo_paths: set[str] | None = None,
        shared_parent_file_counts: dict[str, int] | None = None,
    ):
        self.repo_root = repo_root.resolve()
        self.layout = layout
        self.config = config
        self.run_id = run_id
        self.requested_mode = mode
        self._copy_run_root = Path(config.copy_root_dir).resolve() / run_id
        self._worktree_root = layout.scratch / "worktree"
        self._ignored_repo_paths = {path.replace("\\", "/").rstrip("/") for path in (ignored_repo_paths or set()) if path}
        self._shared_parent_file_counts = dict(shared_parent_file_counts or {})
        self._worktree_ready = False
        self._effective_mode = mode
        self._git_root_cache: dict[Path, Path | None] = {}
        self._baseline_git_changes_cache: dict[Path, set[str]] = {}
        self._cache_lock = Lock()

    @property
    def effective_mode(self) -> str:
        return self._effective_mode

    def _resolve_git_root(self, path: Path) -> Path | None:
        resolved = path.resolve()
        with self._cache_lock:
            cached = self._git_root_cache.get(resolved)
            if resolved in self._git_root_cache:
                return cached
            git_root = _git_repo_root(resolved)
            self._git_root_cache[resolved] = git_root
            return git_root

    def _list_git_changes(self, repo_root: Path) -> set[str]:
        proc = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        changes: set[str] = set()
        for line in proc.stdout.splitlines():
            if len(line) >= 4:
                rel = line[3:].strip().replace("\\", "/")
                if any(rel == ignored or rel.startswith(f"{ignored}/") for ignored in self._ignored_repo_paths):
                    continue
                changes.add(rel)
        return changes

    def _baseline_git_changes(self, repo_root: Path | None) -> set[str]:
        if repo_root is None:
            return set()
        resolved = repo_root.resolve()
        if not self.config.cache_repo_status_baseline:
            return self._list_git_changes(resolved)
        with self._cache_lock:
            cached = self._baseline_git_changes_cache.get(resolved)
            if cached is not None:
                return set(cached)
            baseline = self._list_git_changes(resolved)
            self._baseline_git_changes_cache[resolved] = set(baseline)
            return set(baseline)

    def _copy_root_for_item(self, item: SimpleWorkItem) -> Path:
        return self._copy_run_root / item.item_id / "workspace"

    def _ensure_copy_root(self, item: SimpleWorkItem) -> Path:
        copy_root = self._copy_root_for_item(item)
        copy_root.parent.mkdir(parents=True, exist_ok=True)
        if copy_root.exists():
            shutil.rmtree(copy_root, ignore_errors=True)
        copy_root.mkdir(parents=True, exist_ok=True)
        self._effective_mode = SimpleIsolationMode.COPY.value
        return copy_root

    def _copy_path_to_workspace(self, copy_root: Path, relative_path: str) -> None:
        src = (self.repo_root / relative_path).resolve()
        dst = (copy_root / relative_path).resolve()
        if not src.exists():
            return
        if src.is_dir():
            shutil.copytree(src, dst, dirs_exist_ok=True)
            return
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)

    def _prepare_copy_scope(self, item: SimpleWorkItem) -> Path:
        root = self._ensure_copy_root(item)
        copy_targets: set[str] = set(item.validation_profile.allowed_side_files)
        if item.item_type == SimpleItemType.DIRECTORY_SHARD:
            copy_targets.add(item.target)
        else:
            copy_targets.add(item.target)
            parent_dir = (self.repo_root / item.target).resolve().parent
            package_init = parent_dir.parent / "__init__.py"
            if package_init.exists() and package_init.is_file():
                copy_targets.add(str(package_init.relative_to(self.repo_root)).replace("\\", "/"))
        for relative_path in sorted(copy_targets):
            normalized = relative_path.replace("\\", "/")
            while normalized.startswith("./"):
                normalized = normalized[2:]
            normalized = normalized.strip("/")
            if not normalized:
                continue
            self._copy_path_to_workspace(root, normalized)
        return root

    def _ensure_worktree_root(self, item: SimpleWorkItem) -> tuple[Path, list[str]]:
        warnings: list[str] = []
        if self._worktree_ready:
            return self._worktree_root, warnings
        if os.name == "nt" and len(str(self._worktree_root)) > self.config.windows_path_budget:
            warnings.append("worktree 路径预算超限，自动降级到 copy")
            return self._prepare_copy_scope(item), warnings
        repo_root = _git_repo_root(self.repo_root)
        if repo_root is None:
            warnings.append("当前目录不是 git 仓库，worktree 自动降级到 copy")
            return self._prepare_copy_scope(item), warnings
        branch = f"simple/{self.run_id[:8]}"
        try:
            subprocess.run(
                ["git", "-c", "core.longpaths=true", "worktree", "add", "-b", branch, str(self._worktree_root), "HEAD"],
                cwd=repo_root,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=True,
            )
            self._worktree_ready = True
            self._effective_mode = SimpleIsolationMode.WORKTREE.value
            return self._worktree_root, warnings
        except Exception as exc:
            warnings.append(f"worktree 创建失败，自动降级到 copy: {exc}")
            return self._prepare_copy_scope(item), warnings

    def prepare(self, item: SimpleWorkItem) -> PreparedItemWorkspace:
        warnings: list[str] = []
        if self.requested_mode == SimpleIsolationMode.NONE.value:
            cwd = self.repo_root
            effective_mode = SimpleIsolationMode.NONE.value
        elif self.requested_mode == SimpleIsolationMode.WORKTREE.value:
            cwd, warnings = self._ensure_worktree_root(item)
            effective_mode = self._effective_mode
        else:
            cwd = self._prepare_copy_scope(item)
            effective_mode = self._effective_mode

        use_repo_status_tracking = (
            effective_mode in {SimpleIsolationMode.NONE.value, SimpleIsolationMode.WORKTREE.value}
            and not (effective_mode == SimpleIsolationMode.NONE.value and item.item_type == SimpleItemType.FILE)
        )
        need_repo_status = effective_mode in {SimpleIsolationMode.NONE.value, SimpleIsolationMode.WORKTREE.value}
        git_root = self._resolve_git_root(self.repo_root) if need_repo_status else None
        restore_baseline_changes: set[str] = set()
        if git_root is not None and (use_repo_status_tracking or effective_mode == SimpleIsolationMode.NONE.value):
            restore_baseline_changes = self._baseline_git_changes(git_root)
        source_target_path = (self.repo_root / item.target).resolve()
        target_path = (cwd / item.target).resolve()
        repo_for_status = self._resolve_git_root(cwd) if use_repo_status_tracking else None
        baseline_changes = restore_baseline_changes if repo_for_status is not None else set()
        backup_paths = {item.target, *item.validation_profile.allowed_side_files}
        if item.item_type == SimpleItemType.DIRECTORY_SHARD:
            directory_root = (self.repo_root / item.target).resolve()
            if directory_root.exists() and directory_root.is_dir():
                for path in directory_root.rglob("*"):
                    if path.is_file():
                        backup_paths.add(str(path.relative_to(self.repo_root)).replace("\\", "/"))
        elif effective_mode != SimpleIsolationMode.COPY.value:
            parent_dir = source_target_path.parent
            if parent_dir.exists() and parent_dir.is_dir():
                for path in parent_dir.iterdir():
                    if path.is_file():
                        backup_paths.add(str(path.relative_to(self.repo_root)).replace("\\", "/"))
            package_init = parent_dir.parent / "__init__.py"
            if package_init.exists() and package_init.is_file():
                backup_paths.add(str(package_init.relative_to(self.repo_root)).replace("\\", "/"))
        source_file_backups: dict[str, bytes | None] = {}
        for rel in backup_paths:
            source_file_backups[rel.replace("\\", "/")] = _read_bytes((self.repo_root / rel).resolve())
        return PreparedItemWorkspace(
            item=item,
            requested_mode=self.requested_mode,
            effective_mode=effective_mode,
            cwd=cwd,
            target_path=target_path,
            source_target_path=source_target_path,
            git_root=git_root,
            workspace_git_root=repo_for_status,
            track_repo_status=use_repo_status_tracking,
            source_baseline_hash=_file_hash(source_target_path),
            target_baseline_hash=_file_hash(target_path),
            source_baseline_bytes=_read_bytes(source_target_path),
            source_file_backups=source_file_backups,
            baseline_git_changes=baseline_changes,
            baseline_tree_hashes=_scan_tree_hashes(cwd) if effective_mode == SimpleIsolationMode.COPY.value else {},
            ignored_repo_paths=set(self._ignored_repo_paths),
            can_git_restore=(effective_mode == SimpleIsolationMode.NONE.value and git_root is not None and not restore_baseline_changes),
            warnings=warnings,
        )

    def rollback_target(self, prepared: PreparedItemWorkspace) -> bool:
        target = prepared.target_path
        source = prepared.source_target_path if prepared.effective_mode != SimpleIsolationMode.NONE.value else None
        try:
            if prepared.item.item_type == SimpleItemType.DIRECTORY_SHARD:
                return True
            if prepared.effective_mode == SimpleIsolationMode.NONE.value:
                if prepared.source_baseline_hash == "":
                    if target.exists():
                        target.unlink()
                elif prepared.source_baseline_bytes is not None:
                    target.write_bytes(prepared.source_baseline_bytes)
            else:
                if prepared.source_baseline_bytes is not None:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(prepared.source_baseline_bytes)
                    prepared.source_target_path.parent.mkdir(parents=True, exist_ok=True)
                    prepared.source_target_path.write_bytes(prepared.source_baseline_bytes)
                else:
                    if target.exists():
                        target.unlink()
                    if prepared.source_target_path.exists():
                        prepared.source_target_path.unlink()
            return True
        except Exception:
            return False

    def restore_from_source(self, prepared: PreparedItemWorkspace, changed_files: list[str]) -> None:
        if prepared.effective_mode == SimpleIsolationMode.NONE.value:
            return
        for rel in changed_files:
            src = (self.repo_root / rel).resolve()
            dst = (prepared.cwd / rel).resolve()
            try:
                if src.exists():
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src, dst)
                elif dst.exists():
                    dst.unlink()
            except Exception:
                continue

    def rollback_changed_files(self, prepared: PreparedItemWorkspace, changed_files: list[str]) -> bool:
        try:
            if prepared.effective_mode == SimpleIsolationMode.NONE.value and prepared.can_git_restore and prepared.git_root:
                for rel in changed_files:
                    restore = subprocess.run(
                        ["git", "restore", "--worktree", "--source=HEAD", "--", rel],
                        cwd=prepared.git_root,
                        capture_output=True,
                        text=True,
                        encoding="utf-8",
                        errors="replace",
                        check=False,
                    )
                    if restore.returncode == 0:
                        continue
                    path = prepared.git_root / rel
                    show = subprocess.run(
                        ["git", "show", f"HEAD:{rel}"],
                        cwd=prepared.git_root,
                        capture_output=True,
                        check=False,
                    )
                    if show.returncode == 0:
                        path.parent.mkdir(parents=True, exist_ok=True)
                        path.write_bytes(show.stdout)
                        continue
                    if path.is_file():
                        path.unlink(missing_ok=True)
                    elif path.is_dir():
                        shutil.rmtree(path, ignore_errors=True)
                return True

            for rel in changed_files:
                normalized = rel.replace("\\", "/")
                target = (self.repo_root / normalized).resolve() if prepared.effective_mode == SimpleIsolationMode.NONE.value else (prepared.cwd / normalized).resolve()
                backup = prepared.source_file_backups.get(normalized)
                if backup is None:
                    if target.exists():
                        if target.is_file():
                            target.unlink()
                        elif target.is_dir():
                            shutil.rmtree(target, ignore_errors=True)
                    continue
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(backup)
            return True
        except Exception:
            return False

    def copy_back(self, prepared: PreparedItemWorkspace, changed_files: list[str]) -> tuple[bool, str]:
        if prepared.effective_mode == SimpleIsolationMode.NONE.value:
            return True, ""
        if prepared.item.item_type == SimpleItemType.DIRECTORY_SHARD:
            target_prefix = prepared.item.target.rstrip("/\\").replace("\\", "/") + "/"
            for rel in changed_files:
                normalized = rel.replace("\\", "/")
                if not (normalized == prepared.item.target.replace("\\", "/") or normalized.startswith(target_prefix)):
                    continue
                source_path = (self.repo_root / normalized).resolve()
                current_hash = _file_hash(source_path)
                baseline_backup = prepared.source_file_backups.get(normalized)
                baseline_hash = hashlib.sha256(baseline_backup).hexdigest() if baseline_backup is not None else ""
                if current_hash != baseline_hash:
                    return False, f"source file changed during copy-back: {normalized}"
                workspace_path = (prepared.cwd / normalized).resolve()
                if workspace_path.exists():
                    source_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(workspace_path, source_path)
                elif source_path.exists():
                    source_path.unlink()
            return True, ""
        if not prepared.target_path.exists() or not prepared.target_path.is_file():
            return False, "target missing during copy-back"
        source_hash_now = _file_hash(prepared.source_target_path)
        if source_hash_now != prepared.source_baseline_hash:
            return False, "source target changed during copy-back"
        prepared.source_target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(prepared.target_path, prepared.source_target_path)
        return True, ""
