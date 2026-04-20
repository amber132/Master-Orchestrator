"""Mandatory backup gate for data-touching tasks."""

from __future__ import annotations

import shutil
import subprocess
import uuid
from pathlib import Path

from .backup_manifest import BackupEntry, BackupManifest, BackupResourceType
from .command_runtime import normalize_python_command
from .config import BackupConfig
from .runtime_layout import RuntimeLayout
from .task_contract import TaskContract


class BackupGateError(RuntimeError):
    pass


class BackupGate:
    def __init__(self, config: BackupConfig):
        self._config = config

    def run(self, contract: TaskContract, layout: RuntimeLayout, repo_root: str | Path) -> BackupManifest:
        repo_path = Path(repo_root).resolve()
        manifest = BackupManifest(
            run_id=uuid.uuid4().hex[:12],
            repo_revision=self._repo_revision(repo_path),
            summary="未涉及数据",
        )

        if not contract.requires_backup:
            return manifest

        entries: list[BackupEntry] = []
        restore_instructions: list[str] = []

        if contract.touches_files:
            file_paths = self._collect_file_paths(repo_path, contract)
            if not file_paths:
                raise BackupGateError("涉文件数据任务未找到可备份路径")
            for file_path in file_paths:
                backup_target = layout.backups / file_path.name
                if file_path.is_dir():
                    shutil.copytree(file_path, backup_target, dirs_exist_ok=True)
                else:
                    backup_target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(file_path, backup_target)
                entries.append(
                    BackupEntry(
                        resource_type=BackupResourceType.FILES,
                        source_path=str(file_path),
                        backup_path=str(backup_target),
                    )
                )
                restore_instructions.append(f"恢复文件数据: 将 {backup_target} 覆盖回 {file_path}")

        if contract.touches_database:
            db_commands = list(contract.metadata.get("database_backup_commands", [])) or list(self._config.database_backup_commands)
            if not db_commands:
                raise BackupGateError("涉数据库任务缺少 database_backup_commands 配置")
            for index, command in enumerate(db_commands, start=1):
                output_path = layout.backups / f"database_backup_{index}.dump"
                rendered_command = normalize_python_command(command.format(output=str(output_path)))
                proc = subprocess.run(
                    rendered_command,
                    shell=True,
                    cwd=repo_path,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                )
                if proc.returncode != 0:
                    raise BackupGateError(f"数据库备份命令失败: {rendered_command}\n{proc.stderr[:500]}")
                if not output_path.exists():
                    output_path.write_text(proc.stdout or "database backup completed", encoding="utf-8")
                entries.append(
                    BackupEntry(
                        resource_type=BackupResourceType.DATABASE,
                        source_path="database",
                        backup_path=str(output_path),
                        metadata={"command": rendered_command},
                    )
                )
                restore_instructions.append(f"恢复数据库: 使用 {output_path} 对应的数据库恢复命令")

        manifest.entries = entries
        manifest.restore_instructions = restore_instructions
        manifest.summary = f"已备份 {len(entries)} 项资源"
        return manifest

    def _collect_file_paths(self, repo_root: Path, contract: TaskContract) -> list[Path]:
        contract_metadata_paths = [Path(path) for path in contract.metadata.get("backup_metadata_paths", [])]
        configured = [Path(path) for path in [*self._config.file_paths, *self._config.metadata_paths, *contract.data_paths, *contract_metadata_paths]]
        resolved: list[Path] = []
        for path in configured:
            candidate = path if path.is_absolute() else repo_root / path
            if candidate.exists():
                resolved.append(candidate)

        if resolved:
            return list(dict.fromkeys(resolved))

        common_names = ("uploads", "upload", "storage", "data", "assets")
        for name in common_names:
            candidate = repo_root / name
            if candidate.exists():
                resolved.append(candidate)
        return list(dict.fromkeys(resolved))

    def _repo_revision(self, repo_root: Path) -> str:
        try:
            proc = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=repo_root,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=True,
            )
            return proc.stdout.strip()
        except Exception:
            return ""
