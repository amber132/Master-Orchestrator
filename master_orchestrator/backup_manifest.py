"""Backup manifests for data-touching tasks."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class BackupResourceType(Enum):
    FILES = "files"
    DATABASE = "database"


@dataclass
class BackupEntry:
    resource_type: BackupResourceType
    source_path: str
    backup_path: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class BackupManifest:
    run_id: str
    repo_revision: str = ""
    created_at: datetime = field(default_factory=datetime.now)
    entries: list[BackupEntry] = field(default_factory=list)
    restore_instructions: list[str] = field(default_factory=list)
    summary: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "repo_revision": self.repo_revision,
            "created_at": self.created_at.isoformat(),
            "entries": [
                {
                    **asdict(entry),
                    "resource_type": entry.resource_type.value,
                }
                for entry in self.entries
            ],
            "restore_instructions": self.restore_instructions,
            "summary": self.summary,
        }
