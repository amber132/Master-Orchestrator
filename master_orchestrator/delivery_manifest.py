"""Local delivery manifest for completed autonomous runs."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class DeliveryManifest:
    run_id: str
    status: str
    branch_names: list[str] = field(default_factory=list)
    worktree_paths: list[str] = field(default_factory=list)
    backup_summary: str = ""
    verification_summary: str = ""
    recommended_merge_order: list[str] = field(default_factory=list)
    suggested_pr_titles: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
