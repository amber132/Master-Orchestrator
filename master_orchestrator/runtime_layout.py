"""Per-run isolated runtime directory layout."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass
class RuntimeLayout:
    root: Path
    workspace: Path
    state: Path
    logs: Path
    cache: Path
    evidence: Path
    backups: Path
    handoff: Path

    @classmethod
    def create(cls, root: str | Path) -> "RuntimeLayout":
        run_root = Path(root)
        layout = cls(
            root=run_root,
            workspace=run_root / "workspace",
            state=run_root / "state",
            logs=run_root / "logs",
            cache=run_root / "cache",
            evidence=run_root / "evidence",
            backups=run_root / "backups",
            handoff=run_root / "handoff",
        )
        for path in (layout.root, layout.workspace, layout.state, layout.logs, layout.cache, layout.evidence, layout.backups, layout.handoff):
            path.mkdir(parents=True, exist_ok=True)
        return layout
