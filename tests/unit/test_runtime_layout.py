from __future__ import annotations

from pathlib import Path

from claude_orchestrator.runtime_layout import RuntimeLayout


def test_runtime_layout_create_builds_expected_directories(tmp_path: Path) -> None:
    layout = RuntimeLayout.create(tmp_path / "run-001")

    assert layout.root == tmp_path / "run-001"
    for path in (
        layout.root,
        layout.workspace,
        layout.state,
        layout.logs,
        layout.cache,
        layout.evidence,
        layout.backups,
        layout.handoff,
    ):
        assert path.exists()
        assert path.is_dir()
