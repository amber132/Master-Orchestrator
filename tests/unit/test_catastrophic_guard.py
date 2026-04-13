from pathlib import Path

from claude_orchestrator.catastrophic_guard import CatastrophicGuard
from claude_orchestrator.config import Config
from claude_orchestrator.runtime_layout import RuntimeLayout


def test_catastrophic_guard_detects_missing_workspace(tmp_path: Path) -> None:
    cfg = Config()
    layout = RuntimeLayout(root=tmp_path / "run", workspace=tmp_path / "missing", state=tmp_path / "state", logs=tmp_path / "logs", cache=tmp_path / "cache", evidence=tmp_path / "evidence", backups=tmp_path / "backups", handoff=tmp_path / "handoff")

    result = CatastrophicGuard(cfg.limits).check(layout)

    assert result.is_catastrophic is True
    assert "workspace" in result.reason.lower()
