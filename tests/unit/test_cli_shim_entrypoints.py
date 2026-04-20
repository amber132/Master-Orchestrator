from __future__ import annotations

import subprocess
from pathlib import Path

from master_orchestrator.flow_matrix import FlowContext


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_flow_context_prefers_repo_local_cli_shims(tmp_path: Path) -> None:
    ctx = FlowContext(
        repo_root=REPO_ROOT,
        audit_root=tmp_path / "audit",
        python_executable="python3",
    )

    assert ctx.cli_script("master-orchestrator") == REPO_ROOT / "master-orchestrator"
    assert ctx.cli_script("mo") == REPO_ROOT / "mo"


def test_repo_local_master_orchestrator_shim_prints_help() -> None:
    result = subprocess.run(
        [str(REPO_ROOT / "master-orchestrator"), "--help"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
        cwd=REPO_ROOT,
    )

    assert result.returncode == 0
    assert "Master Orchestrator" in result.stdout
    assert "do" in result.stdout


def test_repo_local_mo_shim_prints_help() -> None:
    result = subprocess.run(
        [str(REPO_ROOT / "mo"), "--help"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
        cwd=REPO_ROOT,
    )

    assert result.returncode == 0
    assert "Master Orchestrator" in result.stdout
    assert "runs" in result.stdout
