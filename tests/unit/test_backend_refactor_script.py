from pathlib import Path


def test_run_backend_refactor_ps1_exists_and_contains_required_flags() -> None:
    script = Path("run_backend_refactor.ps1")

    assert script.exists()
    content = script.read_text(encoding="utf-8")
    assert "python" in content.lower()
    assert "claude_orchestrator.cli" in content
    assert "backend-refactor-constitution.md" in content
    assert "--skip-gather" in content
    assert "-y" in content



def test_run_backend_refactor_ps1_is_utf8_bom_for_windows_powershell() -> None:
    script = Path("run_backend_refactor.ps1")
    raw = script.read_bytes()

    assert raw.startswith(bytes([0xEF, 0xBB, 0xBF]))
