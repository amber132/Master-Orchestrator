import subprocess
import sys


def test_python_module_cli_help_prints_usage() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "claude_orchestrator.cli", "--help"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )

    assert result.returncode == 0
    assert "Master Orchestrator" in result.stdout
    assert "do" in result.stdout
    assert "runs" in result.stdout
