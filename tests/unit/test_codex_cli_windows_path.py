from datetime import datetime
from pathlib import Path

from codex_orchestrator.codex_cli import _resolve_windows_native_codex_cli_path, _run_codex_subprocess
from codex_orchestrator.config import LimitsConfig
from codex_orchestrator.model import TaskNode, TaskStatus


def test_resolve_windows_native_codex_cli_path_supports_npm_cmd_wrapper(tmp_path: Path, monkeypatch) -> None:
    npm_dir = tmp_path / "npm"
    npm_dir.mkdir()
    wrapper = npm_dir / "codex.cmd"
    wrapper.write_text(
        "@ECHO off\n"
        "endLocal & goto #_undefined_# 2>NUL || title %COMSPEC% & "
        "\"%_prog%\"  \"%dp0%\\node_modules\\@openai\\codex\\bin\\codex.js\" %*\n",
        encoding="utf-8",
    )
    vendor = (
        npm_dir
        / "node_modules"
        / "@openai"
        / "codex"
        / "node_modules"
        / "@openai"
        / "codex-win32-x64"
        / "vendor"
        / "x86_64-pc-windows-msvc"
        / "codex"
        / "codex.exe"
    )
    vendor.parent.mkdir(parents=True)
    vendor.write_text("stub", encoding="utf-8")
    monkeypatch.setattr("codex_orchestrator.codex_cli.shutil.which", lambda cli_path: str(wrapper))

    assert _resolve_windows_native_codex_cli_path("codex") == str(vendor.resolve())


def test_resolve_windows_native_codex_cli_path_falls_back_to_resolved_cmd_wrapper(
    tmp_path: Path,
    monkeypatch,
) -> None:
    wrapper = tmp_path / "codex.cmd"
    wrapper.write_text("@ECHO off\nREM wrapper without inline metadata\n", encoding="utf-8")
    monkeypatch.setattr("codex_orchestrator.codex_cli.shutil.which", lambda cli_path: str(wrapper))

    assert _resolve_windows_native_codex_cli_path("codex") == str(wrapper)


def test_run_codex_subprocess_returns_failed_task_result_on_permission_error(monkeypatch) -> None:
    monkeypatch.setattr(
        "codex_orchestrator.codex_cli.subprocess.Popen",
        lambda *args, **kwargs: (_ for _ in ()).throw(PermissionError("[WinError 5] Access is denied")),
    )

    result = _run_codex_subprocess(
        task=TaskNode(id="task_bootstrap", prompt_template="prompt"),
        prompt="prompt",
        limits=LimitsConfig(),
        budget_tracker=None,
        started_at=datetime.now(),
        model="gpt-5.4-pro",
        cmd=["codex.cmd", "exec"],
        env={},
        cwd=None,
        timeout=30,
        on_progress=None,
        audit_logger=None,
        rate_limiter=None,
    )

    assert result.status is TaskStatus.FAILED
    assert result.error == "PermissionError: [WinError 5] Access is denied"
