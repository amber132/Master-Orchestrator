from __future__ import annotations

from pathlib import Path

from master_orchestrator.cli import _build_parser, _resolve_self_improve_auto_config, _resolve_self_improve_quality_gates


def test_improve_parser_accepts_plan_file_and_monitor_flags(tmp_path: Path) -> None:
    plan_path = tmp_path / "phase0.json"
    parser = _build_parser()

    args = parser.parse_args(
        [
            "improve",
            "-d",
            str(tmp_path),
            "--plan-file",
            str(plan_path),
            "--monitor-required",
            "--monitor-flow",
            "operation_dag_run",
        ]
    )

    assert args.plan_file == str(plan_path)
    assert args.monitor_required is True
    assert args.monitor_flow == ["operation_dag_run"]


def test_resolve_self_improve_quality_gates_appends_flow_matrix_command(tmp_path: Path) -> None:
    parser = _build_parser()
    args = parser.parse_args(
        [
            "improve",
            "-d",
            str(tmp_path),
            "--quality-gate",
            "pytest -q",
            "--monitor-required",
            "--monitor-flow",
            "operation_dag_run",
            "--monitor-flow",
            "runs_graph_after_operation",
        ]
    )

    commands = _resolve_self_improve_quality_gates(args)

    assert commands[0] == "pytest -q"
    assert any("run_flow_matrix.py" in command for command in commands)
    monitor_command = next(command for command in commands if "run_flow_matrix.py" in command)
    assert "--repo-root {workspace_dir}" in monitor_command
    assert "--python-executable" in monitor_command
    assert "--flow operation_dag_run" in monitor_command
    assert "--flow runs_graph_after_operation" in monitor_command


def test_resolve_self_improve_auto_config_uses_codex_provider() -> None:
    parser = _build_parser()
    args = parser.parse_args(
        [
            "improve",
            "-d",
            "D:/repo",
            "--provider",
            "codex",
        ]
    )

    from types import SimpleNamespace

    config = SimpleNamespace(
        claude=SimpleNamespace(default_model="sonnet"),
        codex=SimpleNamespace(default_model="gpt-5.4"),
    )

    auto_config = _resolve_self_improve_auto_config(args, config)

    assert auto_config.decomposition_model == "gpt-5.4"
    assert auto_config.review_model == "gpt-5.4"
    assert auto_config.execution_model == "gpt-5.4"
