from __future__ import annotations

import argparse
from pathlib import Path

from master_orchestrator.cli import _cmd_retry_failed, _cmd_run


def test_retry_failed_reuses_same_dag_hash_for_operation_flow(tmp_path: Path) -> None:
    sandbox = tmp_path / "repo"
    sandbox.mkdir()
    (sandbox / "retry_mode.txt").write_text("fail", encoding="utf-8")
    dag_path = sandbox / "retry.toml"
    dag_path.write_text(
        "\n".join(
            [
                "[dag]",
                'name = "retry-flow"',
                "max_parallel = 1",
                "",
                "[tasks.retry_once]",
                'prompt = "operation flow"',
                'type = "operation"',
                'output_format = "json"',
                "timeout = 1800",
                "",
                "[tasks.retry_once.executor_config]",
                'rollback_refs = ["rollback_marker"]',
                'cutover_gates = []',
                "",
                "[[tasks.retry_once.executor_config.commands]]",
                'id = "cmd_1"',
                'command = "python -c \\"from pathlib import Path; import sys; mode = Path(\'retry_mode.txt\').read_text(encoding=\'utf-8\').strip(); sys.exit(1 if mode == \'fail\' else 0)\\""',
                'evidence_refs = ["marker"]',
            ]
        ),
        encoding="utf-8",
    )
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        "\n".join(
            [
                "[checkpoint]",
                f'db_path = "{(tmp_path / "state.db").as_posix()}"',
                "",
                "[workspace]",
                "enabled = false",
                f'root_dir = "{(tmp_path / "runs").as_posix()}"',
            ]
        ),
        encoding="utf-8",
    )

    run_args = argparse.Namespace(
        config=str(config_path),
        dag=str(dag_path),
        dir=str(sandbox),
        log_file=None,
        log_dir=None,
        pool_config=None,
        pool_profile=None,
        rate_limit=None,
        error_policy=None,
        enable_streaming=False,
    )
    assert _cmd_run(run_args) == 1

    (sandbox / "retry_mode.txt").write_text("pass", encoding="utf-8")

    retry_args = argparse.Namespace(
        config=str(config_path),
        dag=str(dag_path),
        run_id=None,
        dir=str(sandbox),
        log_file=None,
        log_dir=None,
        pool_config=None,
        pool_profile=None,
    )
    assert _cmd_retry_failed(retry_args) == 0
