from __future__ import annotations

from datetime import datetime

from master_orchestrator.model import RunInfo, RunStatus, TaskResult, TaskStatus
from master_orchestrator.store import Store


def test_store_persists_provider_used_for_task_results(tmp_path) -> None:
    db_path = tmp_path / "state.db"

    with Store(db_path) as store:
        run = RunInfo(
            run_id="run-1",
            dag_name="demo",
            dag_hash="hash",
            status=RunStatus.RUNNING,
            started_at=datetime.now(),
        )
        store.create_run(run)
        store.init_task(run.run_id, "task-1")
        store.update_task(
            run.run_id,
            TaskResult(
                task_id="task-1",
                status=TaskStatus.SUCCESS,
                model_used="gpt-5.4-pro",
                provider_used="codex",
                started_at=datetime.now(),
                finished_at=datetime.now(),
            ),
        )

        loaded = store.get_task_result(run.run_id, "task-1")

    assert loaded is not None
    assert loaded.provider_used == "codex"
