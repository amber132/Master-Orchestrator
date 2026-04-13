from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import pytest

from master_orchestrator.simple_control import SimpleRunController
from master_orchestrator.simple_model import SimpleItemStatus, SimpleItemType, SimpleRun, SimpleRunStatus, SimpleWorkItem
from master_orchestrator.simple_store import SimpleStore
from master_orchestrator.store import Store


@pytest.mark.skipif(os.name != "nt", reason="Windows-specific no-psutil fallback")
def test_simple_control_dry_run_does_not_touch_proc_without_psutil(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    db_path = tmp_path / "state.db"

    run = SimpleRun(
        instruction_template="annotate",
        working_dir=str(repo),
        started_at=datetime.now(),
        status=SimpleRunStatus.RUNNING,
    )
    item = SimpleWorkItem(
        item_id="item-1",
        item_type=SimpleItemType.FILE,
        target="sample.py",
        bucket="root",
        priority=0,
        instruction="annotate",
        status=SimpleItemStatus.EXECUTING,
    )

    with Store(str(db_path)) as store:
        SimpleStore(store).create_run(run, [item])
        monkeypatch.setattr("master_orchestrator.simple_control.psutil", None)

        controller = SimpleRunController(store)
        cancel_payload = controller.cancel(run.run_id, dry_run=True)
        reconcile_payload = controller.reconcile(run.run_id, dry_run=True)

    assert cancel_payload["action"] == "cancel"
    assert reconcile_payload["action"] == "reconcile"
    assert cancel_payload["processes"]["runner_pids"] == []
    assert cancel_payload["processes"]["exec_pids"] == []
