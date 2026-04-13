from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

from claude_orchestrator.config import Config
from claude_orchestrator.simple_runtime import SimpleTaskRunner
from claude_orchestrator.store import Store


def test_simple_run_persists_status_and_manifest(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    target = repo / "a.py"
    target.write_text("print('a')\n", encoding="utf-8")
    db_path = tmp_path / "state.db"

    config = Config()
    config.checkpoint.db_path = str(db_path)
    config.simple.manifest_dir = str(tmp_path / "runs")
    config.simple.copy_root_dir = str(tmp_path / "copies")
    config.simple.claude_home_isolation = "none"

    def fake_execute(self, prepared, worker_id):
        prepared.target_path.write_text("# note\nprint('a')\n", encoding="utf-8")
        from claude_orchestrator.model import TaskResult, TaskStatus
        from claude_orchestrator.simple_executor import ExecutionOutcome
        from claude_orchestrator.simple_model import SimpleAttempt, SimpleItemStatus
        result = TaskResult(task_id=prepared.item.item_id, status=TaskStatus.SUCCESS, output="ok")
        attempt = SimpleAttempt(
            item_id=prepared.item.item_id,
            attempt=prepared.item.attempt_state.attempt,
            status=SimpleItemStatus.RUNNING,
            worker_id=worker_id,
            cli_duration_ms=120.5,
            claude_home_ready_ms=15.0,
            execution_wall_ms=140.0,
        )
        return ExecutionOutcome(result=result, attempt=attempt, changed_files=[prepared.item.target], prompt="test")

    monkeypatch.setattr("claude_orchestrator.simple_executor.SimpleExecutor.execute", fake_execute)

    with Store(config.checkpoint.db_path) as store:
        runner = SimpleTaskRunner(config, store, working_dir=str(repo))
        run, payload = runner.run("annotate", files=["a.py"], isolation_mode="none")

        assert run.status.value == "completed"
        assert payload["manifest"]["completed_items"] == 1
        assert store.get_simple_manifest(run.run_id)["completed_items"] == 1
        loaded_run = store.get_simple_run(run.run_id)
        assert loaded_run is not None
        assert loaded_run.last_heartbeat_at is not None
        execution_stats = payload["manifest"]["execution_stats"]
        stage_timing_stats = payload["manifest"]["stage_timing_stats"]
        assert execution_stats["attempts_total"] == 1
        assert execution_stats["cli_duration_ms_avg"] == 120.5
        assert execution_stats["claude_home_ready_ms_avg"] == 15.0
        assert execution_stats["execution_wall_ms_avg"] == 140.0
        assert stage_timing_stats["prepare"]["count"] == 1
        assert stage_timing_stats["execute"]["count"] == 1
        assert stage_timing_stats["validate"]["count"] == 1


def test_simple_run_recovers_unauthorized_side_changes_in_none_mode(git_repo: Path, monkeypatch) -> None:
    target = git_repo / "a.py"
    side = git_repo / "b.py"
    target.write_text("print('a')\n", encoding="utf-8")
    side.write_text("print('b')\n", encoding="utf-8")

    import subprocess
    subprocess.run(["git", "add", "."], cwd=git_repo, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "add files"], cwd=git_repo, check=True, capture_output=True)

    db_path = git_repo / "state.db"
    config = Config()
    config.checkpoint.db_path = str(db_path)
    config.simple.manifest_dir = str(git_repo / ".runs")
    config.simple.copy_root_dir = str(git_repo / ".copies")
    config.simple.claude_home_isolation = "none"

    def fake_execute(self, prepared, worker_id):
        prepared.target_path.write_text("# note\nprint('a')\n", encoding="utf-8")
        (prepared.cwd / "b.py").write_text("# drift\nprint('b')\n", encoding="utf-8")
        from claude_orchestrator.model import TaskResult, TaskStatus
        from claude_orchestrator.simple_executor import ExecutionOutcome
        from claude_orchestrator.simple_model import SimpleAttempt, SimpleItemStatus
        result = TaskResult(task_id=prepared.item.item_id, status=TaskStatus.SUCCESS, output="ok")
        attempt = SimpleAttempt(
            item_id=prepared.item.item_id,
            attempt=prepared.item.attempt_state.attempt,
            status=SimpleItemStatus.RUNNING,
            worker_id=worker_id,
        )
        return ExecutionOutcome(result=result, attempt=attempt, changed_files=["a.py", "b.py"], prompt="test")

    monkeypatch.setattr("claude_orchestrator.simple_executor.SimpleExecutor.execute", fake_execute)

    with Store(config.checkpoint.db_path) as store:
        runner = SimpleTaskRunner(config, store, working_dir=str(git_repo))
        run, payload = runner.run("annotate", files=["a.py"], isolation_mode="none")

        assert run.status.value == "completed"
        assert target.read_text(encoding="utf-8") == "# note\nprint('a')\n"
        assert side.read_text(encoding="utf-8") == "print('b')\n"
        assert payload["manifest"]["completed_items"] == 1
        events = store.get_simple_events(run.run_id, limit=20)
        assert any(event["event_type"] == "item_unauthorized_changes_recovered" for event in events)


def test_simple_run_supports_directory_shards(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    (repo / "pkg").mkdir(parents=True)
    (repo / "pkg" / "a.py").write_text("print('a')\n", encoding="utf-8")
    (repo / "pkg" / "b.py").write_text("print('b')\n", encoding="utf-8")
    db_path = tmp_path / "state.db"

    config = Config()
    config.checkpoint.db_path = str(db_path)
    config.simple.manifest_dir = str(tmp_path / "runs")
    config.simple.copy_root_dir = str(tmp_path / "copies")
    config.simple.claude_home_isolation = "none"

    def fake_execute(self, prepared, worker_id):
        (prepared.cwd / "pkg" / "a.py").write_text("# note\nprint('a')\n", encoding="utf-8")
        (prepared.cwd / "pkg" / "b.py").write_text("# note\nprint('b')\n", encoding="utf-8")
        from claude_orchestrator.model import TaskResult, TaskStatus
        from claude_orchestrator.simple_executor import ExecutionOutcome
        from claude_orchestrator.simple_model import SimpleAttempt, SimpleItemStatus
        result = TaskResult(task_id=prepared.item.item_id, status=TaskStatus.SUCCESS, output="ok")
        attempt = SimpleAttempt(
            item_id=prepared.item.item_id,
            attempt=prepared.item.attempt_state.attempt,
            status=SimpleItemStatus.RUNNING,
            worker_id=worker_id,
        )
        return ExecutionOutcome(result=result, attempt=attempt, changed_files=["pkg/a.py", "pkg/b.py"], prompt="test")

    monkeypatch.setattr("claude_orchestrator.simple_executor.SimpleExecutor.execute", fake_execute)

    with Store(config.checkpoint.db_path) as store:
        runner = SimpleTaskRunner(config, store, working_dir=str(repo))
        run, payload = runner.run("annotate shard", files=["pkg"], isolation_mode="none")

        assert run.status.value == "completed"
        assert payload["manifest"]["completed_items"] == 1


def test_simple_run_respects_global_execution_lease_limit(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "a.py").write_text("print('a')\n", encoding="utf-8")
    (repo / "b.py").write_text("print('b')\n", encoding="utf-8")
    db_path = tmp_path / "state.db"

    config = Config()
    config.checkpoint.db_path = str(db_path)
    config.simple.manifest_dir = str(tmp_path / "runs")
    config.simple.copy_root_dir = str(tmp_path / "copies")
    config.simple.max_running_processes = 2
    config.simple.global_max_running_processes = 1
    config.simple.prepare_workers = 2
    config.simple.max_prepared_items = 2
    config.simple.execution_lease_db_path = str(tmp_path / "leases.sqlite3")
    config.simple.claude_home_isolation = "none"

    active = 0
    peak = 0
    active_lock = threading.Lock()

    def fake_execute(self, prepared, worker_id):
        nonlocal active, peak
        with active_lock:
            active += 1
            peak = max(peak, active)
        time.sleep(0.2)
        original = prepared.target_path.read_text(encoding="utf-8")
        prepared.target_path.write_text(f"# note\n{original}", encoding="utf-8")
        with active_lock:
            active -= 1
        from claude_orchestrator.model import TaskResult, TaskStatus
        from claude_orchestrator.simple_executor import ExecutionOutcome
        from claude_orchestrator.simple_model import SimpleAttempt, SimpleItemStatus

        result = TaskResult(task_id=prepared.item.item_id, status=TaskStatus.SUCCESS, output="ok")
        attempt = SimpleAttempt(
            item_id=prepared.item.item_id,
            attempt=prepared.item.attempt_state.attempt,
            status=SimpleItemStatus.RUNNING,
            worker_id=worker_id,
        )
        return ExecutionOutcome(result=result, attempt=attempt, changed_files=[prepared.item.target], prompt="test")

    monkeypatch.setattr("claude_orchestrator.simple_executor.SimpleExecutor.execute", fake_execute)

    with Store(config.checkpoint.db_path) as store:
        runner = SimpleTaskRunner(config, store, working_dir=str(repo))
        run, payload = runner.run("annotate", files=["a.py", "b.py"], isolation_mode="none")

        assert run.status.value == "completed"
        assert payload["manifest"]["completed_items"] == 2
        assert peak == 1


def test_simple_run_fills_available_global_execution_leases(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    for name in ("a.py", "b.py", "c.py", "d.py"):
        (repo / name).write_text(f"print('{name}')\n", encoding="utf-8")
    db_path = tmp_path / "state.db"

    config = Config()
    config.checkpoint.db_path = str(db_path)
    config.simple.manifest_dir = str(tmp_path / "runs")
    config.simple.copy_root_dir = str(tmp_path / "copies")
    config.simple.max_running_processes = 4
    config.simple.global_max_running_processes = 2
    config.simple.prepare_workers = 8
    config.simple.max_prepared_items = 8
    config.simple.execution_lease_db_path = str(tmp_path / "leases.sqlite3")
    config.simple.dynamic_throttle_enabled = False
    config.simple.claude_home_isolation = "none"

    active = 0
    peak = 0
    active_lock = threading.Lock()

    def fake_execute(self, prepared, worker_id):
        nonlocal active, peak
        with active_lock:
            active += 1
            peak = max(peak, active)
        time.sleep(0.2)
        original = prepared.target_path.read_text(encoding="utf-8")
        prepared.target_path.write_text(f"# note\n{original}", encoding="utf-8")
        with active_lock:
            active -= 1
        from claude_orchestrator.model import TaskResult, TaskStatus
        from claude_orchestrator.simple_executor import ExecutionOutcome
        from claude_orchestrator.simple_model import SimpleAttempt, SimpleItemStatus

        result = TaskResult(task_id=prepared.item.item_id, status=TaskStatus.SUCCESS, output="ok")
        attempt = SimpleAttempt(
            item_id=prepared.item.item_id,
            attempt=prepared.item.attempt_state.attempt,
            status=SimpleItemStatus.RUNNING,
            worker_id=worker_id,
        )
        return ExecutionOutcome(result=result, attempt=attempt, changed_files=[prepared.item.target], prompt="test")

    monkeypatch.setattr("claude_orchestrator.simple_executor.SimpleExecutor.execute", fake_execute)

    with Store(config.checkpoint.db_path) as store:
        runner = SimpleTaskRunner(config, store, working_dir=str(repo))
        run, payload = runner.run("annotate", files=["a.py", "b.py", "c.py", "d.py"], isolation_mode="none")

        assert run.status.value == "completed"
        assert payload["manifest"]["completed_items"] == 4
        assert peak == 2


def test_simple_resume_uses_original_working_dir(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    target = repo / "a.py"
    target.write_text("print('a')\n", encoding="utf-8")
    db_path = tmp_path / "state.db"

    config = Config()
    config.checkpoint.db_path = str(db_path)
    config.simple.manifest_dir = str(tmp_path / "runs")
    config.simple.copy_root_dir = str(tmp_path / "copies")
    config.simple.claude_home_isolation = "none"

    def fake_execute(self, prepared, worker_id):
        assert prepared.cwd == repo
        prepared.target_path.write_text("# note\nprint('a')\n", encoding="utf-8")
        from claude_orchestrator.model import TaskResult, TaskStatus
        from claude_orchestrator.simple_executor import ExecutionOutcome
        from claude_orchestrator.simple_model import SimpleAttempt, SimpleItemStatus

        result = TaskResult(task_id=prepared.item.item_id, status=TaskStatus.SUCCESS, output="ok")
        attempt = SimpleAttempt(
            item_id=prepared.item.item_id,
            attempt=prepared.item.attempt_state.attempt,
            status=SimpleItemStatus.RUNNING,
            worker_id=worker_id,
        )
        return ExecutionOutcome(result=result, attempt=attempt, changed_files=[prepared.item.target], prompt="test")

    monkeypatch.setattr("claude_orchestrator.simple_executor.SimpleExecutor.execute", fake_execute)

    from claude_orchestrator.simple_model import SimpleItemStatus

    with Store(config.checkpoint.db_path) as store:
        runner = SimpleTaskRunner(config, store, working_dir=str(repo))
        run, _ = runner.run("annotate", files=["a.py"], isolation_mode="none")
        target.write_text("print('a')\n", encoding="utf-8")
        store.reset_simple_items(run.run_id, [SimpleItemStatus.SUCCEEDED], SimpleItemStatus.READY)
        resumed_run, payload = runner.resume(run.run_id, retry_failed=False)

        assert resumed_run.status.value == "completed"
        assert payload["manifest"]["completed_items"] == 1


def test_simple_run_reuses_execution_worker_slots_for_claude_home(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    for name in ("a.py", "b.py", "c.py", "d.py"):
        (repo / name).write_text(f"print('{name}')\n", encoding="utf-8")
    db_path = tmp_path / "state.db"

    config = Config()
    config.checkpoint.db_path = str(db_path)
    config.simple.manifest_dir = str(tmp_path / "runs")
    config.simple.copy_root_dir = str(tmp_path / "copies")
    config.simple.max_running_processes = 2
    config.simple.prepare_workers = 6
    config.simple.max_prepared_items = 6

    warmed_worker_ids: list[str] = []
    warmed_lock = threading.Lock()

    def fake_warm_worker_home(self, worker_id):
        with warmed_lock:
            warmed_worker_ids.append(worker_id)
        return None

    def fake_execute(self, prepared, worker_id):
        time.sleep(0.05)
        original = prepared.target_path.read_text(encoding="utf-8")
        prepared.target_path.write_text(f"# note\n{original}", encoding="utf-8")
        from claude_orchestrator.model import TaskResult, TaskStatus
        from claude_orchestrator.simple_executor import ExecutionOutcome
        from claude_orchestrator.simple_model import SimpleAttempt, SimpleItemStatus

        result = TaskResult(task_id=prepared.item.item_id, status=TaskStatus.SUCCESS, output="ok")
        attempt = SimpleAttempt(
            item_id=prepared.item.item_id,
            attempt=prepared.item.attempt_state.attempt,
            status=SimpleItemStatus.RUNNING,
            worker_id=worker_id,
        )
        return ExecutionOutcome(result=result, attempt=attempt, changed_files=[prepared.item.target], prompt="test")

    monkeypatch.setattr("claude_orchestrator.simple_executor.SimpleExecutor.warm_worker_home", fake_warm_worker_home)
    monkeypatch.setattr("claude_orchestrator.simple_executor.SimpleExecutor.execute", fake_execute)

    with Store(config.checkpoint.db_path) as store:
        runner = SimpleTaskRunner(config, store, working_dir=str(repo))
        run, payload = runner.run("annotate", files=["a.py", "b.py", "c.py", "d.py"], isolation_mode="none")

        assert run.status.value == "completed"
        assert payload["manifest"]["completed_items"] == 4
        assert sorted(set(warmed_worker_ids)) == ["exec-slot-00", "exec-slot-01"]
        assert len(warmed_worker_ids) == 2


def test_simple_run_only_prewarms_slots_needed_by_schedulable_items(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "a.py").write_text("print('a')\n", encoding="utf-8")
    db_path = tmp_path / "state.db"

    config = Config()
    config.checkpoint.db_path = str(db_path)
    config.simple.manifest_dir = str(tmp_path / "runs")
    config.simple.copy_root_dir = str(tmp_path / "copies")
    config.simple.max_running_processes = 16
    config.simple.prepare_workers = 4
    config.simple.max_prepared_items = 4

    warmed_worker_ids: list[str] = []

    def fake_warm_worker_home(self, worker_id):
        warmed_worker_ids.append(worker_id)
        return None

    def fake_execute(self, prepared, worker_id):
        prepared.target_path.write_text("# note\nprint('a')\n", encoding="utf-8")
        from claude_orchestrator.model import TaskResult, TaskStatus
        from claude_orchestrator.simple_executor import ExecutionOutcome
        from claude_orchestrator.simple_model import SimpleAttempt, SimpleItemStatus

        result = TaskResult(task_id=prepared.item.item_id, status=TaskStatus.SUCCESS, output="ok")
        attempt = SimpleAttempt(
            item_id=prepared.item.item_id,
            attempt=prepared.item.attempt_state.attempt,
            status=SimpleItemStatus.RUNNING,
            worker_id=worker_id,
        )
        return ExecutionOutcome(result=result, attempt=attempt, changed_files=[prepared.item.target], prompt="test")

    monkeypatch.setattr("claude_orchestrator.simple_executor.SimpleExecutor.warm_worker_home", fake_warm_worker_home)
    monkeypatch.setattr("claude_orchestrator.simple_executor.SimpleExecutor.execute", fake_execute)

    with Store(config.checkpoint.db_path) as store:
        runner = SimpleTaskRunner(config, store, working_dir=str(repo))
        run, payload = runner.run("annotate", files=["a.py"], isolation_mode="none")

        assert run.status.value == "completed"
        assert payload["manifest"]["completed_items"] == 1
        assert warmed_worker_ids == ["exec-slot-00"]
        events = sorted(store.get_simple_events(run.run_id, limit=10), key=lambda event: event["event_id"])
        run_started = next(event for event in events if event["event_type"] == "run_started")
        assert run_started["data"]["prepare_workers"] == 1
        assert run_started["data"]["prepared_capacity"] == 1
        assert run_started["data"]["effective_exec_slots"] == 1


def test_simple_run_does_not_block_dispatch_on_execution_slot_prewarm(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    for name in ("a.py", "b.py"):
        (repo / name).write_text(f"print('{name}')\n", encoding="utf-8")
    db_path = tmp_path / "state.db"

    config = Config()
    config.checkpoint.db_path = str(db_path)
    config.simple.manifest_dir = str(tmp_path / "runs")
    config.simple.copy_root_dir = str(tmp_path / "copies")
    config.simple.max_running_processes = 2
    config.simple.prepare_workers = 4
    config.simple.max_prepared_items = 4

    def fake_warm_worker_home(self, worker_id):
        time.sleep(0.05 if worker_id.endswith("00") else 0.35)
        return None

    def fake_execute(self, prepared, worker_id):
        original = prepared.target_path.read_text(encoding="utf-8")
        prepared.target_path.write_text(f"# note\n{original}", encoding="utf-8")
        from claude_orchestrator.model import TaskResult, TaskStatus
        from claude_orchestrator.simple_executor import ExecutionOutcome
        from claude_orchestrator.simple_model import SimpleAttempt, SimpleItemStatus

        result = TaskResult(task_id=prepared.item.item_id, status=TaskStatus.SUCCESS, output="ok")
        attempt = SimpleAttempt(
            item_id=prepared.item.item_id,
            attempt=prepared.item.attempt_state.attempt,
            status=SimpleItemStatus.RUNNING,
            worker_id=worker_id,
        )
        return ExecutionOutcome(result=result, attempt=attempt, changed_files=[prepared.item.target], prompt="test")

    monkeypatch.setattr("claude_orchestrator.simple_executor.SimpleExecutor.warm_worker_home", fake_warm_worker_home)
    monkeypatch.setattr("claude_orchestrator.simple_executor.SimpleExecutor.execute", fake_execute)

    with Store(config.checkpoint.db_path) as store:
        runner = SimpleTaskRunner(config, store, working_dir=str(repo))
        run, payload = runner.run("annotate", files=["a.py", "b.py"], isolation_mode="none")

        assert run.status.value == "completed"
        assert payload["manifest"]["completed_items"] == 2

        events = sorted(store.get_simple_events(run.run_id, limit=20), key=lambda event: event["event_id"])
        item_started_index = next(
            index for index, event in enumerate(events) if event["event_type"] == "item_started"
        )
        warmed_index = next(
            index for index, event in enumerate(events) if event["event_type"] == "execution_slots_warmed"
        )
        assert item_started_index < warmed_index


def test_dynamic_throttle_ignores_retry_wait_backlog(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "a.py").write_text("print('a')\n", encoding="utf-8")
    db_path = tmp_path / "state.db"

    config = Config()
    config.checkpoint.db_path = str(db_path)
    config.simple.manifest_dir = str(tmp_path / "runs")
    config.simple.copy_root_dir = str(tmp_path / "copies")
    config.simple.max_running_processes = 8
    config.simple.claude_home_isolation = "none"

    monkeypatch.setattr("claude_orchestrator.simple_runtime.psutil", None)

    from claude_orchestrator.simple_model import AttemptState, SimpleItemStatus, SimpleItemType, SimpleManifest, SimpleWorkItem
    from claude_orchestrator.simple_scheduler import SimpleScheduler

    retry_item = SimpleWorkItem(
        item_id="retry-item",
        item_type=SimpleItemType.FILE,
        target="a.py",
        bucket="src",
        priority=0,
        instruction="annotate",
        attempt_state=AttemptState(max_attempts=3),
        timeout_seconds=30,
        status=SimpleItemStatus.RETRY_WAIT,
    )

    with Store(config.checkpoint.db_path) as store:
        runner = SimpleTaskRunner(config, store, working_dir=str(repo))
        runner._throttle_limit = 2
        scheduler = SimpleScheduler([retry_item], max_pending_tasks=10)
        manifest = SimpleManifest(run_id="run-test", total_items=1, isolation_mode="none", input_sources={})
        runner._adjust_dynamic_limit("run-test", manifest, scheduler)
        assert runner._throttle_limit == 8


def test_simple_status_payload_recovers_stale_run_and_resets_transient_items(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "a.py").write_text("print('a')\n", encoding="utf-8")
    db_path = tmp_path / "state.db"

    config = Config()
    config.checkpoint.db_path = str(db_path)
    config.simple.manifest_dir = str(tmp_path / "runs")
    config.simple.copy_root_dir = str(tmp_path / "copies")
    config.simple.stale_run_timeout_seconds = 1

    from claude_orchestrator.simple_model import SimpleItemStatus, SimpleItemType, SimpleRun, SimpleRunStatus, SimpleWorkItem
    from claude_orchestrator.simple_store import SimpleStore

    stale_at = datetime.now() - timedelta(minutes=10)
    run = SimpleRun(
        instruction_template="annotate",
        working_dir=str(repo),
        started_at=stale_at,
        last_heartbeat_at=stale_at,
        status=SimpleRunStatus.RUNNING,
    )
    item = SimpleWorkItem(
        item_id="stale-item",
        item_type=SimpleItemType.FILE,
        target="a.py",
        bucket="root",
        priority=0,
        instruction="annotate",
        status=SimpleItemStatus.EXECUTING,
    )

    with Store(config.checkpoint.db_path) as store:
        simple_store = SimpleStore(store)
        simple_store.create_run(run, [item])

        runner = SimpleTaskRunner(config, store, working_dir=str(repo))
        payload = runner.status_payload(run.run_id)

        refreshed_run = store.get_simple_run(run.run_id)
        refreshed_item = store.get_simple_item(run.run_id, item.item_id)
        assert refreshed_run is not None
        assert refreshed_item is not None
        assert refreshed_run.status.value == "failed"
        assert refreshed_item.status.value == "ready"
        assert payload["status"] == "failed"
        events = store.get_simple_events(run.run_id, limit=10)
        assert any(event["event_type"] == "stale_run_recovered" for event in events)


def test_simple_run_heartbeat_pump_prevents_false_stale_recovery(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    target = repo / "a.py"
    target.write_text("print('a')\n", encoding="utf-8")
    db_path = tmp_path / "state.db"

    config = Config()
    config.checkpoint.db_path = str(db_path)
    config.simple.manifest_dir = str(tmp_path / "runs")
    config.simple.copy_root_dir = str(tmp_path / "copies")
    config.simple.claude_home_isolation = "none"
    config.simple.run_heartbeat_interval_seconds = 1
    config.simple.stale_run_timeout_seconds = 2

    def fake_execute(self, prepared, worker_id):
        time.sleep(3.2)
        prepared.target_path.write_text("# note\nprint('a')\n", encoding="utf-8")
        from claude_orchestrator.model import TaskResult, TaskStatus
        from claude_orchestrator.simple_executor import ExecutionOutcome
        from claude_orchestrator.simple_model import SimpleAttempt, SimpleItemStatus

        result = TaskResult(task_id=prepared.item.item_id, status=TaskStatus.SUCCESS, output="ok")
        attempt = SimpleAttempt(
            item_id=prepared.item.item_id,
            attempt=prepared.item.attempt_state.attempt,
            status=SimpleItemStatus.RUNNING,
            worker_id=worker_id,
        )
        return ExecutionOutcome(result=result, attempt=attempt, changed_files=[prepared.item.target], prompt="test")

    monkeypatch.setattr("claude_orchestrator.simple_executor.SimpleExecutor.execute", fake_execute)

    outcome: dict[str, object] = {}
    failure: list[Exception] = []

    with Store(config.checkpoint.db_path) as store:
        runner = SimpleTaskRunner(config, store, working_dir=str(repo))
        runner._maintenance_tick = lambda run, force=False: None  # type: ignore[method-assign]

        def _run() -> None:
            try:
                outcome["result"] = runner.run("annotate", files=["a.py"], isolation_mode="none")
            except Exception as exc:  # pragma: no cover - 调试保护
                failure.append(exc)

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()

        time.sleep(2.5)
        with Store(config.checkpoint.db_path) as observer_store:
            observer = SimpleTaskRunner(config, observer_store, working_dir=str(repo))
            assert observer.recover_stale_runs() == []
            run_id = observer_store._conn.execute(
                "SELECT run_id FROM simple_runs ORDER BY started_at DESC LIMIT 1"
            ).fetchone()[0]
            live_run = observer_store.get_simple_run(run_id)
            assert live_run is not None
            assert live_run.status.value == "running"
            assert live_run.last_heartbeat_at is not None
            assert live_run.last_heartbeat_at > live_run.started_at

        thread.join(timeout=10)

    assert failure == []
    assert thread.is_alive() is False
    run, payload = outcome["result"]  # type: ignore[misc]
    assert run.status.value == "completed"
    assert payload["manifest"]["completed_items"] == 1
