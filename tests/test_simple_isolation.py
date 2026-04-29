from __future__ import annotations

import threading
import time
from pathlib import Path

from claude_orchestrator.config import Config
from claude_orchestrator.simple_isolation import SimpleIsolationManager
from claude_orchestrator.simple_model import AttemptState, SimpleItemType, SimpleWorkItem
from claude_orchestrator.simple_runtime import SimpleRuntimeLayout


def test_copy_mode_syncs_only_relevant_scope(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    (repo / "pkg").mkdir(parents=True)
    (repo / "other").mkdir(parents=True)
    (repo / "pkg" / "target.py").write_text("print('t')\n", encoding="utf-8")
    (repo / "pkg" / "helper.py").write_text("print('h')\n", encoding="utf-8")
    (repo / "other" / "ignored.py").write_text("print('x')\n", encoding="utf-8")

    config = Config()
    config.simple.copy_root_dir = str(tmp_path / "copies")
    layout = SimpleRuntimeLayout.create(tmp_path / "run")
    manager = SimpleIsolationManager(repo, layout, config.simple, "run123", "copy")
    item = SimpleWorkItem(
        item_id="a",
        item_type=SimpleItemType.FILE,
        target="pkg/target.py",
        bucket="pkg",
        priority=0,
        instruction="annotate",
        attempt_state=AttemptState(max_attempts=3),
        timeout_seconds=30,
    )

    prepared = manager.prepare(item)

    assert prepared.target_path.exists()
    assert not (prepared.cwd / "pkg" / "helper.py").exists()
    assert not (prepared.cwd / "other" / "ignored.py").exists()


def test_copy_mode_uses_item_scoped_workspace(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    (repo / "pkg").mkdir(parents=True)
    (repo / "pkg" / "a.py").write_text("print('a')\n", encoding="utf-8")
    (repo / "pkg" / "b.py").write_text("print('b')\n", encoding="utf-8")

    config = Config()
    config.simple.copy_root_dir = str(tmp_path / "copies")
    layout = SimpleRuntimeLayout.create(tmp_path / "run")
    manager = SimpleIsolationManager(repo, layout, config.simple, "run123", "copy")
    item_a = SimpleWorkItem(
        item_id="item-a",
        item_type=SimpleItemType.FILE,
        target="pkg/a.py",
        bucket="pkg",
        priority=0,
        instruction="annotate",
        attempt_state=AttemptState(max_attempts=3),
        timeout_seconds=30,
    )
    item_b = SimpleWorkItem(
        item_id="item-b",
        item_type=SimpleItemType.FILE,
        target="pkg/b.py",
        bucket="pkg",
        priority=0,
        instruction="annotate",
        attempt_state=AttemptState(max_attempts=3),
        timeout_seconds=30,
    )

    prepared_a = manager.prepare(item_a)
    prepared_b = manager.prepare(item_b)

    assert prepared_a.cwd != prepared_b.cwd
    assert prepared_a.cwd == Path(config.simple.copy_root_dir) / "run123" / "item-a" / "workspace"
    assert prepared_b.cwd == Path(config.simple.copy_root_dir) / "run123" / "item-b" / "workspace"

    prepared_a.target_path.write_text("# note\nprint('a')\n", encoding="utf-8")
    assert prepared_a.collect_changed_files() == ["pkg/a.py"]
    assert prepared_b.collect_changed_files() == []


def test_copy_mode_skips_repo_status_baseline(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "a.py").write_text("print('a')\n", encoding="utf-8")

    config = Config()
    config.simple.copy_root_dir = str(tmp_path / "copies")
    layout = SimpleRuntimeLayout.create(tmp_path / "run")
    manager = SimpleIsolationManager(repo, layout, config.simple, "run123", "copy")
    item = SimpleWorkItem(
        item_id="a",
        item_type=SimpleItemType.FILE,
        target="a.py",
        bucket=".",
        priority=0,
        instruction="annotate",
        attempt_state=AttemptState(max_attempts=3),
        timeout_seconds=30,
    )

    calls = {"git_root": 0, "git_status": 0}

    def fake_git_root(path: Path) -> Path:
        calls["git_root"] += 1
        raise AssertionError("copy mode should not resolve git root")

    def fake_run(cmd, cwd=None, **kwargs):
        if cmd[:2] == ["git", "status"]:
            calls["git_status"] += 1
            raise AssertionError("copy mode should not call git status")
        raise AssertionError(f"unexpected subprocess call: {cmd}")

    monkeypatch.setattr("claude_orchestrator.simple_isolation._git_repo_root", fake_git_root)
    monkeypatch.setattr("claude_orchestrator.simple_isolation.subprocess.run", fake_run)

    manager.prepare(item)

    assert calls["git_root"] == 0
    assert calls["git_status"] == 0


def test_none_mode_caches_repo_status_baseline(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "a.py").write_text("print('a')\n", encoding="utf-8")

    config = Config()
    layout = SimpleRuntimeLayout.create(tmp_path / "run")
    manager = SimpleIsolationManager(repo, layout, config.simple, "run123", "none")
    item = SimpleWorkItem(
        item_id="a",
        item_type=SimpleItemType.FILE,
        target="a.py",
        bucket=".",
        priority=0,
        instruction="annotate",
        attempt_state=AttemptState(max_attempts=3),
        timeout_seconds=30,
    )

    calls = {"git_status": 0}

    def fake_git_root(path: Path) -> Path:
        return repo

    class Result:
        def __init__(self, stdout: str = ""):
            self.stdout = stdout

    def fake_run(cmd, cwd=None, **kwargs):
        if cmd[:2] == ["git", "status"]:
            calls["git_status"] += 1
            return Result("")
        raise AssertionError(f"unexpected subprocess call: {cmd}")

    monkeypatch.setattr("claude_orchestrator.simple_isolation._git_repo_root", fake_git_root)
    monkeypatch.setattr("claude_orchestrator.simple_isolation.subprocess.run", fake_run)

    manager.prepare(item)
    manager.prepare(item)

    assert calls["git_status"] == 1


def test_none_mode_baseline_cache_is_singleflight_under_concurrency(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "a.py").write_text("print('a')\n", encoding="utf-8")

    config = Config()
    layout = SimpleRuntimeLayout.create(tmp_path / "run")
    manager = SimpleIsolationManager(repo, layout, config.simple, "run123", "none")
    item = SimpleWorkItem(
        item_id="a",
        item_type=SimpleItemType.FILE,
        target="a.py",
        bucket=".",
        priority=0,
        instruction="annotate",
        attempt_state=AttemptState(max_attempts=3),
        timeout_seconds=30,
    )

    calls = {"git_status": 0}
    lock = threading.Lock()

    def fake_git_root(path: Path) -> Path:
        return repo

    class Result:
        def __init__(self, stdout: str = ""):
            self.stdout = stdout

    def fake_run(cmd, cwd=None, **kwargs):
        if cmd[:2] == ["git", "status"]:
            time.sleep(0.05)
            with lock:
                calls["git_status"] += 1
            return Result("")
        raise AssertionError(f"unexpected subprocess call: {cmd}")

    monkeypatch.setattr("claude_orchestrator.simple_isolation._git_repo_root", fake_git_root)
    monkeypatch.setattr("claude_orchestrator.simple_isolation.subprocess.run", fake_run)

    threads = [threading.Thread(target=manager.prepare, args=(item,)) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert calls["git_status"] == 1


def test_none_mode_collect_changed_files_falls_back_without_git_repo(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "a.py").write_text("print('a')\n", encoding="utf-8")
    (repo / "b.py").write_text("print('b')\n", encoding="utf-8")

    config = Config()
    layout = SimpleRuntimeLayout.create(tmp_path / "run")
    manager = SimpleIsolationManager(repo, layout, config.simple, "run123", "none")
    item = SimpleWorkItem(
        item_id="a",
        item_type=SimpleItemType.FILE,
        target="a.py",
        bucket=".",
        priority=0,
        instruction="annotate",
        attempt_state=AttemptState(max_attempts=3),
        timeout_seconds=30,
    )

    prepared = manager.prepare(item)
    (repo / "a.py").write_text("# note\nprint('a')\n", encoding="utf-8")

    assert prepared.collect_changed_files() == ["a.py"]


def test_none_mode_collect_changed_files_ignores_unrelated_parallel_changes(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    (repo / "pkg").mkdir(parents=True)
    (repo / "other").mkdir(parents=True)
    (repo / "pkg" / "target.py").write_text("print('a')\n", encoding="utf-8")
    (repo / "pkg" / "helper.py").write_text("print('h')\n", encoding="utf-8")
    (repo / "other" / "neighbor.py").write_text("print('n')\n", encoding="utf-8")

    config = Config()
    layout = SimpleRuntimeLayout.create(tmp_path / "run")
    manager = SimpleIsolationManager(repo, layout, config.simple, "run123", "none")
    item = SimpleWorkItem(
        item_id="a",
        item_type=SimpleItemType.FILE,
        target="pkg/target.py",
        bucket="pkg",
        priority=0,
        instruction="annotate",
        attempt_state=AttemptState(max_attempts=3),
        timeout_seconds=30,
    )

    prepared = manager.prepare(item)
    (repo / "pkg" / "target.py").write_text("# note\nprint('a')\n", encoding="utf-8")
    (repo / "other" / "neighbor.py").write_text("# drift\nprint('n')\n", encoding="utf-8")

    assert prepared.collect_changed_files() == ["pkg/target.py"]


def test_none_mode_collect_changed_files_keeps_local_sibling_detection(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    (repo / "pkg").mkdir(parents=True)
    (repo / "pkg" / "target.py").write_text("print('a')\n", encoding="utf-8")
    (repo / "pkg" / "helper.py").write_text("print('h')\n", encoding="utf-8")

    config = Config()
    layout = SimpleRuntimeLayout.create(tmp_path / "run")
    manager = SimpleIsolationManager(repo, layout, config.simple, "run123", "none")
    item = SimpleWorkItem(
        item_id="a",
        item_type=SimpleItemType.FILE,
        target="pkg/target.py",
        bucket="pkg",
        priority=0,
        instruction="annotate",
        attempt_state=AttemptState(max_attempts=3),
        timeout_seconds=30,
    )

    prepared = manager.prepare(item)
    (repo / "pkg" / "target.py").write_text("# note\nprint('a')\n", encoding="utf-8")
    (repo / "pkg" / "helper.py").write_text("# side\nprint('h')\n", encoding="utf-8")

    assert prepared.collect_changed_files() == ["pkg/helper.py", "pkg/target.py"]


def test_none_mode_file_item_does_not_rescan_dirty_parent_repo(tmp_path: Path, monkeypatch) -> None:
    outer_repo = tmp_path / "outer"
    workspace = outer_repo / "workspace"
    workspace.mkdir(parents=True)
    (outer_repo / "README.md").write_text("dirty parent repo\n", encoding="utf-8")
    (workspace / "sample.py").write_text("print('a')\n", encoding="utf-8")

    config = Config()
    layout = SimpleRuntimeLayout.create(tmp_path / "run")
    manager = SimpleIsolationManager(workspace, layout, config.simple, "run123", "none")
    item = SimpleWorkItem(
        item_id="a",
        item_type=SimpleItemType.FILE,
        target="sample.py",
        bucket=".",
        priority=0,
        instruction="annotate",
        attempt_state=AttemptState(max_attempts=3),
        timeout_seconds=30,
    )

    calls = {"git_status": 0}

    def fake_git_root(path: Path) -> Path:
        return outer_repo

    class Result:
        stdout = "?? README.md\n?? workspace/sample.py\n"

    def fake_run(cmd, cwd=None, **kwargs):
        if cmd[:2] == ["git", "status"]:
            calls["git_status"] += 1
            return Result()
        raise AssertionError(f"unexpected subprocess call: {cmd}")

    monkeypatch.setattr("claude_orchestrator.simple_isolation._git_repo_root", fake_git_root)
    monkeypatch.setattr("claude_orchestrator.simple_isolation.subprocess.run", fake_run)

    prepared = manager.prepare(item)
    (workspace / "sample.py").write_text("# note\nprint('a')\n", encoding="utf-8")

    assert prepared.collect_changed_files() == ["sample.py"]
    assert calls["git_status"] == 1


def test_none_mode_file_fallback_ignores_runtime_paths(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "a.py").write_text("print('a')\n", encoding="utf-8")
    (repo / "state.db").write_text("before\n", encoding="utf-8")

    config = Config()
    layout = SimpleRuntimeLayout.create(tmp_path / "run")
    manager = SimpleIsolationManager(
        repo,
        layout,
        config.simple,
        "run123",
        "none",
        ignored_repo_paths={"state.db"},
    )
    item = SimpleWorkItem(
        item_id="a",
        item_type=SimpleItemType.FILE,
        target="a.py",
        bucket=".",
        priority=0,
        instruction="annotate",
        attempt_state=AttemptState(max_attempts=3),
        timeout_seconds=30,
    )

    prepared = manager.prepare(item)
    (repo / "a.py").write_text("# note\nprint('a')\n", encoding="utf-8")
    (repo / "state.db").write_text("after\n", encoding="utf-8")

    assert prepared.collect_changed_files() == ["a.py"]


def test_worktree_mode_falls_back_to_copy_when_git_root_missing(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "a.py").write_text("print('a')\n", encoding="utf-8")

    config = Config()
    config.simple.copy_root_dir = str(tmp_path / "copies")
    layout = SimpleRuntimeLayout.create(tmp_path / "run")
    manager = SimpleIsolationManager(repo, layout, config.simple, "run123", "worktree")
    item = SimpleWorkItem(
        item_id="a",
        item_type=SimpleItemType.FILE,
        target="a.py",
        bucket=".",
        priority=0,
        instruction="annotate",
        attempt_state=AttemptState(max_attempts=3),
        timeout_seconds=30,
    )

    monkeypatch.setattr("claude_orchestrator.simple_isolation._git_repo_root", lambda path: None)

    prepared = manager.prepare(item)

    assert prepared.effective_mode == "copy"
    assert prepared.target_path == Path(config.simple.copy_root_dir) / "run123" / "a" / "workspace" / "a.py"
    assert prepared.target_path.exists()
    assert "worktree 自动降级到 copy" in prepared.warnings[0]


def test_copy_back_returns_validation_error_when_target_missing(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    (repo / ".github" / "scripts").mkdir(parents=True)
    target = repo / ".github" / "scripts" / "select-release-milestone.py"
    target.write_text("print('a')\n", encoding="utf-8")

    config = Config()
    config.simple.copy_root_dir = str(tmp_path / "copies")
    layout = SimpleRuntimeLayout.create(tmp_path / "run")
    manager = SimpleIsolationManager(repo, layout, config.simple, "run123", "copy")
    item = SimpleWorkItem(
        item_id="item-a",
        item_type=SimpleItemType.FILE,
        target=".github/scripts/select-release-milestone.py",
        bucket=".github/scripts",
        priority=0,
        instruction="annotate",
        attempt_state=AttemptState(max_attempts=3),
        timeout_seconds=30,
    )

    prepared = manager.prepare(item)
    assert prepared.target_path.exists()
    prepared.target_path.unlink()

    ok, reason = manager.copy_back(prepared, [])

    assert ok is False
    assert reason == "target missing during copy-back"
