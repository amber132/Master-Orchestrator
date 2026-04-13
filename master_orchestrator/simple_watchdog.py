"""External watchdog for unattended simple-mode runs."""

from __future__ import annotations

import csv
import json
import logging
import os
import shlex
import signal
import subprocess
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable

from .failover_pool import load_failover_pool_config, load_pool_state
from .pool_supervisor import (
    ENV_POOL_ACTIVE_PROFILE,
    ENV_POOL_CHILD,
    ENV_POOL_FIXED_PROFILE,
    ENV_POOL_REQUEST_FILE,
    ENV_POOL_STATE_FILE,
)
from .simple_completion import (
    build_unresolved_fingerprint,
    completion_exit_reason,
    count_failure_categories,
    SimpleCompletionSnapshot,
    SimpleCompletionState,
    track_completion_state,
)
from .simple_model import SimpleItemStatus, SimpleRun, SimpleRunStatus
from .store import Store

logger = logging.getLogger(__name__)

ACTIVE_RUN_STATUSES = {
    SimpleRunStatus.QUEUED.value,
    SimpleRunStatus.SCANNING.value,
    SimpleRunStatus.READY.value,
    SimpleRunStatus.RUNNING.value,
    SimpleRunStatus.DRAINING.value,
}
SUCCESS_RUN_STATUSES = {SimpleRunStatus.COMPLETED.value}
TERMINAL_RUN_STATUSES = SUCCESS_RUN_STATUSES | {
    SimpleRunStatus.PARTIAL_SUCCESS.value,
    SimpleRunStatus.FAILED.value,
    SimpleRunStatus.CANCELLED.value,
}
TERMINAL_ITEM_STATUSES = {"succeeded", "failed", "blocked", "skipped"}


@dataclass
class SimpleWatchTarget:
    repo: str
    working_dir: str = ""
    run_id: str = ""
    session: str = ""
    log_file: str = ""
    exit_file: str = ""
    task_file: str = ""
    wave: int = 1
    restart_count: int = 0
    latest_status: str = ""
    done: bool = False
    exhausted: bool = False
    last_terminal_count: int = 0
    last_progress_at: float = 0.0
    mismatch_since: float = 0.0
    cooldown_until: float = 0.0
    pool_id: str = ""
    active_profile: str = ""
    failback_pending: bool = False
    pool_state_file: str = ""
    pool_request_file: str = ""
    last_completed_count: int = 0
    last_unresolved_count: int = 0
    last_failure_fingerprint: str = ""
    stagnant_waves: int = 0
    identical_failure_waves: int = 0
    exhausted_reason: str = ""


@dataclass
class SimpleRunSnapshot:
    run: SimpleRun | None
    counts: dict[str, int] = field(default_factory=dict)
    latest_event_ts: datetime | None = None
    runner_pids: list[int] = field(default_factory=list)
    real_exec_count: int = 0
    completed_count: int = 0
    unresolved_count: int = 0
    unresolved_fingerprint: str = ""
    unresolved_categories: dict[str, int] = field(default_factory=dict)

    @property
    def terminal_count(self) -> int:
        return sum(self.counts.get(status, 0) for status in TERMINAL_ITEM_STATUSES)

    @property
    def db_executing_count(self) -> int:
        return self.counts.get("executing", 0) + self.counts.get("validating", 0)


@dataclass
class SimpleWatchOptions:
    config_path: str
    project_root: str
    poll_seconds: int = 60
    max_waves: int = 8
    progress_stall_seconds: int = 900
    event_stall_seconds: int = 300
    heartbeat_stall_seconds: int = 180
    exec_mismatch_grace_seconds: int = 180
    exec_mismatch_ratio: float = 0.2
    exec_mismatch_min_executing: int = 4
    restart_cooldown_seconds: int = 120
    stop_when_all_done: bool = True
    completion_until_clean_enabled: bool = True
    completion_retry_cancelled_runs: bool = False
    completion_max_retry_waves: int = 0
    completion_max_stagnant_waves: int = 4
    completion_max_identical_failure_waves: int = 3
    state_file: str | None = None
    log_file: str | None = None
    pool_config: str | None = None
    pool_profile: str | None = None


def load_watch_targets_from_manifest(manifest_path: Path) -> dict[str, SimpleWatchTarget]:
    targets: dict[str, SimpleWatchTarget] = {}
    with manifest_path.open("r", encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            repo = row.get("repo") or row.get("run_id") or row.get("repo_path") or f"target-{len(targets) + 1}"
            targets[repo] = SimpleWatchTarget(
                repo=repo,
                working_dir=row.get("repo_path") or row.get("working_dir") or "",
                run_id=row.get("run_id") or "",
                session=row.get("session") or "",
                log_file=row.get("log_file") or "",
                exit_file=row.get("exit_file") or "",
                task_file=row.get("task_file") or "",
            )
    return targets


def load_watch_state(state_path: Path, targets: dict[str, SimpleWatchTarget]) -> dict[str, SimpleWatchTarget]:
    if not state_path.exists():
        return targets
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    for repo, state in payload.items():
        if repo not in targets:
            continue
        target = targets[repo]
        for key, value in state.items():
            if hasattr(target, key):
                setattr(target, key, value)
    return targets


def save_watch_state(state_path: Path | None, targets: dict[str, SimpleWatchTarget]) -> None:
    if state_path is None:
        return
    payload = {repo: asdict(target) for repo, target in targets.items()}
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


class SimpleWatchdog:
    def __init__(
        self,
        store: Store,
        targets: dict[str, SimpleWatchTarget],
        options: SimpleWatchOptions,
        *,
        now_fn: Callable[[], float] | None = None,
        sleep_fn: Callable[[float], None] | None = None,
    ) -> None:
        self._store = store
        self._targets = targets
        self._options = options
        self._project_root = Path(options.project_root).resolve()
        self._config_path = Path(options.config_path).resolve()
        self._state_path = Path(options.state_file).resolve() if options.state_file else None
        self._watch_log_path = Path(options.log_file).resolve() if options.log_file else None
        self._pool_config_path = Path(options.pool_config).resolve() if options.pool_config else None
        self._pool_config = load_failover_pool_config(self._pool_config_path) if self._pool_config_path else None
        self._now = now_fn or time.time
        self._sleep = sleep_fn or time.sleep

    def run(self) -> int:
        save_watch_state(self._state_path, self._targets)
        self._log("watchdog started")
        while True:
            all_settled = self.poll_once()
            if all_settled and self._options.stop_when_all_done:
                self._log("all targets settled")
                return 0
            self._sleep(max(1, self._options.poll_seconds))

    def poll_once(self) -> bool:
        now = self._now()
        aggregate_counts: dict[str, int] = {}
        total_real_execs = 0
        settled_targets = 0
        exhausted_targets = 0
        all_done = True

        for target in self._targets.values():
            self._ensure_pool_artifacts(target)
            snapshot = self._snapshot_for_target(target)
            target.done = False
            if snapshot.run is None:
                all_done = False
                continue

            target.run_id = snapshot.run.run_id
            target.latest_status = snapshot.run.status.value
            if snapshot.run.pool_id:
                target.pool_id = snapshot.run.pool_id
            if snapshot.run.active_profile:
                target.active_profile = snapshot.run.active_profile
            self._sync_target_pool_state(target)
            total_real_execs += snapshot.real_exec_count
            for status, count in snapshot.counts.items():
                aggregate_counts[status] = aggregate_counts.get(status, 0) + count

            action = self._pool_request_action(target, snapshot) or self._plan_action(target, snapshot, now)
            if action is not None:
                all_done = False
                self._perform_action(target, snapshot, action, now)
            elif target.done or target.exhausted:
                settled_targets += 1
            else:
                all_done = False

            if target.exhausted:
                exhausted_targets += 1

        save_watch_state(self._state_path, self._targets)
        status_line = " ".join(f"{status}={aggregate_counts[status]}" for status in sorted(aggregate_counts))
        self._log(
            f"summary settled={settled_targets}/{len(self._targets)} exhausted={exhausted_targets} real_exec={total_real_execs}"
            + (f" {status_line}" if status_line else "")
        )
        return all_done

    def _snapshot_for_target(self, target: SimpleWatchTarget) -> SimpleRunSnapshot:
        run = None
        if target.run_id:
            run = self._store.get_simple_run(target.run_id)
        if run is None and target.working_dir:
            run = self._store.get_latest_simple_run_for_working_dir(target.working_dir)
        if run is None:
            return SimpleRunSnapshot(run=None)

        if run.working_dir:
            target.working_dir = run.working_dir
        counts = self._store.get_simple_item_counts(run.run_id)
        latest_event = self._store.get_latest_simple_event(run.run_id)
        latest_event_ts = datetime.fromisoformat(latest_event["ts"]) if latest_event and latest_event.get("ts") else None
        runner_pids = self._find_runner_pids(target, run.run_id)
        real_exec_count = self._count_exec_processes(target, run.run_id, runner_pids)
        completed_count = counts.get(SimpleItemStatus.SUCCEEDED.value, 0)
        unresolved_count = counts.get(SimpleItemStatus.FAILED.value, 0) + counts.get(SimpleItemStatus.BLOCKED.value, 0)
        unresolved_fingerprint = ""
        unresolved_categories: dict[str, int] = {}
        if unresolved_count > 0 and run.status.value in TERMINAL_RUN_STATUSES:
            unresolved_items = self._store.get_simple_items(
                run.run_id,
                statuses=[SimpleItemStatus.FAILED, SimpleItemStatus.BLOCKED],
            )
            unresolved_categories = self._count_failure_categories(unresolved_items)
            unresolved_fingerprint = self._build_unresolved_fingerprint(unresolved_items)
        return SimpleRunSnapshot(
            run=run,
            counts=counts,
            latest_event_ts=latest_event_ts,
            runner_pids=runner_pids,
            real_exec_count=real_exec_count,
            completed_count=completed_count,
            unresolved_count=unresolved_count,
            unresolved_fingerprint=unresolved_fingerprint,
            unresolved_categories=unresolved_categories,
        )

    @staticmethod
    def _count_failure_categories(items) -> dict[str, int]:
        return count_failure_categories(items)

    @staticmethod
    def _build_unresolved_fingerprint(items) -> str:
        return build_unresolved_fingerprint(items)

    def _completion_exit_reason(
        self,
        target: SimpleWatchTarget,
        snapshot: SimpleRunSnapshot,
    ) -> str | None:
        return completion_exit_reason(
            SimpleCompletionState(
                wave=target.wave,
                last_completed_count=target.last_completed_count,
                last_unresolved_count=target.last_unresolved_count,
                last_failure_fingerprint=target.last_failure_fingerprint,
                stagnant_waves=target.stagnant_waves,
                identical_failure_waves=target.identical_failure_waves,
            ),
            SimpleCompletionSnapshot(
                completed_count=snapshot.completed_count,
                unresolved_count=snapshot.unresolved_count,
                unresolved_fingerprint=snapshot.unresolved_fingerprint,
                unresolved_categories=dict(snapshot.unresolved_categories),
            ),
            max_retry_waves=self._options.completion_max_retry_waves,
            max_stagnant_waves=self._options.completion_max_stagnant_waves,
            max_identical_failure_waves=self._options.completion_max_identical_failure_waves,
        )

    def _track_terminal_completion_state(
        self,
        target: SimpleWatchTarget,
        snapshot: SimpleRunSnapshot,
    ) -> None:
        state = SimpleCompletionState(
            wave=target.wave,
            last_completed_count=target.last_completed_count,
            last_unresolved_count=target.last_unresolved_count,
            last_failure_fingerprint=target.last_failure_fingerprint,
            stagnant_waves=target.stagnant_waves,
            identical_failure_waves=target.identical_failure_waves,
        )
        track_completion_state(
            state,
            SimpleCompletionSnapshot(
                completed_count=snapshot.completed_count,
                unresolved_count=snapshot.unresolved_count,
                unresolved_fingerprint=snapshot.unresolved_fingerprint,
                unresolved_categories=dict(snapshot.unresolved_categories),
            ),
        )
        target.last_completed_count = state.last_completed_count
        target.last_unresolved_count = state.last_unresolved_count
        target.last_failure_fingerprint = state.last_failure_fingerprint
        target.stagnant_waves = state.stagnant_waves
        target.identical_failure_waves = state.identical_failure_waves

    def _plan_action(
        self,
        target: SimpleWatchTarget,
        snapshot: SimpleRunSnapshot,
        now: float,
    ) -> tuple[str, str] | None:
        assert snapshot.run is not None
        run = snapshot.run
        status = run.status.value

        if snapshot.terminal_count > target.last_terminal_count:
            target.last_terminal_count = snapshot.terminal_count
            target.last_progress_at = now
            target.mismatch_since = 0.0
        elif target.last_progress_at <= 0:
            target.last_progress_at = now

        if status in SUCCESS_RUN_STATUSES:
            target.done = True
            target.exhausted = False
            target.exhausted_reason = ""
            return None

        if status in TERMINAL_RUN_STATUSES:
            if self._session_exists(target.session):
                return None
            if status == SimpleRunStatus.CANCELLED.value and not self._options.completion_retry_cancelled_runs:
                target.done = True
                target.exhausted = True
                target.exhausted_reason = "cancelled run will not be retried"
                return None
            if self._options.completion_until_clean_enabled:
                if snapshot.unresolved_count <= 0:
                    target.done = True
                    target.exhausted = False
                    target.exhausted_reason = ""
                    return None
                self._track_terminal_completion_state(target, snapshot)
                exit_reason = self._completion_exit_reason(target, snapshot)
                if exit_reason is not None:
                    target.exhausted = True
                    target.exhausted_reason = exit_reason
                    self._store.save_simple_event(
                        run.run_id,
                        "watch_completion_exhausted",
                        {
                            "repo": target.repo,
                            "status": status,
                            "wave": target.wave,
                            "reason": exit_reason,
                            "unresolved_count": snapshot.unresolved_count,
                            "completed_count": snapshot.completed_count,
                            "unresolved_categories": snapshot.unresolved_categories,
                        },
                        level="warning",
                    )
                    self._log(
                        f"completion exhausted repo={target.repo} run_id={run.run_id} status={status} "
                        f"wave={target.wave} reason={exit_reason}"
                    )
                    return None
                target.exhausted = False
                target.exhausted_reason = ""
                return ("retry", f"terminal status={status} unresolved={snapshot.unresolved_count}")
            if target.wave >= self._options.max_waves:
                target.exhausted = True
                target.exhausted_reason = f"max waves reached ({self._options.max_waves})"
                self._log(
                    f"max waves reached repo={target.repo} run_id={run.run_id} status={status}"
                )
                return None
            target.exhausted = False
            target.exhausted_reason = ""
            return ("retry", f"terminal status={status}")

        target.done = False
        target.exhausted = False
        session_alive = self._session_exists(target.session)
        heartbeat_at = run.last_heartbeat_at or run.started_at
        heartbeat_age = max(0.0, now - heartbeat_at.timestamp())
        event_age = max(
            0.0,
            now - (snapshot.latest_event_ts or run.started_at).timestamp(),
        )
        progress_age = max(0.0, now - target.last_progress_at)
        db_exec_count = snapshot.counts.get("executing", 0)
        allowed_real_execs = 0
        has_live_exec_capacity = False

        if db_exec_count > 0:
            allowed_real_execs = max(0, int(db_exec_count * self._options.exec_mismatch_ratio))
            has_live_exec_capacity = snapshot.real_exec_count > allowed_real_execs

        if db_exec_count >= self._options.exec_mismatch_min_executing:
            if snapshot.real_exec_count <= allowed_real_execs:
                if target.mismatch_since <= 0:
                    target.mismatch_since = now
            else:
                target.mismatch_since = 0.0
        else:
            target.mismatch_since = 0.0

        if not session_alive and not snapshot.runner_pids:
            return ("resume", "active run lost session and runner")
        if heartbeat_age >= self._options.heartbeat_stall_seconds:
            return ("resume", f"heartbeat stalled for {heartbeat_age:.0f}s")
        if progress_age >= self._options.progress_stall_seconds and event_age >= self._options.event_stall_seconds:
            # Long-running claude -p attempts can legitimately spend a long time without
            # producing terminal item progress. Do not restart while healthy exec workers
            # are still alive and the run heartbeat is fresh.
            if db_exec_count > 0 and has_live_exec_capacity:
                return None
            return (
                "resume",
                f"progress stalled for {progress_age:.0f}s and events stale for {event_age:.0f}s",
            )
        if (
            target.mismatch_since > 0
            and now - target.mismatch_since >= self._options.exec_mismatch_grace_seconds
            and progress_age >= self._options.progress_stall_seconds
        ):
            return (
                "resume",
                f"db executing={db_exec_count} real_exec={snapshot.real_exec_count} stalled={now - target.mismatch_since:.0f}s",
            )
        return None

    def _perform_action(
        self,
        target: SimpleWatchTarget,
        snapshot: SimpleRunSnapshot,
        action: tuple[str, str],
        now: float,
    ) -> None:
        verb, reason = action
        if now < target.cooldown_until:
            self._log(
                f"cooldown repo={target.repo} run_id={target.run_id} action={verb} reason={reason}"
            )
            return

        if verb == "resume":
            self._log(
                f"recover resume repo={target.repo} run_id={target.run_id} reason={reason}"
            )
            self._send_debug_signal(snapshot.runner_pids)
            self._stop_target_processes(target, snapshot.runner_pids)
            self._launch_target(target, "resume")
            target.restart_count += 1
        elif verb == "retry":
            target.wave += 1
            self._log(
                f"recover retry repo={target.repo} wave={target.wave} run_id={target.run_id} reason={reason}"
            )
            self._launch_target(target, "retry")
        else:
            raise ValueError(f"unsupported action: {verb}")

        target.cooldown_until = now + self._options.restart_cooldown_seconds
        target.last_progress_at = now
        target.mismatch_since = 0.0
        target.exhausted = False
        target.exhausted_reason = ""

    def _log(self, message: str) -> None:
        line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}"
        print(line, flush=True)
        logger.info(message)
        if self._watch_log_path is not None:
            self._watch_log_path.parent.mkdir(parents=True, exist_ok=True)
            with self._watch_log_path.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")

    def _ensure_pool_artifacts(self, target: SimpleWatchTarget) -> None:
        if self._pool_config is None:
            return
        if not target.active_profile:
            target.active_profile = self._options.pool_profile or self._pool_config.primary_profile.name
        if not target.pool_id:
            target.pool_id = self._pool_config.pool_id
        slug = target.run_id or target.repo.replace("/", "_").replace("\\", "_")
        root = self._project_root / ".simple_watch_pool" / slug
        root.mkdir(parents=True, exist_ok=True)
        if not target.pool_state_file:
            target.pool_state_file = str((root / "pool_state.json").resolve())
        if not target.pool_request_file:
            target.pool_request_file = str((root / "pool_request.json").resolve())

    def _sync_target_pool_state(self, target: SimpleWatchTarget) -> None:
        if not target.pool_state_file:
            return
        state = load_pool_state(target.pool_state_file)
        if state is None:
            return
        if state.pool_id:
            target.pool_id = state.pool_id
        if state.active_profile:
            target.active_profile = state.active_profile
        target.failback_pending = state.failback_pending

    def _pool_request_action(
        self,
        target: SimpleWatchTarget,
        snapshot: SimpleRunSnapshot,
    ) -> tuple[str, str] | None:
        if not target.pool_request_file:
            return None
        request_path = Path(target.pool_request_file)
        if not request_path.exists():
            return None
        payload = json.loads(request_path.read_text(encoding="utf-8"))
        target_profile = str(payload.get("target_profile", "")).strip()
        if target_profile:
            target.active_profile = target_profile
        target.failback_pending = payload.get("action") == "failback"
        target.cooldown_until = 0.0
        request_path.unlink(missing_ok=True)
        return ("resume", f"pool request: {payload.get('action', 'takeover')}")

    def _session_exists(self, session: str) -> bool:
        if not session:
            return False
        try:
            proc = subprocess.run(
                ["tmux", "has-session", "-t", session],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        except FileNotFoundError:
            return False
        return proc.returncode == 0

    def _tmux_pane_pids(self, session: str) -> list[int]:
        if not session:
            return []
        try:
            proc = subprocess.run(
                ["tmux", "list-panes", "-t", session, "-F", "#{pane_pid}"],
                capture_output=True,
                text=True,
                check=False,
            )
        except FileNotFoundError:
            return []
        if proc.returncode != 0:
            return []
        result: list[int] = []
        for line in proc.stdout.splitlines():
            line = line.strip()
            if line.isdigit():
                result.append(int(line))
        return result

    def _find_runner_pids(self, target: SimpleWatchTarget, run_id: str) -> list[int]:
        candidates: set[int] = set()
        for pane_pid in self._tmux_pane_pids(target.session):
            for pid in [pane_pid, *self._children_recursive(pane_pid)]:
                cmdline = self._read_cmdline(pid)
                if "claude_orchestrator" not in cmdline:
                    continue
                if " simple " not in f" {cmdline} ":
                    continue
                if run_id and run_id not in cmdline and "--run-id" in cmdline:
                    continue
                candidates.add(pid)
        if candidates:
            return sorted(candidates)
        working_dir = str(Path(target.working_dir).resolve()) if target.working_dir else ""
        if run_id:
            for pid in self._list_process_ids():
                cmdline = self._read_cmdline(pid)
                if "claude_orchestrator" not in cmdline or " simple " not in f" {cmdline} ":
                    continue
                if run_id in cmdline:
                    candidates.add(pid)
                    continue
                if working_dir and working_dir in cmdline:
                    candidates.add(pid)
        return sorted(candidates)

    def _count_exec_processes(self, target: SimpleWatchTarget, run_id: str, runner_pids: list[int]) -> int:
        exec_pids: set[int] = set()
        for runner_pid in runner_pids:
            for pid in self._children_recursive(runner_pid):
                cmdline = self._read_cmdline(pid)
                if "claude -p" in cmdline or "claude-p" in cmdline:
                    exec_pids.add(pid)
        working_dir = str(Path(target.working_dir).resolve()) if target.working_dir else ""
        if exec_pids or (not run_id and not working_dir):
            return len(exec_pids)
        for pid in self._list_process_ids():
            cmdline = self._read_cmdline(pid)
            if "claude -p" not in cmdline and "claude-p" not in cmdline:
                continue
            if run_id and run_id in cmdline:
                exec_pids.add(pid)
                continue
            if working_dir and working_dir in cmdline:
                exec_pids.add(pid)
        return len(exec_pids)

    def _children_recursive(self, pid: int) -> list[int]:
        children: list[int] = []
        frontier = [pid]
        seen = {pid}
        while frontier:
            parent = frontier.pop()
            direct_children = self._direct_children(parent)
            for child in direct_children:
                if child in seen:
                    continue
                seen.add(child)
                children.append(child)
                frontier.append(child)
        return children

    def _direct_children(self, pid: int) -> list[int]:
        try:
            proc = subprocess.run(
                ["pgrep", "-P", str(pid)],
                capture_output=True,
                text=True,
                check=False,
            )
        except FileNotFoundError:
            return []
        if proc.returncode not in (0, 1):
            return []
        result: list[int] = []
        for line in proc.stdout.splitlines():
            line = line.strip()
            if line.isdigit():
                result.append(int(line))
        return result

    def _read_cmdline(self, pid: int) -> str:
        try:
            data = Path(f"/proc/{pid}/cmdline").read_bytes()
        except OSError:
            return ""
        return data.replace(b"\x00", b" ").decode("utf-8", errors="ignore").strip()

    def _list_process_ids(self) -> list[int]:
        proc_root = Path("/proc")
        if not proc_root.exists():
            return []
        result: list[int] = []
        try:
            for entry in proc_root.iterdir():
                if entry.name.isdigit():
                    result.append(int(entry.name))
        except OSError:
            return []
        return result

    def _send_debug_signal(self, runner_pids: list[int]) -> None:
        debug_signal = getattr(signal, "SIGUSR1", None)
        if debug_signal is None:
            return
        for pid in runner_pids:
            try:
                os.kill(pid, debug_signal)
            except ProcessLookupError:
                continue
            except OSError:
                continue

    def _stop_target_processes(self, target: SimpleWatchTarget, runner_pids: list[int]) -> None:
        if target.session and self._session_exists(target.session):
            subprocess.run(
                ["tmux", "kill-session", "-t", target.session],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        time.sleep(1.0)
        related_pids: set[int] = set(runner_pids)
        for pid in list(related_pids):
            related_pids.update(self._children_recursive(pid))
        for pid in sorted(related_pids):
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                continue
            except OSError:
                continue

    def _launch_target(self, target: SimpleWatchTarget, subcommand: str) -> None:
        session = target.session or f"simple_watch_{target.repo.lower().replace('-', '_')}"
        target.session = session
        log_file, exit_file = self._action_artifact_paths(target, subcommand)
        config_path = self._config_path
        pool_flags = ""
        env_prefix = ""
        if self._pool_config is not None:
            profile_name = target.active_profile or self._options.pool_profile or self._pool_config.primary_profile.name
            config_path = Path(self._pool_config.get_profile(profile_name).config_path).resolve()
            env_parts = [
                f"{ENV_POOL_CHILD}=1",
                f"{ENV_POOL_ACTIVE_PROFILE}={shlex.quote(profile_name)}",
                f"{ENV_POOL_STATE_FILE}={shlex.quote(target.pool_state_file)}",
                f"{ENV_POOL_REQUEST_FILE}={shlex.quote(target.pool_request_file)}",
            ]
            if self._options.pool_profile:
                env_parts.append(f"{ENV_POOL_FIXED_PROFILE}={shlex.quote(self._options.pool_profile)}")
            env_prefix = "env " + " ".join(env_parts) + " "
            pool_flags = f" --pool-config {shlex.quote(str(self._pool_config_path))}"
            if self._options.pool_profile:
                pool_flags += f" --pool-profile {shlex.quote(self._options.pool_profile)}"
        command = (
            f"cd {shlex.quote(str(self._project_root))} && "
            f"{env_prefix}python3 -u -m claude_orchestrator -c {shlex.quote(str(config_path))} "
            f"simple {subcommand} --run-id {shlex.quote(target.run_id)}{pool_flags} "
            f"> {shlex.quote(str(log_file))} 2>&1; "
            f"printf '%s' $? > {shlex.quote(str(exit_file))}"
        )
        subprocess.run(["tmux", "kill-session", "-t", session], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        subprocess.run(["tmux", "new-session", "-d", "-s", session, command], check=True)

    def _action_artifact_paths(self, target: SimpleWatchTarget, subcommand: str) -> tuple[Path, Path]:
        base_log = Path(target.log_file) if target.log_file else (self._project_root / "logs" / f"{target.repo}.log")
        base_exit = Path(target.exit_file) if target.exit_file else (self._project_root / "logs" / f"{target.repo}.exit")
        suffix = f".watch_{subcommand}_{target.restart_count if subcommand == 'resume' else target.wave}"
        log_file = base_log.with_name(f"{base_log.stem}{suffix}{base_log.suffix or '.log'}")
        exit_file = base_exit.with_name(f"{base_exit.stem}{suffix}{base_exit.suffix or '.exit'}")
        log_file.parent.mkdir(parents=True, exist_ok=True)
        exit_file.parent.mkdir(parents=True, exist_ok=True)
        return log_file, exit_file


def build_watch_targets(
    manifest_path: str | None,
    run_ids: list[str],
    state_file: str | None,
) -> dict[str, SimpleWatchTarget]:
    targets: dict[str, SimpleWatchTarget] = {}
    if manifest_path:
        targets.update(load_watch_targets_from_manifest(Path(manifest_path).resolve()))
    for run_id in run_ids:
        key = run_id
        targets.setdefault(key, SimpleWatchTarget(repo=key, run_id=run_id))
    if not targets:
        raise ValueError("simple watch 需要 --manifest 或至少一个 --run-id")
    if state_file:
        targets = load_watch_state(Path(state_file).resolve(), targets)
    return targets
