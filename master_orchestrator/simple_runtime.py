"""Runtime layout helpers for simple task mode."""

from __future__ import annotations

import json
import math
import shutil
import time
import faulthandler
import signal
from collections import Counter, deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from threading import Event, Lock, Thread
from typing import Any

try:
    import psutil
except ImportError:
    psutil = None  # type: ignore[assignment]

from .audit_log import AuditLogger
from .claude_cli import BudgetTracker
from .config import Config
from .failover_pool import PoolRuntime
from .heartbeat import Heartbeat
from .rate_limiter import RateLimiter
from .simple_executor import ExecutionOutcome, SimpleExecutor
from .simple_isolation import PreparedItemWorkspace, SimpleIsolationManager
from .simple_lease import ExecutionLease, SimpleExecutionLeaseManager
from .simple_loader import load_simple_work_items
from .simple_manifest import write_simple_manifest
from .simple_model import (
    SimpleAttempt,
    SimpleErrorCategory,
    SimpleIsolationMode,
    SimpleItemStatus,
    SimpleItemType,
    SimpleManifest,
    SimpleRun,
    SimpleRunStatus,
    SimpleWorkItem,
    ValidationReport,
)
from .simple_scheduler import SimpleScheduler
from .simple_status import build_simple_status_payload
from .simple_store import SimpleStore
from .simple_validation import SimpleValidationPipeline, classify_simple_failure
from .store import Store
from .model import RunStatus, TaskResult, TaskStatus

_DEBUG_SIGNAL_LOCK = Lock()
_DEBUG_SIGNAL_REGISTERED = False


def _install_simple_debug_signal_handler() -> None:
    global _DEBUG_SIGNAL_REGISTERED
    if _DEBUG_SIGNAL_REGISTERED:
        return
    debug_signal = getattr(signal, "SIGUSR1", None)
    if debug_signal is None:
        return
    with _DEBUG_SIGNAL_LOCK:
        if _DEBUG_SIGNAL_REGISTERED:
            return
        try:
            faulthandler.register(debug_signal, all_threads=True, chain=False)
        except (AttributeError, OSError, RuntimeError, ValueError):
            return
        _DEBUG_SIGNAL_REGISTERED = True


def _next_run_wallclock(run: SimpleRun, *, now: datetime | None = None) -> datetime:
    candidate = now or datetime.now()
    floor = run.last_heartbeat_at or run.started_at
    if candidate <= floor:
        return floor + timedelta(microseconds=1)
    return candidate


@dataclass
class SimpleRuntimeLayout:
    root: Path
    logs: Path
    state: Path
    manifests: Path
    artifacts: Path
    scratch: Path

    @classmethod
    def create(cls, root: str | Path) -> "SimpleRuntimeLayout":
        base = Path(root)
        layout = cls(
            root=base,
            logs=base / "logs",
            state=base / "state",
            manifests=base / "manifests",
            artifacts=base / "artifacts",
            scratch=base / "scratch",
        )
        for path in (layout.root, layout.logs, layout.state, layout.manifests, layout.artifacts, layout.scratch):
            path.mkdir(parents=True, exist_ok=True)
        return layout


@dataclass
class PreparedExecution:
    item: SimpleWorkItem
    prepared: PreparedItemWorkspace


@dataclass
class ValidationEnvelope:
    dispatch: PreparedExecution
    outcome: ExecutionOutcome
    reused_target_changed_files: list[str] | None = None
    revalidated_without_exec: bool = False


@dataclass(frozen=True)
class RunTargetIndex:
    exact_targets: dict[str, set[str]]
    directory_targets: tuple[tuple[str, set[str]], ...]


class SimpleTaskRunner:
    def __init__(
        self,
        config: Config,
        store: Store,
        *,
        working_dir: str,
        log_file: str | None = None,
        pool_runtime: PoolRuntime | None = None,
        preferred_provider: str = "auto",
    ):
        self._config = config
        self._store = store
        self._simple_store = SimpleStore(store)
        self._working_dir = Path(working_dir).resolve()
        self._preferred_provider = preferred_provider
        # 持久化到 working_dir 下的 budget_tracker.json
        _budget_persist = str(self._working_dir / "budget_tracker.json")
        self._budget = BudgetTracker(
            config.claude.max_budget_usd,
            persist_path=_budget_persist,
            enforcement_mode=config.claude.budget_enforcement_mode,
        )
        self._rate_limiter = RateLimiter(config.rate_limit)
        self._audit_logger: AuditLogger | None = None
        self._validation = SimpleValidationPipeline(config)
        self._log_file = log_file
        self._throttle_limit = config.simple.max_running_processes
        self._recent_pressure_events: deque[tuple[float, str]] = deque()
        self._pressure_window_seconds = 300.0
        self._effective_exec_limit = config.simple.max_running_processes
        self._last_maintenance_at = 0.0
        self._last_live_run_count = 1
        self._next_slot_warm_ready_at = 0.0
        self._heartbeat_lock = Lock()
        self._process_heartbeat = Heartbeat()
        self._pool_runtime = pool_runtime
        self.requested_exit_code: int = 0
        _install_simple_debug_signal_handler()

    def _request_pool_exit(self, exit_code: int) -> None:
        if self.requested_exit_code == 0:
            self.requested_exit_code = exit_code

    @staticmethod
    def _empty_stage_timing_stats() -> dict[str, dict[str, float | int]]:
        return {
            name: {"count": 0, "total_ms": 0.0, "avg_ms": 0.0, "max_ms": 0.0}
            for name in ("prepare", "execute", "validate")
        }

    @staticmethod
    def _empty_execution_stats() -> dict[str, float | int]:
        return {
            "attempts_total": 0,
            "tool_uses_total": 0,
            "turn_started_total": 0,
            "turn_completed_total": 0,
            "token_input_total": 0,
            "token_output_total": 0,
            "cli_duration_ms_total": 0.0,
            "cli_duration_ms_avg": 0.0,
            "cli_duration_ms_max": 0.0,
            "claude_home_ready_ms_total": 0.0,
            "claude_home_ready_ms_avg": 0.0,
            "claude_home_ready_ms_max": 0.0,
            "execution_wall_ms_total": 0.0,
            "execution_wall_ms_avg": 0.0,
            "execution_wall_ms_max": 0.0,
            "max_turns_exceeded_attempts": 0,
        }

    @staticmethod
    def _record_stage_timing(manifest: SimpleManifest, stage: str, duration_ms: float) -> None:
        stats = manifest.stage_timing_stats.setdefault(
            stage,
            {"count": 0, "total_ms": 0.0, "avg_ms": 0.0, "max_ms": 0.0},
        )
        stats["count"] += 1
        stats["total_ms"] = round(float(stats["total_ms"]) + duration_ms, 1)
        stats["max_ms"] = round(max(float(stats["max_ms"]), duration_ms), 1)
        stats["avg_ms"] = round(float(stats["total_ms"]) / int(stats["count"]), 1) if stats["count"] else 0.0

    @staticmethod
    def _record_attempt_metrics(manifest: SimpleManifest, attempt: SimpleAttempt) -> None:
        stats = manifest.execution_stats
        stats["attempts_total"] = int(stats.get("attempts_total", 0)) + 1
        stats["tool_uses_total"] = int(stats.get("tool_uses_total", 0)) + int(attempt.tool_uses or 0)
        stats["turn_started_total"] = int(stats.get("turn_started_total", 0)) + int(attempt.turn_started or 0)
        stats["turn_completed_total"] = int(stats.get("turn_completed_total", 0)) + int(attempt.turn_completed or 0)
        stats["token_input_total"] = int(stats.get("token_input_total", 0)) + int(attempt.token_input or 0)
        stats["token_output_total"] = int(stats.get("token_output_total", 0)) + int(attempt.token_output or 0)
        stats["cli_duration_ms_total"] = round(float(stats.get("cli_duration_ms_total", 0.0)) + float(attempt.cli_duration_ms or 0.0), 1)
        stats["cli_duration_ms_max"] = round(max(float(stats.get("cli_duration_ms_max", 0.0)), float(attempt.cli_duration_ms or 0.0)), 1)
        stats["claude_home_ready_ms_total"] = round(
            float(stats.get("claude_home_ready_ms_total", 0.0)) + float(attempt.claude_home_ready_ms or 0.0),
            1,
        )
        stats["claude_home_ready_ms_max"] = round(
            max(float(stats.get("claude_home_ready_ms_max", 0.0)), float(attempt.claude_home_ready_ms or 0.0)),
            1,
        )
        stats["execution_wall_ms_total"] = round(
            float(stats.get("execution_wall_ms_total", 0.0)) + float(attempt.execution_wall_ms or 0.0),
            1,
        )
        stats["execution_wall_ms_max"] = round(
            max(float(stats.get("execution_wall_ms_max", 0.0)), float(attempt.execution_wall_ms or 0.0)),
            1,
        )
        if attempt.max_turns_exceeded:
            stats["max_turns_exceeded_attempts"] = int(stats.get("max_turns_exceeded_attempts", 0)) + 1
        attempts_total = int(stats["attempts_total"])
        stats["cli_duration_ms_avg"] = round(float(stats["cli_duration_ms_total"]) / attempts_total, 1) if attempts_total else 0.0
        stats["claude_home_ready_ms_avg"] = round(float(stats["claude_home_ready_ms_total"]) / attempts_total, 1) if attempts_total else 0.0
        stats["execution_wall_ms_avg"] = round(float(stats["execution_wall_ms_total"]) / attempts_total, 1) if attempts_total else 0.0

    def _runtime_layout(self, run_id: str) -> SimpleRuntimeLayout:
        return SimpleRuntimeLayout.create(Path(self._config.simple.manifest_dir).resolve() / run_id)

    def _repo_relative_path(self, path: str | Path | None) -> str | None:
        if not path:
            return None
        candidate = Path(path)
        try:
            resolved = candidate.resolve()
        except Exception:
            return None
        try:
            relative = resolved.relative_to(self._working_dir)
        except ValueError:
            return None
        normalized = str(relative).replace("\\", "/").strip("/")
        return normalized or None

    def _ignored_repo_paths(self, layout: SimpleRuntimeLayout) -> set[str]:
        ignored: set[str] = set()
        candidates: list[str | Path | None] = [
            self._config.checkpoint.db_path,
            self._config.simple.execution_lease_db_path,
            self._config.simple.manifest_dir,
            self._config.simple.copy_root_dir,
            self._log_file,
        ]
        for candidate in candidates:
            relative = self._repo_relative_path(candidate)
            if relative:
                ignored.add(relative)
            if candidate:
                candidate_path = Path(candidate)
                for suffix in ("-wal", "-shm", "-journal", ".init.lock"):
                    sidecar_relative = self._repo_relative_path(candidate_path.with_name(candidate_path.name + suffix))
                    if sidecar_relative:
                        ignored.add(sidecar_relative)
        layout_relative = self._repo_relative_path(layout.root)
        if layout_relative:
            ignored.add(layout_relative)
        return ignored

    def load_items(
        self,
        instruction: str,
        *,
        files: list[str] | None = None,
        globs: list[str] | None = None,
        task_file: str | None = None,
    ):
        return load_simple_work_items(
            self._working_dir,
            instruction,
            files=files,
            globs=globs,
            task_file=task_file,
            bucket_strategy=self._config.simple.bucket_strategy,
            default_timeout=self._config.simple.default_timeout_seconds,
            default_max_attempts=self._config.simple.default_max_attempts,
            validate_task_file_targets=self._config.simple.validate_task_file_targets,
        )

    @staticmethod
    def _cap_small_run(value: int, item_count: int) -> int:
        if item_count <= 0:
            return 1
        return max(1, min(value, item_count))

    def _prepare_worker_count(self, item_count: int) -> int:
        configured = self._config.simple.prepare_workers
        if configured > 0:
            return self._cap_small_run(configured, item_count)
        computed = max(self._config.simple.max_running_processes * 2, self._config.simple.max_running_processes + 2)
        return self._cap_small_run(computed, item_count)

    def _validate_worker_count(self, item_count: int) -> int:
        return self._cap_small_run(max(1, self._config.simple.validate_workers), item_count)

    def _prepared_queue_capacity(self, item_count: int) -> int:
        configured = self._config.simple.max_prepared_items
        if configured > 0:
            return self._cap_small_run(configured, item_count)
        computed = max(self._config.simple.max_running_processes * 2, self._config.simple.max_running_processes + 1)
        return self._cap_small_run(computed, item_count)

    def _worker_id_for(self, item: SimpleWorkItem) -> str:
        return f"worker-{item.item_id[:6]}"

    def _execution_lease_manager(self) -> SimpleExecutionLeaseManager | None:
        global_limit = self._config.simple.global_max_running_processes
        if global_limit <= 0:
            return None
        lease_db_path = self._config.simple.execution_lease_db_path
        if lease_db_path:
            db_path = Path(lease_db_path).resolve()
        else:
            db_path = Path(self._config.simple.manifest_dir).resolve() / "simple_execution_leases.sqlite3"
        return SimpleExecutionLeaseManager(
            db_path,
            max_leases=global_limit,
            ttl_seconds=self._config.simple.execution_lease_ttl_seconds,
        )

    def _schedulable_item_count(self, items: list[SimpleWorkItem]) -> int:
        terminal = {
            SimpleItemStatus.SUCCEEDED,
            SimpleItemStatus.FAILED,
            SimpleItemStatus.SKIPPED,
            SimpleItemStatus.BLOCKED,
        }
        return sum(1 for item in items if item.status not in terminal)

    def _execution_worker_ids(self, item_count: int) -> list[str]:
        target_slots = min(max(0, item_count), max(1, self._effective_exec_limit))
        return [
            f"exec-slot-{index:02d}"
            for index in range(target_slots)
        ]

    def _initial_execution_slot_count(self, max_slots: int) -> int:
        if max_slots <= 0:
            return 0
        configured = self._config.simple.initial_execution_slots
        if configured > 0:
            return min(max_slots, configured)
        return min(max_slots, 8)

    def _execution_slot_batch_size(self, max_slots: int) -> int:
        if max_slots <= 0:
            return 0
        configured = self._config.simple.execution_slot_batch_size
        if configured > 0:
            return min(max_slots, configured)
        return min(max_slots, 4)

    def _execution_slot_ramp_interval_seconds(self) -> float:
        return max(0.0, float(self._config.simple.execution_slot_ramp_interval_seconds))

    def _execution_slot_ramp_wait_seconds(self, *, now: float | None = None) -> float:
        current = time.monotonic() if now is None else now
        return max(0.0, self._next_slot_warm_ready_at - current)

    def _prewarm_execution_slots(self, run_id: str, executor: SimpleExecutor, worker_ids: list[str]) -> None:
        if not worker_ids:
            return
        started_at = time.monotonic()
        failures: list[tuple[str, str]] = []
        with ThreadPoolExecutor(max_workers=min(len(worker_ids), max(1, self._config.simple.max_running_processes))) as pool:
            futures = {pool.submit(executor.warm_worker_home, worker_id): worker_id for worker_id in worker_ids}
            for future, worker_id in futures.items():
                try:
                    future.result()
                except Exception as exc:
                    failures.append((worker_id, str(exc)))
        duration_ms = round((time.monotonic() - started_at) * 1000, 1)
        self._store.save_simple_event(
            run_id,
            "execution_slots_warmed",
            {
                "worker_ids": worker_ids,
                "duration_ms": duration_ms,
                "failures": failures,
            },
            level="warning" if failures else "info",
        )

    def _start_execution_slot_prewarm(
        self,
        run_id: str,
        executor: SimpleExecutor,
        worker_ids: list[str],
    ) -> Thread | None:
        if not worker_ids:
            return None
        self._store.save_simple_event(
            run_id,
            "execution_slots_prewarm_started",
            {"worker_ids": worker_ids, "count": len(worker_ids)},
        )
        thread = Thread(
            target=self._prewarm_execution_slots,
            args=(run_id, executor, worker_ids),
            name=f"simple-prewarm-{run_id}",
            daemon=True,
        )
        thread.start()
        return thread

    def _prepare_dispatch(
        self,
        isolation: SimpleIsolationManager,
        item: SimpleWorkItem,
    ) -> PreparedExecution:
        prepared = isolation.prepare(item)
        return PreparedExecution(item=item, prepared=prepared)

    @staticmethod
    def _parent_scope_for_target(target: str) -> str:
        normalized = target.replace("\\", "/").strip("/")
        if not normalized or "/" not in normalized:
            return "."
        return normalized.rsplit("/", 1)[0] or "."

    @staticmethod
    def _is_file_scoped_item(item: SimpleWorkItem) -> bool:
        return item.item_type != SimpleItemType.DIRECTORY_SHARD

    def _shared_file_parent_counts(self, items: list[SimpleWorkItem]) -> dict[str, int]:
        terminal_statuses = {
            SimpleItemStatus.SUCCEEDED,
            SimpleItemStatus.FAILED,
            SimpleItemStatus.SKIPPED,
            SimpleItemStatus.BLOCKED,
        }
        counts = Counter(
            self._parent_scope_for_target(item.target)
            for item in items
            if self._is_file_scoped_item(item) and item.status not in terminal_statuses
        )
        return {key: value for key, value in counts.items() if value > 1}

    @staticmethod
    def _normalize_repo_path(path: str) -> str:
        normalized = path.replace("\\", "/").strip()
        while normalized.startswith("./"):
            normalized = normalized[2:]
        return normalized.strip("/")

    @classmethod
    def _build_run_target_index(cls, items: list[SimpleWorkItem]) -> RunTargetIndex:
        exact_targets: dict[str, set[str]] = {}
        directory_targets: dict[str, set[str]] = {}
        for item in items:
            normalized = cls._normalize_repo_path(item.target)
            if not normalized:
                continue
            if item.item_type == SimpleItemType.DIRECTORY_SHARD:
                directory_targets.setdefault(normalized, set()).add(item.item_id)
            else:
                exact_targets.setdefault(normalized, set()).add(item.item_id)
        return RunTargetIndex(
            exact_targets=exact_targets,
            directory_targets=tuple(
                sorted(directory_targets.items(), key=lambda entry: (-len(entry[0]), entry[0]))
            ),
        )

    @classmethod
    def _run_target_owners(cls, path: str, target_index: RunTargetIndex) -> set[str]:
        normalized = cls._normalize_repo_path(path)
        owners = set(target_index.exact_targets.get(normalized, set()))
        for prefix, item_ids in target_index.directory_targets:
            if normalized == prefix or normalized.startswith(f"{prefix}/"):
                owners.update(item_ids)
        return owners

    @classmethod
    def _conflicting_run_targets(
        cls,
        item: SimpleWorkItem,
        report: ValidationReport,
        target_index: RunTargetIndex,
    ) -> list[str]:
        if report.failure_code not in {"target_path_mismatch", "unauthorized_side_files"}:
            return []
        if not report.unauthorized_changes:
            return []
        conflicts: list[str] = []
        for path in report.unauthorized_changes:
            owners = cls._run_target_owners(path, target_index) - {item.item_id}
            if not owners:
                return []
            conflicts.append(cls._normalize_repo_path(path))
        return sorted(dict.fromkeys(conflicts))

    @staticmethod
    def _conflict_failure_reason(conflicting_targets: list[str]) -> str:
        sample = ", ".join(conflicting_targets[:3])
        suffix = "" if len(conflicting_targets) <= 3 else f" 等 {len(conflicting_targets)} 个目标"
        return f"detected cross-item conflict with same-run targets: {sample}{suffix}"

    def _item_conflict_key(
        self,
        item: SimpleWorkItem,
        *,
        requested_mode: str,
        shared_parent_counts: dict[str, int],
    ) -> str:
        target_key = f"target:{item.target}"
        if not self._config.simple.conflict_detection_enabled:
            return target_key
        if requested_mode != SimpleIsolationMode.NONE.value:
            return target_key
        if not self._is_file_scoped_item(item):
            return target_key
        if self._config.simple.none_file_conflict_strategy != "serialize":
            return target_key
        parent_scope = self._parent_scope_for_target(item.target)
        if shared_parent_counts.get(parent_scope, 0) <= 1:
            return target_key
        return f"dir:{parent_scope}"

    def _active_targets(
        self,
        preparing_items: dict[Future, SimpleWorkItem],
        prepared_conflict_keys: set[str],
        exec_futures: dict[Future, tuple[PreparedExecution, ExecutionLease | None, str]],
        validate_futures: dict[Future, ValidationEnvelope],
        *,
        requested_mode: str,
        shared_parent_counts: dict[str, int],
    ) -> set[str]:
        conflict_keys = {
            self._item_conflict_key(
                item,
                requested_mode=requested_mode,
                shared_parent_counts=shared_parent_counts,
            )
            for item in preparing_items.values()
        }
        conflict_keys.update(prepared_conflict_keys)
        conflict_keys.update(
            self._item_conflict_key(
                dispatch.item,
                requested_mode=requested_mode,
                shared_parent_counts=shared_parent_counts,
            )
            for dispatch, _, _worker_id in exec_futures.values()
        )
        conflict_keys.update(
            self._item_conflict_key(
                envelope.dispatch.item,
                requested_mode=requested_mode,
                shared_parent_counts=shared_parent_counts,
            )
            for envelope in validate_futures.values()
        )
        return conflict_keys

    def _renew_execution_leases(self, exec_futures: dict[Future, tuple[PreparedExecution, ExecutionLease | None, str]]) -> None:
        now = datetime.now()
        lease_manager = getattr(self, "_lease_manager", None)
        if lease_manager is None:
            return
        for _dispatch, lease, _worker_id in exec_futures.values():
            if lease is None:
                continue
            remaining = (lease.expires_at - now).total_seconds()
            if remaining <= max(30.0, self._config.simple.execution_lease_ttl_seconds / 3):
                renewed = lease_manager.renew(lease)
                if renewed is not None:
                    lease.expires_at = renewed.expires_at

    @staticmethod
    def _wait_timeout_seconds(
        prepared_backlog: int,
        exec_inflight: int,
        local_exec_limit: int,
        prepare_inflight: int,
        *,
        slot_ramp_wait_seconds: float = 0.0,
    ) -> float:
        if prepared_backlog > 0 and exec_inflight < local_exec_limit:
            if slot_ramp_wait_seconds > 0:
                return min(1.0, max(0.2, slot_ramp_wait_seconds))
            return 0.05
        if prepare_inflight > 0 and exec_inflight < local_exec_limit:
            return 0.1
        return 1.0

    @staticmethod
    def _transient_item_statuses() -> list[SimpleItemStatus]:
        return [
            SimpleItemStatus.PREPARING,
            SimpleItemStatus.EXECUTING,
            SimpleItemStatus.RUNNING,
            SimpleItemStatus.VALIDATING,
        ]

    @staticmethod
    def _revalidation_eligible_categories() -> set[str]:
        return {
            SimpleErrorCategory.SEMANTIC_VALIDATION_FAILED.value,
            SimpleErrorCategory.SYNTAX_ERROR.value,
            SimpleErrorCategory.PATTERN_MISSING.value,
            SimpleErrorCategory.VERIFY_COMMAND_FAILED.value,
        }

    def _existing_state_revalidation_files(self, run_id: str, item: SimpleWorkItem) -> list[str] | None:
        if item.attempt_state.attempt <= 0:
            return None
        latest_attempt = self._simple_store.get_latest_attempt(run_id, item.item_id)
        if latest_attempt is None or latest_attempt.validation_report is None:
            return None
        if latest_attempt.error_category not in self._revalidation_eligible_categories():
            return None
        report = latest_attempt.validation_report
        if report.passed:
            return None
        if not report.target_touched or not report.target_content_changed:
            return None
        if report.unauthorized_changes:
            return None
        reused = [path.replace("\\", "/") for path in report.target_changed_files or latest_attempt.changed_files]
        if not reused:
            return None
        return reused

    @staticmethod
    def _build_revalidation_outcome(item: SimpleWorkItem) -> ExecutionOutcome:
        now = datetime.now()
        return ExecutionOutcome(
            result=TaskResult(
                task_id=item.item_id,
                status=TaskStatus.SUCCESS,
                output="revalidated existing target state",
                started_at=now,
                finished_at=now,
            ),
            attempt=SimpleAttempt(
                item_id=item.item_id,
                attempt=item.attempt_state.attempt,
                status=SimpleItemStatus.VALIDATING,
                worker_id="revalidate",
                started_at=now,
                finished_at=now,
            ),
            changed_files=[],
            prompt="",
        )

    def _stale_run_cutoff(self) -> datetime:
        return datetime.now() - timedelta(seconds=self._config.simple.stale_run_timeout_seconds)

    def recover_stale_runs(self, *, exclude_run_id: str | None = None) -> list[dict[str, Any]]:
        recovered: list[dict[str, Any]] = []
        stale_runs = self._simple_store.find_stale_runs(
            self._stale_run_cutoff(),
            statuses=[SimpleRunStatus.RUNNING, SimpleRunStatus.DRAINING],
        )
        for stale_run in stale_runs:
            if exclude_run_id and stale_run.run_id == exclude_run_id:
                continue
            recovered_at = datetime.now()
            closed = self._store.close_simple_run_state(
                stale_run.run_id,
                simple_status=SimpleRunStatus.FAILED,
                run_status=RunStatus.FAILED,
                item_from_statuses=self._transient_item_statuses(),
                item_to_status=SimpleItemStatus.READY,
                finished_at=recovered_at,
                last_heartbeat_at=recovered_at,
            )
            self._store.save_simple_event(
                stale_run.run_id,
                "stale_run_recovered",
                {
                    "previous_status": stale_run.status.value,
                    "reset_items": closed["items_updated"],
                    "last_heartbeat_at": stale_run.last_heartbeat_at.isoformat() if stale_run.last_heartbeat_at else None,
                    "stale_before": self._stale_run_cutoff().isoformat(),
                },
                level="warning",
            )
            recovered.append(
                {
                    "run_id": stale_run.run_id,
                    "previous_status": stale_run.status.value,
                    "reset_items": closed["items_updated"],
                }
            )
        return recovered

    def _touch_run_heartbeat(self, run: SimpleRun, *, force: bool = False) -> None:
        with self._heartbeat_lock:
            last = run.last_heartbeat_at or run.started_at
            candidate = datetime.now()
            if not force and (candidate - last).total_seconds() < self._config.simple.run_heartbeat_interval_seconds:
                return
            now = _next_run_wallclock(run, now=candidate)
            self._simple_store.touch_run_heartbeat(run.run_id, now)
            run.last_heartbeat_at = now

    def _start_run_heartbeat_pump(self, run: SimpleRun) -> tuple[Event, Thread]:
        stop_event = Event()
        interval = max(1.0, float(self._config.simple.run_heartbeat_interval_seconds))

        def _heartbeat_loop() -> None:
            while not stop_event.wait(interval):
                try:
                    self._touch_run_heartbeat(run, force=True)
                except Exception:
                    # 保活线程不负责失败传播，避免心跳异常反向打崩主执行流。
                    continue

        thread = Thread(
            target=_heartbeat_loop,
            name=f"simple-heartbeat-{run.run_id}",
            daemon=True,
        )
        thread.start()
        return stop_event, thread

    def _stop_run_heartbeat_pump(
        self,
        run: SimpleRun,
        stop_event: Event | None,
        thread: Thread | None,
    ) -> None:
        if stop_event is not None:
            stop_event.set()
        if thread is not None:
            thread.join(timeout=max(1.0, float(self._config.simple.run_heartbeat_interval_seconds)))
        try:
            self._touch_run_heartbeat(run, force=True)
        except Exception:
            pass

    def _refresh_effective_exec_limit(self, run: SimpleRun) -> None:
        effective_limit = min(self._throttle_limit, self._config.simple.max_running_processes)
        if self._config.simple.global_max_running_processes > 0 and self._config.simple.global_fair_share_enabled:
            live_runs = max(
                1,
                self._simple_store.count_live_runs(
                    self._stale_run_cutoff(),
                    statuses=[SimpleRunStatus.RUNNING, SimpleRunStatus.DRAINING],
                ),
            )
            self._last_live_run_count = live_runs
            fair_share_limit = max(1, math.ceil(self._config.simple.global_max_running_processes / live_runs))
            effective_limit = min(effective_limit, fair_share_limit)
        if effective_limit != self._effective_exec_limit:
            previous = self._effective_exec_limit
            self._effective_exec_limit = effective_limit
            self._store.save_simple_event(
                run.run_id,
                "effective_exec_limit_changed",
                {
                    "previous_limit": previous,
                    "new_limit": effective_limit,
                    "live_runs": self._last_live_run_count,
                    "global_limit": self._config.simple.global_max_running_processes,
                    "dynamic_limit": self._throttle_limit,
                },
            )

    def _maintenance_tick(self, run: SimpleRun, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._last_maintenance_at < self._config.simple.run_heartbeat_interval_seconds:
            return
        self._last_maintenance_at = now
        self._touch_run_heartbeat(run, force=True)
        recovered = self.recover_stale_runs(exclude_run_id=run.run_id)
        self._refresh_effective_exec_limit(run)
        if recovered:
            self._store.save_simple_event(
                run.run_id,
                "cluster_stale_runs_recovered",
                {"runs": recovered},
                level="warning",
            )

    def _mark_run_aborted(self, run: SimpleRun, exc: Exception) -> None:
        aborted_at = _next_run_wallclock(run)
        closed = self._store.close_simple_run_state(
            run.run_id,
            simple_status=SimpleRunStatus.FAILED,
            run_status=RunStatus.FAILED,
            item_from_statuses=self._transient_item_statuses(),
            item_to_status=SimpleItemStatus.READY,
            finished_at=aborted_at,
            last_heartbeat_at=aborted_at,
        )
        self._store.save_simple_event(
            run.run_id,
            "run_aborted",
            {
                "error": str(exc),
                "exception_type": type(exc).__name__,
                "reset_items": closed["items_updated"],
            },
            level="error",
        )

    def run(
        self,
        instruction: str,
        *,
        files: list[str] | None = None,
        globs: list[str] | None = None,
        task_file: str | None = None,
        isolation_mode: str | None = None,
        existing_run: SimpleRun | None = None,
        existing_items: list[SimpleWorkItem] | None = None,
    ) -> tuple[SimpleRun, dict[str, Any]]:
        self.requested_exit_code = 0
        self.recover_stale_runs()
        load_result = None
        if existing_run is None:
            load_result = self.load_items(instruction, files=files, globs=globs, task_file=task_file)
            run = SimpleRun(
                instruction_template=instruction,
                status=SimpleRunStatus.READY,
                source_summary=load_result.source_summary,
                isolation_mode=isolation_mode or self._config.simple.default_isolation,
                scheduler_config_snapshot={
                    "max_pending_tasks": self._config.simple.max_pending_tasks,
                    "max_running_processes": self._config.simple.max_running_processes,
                    "initial_execution_slots": self._config.simple.initial_execution_slots,
                    "execution_slot_batch_size": self._config.simple.execution_slot_batch_size,
                    "execution_slot_ramp_interval_seconds": self._config.simple.execution_slot_ramp_interval_seconds,
                    "bucket_strategy": self._config.simple.bucket_strategy,
                },
                working_dir=str(self._working_dir),
                pool_id=self._pool_runtime.pool_id if self._pool_runtime else "",
                active_profile=self._pool_runtime.active_profile if self._pool_runtime else "",
            )
            run.last_heartbeat_at = run.started_at
            items = load_result.items
        else:
            run = existing_run
            run.last_heartbeat_at = _next_run_wallclock(run)
            if self._pool_runtime is not None:
                run.pool_id = self._pool_runtime.pool_id
                run.active_profile = self._pool_runtime.active_profile
            items = existing_items or []
        layout = self._runtime_layout(run.run_id)
        self._audit_logger = AuditLogger(log_dir=layout.logs)
        run.manifest_path = str(layout.manifests / "simple_manifest.json")
        if self._pool_runtime is not None:
            self._pool_runtime.mark_execution(
                execution_id=run.run_id,
                execution_kind="simple",
                state_db_path=self._config.checkpoint.db_path,
                active_profile=self._pool_runtime.active_profile,
            )
        if existing_run is None:
            self._simple_store.create_run(run, items)
        self._store.update_simple_run(
            run.run_id,
            status=SimpleRunStatus.RUNNING,
            manifest_path=run.manifest_path,
            last_heartbeat_at=run.last_heartbeat_at,
            clear_finished_at=True,
            pool_id=run.pool_id,
            active_profile=run.active_profile,
        )
        self._store.update_run_status(run.run_id, RunStatus.RUNNING)
        heartbeat_stop, heartbeat_thread = self._start_run_heartbeat_pump(run)
        self._process_heartbeat.start_background()

        saved_rate = self._store.get_context(run.run_id, "simple.rate_limiter")
        if saved_rate:
            try:
                self._rate_limiter.restore_state(json.loads(saved_rate))
            except Exception:
                pass

        scheduler = SimpleScheduler(
            items,
            max_pending_tasks=self._config.simple.max_pending_tasks,
            fair_scheduling=self._config.simple.fair_scheduling,
        )
        isolation = SimpleIsolationManager(
            self._working_dir,
            layout,
            self._config.simple,
            run.run_id,
            run.isolation_mode,
            ignored_repo_paths=self._ignored_repo_paths(layout),
            shared_parent_file_counts=self._shared_file_parent_counts(items),
        )
        executor = SimpleExecutor(
            self._config,
            self._budget,
            self._rate_limiter,
            self._audit_logger,
            state_root=layout.state,
            pool_runtime=self._pool_runtime,
            store=self._store,
            run_id=run.run_id,
            on_process_request=self._request_pool_exit,
            preferred_provider=self._preferred_provider,
        )
        self._next_slot_warm_ready_at = 0.0
        self._maintenance_tick(run, force=True)
        schedulable_items = self._schedulable_item_count(items)
        prepare_workers = self._prepare_worker_count(schedulable_items)
        validate_workers = self._validate_worker_count(schedulable_items)
        prepared_capacity = self._prepared_queue_capacity(schedulable_items)
        self._lease_manager = self._execution_lease_manager()
        max_execution_slots = min(max(0, schedulable_items), max(1, self._config.simple.max_running_processes))
        initial_execution_slots = self._initial_execution_slot_count(max_execution_slots)
        slot_batch_size = self._execution_slot_batch_size(max_execution_slots)
        shared_parent_counts = self._shared_file_parent_counts(items)
        run_target_index = self._build_run_target_index(items)
        worker_ids = [
            f"exec-slot-{index:02d}"
            for index in range(max_execution_slots)
        ]
        max_reserved_items = prepared_capacity + max_execution_slots
        self._store.save_simple_event(
            run.run_id,
            "run_started",
            {
                "items": len(items),
                "isolation_mode": run.isolation_mode,
                "prepare_workers": prepare_workers,
                "validate_workers": validate_workers,
                "prepared_capacity": prepared_capacity,
                "local_exec_limit": self._config.simple.max_running_processes,
                "effective_exec_slots": initial_execution_slots,
                "max_execution_slots": max_execution_slots,
                "execution_slot_batch_size": slot_batch_size,
                "execution_slot_ramp_interval_seconds": self._execution_slot_ramp_interval_seconds(),
                "configured_initial_execution_slots": self._config.simple.initial_execution_slots,
                "configured_execution_slot_batch_size": self._config.simple.execution_slot_batch_size,
                "global_exec_limit": self._config.simple.global_max_running_processes,
                "execution_workers": worker_ids[:initial_execution_slots],
                "none_file_conflict_strategy": self._config.simple.none_file_conflict_strategy,
                "shared_parent_directories": len(shared_parent_counts),
                "shared_parent_targets": sum(shared_parent_counts.values()),
            },
        )
        prewarm_futures: dict[Future, str] = {}
        prepare_futures: dict[Future, SimpleWorkItem] = {}
        exec_futures: dict[Future, tuple[PreparedExecution, ExecutionLease | None, str]] = {}
        validate_futures: dict[Future, ValidationEnvelope] = {}
        prepared_queue: deque[PreparedExecution] = deque()
        prepared_conflict_keys: set[str] = set()
        available_worker_ids = deque()
        pending_worker_ids = deque(worker_ids)
        warmed_worker_ids: list[str] = []
        current_prewarm_wave_started_at: float | None = None
        current_prewarm_wave_worker_ids: list[str] = []
        current_prewarm_wave_failures: list[dict[str, str]] = []
        outcome_lock = Lock()
        prepare_started_at: dict[str, float] = {}
        execute_started_at: dict[str, float] = {}
        manifest = SimpleManifest(
            run_id=run.run_id,
            total_items=len(items),
            isolation_mode=run.isolation_mode,
            input_sources=run.source_summary,
            execution_stats=self._empty_execution_stats(),
            stage_timing_stats=self._empty_stage_timing_stats(),
        )

        def submit_prepare(pool: ThreadPoolExecutor, item: SimpleWorkItem) -> None:
            item.status = SimpleItemStatus.PREPARING
            prepare_started_at[item.item_id] = time.monotonic()
            self._store.update_simple_item_status(run.run_id, item.item_id, SimpleItemStatus.PREPARING)
            self._store.save_simple_event(
                run.run_id,
                "item_prepare_started",
                {"target": item.target},
                item_id=item.item_id,
                bucket=item.bucket,
            )
            future = pool.submit(self._prepare_dispatch, isolation, item)
            prepare_futures[future] = item

        def submit_execute(
            pool: ThreadPoolExecutor,
            dispatch: PreparedExecution,
            lease: ExecutionLease | None,
            worker_id: str,
        ) -> None:
            item = dispatch.item
            item.attempt_state.attempt += 1
            item.status = SimpleItemStatus.EXECUTING
            execute_started_at[item.item_id] = time.monotonic()
            self._store.update_simple_item_status(
                run.run_id,
                item.item_id,
                SimpleItemStatus.EXECUTING,
                attempt_state=item.attempt_state.to_dict(),
            )
            self._store.save_simple_event(
                run.run_id,
                "item_started",
                {
                    "target": item.target,
                    "attempt": item.attempt_state.attempt,
                    "warnings": dispatch.prepared.warnings,
                    "worker_id": worker_id,
                    "lease_owner": self._lease_manager.owner_id if self._lease_manager is not None else "",
                },
                item_id=item.item_id,
                bucket=item.bucket,
            )
            future = pool.submit(executor.execute, dispatch.prepared, worker_id)
            exec_futures[future] = (dispatch, lease, worker_id)

        def submit_validate(pool: ThreadPoolExecutor, envelope: ValidationEnvelope) -> None:
            def _validate() -> None:
                validation_started_at = time.monotonic()
                with outcome_lock:
                    scheduler.mark_validating(envelope.dispatch.item.item_id)
                    item = envelope.dispatch.item
                    item.status = SimpleItemStatus.VALIDATING
                    self._store.update_simple_item_status(run.run_id, item.item_id, SimpleItemStatus.VALIDATING)
                    self._store.save_simple_event(
                        run.run_id,
                        "item_revalidation_started" if envelope.revalidated_without_exec else "item_validation_started",
                        {
                            "target": item.target,
                            "revalidated_without_exec": envelope.revalidated_without_exec,
                            "reused_target_changed_files": list(envelope.reused_target_changed_files or []),
                        },
                        item_id=item.item_id,
                        bucket=item.bucket,
                    )
                    outcome_summary = self._handle_outcome(
                        run,
                        layout,
                        scheduler,
                        isolation,
                        run_target_index,
                        item,
                        envelope.dispatch.prepared,
                        envelope.outcome,
                        manifest,
                        reused_target_changed_files=envelope.reused_target_changed_files,
                    )
                    self._store.save_simple_event(
                        run.run_id,
                        "item_validated",
                        {
                            "target": item.target,
                            "duration_ms": round((time.monotonic() - validation_started_at) * 1000, 1),
                            "final_status": outcome_summary["final_status"],
                            "category": outcome_summary["category"],
                            "failure_reason": outcome_summary["failure_reason"],
                        },
                        item_id=item.item_id,
                        bucket=item.bucket,
                        level="warning" if outcome_summary["category"] else "info",
                    )
                    self._record_stage_timing(
                        manifest,
                        "validate",
                        round((time.monotonic() - validation_started_at) * 1000, 1),
                    )

            future = pool.submit(_validate)
            validate_futures[future] = envelope

        def dispatch_prepared(pool: ThreadPoolExecutor) -> bool:
            dispatched = False
            local_exec_limit = min(self._effective_exec_limit, self._throttle_limit, self._config.simple.max_running_processes)
            while len(exec_futures) < local_exec_limit and prepared_queue and available_worker_ids:
                dispatch = prepared_queue.popleft()
                prepared_conflict_keys.discard(
                    self._item_conflict_key(
                        dispatch.item,
                        requested_mode=run.isolation_mode,
                        shared_parent_counts=shared_parent_counts,
                    )
                )
                lease = None
                if self._lease_manager is not None:
                    lease = self._lease_manager.acquire(run.run_id, dispatch.item.item_id, timeout_seconds=0.0)
                    if lease is None:
                        prepared_queue.appendleft(dispatch)
                        prepared_conflict_keys.add(
                            self._item_conflict_key(
                                dispatch.item,
                                requested_mode=run.isolation_mode,
                                shared_parent_counts=shared_parent_counts,
                            )
                        )
                        break
                worker_id = available_worker_ids.popleft()
                submit_execute(pool, dispatch, lease, worker_id)
                dispatched = True
            return dispatched

        def request_worker_slot_warm(pool: ThreadPoolExecutor, requested_count: int, *, reason: str) -> int:
            nonlocal current_prewarm_wave_started_at, current_prewarm_wave_worker_ids, current_prewarm_wave_failures
            if requested_count <= 0:
                return 0
            batch: list[str] = []
            while pending_worker_ids and len(batch) < requested_count:
                batch.append(pending_worker_ids.popleft())
            if not batch:
                return 0
            if current_prewarm_wave_started_at is None:
                current_prewarm_wave_started_at = time.monotonic()
                current_prewarm_wave_worker_ids = []
                current_prewarm_wave_failures = []
            current_prewarm_wave_worker_ids.extend(batch)
            requested_at = time.monotonic()
            ramp_interval_seconds = self._execution_slot_ramp_interval_seconds()
            self._next_slot_warm_ready_at = requested_at + ramp_interval_seconds
            self._store.save_simple_event(
                run.run_id,
                "execution_slots_prewarm_started",
                {
                    "worker_ids": batch,
                    "count": len(batch),
                    "reason": reason,
                    "total_requested_slots": len(warmed_worker_ids) + len(prewarm_futures) + len(batch),
                    "max_execution_slots": max_execution_slots,
                    "slot_ramp_interval_seconds": ramp_interval_seconds,
                },
            )
            for worker_id in batch:
                prewarm_futures[pool.submit(executor.warm_worker_home, worker_id)] = worker_id
            return len(batch)

        def maybe_expand_warm_pool(pool: ThreadPoolExecutor) -> None:
            if not pending_worker_ids:
                return
            local_exec_limit = min(self._effective_exec_limit, self._throttle_limit, self._config.simple.max_running_processes)
            target_slots = min(max_execution_slots, max(1, local_exec_limit))
            known_slots = len(available_worker_ids) + len(prewarm_futures) + len(exec_futures)
            if known_slots >= target_slots:
                return
            if self._execution_slot_ramp_wait_seconds() > 0:
                return
            if not prepared_queue and not prepare_futures:
                return
            if len(available_worker_ids) + len(prewarm_futures) >= 2:
                return
            request_worker_slot_warm(
                pool,
                min(slot_batch_size, target_slots - known_slots),
                reason="prepared_backlog",
            )

        def prepare_failure(item: SimpleWorkItem, exc: Exception) -> None:
            category = "path_budget_exceeded" if "path budget" in str(exc).lower() else "resource_exhausted"
            duration_ms = round((time.monotonic() - prepare_started_at.pop(item.item_id, time.monotonic())) * 1000, 1)
            self._record_stage_timing(
                manifest,
                "prepare",
                duration_ms,
            )
            self._store.save_simple_event(
                run.run_id,
                "item_prepare_failed",
                {
                    "target": item.target,
                    "duration_ms": duration_ms,
                    "category": category,
                    "reason": str(exc),
                },
                item_id=item.item_id,
                bucket=item.bucket,
                level="error",
            )
            item.attempt_state.attempt += 1
            item.attempt_state.last_error_category = category
            item.attempt_state.last_failure_reason = str(exc)
            attempt = SimpleAttempt(
                item_id=item.item_id,
                attempt=item.attempt_state.attempt,
                status=SimpleItemStatus.FAILED,
                worker_id=self._worker_id_for(item),
                started_at=datetime.now(),
                finished_at=datetime.now(),
                error_category=category,
                failure_reason=str(exc),
                error=str(exc),
            )
            self._simple_store.save_attempt(run.run_id, item, attempt)
            retry_delay = self._retry_delay(item, category)
            if retry_delay is not None:
                item.attempt_state.next_retry_at = datetime.now() + timedelta(seconds=retry_delay)
                scheduler.mark_failed(item.item_id, retry_delay_seconds=retry_delay)
                item.status = SimpleItemStatus.RETRY_WAIT
                self._store.save_simple_event(
                    run.run_id,
                    "item_retry_scheduled",
                    {"target": item.target, "delay_seconds": retry_delay, "category": category, "phase": "prepare"},
                    item_id=item.item_id,
                    bucket=item.bucket,
                    level="warning",
                )
            else:
                scheduler.mark_failed(item.item_id, blocked=(category == "path_budget_exceeded"))
                item.status = SimpleItemStatus.BLOCKED if category == "path_budget_exceeded" else SimpleItemStatus.FAILED
                manifest.failed_items += 1
                manifest.error_stats[category] = manifest.error_stats.get(category, 0) + 1
                self._store.save_simple_event(
                    run.run_id,
                    "item_failed",
                    {"target": item.target, "category": category, "reason": str(exc), "phase": "prepare"},
                    item_id=item.item_id,
                    bucket=item.bucket,
                    level="error",
                )
            self._store.update_simple_item_status(
                run.run_id,
                item.item_id,
                item.status,
                attempt_state=item.attempt_state.to_dict(),
                last_error_category=item.attempt_state.last_error_category,
                last_failure_reason=item.attempt_state.last_failure_reason,
            )

        try:
            with (
                ThreadPoolExecutor(max_workers=max(1, min(max_execution_slots, self._config.simple.max_running_processes))) as prewarm_pool,
                ThreadPoolExecutor(max_workers=prepare_workers) as prepare_pool,
                ThreadPoolExecutor(max_workers=self._config.simple.max_running_processes) as exec_pool,
                ThreadPoolExecutor(max_workers=validate_workers) as validate_pool,
            ):
                request_worker_slot_warm(prewarm_pool, initial_execution_slots, reason="initial")
                while (
                    scheduler.has_work()
                    or prewarm_futures
                    or prepare_futures
                    or exec_futures
                    or validate_futures
                    or prepared_queue
                ):
                    self._adjust_dynamic_limit(run.run_id, manifest, scheduler)
                    self._maintenance_tick(run)
                    self._renew_execution_leases(exec_futures)

                    if (
                        self.requested_exit_code == 0
                        and self._pool_runtime is not None
                        and not prewarm_futures
                        and not prepare_futures
                        and not exec_futures
                        and not validate_futures
                        and not prepared_queue
                        and self._pool_runtime.should_failback()
                    ):
                        primary = self._pool_runtime.config.primary_profile.name
                        self._pool_runtime.write_request(
                            "failback",
                            target_profile=primary,
                            reason="primary_recovered",
                            metadata={"run_id": run.run_id},
                        )
                        self.requested_exit_code = PoolRuntime.EXIT_CODE_FAILBACK

                    reserved_items = len(prepare_futures) + len(prepared_queue) + len(exec_futures) + len(validate_futures)
                    available_prepare = max(0, max_reserved_items - reserved_items)
                    active_conflict_keys = self._active_targets(
                        prepare_futures,
                        prepared_conflict_keys,
                        exec_futures,
                        validate_futures,
                        requested_mode=run.isolation_mode,
                        shared_parent_counts=shared_parent_counts,
                    )
                    if self.requested_exit_code == 0:
                        for item in scheduler.pop_ready(
                            available_prepare,
                            active_conflict_keys,
                            conflict_key_fn=lambda current_item: self._item_conflict_key(
                                current_item,
                                requested_mode=run.isolation_mode,
                                shared_parent_counts=shared_parent_counts,
                            ),
                        ):
                            submit_prepare(prepare_pool, item)

                    local_exec_limit = min(self._effective_exec_limit, self._throttle_limit, self._config.simple.max_running_processes)
                    dispatch_prepared(exec_pool)
                    maybe_expand_warm_pool(prewarm_pool)
                    slot_ramp_wait_seconds = self._execution_slot_ramp_wait_seconds()

                    all_futures: list[Future] = [*prewarm_futures.keys(), *prepare_futures.keys(), *exec_futures.keys(), *validate_futures.keys()]
                    if not all_futures:
                        if self.requested_exit_code != 0:
                            break
                        time.sleep(
                            self._wait_timeout_seconds(
                                len(prepared_queue),
                                len(exec_futures),
                                local_exec_limit,
                                len(prepare_futures),
                                slot_ramp_wait_seconds=slot_ramp_wait_seconds,
                            )
                        )
                        continue

                    done, _ = wait(
                        all_futures,
                        timeout=self._wait_timeout_seconds(
                            len(prepared_queue),
                            len(exec_futures),
                            local_exec_limit,
                            len(prepare_futures),
                            slot_ramp_wait_seconds=slot_ramp_wait_seconds,
                        ),
                        return_when=FIRST_COMPLETED,
                    )
                    if not done:
                        continue
                    completed_prewarm_waves: list[dict[str, object]] = []
                    for future in done:
                        if future in prewarm_futures:
                            worker_id = prewarm_futures.pop(future)
                            try:
                                future.result()
                                available_worker_ids.append(worker_id)
                                warmed_worker_ids.append(worker_id)
                            except Exception as exc:
                                current_prewarm_wave_failures.append({"worker_id": worker_id, "error": str(exc)})
                                available_worker_ids.append(worker_id)
                                warmed_worker_ids.append(worker_id)
                                self._store.save_simple_event(
                                    run.run_id,
                                    "execution_slot_warm_failed",
                                    {"worker_id": worker_id, "error": str(exc)},
                                    level="warning",
                                )
                            if not prewarm_futures and current_prewarm_wave_started_at is not None:
                                completed_prewarm_waves.append(
                                    {
                                        "worker_ids": list(current_prewarm_wave_worker_ids),
                                        "duration_ms": round((time.monotonic() - current_prewarm_wave_started_at) * 1000, 1),
                                        "failures": list(current_prewarm_wave_failures),
                                        "total_warmed_slots": len(warmed_worker_ids),
                                        "max_execution_slots": max_execution_slots,
                                    }
                                )
                                current_prewarm_wave_started_at = None
                                current_prewarm_wave_worker_ids = []
                                current_prewarm_wave_failures = []
                            continue
                        if future in prepare_futures:
                            item = prepare_futures.pop(future)
                            try:
                                dispatch = future.result()
                            except Exception as exc:
                                prepare_failure(item, exc)
                                continue
                            prepare_duration_ms = round((time.monotonic() - prepare_started_at.pop(item.item_id, time.monotonic())) * 1000, 1)
                            self._record_stage_timing(manifest, "prepare", prepare_duration_ms)
                            self._store.save_simple_event(
                                run.run_id,
                                "item_prepared",
                                {
                                    "target": item.target,
                                    "duration_ms": prepare_duration_ms,
                                    "warnings": dispatch.prepared.warnings,
                                    "effective_mode": dispatch.prepared.effective_mode,
                                },
                                item_id=item.item_id,
                                bucket=item.bucket,
                            )
                            reused_target_changed_files = self._existing_state_revalidation_files(run.run_id, item)
                            if reused_target_changed_files:
                                item.attempt_state.attempt += 1
                                item.status = SimpleItemStatus.VALIDATING
                                self._store.update_simple_item_status(
                                    run.run_id,
                                    item.item_id,
                                    SimpleItemStatus.VALIDATING,
                                    attempt_state=item.attempt_state.to_dict(),
                                )
                                submit_validate(
                                    validate_pool,
                                    ValidationEnvelope(
                                        dispatch=dispatch,
                                        outcome=self._build_revalidation_outcome(item),
                                        reused_target_changed_files=reused_target_changed_files,
                                        revalidated_without_exec=True,
                                    ),
                                )
                                continue
                            prepared_queue.append(dispatch)
                            prepared_conflict_keys.add(
                                self._item_conflict_key(
                                    dispatch.item,
                                    requested_mode=run.isolation_mode,
                                    shared_parent_counts=shared_parent_counts,
                                )
                            )
                            maybe_expand_warm_pool(prewarm_pool)
                            continue
                        if future in exec_futures:
                            dispatch, lease, worker_id = exec_futures.pop(future)
                            if self._lease_manager is not None:
                                self._lease_manager.release(lease)
                            available_worker_ids.append(worker_id)
                            try:
                                outcome = future.result()
                            except Exception as exc:
                                task_result = TaskResult(
                                    task_id=dispatch.item.item_id,
                                    status=TaskStatus.FAILED,
                                    error=str(exc),
                                    started_at=datetime.now(),
                                    finished_at=datetime.now(),
                                )
                                outcome = ExecutionOutcome(
                                    result=task_result,
                                    attempt=SimpleAttempt(
                                        item_id=dispatch.item.item_id,
                                        attempt=dispatch.item.attempt_state.attempt,
                                        status=SimpleItemStatus.FAILED,
                                        worker_id=worker_id,
                                        started_at=datetime.now(),
                                        finished_at=datetime.now(),
                                        error=str(exc),
                                    ),
                                    changed_files=[],
                                    prompt="",
                                )
                            execute_duration_ms = round((time.monotonic() - execute_started_at.pop(dispatch.item.item_id, time.monotonic())) * 1000, 1)
                            self._record_stage_timing(manifest, "execute", execute_duration_ms)
                            self._store.save_simple_event(
                                run.run_id,
                                "item_execution_finished",
                                {
                                    "target": dispatch.item.target,
                                    "worker_id": worker_id,
                                    "duration_ms": execute_duration_ms,
                                    "task_status": outcome.result.status.value,
                                    "changed_files": outcome.changed_files,
                                    "pid": outcome.attempt.pid,
                                    "tool_uses": outcome.attempt.tool_uses,
                                    "turn_started": outcome.attempt.turn_started,
                                    "turn_completed": outcome.attempt.turn_completed,
                                    "token_input": outcome.attempt.token_input,
                                    "token_output": outcome.attempt.token_output,
                                    "cli_duration_ms": outcome.attempt.cli_duration_ms,
                                    "claude_home_ready_ms": outcome.attempt.claude_home_ready_ms,
                                    "execution_wall_ms": outcome.attempt.execution_wall_ms,
                                    "max_turns_exceeded": outcome.attempt.max_turns_exceeded,
                                },
                                item_id=dispatch.item.item_id,
                                bucket=dispatch.item.bucket,
                                level="warning" if outcome.result.status.value != "success" else "info",
                            )
                            submit_validate(validate_pool, ValidationEnvelope(dispatch=dispatch, outcome=outcome))
                            continue
                        envelope = validate_futures.pop(future)
                        future.result()
                    dispatch_prepared(exec_pool)
                    maybe_expand_warm_pool(prewarm_pool)
                    for wave in completed_prewarm_waves:
                        self._store.save_simple_event(
                            run.run_id,
                            "execution_slots_warmed",
                            wave,
                            level="warning" if wave["failures"] else "info",
                        )
        except Exception as exc:
            self._mark_run_aborted(run, exc)
            raise
        finally:
            if self._lease_manager is not None:
                self._lease_manager.close()
            self._lease_manager = None
            self._process_heartbeat.stop()
            self._stop_run_heartbeat_pump(run, heartbeat_stop, heartbeat_thread)

        if self.requested_exit_code != 0:
            run.status = SimpleRunStatus.RUNNING
            run.last_heartbeat_at = _next_run_wallclock(run)
            run.finished_at = None
            if self._pool_runtime is not None:
                run.pool_id = self._pool_runtime.pool_id
                run.active_profile = self._pool_runtime.active_profile
            self._store.set_context(run.run_id, "simple.rate_limiter", json.dumps(self._rate_limiter.get_state()))
            self._store.update_simple_run(
                run.run_id,
                status=SimpleRunStatus.RUNNING,
                last_heartbeat_at=run.last_heartbeat_at,
                clear_finished_at=True,
                pool_id=run.pool_id,
                active_profile=run.active_profile,
            )
            self._store.update_run_status(run.run_id, RunStatus.RUNNING)
            payload = build_simple_status_payload(
                run,
                manifest.to_dict(),
                items,
                self._store.get_simple_events(run.run_id, limit=20),
            )
            return run, payload

        manifest.completed_items = sum(1 for item in items if item.status == SimpleItemStatus.SUCCEEDED)
        manifest.failed_items = sum(1 for item in items if item.status in (SimpleItemStatus.FAILED, SimpleItemStatus.BLOCKED))
        manifest.retried_success_items = sum(
            1 for item in items
            if item.status == SimpleItemStatus.SUCCEEDED and item.attempt_state.attempt > 1
        )
        manifest.error_stats = {}
        for item in items:
            if item.status in (SimpleItemStatus.FAILED, SimpleItemStatus.BLOCKED) and item.attempt_state.last_error_category:
                manifest.error_stats[item.attempt_state.last_error_category] = (
                    manifest.error_stats.get(item.attempt_state.last_error_category, 0) + 1
                )
        run.status = SimpleRunStatus.COMPLETED if manifest.failed_items == 0 else (
            SimpleRunStatus.PARTIAL_SUCCESS if manifest.completed_items > 0 else SimpleRunStatus.FAILED
        )
        manifest.total_cost_usd = self._budget.spent
        run.finished_at = _next_run_wallclock(run)
        manifest.duration_seconds = max(0.0, (run.finished_at - run.started_at).total_seconds())
        run.last_heartbeat_at = run.finished_at
        manifest.throttle_events = self._store.get_simple_events(run.run_id, limit=50)
        for bucket_name, stats in scheduler.bucket_stats().items():
            manifest.bucket_stats[bucket_name] = stats
            self._store.save_simple_bucket(run.run_id, stats)
        manifest_path = write_simple_manifest(layout, run, manifest, items)
        run.manifest_path = str(manifest_path)
        self._store.set_context(run.run_id, "simple.rate_limiter", json.dumps(self._rate_limiter.get_state()))
        self._simple_store.complete_run(run, manifest)
        return run, build_simple_status_payload(run, manifest.to_dict(), items, self._store.get_simple_events(run.run_id, limit=20))

    def resume(self, run_id: str, *, retry_failed: bool = False) -> tuple[SimpleRun, dict[str, Any]]:
        run = self._simple_store.load_run(run_id)
        if run is None:
            raise ValueError(f"simple run '{run_id}' not found")
        self.recover_stale_runs()
        run = self._simple_store.load_run(run_id)
        if run is None:
            raise ValueError(f"simple run '{run_id}' not found")
        if retry_failed:
            self._simple_store.reset_failed_for_retry(run_id)
        elif self._config.simple.resume_reset_running_to_pending:
            self._simple_store.reset_for_resume(run_id)
        items = self._simple_store.load_items(run_id)
        original_working_dir = self._working_dir
        try:
            self._working_dir = Path(run.working_dir).resolve()
            return self.run(run.instruction_template, isolation_mode=run.isolation_mode, existing_run=run, existing_items=items)
        finally:
            self._working_dir = original_working_dir

    def status_payload(self, run_id: str) -> dict[str, Any]:
        self.recover_stale_runs()
        run = self._simple_store.load_run(run_id)
        if run is None:
            raise ValueError(f"simple run '{run_id}' not found")
        items = self._simple_store.load_items(run_id)
        manifest = self._store.get_simple_manifest(run_id)
        events = self._store.get_simple_events(run_id, limit=20)
        return build_simple_status_payload(run, manifest, items, events)

    def _try_recover_unauthorized_changes(
        self,
        run: SimpleRun,
        item: SimpleWorkItem,
        prepared: PreparedItemWorkspace,
        isolation: SimpleIsolationManager,
        outcome: ExecutionOutcome,
        report,
        *,
        copyback_ok: bool,
        copyback_reason: str,
    ):
        if not self._config.simple.auto_recover_unauthorized_changes:
            return report
        if report.failure_code != "unauthorized_side_files":
            return report
        if not report.target_touched or not report.target_changed_files:
            return report
        if not report.unauthorized_changes:
            return report
        recovered = isolation.rollback_changed_files(prepared, report.unauthorized_changes)
        if not recovered:
            return report
        recovered_changed_files = prepared.collect_changed_files()
        recovered_report = self._validation.validate(
            prepared,
            outcome.result,
            recovered_changed_files,
            copyback_ok=copyback_ok,
            copyback_reason=copyback_reason,
        )
        if not recovered_report.passed:
            return report
        recovered_report.rollback_performed = True
        recovered_report.recovered_unauthorized_changes = list(report.unauthorized_changes)
        recovered_report.warnings.append(
            "编排器已自动回滚未授权旁路修改，仅保留目标文件改动"
        )
        outcome.changed_files = recovered_changed_files
        outcome.attempt.changed_files = list(recovered_changed_files)
        self._store.save_simple_event(
            run.run_id,
            "item_unauthorized_changes_recovered",
            {
                "target": item.target,
                "recovered_files": list(report.unauthorized_changes),
                "kept_files": list(recovered_changed_files),
            },
            item_id=item.item_id,
            bucket=item.bucket,
            level="warning",
        )
        return recovered_report

    def _handle_outcome(
        self,
        run: SimpleRun,
        layout: SimpleRuntimeLayout,
        scheduler: SimpleScheduler,
        isolation: SimpleIsolationManager,
        run_target_index: RunTargetIndex,
        item: SimpleWorkItem,
        prepared: PreparedItemWorkspace,
        outcome: ExecutionOutcome,
        manifest: SimpleManifest,
        *,
        reused_target_changed_files: list[str] | None = None,
    ) -> dict[str, str]:
        result = outcome.result
        copyback_ok = True
        copyback_reason = ""
        if result.status.value == "success":
            copyback_ok, copyback_reason = isolation.copy_back(prepared, outcome.changed_files)
        report = self._validation.validate(
            prepared,
            result,
            outcome.changed_files,
            copyback_ok=copyback_ok,
            copyback_reason=copyback_reason,
            reused_target_changed_files=reused_target_changed_files,
        )
        if prepared.effective_mode == SimpleIsolationMode.COPY.value:
            report = self._try_recover_unauthorized_changes(
                run,
                item,
                prepared,
                isolation,
                outcome,
                report,
                copyback_ok=copyback_ok,
                copyback_reason=copyback_reason,
            )
        conflicting_targets = self._conflicting_run_targets(item, report, run_target_index)
        if not conflicting_targets:
            report = self._try_recover_unauthorized_changes(
                run,
                item,
                prepared,
                isolation,
                outcome,
                report,
                copyback_ok=copyback_ok,
                copyback_reason=copyback_reason,
            )
            conflicting_targets = self._conflicting_run_targets(item, report, run_target_index)
        if not report.passed and self._config.simple.rollback_on_validation_failure:
            report.rollback_performed = isolation.rollback_target(prepared)
            if report.target_changed_files and prepared.item.item_type.value == "directory_shard":
                report.rollback_performed = isolation.rollback_changed_files(prepared, report.target_changed_files) and report.rollback_performed
            if report.unauthorized_changes and not conflicting_targets:
                report.rollback_performed = isolation.rollback_changed_files(prepared, report.unauthorized_changes) and report.rollback_performed
            elif conflicting_targets:
                report.warnings.append(
                    "检测到同 run 目标冲突，已跳过未授权文件回滚以避免覆盖其他工单结果"
                )
            isolation.restore_from_source(prepared, outcome.changed_files)

        base_category = classify_simple_failure(result, report)
        category = base_category
        if not report.passed and conflicting_targets:
            category = SimpleErrorCategory.CONFLICT_DETECTED.value
            report.failure_code = SimpleErrorCategory.CONFLICT_DETECTED.value
            report.failure_reason = self._conflict_failure_reason(conflicting_targets)
            self._store.save_simple_event(
                run.run_id,
                "item_conflict_detected",
                {
                    "target": item.target,
                    "conflicting_targets": conflicting_targets,
                    "original_failure_code": base_category,
                },
                item_id=item.item_id,
                bucket=item.bucket,
                level="warning",
            )
        outcome.attempt.status = SimpleItemStatus.SUCCEEDED if report.passed else SimpleItemStatus.FAILED
        outcome.attempt.validation_report = report
        outcome.attempt.changed_files = list(outcome.changed_files or reused_target_changed_files or [])
        outcome.attempt.error_category = "" if report.passed else category
        outcome.attempt.failure_reason = report.failure_reason
        self._record_attempt_metrics(manifest, outcome.attempt)
        self._simple_store.save_attempt(run.run_id, item, outcome.attempt)
        self._write_attempt_artifact(layout, item, outcome)

        if report.passed:
            scheduler.mark_succeeded(item.item_id)
            item.status = SimpleItemStatus.SUCCEEDED
            item.attempt_state.last_error_category = ""
            item.attempt_state.last_failure_reason = ""
            manifest.completed_items += 1
            if item.attempt_state.attempt > 1:
                manifest.retried_success_items += 1
            self._store.save_simple_event(
                run.run_id,
                "item_succeeded",
                {
                    "target": item.target,
                    "attempt": item.attempt_state.attempt,
                    "changed_files": list(outcome.attempt.changed_files),
                    "revalidated_without_exec": bool(reused_target_changed_files and not outcome.changed_files),
                },
                item_id=item.item_id,
                bucket=item.bucket,
            )
            self._record_pressure_event("success")
        else:
            item.attempt_state.last_error_category = category
            item.attempt_state.last_failure_reason = report.failure_reason
            self._record_pressure_event(category)
            retry_delay = self._retry_delay(item, category)
            if retry_delay is not None:
                item.attempt_state.next_retry_at = datetime.now() + timedelta(seconds=retry_delay)
                scheduler.mark_failed(item.item_id, retry_delay_seconds=retry_delay)
                item.status = SimpleItemStatus.RETRY_WAIT
                self._store.save_simple_event(
                    run.run_id,
                    "item_retry_scheduled",
                    {"target": item.target, "delay_seconds": retry_delay, "category": category},
                    item_id=item.item_id,
                    bucket=item.bucket,
                    level="warning",
                )
            else:
                scheduler.mark_failed(item.item_id)
                item.status = SimpleItemStatus.FAILED
                manifest.failed_items += 1
                manifest.error_stats[category] = manifest.error_stats.get(category, 0) + 1
                self._store.save_simple_event(
                    run.run_id,
                    "item_failed",
                    {"target": item.target, "category": category, "reason": report.failure_reason},
                    item_id=item.item_id,
                    bucket=item.bucket,
                    level="error",
                )

        for stage in report.stage_results:
            key = f"{stage.name}:{'pass' if stage.passed else 'fail'}"
            manifest.validation_stats[key] = manifest.validation_stats.get(key, 0) + 1

        self._store.update_simple_item_status(
            run.run_id,
            item.item_id,
            item.status,
            attempt_state=item.attempt_state.to_dict(),
            last_error_category=item.attempt_state.last_error_category,
            last_failure_reason=item.attempt_state.last_failure_reason,
        )

        bucket_stats = scheduler.bucket_stats().get(item.bucket)
        if bucket_stats is not None:
            manifest.bucket_stats[item.bucket] = bucket_stats
            self._store.save_simple_bucket(run.run_id, bucket_stats)
        self._store.update_simple_run(run.run_id, status=SimpleRunStatus.RUNNING)
        return {
            "final_status": item.status.value,
            "category": "" if report.passed else category,
            "failure_reason": report.failure_reason,
        }

    def _retry_delay(self, item: SimpleWorkItem, category: str) -> float | None:
        if item.attempt_state.attempt >= item.attempt_state.max_attempts:
            return None
        if category == "network_error":
            if item.attempt_state.attempt <= 2:
                return 0.0
            return min(60.0, 5.0 * (2 ** max(0, item.attempt_state.attempt - 3)))
        if category == SimpleErrorCategory.CONFLICT_DETECTED.value:
            return 0.0
        if category in {"rate_limited", "resource_exhausted", "copyback_conflict"}:
            return min(300.0, 15.0 * (2 ** (item.attempt_state.attempt - 1)))
        if category in {"timeout", "auth_expired"}:
            return 30.0
        return None

    def _adjust_dynamic_limit(self, run_id: str, manifest: SimpleManifest, scheduler: SimpleScheduler) -> None:
        if not self._config.simple.dynamic_throttle_enabled:
            return
        target = self._config.simple.max_running_processes
        new_limit = target
        if psutil is not None:
            try:
                cpu = psutil.cpu_percent(interval=None)
                memory = psutil.virtual_memory().percent
                disk_free_mb = shutil.disk_usage(self._working_dir).free // (1024 * 1024)
                if cpu > self._config.simple.cpu_percent_max or memory > self._config.simple.memory_percent_max or disk_free_mb < self._config.simple.disk_free_mb_min:
                    new_limit = max(1, target // 2)
            except Exception:
                new_limit = target
        pressure_counts = self._recent_pressure_counts()
        if pressure_counts.get("rate_limited", 0) or pressure_counts.get("resource_exhausted", 0) or pressure_counts.get("copyback_conflict", 0):
            new_limit = max(1, min(new_limit, target // 2 or 1))
        if new_limit != self._throttle_limit:
            self._throttle_limit = new_limit
            self._store.save_simple_event(
                run_id,
                "dynamic_throttle",
                {"max_running_processes": new_limit},
                level="warning" if new_limit < target else "info",
            )

    def _record_pressure_event(self, category: str) -> None:
        now = time.time()
        self._recent_pressure_events.append((now, category))
        cutoff = now - self._pressure_window_seconds
        while self._recent_pressure_events and self._recent_pressure_events[0][0] < cutoff:
            self._recent_pressure_events.popleft()

    def _recent_pressure_counts(self) -> dict[str, int]:
        self._record_pressure_event("__tick__")
        counts: dict[str, int] = {}
        filtered: deque[tuple[float, str]] = deque()
        for ts, category in self._recent_pressure_events:
            if category == "__tick__":
                continue
            filtered.append((ts, category))
            counts[category] = counts.get(category, 0) + 1
        self._recent_pressure_events = filtered
        return counts

    def _write_attempt_artifact(self, layout: SimpleRuntimeLayout, item: SimpleWorkItem, outcome: ExecutionOutcome) -> None:
        item_dir = layout.artifacts / item.item_id
        item_dir.mkdir(parents=True, exist_ok=True)
        attempt_path = item_dir / f"attempt-{item.attempt_state.attempt}.json"
        payload = {
            "item": item.to_dict(),
            "attempt": outcome.attempt.to_dict(),
            "prompt": outcome.prompt,
        }
        attempt_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        self._store.save_simple_artifact(
            layout.root.name,
            item.item_id,
            "attempt_json",
            str(attempt_path),
            {"attempt": item.attempt_state.attempt},
        )
