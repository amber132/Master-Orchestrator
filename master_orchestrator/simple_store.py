"""Higher-level store helpers for simple mode."""

from __future__ import annotations

from datetime import datetime

from .model import RunInfo, RunStatus
from .simple_model import (
    BucketStats,
    SimpleAttempt,
    SimpleItemStatus,
    SimpleManifest,
    SimpleRun,
    SimpleRunStatus,
    SimpleWorkItem,
)
from .store import Store


class SimpleStore:
    def __init__(self, store: Store):
        self._store = store

    def create_run(self, run: SimpleRun, items: list[SimpleWorkItem]) -> None:
        bucket_totals: dict[str, int] = {}
        for item in items:
            bucket_totals[item.bucket] = bucket_totals.get(item.bucket, 0) + 1
        # 用指令文本作为 DAG 名称，便于在 dashboard 中区分不同任务
        instruction_name = run.instruction_template[:60].strip() if run.instruction_template else ""
        dag_name = f"simple:{instruction_name}" if instruction_name else f"simple:{run.isolation_mode}"
        self._store.create_simple_run_bundle(
            RunInfo(
                run_id=run.run_id,
                dag_name=dag_name,
                dag_hash="simple",
                status=RunStatus.RUNNING,
                started_at=run.started_at,
            ),
            run,
            items,
            [BucketStats(name=bucket_name, total_items=total) for bucket_name, total in sorted(bucket_totals.items())],
        )

    def complete_run(self, run: SimpleRun, manifest: SimpleManifest) -> None:
        self._store.save_simple_manifest(run.run_id, manifest)
        run.finished_at = datetime.now()
        run.status = (
            SimpleRunStatus.COMPLETED if manifest.failed_items == 0
            else SimpleRunStatus.PARTIAL_SUCCESS if manifest.completed_items > 0
            else SimpleRunStatus.FAILED
        )
        self._store.update_simple_run(
            run.run_id,
            status=run.status,
            manifest_path=run.manifest_path,
            last_heartbeat_at=run.last_heartbeat_at,
            finished_at=run.finished_at,
        )
        self._store.update_run_status(
            run.run_id,
            RunStatus.COMPLETED if run.status in (SimpleRunStatus.COMPLETED, SimpleRunStatus.PARTIAL_SUCCESS) else RunStatus.FAILED,
            cost=manifest.total_cost_usd,
        )

    def save_attempt(self, run_id: str, item: SimpleWorkItem, attempt: SimpleAttempt) -> None:
        self._store.save_simple_attempt(run_id, attempt)
        self._store.update_simple_item_status(
            run_id,
            item.item_id,
            attempt.status,
            attempt_state=item.attempt_state.to_dict(),
            last_error_category=attempt.error_category,
            last_failure_reason=attempt.failure_reason,
        )

    def load_run(self, run_id: str) -> SimpleRun | None:
        return self._store.get_simple_run(run_id)

    def touch_run_heartbeat(self, run_id: str, at: datetime | None = None) -> None:
        self._store.touch_simple_run_heartbeat(run_id, at)

    def find_stale_runs(self, stale_before: datetime, statuses: list[SimpleRunStatus] | None = None) -> list[SimpleRun]:
        return self._store.find_stale_simple_runs(stale_before, statuses=statuses)

    def count_live_runs(self, stale_before: datetime, statuses: list[SimpleRunStatus] | None = None) -> int:
        return self._store.count_live_simple_runs(stale_before, statuses=statuses)

    def load_items(self, run_id: str, statuses: list[SimpleItemStatus] | None = None) -> list[SimpleWorkItem]:
        return self._store.get_simple_items(run_id, statuses=statuses)

    def get_latest_attempt(self, run_id: str, item_id: str) -> SimpleAttempt | None:
        attempts = self._store.get_simple_attempts(run_id, item_id=item_id)
        return attempts[-1] if attempts else None

    def reset_for_resume(self, run_id: str) -> int:
        return self._store.reset_simple_items(
            run_id,
            [
                SimpleItemStatus.PREPARING,
                SimpleItemStatus.EXECUTING,
                SimpleItemStatus.RUNNING,
                SimpleItemStatus.VALIDATING,
            ],
            SimpleItemStatus.READY,
        )

    def reset_failed_for_retry(self, run_id: str) -> int:
        return self._store.reset_simple_items(
            run_id,
            [SimpleItemStatus.FAILED, SimpleItemStatus.BLOCKED],
            SimpleItemStatus.READY,
        )
