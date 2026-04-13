"""In-memory scheduler for simple mode."""

from __future__ import annotations

import heapq
import itertools
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Callable, Iterable

from .simple_model import BucketStats, SimpleItemStatus, SimpleWorkItem


@dataclass
class SchedulerSnapshot:
    pending: int
    ready: int
    running: int
    retry_wait: int


class SimpleScheduler:
    def __init__(
        self,
        items: Iterable[SimpleWorkItem],
        *,
        max_pending_tasks: int,
        fair_scheduling: bool = True,
    ):
        self._items: dict[str, SimpleWorkItem] = {item.item_id: item for item in items}
        if len(self._items) > max_pending_tasks:
            raise ValueError(f"too many simple work items: {len(self._items)} > {max_pending_tasks}")
        self._fair = fair_scheduling
        self._bucket_order: deque[str] = deque()
        self._bucket_queues: dict[str, deque[str]] = defaultdict(deque)
        self._retry_heap: list[tuple[float, int, str]] = []
        self._retry_counter = itertools.count()
        self._running: set[str] = set()
        self._blocked: set[str] = set()
        self._bucket_stats: dict[str, BucketStats] = {}
        for item in self._items.values():
            if item.status in (SimpleItemStatus.PENDING, SimpleItemStatus.READY):
                item.status = SimpleItemStatus.READY
                self._bucket_queues[item.bucket].append(item.item_id)
            elif item.status == SimpleItemStatus.RETRY_WAIT:
                next_retry_at = item.attempt_state.next_retry_at.timestamp() if item.attempt_state.next_retry_at else time.time()
                heapq.heappush(self._retry_heap, (next_retry_at, next(self._retry_counter), item.item_id))
            elif item.status == SimpleItemStatus.BLOCKED:
                self._blocked.add(item.item_id)
            if item.bucket not in self._bucket_order:
                self._bucket_order.append(item.bucket)
            stats = self._bucket_stats.setdefault(item.bucket, BucketStats(name=item.bucket))
            stats.total_items += 1
            if item.status == SimpleItemStatus.SUCCEEDED:
                stats.completed_items += 1
            elif item.status in (SimpleItemStatus.FAILED, SimpleItemStatus.BLOCKED):
                stats.failed_items += 1

    def snapshot(self) -> SchedulerSnapshot:
        ready = sum(len(queue) for queue in self._bucket_queues.values())
        return SchedulerSnapshot(
            pending=ready + len(self._retry_heap),
            ready=ready,
            running=len(self._running),
            retry_wait=len(self._retry_heap),
        )

    def has_work(self) -> bool:
        return bool(self._running or self._retry_heap or any(self._bucket_queues.values()))

    def release_due_retries(self) -> None:
        now = time.time()
        while self._retry_heap and self._retry_heap[0][0] <= now:
            _, _, item_id = heapq.heappop(self._retry_heap)
            item = self._items[item_id]
            item.status = SimpleItemStatus.READY
            self._bucket_queues[item.bucket].append(item_id)

    def pop_ready(
        self,
        limit: int,
        active_targets: set[str] | None = None,
        *,
        conflict_key_fn: Callable[[SimpleWorkItem], str] | None = None,
    ) -> list[SimpleWorkItem]:
        active_targets = active_targets or set()
        self.release_due_retries()
        chosen: list[SimpleWorkItem] = []
        skipped: list[str] = []
        while len(chosen) < limit and any(self._bucket_queues.values()):
            bucket = self._next_bucket()
            if bucket is None:
                break
            queue = self._bucket_queues[bucket]
            if not queue:
                continue
            item_id = queue.popleft()
            item = self._items[item_id]
            # 冲突检测：优先使用 conflict_key_fn，否则回退到 target
            conflict_key = conflict_key_fn(item) if conflict_key_fn else item.target
            if conflict_key in active_targets:
                skipped.append(item_id)
                continue
            item.status = SimpleItemStatus.PREPARING
            self._running.add(item_id)
            chosen.append(item)
            stats = self._bucket_stats[item.bucket]
            stats.running_items += 1
        for item_id in skipped:
            item = self._items[item_id]
            self._bucket_queues[item.bucket].append(item_id)
        return chosen

    def _next_bucket(self) -> str | None:
        if not self._bucket_order:
            return None
        if not self._fair:
            for bucket in self._bucket_order:
                if self._bucket_queues[bucket]:
                    return bucket
            return None
        for _ in range(len(self._bucket_order)):
            bucket = self._bucket_order[0]
            self._bucket_order.rotate(-1)
            if self._bucket_queues[bucket]:
                return bucket
        return None

    def mark_validating(self, item_id: str) -> None:
        item = self._items[item_id]
        item.status = SimpleItemStatus.VALIDATING

    def mark_succeeded(self, item_id: str) -> None:
        item = self._items[item_id]
        item.status = SimpleItemStatus.SUCCEEDED
        self._running.discard(item_id)
        stats = self._bucket_stats[item.bucket]
        stats.running_items = max(0, stats.running_items - 1)
        stats.completed_items += 1

    def mark_failed(self, item_id: str, *, retry_delay_seconds: float | None = None, blocked: bool = False) -> None:
        item = self._items[item_id]
        self._running.discard(item_id)
        stats = self._bucket_stats[item.bucket]
        stats.running_items = max(0, stats.running_items - 1)
        if blocked:
            item.status = SimpleItemStatus.BLOCKED
            self._blocked.add(item_id)
            stats.failed_items += 1
            return
        if retry_delay_seconds is not None:
            item.status = SimpleItemStatus.RETRY_WAIT
            stats.retries += 1
            heapq.heappush(self._retry_heap, (time.time() + retry_delay_seconds, next(self._retry_counter), item_id))
            return
        item.status = SimpleItemStatus.FAILED
        stats.failed_items += 1

    def all_items(self) -> list[SimpleWorkItem]:
        return list(self._items.values())

    def bucket_stats(self) -> dict[str, BucketStats]:
        return self._bucket_stats
