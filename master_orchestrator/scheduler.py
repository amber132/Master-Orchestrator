"""DAG scheduler: topological ordering, ready queue, parallel control, failure propagation."""

from __future__ import annotations

import logging
import threading
from typing import Any

from .concurrency_group import ConcurrencyGroupManager
from .exceptions import TaskConditionError
from .model import DAG, TaskNode, TaskStatus
from .redundancy_detector import RedundancyDetector

logger = logging.getLogger(__name__)


class Scheduler:
    """Manages task states and determines which tasks are ready to run."""

    def __init__(self, dag: DAG, max_parallel: int | None = None, queue_capacity: int = 1000, concurrency_manager: ConcurrencyGroupManager | None = None, redundancy_detector: RedundancyDetector | None = None, max_write_parallel: int | None = None):
        self._dag = dag
        self._max_parallel = max_parallel if max_parallel is not None else dag.max_parallel
        self._max_write_parallel = max_write_parallel if max_write_parallel is not None else self._max_parallel
        self._read_pool_size = self._max_parallel + self._max_parallel // 2  # 读任务独立并发池
        self._queue_capacity = queue_capacity
        self._concurrency_manager = concurrency_manager
        self._redundancy_detector = redundancy_detector
        self._states: dict[str, TaskStatus] = {tid: TaskStatus.PENDING for tid in dag.tasks}
        self._running_count = 0
        self._read_running_count = 0  # 只读任务的运行计数
        self._loop_counts: dict[str, int] = {}  # 跟踪每个任务的循环次数
        self._lock = threading.Lock()  # 保护 _states, _running_count, _read_running_count, _loop_counts
        self._detect_cycles()

    def _detect_cycles(self) -> None:
        """检测 DAG 中的循环依赖，如果存在则抛出 ValueError。"""
        in_degree = {}
        for tid, task in self._dag.tasks.items():
            in_degree.setdefault(tid, 0)
            for dep in task.depends_on:
                if dep in self._dag.tasks:
                    in_degree[tid] = in_degree.get(tid, 0) + 1
        queue_list = [tid for tid, deg in in_degree.items() if deg == 0]
        visited = 0
        while queue_list:
            node = queue_list.pop(0)
            visited += 1
            for tid, task in self._dag.tasks.items():
                if node in task.depends_on:
                    in_degree[tid] -= 1
                    if in_degree[tid] == 0:
                        queue_list.append(tid)
        if visited < len(self._dag.tasks):
            cycle_tasks = [tid for tid, deg in in_degree.items() if deg > 0]
            raise ValueError(f"DAG 存在循环依赖: {cycle_tasks[:10]}")

    @property
    def states(self) -> dict[str, TaskStatus]:
        with self._lock:
            return dict(self._states)

    @property
    def running_count(self) -> int:
        with self._lock:
            return self._running_count

    def restore_state(self, task_states: dict[str, TaskStatus], loop_counts: dict[str, int] | None = None) -> None:
        """Restore task states from checkpoint (for resume)."""
        with self._lock:
            for tid, status in task_states.items():
                if tid in self._states:
                    self._states[tid] = status
            self._running_count = sum(1 for s in self._states.values() if s == TaskStatus.RUNNING)
            self._read_running_count = sum(
                1 for tid, s in self._states.items()
                if s == TaskStatus.RUNNING and self._dag.tasks[tid].is_read_only
            )
            if loop_counts:
                self._loop_counts = loop_counts.copy()

    def mark_running(self, task_id: str) -> None:
        with self._lock:
            self._states[task_id] = TaskStatus.RUNNING
            self._running_count += 1
            task = self._dag.tasks.get(task_id)
            if task and task.is_read_only:
                self._read_running_count += 1

    def reset_running_to_pending(self) -> list[str]:
        """将所有 RUNNING 状态的任务重置为 PENDING，返回被重置的任务 ID 列表。

        用于状态不一致恢复：scheduler 认为有任务在运行，但实际没有对应的 future。
        """
        with self._lock:
            reset_ids = []
            for tid, status in self._states.items():
                if status == TaskStatus.RUNNING:
                    self._states[tid] = TaskStatus.PENDING
                    reset_ids.append(tid)
            self._running_count = 0
            self._read_running_count = 0
            return reset_ids

    def mark_completed(self, task_id: str, status: TaskStatus, outputs: dict[str, Any] | None = None) -> None:
        """Mark a task as completed. If task has loop config and condition not met, reset to PENDING."""
        with self._lock:
            task = self._dag.tasks.get(task_id)
            was_read_only = task.is_read_only if task else False

            self._states[task_id] = status
            self._running_count = max(0, self._running_count - 1)
            if was_read_only:
                self._read_running_count = max(0, self._read_running_count - 1)

            # Release concurrency group slot
            if task and task.concurrency_group and self._concurrency_manager:
                self._concurrency_manager.release(task.concurrency_group)

            # 检查是否需要循环
            if status == TaskStatus.SUCCESS:
                if task and task.loop:
                    # 增加循环计数
                    current_count = self._loop_counts.get(task_id, 0) + 1
                    self._loop_counts[task_id] = current_count

                    # 检查是否达到最大迭代次数
                    if current_count >= task.loop.max_iterations:
                        logger.info(
                            "Task '%s' reached max iterations (%d), stopping loop",
                            task_id, task.loop.max_iterations
                        )
                        return

                    # 评估循环终止条件
                    if task.loop.until_condition:
                        try:
                            condition_met = self._evaluate_condition_expr(
                                task.loop.until_condition,
                                outputs or {}
                            )
                            if condition_met:
                                logger.info(
                                    "Task '%s' loop condition met after %d iterations, stopping loop",
                                    task_id, current_count
                                )
                                return
                        except Exception as e:
                            logger.warning(
                                "Failed to evaluate loop condition for task '%s': %s. Stopping loop.",
                                task_id, e
                            )
                            return

                    # 条件未满足且未达最大次数，重置为 PENDING 继续循环
                    logger.info(
                        "Task '%s' loop iteration %d/%d, resetting to PENDING",
                        task_id, current_count, task.loop.max_iterations
                    )
                    self._states[task_id] = TaskStatus.PENDING

    def get_task_status(self, task_id: str) -> TaskStatus:
        """获取任务的当前状态。"""
        with self._lock:
            return self._states.get(task_id, TaskStatus.PENDING)

    def get_ready_tasks(self, outputs: dict[str, Any] | None = None) -> list[TaskNode]:
        """Return tasks that are ready to execute, sorted by priority (highest first)."""
        with self._lock:
            # 背压检查：防止过多任务同时排队
            if self._running_count >= self._queue_capacity:
                logger.warning(
                    "Backpressure triggered: running count (%d) >= queue capacity (%d), no new tasks will be scheduled",
                    self._running_count, self._queue_capacity
                )
                return []

            # 快速检查：读池和写池是否都已满
            current_write_running = self._running_count - self._read_running_count
            if (self._read_running_count >= self._read_pool_size
                    and current_write_running >= self._max_write_parallel):
                return []

            # 检查是否有 sequential 任务正在运行，如果有则阻塞所有其他任务
            for tid, node in self._dag.tasks.items():
                if self._states[tid] == TaskStatus.RUNNING and node.is_sequential:
                    logger.debug("Sequential task '%s' is running, blocking all other tasks", tid)
                    return []

            # 第一阶段：收集所有满足依赖和条件的候选任务（不检查并发组）
            candidates = []
            for tid, node in self._dag.tasks.items():
                if self._states[tid] != TaskStatus.PENDING:
                    continue

                # Check all dependencies are SUCCESS
                deps_met = all(
                    self._states.get(dep) == TaskStatus.SUCCESS
                    for dep in node.depends_on
                )
                if not deps_met:
                    continue

                # Evaluate condition if present
                if node.condition:
                    if not self._evaluate_condition(node, outputs or {}):
                        self._states[tid] = TaskStatus.SKIPPED
                        logger.info("Task '%s' skipped: condition evaluated to False", tid)
                        continue

                candidates.append(node)

            # 第二阶段：按优先级降序排序（高优先级在前）
            candidates.sort(key=lambda t: t.priority, reverse=True)

            # 第2.5阶段：冗余检测（仅记录日志，不跳过任务）
            if self._redundancy_detector and candidates:
                candidate_dict = {node.id: node for node in candidates}
                redundancy_groups = self._redundancy_detector.detect(candidate_dict)
                if redundancy_groups:
                    for group in redundancy_groups:
                        logger.info(
                            "Redundancy info (similarity=%.2f): tasks %s are similar (non-blocking)",
                            group.similarity_score,
                            group.task_ids
                        )

            # 第三阶段：处理 sequential 任务的独占逻辑
            if self._running_count > 0:
                # 有任务正在运行，过滤掉所有 sequential 任务
                filtered = [t for t in candidates if not t.is_sequential]
                if len(filtered) < len(candidates):
                    logger.debug(
                        "Filtered out %d sequential task(s) because %d task(s) are running",
                        len(candidates) - len(filtered), self._running_count
                    )
                candidates = filtered
            else:
                # 没有任务运行，但如果有多个 sequential 任务，只保留第一个（最高优先级）
                sequential_tasks = [t for t in candidates if t.is_sequential]
                if len(sequential_tasks) > 1:
                    # 保留第一个 sequential 任务，移除其他 sequential 任务
                    first_seq = sequential_tasks[0]
                    candidates = [t for t in candidates if not t.is_sequential or t.id == first_seq.id]
                    logger.debug(
                        "Multiple sequential tasks found, keeping only highest priority: '%s'",
                        first_seq.id
                    )

            # 第四阶段：读写分区调度，两个池完全独立互不影响
            read_only_candidates = [c for c in candidates if c.is_read_only]
            write_candidates = [c for c in candidates if not c.is_read_only]

            # 独立计算读池和写池的可用槽位
            read_available = max(0, self._read_pool_size - self._read_running_count)
            write_available = max(0, self._max_write_parallel - current_write_running)

            selected_write = write_candidates[:write_available]
            selected_read = read_only_candidates[:read_available]

            # 日志记录调度决策
            for node in selected_write:
                logger.info(
                    "Scheduling write task %s (write_pool: %d/%d)",
                    node.id, current_write_running, self._max_write_parallel,
                )
            for node in selected_read:
                logger.info(
                    "Scheduling read-only task %s (read_pool: %d/%d)",
                    node.id, self._read_running_count, self._read_pool_size,
                )

            candidates = selected_write + selected_read
            candidates.sort(key=lambda t: t.priority, reverse=True)

            # 第五阶段：检查并发组，只对最终选中的任务 acquire（避免槽位泄漏）
            ready = []
            acquired_groups = []  # 记录已 acquire 的并发组，用于失败时回滚
            for node in candidates:
                if node.concurrency_group and self._concurrency_manager:
                    if not self._concurrency_manager.acquire(node.concurrency_group):
                        logger.debug(
                            "Task '%s' blocked: no available slot in concurrency group '%s",
                            node.id, node.concurrency_group
                        )
                        # 释放前面已 acquire 的槽位
                        for group in acquired_groups:
                            self._concurrency_manager.release(group)
                        return []  # 返回空列表，避免部分任务获得槽位而部分任务没有
                    acquired_groups.append(node.concurrency_group)
                ready.append(node)

            return ready

    def _evaluate_condition(self, node: TaskNode, outputs: dict[str, Any]) -> bool:
        """Evaluate a task's condition expression."""
        return self._evaluate_condition_expr(node.condition or "", outputs)

    def _evaluate_condition_expr(self, condition: str, outputs: dict[str, Any]) -> bool:
        """Evaluate a condition expression string."""
        try:
            # Provide outputs as local variables for the condition
            local_vars = {"outputs": outputs}
            for tid, out in outputs.items():
                local_vars[tid] = out
            return bool(eval(condition, {"__builtins__": {}}, local_vars))  # noqa: S307
        except Exception as e:
            raise TaskConditionError(
                f"Condition evaluation failed: {condition!r} -> {e}"
            ) from e

    def propagate_failure(self, failed_task_id: str) -> list[str]:
        """Cancel all downstream dependents of a failed task. Returns cancelled task IDs."""
        with self._lock:
            cancelled = []
            queue = [failed_task_id]
            visited = {failed_task_id}

            while queue:
                current = queue.pop(0)
                for tid, node in self._dag.tasks.items():
                    if tid in visited:
                        continue
                    if current in node.depends_on and self._states[tid] in (TaskStatus.PENDING, TaskStatus.WAITING):
                        self._states[tid] = TaskStatus.CANCELLED
                        cancelled.append(tid)
                        visited.add(tid)
                        queue.append(tid)

            return cancelled

    def has_work_remaining(self) -> bool:
        """Check if there's any work left (pending or running tasks)."""
        with self._lock:
            return any(
                s in (TaskStatus.PENDING, TaskStatus.RUNNING)
                for s in self._states.values()
            )

    def all_done(self) -> bool:
        """Check if all tasks have reached a terminal state."""
        with self._lock:
            terminal = {TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.SKIPPED, TaskStatus.CANCELLED}
            return all(s in terminal for s in self._states.values())

    def get_loop_counts(self) -> dict[str, int]:
        """获取所有任务的循环计数。"""
        with self._lock:
            return dict(self._loop_counts)

    def restore_loop_counts(self, counts: dict[str, int]) -> None:
        """恢复循环计数（用于断点续传）。"""
        with self._lock:
            self._loop_counts = counts.copy()

    def summary(self) -> dict[str, int]:
        """Return a count of tasks in each status."""
        with self._lock:
            counts: dict[str, int] = {}
            for s in self._states.values():
                counts[s.value] = counts.get(s.value, 0) + 1
            return counts

    def topological_order(self) -> list[str]:
        """Return task IDs in topological order."""
        in_degree: dict[str, int] = {tid: 0 for tid in self._dag.tasks}
        for node in self._dag.tasks.values():
            for dep in node.depends_on:
                if dep in in_degree:
                    in_degree[node.id] = in_degree[node.id] + 1

        queue = [tid for tid, deg in in_degree.items() if deg == 0]
        order = []

        while queue:
            queue.sort()  # deterministic ordering
            tid = queue.pop(0)
            order.append(tid)
            for node in self._dag.tasks.values():
                if tid in node.depends_on:
                    in_degree[node.id] -= 1
                    if in_degree[node.id] == 0:
                        queue.append(node.id)

        return order
