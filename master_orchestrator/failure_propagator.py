"""任务失败时的级联传播逻辑，从 orchestrator.py 提取为独立模块。"""

from __future__ import annotations

import logging
from collections import OrderedDict
from typing import Any

from .model import TaskResult, TaskStatus
from .scheduler import Scheduler
from .store import Store

logger = logging.getLogger("claude_orchestrator")


class FailurePropagator:
    """封装任务失败时的级联传播逻辑，消除 _handle_result 中的重复代码。"""

    def __init__(
        self,
        scheduler: Scheduler,
        store: Store,
        outputs: OrderedDict[str, Any],
        results: OrderedDict[str, TaskResult],
        lru_set_fn,  # Orchestrator._lru_set 静态方法引用
        get_run_info,  # callable，返回当前 RunInfo（lambda: self._run_info）
        lru_max_results: int,
    ):
        self._scheduler = scheduler
        self._store = store
        self._outputs = outputs
        self._results = results
        self._lru_set = lru_set_fn
        self._get_run_info = get_run_info
        self._lru_max_results = lru_max_results

    def propagate(
        self,
        task_id: str,
        *,
        cancel_reason_template: str,
        log_description: str,
    ) -> list[str]:
        """执行失败传播：标记失败 → 传播取消 → 批量生成取消结果 → 更新 store。

        Args:
            task_id: 失败的任务 ID。
            cancel_reason_template: 取消原因模板，用 {task_id} 占位，
                例如 "Cancelled due to upstream failure of '{task_id}'"。
            log_description: 日志描述片段，插入 "Cancelled N downstream tasks due to 'X' <desc>: ..."。

        Returns:
            被取消的下游任务 ID 列表（可能为空）。
        """
        self._scheduler.mark_completed(task_id, TaskStatus.FAILED, self._outputs)
        cancelled = self._scheduler.propagate_failure(task_id)
        if cancelled:
            logger.warning(
                "Cancelled %d downstream tasks due to '%s' %s: %s",
                len(cancelled), task_id, log_description, ", ".join(cancelled),
            )
            for tid in cancelled:
                cancel_result = TaskResult.from_exception(
                    tid,
                    Exception(cancel_reason_template.format(task_id=task_id)),
                )
                self._lru_set(self._results, tid, cancel_result, max_size=self._lru_max_results)
                self._store.update_task(self._get_run_info().run_id, cancel_result)
        return cancelled or []
