"""结构化诊断日志 — 以 JSONL 格式记录编排器关键事件，方便事后分析。

借鉴 Claude Code 的结构化日志理念，将分散的 logger 调用转化为可查询的 JSON 记录。
"""
from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class DiagnosticEventType(Enum):
    """诊断事件类型。"""
    TASK_START = "task_start"
    TASK_COMPLETE = "task_complete"
    TASK_FAIL = "task_fail"
    TASK_RETRY = "task_retry"
    MODEL_SWITCH = "model_switch"
    BUDGET_CHECKPOINT = "budget_checkpoint"
    HEALTH_CHECK = "health_check"
    COMPACTION = "compaction"
    VALIDATION = "validation"
    RATE_LIMIT = "rate_limit"
    FAILOVER = "failover"


@dataclass
class DiagnosticEvent:
    """结构化诊断事件。"""
    timestamp: str = ""
    event_type: str = ""
    run_id: str = ""
    task_id: str = ""
    duration_ms: float = 0.0
    cost_usd: float = 0.0
    model: str = ""
    attempt: int = 0
    error: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now().isoformat()


class DiagnosticLogger:
    """结构化诊断日志记录器。

    以 JSONL 格式（每行一个 JSON 对象）写入诊断文件。
    线程安全（通过 lock 保护写入操作）。
    """

    def __init__(self, log_dir: str | Path = ".", filename: str = "diagnostics.jsonl"):
        self._log_path = Path(log_dir) / filename
        self._lock = threading.Lock()
        self._event_count = 0
        self._start_time = time.monotonic()
        # 确保目录存在
        self._log_path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def log_path(self) -> Path:
        return self._log_path

    @property
    def event_count(self) -> int:
        return self._event_count

    def record(self, event: DiagnosticEvent) -> None:
        """记录一条诊断事件。"""
        try:
            data = asdict(event)
            # 移除空值，减少日志体积
            data = {k: v for k, v in data.items() if v not in ("", 0, 0.0, None, [])}
            line = json.dumps(data, ensure_ascii=False, default=str)

            with self._lock:
                with open(self._log_path, "a", encoding="utf-8") as f:
                    f.write(line)
                    f.write("\n")
                self._event_count += 1
        except Exception as e:
            logger.debug("Failed to write diagnostic event: %s", e)

    def record_task_lifecycle(
        self,
        event_type: DiagnosticEventType,
        run_id: str,
        task_id: str,
        model: str = "",
        attempt: int = 0,
        duration_ms: float = 0.0,
        cost_usd: float = 0.0,
        error: str = "",
        **extra: Any,
    ) -> None:
        """便捷方法：记录任务生命周期事件。"""
        event = DiagnosticEvent(
            event_type=event_type.value,
            run_id=run_id,
            task_id=task_id,
            model=model,
            attempt=attempt,
            duration_ms=duration_ms,
            cost_usd=cost_usd,
            error=error,
            metadata=extra,
        )
        self.record(event)

    def summary(self) -> dict[str, Any]:
        """返回诊断日志摘要统计。"""
        elapsed = time.monotonic() - self._start_time
        return {
            "log_path": str(self._log_path),
            "event_count": self._event_count,
            "elapsed_seconds": round(elapsed, 2),
            "events_per_minute": round(self._event_count / max(elapsed, 1) * 60, 2),
        }

    def close(self) -> None:
        """关闭诊断日志（写入摘要）。"""
        if self._event_count > 0:
            summary_event = DiagnosticEvent(
                event_type="session_summary",
                metadata=self.summary(),
            )
            self.record(summary_event)
