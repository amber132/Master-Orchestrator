"""
任务指标收集模块

提供任务执行指标的记录、查询和统计功能。
"""

import json
import threading
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, Literal


@dataclass
class TaskMetrics:
    """任务执行指标"""
    task_id: str
    start_time: str  # ISO 8601 格式
    end_time: Optional[str] = None  # ISO 8601 格式
    duration_ms: Optional[float] = None
    retry_count: int = 0
    cli_duration_ms: Optional[float] = None
    token_input: Optional[int] = None
    token_output: Optional[int] = None
    status: Literal["success", "failed", "timeout"] = "success"

    def to_dict(self) -> dict:
        """转换为字典"""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "TaskMetrics":
        """从字典创建"""
        return cls(**data)


class MetricsCollector:
    """指标收集器 - 线程安全的 JSONL 存储"""

    def __init__(self, metrics_file: str = "metrics.jsonl"):
        """
        初始化指标收集器
        
        Args:
            metrics_file: 指标文件路径（JSONL 格式）
        """
        self.metrics_file = Path(metrics_file)
        self._lock = threading.Lock()
        self._ensure_file_exists()

    def _ensure_file_exists(self):
        """确保指标文件存在"""
        if not self.metrics_file.exists():
            self.metrics_file.parent.mkdir(parents=True, exist_ok=True)
            self.metrics_file.touch()

    def record(self, metric: TaskMetrics) -> None:
        """
        记录任务指标（线程安全）
        
        Args:
            metric: 任务指标对象
        """
        with self._lock:
            with open(self.metrics_file, "a", encoding="utf-8") as f:
                json.dump(metric.to_dict(), f, ensure_ascii=False)
                f.write("\n")

    def query(self, task_id: str) -> list[TaskMetrics]:
        """
        查询指定任务的所有指标记录
        
        Args:
            task_id: 任务 ID
            
        Returns:
            匹配的指标列表（按时间顺序）
        """
        results = []
        if not self.metrics_file.exists():
            return results

        with self._lock:
            with open(self.metrics_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        if data.get("task_id") == task_id:
                            results.append(TaskMetrics.from_dict(data))
                    except (json.JSONDecodeError, TypeError):
                        continue

        return results

    def summary(self) -> dict:
        """
        生成指标统计摘要
        
        Returns:
            包含以下字段的字典：
            - total_tasks: 总任务数
            - success_count: 成功任务数
            - failed_count: 失败任务数
            - timeout_count: 超时任务数
            - avg_duration_ms: 平均执行时长（毫秒）
            - total_retries: 总重试次数
            - total_tokens_input: 总输入 token 数
            - total_tokens_output: 总输出 token 数
        """
        if not self.metrics_file.exists():
            return self._empty_summary()

        total_tasks = 0
        success_count = 0
        failed_count = 0
        timeout_count = 0
        total_duration_ms = 0.0
        duration_count = 0
        total_retries = 0
        total_tokens_input = 0
        total_tokens_output = 0

        with self._lock:
            with open(self.metrics_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        total_tasks += 1

                        status = data.get("status", "success")
                        if status == "success":
                            success_count += 1
                        elif status == "failed":
                            failed_count += 1
                        elif status == "timeout":
                            timeout_count += 1

                        if data.get("duration_ms") is not None:
                            total_duration_ms += data["duration_ms"]
                            duration_count += 1

                        total_retries += data.get("retry_count", 0)

                        if data.get("token_input") is not None:
                            total_tokens_input += data["token_input"]
                        if data.get("token_output") is not None:
                            total_tokens_output += data["token_output"]

                    except (json.JSONDecodeError, TypeError, KeyError):
                        continue

        avg_duration_ms = total_duration_ms / duration_count if duration_count > 0 else 0.0

        return {
            "total_tasks": total_tasks,
            "success_count": success_count,
            "failed_count": failed_count,
            "timeout_count": timeout_count,
            "avg_duration_ms": round(avg_duration_ms, 2),
            "total_retries": total_retries,
            "total_tokens_input": total_tokens_input,
            "total_tokens_output": total_tokens_output,
        }

    def _empty_summary(self) -> dict:
        """返回空摘要"""
        return {
            "total_tasks": 0,
            "success_count": 0,
            "failed_count": 0,
            "timeout_count": 0,
            "avg_duration_ms": 0.0,
            "total_retries": 0,
            "total_tokens_input": 0,
            "total_tokens_output": 0,
        }

    def clear(self) -> None:
        """清空所有指标记录"""
        with self._lock:
            if self.metrics_file.exists():
                self.metrics_file.unlink()
            self._ensure_file_exists()
