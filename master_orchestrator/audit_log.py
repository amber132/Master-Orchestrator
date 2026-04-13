"""审计日志模块，记录 prompt 和执行结果的审计信息。"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

_logger = logging.getLogger(__name__)


class AuditLogger:
    """审计日志记录器，写入 JSONL 格式的审计文件。
    
    线程安全，支持并发写入。对 prompt 内容只记录 SHA256 摘要和前200字截断。
    """
    
    def __init__(self, log_dir: Path | str = "."):
        """初始化审计日志记录器。
        
        Args:
            log_dir: 日志文件目录，默认当前目录
        """
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.log_file = self.log_dir / "audit.jsonl"
        self._lock = threading.Lock()
        
    def log_prompt(
        self,
        task_id: str,
        prompt: str,
        source_type: str = "unknown",
        **extra: Any
    ) -> None:
        """记录 prompt 审计信息。
        
        Args:
            task_id: 任务 ID
            prompt: prompt 完整内容（仅用于计算 hash 和截断，不记录完整内容）
            source_type: prompt 来源类型（如 "user", "template", "auto_generated"）
            **extra: 额外的审计字段
        """
        # 计算 SHA256 摘要
        prompt_hash = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
        
        # 截取前 200 字符
        prompt_preview = prompt[:200]
        if len(prompt) > 200:
            prompt_preview += "..."
        
        entry = {
            "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "event_type": "prompt",
            "task_id": task_id,
            "prompt_hash": prompt_hash,
            "prompt_length": len(prompt),
            "prompt_preview": prompt_preview,
            "source_type": source_type,
            **extra
        }
        
        self._write_entry(entry)
        
    def log_result(
        self,
        task_id: str,
        success: bool,
        error_category: str | None = None,
        **extra: Any
    ) -> None:
        """记录任务执行结果。
        
        Args:
            task_id: 任务 ID
            success: 是否执行成功
            error_category: 错误分类（失败时提供，如 "timeout", "api_error", "validation_error"）
            **extra: 额外的审计字段
        """
        entry = {
            "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "event_type": "result",
            "task_id": task_id,
            "success": success,
            "error_category": error_category,
            **extra
        }
        
        self._write_entry(entry)
        
    def _write_entry(self, entry: dict[str, Any]) -> None:
        """原子写入审计条目到 JSONL 文件。
        
        Args:
            entry: 审计条目字典
        """
        with self._lock:
            try:
                # 追加模式写入 JSONL
                with open(self.log_file, "a", encoding="utf-8") as f:
                    json.dump(entry, f, ensure_ascii=False)
                    f.write("\n")
            except Exception as e:
                _logger.error(f"Failed to write audit log: {e}", exc_info=True)
                
    def query_by_task(self, task_id: str) -> list[dict[str, Any]]:
        """查询指定任务的所有审计记录。
        
        Args:
            task_id: 任务 ID
            
        Returns:
            审计记录列表（按时间顺序）
        """
        if not self.log_file.exists():
            return []
            
        results = []
        try:
            with open(self.log_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                        if entry.get("task_id") == task_id:
                            results.append(entry)
                    except json.JSONDecodeError:
                        _logger.warning(f"Skipping malformed audit log line: {line[:100]}")
                        continue
        except Exception as e:
            _logger.error(f"Failed to query audit log: {e}", exc_info=True)
            
        return results
        
    def query_by_time_range(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> list[dict[str, Any]]:
        """查询指定时间范围内的审计记录。
        
        Args:
            start_time: 起始时间（UTC）
            end_time: 结束时间（UTC）
            
        Returns:
            审计记录列表（按时间顺序）
        """
        if not self.log_file.exists():
            return []
            
        start_iso = start_time.isoformat() + "Z"
        end_iso = end_time.isoformat() + "Z"
        
        results = []
        try:
            with open(self.log_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                        timestamp = entry.get("timestamp", "")
                        if start_iso <= timestamp <= end_iso:
                            results.append(entry)
                    except json.JSONDecodeError:
                        _logger.warning(f"Skipping malformed audit log line: {line[:100]}")
                        continue
        except Exception as e:
            _logger.error(f"Failed to query audit log: {e}", exc_info=True)
            
        return results
