"""Checkpoint management for phase-level state persistence."""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from .exceptions import CheckpointError

_logger = logging.getLogger(__name__)


@dataclass
class CheckpointData:
    """检查点数据容器。
    
    Attributes:
        run_id: 运行 ID
        phase: 阶段名称（如 "analyze", "implement", "review"）
        task_states: 任务状态字典 {task_id: status}
        metadata: 额外元数据（如进度、时间戳等）
        created_at: 检查点创建时间
    """
    run_id: str
    phase: str
    task_states: dict[str, str]
    metadata: dict[str, any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)


class CheckpointManager:
    """阶段级检查点管理器，支持崩溃恢复和断点续传。
    
    与 Store 的原子写入模式兼容，使用独立的 checkpoints 表存储阶段快照。
    """

    def __init__(self, db_path: str | Path = "./orchestrator_state.db"):
        """初始化检查点管理器。
        
        Args:
            db_path: SQLite 数据库路径（与 Store 共享同一数据库）
        """
        self._db_path = str(db_path)
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False, timeout=30)
        
        # 启用 WAL 模式（与 Store 保持一致）
        wal_result = self._conn.execute("PRAGMA journal_mode=WAL").fetchone()
        if wal_result and wal_result[0].lower() != "wal":
            _logger.warning(
                "SQLite WAL 模式启用失败 (当前: %s)，可能影响并发性能", wal_result[0]
            )
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._lock = threading.Lock()
        
        # 初始化 checkpoints 表
        self._init_schema()

    def _init_schema(self) -> None:
        """初始化检查点表结构。"""
        try:
            with self._lock:
                self._conn.execute("""
                    CREATE TABLE IF NOT EXISTS checkpoints (
                        run_id       TEXT NOT NULL,
                        phase        TEXT NOT NULL,
                        task_states  TEXT NOT NULL,
                        metadata     TEXT NOT NULL DEFAULT '{}',
                        created_at   TEXT NOT NULL,
                        PRIMARY KEY (run_id, phase)
                    )
                """)
                self._conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_checkpoints_run_id 
                    ON checkpoints(run_id)
                """)
                self._conn.commit()
        except sqlite3.Error as e:
            raise CheckpointError(f"初始化检查点表失败: {e}") from e

    def save_checkpoint(
        self,
        run_id: str,
        phase: str,
        task_states: dict[str, str],
        metadata: dict[str, any] | None = None
    ) -> None:
        """保存阶段检查点（原子写入）。
        
        Args:
            run_id: 运行 ID
            phase: 阶段名称
            task_states: 任务状态字典 {task_id: status}
            metadata: 额外元数据（可选）
            
        Raises:
            CheckpointError: 序列化失败或数据库写入失败
        """
        if not run_id or not phase:
            raise CheckpointError("run_id 和 phase 不能为空")
        
        try:
            task_states_json = json.dumps(task_states)
            metadata_json = json.dumps(metadata or {})
        except (TypeError, ValueError) as e:
            raise CheckpointError(f"检查点数据序列化失败: {e}") from e
        
        try:
            with self._lock:
                self._conn.execute(
                    "INSERT OR REPLACE INTO checkpoints "
                    "(run_id, phase, task_states, metadata, created_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (run_id, phase, task_states_json, metadata_json, datetime.now().isoformat())
                )
                self._conn.commit()
        except sqlite3.OperationalError as e:
            err_msg = str(e).lower()
            if "disk" in err_msg or "full" in err_msg or "no space" in err_msg:
                raise CheckpointError(
                    f"磁盘空间不足，保存检查点失败: {e}. 请清理磁盘空间后重试。"
                ) from e
            if "database is locked" in err_msg:
                raise CheckpointError(
                    f"数据库锁定，保存检查点失败: {e}. 可能有其他进程正在访问数据库。"
                ) from e
            raise CheckpointError(f"保存检查点失败: {e}") from e
        except sqlite3.Error as e:
            raise CheckpointError(f"保存检查点失败: {e}") from e
        
        _logger.info("检查点已保存: run_id=%s, phase=%s, tasks=%d", run_id, phase, len(task_states))

    def restore_checkpoint(self, run_id: str, phase: str | None = None) -> CheckpointData | None:
        """恢复检查点数据。
        
        Args:
            run_id: 运行 ID
            phase: 阶段名称（可选）。如果指定则恢复该阶段，否则恢复最新阶段
            
        Returns:
            CheckpointData 对象，不存在则返回 None
            
        Raises:
            CheckpointError: JSON 反序列化失败
        """
        try:
            with self._lock:
                if phase:
                    row = self._conn.execute(
                        "SELECT run_id, phase, task_states, metadata, created_at "
                        "FROM checkpoints WHERE run_id = ? AND phase = ?",
                        (run_id, phase)
                    ).fetchone()
                else:
                    row = self._conn.execute(
                        "SELECT run_id, phase, task_states, metadata, created_at "
                        "FROM checkpoints WHERE run_id = ? ORDER BY created_at DESC LIMIT 1",
                        (run_id,)
                    ).fetchone()
        except sqlite3.Error as e:
            raise CheckpointError(f"查询检查点失败: {e}") from e
        
        if not row:
            return None
        
        try:
            task_states = json.loads(row[2])
            metadata = json.loads(row[3])
        except json.JSONDecodeError as e:
            raise CheckpointError(
                f"检查点数据损坏 (run_id={row[0]}, phase={row[1]}): {e}"
            ) from e
        
        checkpoint = CheckpointData(
            run_id=row[0],
            phase=row[1],
            task_states=task_states,
            metadata=metadata,
            created_at=datetime.fromisoformat(row[4])
        )
        
        _logger.info("检查点已恢复: run_id=%s, phase=%s, tasks=%d", 
                     checkpoint.run_id, checkpoint.phase, len(checkpoint.task_states))
        return checkpoint

    def list_checkpoints(self, run_id: str) -> list[tuple[str, datetime]]:
        """列出某个运行的所有检查点。
        
        Args:
            run_id: 运行 ID
            
        Returns:
            [(phase, created_at), ...] 列表，按创建时间倒序排列
        """
        try:
            with self._lock:
                rows = self._conn.execute(
                    "SELECT phase, created_at FROM checkpoints "
                    "WHERE run_id = ? ORDER BY created_at DESC",
                    (run_id,)
                ).fetchall()
        except sqlite3.Error as e:
            raise CheckpointError(f"列出检查点失败: {e}") from e
        
        return [(row[0], datetime.fromisoformat(row[1])) for row in rows]

    def delete_checkpoint(self, run_id: str, phase: str | None = None) -> int:
        """删除检查点。
        
        Args:
            run_id: 运行 ID
            phase: 阶段名称（可选）。如果指定则删除该阶段，否则删除该运行的所有检查点
            
        Returns:
            删除的检查点数量
        """
        try:
            with self._lock:
                if phase:
                    cur = self._conn.execute(
                        "DELETE FROM checkpoints WHERE run_id = ? AND phase = ?",
                        (run_id, phase)
                    )
                else:
                    cur = self._conn.execute(
                        "DELETE FROM checkpoints WHERE run_id = ?",
                        (run_id,)
                    )
                self._conn.commit()
                return cur.rowcount
        except sqlite3.Error as e:
            raise CheckpointError(f"删除检查点失败: {e}") from e

    def close(self) -> None:
        """关闭数据库连接。"""
        try:
            self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception:
            pass
        self._conn.close()

    def __enter__(self) -> CheckpointManager:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.close()
        return False
