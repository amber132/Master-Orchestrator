"""
ContextStore - 基于 SQLite 的 key-value 存储

用于在 DAG 执行过程中存储和检索上下文数据。
支持按 run_id 隔离的键值对存储，value 自动 JSON 序列化。
"""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class ContextStore:
    """基于 SQLite 的上下文存储器

    表结构:
        context_data(
            run_id TEXT,
            key TEXT,
            value TEXT,  -- JSON 序列化后的值
            updated_at TEXT,
            PRIMARY KEY(run_id, key)
        )
    """

    def __init__(self, db_path: str | Path):
        """初始化上下文存储

        Args:
            db_path: SQLite 数据库文件路径
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row  # 支持字典式访问
        self._init_db()

    def _init_db(self):
        """创建表结构（如果不存在）"""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS context_data (
                run_id TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (run_id, key)
            )
        """)
        # 创建索引以加速按 run_id 查询
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_run_id
            ON context_data(run_id)
        """)
        self.conn.commit()

    def set(self, run_id: str, key: str, value: Any) -> None:
        """设置键值对（如果已存在则更新）

        Args:
            run_id: 运行 ID
            key: 键名
            value: 值（将被 JSON 序列化）
        """
        serialized_value = json.dumps(value, ensure_ascii=False)
        updated_at = datetime.now(timezone.utc).isoformat()

        self.conn.execute("""
            INSERT OR REPLACE INTO context_data (run_id, key, value, updated_at)
            VALUES (?, ?, ?, ?)
        """, (run_id, key, serialized_value, updated_at))
        self.conn.commit()

    def get(self, run_id: str, key: str) -> Any | None:
        """获取键值

        Args:
            run_id: 运行 ID
            key: 键名

        Returns:
            反序列化后的值，如果不存在则返回 None
        """
        cursor = self.conn.execute("""
            SELECT value FROM context_data
            WHERE run_id = ? AND key = ?
        """, (run_id, key))

        row = cursor.fetchone()
        if row is None:
            return None

        return json.loads(row["value"])

    def get_all(self, run_id: str) -> dict[str, Any]:
        """获取某个 run_id 的所有键值对

        Args:
            run_id: 运行 ID

        Returns:
            字典，key 为键名，value 为反序列化后的值
        """
        cursor = self.conn.execute("""
            SELECT key, value FROM context_data
            WHERE run_id = ?
        """, (run_id,))

        result = {}
        for row in cursor:
            result[row["key"]] = json.loads(row["value"])

        return result

    def delete(self, run_id: str, key: str) -> None:
        """删除键值对

        Args:
            run_id: 运行 ID
            key: 键名
        """
        self.conn.execute("""
            DELETE FROM context_data
            WHERE run_id = ? AND key = ?
        """, (run_id, key))
        self.conn.commit()

    def delete_run(self, run_id: str) -> None:
        """删除某个 run_id 的所有数据

        Args:
            run_id: 运行 ID
        """
        self.conn.execute("""
            DELETE FROM context_data
            WHERE run_id = ?
        """, (run_id,))
        self.conn.commit()

    def close(self) -> None:
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出"""
        self.close()
        return False
