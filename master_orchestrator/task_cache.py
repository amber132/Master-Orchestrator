"""任务缓存模块：基于 SQLite 的幂等任务结果缓存"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from .model import TaskResult, TaskStatus


class TaskCache:
    """
    任务结果缓存，用于幂等任务的结果复用。
    
    缓存键 = hash(task_id + prompt_hash + model)
    缓存值 = TaskResult JSON + 过期时间
    
    线程安全，支持 TTL 过期和手动失效。
    """
    
    def __init__(self, db_path: str | Path = "task_cache.db"):
        """
        初始化任务缓存。
        
        Args:
            db_path: SQLite 数据库路径，默认为当前目录下的 task_cache.db
        """
        self.db_path = Path(db_path)
        self._lock = threading.Lock()
        self._init_db()
    
    def _init_db(self) -> None:
        """初始化数据库表结构"""
        with self._lock:
            conn = sqlite3.connect(str(self.db_path))
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS task_cache (
                        cache_key TEXT PRIMARY KEY,
                        task_id TEXT NOT NULL,
                        prompt_hash TEXT NOT NULL,
                        model TEXT NOT NULL,
                        result_json TEXT NOT NULL,
                        created_at REAL NOT NULL,
                        expires_at REAL NOT NULL,
                        hit_count INTEGER DEFAULT 0
                    )
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_task_id 
                    ON task_cache(task_id)
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_expires_at 
                    ON task_cache(expires_at)
                """)
                conn.commit()
            finally:
                conn.close()
    
    def _compute_cache_key(self, task_id: str, prompt: str, model: str) -> str:
        """
        计算缓存键。
        
        Args:
            task_id: 任务 ID
            prompt: 任务 prompt（完整内容）
            model: 使用的模型名称
        
        Returns:
            缓存键（SHA256 哈希）
        """
        prompt_hash = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
        cache_key_raw = f"{task_id}:{prompt_hash}:{model}"
        return hashlib.sha256(cache_key_raw.encode("utf-8")).hexdigest()
    
    def get(self, task_id: str, prompt: str, model: str) -> TaskResult | None:
        """
        获取缓存的任务结果。
        
        Args:
            task_id: 任务 ID
            prompt: 任务 prompt
            model: 模型名称
        
        Returns:
            缓存的 TaskResult，如果未命中或已过期则返回 None
        """
        cache_key = self._compute_cache_key(task_id, prompt, model)
        
        with self._lock:
            conn = sqlite3.connect(str(self.db_path))
            try:
                cursor = conn.execute(
                    """
                    SELECT result_json, expires_at 
                    FROM task_cache 
                    WHERE cache_key = ?
                    """,
                    (cache_key,)
                )
                row = cursor.fetchone()
                
                if row is None:
                    return None
                
                result_json, expires_at = row
                
                if time.time() > expires_at:
                    conn.execute("DELETE FROM task_cache WHERE cache_key = ?", (cache_key,))
                    conn.commit()
                    return None
                
                conn.execute(
                    "UPDATE task_cache SET hit_count = hit_count + 1 WHERE cache_key = ?",
                    (cache_key,)
                )
                conn.commit()
                
                return self._deserialize_result(result_json)
            
            finally:
                conn.close()
    
    def put(self, task_id: str, prompt: str, model: str, result: TaskResult, ttl: int = 86400) -> None:
        """
        存储任务结果到缓存。
        
        Args:
            task_id: 任务 ID
            prompt: 任务 prompt
            model: 模型名称
            result: 任务结果
            ttl: 缓存有效期（秒），默认 86400 秒（24 小时）
        """
        cache_key = self._compute_cache_key(task_id, prompt, model)
        prompt_hash = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
        result_json = self._serialize_result(result)
        
        now = time.time()
        expires_at = now + ttl
        
        with self._lock:
            conn = sqlite3.connect(str(self.db_path))
            try:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO task_cache 
                    (cache_key, task_id, prompt_hash, model, result_json, created_at, expires_at, hit_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 0)
                    """,
                    (cache_key, task_id, prompt_hash, model, result_json, now, expires_at)
                )
                conn.commit()
            finally:
                conn.close()
    
    def invalidate(self, task_id: str, prompt: str | None = None, model: str | None = None) -> int:
        """
        失效缓存。
        
        Args:
            task_id: 任务 ID（必需）
            prompt: 任务 prompt（可选，如果提供则精确匹配）
            model: 模型名称（可选，如果提供则精确匹配）
        
        Returns:
            删除的缓存条目数量
        """
        with self._lock:
            conn = sqlite3.connect(str(self.db_path))
            try:
                if prompt is not None and model is not None:
                    cache_key = self._compute_cache_key(task_id, prompt, model)
                    cursor = conn.execute("DELETE FROM task_cache WHERE cache_key = ?", (cache_key,))
                else:
                    cursor = conn.execute("DELETE FROM task_cache WHERE task_id = ?", (task_id,))
                
                conn.commit()
                return cursor.rowcount
            finally:
                conn.close()
    
    def cleanup_expired(self) -> int:
        """
        清理所有过期的缓存条目。
        
        Returns:
            删除的条目数量
        """
        with self._lock:
            conn = sqlite3.connect(str(self.db_path))
            try:
                cursor = conn.execute("DELETE FROM task_cache WHERE expires_at < ?", (time.time(),))
                conn.commit()
                return cursor.rowcount
            finally:
                conn.close()
    
    def get_stats(self) -> dict[str, Any]:
        """
        获取缓存统计信息。
        
        Returns:
            包含总条目数、命中次数、平均 TTL 等信息的字典
        """
        with self._lock:
            conn = sqlite3.connect(str(self.db_path))
            try:
                cursor = conn.execute("""
                    SELECT 
                        COUNT(*) as total_entries,
                        SUM(hit_count) as total_hits,
                        AVG(expires_at - created_at) as avg_ttl,
                        COUNT(CASE WHEN expires_at < ? THEN 1 END) as expired_entries
                    FROM task_cache
                """, (time.time(),))
                
                row = cursor.fetchone()
                return {
                    "total_entries": row[0] or 0,
                    "total_hits": row[1] or 0,
                    "avg_ttl_seconds": row[2] or 0,
                    "expired_entries": row[3] or 0,
                }
            finally:
                conn.close()
    
    def _serialize_result(self, result: TaskResult) -> str:
        """序列化 TaskResult 为 JSON 字符串"""
        return json.dumps(result.to_dict())
    
    def _deserialize_result(self, result_json: str) -> TaskResult:
        """从 JSON 字符串反序列化 TaskResult"""
        data = json.loads(result_json)
        from datetime import datetime
        
        return TaskResult(
            task_id=data["task_id"],
            status=TaskStatus(data["status"]),
            output=data.get("output"),
            parsed_output=data.get("parsed_output"),
            error=data.get("error"),
            attempt=data.get("attempt", 1),
            started_at=datetime.fromisoformat(data["started_at"]) if data.get("started_at") else None,
            finished_at=datetime.fromisoformat(data["finished_at"]) if data.get("finished_at") else None,
            duration_seconds=data.get("duration_seconds", 0.0),
            cost_usd=data.get("cost_usd", 0.0),
            model_used=data.get("model_used", ""),
            pid=data.get("pid", 0),
            validation_passed=data.get("validation_passed"),
        )
