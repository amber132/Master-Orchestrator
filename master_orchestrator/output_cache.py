"""
输出缓存模块

提供线程安全的 LLM 输出缓存，避免重复执行相同的 prompt。
"""

import hashlib
import threading
import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class CacheEntry:
    """缓存条目"""
    output: str
    cost_usd: float
    created_at: float
    ttl: float  # 生存时间（秒）

    def is_expired(self) -> bool:
        """检查是否过期"""
        return time.time() > self.created_at + self.ttl


@dataclass
class CacheStats:
    """缓存统计信息"""
    hits: int
    misses: int
    saved_cost_usd: float


class OutputCache:
    """
    线程安全的输出缓存

    使用 prompt 指纹作为 key，缓存 LLM 输出结果。
    支持 TTL 过期、统计信息追踪。
    """

    def __init__(self):
        self._cache: dict[str, CacheEntry] = {}
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0
        self._saved_cost_usd = 0.0

    def get(self, prompt_hash: str) -> Optional[CacheEntry]:
        """
        获取缓存条目

        Args:
            prompt_hash: prompt 指纹

        Returns:
            CacheEntry 或 None（未命中或已过期）
        """
        with self._lock:
            entry = self._cache.get(prompt_hash)
            if entry is None:
                self._misses += 1
                return None

            if entry.is_expired():
                # 过期条目自动删除
                del self._cache[prompt_hash]
                self._misses += 1
                return None

            # 命中
            self._hits += 1
            self._saved_cost_usd += entry.cost_usd
            return entry

    def put(
        self,
        prompt_hash: str,
        output: str,
        cost_usd: float,
        ttl_seconds: float = 3600
    ) -> None:
        """
        存入缓存条目

        Args:
            prompt_hash: prompt 指纹
            output: LLM 输出
            cost_usd: 本次调用成本
            ttl_seconds: 生存时间（秒），默认 1 小时
        """
        with self._lock:
            entry = CacheEntry(
                output=output,
                cost_usd=cost_usd,
                created_at=time.time(),
                ttl=ttl_seconds
            )
            self._cache[prompt_hash] = entry

    def compute_hash(self, prompt: str, model: str, working_dir: str) -> str:
        """
        计算 prompt 指纹

        Args:
            prompt: prompt 文本
            model: 模型名称
            working_dir: 工作目录

        Returns:
            SHA256 哈希字符串（十六进制）
        """
        # 组合所有影响输出的因素
        content = f"{prompt}|{model}|{working_dir}"
        return hashlib.sha256(content.encode("utf-8")).hexdigest()

    def stats(self) -> CacheStats:
        """
        获取缓存统计信息

        Returns:
            CacheStats 对象
        """
        with self._lock:
            return CacheStats(
                hits=self._hits,
                misses=self._misses,
                saved_cost_usd=self._saved_cost_usd
            )

    def evict_expired(self) -> int:
        """
        清理过期条目

        Returns:
            清理的条目数量
        """
        with self._lock:
            expired_keys = [
                key for key, entry in self._cache.items()
                if entry.is_expired()
            ]
            for key in expired_keys:
                del self._cache[key]
            return len(expired_keys)

    def clear(self) -> None:
        """清空所有缓存"""
        with self._lock:
            self._cache.clear()

    def size(self) -> int:
        """获取当前缓存条目数"""
        with self._lock:
            return len(self._cache)
