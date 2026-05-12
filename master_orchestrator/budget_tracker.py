"""线程安全的全局预算追踪器。

支持按模型分组的 token 使用统计，区分 cache_read 和 cache_creation。
从 claude_cli.py 提取，保持向后兼容。
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .exceptions import BudgetExhaustedError

logger = logging.getLogger(__name__)


@dataclass
class ModelTokenUsage:
    """单个模型的 token 使用统计"""
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_creation_tokens: int = 0
    cost_usd: float = 0.0
    request_count: int = 0


class BudgetTracker:
    """Thread-safe global budget tracker.

    不再拦截预算超限，仅记录花费。预算检查由编排器上层统一管理。
    支持按模型分组的 token 使用统计，区分 cache_read 和 cache_creation。
    """

    def __init__(
        self,
        max_budget_usd: float,
        persist_path: str | None = None,
        enforcement_mode: str = "accounting",
    ):
        self._max = max_budget_usd
        self._spent = 0.0
        self._lock = threading.Lock()
        self._model_usage: dict[str, ModelTokenUsage] = {}
        # 持久化：每次 record_usage 后原子写入 JSON 文件
        self._persist_path = Path(persist_path) if persist_path else None
        self._entries: list[dict] = []  # 每条记录的详情
        self._enforcement_mode = enforcement_mode
        if self._enforcement_mode not in {"accounting", "hard_limit"}:
            raise ValueError(
                f"Unsupported enforcement_mode {self._enforcement_mode!r}; "
                "expected 'accounting' or 'hard_limit'"
            )

    @property
    def spent(self) -> float:
        with self._lock:
            return self._spent

    @spent.setter
    def spent(self, value: float) -> None:
        with self._lock:
            self._spent = value

    @property
    def limit(self) -> float:
        return self._max

    @property
    def remaining(self) -> float:
        if self._enforcement_mode == "hard_limit" and self._max > 0:
            with self._lock:
                return max(0.0, self._max - self._spent)
        return float("inf")

    @property
    def model_usage(self) -> dict[str, ModelTokenUsage]:
        """按模型分组的 token 使用统计（返回副本）。"""
        with self._lock:
            return dict(self._model_usage)

    def check_and_add(self, cost: float) -> None:
        """记录花费；hard_limit 模式下超限会抛 BudgetExhaustedError。"""
        with self._lock:
            normalized_cost = max(0.0, cost)
            self._ensure_can_charge_unlocked(normalized_cost)
            prev = self._spent
            self._spent += normalized_cost
            logger.debug("BudgetTracker._spend: cost=%.6f, spent %.6f -> %.6f", normalized_cost, prev, self._spent)

    def can_afford(self, estimated_cost: float = 0.0) -> bool:
        """hard_limit 模式下返回是否仍能支付；accounting 模式下始终为 True。"""
        if self._enforcement_mode != "hard_limit" or self._max <= 0:
            return True
        with self._lock:
            return self._spent + max(0.0, estimated_cost) <= self._max

    def add_spent(self, cost: float) -> None:
        """原子地增加已花费金额（不检查预算上限）。"""
        with self._lock:
            prev = self._spent
            self._spent += cost
            logger.debug("BudgetTracker.add_spent: cost=%.6f, spent %.6f -> %.6f", cost, prev, self._spent)

    def add_cost(self, amount: float) -> None:
        """添加成本到已花费金额（用于断点续传恢复预算）。"""
        with self._lock:
            prev = self._spent
            self._spent += amount
            logger.debug("BudgetTracker.add_cost: amount=%.6f, spent %.6f -> %.6f", amount, prev, self._spent)

    def get_total_cost(self) -> float:
        """返回所有线程累加的总花费（线程安全）。"""
        with self._lock:
            return self._spent

    def get_cost_by_model(self) -> dict[str, float]:
        """返回按模型分组的成本映射（线程安全）。"""
        with self._lock:
            return {model: round(u.cost_usd, 6) for model, u in self._model_usage.items()}

    @property
    def total_cost(self) -> float:
        """总花费（属性访问方式，等价于 get_total_cost）。"""
        with self._lock:
            return self._spent

    @property
    def remaining_budget(self) -> float:
        """剩余预算。"""
        with self._lock:
            return max(0.0, self._max - self._spent)

    def record_usage(self, model: str, input_tokens: int = 0, output_tokens: int = 0,
                     cache_read_tokens: int = 0, cache_creation_tokens: int = 0,
                     cost_usd: float = 0.0) -> None:
        """记录模型级别的 token 使用（含 cache token 区分），并持久化到文件。"""
        with self._lock:
            self._ensure_can_charge_unlocked(max(0.0, cost_usd))
            if model not in self._model_usage:
                self._model_usage[model] = ModelTokenUsage()
            usage = self._model_usage[model]
            usage.input_tokens += input_tokens
            usage.output_tokens += output_tokens
            usage.cache_read_tokens += cache_read_tokens
            usage.cache_creation_tokens += cache_creation_tokens
            usage.cost_usd += cost_usd
            usage.request_count += 1
            prev_spent = self._spent
            self._spent += cost_usd
            logger.debug(
                "BudgetTracker.record_usage: model=%s, cost_usd=%.6f, "
                "spent %.6f -> %.6f, tokens in=%d/out=%d",
                model, cost_usd, prev_spent, self._spent,
                input_tokens, output_tokens,
            )
            logger.info(
                'record_usage: model=%s, cost_usd=%.6f, new_total_spent=%.4f',
                model, cost_usd, self._spent,
            )
            # 追加条目并持久化
            self._entries.append({
                "timestamp": datetime.now().isoformat(),
                "model": model,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cache_read_tokens": cache_read_tokens,
                "cache_creation_tokens": cache_creation_tokens,
                "cost_usd": round(cost_usd, 6),
            })
            self._persist_to_file_unlocked()

    def _ensure_can_charge_unlocked(self, delta_cost: float) -> None:
        if self._enforcement_mode != "hard_limit":
            return
        if self._max <= 0:
            return
        projected = self._spent + delta_cost
        if projected <= self._max:
            return
        raise BudgetExhaustedError(
            f"Budget limit exceeded: projected=${projected:.4f} > limit=${self._max:.4f}"
        )

    @property
    def total_request_count(self) -> int:
        """全局累计 CLI 请求次数（所有模型的 request_count 之和）。"""
        with self._lock:
            return sum(u.request_count for u in self._model_usage.values())

    def cache_hit_rate(self) -> float:
        """全局缓存命中率：cache_read / (input + cache_read)。"""
        with self._lock:
            total_input = sum(u.input_tokens for u in self._model_usage.values())
            total_cache = sum(u.cache_read_tokens for u in self._model_usage.values())
            denominator = total_input + total_cache
            return total_cache / denominator if denominator > 0 else 0.0

    def summary(self) -> dict:
        """完整的预算摘要，包含按模型分组的 token 详情。"""
        with self._lock:
            return {
                "total_spent_usd": self._spent,
                "max_budget_usd": self._max,
                "remaining_usd": max(0, self._max - self._spent),
                "cache_hit_rate": self._cache_hit_rate_unlocked(),
                "models": {
                    model: {
                        "input_tokens": u.input_tokens,
                        "output_tokens": u.output_tokens,
                        "cache_read_tokens": u.cache_read_tokens,
                        "cache_creation_tokens": u.cache_creation_tokens,
                        "cost_usd": round(u.cost_usd, 6),
                        "request_count": u.request_count,
                    }
                    for model, u in self._model_usage.items()
                }
            }

    def _cache_hit_rate_unlocked(self) -> float:
        """不加锁版本的缓存命中率，仅供内部 summary() 使用。"""
        total_input = sum(u.input_tokens for u in self._model_usage.values())
        total_cache = sum(u.cache_read_tokens for u in self._model_usage.values())
        denominator = total_input + total_cache
        return total_cache / denominator if denominator > 0 else 0.0

    def _persist_to_file_unlocked(self) -> None:
        """原子写入 budget_tracker.json（调用方已持有锁）。

        格式：{total_cost, last_update, entries[]}
        使用 tmp + rename 实现原子写入，防止写入中断导致文件损坏。
        """
        if self._persist_path is None:
            return
        try:
            data = {
                "total_cost": round(self._spent, 6),
                "last_update": datetime.now().isoformat(),
                "entries": self._entries,
            }
            # 确保父目录存在
            self._persist_path.parent.mkdir(parents=True, exist_ok=True)
            # 原子写入：先写临时文件，再 rename
            tmp_fd, tmp_path = tempfile.mkstemp(
                dir=str(self._persist_path.parent),
                suffix=".tmp",
            )
            try:
                with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                # Windows 下 rename 需要目标文件不存在（或用 os.replace 覆盖）
                os.replace(tmp_path, str(self._persist_path))
            except Exception:
                # 写入失败时清理临时文件
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
            logger.debug(
                "BudgetTracker 持久化: total_cost=%.4f, entries=%d -> %s",
                self._spent, len(self._entries), self._persist_path,
            )
        except Exception as e:
            # 持久化失败不应阻塞主流程，仅记录警告
            logger.warning("BudgetTracker 持久化失败: %s", e)
