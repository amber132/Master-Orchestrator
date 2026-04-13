"""Rate limiter using token bucket algorithm for API call throttling.

使用令牌桶算法实现线程安全的速率限制，支持全局限制和 per_model 限制。
支持全局 429 退避：当任何任务收到 429 时，所有并行任务共享退避状态。
"""

from __future__ import annotations

import logging
import threading
import time

from .config import RateLimitConfig


class RateLimiter:
    """令牌桶算法实现的速率限制器。

    维护两层限制：
    1. 全局桶：限制所有请求的总速率
    2. per_model 桶：每个模型的独立限制

    线程安全，使用 threading.Condition 实现阻塞等待。
    """

    def __init__(self, config: RateLimitConfig):
        """初始化速率限制器。

        Args:
            config: 速率限制配置
        """
        self.config = config
        self.condition = threading.Condition()

        # 全局桶
        self.global_tokens = float(config.burst_size)
        self.global_capacity = float(config.burst_size)
        self.global_rate = config.requests_per_minute / 60.0  # 每秒补充速率
        self.global_last_refill = time.time()

        # per_model 桶
        self.model_tokens: dict[str, float] = {}
        self.model_capacity: dict[str, float] = {}
        self.model_rate: dict[str, float] = {}
        self.model_last_refill: dict[str, float] = {}

        for model, limit in config.per_model_limits.items():
            self.model_tokens[model] = float(config.burst_size)
            self.model_capacity[model] = float(config.burst_size)
            self.model_rate[model] = limit / 60.0  # 每秒补充速率
            self.model_last_refill[model] = time.time()

        # 全局 429 退避状态
        self._backoff_until: float = 0.0  # 退避截止时间戳
        self._backoff_consecutive: int = 0  # 连续 429 次数
        self._backoff_base: float = 30.0  # 基础退避秒数
        self._backoff_max: float = 300.0  # 最大退避秒数

        # 自适应限流
        self._original_rate: float = self.global_rate  # 保存原始速率
        self._adaptive_factor: float = 1.0  # 当前速率因子（0.1 ~ 1.0）
        self._adaptive_min_factor: float = 0.1  # 最低降到原始速率的 10%
        self._adaptive_recovery_step: float = 0.1  # 每次恢复步长
        self._adaptive_decay: float = 0.5  # 每次 429 衰减因子
        self._last_success_time: float = time.time()  # 上次成功时间
        self._success_streak: int = 0  # 连续成功次数
        self._recovery_threshold: int = 10  # 连续成功多少次后开始恢复

    def _refill_tokens(self) -> None:
        """补充令牌（需在锁内调用）。

        根据时间流逝按速率补充令牌，不超过桶容量。
        """
        now = time.time()

        # 补充全局桶
        elapsed = now - self.global_last_refill
        self.global_tokens = min(
            self.global_capacity,
            self.global_tokens + elapsed * self.global_rate
        )
        self.global_last_refill = now

        # 补充 per_model 桶
        for model in self.model_tokens:
            elapsed = now - self.model_last_refill[model]
            self.model_tokens[model] = min(
                self.model_capacity[model],
                self.model_tokens[model] + elapsed * self.model_rate[model]
            )
            self.model_last_refill[model] = now

    def _try_consume(self, model: str) -> bool:
        """尝试消耗令牌（需在锁内调用）。

        Args:
            model: 模型名称，空字符串表示不检查 per_model 限制

        Returns:
            是否成功消耗令牌
        """
        self._refill_tokens()

        # 检查全局桶
        if self.global_tokens < 1.0:
            return False

        # 检查 per_model 桶（如果配置了）
        if model and model in self.model_tokens:
            if self.model_tokens[model] < 1.0:
                return False

        # 消耗令牌
        self.global_tokens -= 1.0
        if model and model in self.model_tokens:
            self.model_tokens[model] -= 1.0

        return True

    def _calculate_wait_time(self, model: str) -> float:
        """计算需要等待的时间（需在锁内调用）。

        Args:
            model: 模型名称

        Returns:
            需要等待的秒数
        """
        # 计算全局桶需要等待的时间
        global_wait = 0.0
        if self.global_tokens < 1.0:
            tokens_needed = 1.0 - self.global_tokens
            global_wait = tokens_needed / self.global_rate if self.global_rate > 0 else 0.0

        # 计算 per_model 桶需要等待的时间
        model_wait = 0.0
        if model and model in self.model_tokens:
            if self.model_tokens[model] < 1.0:
                tokens_needed = 1.0 - self.model_tokens[model]
                model_wait = tokens_needed / self.model_rate[model] if self.model_rate[model] > 0 else 0.0

        # 返回较大的等待时间，至少等待 0.1 秒
        return max(global_wait, model_wait, 0.1)

    def acquire(self, model: str = '') -> None:
        """阻塞等待直到获得令牌。

        如果处于全局 429 退避期间，先等待退避结束再获取令牌。

        Args:
            model: 模型名称，空字符串表示不检查 per_model 限制
        """
        with self.condition:
            # 先等待全局 429 退避结束
            while True:
                now = time.time()
                if now >= self._backoff_until:
                    break
                wait_secs = self._backoff_until - now
                logging.getLogger("claude_orchestrator").info(
                    "全局 429 退避中，等待 %.1f 秒 (连续 %d 次)",
                    wait_secs, self._backoff_consecutive,
                )
                self.condition.wait(timeout=wait_secs)

            # 正常令牌桶逻辑
            while not self._try_consume(model):
                wait_time = self._calculate_wait_time(model)
                self.condition.wait(timeout=wait_time)

    def try_acquire(self, model: str = '') -> bool:
        """非阻塞尝试获取令牌。

        Args:
            model: 模型名称，空字符串表示不检查 per_model 限制

        Returns:
            是否成功获取令牌
        """
        with self.condition:
            return self._try_consume(model)

    def get_state(self) -> dict:
        """获取当前速率限制器状态（用于断点续传）。

        Returns:
            包含令牌数、时间戳和自适应限流状态的字典
        """
        with self.condition:
            return {
                'global_tokens': self.global_tokens,
                'global_last_refill': self.global_last_refill,
                'model_tokens': dict(self.model_tokens),
                'model_last_refill': dict(self.model_last_refill),
                'backoff_until': self._backoff_until,
                'backoff_consecutive': self._backoff_consecutive,
                # 自适应限流状态
                'adaptive_factor': self._adaptive_factor,
                'success_streak': self._success_streak,
                'last_success_time': self._last_success_time,
            }

    def restore_state(self, state: dict) -> None:
        """恢复速率限制器状态（用于断点续传）。

        Args:
            state: 从 get_state() 获取的状态字典
        """
        with self.condition:
            # 恢复全局桶状态
            if 'global_tokens' in state:
                self.global_tokens = float(state['global_tokens'])
            if 'global_last_refill' in state:
                self.global_last_refill = float(state['global_last_refill'])

            # 恢复 per_model 桶状态
            if 'model_tokens' in state:
                for model, tokens in state['model_tokens'].items():
                    if model in self.model_tokens:
                        self.model_tokens[model] = float(tokens)
            if 'model_last_refill' in state:
                for model, timestamp in state['model_last_refill'].items():
                    if model in self.model_last_refill:
                        self.model_last_refill[model] = float(timestamp)

            # 恢复 429 退避状态
            if 'backoff_until' in state:
                saved_until = float(state['backoff_until'])
                if saved_until > time.time():
                    self._backoff_until = saved_until
                else:
                    self._backoff_until = 0.0
            if 'backoff_consecutive' in state:
                self._backoff_consecutive = int(state['backoff_consecutive'])

            # 恢复自适应限流状态
            if 'adaptive_factor' in state:
                self._adaptive_factor = max(
                    self._adaptive_min_factor,
                    min(1.0, float(state['adaptive_factor'])),
                )
                self.global_rate = self._original_rate * self._adaptive_factor
            if 'success_streak' in state:
                self._success_streak = int(state['success_streak'])
            if 'last_success_time' in state:
                self._last_success_time = float(state['last_success_time'])

    def report_429(self) -> None:
        """报告收到 429 限流响应，触发全局退避 + 自适应降速。

        所有并行任务共享退避状态。退避时间指数增长：
        30s → 60s → 120s → 240s → 300s（上限）
        同时按衰减因子降低全局速率，减少后续请求压力。
        """
        log = logging.getLogger("claude_orchestrator")
        with self.condition:
            self._backoff_consecutive += 1
            delay = min(
                self._backoff_base * (2 ** (self._backoff_consecutive - 1)),
                self._backoff_max,
            )
            new_until = time.time() + delay
            # 只延长，不缩短（避免并发 report 互相覆盖）
            if new_until > self._backoff_until:
                self._backoff_until = new_until

            # 自适应降速：衰减速率因子，重置连续成功计数
            self._adaptive_factor = max(
                self._adaptive_min_factor,
                self._adaptive_factor * self._adaptive_decay,
            )
            self.global_rate = self._original_rate * self._adaptive_factor
            self._success_streak = 0

            effective_rpm = self.global_rate * 60.0
            log.warning(
                "全局 429 退避触发: 第 %d 次，退避 %.0f 秒 | "
                "自适应因子 %.2f，实际 RPM %.1f",
                self._backoff_consecutive, delay,
                self._adaptive_factor, effective_rpm,
            )
            # 唤醒所有等待线程，让它们重新检查退避状态
            self.condition.notify_all()

    def report_success(self) -> None:
        """报告请求成功，重置连续 429 计数器，并尝试自适应恢复速率。

        连续成功次数达到 _recovery_threshold 后，每次成功逐步回升速率因子，
        直到恢复到原始速率（factor = 1.0）。
        """
        log = logging.getLogger("claude_orchestrator")
        with self.condition:
            if self._backoff_consecutive > 0:
                self._backoff_consecutive = 0
                log.info("429 退避计数器已重置（请求成功）")

            self._last_success_time = time.time()
            self._success_streak += 1

            # 连续成功达到阈值后，逐步恢复速率
            if (
                self._success_streak >= self._recovery_threshold
                and self._adaptive_factor < 1.0
            ):
                old_factor = self._adaptive_factor
                self._adaptive_factor = min(
                    1.0,
                    self._adaptive_factor + self._adaptive_recovery_step,
                )
                self.global_rate = self._original_rate * self._adaptive_factor
                effective_rpm = self.global_rate * 60.0
                log.info(
                    "自适应恢复: 因子 %.2f → %.2f，实际 RPM %.1f "
                    "(连续成功 %d 次)",
                    old_factor, self._adaptive_factor, effective_rpm,
                    self._success_streak,
                )
                # 恢复一步后重置连续成功计数，需要再积累才能继续恢复
                self._success_streak = 0

    def get_adaptive_stats(self) -> dict:
        """获取当前自适应限流状态，用于监控和 dashboard。

        Returns:
            包含自适应限流各项指标的字典
        """
        with self.condition:
            return {
                'adaptive_factor': self._adaptive_factor,
                'original_rpm': self._original_rate * 60.0,
                'effective_rpm': self.global_rate * 60.0,
                'success_streak': self._success_streak,
                'recovery_threshold': self._recovery_threshold,
                'backoff_consecutive': self._backoff_consecutive,
                'last_success_time': self._last_success_time,
                'is_degraded': self._adaptive_factor < 1.0,
                'degradation_pct': round((1.0 - self._adaptive_factor) * 100, 1),
            }
