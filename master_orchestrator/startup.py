"""启动性能优化模块。

借鉴 Claude Code 的并行预取策略，将互不依赖的初始化步骤并行化。
提供 checkpoint 记录每个阶段的耗时，帮助定位启动瓶颈。
"""
from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable

logger = logging.getLogger(__name__)


@dataclass
class StartupCheckpoint:
    """启动阶段计时点。"""
    name: str
    start_time: float
    duration_ms: float = 0.0


@dataclass
class StartupProfile:
    """完整的启动性能画像。"""
    checkpoints: list[StartupCheckpoint] = field(default_factory=list)
    total_ms: float = 0.0

    def record(self, name: str, start_time: float) -> None:
        duration = (time.monotonic() - start_time) * 1000
        self.checkpoints.append(StartupCheckpoint(name=name, start_time=start_time, duration_ms=duration))
        logger.debug("启动阶段 '%s': %.1fms", name, duration)

    def summary(self) -> dict:
        return {
            "total_ms": self.total_ms,
            "stages": {cp.name: cp.duration_ms for cp in self.checkpoints},
        }


def parallel_init(
    tasks: dict[str, Callable[[], Any]],
    max_workers: int = 4,
) -> dict[str, Any]:
    """并行执行互不依赖的初始化任务。

    Args:
        tasks: {名称: 可调用对象} 字典
        max_workers: 最大并行线程数

    Returns:
        {名称: 返回值} 字典
    """
    results: dict[str, Any] = {}
    profile = StartupProfile()

    with ThreadPoolExecutor(max_workers=min(len(tasks), max_workers)) as pool:
        futures = {pool.submit(fn): name for name, fn in tasks.items()}
        for future in as_completed(futures):
            name = futures[future]
            start = time.monotonic()
            try:
                results[name] = future.result()
                profile.record(name, start)
            except Exception as e:
                logger.error("启动任务 '%s' 失败: %s", name, e)
                results[name] = None
                profile.record(name, start)

    profile.total_ms = sum(cp.duration_ms for cp in profile.checkpoints)
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("启动性能: %s", profile.summary())

    return results
