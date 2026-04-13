"""上下文压缩器 — 在 DAG 执行过程中定期裁剪累积输出，防止内存膨胀。"""
from __future__ import annotations

import json
import logging
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class CompactStrategy(Enum):
    """压缩策略。"""
    TRUNCATE = "truncate"      # 截断到阈值
    SUMMARIZE = "summarize"    # 使用 HierarchicalSummarizer 生成摘要


@dataclass
class CompactResult:
    """压缩结果。"""
    compacted_keys: list[str] = field(default_factory=list)
    total_before: int = 0       # 压缩前总字符数
    total_after: int = 0        # 压缩后总字符数
    strategy_used: CompactStrategy = CompactStrategy.TRUNCATE


class ContextCompactor:
    """累积输出压缩器。

    在 _execute_loop 的每轮迭代中检查 _outputs 总大小，
    超过阈值时自动压缩最早的任务输出（LRU 顺序）。
    """

    def __init__(
        self,
        max_total_chars: int = 500_000,
        strategy: CompactStrategy = CompactStrategy.TRUNCATE,
        max_single_chars: int = 20_000,
        compaction_interval: int = 5,  # 每 N 轮检查一次
    ):
        self._max_total = max_total_chars
        self._strategy = strategy
        self._max_single = max_single_chars
        self._interval = compaction_interval
        self._loop_count = 0
        self._summarizer = None  # 延迟导入，避免循环依赖

    def _get_summarizer(self):
        """延迟获取 HierarchicalSummarizer 实例。"""
        if self._summarizer is None:
            from .context_summarizer import HierarchicalSummarizer
            self._summarizer = HierarchicalSummarizer()
        return self._summarizer

    @staticmethod
    def _measure_outputs(outputs: OrderedDict[str, Any]) -> int:
        """计算所有输出的总字符数。"""
        total = 0
        for key, value in outputs.items():
            if isinstance(value, str):
                total += len(value)
            elif isinstance(value, (dict, list)):
                total += len(json.dumps(value, ensure_ascii=False))
            else:
                total += len(str(value))
        return total

    def should_compact(self, outputs: OrderedDict[str, Any]) -> bool:
        """检查是否需要压缩。"""
        self._loop_count += 1
        if self._loop_count % self._interval != 0:
            return False
        total = self._measure_outputs(outputs)
        return total > self._max_total

    def compact(self, outputs: OrderedDict[str, Any]) -> CompactResult:
        """执行压缩，返回压缩结果。

        策略：
        1. 对单个输出超过 max_single_chars 的先压缩
        2. 如果总量仍超标，从最早（LRU 首位）开始压缩
        3. 压缩后更新 outputs 字典中的值（原地修改）
        """
        total_before = self._measure_outputs(outputs)
        if total_before <= self._max_total:
            return CompactResult(
                total_before=total_before,
                total_after=total_before,
            )

        compacted_keys: list[str] = []

        # 第一轮：压缩单个过大的输出
        for key in list(outputs.keys()):
            value = outputs[key]
            if isinstance(value, str):
                text = value
            elif isinstance(value, (dict, list)):
                text = json.dumps(value, ensure_ascii=False)
            else:
                text = str(value)

            if len(text) > self._max_single:
                compressed = self._compress_text(text)
                # 如果原始值是 dict/list，尝试解析回
                if isinstance(value, (dict, list)):
                    try:
                        outputs[key] = json.loads(compressed)
                    except (json.JSONDecodeError, TypeError):
                        outputs[key] = compressed
                else:
                    outputs[key] = compressed
                compacted_keys.append(key)

        # 第二轮：如果仍超标，从最早开始压缩
        total = self._measure_outputs(outputs)
        if total > self._max_total:
            for key in list(outputs.keys()):
                if total <= self._max_total:
                    break
                value = outputs[key]
                if isinstance(value, str):
                    text = value
                elif isinstance(value, (dict, list)):
                    text = json.dumps(value, ensure_ascii=False)
                else:
                    text = str(value)

                if key not in compacted_keys and len(text) > self._max_single // 2:
                    compressed = self._compress_text(text)
                    if isinstance(value, (dict, list)):
                        try:
                            outputs[key] = json.loads(compressed)
                        except (json.JSONDecodeError, TypeError):
                            outputs[key] = compressed
                    else:
                        outputs[key] = compressed
                    compacted_keys.append(key)
                    total = self._measure_outputs(outputs)

        total_after = self._measure_outputs(outputs)
        logger.info(
            "上下文压缩完成: 压缩 %d 个输出, %d -> %d 字符 (策略=%s)",
            len(compacted_keys), total_before, total_after, self._strategy.value,
        )

        return CompactResult(
            compacted_keys=compacted_keys,
            total_before=total_before,
            total_after=total_after,
            strategy_used=self._strategy,
        )

    def _compress_text(self, text: str) -> str:
        """根据策略压缩单段文本。"""
        if self._strategy == CompactStrategy.SUMMARIZE:
            summarizer = self._get_summarizer()
            result = summarizer.summarize(text, max_chars=self._max_single)
            return result
        else:
            # truncate 策略：保留头尾各一半
            if len(text) <= self._max_single:
                return text
            half = self._max_single // 2
            return text[:half] + f"\n\n... [压缩: 移除 {len(text) - self._max_single} 字符] ...\n\n" + text[-half:]
