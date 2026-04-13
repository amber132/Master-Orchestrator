"""执行策略选择器：根据目标文本判断使用并行模式还是精确迭代模式。"""

from __future__ import annotations

import re
import logging

logger = logging.getLogger(__name__)

# 精确迭代模式的关键词
_SURGICAL_PREFIXES = {
    "fix", "repair", "resolve", "debug", "patch", "correct",
    "修复", "解决", "调试", "修补", "纠正",
}

_SURGICAL_KEYWORDS = [
    "test failure", "error in", "bug in", "regression",
    "failing test", "broken", "crash", "exception",
    "测试失败", "报错", "崩溃", "异常", "回归",
    "改进", "improve", "refactor", "优化", "optimize",
    "加固", "harden", "健壮", "robust",
]


def classify_execution_strategy(goal: str, *, explicit_mode: str = "") -> str:
    """根据目标文本自动判断执行策略。

    Args:
        goal: 目标描述文本。
        explicit_mode: 用户显式指定的模式（"surgical" | "auto" | ""）。

    Returns:
        "surgical" 或 "dag"
    """
    if explicit_mode in ("surgical", "surgical_mode"):
        logger.info("[Strategy] 显式指定精确迭代模式")
        return "surgical"

    text = goal.strip().lower()
    if not text:
        return "dag"

    # 检查第一个词是否是修复类动词
    first_word = re.split(r"[\s\n,，。；;：:！!？?]+", text, maxsplit=1)[0] if text else ""
    if first_word in _SURGICAL_PREFIXES:
        logger.info("[Strategy] 检测到修复类动词 '%s'，使用精确迭代模式", first_word)
        return "surgical"

    # 检查关键词
    for kw in _SURGICAL_KEYWORDS:
        if kw in text:
            logger.info("[Strategy] 检测到关键词 '%s'，使用精确迭代模式", kw)
            return "surgical"

    return "dag"
