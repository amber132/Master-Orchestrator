"""Prompt 共享前缀提取，用于优化 Anthropic API 的 Prompt 缓存命中率。

借鉴 Claude Code 的 CacheSafeParams 模式：同一 DAG 中多个任务如果共享
相同的 system prompt 前缀，可以让 API 端复用已缓存的 token，减少成本。
"""
from __future__ import annotations


def extract_shared_prefix(
    prompts: list[str],
    min_length: int = 20,
    separator: str = "\n",  # 统一使用 \n，因为 prompt 模板内部用 \n 分隔
) -> str:
    """从多个 prompt 中提取最长共享前缀。

    Args:
        prompts: 待比较的 prompt 列表
        min_length: 最短有效前缀长度（低于此值视为无共享）
        separator: 优先在分隔符处截断，保持语义完整

    Returns:
        共享前缀字符串，无共享时返回空字符串
    """
    if not prompts:
        return ""
    if len(prompts) == 1:
        return prompts[0]

    # 二分法找最长共享前缀
    shortest = min(len(p) for p in prompts)
    if shortest < min_length:
        return ""

    lo, hi = 0, shortest
    while lo < hi:
        mid = (lo + hi + 1) // 2
        prefix = prompts[0][:mid]
        if all(p.startswith(prefix) for p in prompts[1:]):
            lo = mid
        else:
            hi = mid - 1

    result = prompts[0][:lo]

    # 在最后一个分隔符处截断，保持语义完整
    if separator and len(result) > min_length:
        last_sep = result.rfind(separator)
        if last_sep >= min_length:
            result = result[:last_sep + len(separator)]

    return result if len(result) >= min_length else ""
