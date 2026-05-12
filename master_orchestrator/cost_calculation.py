"""成本计算纯函数模块。

从 CLI result 事件中提取/估算 USD 成本。所有函数均为纯函数或近纯函数，
无跨模块依赖，可独立测试。
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


# 模型定价表：每百万 token 的美元价格 (input, output, cache_read, cache_creation)
_MODEL_PRICING_PER_MILLION: dict[str, tuple[float, float, float, float]] = {
    "claude-opus-4": (15.0, 75.0, 1.5, 18.75),
    "claude-sonnet-4": (3.0, 15.0, 0.3, 3.75),
    "claude-3-5-sonnet": (3.0, 15.0, 0.3, 3.75),
    "claude-3-5-haiku": (0.80, 4.0, 0.08, 1.0),
    "claude-3-opus": (15.0, 75.0, 1.5, 18.75),
    "claude-3-sonnet": (3.0, 15.0, 0.3, 3.75),
    "claude-3-haiku": (0.25, 1.25, 0.025, 0.3125),
}

# 未知模型默认定价（Sonnet 级别）
_DEFAULT_PRICING = (3.0, 15.0, 0.3, 3.75)


def _get_model_pricing(model_name: str) -> tuple[float, float, float, float]:
    """根据模型名获取定价 (input, output, cache_read, cache_creation 每百万 token)。"""
    if not model_name:
        return _DEFAULT_PRICING
    name_lower = model_name.lower()
    for key, pricing in _MODEL_PRICING_PER_MILLION.items():
        if key in name_lower:
            return pricing
    return _DEFAULT_PRICING


def _estimate_cost_from_tokens(event: dict) -> float:
    """当 CLI 不报告成本时，从 token 使用量估算成本。

    支持两种字段命名风格:
    - snake_case: input_tokens, output_tokens, cache_read_input_tokens, cache_creation_input_tokens
    - camelCase: inputTokens, outputTokens, cacheReadInputTokens, cacheCreationInputTokens
      （Claude CLI stream-json 格式使用的命名）

    同时从 modelUsage 中汇总所有模型的 token 计数（CLI 2025+ 版本的主要 token 数据来源）。
    """
    usage = event.get("usage", {})
    if not isinstance(usage, dict):
        usage = {}

    # 从 usage 对象或顶层提取 token 计数，同时兼容 snake_case 和 camelCase
    input_tokens = (
        event.get("input_tokens", 0)
        or event.get("inputTokens", 0)
        or usage.get("input_tokens", 0)
        or usage.get("inputTokens", 0)
        or event.get("token_count_input", 0)
        or 0
    )
    output_tokens = (
        event.get("output_tokens", 0)
        or event.get("outputTokens", 0)
        or usage.get("output_tokens", 0)
        or usage.get("outputTokens", 0)
        or event.get("token_count_output", 0)
        or 0
    )
    cache_read = (
        usage.get("cache_read_input_tokens", 0)
        or usage.get("cacheReadInputTokens", 0)
        or event.get("cache_read_input_tokens", 0)
        or event.get("cacheReadInputTokens", 0)
        or 0
    )
    cache_creation = (
        usage.get("cache_creation_input_tokens", 0)
        or usage.get("cacheCreationInputTokens", 0)
        or event.get("cache_creation_input_tokens", 0)
        or event.get("cacheCreationInputTokens", 0)
        or 0
    )

    # 从 modelUsage 汇总所有模型的 token 计数（CLI 2025+ 版本）
    # 如果前面的提取都没找到 token 数据，尝试从 modelUsage 中提取
    if not (input_tokens or output_tokens or cache_read or cache_creation):
        model_usage = event.get("modelUsage", {})
        if isinstance(model_usage, dict) and model_usage:
            for _model_key, model_data in model_usage.items():
                if not isinstance(model_data, dict):
                    continue
                input_tokens += (
                    model_data.get("input_tokens", 0)
                    or model_data.get("inputTokens", 0)
                    or 0
                )
                output_tokens += (
                    model_data.get("output_tokens", 0)
                    or model_data.get("outputTokens", 0)
                    or 0
                )
                cache_read += (
                    model_data.get("cache_read_input_tokens", 0)
                    or model_data.get("cacheReadInputTokens", 0)
                    or 0
                )
                cache_creation += (
                    model_data.get("cache_creation_input_tokens", 0)
                    or model_data.get("cacheCreationInputTokens", 0)
                    or 0
                )

    if not (input_tokens or output_tokens or cache_read or cache_creation):
        return 0.0
    # 推断模型名
    model_name = event.get("model", "")
    if not model_name:
        model_usage = event.get("modelUsage", {})
        if isinstance(model_usage, dict) and model_usage:
            model_name = next(iter(model_usage), "")
    inp_price, out_price, cache_read_price, cache_creation_price = _get_model_pricing(model_name)
    cost = (
        input_tokens * inp_price / 1_000_000
        + output_tokens * out_price / 1_000_000
        + cache_read * cache_read_price / 1_000_000
        + cache_creation * cache_creation_price / 1_000_000
    )
    if cost > 0:
        logger.info(
            "Token-based cost estimation: model=%s, tokens=%d/%d (cache: %d read, %d created) -> $%.6f",
            model_name or "unknown", input_tokens, output_tokens, cache_read, cache_creation, cost,
        )
    return cost


def _recursive_find_cost(obj: object, max_depth: int = 4, _depth: int = 0) -> float:
    """递归扫描所有嵌套 key，查找含 cost/usd 的数值字段。

    匹配规则：key 名（小写）包含 'cost' 或以 '_usd' 结尾 或等于 'usd'。
    跳过已知的非成本字段（如 session_id 等含 cost 字样的非数值字段）。
    遇到第一个有效值即返回（不累加，避免重复计算）。
    """
    if _depth > max_depth or not isinstance(obj, dict):
        return 0.0
    # 已知的非成本 key 前缀，避免误匹配
    _skip_prefixes = ("session_", "request_", "response_")
    cost_indicators = ("cost",)
    usd_indicators = ("_usd", "usd")
    # camelCase 变体：CostUSD, totalCost, costUsd 等
    camel_cost_indicators = ("Cost",)
    for key, val in obj.items():
        if not isinstance(key, str):
            continue
        kl = key.lower()
        # 检查是否是成本相关字段（支持 snake_case 和 camelCase）
        is_cost_key = (
            any(ci in kl for ci in cost_indicators)
            or any(kl.endswith(ui) for ui in usd_indicators)
            or kl == "usd"
            # camelCase 匹配：key 中包含 Cost（大写 C），如 totalCost、costUSD
            or any(ci in key for ci in camel_cost_indicators)
        )
        if is_cost_key and not any(kl.startswith(sp) for sp in _skip_prefixes):
            # 尝试转为浮点数
            if isinstance(val, (int, float)) and val > 0:
                logger.debug("_recursive_find_cost 命中 key='%s' val=%s depth=%d", key, val, _depth)
                return float(val)
            if isinstance(val, str):
                try:
                    fv = float(val)
                    if fv > 0:
                        logger.debug("_recursive_find_cost 命中 key='%s' val='%s' depth=%d", key, val, _depth)
                        return fv
                except (ValueError, TypeError):
                    pass
        # 递归深入嵌套 dict
        if isinstance(val, dict):
            found = _recursive_find_cost(val, max_depth, _depth + 1)
            if found > 0:
                return found
    return 0.0


def _try_float(val: object) -> float:
    """安全地将值转为 float，跳过 dict/list 等非数值类型。

    Claude CLI 有时会在 cost_usd 字段返回 dict（如嵌套对象），
    裸 float() 会抛 TypeError，此函数安全地返回 0.0。
    """
    if isinstance(val, (dict, list, tuple, set)):
        return 0.0
    try:
        return float(val)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0


def _extract_cost_usd(event: dict) -> float:
    """从 CLI result 事件中提取成本。

    提取策略（按优先级）:
    - 策略 1-9: 从 CLI 输出的显式成本字段提取
    - 策略 10: 从 token 使用量 + 模型定价估算成本（最终 fallback）

    所有 float() 转换均通过 _try_float() 安全处理，
    防止 cost_usd 字段为 dict 时 TypeError 崩溃。
    """
    # 调试：记录 event 的顶层 key 列表和嵌套结构，用于诊断 cost 字段名
    top_keys = sorted(event.keys())
    nested_info = {}
    for key in ("usage", "modelUsage", "cost", "costs"):
        val = event.get(key)
        if isinstance(val, dict):
            nested_info[key] = sorted(val.keys())
        elif val is not None:
            nested_info[key] = type(val).__name__
    logger.debug(
        "_extract_cost_usd 诊断: top_keys=%s, nested=%s",
        top_keys, nested_info,
    )
    cost = 0.0
    # 1. 顶层 total_cost_usd（最可靠，明确含单位后缀）
    val = event.get("total_cost_usd", 0.0)
    if val and _try_float(val) > 0:
        cost = _try_float(val)
    # 2. 顶层 cost_usd（标准字段）
    elif _try_float(event.get("cost_usd", 0.0)) > 0:
        cost = _try_float(event.get("cost_usd", 0.0))
    # 3. 顶层 total_cost（无 _usd 后缀变体）
    elif _try_float(event.get("total_cost", 0.0)) > 0:
        cost = _try_float(event.get("total_cost", 0.0))
    # 4. 顶层 costUSD（camelCase 变体）
    elif _try_float(event.get("costUSD", 0.0)) > 0:
        cost = _try_float(event.get("costUSD", 0.0))
    # 5. 拆分成本：input_cost_usd + output_cost_usd
    elif _try_float(event.get("input_cost_usd", 0.0)) > 0 or _try_float(event.get("output_cost_usd", 0.0)) > 0:
        cost = _try_float(event.get("input_cost_usd", 0.0)) + _try_float(event.get("output_cost_usd", 0.0))
    # 6. 拆分成本：input_cost + output_cost（无 _usd 后缀）
    elif _try_float(event.get("input_cost", 0.0)) > 0 or _try_float(event.get("output_cost", 0.0)) > 0:
        cost = _try_float(event.get("input_cost", 0.0)) + _try_float(event.get("output_cost", 0.0))
    # 7. 顶层 cost（最短字段名，最后兜底）
    elif _try_float(event.get("cost", 0.0)) > 0:
        cost = _try_float(event.get("cost", 0.0))
    else:
        # 8. 嵌套在 usage 对象中
        usage = event.get("usage", {})
        if isinstance(usage, dict):
            usage_cost = (
                _try_float(usage.get("cost_usd", 0.0))
                or _try_float(usage.get("costUSD", 0.0))
                or _try_float(usage.get("cost", 0.0))
            )
            if usage_cost:
                cost = usage_cost
        # 9. 从 modelUsage 提取（汇总所有模型的 cost，同时支持 camelCase 和 snake_case）
        if cost == 0.0:
            model_usage = event.get("modelUsage", {})
            if model_usage:
                total = sum(
                    _try_float(
                        m.get("costUSD", 0.0)
                        or m.get("cost_usd", 0.0)
                        or m.get("totalCostUSD", 0.0)
                        or m.get("total_cost_usd", 0.0)
                        or m.get("cost", 0.0)
                    )
                    for m in model_usage.values()
                    if isinstance(m, dict)
                )
                if total > 0:
                    cost = total
        # 9.5 递归兜底：遍历 event 所有嵌套 key 查找含 cost/usd 的字段
        # 防止 CLI 格式变更导致新字段名无法被上述策略匹配
        if cost == 0.0:
            cost = _recursive_find_cost(event, max_depth=4)
        # 9.7 从 modelUsage 的 token 计数估算成本（CLI 2025+ 版本可能只有 token 没有直接 cost 字段）
        if cost == 0.0:
            model_usage = event.get("modelUsage", {})
            if isinstance(model_usage, dict) and model_usage:
                est_total = 0.0
                for model_key, model_data in model_usage.items():
                    if not isinstance(model_data, dict):
                        continue
                    inp = model_data.get("inputTokens", 0) or model_data.get("input_tokens", 0) or 0
                    out = model_data.get("outputTokens", 0) or model_data.get("output_tokens", 0) or 0
                    cr = model_data.get("cacheReadInputTokens", 0) or model_data.get("cache_read_input_tokens", 0) or 0
                    cc = model_data.get("cacheCreationInputTokens", 0) or model_data.get("cache_creation_input_tokens", 0) or 0
                    if inp or out or cr or cc:
                        inp_p, out_p, cr_p, cc_p = _get_model_pricing(model_key)
                        est = inp * inp_p / 1_000_000 + out * out_p / 1_000_000 + cr * cr_p / 1_000_000 + cc * cc_p / 1_000_000
                        est_total += est
                        logger.debug(
                            "modelUsage token 估算: model=%s, tokens=%d/%d/%d/%d -> $%.6f",
                            model_key, inp, out, cr, cc, est,
                        )
                if est_total > 0:
                    cost = est_total
                    logger.info("从 modelUsage token 计数估算总成本: $%.6f", cost)
    # 10. 最终 fallback：从 token 使用量估算成本（已增强 camelCase 支持）
    if cost == 0.0:
        cost = _estimate_cost_from_tokens(event)
    logger.debug(f'extracted cost={cost} from event keys={list(event.keys())}')
    return cost
