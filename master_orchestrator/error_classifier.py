"""错误分类器：通过关键词匹配和退出码分析错误类型，支持智能重试策略。"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from master_orchestrator.model import ErrorCategory, ErrorPolicy, FailoverReason, FailoverStatus

logger = logging.getLogger(__name__)

# 模块级缓存：error_hash -> ErrorCategory（使用 LRU 限制大小，防止长时间运行内存泄漏）
from collections import OrderedDict

_classification_cache: OrderedDict[str, ErrorCategory] = OrderedDict()
_CLASSIFICATION_CACHE_MAX_SIZE = 1024
_cache_lock = threading.Lock()


# ── RateLimitInfo 数据结构 ──

@dataclass
class RateLimitInfo:
    """Anthropic API 响应头中的速率限制信息。"""
    requests_limit: int = 0
    requests_remaining: int = 0
    requests_reset: datetime | None = None
    tokens_limit: int = 0
    tokens_remaining: int = 0
    tokens_reset: datetime | None = None
    retry_after_seconds: float | None = None

    @property
    def is_empty(self) -> bool:
        """是否为空信息（未解析到任何有效数据）。"""
        return (
            self.requests_limit == 0
            and self.requests_remaining == 0
            and self.requests_reset is None
            and self.tokens_limit == 0
            and self.tokens_remaining == 0
            and self.tokens_reset is None
            and self.retry_after_seconds is None
        )

    @property
    def requests_utilization(self) -> float:
        """请求配额使用率（0.0 ~ 1.0），无数据时返回 0.0。"""
        if self.requests_limit <= 0:
            return 0.0
        return 1.0 - (self.requests_remaining / self.requests_limit)

    @property
    def tokens_utilization(self) -> float:
        """Token 配额使用率（0.0 ~ 1.0），无数据时返回 0.0。"""
        if self.tokens_limit <= 0:
            return 0.0
        return 1.0 - (self.tokens_remaining / self.tokens_limit)

    def to_dict(self) -> dict[str, Any]:
        """序列化为字典，方便日志和 JSON 输出。"""
        return {
            "requests_limit": self.requests_limit,
            "requests_remaining": self.requests_remaining,
            "requests_reset": self.requests_reset.isoformat() if self.requests_reset else None,
            "tokens_limit": self.tokens_limit,
            "tokens_remaining": self.tokens_remaining,
            "tokens_reset": self.tokens_reset.isoformat() if self.tokens_reset else None,
            "retry_after_seconds": self.retry_after_seconds,
        }


def parse_rate_limit_headers(headers: dict) -> RateLimitInfo:
    """解析 Anthropic API 响应头中的速率限制信息。

    支持的响应头：
        - anthropic-ratelimit-requests-limit
        - anthropic-ratelimit-requests-remaining
        - anthropic-ratelimit-requests-reset
        - anthropic-ratelimit-tokens-limit
        - anthropic-ratelimit-tokens-remaining
        - anthropic-ratelimit-tokens-reset
        - retry-after（标准 HTTP 头）

    Args:
        headers: 响应头字典，key 不区分大小写

    Returns:
        RateLimitInfo: 解析后的限流信息，无效字段保留默认值
    """
    info = RateLimitInfo()

    if not headers:
        return info

    # 统一为小写 key 以便不区分大小写查找
    normalized = {k.lower(): v for k, v in headers.items()}

    def _safe_int(value: str | None) -> int:
        if value is None:
            return 0
        try:
            return int(value.strip())
        except (ValueError, TypeError):
            return 0

    def _parse_reset(value: str | None) -> datetime | None:
        """解析 reset 时间戳。Anthropic 返回的是 Unix epoch 秒数。"""
        if value is None:
            return None
        try:
            ts = float(value.strip())
            return datetime.fromtimestamp(ts)
        except (ValueError, TypeError, OSError):
            return None

    # 解析 requests 限流头
    info.requests_limit = _safe_int(normalized.get("anthropic-ratelimit-requests-limit"))
    info.requests_remaining = _safe_int(normalized.get("anthropic-ratelimit-requests-remaining"))
    info.requests_reset = _parse_reset(normalized.get("anthropic-ratelimit-requests-reset"))

    # 解析 tokens 限流头
    info.tokens_limit = _safe_int(normalized.get("anthropic-ratelimit-tokens-limit"))
    info.tokens_remaining = _safe_int(normalized.get("anthropic-ratelimit-tokens-remaining"))
    info.tokens_reset = _parse_reset(normalized.get("anthropic-ratelimit-tokens-reset"))

    # 解析 retry-after（标准 HTTP 头）
    retry_after = normalized.get("retry-after")
    if retry_after is not None:
        try:
            info.retry_after_seconds = float(retry_after.strip())
        except (ValueError, TypeError):
            # retry-after 也可能是 HTTP 日期格式，尝试解析
            from email.utils import parsedate_to_datetime
            try:
                dt = parsedate_to_datetime(retry_after.strip())
                info.retry_after_seconds = max(0.0, (dt - datetime.now(dt.tzinfo)).total_seconds())
            except Exception:
                pass

    return info


# ── PromptTooLongInfo 数据结构 ──

@dataclass
class PromptTooLongInfo:
    """prompt 超长错误的详细信息。"""
    max_tokens: int = 0
    actual_tokens: int = 0
    tokens_over_by: int = 0

    @property
    def has_details(self) -> bool:
        """是否提取到了有效的 token 差值信息。"""
        return self.max_tokens > 0 or self.actual_tokens > 0 or self.tokens_over_by > 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "max_tokens": self.max_tokens,
            "actual_tokens": self.actual_tokens,
            "tokens_over_by": self.tokens_over_by,
        }


# 正则提取 prompt 超长错误中的 token 数值
_PROMPT_TOO_LONG_RE = re.compile(
    r"prompt is too long.*?(?:"
    r"maximum\s+context\s+length\s+is\s+(\d+)|"
    r"maximum\s+(?:is|=|:)\s*(\d+)|"
    r"max(?:imum)?\s*(?:context|tokens?)\s*(?:is|=|:)\s*(\d+)|"
    r"max_tokens?\s*(?:is|=|:)\s*(\d+)|"
    r"limit\s*(?:is|=|:)\s*(\d+))",
    re.IGNORECASE,
)
_PROMPT_TOO_LONG_CURRENT_RE = re.compile(
    r"prompt is too long.*?(?:"
    r"you\s+passed\s+(\d+)|"
    r"input\s*(?:was|=|:)\s*(\d+)|"
    r"(?:got|received|sent|actual)\s*(?:is|=|:)?\s*(\d+)|"
    r"your\s+(?:prompt|message)\s+(?:has|contains)\s+(\d+))",
    re.IGNORECASE,
)


def extract_prompt_too_long_info(error_msg: str) -> PromptTooLongInfo:
    """从错误消息中提取 prompt 超长的 token 信息。

    尝试匹配类似以下格式的错误消息：
        - "prompt is too long: maximum context length is 200000 tokens, you passed 250000"
        - "prompt is too long (max_tokens: 200000, got: 250000)"

    Args:
        error_msg: 错误消息文本

    Returns:
        PromptTooLongInfo: 提取到的 token 信息
    """
    info = PromptTooLongInfo()

    if not error_msg:
        return info

    # 提取 max_tokens（取所有匹配组中的非 None 值）
    max_match = _PROMPT_TOO_LONG_RE.search(error_msg)
    if max_match:
        for group in max_match.groups():
            if group is not None:
                info.max_tokens = int(group)
                break

    # 提取 actual_tokens
    current_match = _PROMPT_TOO_LONG_CURRENT_RE.search(error_msg)
    if current_match:
        for group in current_match.groups():
            if group is not None:
                info.actual_tokens = int(group)
                break

    # 计算 tokens_over_by
    if info.max_tokens > 0 and info.actual_tokens > 0:
        info.tokens_over_by = info.actual_tokens - info.max_tokens
    elif info.tokens_over_by == 0:
        # 尝试直接匹配 "exceeds by X" 或 "over by X" 模式
        over_match = re.search(
            r"(?:exceeds?|over|over by|exceeded by)\s+(\d+)\s*tokens?",
            error_msg,
            re.IGNORECASE,
        )
        if over_match:
            info.tokens_over_by = int(over_match.group(1))

    return info


# ── FailoverResult（带 metadata 的故障转移结果） ──

@dataclass(eq=False)
class FailoverResult:
    """故障转移分类结果，携带 metadata 以供上层决策。"""
    reason: FailoverReason
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def value(self) -> str:
        """向后兼容：代理到 reason.value，使 FailoverResult 可像 FailoverReason 枚举一样使用。"""
        return self.reason.value

    def to_dict(self) -> dict[str, Any]:
        return {
            "reason": self.reason.value,
            "metadata": self.metadata,
        }

    def __eq__(self, other: object) -> bool:
        if isinstance(other, FailoverResult):
            return self.reason == other.reason and self.metadata == other.metadata
        if isinstance(other, FailoverReason):
            return self.reason == other
        return NotImplemented


def classify_error(error_msg: str, exit_code: int = 1) -> ErrorCategory:
    """通过关键词匹配分类错误。

    Args:
        error_msg: 错误消息文本
        exit_code: 进程退出码（默认 1）

    Returns:
        ErrorCategory: 错误分类结果

    分类规则：
        - RETRYABLE: 临时性错误（超时、网络、连接重置等）
        - NEEDS_HUMAN: 需要人工介入（预算、权限、认证等）
        - NON_RETRYABLE: 不可重试错误（其他所有情况）
    """
    if exit_code == 124:
        return ErrorCategory.RETRYABLE
    if exit_code in {130, 137, 143}:
        return ErrorCategory.NEEDS_HUMAN
    if not error_msg:
        return ErrorCategory.NON_RETRYABLE

    # 转小写便于匹配
    msg_lower = error_msg.lower()

    # 可重试错误：临时性故障
    retryable_keywords = [
        'timeout',
        'timed out',
        'rate limit',
        'rate-limit',
        'ratelimit',
        '429',  # HTTP Too Many Requests — 必须归类为 RETRYABLE
        'too many requests',
        'connection',
        'econnreset',
        'econnrefused',
        'network',
        'temporary failure',
        'try again',
        'retry',
        'unavailable',
        'service unavailable',
        '503',
        '502',
        '504',
        'gateway timeout',
        'overloaded',
        'capacity',
        'context_length_exceeded',
        'context overflow',
        'context length',
        'model_not_available',
    ]

    for keyword in retryable_keywords:
        if keyword in msg_lower:
            return ErrorCategory.RETRYABLE

    # 需要人工介入：预算、权限、认证问题
    human_keywords = [
        'budget',
        'quota',
        'permission',
        'forbidden',
        'unauthorized',
        'auth',
        'authentication',
        'credential',
        'access denied',
        '401',
        '403',
        'insufficient funds',
        'payment required',
        'invalid_api_key',
        'invalid api key',
        'invalid key',
    ]

    for keyword in human_keywords:
        if keyword in msg_lower:
            return ErrorCategory.NEEDS_HUMAN

    # 默认：不可重试
    return ErrorCategory.NON_RETRYABLE


def classify_error_smart(
    error_msg: str,
    stderr: str = "",
    claude_config: object | None = None,
    budget_tracker: object | None = None,
) -> ErrorCategory:
    """智能错误分类：先快速路径，NON_RETRYABLE 时用 Claude haiku 二次分类。

    Args:
        error_msg: 错误消息文本
        stderr: 标准错误输出
        claude_config: ClaudeConfig 实例（用于调用 Claude）
        budget_tracker: BudgetTracker 实例（用于预算控制）

    Returns:
        ErrorCategory: 错误分类结果
    """
    # 快速路径
    quick_result = classify_error(error_msg)
    if quick_result != ErrorCategory.NON_RETRYABLE:
        return quick_result

    # 缓存检查
    combined = f"{error_msg[:500]}|{stderr[:500]}"
    cache_key = hashlib.md5(combined.encode()).hexdigest()
    with _cache_lock:
        if cache_key in _classification_cache:
            logger.debug("错误分类缓存命中: %s", cache_key[:8])
            _classification_cache.move_to_end(cache_key)
            return _classification_cache[cache_key]

    # Claude haiku 二次分类
    if claude_config is None:
        return quick_result

    try:
        from .claude_cli import run_claude_task, BudgetTracker
        from .model import TaskNode

        # 获取 claude_config 的属性
        cli_path = getattr(claude_config, 'cli_path', 'claude')
        from .config import ClaudeConfig, LimitsConfig
        config = claude_config if isinstance(claude_config, ClaudeConfig) else ClaudeConfig()
        limits = LimitsConfig()

        prompt = (
            "对以下错误消息进行分类，只回复一个词：retryable / needs_human / non_retryable\n\n"
            f"错误消息（前500字符）:\n{error_msg[:500]}\n\n"
            f"stderr（前500字符）:\n{stderr[:500]}"
        )

        task = TaskNode(
            id="_error_classify",
            prompt_template=prompt,
            timeout=30,
            model="haiku",
            output_format="text",
        )

        result = run_claude_task(
            task=task,
            prompt=prompt,
            claude_config=config,
            limits=limits,
            budget_tracker=budget_tracker,
        )

        if result.output:
            output_lower = result.output.strip().lower()
            if "retryable" in output_lower:
                category = ErrorCategory.RETRYABLE
            elif "needs_human" in output_lower:
                category = ErrorCategory.NEEDS_HUMAN
            else:
                category = ErrorCategory.NON_RETRYABLE

            with _cache_lock:
                _classification_cache[cache_key] = category
                while len(_classification_cache) > _CLASSIFICATION_CACHE_MAX_SIZE:
                    _classification_cache.popitem(last=False)
            logger.info("智能错误分类结果: %s (by haiku)", category.value)
            return category

    except Exception as e:
        logger.debug("智能错误分类调用失败，回退到关键词分类: %s", e)

    # 回退到快速路径结果
    with _cache_lock:
        _classification_cache[cache_key] = quick_result
        while len(_classification_cache) > _CLASSIFICATION_CACHE_MAX_SIZE:
            _classification_cache.popitem(last=False)
    return quick_result


def should_retry(category: ErrorCategory, policy: ErrorPolicy) -> bool:
    """根据错误分类和策略决定是否重试。

    Args:
        category: 错误分类
        policy: 错误处理策略

    Returns:
        bool: 是否应该重试

    决策逻辑：
        - 如果 policy.classify_errors=False，不使用智能分类，遵循 on_error 策略
        - 如果 category=RETRYABLE，允许重试
        - 如果 category=NEEDS_HUMAN，不重试（需要人工介入）
        - 如果 category=NON_RETRYABLE，不重试
        - 如果 policy.on_error='continue-on-error'，即使不可重试也继续执行
    """
    # 如果未启用智能分类，遵循 on_error 策略
    if not policy.classify_errors:
        return policy.on_error == 'continue-on-error'

    # 启用智能分类时，只有 RETRYABLE 才重试
    if category == ErrorCategory.RETRYABLE:
        return True

    # NEEDS_HUMAN 和 NON_RETRYABLE 不重试
    # 但如果 on_error='continue-on-error'，可以继续执行（不是重试，是跳过）
    return False


def classify_failover_reason(
    error_msg: str,
    exit_code: int = 1,
    stderr: str = "",
    headers: dict | None = None,
) -> FailoverResult:
    """通过关键词匹配将错误分类为具体的 FailoverResult（含 metadata）。

    Args:
        error_msg: 错误消息文本
        exit_code: 进程退出码（默认 1）
        stderr: 标准错误输出（可选）
        headers: HTTP 响应头（可选，用于提取精确限流信息）

    Returns:
        FailoverResult: 故障转移结果，包含 reason 和 metadata

    分类规则：
        - RATE_LIMIT: API 速率限制（metadata 含 rate_limit_info）
        - AUTH_EXPIRED: 认证过期
        - CONTEXT_OVERFLOW: 上下文长度超限
        - TIMEOUT: 请求超时
        - NETWORK_ERROR: 网络错误
        - BUDGET_EXHAUSTED: 预算耗尽
        - PROMPT_TOO_LONG: prompt 超出模型 token 上限（metadata 含 prompt_info）
        - UNKNOWN: 未知错误（默认）
    """
    # 合并所有错误信息并转小写
    combined = f"{error_msg} {stderr}".lower()

    # ── 细粒度检测（更具体的关键词优先，放在通用检测之前）──

    # Prompt 超长：400 + "prompt is too long"（在上下文超限检测之前）
    if 'prompt is too long' in combined or 'prompt_too_long' in combined:
        prompt_info = extract_prompt_too_long_info(error_msg)
        return FailoverResult(
            reason=FailoverReason.PROMPT_TOO_LONG,
            metadata={"prompt_info": prompt_info.to_dict()} if prompt_info.has_details else {},
        )

    # 模型名无效（在认证检测之前，避免被 auth 关键词抢先）
    if any(kw in combined for kw in ('model_not_found', 'model not found', 'does not exist', 'invalid model')):
        return FailoverResult(reason=FailoverReason.INVALID_MODEL)

    # 工具调用格式不匹配
    if any(kw in combined for kw in ('tool_use', 'tool_result', 'mismatch', 'does not match')):
        return FailoverResult(reason=FailoverReason.TOOL_USE_MISMATCH)

    # 输出内容超长（在上下文超限检测之前，避免被 context 关键词抢先）
    if any(kw in combined for kw in ('content too large', 'output too long', 'response too large')):
        return FailoverResult(reason=FailoverReason.CONTENT_TOO_LARGE)

    # 信用额度耗尽（在预算耗尽检测之前，区分 credit 和 budget）
    if any(kw in combined for kw in ('credit', 'balance', 'quota exceeded')):
        return FailoverResult(reason=FailoverReason.CREDIT_EXHAUSTED)

    # 组织被禁用
    if any(kw in combined for kw in ('organization', 'org disabled', 'account suspended')):
        return FailoverResult(reason=FailoverReason.ORGANIZATION_BLOCKED)

    # 529 服务器过载（在 429 速率限制检测之前，区分 529 和 429）
    if '529' in combined or 'overloaded' in combined or 'capacity' in combined:
        return FailoverResult(reason=FailoverReason.MODEL_OVERLOAD)

    # ── 通用检测（原有逻辑保持不变）──

    # 速率限制 — 附带精确的限流信息
    if any(kw in combined for kw in ['rate limit', 'rate-limit', 'ratelimit', 'too many requests', '429']):
        rate_limit_info = parse_rate_limit_headers(headers) if headers else RateLimitInfo()
        metadata: dict[str, Any] = {}
        if not rate_limit_info.is_empty:
            metadata["rate_limit_info"] = rate_limit_info.to_dict()
            if rate_limit_info.retry_after_seconds is not None:
                metadata["retry_after_seconds"] = rate_limit_info.retry_after_seconds
        return FailoverResult(reason=FailoverReason.RATE_LIMIT, metadata=metadata)

    # 认证过期
    if any(kw in combined for kw in ['auth', 'authentication', 'unauthorized', '401', 'credential', 'token expired', 'session expired']):
        return FailoverResult(reason=FailoverReason.AUTH_EXPIRED)

    # 上下文超限
    if any(kw in combined for kw in ['context', 'token limit', 'max tokens', 'context length', 'too long', 'input too large']):
        return FailoverResult(reason=FailoverReason.CONTEXT_OVERFLOW)

    # 超时 — 记录超时类型
    if any(kw in combined for kw in ['timeout', 'timed out', 'deadline exceeded', 'request timeout']):
        timeout_metadata: dict[str, Any] = {}
        if 'deadline exceeded' in combined:
            timeout_metadata["timeout_type"] = "deadline"
        elif 'request timeout' in combined:
            timeout_metadata["timeout_type"] = "request"
        else:
            timeout_metadata["timeout_type"] = "generic"
        return FailoverResult(reason=FailoverReason.TIMEOUT, metadata=timeout_metadata)

    # 网络错误
    if any(kw in combined for kw in ['network', 'connection', 'econnreset', 'econnrefused', 'dns', 'socket', '502', '503', '504']):
        return FailoverResult(reason=FailoverReason.NETWORK_ERROR)

    # 预算耗尽
    if any(kw in combined for kw in ['budget', 'quota', 'insufficient funds', 'payment required', 'billing']):
        return FailoverResult(reason=FailoverReason.BUDGET_EXHAUSTED)

    # 默认：未知错误
    return FailoverResult(reason=FailoverReason.UNKNOWN)


def resolve_failover_status(
    reason: FailoverReason | FailoverResult,
    attempt: int,
    max_attempts: int,
) -> FailoverStatus:
    """根据错误原因和重试次数决定处理策略。

    Args:
        reason: 错误原因（FailoverReason 枚举或 FailoverResult 实例）
        attempt: 当前重试次数（1-based）
        max_attempts: 最大重试次数

    Returns:
        FailoverStatus: 处理动作
    """
    # 兼容 FailoverResult：提取内部的 FailoverReason
    if isinstance(reason, FailoverResult):
        reason = reason.reason

    # 检查是否还有重试次数
    has_retries = attempt < max_attempts

    if reason == FailoverReason.RATE_LIMIT:
        return FailoverStatus.RETRY_WITH_BACKOFF if has_retries else FailoverStatus.ABORT

    elif reason == FailoverReason.AUTH_EXPIRED:
        return FailoverStatus.NEEDS_HUMAN

    elif reason == FailoverReason.CONTEXT_OVERFLOW:
        return FailoverStatus.SWITCH_MODEL

    elif reason == FailoverReason.PROMPT_TOO_LONG:
        # prompt 超长：无法通过重试解决，需切换模型或人工干预
        return FailoverStatus.SWITCH_MODEL

    elif reason == FailoverReason.TIMEOUT:
        return FailoverStatus.RETRY_WITH_BACKOFF if has_retries else FailoverStatus.ABORT

    elif reason == FailoverReason.NETWORK_ERROR:
        # 网络错误前几次立即重试，后续退避
        if attempt <= 2:
            return FailoverStatus.RETRY_IMMEDIATELY if has_retries else FailoverStatus.ABORT
        else:
            return FailoverStatus.RETRY_WITH_BACKOFF if has_retries else FailoverStatus.ABORT

    elif reason == FailoverReason.BUDGET_EXHAUSTED:
        return FailoverStatus.ABORT

    elif reason == FailoverReason.MODEL_OVERLOAD:
        return FailoverStatus.SWITCH_MODEL

    elif reason == FailoverReason.INVALID_MODEL:
        return FailoverStatus.SWITCH_MODEL

    elif reason == FailoverReason.TOOL_USE_MISMATCH:
        return FailoverStatus.ABORT

    elif reason == FailoverReason.CONTENT_TOO_LARGE:
        return FailoverStatus.RETRY_WITH_BACKOFF if has_retries else FailoverStatus.ABORT

    elif reason == FailoverReason.CREDIT_EXHAUSTED:
        return FailoverStatus.ABORT

    elif reason == FailoverReason.ORGANIZATION_BLOCKED:
        return FailoverStatus.NEEDS_HUMAN

    else:
        return FailoverStatus.RETRY_WITH_BACKOFF if has_retries else FailoverStatus.ABORT


# ── 快速布尔分类函数（供 CLI 封装层使用） ──

_RATE_LIMIT_KEYWORDS = (
    "rate limit", "rate-limit", "ratelimit", "too many requests",
    "429", "quota exceeded", "throttl", "slow down",
)

_NETWORK_ERROR_KEYWORDS = (
    "econnreset", "econnrefused", "network", "connection reset",
    "connection refused", "socket hang up", "dns", "enotfound",
    "etimedout", "gateway timeout", "502", "503", "504",
    "service unavailable", "bad gateway",
)

_AUTH_ERROR_KEYWORDS = (
    "authentication", "unauthorized", "401", "session expired",
    "token expired", "login required", "not authenticated",
    "api key", "credential", "invalid key", "access denied",
)


def looks_like_rate_limit_error(text: str) -> bool:
    """快速判断文本是否包含速率限制相关关键词。"""
    if not text:
        return False
    lower = text.lower()
    return any(kw in lower for kw in _RATE_LIMIT_KEYWORDS)


def looks_like_network_error(text: str) -> bool:
    """快速判断文本是否包含网络错误相关关键词。"""
    if not text:
        return False
    lower = text.lower()
    return any(kw in lower for kw in _NETWORK_ERROR_KEYWORDS)


def looks_like_auth_error(text: str) -> bool:
    """快速判断文本是否包含认证过期相关关键词。"""
    if not text:
        return False
    lower = text.lower()
    return any(kw in lower for kw in _AUTH_ERROR_KEYWORDS)


def should_retry_with_priority(
    category: ErrorCategory,
    policy: ErrorPolicy,
    *,
    is_critical: bool = False,
) -> bool:
    """带优先级的重试决策。

    主链路任务（is_critical=True）：
      - RETRYABLE -> 重试
      - NEEDS_HUMAN -> 不重试
      - NON_RETRYABLE -> 仍尝试重试（可能只是 prompt 问题）

    辅助任务（is_critical=False）：
      - RETRYABLE -> 重试但次数更少（由 orchestrator 控制 max_attempts）
      - NEEDS_HUMAN / NON_RETRYABLE -> 立即失败，不浪费预算
    """
    if not policy.classify_errors:
        return policy.on_error == "continue-on-error"

    if is_critical:
        return category != ErrorCategory.NEEDS_HUMAN
    else:
        return category == ErrorCategory.RETRYABLE


# ── 细粒度错误分类 ──

class DetailedErrorInfo:
    """细粒度错误信息，包含分类、严重级别、可恢复性和恢复建议。"""
    __slots__ = (
        "category", "reason", "severity", "recoverable", "suggested_action",
        "rate_limit_info", "prompt_info", "metadata",
    )

    def __init__(
        self,
        category: ErrorCategory,
        reason: FailoverReason,
        severity: str = "medium",
        recoverable: bool = True,
        suggested_action: str = "",
        rate_limit_info: RateLimitInfo | None = None,
        prompt_info: PromptTooLongInfo | None = None,
        metadata: dict[str, Any] | None = None,
    ):
        self.category = category
        self.reason = reason
        self.severity = severity
        self.recoverable = recoverable
        self.suggested_action = suggested_action
        self.rate_limit_info = rate_limit_info
        self.prompt_info = prompt_info
        self.metadata = metadata or {}


# 各 FailoverReason 对应的恢复建议
_DETAILED_SUGGESTIONS: dict[FailoverReason, str] = {
    FailoverReason.RATE_LIMIT: "等待后重试，或降低并发数",
    FailoverReason.AUTH_EXPIRED: "检查 API Key 或重新认证",
    FailoverReason.CONTEXT_OVERFLOW: "减小任务上下文或拆分任务",
    FailoverReason.TIMEOUT: "增加 timeout 或简化任务",
    FailoverReason.NETWORK_ERROR: "检查网络连接，重试",
    FailoverReason.BUDGET_EXHAUSTED: "增加预算或减少任务数",
    FailoverReason.MODEL_OVERLOAD: "切换到轻量模型或稍后重试",
    FailoverReason.INVALID_MODEL: "检查模型名称，使用有效模型",
    FailoverReason.TOOL_USE_MISMATCH: "检查任务 prompt 格式，修复工具调用",
    FailoverReason.CONTENT_TOO_LARGE: "减小输出大小或拆分任务",
    FailoverReason.CREDIT_EXHAUSTED: "充值或更换 API Key",
    FailoverReason.ORGANIZATION_BLOCKED: "联系组织管理员",
    FailoverReason.PROMPT_TOO_LONG: "拆分任务或减小 prompt 内容",
}

# 不可自动恢复的错误类型
_NON_RECOVERABLE_REASONS = frozenset({
    FailoverReason.BUDGET_EXHAUSTED,
    FailoverReason.CREDIT_EXHAUSTED,
    FailoverReason.ORGANIZATION_BLOCKED,
    FailoverReason.INVALID_MODEL,
    FailoverReason.TOOL_USE_MISMATCH,
})

# 严重级别映射
_SEVERITY_MAP: dict[FailoverReason, str] = {
    FailoverReason.RATE_LIMIT: "low",
    FailoverReason.MODEL_OVERLOAD: "low",
    FailoverReason.NETWORK_ERROR: "low",
    FailoverReason.TIMEOUT: "medium",
    FailoverReason.CONTEXT_OVERFLOW: "medium",
    FailoverReason.CONTENT_TOO_LARGE: "medium",
    FailoverReason.TOOL_USE_MISMATCH: "high",
    FailoverReason.AUTH_EXPIRED: "high",
    FailoverReason.BUDGET_EXHAUSTED: "critical",
    FailoverReason.CREDIT_EXHAUSTED: "critical",
    FailoverReason.ORGANIZATION_BLOCKED: "critical",
    FailoverReason.INVALID_MODEL: "high",
    FailoverReason.PROMPT_TOO_LONG: "medium",
}


def classify_detailed(
    error_msg: str,
    exit_code: int = 1,
    headers: dict | None = None,
) -> DetailedErrorInfo:
    """返回细粒度错误分类，包含严重级别、可恢复性、恢复建议和可选的限流信息。

    Args:
        error_msg: 错误消息文本
        exit_code: 进程退出码（默认 1）
        headers: HTTP 响应头（可选，用于提取精确限流信息）

    Returns:
        DetailedErrorInfo: 细粒度错误信息
    """
    failover_result = classify_failover_reason(error_msg, exit_code, headers=headers)
    reason = failover_result.reason
    category = classify_error(error_msg, exit_code)

    # 提取结构化信息
    rate_limit_info = None
    prompt_info = None
    metadata = failover_result.metadata

    if reason == FailoverReason.RATE_LIMIT and "rate_limit_info" in metadata:
        # 从 metadata 中的字典重建 RateLimitInfo
        rli = metadata["rate_limit_info"]
        rate_limit_info = RateLimitInfo(
            requests_limit=rli.get("requests_limit", 0),
            requests_remaining=rli.get("requests_remaining", 0),
            requests_reset=_parse_iso_datetime(rli.get("requests_reset")),
            tokens_limit=rli.get("tokens_limit", 0),
            tokens_remaining=rli.get("tokens_remaining", 0),
            tokens_reset=_parse_iso_datetime(rli.get("tokens_reset")),
            retry_after_seconds=rli.get("retry_after_seconds"),
        )

    if reason == FailoverReason.PROMPT_TOO_LONG and "prompt_info" in metadata:
        pi = metadata["prompt_info"]
        prompt_info = PromptTooLongInfo(
            max_tokens=pi.get("max_tokens", 0),
            actual_tokens=pi.get("actual_tokens", 0),
            tokens_over_by=pi.get("tokens_over_by", 0),
        )

    return DetailedErrorInfo(
        category=category,
        reason=reason,
        severity=_SEVERITY_MAP.get(reason, "medium"),
        recoverable=reason not in _NON_RECOVERABLE_REASONS,
        suggested_action=_DETAILED_SUGGESTIONS.get(reason, ""),
        rate_limit_info=rate_limit_info,
        prompt_info=prompt_info,
        metadata=metadata,
    )


def _parse_iso_datetime(value: str | None) -> datetime | None:
    """安全解析 ISO 格式的 datetime 字符串。"""
    if value is None:
        return None
    try:
        return datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return None
