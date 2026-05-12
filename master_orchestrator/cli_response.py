"""CLI 响应数据结构与流事件解析。

从 claude_cli.py 提取的数据类和解析函数。
"""
from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field

from .cost_calculation import _extract_cost_usd, _estimate_cost_from_tokens

logger = logging.getLogger(__name__)


class CostAssertionError(RuntimeError):
    """成本断言失败：存在 token 使用但成本为零。"""
    pass


@dataclass
class CLIResponse:
    raw: str
    result: str
    is_error: bool
    cost_usd: float
    model: str
    token_input: int = 0
    token_output: int = 0
    cli_duration_ms: float = 0.0


@dataclass
class StreamProgress:
    """Tracks progress from stream-json events."""
    tool_uses: int = 0
    text_chunks: int = 0
    last_tool: str = ""
    last_text_preview: str = ""
    result_event: dict | None = None
    all_events: list[dict] = field(default_factory=list)
    on_progress: Callable | None = None
    _max_events: int = 500  # 防止内存无限增长
    token_input: int = 0
    token_output: int = 0
    cache_read_tokens: int = 0
    cache_creation_tokens: int = 0
    cli_duration_ms: float = 0.0
    turn_started: int = 0
    turn_completed: int = 0
    max_turns_exceeded: bool = False
    json_parse_errors: int = 0  # 流事件 JSON 解析失败次数
    json_parse_error_samples: list[str] = field(default_factory=list)  # 前 N 个解析错误详情，用于 TaskResult.error 诊断
    # 停滞检测
    consecutive_read_only_calls: int = 0  # 连续只读工具调用次数（Write/Edit 重置为 0）
    stagnation_warning_sent: bool = False  # 20 次阈值警告是否已发送
    stagnation_killed: bool = False  # 40 次阈值是否已强制终止

    def append_event(self, event: dict) -> None:
        """追加事件，超过上限时丢弃最早的事件。"""
        self.all_events.append(event)
        if len(self.all_events) > self._max_events:
            # 保留最后 _max_events 个事件
            self.all_events = self.all_events[-self._max_events:]


def _parse_cli_output(raw: str) -> CLIResponse:
    """Parse the JSON envelope from claude -p --output-format json."""
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        # JSON 解析失败：记录警告
        # 注意：CLI 可能返回非 JSON 的纯文本（如错误信息），此时 is_error 由调用方判定
        logger.warning(
            "CLI 输出 JSON 解析失败: %s | 原始输出前200字符: %s",
            e, raw[:200],
        )
        # 如果原始输出为空，标记为错误（CLI 未产生有效输出）
        if not raw.strip():
            return CLIResponse(
                raw=raw, result="[PARSE_ERROR] CLI 输出为空且非 JSON",
                is_error=True, cost_usd=0.0, model="",
            )
        return CLIResponse(raw=raw, result=raw.strip(), is_error=False, cost_usd=0.0, model="")

    if isinstance(data, dict) and data.get("type") == "result":
        # 从 modelUsage 提取模型名（CLI 不再在顶层输出 model 字段）
        model_name = data.get("model", "")
        if not model_name:
            model_usage = data.get("modelUsage", {})
            if model_usage:
                model_name = next(iter(model_usage), "")
        return CLIResponse(
            raw=raw,
            result=data.get("result", ""),
            is_error=data.get("is_error", False),
            cost_usd=_extract_cost_usd(data),
            model=model_name,
        )

    return CLIResponse(raw=raw, result=raw.strip(), is_error=False, cost_usd=0.0, model="")


def _parse_stream_event(line: str, task_id: str, progress: StreamProgress) -> None:
    """Parse a single stream-json event line and log progress."""
    line = line.strip()
    if not line:
        return

    try:
        event = json.loads(line)
    except json.JSONDecodeError as e:
        # 记录解析失败，帮助诊断 CLI 输出异常问题
        progress.json_parse_errors += 1
        # 保留前 3 个解析错误详情，供 TaskResult.error 使用
        if len(progress.json_parse_error_samples) < 3:
            progress.json_parse_error_samples.append(f"{e} | 原始行: {line[:150]}")
        logger.warning(
            "[%s] 流事件 JSON 解析失败 (#%d): %s | 原始行: %s",
            task_id, progress.json_parse_errors, e, line[:200],
        )
        return

    progress.append_event(event)
    event_type = event.get("type", "")

    if event_type == "assistant":
        # Assistant turn with content blocks — 计数 turn
        progress.turn_started += 1
        message = event.get("message", {})
        content = message.get("content", [])
        for block in content:
            block_type = block.get("type", "")
            if block_type == "tool_use":
                tool_name = block.get("name", "unknown")
                tool_input = block.get("input", {})
                progress.tool_uses += 1
                progress.last_tool = tool_name
                # 停滞检测：Write/Edit 重置计数器，其他工具递增
                if tool_name in ("Write", "Edit"):
                    if progress.consecutive_read_only_calls > 0:
                        logger.info(
                            "[%s] 停滞检测重置: 写入工具 %s（之前连续 %d 次只读）",
                            task_id, tool_name, progress.consecutive_read_only_calls,
                        )
                    progress.consecutive_read_only_calls = 0
                else:
                    progress.consecutive_read_only_calls += 1
                # Log tool usage with brief input summary
                input_preview = str(tool_input)[:120]
                logger.info(
                    "[%s] 🔧 工具调用 #%d: %s | %s",
                    task_id, progress.tool_uses, tool_name, input_preview,
                )
            elif block_type == "text":
                text = block.get("text", "")
                if text.strip():
                    progress.text_chunks += 1
                    progress.last_text_preview = text[:100]
                    # Only log substantial text (not tiny fragments)
                    if len(text.strip()) > 20:
                        logger.info(
                            "[%s] 📝 文本输出 #%d: %s",
                            task_id, progress.text_chunks, text[:150].replace("\n", " "),
                        )
        # 从 assistant 事件的 message.usage 中累加 token（result 事件可能不存在）
        # 仅在 progress 计数仍为 0 时填充，避免与 result 事件的更精确值冲突
        a_usage = message.get("usage", {}) if isinstance(message, dict) else {}
        if isinstance(a_usage, dict):
            if not progress.token_input:
                progress.token_input = a_usage.get("input_tokens", 0) or a_usage.get("inputTokens", 0) or 0
            if not progress.token_output:
                progress.token_output = a_usage.get("output_tokens", 0) or a_usage.get("outputTokens", 0) or 0
            if not progress.cache_read_tokens:
                progress.cache_read_tokens = a_usage.get("cache_read_input_tokens", 0) or a_usage.get("cacheReadInputTokens", 0) or 0
            if not progress.cache_creation_tokens:
                progress.cache_creation_tokens = a_usage.get("cache_creation_input_tokens", 0) or a_usage.get("cacheCreationInputTokens", 0) or 0

    elif event_type == "item.completed":
        # 兼容旧版 stream-json：agent_message 文本承载在 item.completed 中。
        item = event.get("item", {})
        if item.get("type") in {"agent_message", "assistant_message"}:
            text = item.get("text", "")
            if text.strip():
                progress.text_chunks += 1
                progress.last_text_preview = text[:100]

    elif event_type == "tool_result":
        # Tool execution result
        tool_name = event.get("tool_name", "")
        is_error = event.get("is_error", False)
        content = event.get("content", "")
        status = "❌ 失败" if is_error else "✅ 完成"
        content_preview = str(content)[:100].replace("\n", " ") if content else ""
        logger.info("[%s] %s %s | %s", task_id, status, tool_name, content_preview)

    elif event_type == "result":
        # Final result event — 计数完成的 turn
        progress.turn_completed += 1
        progress.result_event = event
        # 调试：记录完整 result event 以诊断 cost 字段缺失问题
        logger.debug(
            "[%s] result event 完整内容:\n%s",
            task_id, json.dumps(event, indent=2, ensure_ascii=False),
        )
        # 成本字段：CLI 使用 total_cost_usd（顶层），旧版可能用 cost_usd
        cost = _extract_cost_usd(event)
        is_error = event.get("is_error", False)
        logger.info(
            "[%s] 🏁 任务结束 | 工具调用: %d 次 | 费用: $%.4f | 错误: %s",
            task_id, progress.tool_uses, cost, is_error,
        )
        # 解析 token 使用量和 CLI duration
        # Claude CLI 将 token 嵌套在 usage 对象中，同时支持顶层字段做兼容
        # 同时兼容 snake_case（input_tokens）和 camelCase（inputTokens）命名
        # 重要：仅在提取到非零值时更新 progress，避免 result 事件无 token 数据时
        # 覆盖从 assistant 事件中已收集的非零 token 值（这是 cost 始终为 0 的根因）
        usage = event.get("usage", {})
        _inp = (
            event.get("token_count_input", 0)
            or event.get("input_tokens", 0)
            or event.get("inputTokens", 0)
            or usage.get("input_tokens", 0)
            or usage.get("inputTokens", 0)
        )
        _out = (
            event.get("token_count_output", 0)
            or event.get("output_tokens", 0)
            or event.get("outputTokens", 0)
            or usage.get("output_tokens", 0)
            or usage.get("outputTokens", 0)
        )
        if _inp:
            progress.token_input = _inp
        if _out:
            progress.token_output = _out
        # 从 modelUsage 提取 token（CLI 2025+ 版本可能将 token 放在此处而非顶层）
        model_usage = event.get("modelUsage", {})
        if isinstance(model_usage, dict) and model_usage:
            if not progress.token_input or not progress.token_output:
                for _mk, _md in model_usage.items():
                    if not isinstance(_md, dict):
                        continue
                    if not progress.token_input:
                        _v = _md.get("input_tokens", 0) or _md.get("inputTokens", 0) or 0
                        if _v:
                            progress.token_input = _v
                    if not progress.token_output:
                        _v = _md.get("output_tokens", 0) or _md.get("outputTokens", 0) or 0
                        if _v:
                            progress.token_output = _v
            # cache tokens 也从 modelUsage 提取
            if not progress.cache_read_tokens or not progress.cache_creation_tokens:
                for _mk, _md in model_usage.items():
                    if not isinstance(_md, dict):
                        continue
                    if not progress.cache_read_tokens:
                        _v = _md.get("cache_read_input_tokens", 0) or _md.get("cacheReadInputTokens", 0) or 0
                        if _v:
                            progress.cache_read_tokens = _v
                    if not progress.cache_creation_tokens:
                        _v = _md.get("cache_creation_input_tokens", 0) or _md.get("cacheCreationInputTokens", 0) or 0
                        if _v:
                            progress.cache_creation_tokens = _v

    elif event_type == "turn.completed":
        # 兼容旧版 stream-json：turn.completed 携带 usage，但不一定有 result 事件。
        progress.turn_completed += 1
        usage = event.get("usage", {})
        if isinstance(usage, dict):
            progress.token_input = usage.get("input_tokens", 0) or usage.get("inputTokens", 0) or progress.token_input
            progress.token_output = usage.get("output_tokens", 0) or usage.get("outputTokens", 0) or progress.token_output
            progress.cache_read_tokens = (
                usage.get("cache_read_input_tokens", 0)
                or usage.get("cacheReadInputTokens", 0)
                or progress.cache_read_tokens
            )
            progress.cache_creation_tokens = (
                usage.get("cache_creation_input_tokens", 0)
                or usage.get("cacheCreationInputTokens", 0)
                or progress.cache_creation_tokens
            )
        progress.cli_duration_ms = event.get("duration_ms", 0.0) or event.get("duration", 0.0)
        # 提取 cache token（Anthropic API 特有字段，嵌套在 usage 中）
        _cr = (
            event.get("cache_read_input_tokens", 0)
            or event.get("cacheReadInputTokens", 0)
            or usage.get("cache_read_input_tokens", 0)
            or usage.get("cacheReadInputTokens", 0)
            or 0
        )
        _cc = (
            event.get("cache_creation_input_tokens", 0)
            or event.get("cacheCreationInputTokens", 0)
            or usage.get("cache_creation_input_tokens", 0)
            or usage.get("cacheCreationInputTokens", 0)
            or 0
        )
        if _cr:
            progress.cache_read_tokens = _cr
        if _cc:
            progress.cache_creation_tokens = _cc

    elif event_type == "system":
        # System messages (e.g., init, model info)
        msg = event.get("message", "") or event.get("subtype", "")
        if msg:
            logger.info("[%s] ⚙️ 系统: %s", task_id, str(msg)[:150])

    # Call on_progress callback if provided
    if progress.on_progress is not None:
        progress.on_progress(event_type, event)


def _build_response_from_stream(progress: StreamProgress) -> CLIResponse:
    """Build a CLIResponse from collected stream events."""
    if progress.result_event:
        evt = progress.result_event
        # 从 modelUsage 提取模型名（CLI 不再在顶层输出 model 字段）
        model_name = evt.get("model", "")
        if not model_name:
            model_usage = evt.get("modelUsage", {})
            if model_usage:
                model_name = next(iter(model_usage), "")
        extracted_cost = _extract_cost_usd(evt)
        # 最终兜底：result 事件不含 cost/token 数据时，用 progress 中从 assistant 事件
        # 收集的 token 数据估算成本（修复 cost 始终为 0 的根因）
        if extracted_cost == 0.0 and (progress.token_input or progress.token_output
                                      or progress.cache_read_tokens or progress.cache_creation_tokens):
            synthetic = {
                "input_tokens": progress.token_input,
                "output_tokens": progress.token_output,
                "cache_read_input_tokens": progress.cache_read_tokens,
                "cache_creation_input_tokens": progress.cache_creation_tokens,
                "model": model_name,
            }
            estimated = _estimate_cost_from_tokens(synthetic)
            if estimated > 0:
                logger.info(
                    "result_event cost=0，用 progress token 估算: model=%s, tokens=%d/%d -> $%.6f",
                    model_name or "unknown", progress.token_input, progress.token_output, estimated,
                )
                extracted_cost = estimated
        return CLIResponse(
            raw=json.dumps(evt, ensure_ascii=False),
            result=evt.get("result", ""),
            is_error=evt.get("is_error", False),
            cost_usd=extracted_cost,
            model=model_name,
            token_input=progress.token_input,
            token_output=progress.token_output,
            cli_duration_ms=progress.cli_duration_ms,
        )

    # No result event found — assemble text from assistant messages
    text_parts = []
    for evt in progress.all_events:
        if evt.get("type") == "assistant":
            for block in evt.get("message", {}).get("content", []):
                if block.get("type") == "text":
                    text_parts.append(block.get("text", ""))
        elif evt.get("type") == "item.completed":
            item = evt.get("item", {})
            if item.get("type") in {"agent_message", "assistant_message"}:
                text_parts.append(item.get("text", ""))

    combined = "".join(text_parts)

    # 如果没有任何输出且存在 JSON 解析错误，标记为错误（防止空响应伪装成功）
    if not combined.strip() and progress.json_parse_errors > 0:
        logger.error(
            "流式响应无 result 事件且 JSON 解析失败 %d 次，总事件数 %d，疑似输出解析全部失败",
            progress.json_parse_errors, len(progress.all_events),
        )
        return CLIResponse(
            raw="",
            result=f"[PARSE_ERROR] 流式响应无 result 事件，{progress.json_parse_errors} 个事件 JSON 解析失败",
            is_error=True,
            cost_usd=0.0,
            model="",
            token_input=progress.token_input,
            token_output=progress.token_output,
            cli_duration_ms=progress.cli_duration_ms,
        )

    if not combined.strip() and len(progress.all_events) == 0:
        # 完全没有任何事件 — CLI 输出为空，可能是进程异常
        logger.error("流式响应完全为空：0 个事件，0 个解析错误")
        return CLIResponse(
            raw="",
            result="[EMPTY_RESPONSE] CLI 输出完全为空，未收到任何流事件",
            is_error=True,
            cost_usd=0.0,
            model="",
            token_input=progress.token_input,
            token_output=progress.token_output,
            cli_duration_ms=progress.cli_duration_ms,
        )

    # result_event 为 None 的回退路径：尝试从已收集的事件中提取成本
    # 遍历 all_events 查找含 cost/usage 数据的事件（优先 result，其次 assistant）
    fallback_cost = 0.0
    fallback_model = ""
    for evt in reversed(progress.all_events):
        evt_type = evt.get("type", "")
        # 尝试从任意事件中提取成本
        c = _extract_cost_usd(evt)
        if c > 0 and fallback_cost == 0.0:
            fallback_cost = c
            logger.info("_build_response_from_stream 回退路径从 %s 事件提取到成本: $%.4f", evt_type, c)
        # 尝试提取模型名
        if not fallback_model:
            m = evt.get("model", "")
            if not m:
                mu = evt.get("modelUsage", {})
                if isinstance(mu, dict) and mu:
                    m = next(iter(mu), "")
            if m:
                fallback_model = m
        # 两个都找到了就提前退出
        if fallback_cost > 0 and fallback_model:
            break

    # 如果从事件中也没提取到成本，用 progress 中的 token 计数估算
    if fallback_cost == 0.0 and (progress.token_input or progress.token_output):
        synthetic_event = {
            "input_tokens": progress.token_input,
            "output_tokens": progress.token_output,
            "cache_read_input_tokens": progress.cache_read_tokens,
            "cache_creation_input_tokens": progress.cache_creation_tokens,
        }
        fallback_cost = _estimate_cost_from_tokens(synthetic_event)
        if fallback_cost > 0:
            logger.info("_build_response_from_stream 回退路径 token 估算成本: $%.6f", fallback_cost)

    return CLIResponse(
        raw=combined,
        result=combined,
        is_error=False,
        cost_usd=fallback_cost,
        model=fallback_model,
        token_input=progress.token_input,
        token_output=progress.token_output,
        cli_duration_ms=progress.cli_duration_ms,
    )
