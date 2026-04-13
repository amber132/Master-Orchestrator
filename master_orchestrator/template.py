"""Prompt template engine with ${task_id.output} variable substitution.

支持长输出自动溢出到文件：当上游输出超过阈值时，完整内容写入文件，
模板中只注入摘要 + 文件路径，下游任务可通过 Read 工具获取完整内容。
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from enum import IntEnum
from pathlib import Path
from typing import Any

from .config import SpillConfig
from .exceptions import TemplateRenderError

_VAR_PATTERN = re.compile(r"\$\{([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)\}")

# Total budget for all substituted outputs in a single prompt
DEFAULT_MAX_TOTAL_CHARS = 80_000
# Per-variable fallback cap
DEFAULT_MAX_OUTPUT_CHARS = 30_000

logger = logging.getLogger(__name__)


def _navigate(obj: Any, path: list[str]) -> Any:
    """Navigate a nested object by dot-separated path segments."""
    for segment in path:
        if isinstance(obj, dict):
            if segment not in obj:
                raise TemplateRenderError(f"Key '{segment}' not found in output")
            obj = obj[segment]
        elif isinstance(obj, (list, tuple)):
            try:
                obj = obj[int(segment)]
            except (ValueError, IndexError) as e:
                raise TemplateRenderError(f"Invalid index '{segment}': {e}") from e
        else:
            raise TemplateRenderError(f"Cannot navigate into {type(obj).__name__} with '{segment}'")
    return obj


def _truncate(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + f"\n... [truncated, {len(text) - max_chars} chars omitted]"


def _make_summary(text: str, summary_chars: int) -> str:
    """从长文本中提取头尾摘要，保留关键信息。"""
    if len(text) <= summary_chars:
        return text
    # 头部占 60%，尾部占 40%，中间用省略标记连接
    head_size = int(summary_chars * 0.6)
    tail_size = summary_chars - head_size
    omitted = len(text) - head_size - tail_size
    return (
        f"{text[:head_size]}\n"
        f"\n... [中间省略 {omitted} 字符，完整内容见下方文件路径] ...\n\n"
        f"{text[-tail_size:]}"
    )


def _spill_to_file(
    task_id: str,
    text: str,
    spill_dir: Path,
    run_id: str,
) -> Path:
    """将长输出写入溢出文件，返回文件绝对路径。"""
    spill_dir.mkdir(parents=True, exist_ok=True)
    # 文件名: {run_id}_{task_id}.txt，run_id 防止多次运行冲突
    filename = f"{run_id}_{task_id}.txt"
    filepath = spill_dir / filename
    filepath.write_text(text, encoding="utf-8")
    logger.info(
        "上游输出 '%s' 溢出到文件: %s (%d 字符)",
        task_id, filepath, len(text),
    )
    return filepath


def _build_spill_reference(
    task_id: str,
    text: str,
    filepath: Path,
    summary_chars: int,
) -> str:
    """构建溢出引用：摘要 + 文件路径提示。"""
    summary = _make_summary(text, summary_chars)
    # 用正斜杠，兼容 Claude CLI 在各平台的 Read 工具
    file_path_str = filepath.as_posix()
    return (
        f"{summary}\n\n"
        f"⚠️ 以上为任务 '{task_id}' 输出的摘要（原文 {len(text)} 字符）。\n"
        f"完整内容已保存到文件，如需查看请使用 Read 工具读取:\n"
        f"  {file_path_str}"
    )


def render_template(
    template: str,
    outputs: dict[str, Any],
    max_output_chars: int = DEFAULT_MAX_OUTPUT_CHARS,
    max_total_chars: int = DEFAULT_MAX_TOTAL_CHARS,
    spill_config: SpillConfig | None = None,
    spill_dir: Path | None = None,
    run_id: str = "",
) -> str:
    """Render a prompt template, substituting ${task_id.output...} references.

    当 spill_config 和 spill_dir 都提供时，启用溢出机制：
    超过 spill_threshold_chars 的输出自动写入文件，模板中注入摘要+路径。

    Args:
        template: The prompt template string.
        outputs: Mapping of task_id -> parsed output (or raw string).
        max_output_chars: Max chars per substituted value (溢出模式下仅作为兜底).
        max_total_chars: Max total chars for all substituted values combined.
        spill_config: 溢出配置，为 None 时退化为原始截断行为.
        spill_dir: 溢出文件存放目录.
        run_id: 当前运行 ID，用于溢出文件命名.

    Returns:
        The rendered prompt string.
    """
    spill_enabled = spill_config is not None and spill_dir is not None

    # Count how many unique variables are referenced to split budget
    refs = _VAR_PATTERN.findall(template)
    unique_tasks = {r.split(".")[0] for r in refs}
    num_refs = max(len(unique_tasks), 1)

    # Dynamic per-ref budget: split total evenly, but cap at per-variable max
    per_ref_budget = min(max_output_chars, max_total_chars // num_refs)

    if num_refs > 1:
        logger.info(
            "模板引用 %d 个上游输出，每个配额 %d 字符（总预算 %d）%s",
            num_refs, per_ref_budget, max_total_chars,
            "，已启用溢出到文件" if spill_enabled else "",
        )

    total_used = 0
    # 记录已溢出的 task_id -> filepath，同一 task 多次引用只写一次文件
    spilled_files: dict[str, Path] = {}

    def _replace(match: re.Match) -> str:
        nonlocal total_used
        full_path = match.group(1)
        parts = full_path.split(".")
        task_id = parts[0]

        if task_id not in outputs:
            raise TemplateRenderError(f"Referenced task '{task_id}' has no output available")

        value = outputs[task_id]

        # Navigate sub-path: ${task_id.output.key.0.name}
        sub_path = parts[1:]
        if sub_path and sub_path[0] == "output":
            sub_path = sub_path[1:]

        if sub_path:
            value = _navigate(value, sub_path)

        # Serialize to string
        if isinstance(value, str):
            text = value
        else:
            text = json.dumps(value, ensure_ascii=False, indent=2)

        # --- 溢出到文件逻辑 ---
        if spill_enabled and len(text) > spill_config.spill_threshold_chars:
            # 写入溢出文件（同一 task 只写一次）
            if task_id not in spilled_files:
                filepath = _spill_to_file(task_id, text, spill_dir, run_id)
                spilled_files[task_id] = filepath
            else:
                filepath = spilled_files[task_id]

            # 构建摘要+文件引用，替代原始截断
            replacement = _build_spill_reference(
                task_id, text, filepath, spill_config.summary_chars,
            )
            total_used += len(replacement)
            return replacement

        # --- 原始截断逻辑（兜底） ---
        remaining = max_total_chars - total_used
        effective_limit = min(per_ref_budget, remaining)
        truncated = _truncate(text, effective_limit)
        total_used += len(truncated)

        if len(text) > effective_limit:
            logger.warning(
                "上游输出 '%s' 被截断: %d -> %d 字符",
                task_id, len(text), effective_limit,
            )

        return truncated

    try:
        return _VAR_PATTERN.sub(_replace, template)
    except TemplateRenderError:
        raise
    except Exception as e:
        raise TemplateRenderError(f"Template rendering failed: {e}") from e


def extract_dependencies(template: str) -> set[str]:
    """Extract task IDs referenced in a template."""
    return {match.split(".")[0] for match in _VAR_PATTERN.findall(template)}


# ---------------------------------------------------------------------------
# 分层上下文压缩 — 结构保留的三层压缩策略
# ---------------------------------------------------------------------------

class BlockType(IntEnum):
    """结构块类型，数值越大优先级越高。"""
    DETAIL = 0       # 普通细节
    CODE = 1         # 代码块
    DECISION = 2     # 关键决策点
    METRIC = 3       # 度量 / 指标 / 分数
    HEADING = 4      # 标题 / 阶段名


@dataclass
class StructureBlock:
    """文本中提取的结构化块。"""

    block_type: BlockType
    content: str
    priority: int = 0

    def __post_init__(self) -> None:
        # 未手动指定 priority 时，使用 block_type 的值作为优先级
        if self.priority == 0:
            self.priority = self.block_type.value


class StructuralPreserver:
    """从文本中提取结构化信息，按类型分块并标记优先级。

    优先级排序：HEADING = METRIC > DECISION > CODE > DETAIL
    """

    # 标题：Markdown 标题、数字编号、阶段标记、方括号标题
    _HEADING_RE = re.compile(
        r"^#{1,6}\s+.+"                                    # # 标题
        r"|^\d+[\.\)]\s+.+"                                 # 1. 标题
        r"|^[\[\u3010].+?[\]\u3011]\s*$"                    # 【标题】
        r"|^(?:阶段|Phase|Step|Stage)\s*\d+[\s:：].+",      # 阶段1：
        re.IGNORECASE,
    )

    # 度量：分数、百分比、计数、耗时
    _METRIC_RE = re.compile(
        r"(?:score|分数|评分|accuracy|精确率|recall|召回率|F1)"
        r"|(?:\d+(?:\.\d+)?\s*%)"                           # 95.3%
        r"|(?:passed|failed|成功|失败|通过|耗时|duration|cost|budget|花费|费用)"
        r"|(?:\d+[/\\]\d+)"                                  # 42/50
        r"|(?:\d+(?:\.\d+)?\s*(?:tokens?|chars?|bytes?|lines?|files?|errors?|个|行|文件))",
        re.IGNORECASE,
    )

    # 决策：关键决策点
    _DECISION_RE = re.compile(
        r"(?:决定|选择|采用|确定|确认|approach|decided|chose|strategy|plan)"
        r"|(?:because|reason|why|rationale|因此|所以|由于|原因是)",
        re.IGNORECASE,
    )

    # 代码围栏 ```...```
    _CODE_FENCE_RE = re.compile(r"^(`{3,}).*$", re.MULTILINE)

    def extract_structure(self, text: str) -> list[StructureBlock]:
        """将文本分割为结构化块列表。

        识别规则：
        - Markdown 代码块 (``` ... ```) → CODE
        - 行首匹配标题模式 → HEADING
        - 包含度量关键词 / 数值 → METRIC
        - 包含决策关键词 → DECISION
        - 其余 → DETAIL

        连续同类行合并为同一个 StructureBlock。
        """
        if not text or not text.strip():
            return []

        code_regions = self._find_code_regions(text)
        segments = self._split_by_code_regions(text, code_regions)

        blocks: list[StructureBlock] = []
        for content, is_code in segments:
            if is_code:
                blocks.append(StructureBlock(block_type=BlockType.CODE, content=content))
            else:
                blocks.extend(self._classify_lines(content))

        return blocks

    def _find_code_regions(self, text: str) -> list[tuple[int, int]]:
        """找到所有配对的代码围栏区域 [(start, end), ...]。"""
        fences = list(self._CODE_FENCE_RE.finditer(text))
        regions: list[tuple[int, int]] = []
        # 每 2 个围栏配对为一组（奇数个尾部围栏直接忽略）
        for i in range(0, len(fences) - 1, 2):
            regions.append((fences[i].start(), fences[i + 1].end()))
        return regions

    @staticmethod
    def _split_by_code_regions(
        text: str,
        code_regions: list[tuple[int, int]],
    ) -> list[tuple[str, bool]]:
        """将文本拆分为交替的 (内容, 是否代码) 片段，保持原始顺序。"""
        segments: list[tuple[str, bool]] = []
        cursor = 0

        for start, end in code_regions:
            if start > cursor:
                segments.append((text[cursor:start], False))
            segments.append((text[start:end], True))
            cursor = end

        if cursor < len(text):
            segments.append((text[cursor:], False))

        return segments

    def _classify_lines(self, text: str) -> list[StructureBlock]:
        """对纯文本按行分类，合并相邻同类行。"""
        lines = text.split("\n")
        blocks: list[StructureBlock] = []
        buf: list[str] = []
        current_type: BlockType | None = None

        def flush() -> None:
            if buf and current_type is not None:
                blocks.append(StructureBlock(
                    block_type=current_type,
                    content="\n".join(buf),
                ))

        for line in lines:
            line_type = self._classify_line(line)
            if line_type != current_type:
                flush()
                buf = [line]
                current_type = line_type
            else:
                buf.append(line)

        flush()
        return blocks

    def _classify_line(self, line: str) -> BlockType:
        """判断单行文本的类型。"""
        stripped = line.strip()
        if not stripped:
            return BlockType.DETAIL

        if self._HEADING_RE.match(stripped):
            return BlockType.HEADING

        if self._METRIC_RE.search(stripped):
            return BlockType.METRIC

        if self._DECISION_RE.search(stripped):
            return BlockType.DECISION

        return BlockType.DETAIL


def _join_structure_blocks(blocks: list[StructureBlock]) -> str:
    """将块列表拼接为文本，块间用换行分隔。"""
    return "\n".join(b.content for b in blocks)


def hierarchical_compress(
    blocks: list[StructureBlock],
    max_chars: int,
    max_detail_length: int = 500,
) -> str:
    """三层压缩策略。

    第一层：结构保留 — 始终保留 heading / metric / decision 类型的块
    第二层：细节压缩 — 对 code / detail 类型的块截断到 max_detail_length
    第三层：预算控制 — 超预算时按 priority 从低到高裁剪，保留原始顺序

    Args:
        blocks: 结构化块列表
        max_chars: 目标最大字符数
        max_detail_length: code / detail 块的截断长度上限

    Returns:
        压缩后的文本
    """
    if not blocks:
        return ""

    # -- 第一层 & 第二层：结构保留 + 细节压缩 ------------------------------

    kept: list[StructureBlock] = []

    for block in blocks:
        if block.block_type in (BlockType.HEADING, BlockType.METRIC, BlockType.DECISION):
            # 高优先级块：原样保留
            kept.append(block)
        else:
            # 低优先级块（code / detail）：截断
            content = block.content
            if len(content) > max_detail_length:
                removed = len(content) - max_detail_length
                content = content[:max_detail_length] + f"\n... [截断 {removed} 字符] ..."
            kept.append(StructureBlock(
                block_type=block.block_type,
                content=content,
            ))

    # -- 第三层：预算控制 --------------------------------------------------

    total = sum(len(b.content) for b in kept)
    if total <= max_chars:
        return _join_structure_blocks(kept)

    # 按优先级从低到高排列索引，优先移除低优先级块
    indexed = sorted(range(len(kept)), key=lambda i: kept[i].priority)

    removed: set[int] = set()
    budget = total

    for idx in indexed:
        if budget <= max_chars:
            break
        budget -= len(kept[idx].content)
        removed.add(idx)

    result = [b for i, b in enumerate(kept) if i not in removed]
    logger.debug(
        "预算控制: 需裁剪至 %d 字符，移除 %d 个低优先级块",
        max_chars, len(removed),
    )

    return _join_structure_blocks(result)


# 匹配 "prompt is 12345 tokens too long" 等模式
_TOKEN_EXCESS_RE = re.compile(
    r"(\d[\d,]*)\s*tokens?\s*too\s*long",
    re.IGNORECASE,
)

# 1 token ≈ 4 chars（英文为主），中文约 2 chars/token，取保守值
_CHARS_PER_TOKEN = 4


def handle_prompt_too_long(
    error_message: str,
    current_context: str,
    *,
    max_detail_length: int = 500,
) -> str:
    """处理 prompt 过长错误，通过分层压缩裁剪上下文。

    1. 从错误消息中解析超出 token 数（如 "prompt is 12345 tokens too long"）
    2. 将 token 差值转换为字符预算（1 token ≈ 4 chars）
    3. 使用三层压缩优先保留结构化内容
    4. 返回压缩后的上下文

    Args:
        error_message: Claude API 返回的错误消息
        current_context: 当前完整上下文文本
        max_detail_length: code / detail 块的截断长度上限

    Returns:
        压缩后的上下文
    """
    match = _TOKEN_EXCESS_RE.search(error_message)

    if match:
        excess_tokens = int(match.group(1).replace(",", ""))
        excess_chars = excess_tokens * _CHARS_PER_TOKEN
        target_chars = max(100, len(current_context) - excess_chars - 200)
    else:
        # 无法解析时保守裁剪 20%
        target_chars = int(len(current_context) * 0.8)
        logger.warning(
            "无法从错误消息中解析 token 差值，保守裁剪至 %d 字符: %s",
            target_chars, error_message[:200],
        )

    preserver = StructuralPreserver()
    blocks = preserver.extract_structure(current_context)

    result = hierarchical_compress(blocks, target_chars, max_detail_length)

    logger.info(
        "handle_prompt_too_long: %d → %d 字符 (目标 %d)",
        len(current_context), len(result), target_chars,
    )

    return result
