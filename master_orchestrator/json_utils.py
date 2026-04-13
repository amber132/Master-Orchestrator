"""统一的 JSON 解析工具模块。

提供多层回退策略解析 Claude 返回的 JSON，应对输出不稳定、截断等问题。
"""

from __future__ import annotations

import json
import re
from typing import Any


def robust_parse_json(text: str) -> Any:
    """多层回退策略解析 Claude 返回的 JSON。

    合并了 goal_decomposer._parse_json 和 introspect._robust_parse_json 的所有策略。

    Args:
        text: Claude 返回的原始文本

    Returns:
        解析后的 Python 对象（dict 或 list）

    Raises:
        ValueError: 所有策略都失败时抛出

    策略顺序：
        1. 直接解析
        2. 提取完整代码块中的 JSON
        3. 代码块内截断（有开头 ``` 但没结尾）
        4. 提取首尾方括号（数组）或花括号（对象）
        5. 修复截断的 JSON
    """
    # 策略1: 直接解析
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # 策略2: 提取完整代码块中的 JSON
    code_block = re.search(r"```(?:json)?\s*\n(.*?)\n```", text, re.DOTALL)
    if code_block:
        try:
            return json.loads(code_block.group(1))
        except json.JSONDecodeError:
            pass

    # 策略2.5: 代码块内截断（有开头 ``` 但没有结尾 ```）
    code_block_start = re.search(r"```(?:json)?\s*\n", text)
    if code_block_start:
        json_text = text[code_block_start.end():]
        # 移除可能的结尾 ```
        json_text = re.sub(r"\n```\s*$", "", json_text)
        try:
            return json.loads(json_text)
        except json.JSONDecodeError:
            # 尝试修复截断
            repaired = repair_truncated_json(json_text)
            if repaired:
                try:
                    return json.loads(repaired)
                except json.JSONDecodeError:
                    pass

    # 策略3: 提取首尾方括号（数组）或花括号（对象）
    # 优先尝试数组（introspect 的做法，因为很多输出是 JSON 数组）
    bracket_start = text.find("[")
    bracket_end = text.rfind("]")
    if bracket_start != -1 and bracket_end > bracket_start:
        try:
            return json.loads(text[bracket_start : bracket_end + 1])
        except json.JSONDecodeError:
            pass

    brace_start = text.find("{")
    brace_end = text.rfind("}")
    if brace_start != -1 and brace_end > brace_start:
        try:
            return json.loads(text[brace_start : brace_end + 1])
        except json.JSONDecodeError:
            pass

    # 策略4: 修复截断的 JSON（对 [ 和 { 都尝试）
    for start_char, start_pos in [("[", bracket_start), ("{", brace_start)]:
        if start_pos != -1:
            truncated = text[start_pos:]
            repaired = repair_truncated_json(truncated)
            if repaired:
                try:
                    return json.loads(repaired)
                except json.JSONDecodeError:
                    pass

    raise ValueError(f"无法解析 JSON（尝试了所有回退策略）:\n{text[:500]}")


def repair_truncated_json(text: str) -> str | None:
    """尝试修复被截断的 JSON，补全缺失的括号和引号。

    使用栈追踪未闭合的括号，按正确嵌套顺序补全（introspect 的高级实现）。

    Args:
        text: 可能被截断的 JSON 文本

    Returns:
        修复后的 JSON 文本，如果无法修复则返回 None
    """
    # 移除末尾不完整的行（可能是截断的字符串值）
    lines = text.rstrip().split("\n")
    # 从末尾向前找到最后一个看起来完整的行
    while lines:
        last = lines[-1].strip()
        # 如果最后一行看起来是完整的 JSON 行，保留
        if last and (
            last.endswith(",")
            or last.endswith("{")
            or last.endswith("[")
            or last.endswith("}")
            or last.endswith("]")
            or last.endswith('"')
            or last.endswith(":")
            or last[-1].isdigit()
            or last.endswith("true")
            or last.endswith("false")
            or last.endswith("null")
        ):
            break
        lines.pop()

    if not lines:
        return None

    repaired = "\n".join(lines)

    # 移除末尾的逗号（JSON 不允许 trailing comma）
    repaired = repaired.rstrip()
    if repaired.endswith(","):
        repaired = repaired[:-1]

    # 检查是否在字符串内（简单检查：奇数个未转义引号）
    in_string = False
    i = 0
    while i < len(repaired):
        if repaired[i] == '"' and (i == 0 or repaired[i - 1] != "\\"):
            in_string = not in_string
        i += 1
    if in_string:
        repaired += '"'

    # 使用栈追踪未闭合的括号，按正确嵌套顺序补全
    stack: list[str] = []
    in_str = False
    prev_ch = ""
    for ch in repaired:
        # 检查前一个字符是否为反斜杠（转义）
        if ch == '"' and prev_ch != "\\":
            in_str = not in_str
        if in_str:
            prev_ch = ch
            continue
        if ch in ("{", "["):
            stack.append(ch)
        elif ch == "}" and stack and stack[-1] == "{":
            stack.pop()
        elif ch == "]" and stack and stack[-1] == "[":
            stack.pop()
        prev_ch = ch

    # 反向补全：栈顶的括号最先闭合
    closing = {"[": "]", "{": "}"}
    for bracket in reversed(stack):
        repaired += closing.get(bracket, "")

    return repaired if repaired else None
