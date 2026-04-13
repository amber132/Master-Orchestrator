"""Pluggable semantic validators for simple mode."""

from __future__ import annotations

import ast
import re
from pathlib import Path

from .simple_model import SimpleWorkItem

_CJK_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff]")
_LATIN_RE = re.compile(r"[A-Za-z]")
_ANNOTATION_TASK_RE = re.compile(
    r"(中文|注释|学习注释|docstring|comment\b|annotat(?:e|ion))",
    re.IGNORECASE,
)
_CLASS_ANNOTATION_MIN_CJK = 48
_PRIMARY_SCHOOL_CUES = (
    "比如",
    "例如",
    "也就是",
    "意思是",
    "你可以把",
    "可以把",
    "可以理解成",
    "就像",
    "好比",
    "用来",
    "负责",
    "先",
    "然后",
    "这样",
    "帮助",
)
_JS_CLASS_RE = re.compile(
    r"^\s*(?:export\s+)?(?:default\s+)?(?:abstract\s+)?class\s+(?P<name>[A-Za-z_$][\w$]*)\b"
)
_JS_FUNCTION_RE = re.compile(
    r"^\s*(?:export\s+)?(?:default\s+)?(?:async\s+)?function\s+(?P<name>[A-Za-z_$][\w$]*)\s*\("
)
_JS_ARROW_FUNCTION_RE = re.compile(
    r"^\s*(?:export\s+)?(?:const|let|var)\s+(?P<name>[A-Za-z_$][\w$]*)\s*=\s*(?:async\s*)?(?:\([^=]*\)|[A-Za-z_$][\w$]*)\s*=>"
)
_JS_METHOD_RE = re.compile(
    r"^\s*(?:(?:public|private|protected|static|readonly|override|abstract|async|get|set)\s+)*(?P<name>#?[A-Za-z_$][\w$]*)\s*\([^;]*\)\s*(?::[^={]+)?\s*\{?\s*$"
)
_JS_CONTROL_WORDS = {
    "if",
    "for",
    "while",
    "switch",
    "catch",
    "return",
    "function",
    "typeof",
    "case",
    "else",
    "do",
    "try",
    "new",
}
_VALIDATOR_PROMPT_HINTS = {
    "zh_annotation_coverage": [
        "该工单启用了 `zh_annotation_coverage` 质量门：只在文件头补一段中文总览不算完成。",
        "必须让目标文件里的模块、类、函数/方法都具备中文学习说明，否则会被判定失败。",
        "如果是 Python，优先补模块 docstring 与类/函数/方法的中文 docstring 或紧邻中文注释。",
        "如果是 JS/TS，优先补文件头中文说明，以及类、函数、方法前的中文注释块。",
        "如果目标是模板或混合内容文件，文件头中文说明可以放在 HTML 注释块里。",
    ],
    "zh_annotation_quality": [
        "该工单启用了 `zh_annotation_quality` 质量门：每个类的中文说明必须明显多于英文，中文字符数至少达到英文字符的 3 倍。",
        "类说明不能只写抽象术语，要像给小学生讲课一样白话，先说它是做什么的，再说什么时候会用到，最好顺手给一个生活化例子。",
        "出现英文术语、类名或协议名时，必须立刻用中文翻译或解释，不要把英文名直接堆在注释里。",
        "类说明需要足够详实，不能只写一两句口号式短句。",
        "英文类名、函数名、模块名最好只点名一次；点名后优先改用“这个类 / 这个步骤 / 这个命令层”这类中文代称。",
        "不要反复用反引号包裹英文标识，也不要把整段英文调用链照搬进类说明，否则很容易因为英文占比过高而失败。",
    ],
}


def _contains_cjk(text: str, *, min_chars: int = 2) -> bool:
    return len(_CJK_RE.findall(text)) >= min_chars


def _count_cjk(text: str) -> int:
    return len(_CJK_RE.findall(text))


def _count_latin(text: str) -> int:
    return len(_LATIN_RE.findall(text))


def _strip_js_comment_prefix(text: str) -> str:
    stripped = text.strip()
    for prefix in ("/**", "/*", "*/", "//", "*"):
        if stripped.startswith(prefix):
            stripped = stripped[len(prefix):].strip()
    if stripped.endswith("*/"):
        stripped = stripped[:-2].strip()
    return stripped


def _strip_html_comment_prefix(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("<!--"):
        stripped = stripped[4:].strip()
    if stripped.endswith("-->"):
        stripped = stripped[:-3].strip()
    return stripped


def _leading_html_comment(text: str) -> str:
    match = re.match(r"^\s*<!--(?P<body>.*?)-->\s*", text, re.DOTALL)
    if not match:
        return ""
    return _strip_html_comment_prefix(match.group(0))


def _leading_js_block_comment(text: str) -> str:
    match = re.match(r"^\s*/\*(?P<body>.*?)\*/\s*", text, re.DOTALL)
    if not match:
        return ""
    body = match.group(0)
    lines = [_strip_js_comment_prefix(line) for line in body.splitlines()]
    return "\n".join(line for line in lines if line)


def _comment_block_before(lines: list[str], line_no: int, *, comment_prefixes: tuple[str, ...], max_gap_lines: int = 1) -> str:
    index = line_no - 2
    blank_count = 0
    while index >= 0 and not lines[index].strip():
        blank_count += 1
        if blank_count > max_gap_lines:
            return ""
        index -= 1

    block: list[str] = []
    in_js_block = False
    while index >= 0:
        stripped = lines[index].strip()
        if not stripped:
            break
        if comment_prefixes == ("#",):
            if not stripped.startswith("#"):
                break
            block.append(stripped[1:].strip())
            index -= 1
            continue
        if in_js_block:
            block.append(_strip_js_comment_prefix(stripped))
            if stripped.startswith("/*") or stripped.startswith("/**"):
                in_js_block = False
            index -= 1
            continue
        if stripped.endswith("*/"):
            in_js_block = True
            block.append(_strip_js_comment_prefix(stripped))
            index -= 1
            continue
        if stripped.startswith("//") or stripped.startswith("/*") or stripped.startswith("/**") or stripped.startswith("*"):
            block.append(_strip_js_comment_prefix(stripped))
            index -= 1
            continue
        break

    block.reverse()
    return "\n".join(part for part in block if part)


def _python_decl_line(node: ast.AST) -> int:
    decorator_lines = [getattr(dec, "lineno", getattr(node, "lineno", 1)) for dec in getattr(node, "decorator_list", [])]
    return min([getattr(node, "lineno", 1), *decorator_lines])


def _python_node_has_annotation(node: ast.AST, lines: list[str]) -> bool:
    docstring = ast.get_docstring(node, clean=False)
    if docstring and _contains_cjk(docstring):
        return True
    return _contains_cjk(_comment_block_before(lines, _python_decl_line(node), comment_prefixes=("#",)))


def _python_node_annotation_text(node: ast.AST, lines: list[str]) -> str:
    docstring = ast.get_docstring(node, clean=False)
    if docstring:
        return docstring
    return _comment_block_before(lines, _python_decl_line(node), comment_prefixes=("#",))


def _validate_python_annotation_coverage(path: Path, text: str) -> list[str]:
    try:
        tree = ast.parse(text)
    except SyntaxError as exc:
        return [f"无法解析 Python 源码: line {exc.lineno or 0}"]

    lines = text.splitlines()
    issues: list[str] = []
    if tree.body:
        module_docstring = ast.get_docstring(tree, clean=False)
        module_comment = _comment_block_before(lines, getattr(tree.body[0], "lineno", 1), comment_prefixes=("#",))
        if not ((module_docstring and _contains_cjk(module_docstring)) or _contains_cjk(module_comment)):
            issues.append("模块缺少中文学习说明")

    def walk_node(node: ast.AST, parent_name: str = "") -> None:
        for child in getattr(node, "body", []):
            if isinstance(child, ast.ClassDef):
                qualified = f"{parent_name}.{child.name}" if parent_name else child.name
                if not _python_node_has_annotation(child, lines):
                    issues.append(f"类 {qualified} 缺少中文说明 (line {child.lineno})")
                walk_node(child, qualified)
                continue
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                kind = "方法" if isinstance(node, ast.ClassDef) else "函数"
                qualified = f"{parent_name}.{child.name}" if parent_name else child.name
                if not _python_node_has_annotation(child, lines):
                    issues.append(f"{kind} {qualified} 缺少中文说明 (line {child.lineno})")
                walk_node(child, qualified)

    walk_node(tree)
    return issues


def _annotation_quality_issues(label: str, annotation_text: str, *, line_no: int) -> list[str]:
    normalized = annotation_text.strip()
    if not normalized:
        return [f"{label} 缺少可供质量校验的中文说明 (line {line_no})"]

    issues: list[str] = []
    cjk_count = _count_cjk(normalized)
    latin_count = _count_latin(normalized)
    if cjk_count < _CLASS_ANNOTATION_MIN_CJK:
        issues.append(
            f"{label} 中文说明不够详实，至少需要 {_CLASS_ANNOTATION_MIN_CJK} 个中文字符 (当前 {cjk_count}) (line {line_no})"
        )
    if latin_count > 0 and cjk_count < latin_count * 3:
        issues.append(
            f"{label} 中文说明里的中文字符必须至少达到英文字符的 3 倍 (中文 {cjk_count} / 英文 {latin_count}) (line {line_no})"
        )
    if not any(cue in normalized for cue in _PRIMARY_SCHOOL_CUES):
        issues.append(
            f"{label} 中文说明不够白话，至少要出现“比如 / 也就是 / 你可以把…看成 / 用来 / 负责”等面向小学生的解释语气 (line {line_no})"
        )
    return issues


def _validate_python_annotation_quality(path: Path, text: str) -> list[str]:
    try:
        tree = ast.parse(text)
    except SyntaxError as exc:
        return [f"无法解析 Python 源码: line {exc.lineno or 0}"]

    lines = text.splitlines()
    issues: list[str] = []

    def walk_node(node: ast.AST, parent_name: str = "") -> None:
        for child in getattr(node, "body", []):
            if isinstance(child, ast.ClassDef):
                qualified = f"{parent_name}.{child.name}" if parent_name else child.name
                annotation_text = _python_node_annotation_text(child, lines)
                issues.extend(
                    _annotation_quality_issues(
                        f"类 {qualified}",
                        annotation_text,
                        line_no=child.lineno,
                    )
                )
                walk_node(child, qualified)
                continue
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                walk_node(child, parent_name)

    walk_node(tree)
    return issues


def _line_comment_has_cjk(lines: list[str], line_no: int) -> bool:
    return _contains_cjk(_comment_block_before(lines, line_no, comment_prefixes=("//", "/*", "*")))


def _validate_js_like_annotation_coverage(path: Path, text: str) -> list[str]:
    lines = text.splitlines()
    issues: list[str] = []
    leading_html_comment = _leading_html_comment(text)
    leading_js_comment = _leading_js_block_comment(text)
    scan_text = text
    consumed_lines = 0
    leading_match = None
    if leading_html_comment:
        leading_match = re.match(r"^\s*<!--(?P<body>.*?)-->\s*", text, re.DOTALL)
    elif leading_js_comment:
        leading_match = re.match(r"^\s*/\*(?P<body>.*?)\*/\s*", text, re.DOTALL)
    if leading_match:
        scan_text = text[leading_match.end():]
        consumed_lines = text[:leading_match.end()].count("\n")
    first_code_line = 0
    for idx, line in enumerate(scan_text.splitlines(), 1):
        stripped = line.strip()
        if not stripped or stripped.startswith("//") or stripped.startswith("/*") or stripped.startswith("/**") or stripped.startswith("*"):
            continue
        first_code_line = idx + consumed_lines
        break
    module_has_annotation = _contains_cjk(leading_html_comment) or _contains_cjk(leading_js_comment)
    if first_code_line and not module_has_annotation and not _line_comment_has_cjk(lines, first_code_line):
        issues.append("模块缺少中文学习说明")

    brace_depth = 0
    class_depths: list[int] = []
    pending_class_depth = False

    for idx, line in enumerate(lines, 1):
        stripped = line.strip()
        if not stripped or stripped.startswith("//") or stripped.startswith("/*") or stripped.startswith("*"):
            continue

        class_match = _JS_CLASS_RE.match(line)
        func_match = _JS_FUNCTION_RE.match(line) or _JS_ARROW_FUNCTION_RE.match(line)
        method_match = _JS_METHOD_RE.match(line) if class_depths else None

        name = ""
        kind = ""
        if class_match:
            name = class_match.group("name")
            kind = "类"
            pending_class_depth = "{" not in line
        elif brace_depth == 0 and func_match:
            name = func_match.group("name")
            kind = "函数"
        elif method_match and brace_depth >= class_depths[-1]:
            candidate = method_match.group("name")
            if candidate not in _JS_CONTROL_WORDS:
                name = candidate
                kind = "方法"

        if name and kind and not _line_comment_has_cjk(lines, idx):
            issues.append(f"{kind} {name} 缺少中文说明 (line {idx})")

        open_count = line.count("{")
        close_count = line.count("}")
        brace_depth += open_count - close_count
        if class_match:
            if "{" in line:
                class_depths.append(brace_depth)
        elif pending_class_depth and open_count > close_count:
            class_depths.append(brace_depth)
            pending_class_depth = False
        while class_depths and brace_depth < class_depths[-1]:
            class_depths.pop()

    return issues


def _validate_js_like_annotation_quality(path: Path, text: str) -> list[str]:
    lines = text.splitlines()
    issues: list[str] = []

    for idx, line in enumerate(lines, 1):
        stripped = line.strip()
        if not stripped or stripped.startswith("//") or stripped.startswith("/*") or stripped.startswith("*"):
            continue
        class_match = _JS_CLASS_RE.match(line)
        if not class_match:
            continue
        annotation_text = _comment_block_before(lines, idx, comment_prefixes=("//", "/*", "*"))
        issues.extend(
            _annotation_quality_issues(
                f"类 {class_match.group('name')}",
                annotation_text,
                line_no=idx,
            )
        )
    return issues


_SEMANTIC_VALIDATORS = {
    "zh_annotation_coverage": (
        lambda path, text: _validate_python_annotation_coverage(path, text)
        if path.suffix.lower() in {".py", ".pyi"}
        else _validate_js_like_annotation_coverage(path, text)
        if path.suffix.lower() in {".js", ".jsx", ".ts", ".tsx"}
        else []
    ),
    "zh_annotation_quality": (
        lambda path, text: _validate_python_annotation_quality(path, text)
        if path.suffix.lower() in {".py", ".pyi"}
        else _validate_js_like_annotation_quality(path, text)
        if path.suffix.lower() in {".js", ".jsx", ".ts", ".tsx"}
        else []
    ),
}


def looks_like_annotation_task(instruction: str, metadata: dict[str, object] | None = None) -> bool:
    if metadata:
        explicit = metadata.get("annotation_task")
        if isinstance(explicit, bool):
            return explicit
    return bool(_ANNOTATION_TASK_RE.search(instruction))


def resolve_semantic_validators(
    default_validators: list[str],
    item: SimpleWorkItem,
) -> list[str]:
    names = list(default_validators)
    names.extend(item.validation_profile.semantic_validators)
    explicit = item.metadata.get("semantic_validators") if item.metadata else None
    if isinstance(explicit, list):
        names.extend(str(entry) for entry in explicit if str(entry).strip())
    if looks_like_annotation_task(item.instruction, item.metadata) and any(
        name in {"zh_annotation_coverage", "zh_annotation_quality"} for name in names
    ):
        names.append("zh_annotation_quality")
    deduped: list[str] = []
    seen: set[str] = set()
    for name in names:
        normalized = str(name).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def semantic_prompt_hints(validator_names: list[str]) -> list[str]:
    hints: list[str] = []
    for name in validator_names:
        hints.extend(_VALIDATOR_PROMPT_HINTS.get(name, []))
    return hints


def run_semantic_validator(name: str, path: Path, text: str) -> tuple[bool, list[str], str]:
    validator = _SEMANTIC_VALIDATORS.get(name)
    if validator is None:
        return False, [f"未知语义校验器: {name}"], "unknown"
    issues = validator(path, text)
    return not issues, issues, ""
