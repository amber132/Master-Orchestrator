"""Verification command execution with targeted error parsing for surgical mode."""

from __future__ import annotations

import logging
import re
import subprocess
from dataclasses import dataclass, field

from .command_runtime import normalize_python_command
from .verification_planner import VerificationPlan

logger = logging.getLogger(__name__)


@dataclass
class VerificationRunResult:
    passed: bool
    command_results: list[dict] = field(default_factory=list)
    summary: str = ""
    full_output: str = ""  # 完整输出（不截断），供精确迭代模式使用


@dataclass
class VerificationIssue:
    """从验证输出中提取的结构化错误。"""
    file: str
    line: int | None
    description: str
    severity: str     # "error" | "warning" | "fail"
    tool: str         # "pytest" | "ruff" | "mypy" | "py_compile" | "generic"
    attempt: int = 0
    last_error: str = ""


def _display_name(command: str) -> str:
    parts = command.split()
    return parts[0] if parts else command


def _run_command_capture(
    *,
    name: str,
    command: str,
    cwd: str | None,
    timeout: int,
) -> dict:
    execution_command = normalize_python_command(command)
    try:
        proc = subprocess.run(
            execution_command,
            shell=True,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=timeout,
            encoding="utf-8",
            errors="replace",
        )
        return {
            "name": name,
            "command": command,
            "cwd": cwd,
            "exit_code": proc.returncode,
            "stdout": proc.stdout,
            "stderr": proc.stderr,
            "passed": proc.returncode == 0,
        }
    except subprocess.TimeoutExpired:
        return {
            "name": name,
            "command": command,
            "cwd": cwd,
            "exit_code": -1,
            "stdout": "",
            "stderr": f"timeout after {timeout}s",
            "passed": False,
        }
    except Exception as exc:
        return {
            "name": name,
            "command": command,
            "cwd": cwd,
            "exit_code": -1,
            "stdout": "",
            "stderr": str(exc),
            "passed": False,
        }


def _build_run_result(results: list[dict], *, include_command_in_header: bool) -> VerificationRunResult:
    passed_count = sum(1 for item in results if item["passed"])
    full_parts: list[str] = []
    for result in results:
        header = f"--- {result['name']}"
        if include_command_in_header:
            header += f": {result['command']}"
        header += f" (exit={result['exit_code']}) ---"
        full_parts.append(header)
        if result["stdout"]:
            full_parts.append(result["stdout"])
        if result["stderr"]:
            full_parts.append(result["stderr"])
    return VerificationRunResult(
        passed=passed_count == len(results) if results else True,
        command_results=results,
        summary=f"{passed_count}/{len(results)} 通过",
        full_output="\n".join(full_parts),
    )


class VerificationRunner:
    def run(self, plan: VerificationPlan, timeout: int = 600) -> VerificationRunResult:
        results = [
            _run_command_capture(
                name=item.name,
                command=item.command,
                cwd=item.cwd or None,
                timeout=timeout,
            )
            for item in plan.commands
        ]
        return _build_run_result(results, include_command_in_header=True)


def run_targeted(
    commands: list[str],
    cwd: str,
    timeout: int = 300,
) -> VerificationRunResult:
    """运行验证命令列表，返回完整输出（不截断）。

    供精确迭代模式使用。每个命令独立运行，收集全部输出。
    """
    results = [
        _run_command_capture(
            name=_display_name(command),
            command=command,
            cwd=cwd,
            timeout=timeout,
        )
        for command in commands
    ]
    return _build_run_result(results, include_command_in_header=False)


def parse_verification_errors(result: VerificationRunResult) -> list[VerificationIssue]:
    """从验证输出中提取结构化错误列表。"""
    issues: list[VerificationIssue] = []
    for r in result.command_results:
        if r["passed"]:
            continue
        tool = _detect_tool(r["command"])
        stderr = r.get("stderr", "")
        stdout = r.get("stdout", "")
        combined = f"{stdout}\n{stderr}"

        if tool == "pytest":
            issues.extend(_parse_pytest_errors(combined))
        elif tool == "ruff":
            issues.extend(_parse_ruff_errors(combined))
        elif tool == "mypy":
            issues.extend(_parse_mypy_errors(combined))
        elif tool == "py_compile":
            issues.extend(_parse_compile_errors(combined))
        else:
            issues.extend(_parse_generic_errors(combined))

    return issues


def _detect_tool(command: str) -> str:
    """从命令字符串推断使用的工具。"""
    cmd_lower = command.lower()
    if "pytest" in cmd_lower:
        return "pytest"
    if "ruff" in cmd_lower:
        return "ruff"
    if "mypy" in cmd_lower:
        return "mypy"
    if "py_compile" in cmd_lower or "pylint" in cmd_lower or "flake8" in cmd_lower:
        return "py_compile"
    return "generic"


def _parse_pytest_errors(output: str) -> list[VerificationIssue]:
    """解析 pytest 输出中的 FAILED 行。"""
    issues: list[VerificationIssue] = []
    for m in re.finditer(r"FAILED\s+(\S+\.py\S*)\s*[-:]\s*(.+)", output):
        issues.append(VerificationIssue(
            file=_extract_file(m.group(1)),
            line=None,
            description=m.group(2).strip(),
            severity="fail",
            tool="pytest",
        ))
    for m in re.finditer(r"ERROR\s+(\S+\.py\S*)\s*[-:]\s*(.+)", output):
        issues.append(VerificationIssue(
            file=_extract_file(m.group(1)),
            line=None,
            description=m.group(2).strip(),
            severity="error",
            tool="pytest",
        ))
    if not issues:
        summary_match = re.search(r"short test summary info\s*\n(.*)", output, re.DOTALL)
        if summary_match:
            for line in summary_match.group(1).strip().splitlines()[:10]:
                line = line.strip()
                if line:
                    issues.append(VerificationIssue(
                        file=_extract_file(line),
                        line=None,
                        description=line[:200],
                        severity="fail",
                        tool="pytest",
                    ))
    return issues


def _parse_ruff_errors(output: str) -> list[VerificationIssue]:
    """解析 ruff 输出：file.py:line:col: CODE message"""
    issues: list[VerificationIssue] = []
    for m in re.finditer(r"(\S+\.py):(\d+):(\d+):\s*(\S+)\s+(.+)", output):
        issues.append(VerificationIssue(
            file=m.group(1),
            line=int(m.group(2)),
            description=f"{m.group(4)} {m.group(5)}".strip(),
            severity="warning",
            tool="ruff",
        ))
    return issues


def _parse_mypy_errors(output: str) -> list[VerificationIssue]:
    """解析 mypy 输出：file.py:line: error: message"""
    issues: list[VerificationIssue] = []
    for m in re.finditer(r"(\S+\.py):(\d+):\s*(error|warning|note):\s*(.+)", output):
        issues.append(VerificationIssue(
            file=m.group(1),
            line=int(m.group(2)),
            description=m.group(4).strip(),
            severity=m.group(3) if m.group(3) in ("error", "warning") else "warning",
            tool="mypy",
        ))
    return issues


def _parse_compile_errors(output: str) -> list[VerificationIssue]:
    """解析 Python 编译错误：SyntaxError/IndentationError 等。"""
    issues: list[VerificationIssue] = []
    for m in re.finditer(r'File "([^"]+)", line (\d+)', output):
        rest = output[m.end():m.end() + 200]
        err_match = re.search(r"(\w+Error):(.+?)(?:\n|$)", rest)
        desc = err_match.group(0).strip() if err_match else "compile error"
        issues.append(VerificationIssue(
            file=m.group(1),
            line=int(m.group(2)),
            description=desc,
            severity="error",
            tool="py_compile",
        ))
    return issues


def _parse_generic_errors(output: str) -> list[VerificationIssue]:
    """通用 fallback：按行查找含 Error/error/fail 的行。"""
    issues: list[VerificationIssue] = []
    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
        if re.search(r"\b(error|Error|ERROR|fail|FAIL|FAILED)\b", line):
            file_match = re.search(r"(\S+\.py)", line)
            issues.append(VerificationIssue(
                file=file_match.group(1) if file_match else "",
                line=None,
                description=line[:300],
                severity="error",
                tool="generic",
            ))
            if len(issues) >= 20:
                break
    return issues


def _extract_file(text: str) -> str:
    """从文本中提取第一个源码文件路径。"""
    m = re.search(r"([\w./\\]+\.(?:py|js|ts|java|go|rs|toml|yaml|json))", text)
    return m.group(1) if m else ""
