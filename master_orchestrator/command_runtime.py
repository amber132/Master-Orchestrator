"""Helpers for shell command execution across mixed environments."""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
from pathlib import Path


def current_python() -> str:
    return sys.executable


def normalize_python_command(command: str) -> str:
    """Replace bare `python` shell invocations with the current interpreter."""

    quoted_python = _quote_shell_token(current_python())
    segments = _split_shell_segments(command)
    normalized: list[str] = []
    for segment in segments:
        if segment in {"&&", "||", ";", "|"}:
            normalized.append(segment)
            continue
        normalized.append(_replace_leading_python(segment, quoted_python))
    return "".join(normalized)


def _replace_leading_python(segment: str, python_cmd: str) -> str:
    leading_ws = len(segment) - len(segment.lstrip())
    stripped = segment[leading_ws:]
    if not stripped:
        return segment
    token, token_end = _leading_shell_token(stripped)
    if token is None or token_end is None:
        return segment
    raw_token = _strip_matching_quotes(token)
    if not _is_python_invocation(raw_token):
        return segment
    return f"{segment[:leading_ws]}{python_cmd}{stripped[token_end:]}"


def _quote_shell_token(token: str) -> str:
    if os.name == "nt":
        return subprocess.list2cmdline([token])
    return shlex.quote(token)


def _leading_shell_token(segment: str) -> tuple[str | None, int | None]:
    if not segment:
        return None, None
    if segment[0] in {"'", '"'}:
        quote = segment[0]
        index = 1
        while index < len(segment):
            char = segment[index]
            if char == quote:
                return segment[: index + 1], index + 1
            if char == "\\" and quote == '"' and index + 1 < len(segment):
                index += 2
                continue
            index += 1
        return segment, len(segment)
    index = 0
    while index < len(segment) and not segment[index].isspace():
        index += 1
    return segment[:index], index


def _strip_matching_quotes(token: str) -> str:
    if len(token) >= 2 and token[0] == token[-1] and token[0] in {"'", '"'}:
        return token[1:-1]
    return token


def _is_python_invocation(token: str) -> bool:
    normalized = token.strip()
    if not normalized:
        return False
    if os.name == "nt":
        normalized = normalized.replace("/", "\\").lower()
        current = current_python().replace("/", "\\").lower()
        python_names = {
            "python",
            "python.exe",
            "python3",
            "python3.exe",
            Path(current_python()).name.lower(),
        }
        return normalized == current or normalized in python_names
    python_names = {
        "python",
        "python3",
        Path(current_python()).name,
    }
    return normalized == current_python() or normalized in python_names


def _split_shell_segments(command: str) -> list[str]:
    segments: list[str] = []
    current: list[str] = []
    quote: str | None = None
    index = 0
    while index < len(command):
        char = command[index]
        if quote is not None:
            current.append(char)
            if char == quote:
                quote = None
            elif char == "\\" and quote == '"' and index + 1 < len(command):
                index += 1
                current.append(command[index])
            index += 1
            continue

        if char in {"'", '"'}:
            quote = char
            current.append(char)
            index += 1
            continue

        two_chars = command[index:index + 2]
        if two_chars in {"&&", "||"}:
            segments.append("".join(current))
            segments.append(two_chars)
            current = []
            index += 2
            continue
        if char in {";", "|"}:
            segments.append("".join(current))
            segments.append(char)
            current = []
            index += 1
            continue
        current.append(char)
        index += 1

    segments.append("".join(current))
    return segments
