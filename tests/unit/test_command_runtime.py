from __future__ import annotations

import sys

from master_orchestrator.command_runtime import normalize_python_command


def test_normalize_python_command_rewrites_bare_python_invocation() -> None:
    command = 'python -c "print(123)"'

    rendered = normalize_python_command(command)

    assert rendered.startswith(sys.executable)
    assert ' -c "print(123)"' in rendered


def test_normalize_python_command_rewrites_chained_python_segments() -> None:
    command = 'python -c "print(1)" && python -m pytest -q'

    rendered = normalize_python_command(command)

    assert rendered.count(sys.executable) == 2
    assert '&&' in rendered


def test_normalize_python_command_leaves_pythonish_binary_names_unchanged() -> None:
    command = 'python-tool --help'

    rendered = normalize_python_command(command)

    assert rendered == command
