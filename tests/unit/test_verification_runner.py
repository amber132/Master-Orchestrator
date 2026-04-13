from pathlib import Path

from claude_orchestrator.verification_planner import VerificationCommand, VerificationPlan
from claude_orchestrator.verification_runner import (
    VerificationRunner,
    parse_verification_errors,
    run_targeted,
)


def test_verification_runner_collects_results(tmp_path: Path) -> None:
    plan = VerificationPlan(commands=[
        VerificationCommand(name="ok", command="python -c \"print('ok')\"", cwd=str(tmp_path)),
        VerificationCommand(name="fail", command="python -c \"import sys; sys.exit(2)\"", cwd=str(tmp_path)),
    ])

    result = VerificationRunner().run(plan)

    assert result.passed is False
    assert len(result.command_results) == 2
    assert any(item["passed"] for item in result.command_results)
    assert any(not item["passed"] for item in result.command_results)
    assert "ok" in result.full_output
    assert "fail" in result.full_output


def test_run_targeted_keeps_full_output(tmp_path: Path) -> None:
    result = run_targeted(
        [
            "python -c \"print('hello')\"",
            "python -c \"import sys; print('boom'); sys.exit(3)\"",
        ],
        cwd=str(tmp_path),
    )

    assert result.passed is False
    assert len(result.command_results) == 2
    assert "--- python (exit=0) ---" in result.full_output
    assert "--- python (exit=3) ---" in result.full_output
    assert "hello" in result.full_output
    assert "boom" in result.full_output


def test_parse_verification_errors_extracts_pytest_failure() -> None:
    issues = parse_verification_errors(
        result=type(
            "R",
            (),
            {
                "command_results": [
                    {
                        "passed": False,
                        "command": "pytest -q",
                        "stdout": "FAILED tests/unit/test_demo.py::test_case - AssertionError: nope",
                        "stderr": "",
                    }
                ]
            },
        )()
    )

    assert len(issues) == 1
    assert issues[0].tool == "pytest"
    assert issues[0].severity == "fail"
    assert "AssertionError" in issues[0].description
