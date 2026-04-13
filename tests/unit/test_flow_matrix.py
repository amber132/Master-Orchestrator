from __future__ import annotations

import json
import subprocess
from pathlib import Path

from master_orchestrator.flow_matrix import (
    _AUTO_PROMPT,
    FlowContext,
    FlowResult,
    _validate_auto_comment_smoke,
    _run_auto_like_flow,
    _run_command,
    build_default_flows,
    write_flow_report,
)
from master_orchestrator.repo_profile import RepoProfile
from master_orchestrator.task_classifier import TaskClassifier
from master_orchestrator.task_contract import DataRisk, TaskType
from master_orchestrator.task_intake import TaskIntakeRequest, build_task_contract


def test_default_flow_matrix_contains_public_and_real_flow_ids() -> None:
    flows = build_default_flows()
    flow_ids = {flow.flow_id for flow in flows}

    assert "import_aliases" in flow_ids
    assert "help_master_module" in flow_ids
    assert "simple_codex_real" in flow_ids
    assert "auto_codex_real" in flow_ids
    assert "improve_real" in flow_ids
    assert any(flow.tier == "required" for flow in flows if not flow.requires_real_provider)
    assert all(flow.tier == "nightly" for flow in flows if flow.requires_real_provider)


def test_write_flow_report_emits_summary_counts(tmp_path: Path) -> None:
    report_path = tmp_path / "flow-matrix.json"
    results = [
        FlowResult(flow_id="a", title="A", status="passed", duration_seconds=0.1, tier="required"),
        FlowResult(flow_id="b", title="B", status="failed", duration_seconds=0.2, tier="required"),
        FlowResult(flow_id="c", title="C", status="blocked", duration_seconds=0.3, tier="nightly"),
        FlowResult(flow_id="d", title="D", status="skipped", duration_seconds=0.0, tier="nightly"),
    ]

    write_flow_report(report_path, results)

    data = json.loads(report_path.read_text(encoding="utf-8"))
    assert data["summary"]["passed"] == 1
    assert data["summary"]["failed"] == 1
    assert data["summary"]["blocked"] == 1
    assert data["summary"]["skipped"] == 1
    assert data["summary"]["required_total"] == 2
    assert data["summary"]["nightly_total"] == 2
    assert data["summary"]["required_failed"] == 1
    assert data["summary"]["required_blocked"] == 0
    assert data["summary"]["gate_passed"] is False


def test_run_command_does_not_mark_plain_timeout_as_provider_blocked(monkeypatch, tmp_path: Path) -> None:
    ctx = FlowContext(
        repo_root=tmp_path,
        audit_root=tmp_path / "audit",
        python_executable="python",
    )
    ctx.audit_root.mkdir(parents=True, exist_ok=True)

    def fake_run(*_args, **_kwargs):
        raise subprocess.TimeoutExpired(cmd=["master-orchestrator"], timeout=240, output="partial", stderr="still running")

    monkeypatch.setattr("master_orchestrator.flow_matrix.subprocess.run", fake_run)

    result = _run_command(
        ctx,
        "timeout_case",
        "timeout case",
        ["master-orchestrator", "do"],
        real_provider=True,
    )

    assert result.status == "failed"
    assert "TIMEOUT after 240s" in Path(result.stderr_path).read_text(encoding="utf-8")


def test_run_auto_like_flow_uses_extended_timeout(monkeypatch, tmp_path: Path) -> None:
    ctx = FlowContext(
        repo_root=tmp_path,
        audit_root=tmp_path / "audit",
        python_executable="python",
    )
    ctx.audit_root.mkdir(parents=True, exist_ok=True)
    sandbox = tmp_path / "sandbox"
    sandbox.mkdir()
    (sandbox / ".git").mkdir()
    (sandbox / "sample.py").write_text("def add(a, b):\n    return a + b\n", encoding="utf-8")
    captured: dict[str, object] = {}

    monkeypatch.setattr("master_orchestrator.flow_matrix._create_git_sandbox", lambda *_args, **_kwargs: sandbox)
    monkeypatch.setattr(ctx, "cli_script", lambda _name: Path("master-orchestrator"))

    def fake_run_command(_ctx, _flow_id, _title, _command, **kwargs):
        captured["timeout"] = kwargs["timeout"]
        return FlowResult(flow_id="auto_codex_real", title="auto", status="passed", duration_seconds=0.1)

    monkeypatch.setattr("master_orchestrator.flow_matrix._run_command", fake_run_command)

    _run_auto_like_flow(ctx, "auto_codex_real", ["--provider", "codex"])

    assert captured["timeout"] == 600


def test_auto_flow_prompt_is_classified_as_refactor_for_native_phase_closure(tmp_path: Path) -> None:
    request = TaskIntakeRequest(goal=_AUTO_PROMPT, document_paths=[], repo_root=tmp_path)
    profile = RepoProfile(root=tmp_path)
    classification = TaskClassifier().classify(request, profile)
    contract = build_task_contract(request, profile, classification)

    assert classification.task_type is TaskType.REFACTOR
    assert contract.data_risk is DataRisk.NONE


def test_validate_auto_comment_smoke_accepts_workspace_delivery(tmp_path: Path) -> None:
    sandbox = tmp_path / "mo-flow-auto_codex_real-abc123"
    sandbox.mkdir()
    (sandbox / "sample.py").write_text("def add(a, b):\n    return a + b\n", encoding="utf-8")

    repo_root = tmp_path / "repo"
    workspace = repo_root / "orchestrator_runs" / sandbox.name / "run-1" / "workspace"
    workspace.mkdir(parents=True)
    (workspace / "sample.py").write_text("# auto smoke test\ndef add(a, b):\n    return a + b\n", encoding="utf-8")
    handoff = repo_root / "orchestrator_runs" / sandbox.name / "run-1" / "handoff"
    handoff.mkdir(parents=True)
    (handoff / "handoff_summary.md").write_text("# handoff\n", encoding="utf-8")

    _validate_auto_comment_smoke(repo_root, sandbox, "# auto smoke test")
