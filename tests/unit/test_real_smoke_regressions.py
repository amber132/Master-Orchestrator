from __future__ import annotations

from pathlib import Path
import subprocess

from master_orchestrator.backup_gate import BackupGate
from master_orchestrator.config import BackupConfig, Config
from master_orchestrator.runtime_layout import RuntimeLayout
from master_orchestrator.cli import _preflight_check
from master_orchestrator.simple_executor import build_simple_prompt
from master_orchestrator.simple_isolation import PreparedItemWorkspace
from master_orchestrator.simple_model import AttemptState, SimpleItemType, SimpleValidationProfile, SimpleWorkItem
from master_orchestrator.task_classifier import TaskClassification
from master_orchestrator.task_contract import DataRisk, TaskType
from master_orchestrator.task_intake import build_task_contract, normalize_request


def test_simple_config_supports_prompt_feedback_and_semantic_defaults() -> None:
    cfg = Config()

    assert isinstance(cfg.simple.default_semantic_validators, list)
    assert cfg.simple.retry_feedback_in_prompt_enabled is True


def test_build_simple_prompt_does_not_crash_on_default_config(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    target = repo / "sample.py"
    target.write_text("print('hi')\n", encoding="utf-8")
    item = SimpleWorkItem(
        item_id="item-1",
        item_type=SimpleItemType.FILE,
        target="sample.py",
        bucket="root",
        priority=0,
        instruction="Add one comment",
        attempt_state=AttemptState(),
        validation_profile=SimpleValidationProfile(),
    )
    prepared = PreparedItemWorkspace(
        item=item,
        requested_mode="none",
        cwd=repo,
        target_path=target,
        source_target_path=target,
        git_root=None,
        effective_mode="none",
        source_baseline_hash="",
        target_baseline_hash="",
        source_baseline_bytes=target.read_bytes(),
    )

    prompt = build_simple_prompt(prepared)

    assert "Add one comment" in prompt


def test_generic_file_word_does_not_force_file_backup_risk(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    request = normalize_request("这个项目能运行吗 你分析一下这个文件", [], repo)
    classification = TaskClassification(
        task_type=TaskType.UNKNOWN,
        confidence=0.6,
        affected_areas=[],
        reasons=[],
    )

    contract = build_task_contract(request, profile=_profile_stub(repo), classification=classification)

    assert contract.data_risk is DataRisk.NONE


def test_backup_gate_skips_when_contract_has_no_real_file_risk(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    layout = RuntimeLayout.create(tmp_path / "runtime")
    request = normalize_request("分析这个文件里有没有问题", [], repo)
    classification = TaskClassification(task_type=TaskType.UNKNOWN, confidence=0.6, affected_areas=[], reasons=[])
    contract = build_task_contract(request, profile=_profile_stub(repo), classification=classification)

    manifest = BackupGate(BackupConfig()).run(contract, layout, repo)

    assert manifest.summary == "未涉及数据"


def test_preflight_check_uses_codex_for_codex_provider(tmp_path: Path, monkeypatch) -> None:
    calls: list[list[str]] = []

    class _Result:
        returncode = 0
        stdout = "codex 0.120.0"
        stderr = ""

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        return _Result()

    monkeypatch.setattr(subprocess, "run", fake_run)
    cfg = Config()
    cfg.codex.cli_path = "codex-custom"

    _preflight_check(tmp_path, provider="codex", config=cfg)

    assert calls == [["codex-custom", "--version"]]


def _profile_stub(repo: Path):
    from types import SimpleNamespace

    return SimpleNamespace(
        root=repo,
        detected_frameworks=[],
        package_managers=[],
        backend_commands=[],
        frontend_commands=[],
        verification_commands=[],
        file_backup_paths=[],
        metadata_backup_paths=[],
        database_backup_commands=[],
        has_backend=False,
        has_frontend=False,
        backend_dir=None,
        frontend_dir=None,
    )
