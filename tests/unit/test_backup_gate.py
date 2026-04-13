from pathlib import Path

import pytest

from claude_orchestrator.backup_gate import BackupGate, BackupGateError
from claude_orchestrator.backup_manifest import BackupResourceType
from claude_orchestrator.config import Config
from claude_orchestrator.runtime_layout import RuntimeLayout
from claude_orchestrator.task_contract import DataRisk, TaskContract, TaskInputType, TaskType


def test_backup_gate_copies_file_data(tmp_path: Path) -> None:
    uploads = tmp_path / "uploads"
    uploads.mkdir()
    (uploads / "a.txt").write_text("hello", encoding="utf-8")

    cfg = Config()
    cfg.backup.file_paths = [str(uploads)]
    layout = RuntimeLayout.create(tmp_path / "runtime")
    contract = TaskContract(
        source_goal="修复上传后处理逻辑",
        normalized_goal="修复上传后处理逻辑",
        input_type=TaskInputType.NATURAL_LANGUAGE,
        task_type=TaskType.BUGFIX,
        data_risk=DataRisk.FILES,
        affected_areas=["backend", "storage"],
        document_paths=[],
    )

    manifest = BackupGate(cfg.backup).run(contract, layout, tmp_path)

    assert manifest.entries
    assert manifest.entries[0].resource_type is BackupResourceType.FILES
    assert Path(manifest.entries[0].backup_path).exists()


def test_backup_gate_fails_closed_without_db_command(tmp_path: Path) -> None:
    cfg = Config()
    layout = RuntimeLayout.create(tmp_path / "runtime")
    contract = TaskContract(
        source_goal="执行数据库迁移",
        normalized_goal="执行数据库迁移",
        input_type=TaskInputType.NATURAL_LANGUAGE,
        task_type=TaskType.REFACTOR,
        data_risk=DataRisk.DATABASE,
        affected_areas=["backend", "database"],
        document_paths=[],
    )

    with pytest.raises(BackupGateError):
        BackupGate(cfg.backup).run(contract, layout, tmp_path)



def test_backup_gate_uses_contract_database_backup_commands(tmp_path: Path) -> None:
    cfg = Config()
    layout = RuntimeLayout.create(tmp_path / "runtime")
    contract = TaskContract(
        source_goal="执行数据库迁移",
        normalized_goal="执行数据库迁移",
        input_type=TaskInputType.NATURAL_LANGUAGE,
        task_type=TaskType.REFACTOR,
        data_risk=DataRisk.DATABASE,
        affected_areas=["backend", "database"],
        document_paths=[],
        metadata={
            "database_backup_commands": [
                "python -c \"from pathlib import Path; Path(r'{output}').write_text('ok', encoding='utf-8')\""
            ]
        },
    )

    manifest = BackupGate(cfg.backup).run(contract, layout, tmp_path)

    assert manifest.entries
    assert Path(manifest.entries[0].backup_path).exists()
