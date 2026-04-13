from pathlib import Path

from claude_orchestrator.backup_gate import BackupGate
from claude_orchestrator.config import Config
from claude_orchestrator.runtime_layout import RuntimeLayout
from claude_orchestrator.task_contract import DataRisk, TaskContract, TaskInputType, TaskType


def test_backup_gate_with_file_data_end_to_end(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    uploads = repo / "uploads"
    uploads.mkdir(parents=True)
    (uploads / "demo.txt").write_text("demo", encoding="utf-8")

    cfg = Config()
    cfg.backup.file_paths = [str(uploads)]
    layout = RuntimeLayout.create(tmp_path / "runtime")
    contract = TaskContract(
        source_goal="修复上传链路",
        normalized_goal="修复上传链路",
        input_type=TaskInputType.NATURAL_LANGUAGE,
        task_type=TaskType.INTEGRATION,
        data_risk=DataRisk.FILES,
        affected_areas=["backend", "storage"],
        document_paths=[],
    )

    manifest = BackupGate(cfg.backup).run(contract, layout, repo)
    assert manifest.entries
