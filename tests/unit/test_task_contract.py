from claude_orchestrator.task_contract import DataRisk, TaskContract, TaskInputType, TaskType


def test_task_contract_flags_backup_for_data_risk() -> None:
    contract = TaskContract(
        source_goal="迁移数据库并同步上传文件",
        normalized_goal="迁移数据库并同步上传文件",
        input_type=TaskInputType.NATURAL_LANGUAGE,
        task_type=TaskType.REFACTOR,
        data_risk=DataRisk.BOTH,
        affected_areas=["backend", "storage"],
        document_paths=[],
    )

    assert contract.requires_backup is True
    assert contract.touches_database is True
    assert contract.touches_files is True


def test_task_contract_defaults_to_single_branch_for_small_task() -> None:
    contract = TaskContract(
        source_goal="修复登录接口 500",
        normalized_goal="修复登录接口 500",
        input_type=TaskInputType.NATURAL_LANGUAGE,
        task_type=TaskType.BUGFIX,
        data_risk=DataRisk.NONE,
        affected_areas=["backend"],
        document_paths=[],
    )

    assert contract.estimated_branch_count == 1
