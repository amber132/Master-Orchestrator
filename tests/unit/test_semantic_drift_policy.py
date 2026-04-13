from claude_orchestrator.semantic_drift import SemanticDriftDetector



def test_semantic_drift_blocks_scope_tasks() -> None:
    detector = SemanticDriftDetector()
    result = detector.detect(
        'scope_phase_1',
        '# 当前任务\n仅缩小 BatchReplaceService 重构范围，不要扩展到其他模块\n# 执行约束\n- noop',
        '我决定顺手重写多个 controller 与数据库迁移逻辑。',
        task_tags=['phase_scope', 'drift_blocking'],
    )

    assert result.drifted is True
    assert result.blocking is True
    assert result.severity == 'critical'



def test_semantic_drift_warns_for_non_blocking_task() -> None:
    detector = SemanticDriftDetector()
    result = detector.detect(
        'implement_phase_1',
        '# 当前任务\n仅调整命名与职责边界\n# 执行约束\n- noop',
        '额外补充一些说明文档。',
        task_tags=['phase_implement'],
    )

    assert result.drifted is True
    assert result.blocking is False
    assert result.severity == 'warning'
