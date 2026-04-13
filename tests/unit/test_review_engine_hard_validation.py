import subprocess
from pathlib import Path

from claude_orchestrator.auto_model import AutoConfig, Phase
from claude_orchestrator.config import ClaudeConfig, LimitsConfig
from claude_orchestrator.review_engine import ReviewEngine
from claude_orchestrator.task_contract import DataRisk, TaskContract, TaskInputType, TaskType
from claude_orchestrator.verification_planner import VerificationCommand, VerificationPlan



def test_review_engine_flags_strict_refactor_boundary_breaks(tmp_path: Path) -> None:
    repo = tmp_path / 'repo'
    (repo / 'src/main/java/com/example/service').mkdir(parents=True)
    (repo / 'src/main/java/com/example/controller').mkdir(parents=True)
    (repo / 'src/test/java/com/example').mkdir(parents=True)
    (repo / 'pom.xml').write_text('<project></project>', encoding='utf-8')
    (repo / 'src/main/java/com/example/service/BatchReplaceService.java').write_text('class BatchReplaceService {}', encoding='utf-8')
    (repo / 'src/main/java/com/example/controller/BatchReplaceController.java').write_text('class BatchReplaceController {}', encoding='utf-8')
    (repo / 'goal_state.json').write_text('{}', encoding='utf-8')

    subprocess.run(['git', 'init'], cwd=repo, check=True, capture_output=True)

    engine = ReviewEngine(ClaudeConfig(), LimitsConfig(), AutoConfig(), working_dir=str(repo))
    phase = Phase(
        id='phase_1',
        name='收敛 BatchReplaceService',
        description='desc',
        order=0,
        metadata={'target_service_family': 'BatchReplaceService'},
    )
    contract = TaskContract(
        source_goal='持续重构后端',
        normalized_goal='持续重构后端',
        input_type=TaskInputType.NATURAL_LANGUAGE,
        task_type=TaskType.REFACTOR,
        data_risk=DataRisk.NONE,
        affected_areas=['backend'],
        document_paths=[],
        allowed_refactor_roots=['src/main', 'src/test', 'pom.xml'],
        max_prod_files_per_iteration=1,
    )
    plan = VerificationPlan(commands=[
        VerificationCommand(name='compile', command='echo ok', cwd=str(repo)),
    ])

    ok, issues = engine._run_hard_validation(phase, {}, task_contract=contract, verification_plan=plan)

    assert ok is False
    assert any('状态文件' in issue for issue in issues)
    assert any('生产代码改动过大' in issue for issue in issues)
    assert any('测试或验证代码改动' in issue for issue in issues)



def test_review_engine_scopes_prod_file_count_to_current_phase_slice(tmp_path: Path) -> None:
    repo = tmp_path / 'repo2'
    (repo / 'src/main/java/com/example/service').mkdir(parents=True)
    (repo / 'src/main/java/com/example/other').mkdir(parents=True)
    (repo / 'src/test/java/com/example').mkdir(parents=True)
    (repo / 'pom.xml').write_text('<project></project>', encoding='utf-8')
    (repo / 'src/main/java/com/example/service/BatchReplaceService.java').write_text('class BatchReplaceService {}', encoding='utf-8')
    (repo / 'src/main/java/com/example/other/UnrelatedService.java').write_text('class UnrelatedService {}', encoding='utf-8')
    (repo / 'src/test/java/com/example/BatchReplaceServiceTest.java').write_text('class BatchReplaceServiceTest {}', encoding='utf-8')

    subprocess.run(['git', 'init'], cwd=repo, check=True, capture_output=True)

    engine = ReviewEngine(ClaudeConfig(), LimitsConfig(), AutoConfig(), working_dir=str(repo))
    phase = Phase(
        id='phase_1',
        name='收敛 BatchReplaceService',
        description='desc',
        order=0,
        metadata={'target_service_family': 'BatchReplaceService'},
    )
    contract = TaskContract(
        source_goal='持续重构后端',
        normalized_goal='持续重构后端',
        input_type=TaskInputType.NATURAL_LANGUAGE,
        task_type=TaskType.REFACTOR,
        data_risk=DataRisk.NONE,
        affected_areas=['backend'],
        document_paths=[],
        allowed_refactor_roots=['src/main', 'src/test', 'pom.xml'],
        max_prod_files_per_iteration=1,
    )
    plan = VerificationPlan(commands=[VerificationCommand(name='compile', command='echo ok', cwd=str(repo))])
    task_outputs = {
        'refactor_service': '修改 src/main/java/com/example/service/BatchReplaceService.java 并更新 src/test/java/com/example/BatchReplaceServiceTest.java'
    }

    ok, issues = engine._run_hard_validation(phase, task_outputs, task_contract=contract, verification_plan=plan)

    assert ok is True
    assert not any('生产代码改动过大' in issue for issue in issues)
