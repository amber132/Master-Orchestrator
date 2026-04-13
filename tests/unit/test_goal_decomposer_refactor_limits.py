from claude_orchestrator.auto_model import AutoConfig
from claude_orchestrator.config import ClaudeConfig, LimitsConfig
from claude_orchestrator.goal_decomposer import GoalDecomposer
from claude_orchestrator.task_contract import DataRisk, TaskContract, TaskInputType, TaskType


def _contract() -> TaskContract:
    return TaskContract(
        source_goal='持续重构后端',
        normalized_goal='持续重构后端',
        input_type=TaskInputType.NATURAL_LANGUAGE,
        task_type=TaskType.REFACTOR,
        data_risk=DataRisk.NONE,
        affected_areas=['backend'],
        document_paths=[],
        allowed_refactor_roots=['src/main', 'src/test', 'pom.xml'],
    )



def test_goal_decomposer_build_prompt_includes_strict_refactor_rules() -> None:
    decomposer = GoalDecomposer(ClaudeConfig(), LimitsConfig(), AutoConfig())
    prompt = decomposer._build_prompt('持续重构后端', 'Spring Boot backend', _contract())

    assert '严格重构模式' in prompt
    assert 'target_service_family' in prompt
    assert 'out_of_scope' in prompt
    assert '禁止编辑这些状态文件' in prompt



def test_goal_decomposer_truncates_strict_refactor_phase_tasks() -> None:
    decomposer = GoalDecomposer(ClaudeConfig(), LimitsConfig(), AutoConfig())
    phases = decomposer._build_phases(
        {
            'phases': [
                {
                    'id': 'phase_1',
                    'name': '收敛 BatchReplaceService',
                    'description': 'desc',
                    'target_service_family': 'BatchReplaceService',
                    'out_of_scope': ['Controller 大范围改造'],
                    'tasks': [
                        {'id': 't1', 'prompt': '改 service A'},
                        {'id': 't2', 'prompt': '改 service B'},
                        {'id': 't3', 'prompt': '改 service C'},
                    ],
                }
            ]
        },
        _contract(),
    )

    assert len(phases) == 1
    assert len(phases[0].raw_tasks) == 2
    assert phases[0].metadata['strict_refactor_mode'] is True
    assert phases[0].metadata['target_service_family'] == 'BatchReplaceService'
    assert 'strict_refactor' in phases[0].raw_tasks[0]['tags']



def test_goal_decomposer_infers_target_service_family_from_prompt() -> None:
    decomposer = GoalDecomposer(ClaudeConfig(), LimitsConfig(), AutoConfig())
    phases = decomposer._build_phases(
        {
            'phases': [
                {
                    'id': 'phase_1',
                    'name': '收敛实现',
                    'description': 'desc',
                    'tasks': [
                        {'id': 't1', 'prompt': '仅重构 BatchReplaceService 的职责边界'},
                    ],
                }
            ]
        },
        _contract(),
    )

    assert phases[0].metadata['target_service_family'] == 'BatchReplaceService'
