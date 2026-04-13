from claude_orchestrator.auto_model import AutoConfig, Phase
from claude_orchestrator.dag_generator import DAGGenerator
from claude_orchestrator.task_contract import DataRisk, TaskContract, TaskInputType, TaskType



def test_dag_generator_propagates_refactor_tags_and_guardrails() -> None:
    phase = Phase(
        id='phase_1',
        name='收敛 BatchReplaceService',
        description='desc',
        order=0,
        raw_tasks=[{'id': 'refactor_service', 'prompt': '仅重构 BatchReplaceService', 'depends_on': [], 'tags': ['strict_refactor', 'bounded_slice']}],
        metadata={'target_service_family': 'BatchReplaceService', 'out_of_scope': ['Controller 全量迁移']},
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
    )

    dag = DAGGenerator(AutoConfig()).generate(phase, 'Spring Boot backend', task_contract=contract)
    node = dag.tasks['refactor_service']

    assert 'strict_refactor' in node.tags
    assert '# 严格重构护栏' in node.prompt_template
    assert 'BatchReplaceService' in node.prompt_template
    assert 'Controller 全量迁移' in node.prompt_template
