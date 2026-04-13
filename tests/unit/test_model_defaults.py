from claude_orchestrator.auto_model import AutoConfig
from claude_orchestrator.config import (
    DEFAULT_CLAUDE_MODEL,
    ClaudeConfig,
    RequirementConfig,
    SUPPORTED_CLAUDE_MODELS,
)
from claude_orchestrator.validator import TaskNodeValidator


def test_default_model_uses_gpt_5_4_pro() -> None:
    assert DEFAULT_CLAUDE_MODEL == "sonnet"
    assert ClaudeConfig().default_model == DEFAULT_CLAUDE_MODEL
    assert RequirementConfig().assessment_model == DEFAULT_CLAUDE_MODEL
    assert RequirementConfig().question_gen_model == DEFAULT_CLAUDE_MODEL
    assert RequirementConfig().synthesis_model == DEFAULT_CLAUDE_MODEL
    assert AutoConfig().decomposition_model == DEFAULT_CLAUDE_MODEL
    assert AutoConfig().review_model == DEFAULT_CLAUDE_MODEL
    assert AutoConfig().execution_model == DEFAULT_CLAUDE_MODEL


def test_validator_accepts_default_and_legacy_models() -> None:
    for model in SUPPORTED_CLAUDE_MODELS:
        validated = TaskNodeValidator(
            id="task-1",
            prompt_template="hello",
            timeout=30,
            model=model,
            depends_on=[],
        )
        assert validated.model == model
