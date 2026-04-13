from __future__ import annotations

from claude_orchestrator.auto_model import (
    IterationHandoff,
    Phase,
    ReviewResult,
    ReviewVerdict,
    TaskError,
)
from claude_orchestrator.autonomous_helpers import build_correction_feedback, build_review_feedback


def test_build_correction_feedback_combines_outputs_summary_and_errors():
    feedback = build_correction_feedback(
        correction_outputs={"fix-a": {"ok": True}, "fix-b": "done"},
        correction_errors=[TaskError(task_id="fix-c", error="boom", attempt=2)],
        architecture_summary="架构摘要",
    )

    assert "已预执行的确定性补救动作" in feedback
    assert "- `fix-a`" in feedback
    assert "- `fix-b`" in feedback
    assert "当前架构执行状态" in feedback
    assert "架构摘要" in feedback
    assert "预执行补救失败" in feedback
    assert "fix-c" in feedback
    assert "boom" in feedback


def test_build_review_feedback_applies_handoff_correction_strategy_and_learning():
    phase = Phase(
        id="phase-1",
        name="Phase 1",
        description="desc",
        order=1,
        strategy_hint="先缩小修复范围",
        review_result=ReviewResult(
            phase_id="phase-1",
            verdict=ReviewVerdict.MAJOR_ISSUES,
            score=0.61,
            summary="phase review summary",
        ),
    )
    handoff = IterationHandoff(
        iteration=1,
        review_summary="handoff review summary",
        review_score=0.72,
    )

    feedback = build_review_feedback(
        phase,
        handoff=handoff,
        correction_feedback="补救反馈",
        learning_text="\n学习记忆",
        handoff_enabled=True,
        handoff_max_chars=4000,
    )

    assert "策略调整指令" in feedback
    assert "先缩小修复范围" in feedback
    assert "补救反馈" in feedback
    assert "handoff review summary" in feedback
    assert feedback.endswith("学习记忆")
    assert phase.strategy_hint == ""
