from __future__ import annotations

from claude_orchestrator.auto_model import AutoConfig, GoalState, IterationRecord, Phase, ReviewVerdict
from claude_orchestrator.convergence import ConvergenceDetector


def _phase() -> Phase:
    return Phase(
        id="phase_1",
        name="stabilize",
        description="desc",
        order=0,
    )


def test_plateau_detects_zero_net_progress_with_zigzag_scores() -> None:
    phase = _phase()
    state = GoalState(
        phases=[phase],
        total_iterations=4,
        iteration_history=[
            IterationRecord(iteration=1, phase_id=phase.id, score=0.82, verdict=ReviewVerdict.PASS),
            IterationRecord(iteration=2, phase_id=phase.id, score=0.62, verdict=ReviewVerdict.MAJOR_ISSUES),
            IterationRecord(iteration=3, phase_id=phase.id, score=0.72, verdict=ReviewVerdict.MINOR_ISSUES),
            IterationRecord(iteration=4, phase_id=phase.id, score=0.82, verdict=ReviewVerdict.PASS),
        ],
    )
    detector = ConvergenceDetector(AutoConfig(convergence_window=4, score_improvement_min=0.05))

    signal = detector._check_plateau(state, phase)

    assert signal.should_stop is True
    assert signal.details["sub_trigger"] == "trend_plateau"


def test_plateau_does_not_trigger_for_clear_upward_progress() -> None:
    phase = _phase()
    state = GoalState(
        phases=[phase],
        total_iterations=4,
        iteration_history=[
            IterationRecord(iteration=1, phase_id=phase.id, score=0.35, verdict=ReviewVerdict.MAJOR_ISSUES),
            IterationRecord(iteration=2, phase_id=phase.id, score=0.48, verdict=ReviewVerdict.MAJOR_ISSUES),
            IterationRecord(iteration=3, phase_id=phase.id, score=0.63, verdict=ReviewVerdict.MINOR_ISSUES),
            IterationRecord(iteration=4, phase_id=phase.id, score=0.79, verdict=ReviewVerdict.MINOR_ISSUES),
        ],
    )
    detector = ConvergenceDetector(AutoConfig(convergence_window=4, score_improvement_min=0.05))

    signal = detector._check_plateau(state, phase)

    assert signal.should_stop is False
