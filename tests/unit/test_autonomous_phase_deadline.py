from __future__ import annotations

from datetime import datetime, timedelta

from claude_orchestrator.autonomous import AutonomousController
from claude_orchestrator.auto_model import GoalState, Phase


def _controller_with_deadline(deadline: datetime) -> AutonomousController:
    controller = AutonomousController.__new__(AutonomousController)
    controller._state = GoalState(deadline=deadline)
    return controller


def test_check_phase_deadline_stops_overdue_phase() -> None:
    controller = _controller_with_deadline(datetime.now() - timedelta(seconds=1))
    phase = Phase(id="phase_1", name="phase", description="desc", order=0)

    should_stop, reason = controller._check_phase_deadline(phase)

    assert should_stop is True
    assert "截止时间" in reason


def test_check_phase_deadline_allows_active_phase_before_deadline() -> None:
    controller = _controller_with_deadline(datetime.now() + timedelta(minutes=5))
    phase = Phase(id="phase_1", name="phase", description="desc", order=0)

    should_stop, reason = controller._check_phase_deadline(phase)

    assert should_stop is False
    assert reason == ""
