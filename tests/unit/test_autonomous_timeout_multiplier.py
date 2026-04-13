from __future__ import annotations

from claude_orchestrator.auto_model import Phase
from claude_orchestrator.autonomous_helpers import apply_phase_timeout_multiplier
from claude_orchestrator.model import DAG, TaskNode


def test_apply_phase_timeout_multiplier_updates_positive_task_timeouts():
    phase = Phase(id="p1", name="Phase 1", description="desc", order=1, timeout_multiplier=1.5)
    dag = DAG(
        name="test",
        tasks={
            "a": TaskNode(id="a", prompt_template="x", timeout=10),
            "b": TaskNode(id="b", prompt_template="x", timeout=0),
        },
    )

    changed = apply_phase_timeout_multiplier(phase, dag)

    assert changed == 1
    assert dag.tasks["a"].timeout == 15
    assert dag.tasks["b"].timeout == 0


def test_apply_phase_timeout_multiplier_skips_when_multiplier_not_greater_than_one():
    phase = Phase(id="p1", name="Phase 1", description="desc", order=1, timeout_multiplier=1.0)
    dag = DAG(
        name="test",
        tasks={"a": TaskNode(id="a", prompt_template="x", timeout=10)},
    )

    changed = apply_phase_timeout_multiplier(phase, dag)

    assert changed == 0
    assert dag.tasks["a"].timeout == 10
