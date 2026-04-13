"""测试重型模块懒加载和 Null 对象行为。"""
from __future__ import annotations

from claude_orchestrator.features import FeatureFlags, is_enabled
from claude_orchestrator.null_objects import (
    NullBlackboard,
    NullDriftDetector,
    NullQuarantine,
)


# ---------- Null 对象行为测试 ----------

def test_null_drift_detector_returns_no_drift():
    det = NullDriftDetector()
    result = det.detect("task1", "some prompt", "some output")
    assert result.drifted is False
    assert result.similarity == 1.0
    assert result.task_id == "task1"


def test_null_blackboard_absorbs_post():
    bb = NullBlackboard()
    bb.post("facts", "key", "value", source_task="t1")  # 不报错
    assert bb.query("facts") == []
    assert bb.query() == []
    assert bb.get_snapshot() == {"facts": [], "hypotheses": [], "intermediate_results": []}


def test_null_blackboard_subscribe_noop():
    bb = NullBlackboard()
    bb.subscribe("facts", lambda e: None)  # 不报错


def test_null_quarantine_pass_through():
    q = NullQuarantine()
    assert q.get_safe_output("task1", "output") == "output"
    assert q.get_safe_output("task1", {"key": "val"}) == {"key": "val"}


def test_null_quarantine_not_quarantined():
    q = NullQuarantine()
    q.quarantine("task1", "some reason")  # 静默吸收
    assert q.is_quarantined("task1") is False
    assert q.get_quarantine_reason("task1") is None
    assert q.get_all_quarantined() == {}


def test_null_quarantine_release_noop():
    q = NullQuarantine()
    q.release("nonexistent")  # 不报错
    q.clear()  # 不报错


# ---------- 特性开关默认值测试 ----------

def test_features_disabled_by_default():
    FeatureFlags.reset()
    assert is_enabled("semantic_drift") is False
    assert is_enabled("blackboard") is False
    assert is_enabled("context_quarantine") is False


def test_features_enabled_via_flags():
    flags = FeatureFlags(semantic_drift=True)
    FeatureFlags.initialize(flags)
    assert is_enabled("semantic_drift") is True
    assert is_enabled("blackboard") is False
    FeatureFlags.reset()


# ---------- 集成测试：Orchestrator 使用 Null 对象 ----------

def test_orchestrator_uses_null_objects_when_disabled():
    """特性关闭时 Orchestrator 应使用 Null 对象。"""
    from claude_orchestrator.null_objects import NullBlackboard, NullDriftDetector, NullQuarantine
    from claude_orchestrator.orchestrator import Orchestrator
    from claude_orchestrator.model import DAG, TaskNode
    from claude_orchestrator.config import Config

    FeatureFlags.reset()

    dag = DAG(name="test", tasks={
        "t1": TaskNode(id="t1", prompt_template="hello"),
    })
    config = Config()
    orch = Orchestrator(dag=dag, config=config)

    assert isinstance(orch._drift_detector, NullDriftDetector)
    assert isinstance(orch._blackboard, NullBlackboard)
    assert isinstance(orch._quarantine, NullQuarantine)


def test_orchestrator_uses_real_objects_when_enabled():
    """特性启用时 Orchestrator 应使用真实对象。"""
    from claude_orchestrator.blackboard import Blackboard
    from claude_orchestrator.context_quarantine import ContextQuarantine
    from claude_orchestrator.semantic_drift import SemanticDriftDetector
    from claude_orchestrator.orchestrator import Orchestrator
    from claude_orchestrator.model import DAG, TaskNode
    from claude_orchestrator.config import Config

    flags = FeatureFlags(
        semantic_drift=True,
        blackboard=True,
        context_quarantine=True,
    )
    FeatureFlags.initialize(flags)

    dag = DAG(name="test", tasks={
        "t1": TaskNode(id="t1", prompt_template="hello"),
    })
    config = Config()
    orch = Orchestrator(dag=dag, config=config)

    assert isinstance(orch._drift_detector, SemanticDriftDetector)
    assert isinstance(orch._blackboard, Blackboard)
    assert isinstance(orch._quarantine, ContextQuarantine)

    FeatureFlags.reset()
