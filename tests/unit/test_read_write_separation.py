"""测试只读/写操作并发分离功能。"""

from __future__ import annotations

from claude_orchestrator.model import DAG, TaskNode
from claude_orchestrator.scheduler import Scheduler


def _make_dag(tasks: dict[str, TaskNode]) -> DAG:
    return DAG(name="test", tasks=tasks, max_parallel=10)


def test_read_only_tasks_can_exceed_write_parallel_limit():
    """只读任务不受写并发限制，可以更积极地调度。"""
    dag = _make_dag({
        "read_1": TaskNode(id="read_1", prompt_template="scan", read_only=True),
        "read_2": TaskNode(id="read_2", prompt_template="scan", read_only=True),
        "read_3": TaskNode(id="read_3", prompt_template="scan", read_only=True),
        "write_1": TaskNode(id="write_1", prompt_template="fix"),
        "write_2": TaskNode(id="write_2", prompt_template="fix"),
    })
    sched = Scheduler(dag, max_parallel=5, max_write_parallel=1)
    ready = sched.get_ready_tasks()
    read_ready = [t for t in ready if t.read_only]
    write_ready = [t for t in ready if not t.read_only]
    assert len(read_ready) >= 2, "只读任务应至少调度 2 个"
    assert len(write_ready) <= 1, "写任务不超过 max_write_parallel"


def test_write_tasks_serial_when_max_write_is_one():
    """max_write_parallel=1 时写任务串行。"""
    dag = _make_dag({
        "w1": TaskNode(id="w1", prompt_template="fix"),
        "w2": TaskNode(id="w2", prompt_template="fix"),
        "r1": TaskNode(id="r1", prompt_template="scan", read_only=True),
    })
    sched = Scheduler(dag, max_parallel=10, max_write_parallel=1)
    ready = sched.get_ready_tasks()
    write_ready = [t for t in ready if not t.read_only]
    assert len(write_ready) <= 1, "写任务最多 1 个"


def test_task_node_default_not_read_only():
    """TaskNode 默认 read_only=False（向后兼容）。"""
    node = TaskNode(id="x", prompt_template="test")
    assert node.read_only is False
