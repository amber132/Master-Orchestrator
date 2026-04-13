"""日志上下文变量 — 通过 contextvars 注入 run_id / task_id 到日志记录。"""

from contextvars import ContextVar

_run_id_var: ContextVar[str] = ContextVar("run_id", default="")
_task_id_var: ContextVar[str] = ContextVar("task_id", default="")


def set_run_id(rid: str) -> None:
    _run_id_var.set(rid)


def get_run_id() -> str:
    return _run_id_var.get()


def set_task_id(tid: str) -> None:
    _task_id_var.set(tid)


def get_task_id() -> str:
    return _task_id_var.get()
