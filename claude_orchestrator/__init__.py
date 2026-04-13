from __future__ import annotations

import importlib
import pkgutil
import sys
from pathlib import Path

_MASTER_ROOT = Path(__file__).resolve().parents[1] / "master_orchestrator"
__path__ = [str(_MASTER_ROOT)]

import master_orchestrator as _master_orchestrator

from master_orchestrator import *  # noqa: F401,F403

__all__ = getattr(_master_orchestrator, "__all__", [])

for module_info in pkgutil.iter_modules([str(_MASTER_ROOT)]):
    if module_info.name.startswith("__") or module_info.name in {"cli"}:
        continue
    module = importlib.import_module(f"master_orchestrator.{module_info.name}")
    sys.modules.setdefault(f"{__name__}.{module_info.name}", module)
    globals().setdefault(module_info.name, module)
