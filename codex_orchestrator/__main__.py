"""Compatibility entrypoint for python -m codex_orchestrator."""

import sys

from master_orchestrator.cli import main

sys.exit(main())
