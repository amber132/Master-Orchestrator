"""Compatibility entrypoint for python -m claude_orchestrator."""

import sys

from master_orchestrator.cli import main

sys.exit(main())
