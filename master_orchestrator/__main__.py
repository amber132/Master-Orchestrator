"""Allow running as: python -m master_orchestrator"""

import sys

from .cli import main

sys.exit(main())
