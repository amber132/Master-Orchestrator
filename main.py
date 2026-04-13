import sys
"""Legacy top-level entrypoint kept for editor and script compatibility."""

from master_orchestrator.cli import main as cli_main

def main() -> None:
    sys.exit(cli_main())


if __name__ == "__main__":
    main()
