#!/usr/bin/env sh
set -eu

REPO_DIR="$(CDPATH= cd -- "$(dirname "$0")" && pwd)"
PYTHON_BIN=""

if [ -x "$REPO_DIR/.venv/bin/python" ]; then
  PYTHON_BIN="$REPO_DIR/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python3)"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python)"
else
  echo "python interpreter not found" >&2
  exit 127
fi

PYTHONPATH="$REPO_DIR${PYTHONPATH:+:$PYTHONPATH}" exec "$PYTHON_BIN" -m master_orchestrator "$@"
