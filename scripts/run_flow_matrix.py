from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from master_orchestrator.flow_matrix import run_flow_matrix, summarize_flow_results, write_flow_report


def _print_safe(line: str) -> None:
    payload = (line + "\n").encode(getattr(sys.stdout, "encoding", None) or "utf-8", errors="replace")
    if hasattr(sys.stdout, "buffer"):
        sys.stdout.buffer.write(payload)
        sys.stdout.flush()
    else:
        sys.stdout.write(payload.decode("utf-8", errors="replace"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the master_orchestrator executable flow matrix.")
    parser.add_argument("--flow", action="append", default=[], help="Run only selected flow ids")
    parser.add_argument("--keep-sandboxes", action="store_true", help="Keep generated sandbox directories")
    parser.add_argument("--repo-root", default=str(REPO_ROOT), help="Target repository root to validate")
    parser.add_argument("--python-executable", default=None, help="Python executable used for nested CLI invocations")
    parser.add_argument(
        "--out",
        default="audit_logs/flow-matrix.json",
        help="Path to write the JSON report",
    )
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    requested_python = Path(args.python_executable).resolve() if args.python_executable else None
    venv_python = repo_root / ".venv" / "Scripts" / "python.exe"
    effective_python = (
        str(requested_python)
        if requested_python is not None
        else str(venv_python if venv_python.exists() else Path(sys.executable).resolve())
    )
    out_path = (repo_root / args.out).resolve()
    results = run_flow_matrix(
        repo_root=repo_root,
        audit_root=out_path.parent / "flow-matrix-artifacts",
        python_executable=effective_python,
        selected_flows=set(args.flow or []),
        keep_sandboxes=args.keep_sandboxes,
    )
    write_flow_report(out_path, results)
    summary = summarize_flow_results(results)
    for result in results:
        _print_safe(f"{result.status:7} {result.flow_id} ({result.duration_seconds:.2f}s)")
        if result.notes:
            _print_safe(f"  note: {result.notes}")
    return 0 if summary["gate_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
