"""Executable flow matrix for end-to-end feature verification."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Callable


_BLOCKED_PATTERNS = (
    "AUTH_EXPIRED",
    "Not authenticated",
    "Please login",
    "429",
    "余额不足",
    "无可用资源包",
    "credit",
    "resource package",
    "rate limit",
)

_SIMPLE_PROMPT = "Add exactly one top-level Python comment '# smoke test' at the top of sample.py. Do not change function logic."
_AUTO_PROMPT = "对 sample.py 做一次极小重构：仅在 sample.py 顶部新增一条 Python 注释 '# auto smoke test'，不要改变任何函数逻辑。"


@dataclass
class FlowResult:
    flow_id: str
    title: str
    status: str
    duration_seconds: float
    command: list[str] = field(default_factory=list)
    exit_code: int | None = None
    notes: str = ""
    category: str = ""
    tier: str = "required"
    requires_real_provider: bool = False
    stdout_path: str = ""
    stderr_path: str = ""
    sandbox_dir: str = ""
    artifacts: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class FlowDefinition:
    flow_id: str
    title: str
    category: str
    tier: str
    requires_real_provider: bool
    runner: Callable[["FlowContext"], FlowResult]
    depends_on: list[str] = field(default_factory=list)


@dataclass
class FlowContext:
    repo_root: Path
    audit_root: Path
    python_executable: str
    keep_sandboxes: bool = False
    results: dict[str, FlowResult] = field(default_factory=dict)
    artifacts: dict[str, dict[str, str]] = field(default_factory=dict)

    def remember(self, flow_id: str, **artifacts: str) -> None:
        self.artifacts.setdefault(flow_id, {}).update({k: str(v) for k, v in artifacts.items()})

    def artifact(self, flow_id: str, key: str) -> str | None:
        return self.artifacts.get(flow_id, {}).get(key)

    def cli_script(self, name: str) -> Path:
        scripts_dir = Path(self.python_executable).resolve().parent
        candidates = [name, f"{name}.exe", f"{name}.cmd", f"{name}.bat"]
        for candidate in candidates:
            path = scripts_dir / candidate
            if path.exists():
                return path
        found = shutil.which(name)
        if found:
            return Path(found)
        return Path(name)

    def sandbox(self, flow_id: str) -> Path:
        return Path(tempfile.mkdtemp(prefix=f"mo-flow-{flow_id}-"))


def build_default_flows() -> list[FlowDefinition]:
    return [
        FlowDefinition("import_aliases", "Import package aliases", "entrypoint", "required", False, _flow_import_aliases),
        FlowDefinition("help_master_module", "python -m master_orchestrator --help", "entrypoint", "required", False, _flow_help_master_module),
        FlowDefinition("help_master_script", "master-orchestrator --help", "entrypoint", "required", False, _flow_help_master_script),
        FlowDefinition("help_mo_script", "mo --help", "entrypoint", "required", False, _flow_help_mo_script),
        FlowDefinition("help_claude_compat", "python -m claude_orchestrator.cli --help", "compat", "required", False, _flow_help_claude_compat),
        FlowDefinition("help_codex_compat", "python -m codex_orchestrator.cli --help", "compat", "required", False, _flow_help_codex_compat),
        FlowDefinition("simple_codex_real", "simple codex real smoke", "simple", "nightly", True, _flow_simple_codex_real),
        FlowDefinition("simple_claude_real", "simple claude real smoke", "simple", "nightly", True, _flow_simple_claude_real),
        FlowDefinition("runs_status_after_simple", "runs status after real simple flow", "runs", "nightly", False, _flow_runs_status_after_simple, depends_on=["simple_codex_real"]),
        FlowDefinition("operation_dag_run", "run operation DAG", "operation", "required", False, _flow_operation_dag_run),
        FlowDefinition("runs_graph_after_operation", "runs graph on operation DAG", "runs", "required", False, _flow_runs_graph_after_operation, depends_on=["operation_dag_run"]),
        FlowDefinition("runs_retry_after_operation", "runs retry on failed operation DAG", "runs", "required", False, _flow_runs_retry_after_operation),
        FlowDefinition("auto_default_real", "auto default provider real smoke", "auto", "nightly", True, _flow_auto_default_real),
        FlowDefinition("auto_claude_real", "auto claude provider real smoke", "auto", "nightly", True, _flow_auto_claude_real),
        FlowDefinition("auto_codex_real", "auto codex provider real smoke", "auto", "nightly", True, _flow_auto_codex_real),
        FlowDefinition("auto_mixed_real", "auto mixed phase-provider real smoke", "auto", "nightly", True, _flow_auto_mixed_real),
        FlowDefinition("improve_real", "improve real minimal smoke", "improve", "nightly", True, _flow_improve_real),
    ]


def run_flow_matrix(
    *,
    repo_root: Path,
    audit_root: Path,
    python_executable: str | None = None,
    selected_flows: set[str] | None = None,
    keep_sandboxes: bool = False,
) -> list[FlowResult]:
    audit_root.mkdir(parents=True, exist_ok=True)
    ctx = FlowContext(
        repo_root=repo_root,
        audit_root=audit_root,
        python_executable=python_executable or sys.executable,
        keep_sandboxes=keep_sandboxes,
    )
    results: list[FlowResult] = []
    selected = selected_flows or set()
    for flow in build_default_flows():
        if selected and flow.flow_id not in selected:
            continue
        missing = [dep for dep in flow.depends_on if ctx.results.get(dep, FlowResult(dep, dep, "skipped", 0)).status != "passed"]
        if missing:
            result = FlowResult(
                flow_id=flow.flow_id,
                title=flow.title,
                status="skipped",
                duration_seconds=0.0,
                notes=f"dependency not passed: {', '.join(missing)}",
                category=flow.category,
                tier=flow.tier,
                requires_real_provider=flow.requires_real_provider,
            )
        else:
            result = flow.runner(ctx)
            result.category = flow.category
            result.tier = flow.tier
            result.requires_real_provider = flow.requires_real_provider
        ctx.results[flow.flow_id] = result
        results.append(result)
    return results


def summarize_flow_results(results: list[FlowResult]) -> dict[str, int | bool]:
    summary: dict[str, int | bool] = {
        "passed": 0,
        "failed": 0,
        "blocked": 0,
        "skipped": 0,
        "total": len(results),
        "required_total": 0,
        "nightly_total": 0,
        "required_failed": 0,
        "required_blocked": 0,
        "nightly_failed": 0,
        "nightly_blocked": 0,
        "gate_passed": True,
    }
    for result in results:
        summary[result.status] = int(summary.get(result.status, 0)) + 1
        tier_total_key = f"{result.tier}_total"
        if tier_total_key in summary:
            summary[tier_total_key] = int(summary[tier_total_key]) + 1
        if result.status in {"failed", "blocked"}:
            tier_status_key = f"{result.tier}_{result.status}"
            if tier_status_key in summary:
                summary[tier_status_key] = int(summary[tier_status_key]) + 1

    summary["gate_passed"] = bool(
        int(summary["required_failed"]) == 0 and int(summary["required_blocked"]) == 0
    )
    return summary


def write_flow_report(path: Path, results: list[FlowResult]) -> None:
    summary = summarize_flow_results(results)
    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "summary": summary,
        "flows": [result.to_dict() for result in results],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _flow_import_aliases(ctx: FlowContext) -> FlowResult:
    return _run_command(
        ctx,
        "import_aliases",
        "Import package aliases",
        [ctx.python_executable, "-c", "import master_orchestrator, claude_orchestrator, codex_orchestrator"],
    )


def _flow_help_master_module(ctx: FlowContext) -> FlowResult:
    return _run_command(ctx, "help_master_module", "master module help", [ctx.python_executable, "-m", "master_orchestrator", "--help"])


def _flow_help_master_script(ctx: FlowContext) -> FlowResult:
    return _run_command(ctx, "help_master_script", "master script help", [str(ctx.cli_script("master-orchestrator")), "--help"])


def _flow_help_mo_script(ctx: FlowContext) -> FlowResult:
    return _run_command(ctx, "help_mo_script", "mo script help", [str(ctx.cli_script("mo")), "--help"])


def _flow_help_claude_compat(ctx: FlowContext) -> FlowResult:
    return _run_command(ctx, "help_claude_compat", "claude compat help", [ctx.python_executable, "-m", "claude_orchestrator.cli", "--help"])


def _flow_help_codex_compat(ctx: FlowContext) -> FlowResult:
    return _run_command(ctx, "help_codex_compat", "codex compat help", [ctx.python_executable, "-m", "codex_orchestrator.cli", "--help"])


def _flow_simple_codex_real(ctx: FlowContext) -> FlowResult:
    return _run_simple_flow(ctx, "simple_codex_real", "codex")


def _flow_simple_claude_real(ctx: FlowContext) -> FlowResult:
    return _run_simple_flow(ctx, "simple_claude_real", "claude")


def _flow_runs_status_after_simple(ctx: FlowContext) -> FlowResult:
    run_id = ctx.artifact("simple_codex_real", "run_id")
    if not run_id:
        return FlowResult("runs_status_after_simple", "runs status after simple", "skipped", 0.0, notes="missing simple_codex_real run_id")
    return _run_command(
        ctx,
        "runs_status_after_simple",
        "runs status after simple flow",
        [str(ctx.cli_script("master-orchestrator")), "runs", "--run-id", run_id],
        cwd=ctx.repo_root,
        validator=lambda result: _assert_contains(result["stdout"], run_id, "status output should contain run id"),
    )


def _flow_operation_dag_run(ctx: FlowContext) -> FlowResult:
    sandbox = _create_git_sandbox(ctx, "operation_dag_run")
    dag_path = _write_operation_dag(
        sandbox,
        dag_name="flow-operation-run",
        task_name="touch_marker",
        command="python -c \"from pathlib import Path; Path('marker.txt').write_text('ok', encoding='utf-8')\"",
    )
    result = _run_command(
        ctx,
        "operation_dag_run",
        "run operation dag",
        [str(ctx.cli_script("master-orchestrator")), "run", str(dag_path), "-d", str(sandbox)],
        cwd=ctx.repo_root,
        sandbox_dir=sandbox,
        validator=lambda completed: _assert_file_contains(sandbox / "marker.txt", "ok"),
    )
    if result.status == "passed":
        ctx.remember("operation_dag_run", dag=str(dag_path), sandbox=str(sandbox))
    return result


def _flow_runs_graph_after_operation(ctx: FlowContext) -> FlowResult:
    dag = ctx.artifact("operation_dag_run", "dag")
    sandbox = ctx.artifact("operation_dag_run", "sandbox")
    if not dag or not sandbox:
        return FlowResult("runs_graph_after_operation", "runs graph after operation", "skipped", 0.0, notes="missing operation run artifacts")
    return _run_command(
        ctx,
        "runs_graph_after_operation",
        "runs graph after operation dag",
        [str(ctx.cli_script("master-orchestrator")), "runs", dag, "--graph"],
        cwd=ctx.repo_root,
        validator=lambda result: _assert_contains(result["stdout"], "flow-operation-run", "graph output should contain dag name"),
    )


def _flow_runs_retry_after_operation(ctx: FlowContext) -> FlowResult:
    sandbox = _create_git_sandbox(ctx, "runs_retry_after_operation")
    (sandbox / "retry_mode.txt").write_text("fail", encoding="utf-8")
    dag_path = _write_operation_dag(
        sandbox,
        dag_name="flow-operation-retry",
        task_name="retry_once",
        command=(
            "python -c \"from pathlib import Path; import sys; "
            "mode = Path('retry_mode.txt').read_text(encoding='utf-8').strip(); "
            "sys.exit(1 if mode == 'fail' else 0)\""
        ),
    )
    first = _run_command(
        ctx,
        "runs_retry_after_operation_initial",
        "initial failing operation run",
        [str(ctx.cli_script("master-orchestrator")), "run", str(dag_path), "-d", str(sandbox)],
        cwd=ctx.repo_root,
        allow_failure=True,
    )
    if first.exit_code == 0:
        return FlowResult("runs_retry_after_operation", "runs retry after operation", "failed", first.duration_seconds, notes="initial run unexpectedly succeeded")
    (sandbox / "retry_mode.txt").write_text("pass", encoding="utf-8")
    second = _run_command(
        ctx,
        "runs_retry_after_operation",
        "runs retry after failed operation dag",
        [str(ctx.cli_script("master-orchestrator")), "runs", str(dag_path), "--retry", "-d", str(sandbox)],
        cwd=ctx.repo_root,
        sandbox_dir=sandbox,
        validator=lambda result: _assert_contains(
            result["stdout"] + "\n" + result["stderr"],
            "completed",
            "retry output should end completed",
        ),
    )
    return second


def _flow_auto_default_real(ctx: FlowContext) -> FlowResult:
    return _run_auto_like_flow(ctx, "auto_default_real", [])


def _flow_auto_claude_real(ctx: FlowContext) -> FlowResult:
    return _run_auto_like_flow(ctx, "auto_claude_real", ["--provider", "claude"])


def _flow_auto_codex_real(ctx: FlowContext) -> FlowResult:
    return _run_auto_like_flow(ctx, "auto_codex_real", ["--provider", "codex"])


def _flow_auto_mixed_real(ctx: FlowContext) -> FlowResult:
    return _run_auto_like_flow(
        ctx,
        "auto_mixed_real",
        ["--phase-provider", "execute=codex", "--phase-provider", "review=claude"],
    )


def _flow_improve_real(ctx: FlowContext) -> FlowResult:
    sandbox = _create_git_sandbox(ctx, "improve_real")
    return _run_command(
        ctx,
        "improve_real",
        "improve minimal real smoke",
        [
            str(ctx.cli_script("master-orchestrator")),
            "improve",
            "-d",
            str(sandbox),
            "--max-hours",
            "0.02",
            "--max-iterations",
            "1",
            "--skip-introspection",
            "--skip-external",
            "--auto-approve",
        ],
        cwd=ctx.repo_root,
        sandbox_dir=sandbox,
        real_provider=True,
    )


def _run_simple_flow(ctx: FlowContext, flow_id: str, provider: str) -> FlowResult:
    sandbox = ctx.sandbox(flow_id)
    (sandbox / "sample.py").write_text("def add(a, b):\n    return a + b\n", encoding="utf-8")
    prompt_path = sandbox / "prompt.txt"
    prompt_path.write_text(_SIMPLE_PROMPT, encoding="utf-8")
    result = _run_command(
        ctx,
        flow_id,
        f"simple {provider} real smoke",
        [
            str(ctx.cli_script("master-orchestrator")),
            "do",
            "--mode",
            "simple",
            "--provider",
            provider,
            "-d",
            str(sandbox),
            "--files",
            "sample.py",
            "--prompt-file",
            str(prompt_path),
        ],
        cwd=ctx.repo_root,
        sandbox_dir=sandbox,
        real_provider=True,
        validator=lambda completed: _validate_python_comment_smoke(sandbox / "sample.py", "# smoke test"),
    )
    if result.status == "passed":
        run_id = _parse_run_id(Path(result.stdout_path).read_text(encoding="utf-8", errors="replace"))
        if run_id:
            ctx.remember(flow_id, run_id=run_id, sandbox=str(sandbox))
    return result


def _run_auto_like_flow(ctx: FlowContext, flow_id: str, extra_args: list[str]) -> FlowResult:
    sandbox = _create_git_sandbox(ctx, flow_id)
    result = _run_command(
        ctx,
        flow_id,
        f"{flow_id} real smoke",
        [
            str(ctx.cli_script("master-orchestrator")),
            "do",
            *extra_args,
            "-d",
            str(sandbox),
            "-y",
            _AUTO_PROMPT,
        ],
        cwd=ctx.repo_root,
        timeout=600,
        sandbox_dir=sandbox,
        real_provider=True,
        validator=lambda completed: _validate_auto_comment_smoke(ctx.repo_root, sandbox, "# auto smoke test"),
    )
    return result


def _run_command(
    ctx: FlowContext,
    flow_id: str,
    title: str,
    command: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    timeout: int = 240,
    allow_failure: bool = False,
    real_provider: bool = False,
    sandbox_dir: Path | None = None,
    validator: Callable[[dict], None] | None = None,
) -> FlowResult:
    started = time.perf_counter()
    stdout_path = ctx.audit_root / f"{flow_id}.stdout.log"
    stderr_path = ctx.audit_root / f"{flow_id}.stderr.log"
    merged_env = dict(os.environ)
    if env:
        merged_env.update(env)
    try:
        completed = subprocess.run(
            command,
            cwd=str(cwd) if cwd else str(ctx.repo_root),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            check=False,
            env=merged_env,
        )
        stdout_text = completed.stdout
        stderr_text = completed.stderr
        exit_code = completed.returncode
    except subprocess.TimeoutExpired as exc:
        stdout_text = exc.stdout or ""
        stderr_text = (exc.stderr or "") + f"\nTIMEOUT after {timeout}s"
        exit_code = -1
        completed = None

    duration = round(time.perf_counter() - started, 3)
    stdout_path.write_text(stdout_text, encoding="utf-8", errors="replace")
    stderr_path.write_text(stderr_text, encoding="utf-8", errors="replace")
    status = "passed" if exit_code == 0 else "failed"
    notes = ""
    combined_output = stdout_text + "\n" + stderr_text
    provider_blocked = _looks_blocked(combined_output)
    if exit_code != 0 and real_provider and provider_blocked:
        status = "blocked"
        notes = "provider unavailable or quota/auth blocked"
    elif exit_code == 0 and validator is not None:
        try:
            validator(
                {
                    "stdout": stdout_text,
                    "stderr": stderr_text,
                    "exit_code": exit_code,
                    "sandbox_dir": str(sandbox_dir) if sandbox_dir else "",
                }
            )
        except AssertionError as exc:
            status = "failed"
            notes = str(exc)
    elif exit_code != 0 and not allow_failure:
        notes = combined_output.strip()[:500]

    if allow_failure and exit_code != 0 and status == "failed":
        notes = combined_output.strip()[:500]

    return FlowResult(
        flow_id=flow_id,
        title=title,
        status=status,
        duration_seconds=duration,
        command=command,
        exit_code=exit_code,
        notes=notes,
        stdout_path=str(stdout_path),
        stderr_path=str(stderr_path),
        sandbox_dir=str(sandbox_dir) if sandbox_dir else "",
    )


def _create_git_sandbox(ctx: FlowContext, flow_id: str) -> Path:
    sandbox = ctx.sandbox(flow_id)
    (sandbox / "sample.py").write_text("def add(a, b):\n    return a + b\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q"], cwd=sandbox, check=True)
    subprocess.run(["git", "config", "user.email", "flow@example.com"], cwd=sandbox, check=True)
    subprocess.run(["git", "config", "user.name", "Flow Matrix"], cwd=sandbox, check=True)
    subprocess.run(["git", "add", "sample.py"], cwd=sandbox, check=True)
    subprocess.run(["git", "commit", "-m", "init", "--quiet"], cwd=sandbox, check=True)
    return sandbox


def _write_operation_dag(sandbox: Path, *, dag_name: str, task_name: str, command: str) -> Path:
    dag_path = sandbox / f"{dag_name}.toml"
    dag_path.write_text(
        "\n".join(
            [
                "[dag]",
                f'name = {json.dumps(dag_name, ensure_ascii=False)}',
                "max_parallel = 1",
                "",
                f"[tasks.{task_name}]",
                'prompt = "operation flow"',
                'type = "operation"',
                'output_format = "json"',
                "timeout = 1800",
                "",
                f"[tasks.{task_name}.executor_config]",
                'rollback_refs = ["rollback_marker"]',
                'cutover_gates = []',
                "",
                f"[[tasks.{task_name}.executor_config.commands]]",
                'id = "cmd_1"',
                f"command = {json.dumps(command, ensure_ascii=False)}",
                'evidence_refs = ["marker"]',
            ]
        ),
        encoding="utf-8",
    )
    return dag_path


def _validate_python_comment_smoke(path: Path, expected_comment: str) -> None:
    _assert_file_contains(path, expected_comment)
    subprocess.run([sys.executable, "-m", "py_compile", str(path)], check=True)


def _validate_auto_comment_smoke(repo_root: Path, sandbox: Path, expected_comment: str) -> None:
    sandbox_file = sandbox / "sample.py"
    if sandbox_file.exists() and expected_comment in sandbox_file.read_text(encoding="utf-8", errors="replace"):
        _validate_python_comment_smoke(sandbox_file, expected_comment)
        return

    runtime_root = repo_root / "orchestrator_runs" / sandbox.name
    workspace_candidates = sorted(
        runtime_root.glob("*/workspace/sample.py"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    assert workspace_candidates, f"expected auto workspace delivery under {runtime_root}"

    workspace_file = workspace_candidates[0]
    _validate_python_comment_smoke(workspace_file, expected_comment)

    handoff_candidates = sorted(
        runtime_root.glob("*/handoff/handoff_summary.md"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    assert handoff_candidates, f"expected handoff summary under {runtime_root}"


def _assert_file_contains(path: Path, expected: str) -> None:
    assert path.exists(), f"expected file does not exist: {path}"
    content = path.read_text(encoding="utf-8", errors="replace")
    assert expected in content, f"expected {expected!r} in {path}"


def _assert_contains(text: str, expected: str, message: str) -> None:
    assert expected in text, message


def _looks_blocked(output: str) -> bool:
    lowered = output.lower()
    return any(pattern.lower() in lowered for pattern in _BLOCKED_PATTERNS)


def _parse_run_id(output: str) -> str | None:
    match = re.search(r"^Run:\s+(\S+)", output, re.MULTILINE)
    return match.group(1) if match else None
