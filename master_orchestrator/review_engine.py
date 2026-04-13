"""Review engine: call Claude to review phase results and produce structured feedback."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import py_compile
import re
import subprocess
from pathlib import Path
from typing import Any

from .architecture_contract import ArchitectureContract
from .agent_cli import run_agent_task
from .auto_model import (
    AutoConfig,
    CorrectiveAction,
    Phase,
    ReviewIssue,
    ReviewResult,
    ReviewVerdict,
)
from .claude_cli import BudgetTracker, run_claude_task
from .config import ClaudeConfig, LimitsConfig
from .json_utils import robust_parse_json
from .model import TaskNode, TaskResult
from .repo_profile import RepoProfile
from .task_contract import TaskContract
from .verification_planner import VerificationPlan

logger = logging.getLogger(__name__)
_RUNTIME_SESSION_FILE = "runtime_session.json"
_IGNORED_DIFF_DIRS = {
    ".git",
    ".gradle",
    ".idea",
    ".orchestrator_spill",
    ".pytest_cache",
    "__pycache__",
    "build",
    "dist",
    "node_modules",
    "target",
}
_VERDICT_MAP = {
    "passed": "pass", "success": "pass", "ok": "pass", "approved": "pass",
    "fail": "major_issues", "failed": "major_issues", "rejected": "major_issues",
    "warning": "minor_issues", "needs_work": "minor_issues",
    "needs_improvement": "minor_issues", "partial": "minor_issues",
    "partial_pass": "minor_issues",
    "通过": "pass", "已通过": "pass",
    "未通过": "major_issues", "不通过": "major_issues",
    "部分通过": "minor_issues",
}
_DEFAULT_SCORES = {
    "pass": 0.9,
    "minor_issues": 0.7,
    "major_issues": 0.45,
    "critical": 0.2,
    "blocked": 0.1,
}
_VERDICT_SCORE_RANGES = {
    "pass": (0.8, 1.0),
    "minor_issues": (0.6, 0.8),
    "major_issues": (0.3, 0.6),
    "critical": (0.0, 0.3),
    "blocked": (0.0, 0.2),
}


_REVIEW_SYSTEM_PROMPT = """\
你是一个严格的代码审查专家。你的任务是审查一个阶段的执行结果，判断是否达到验收标准。

你可以使用工具来实际检查工作目录中的文件，验证代码是否正确。

## 评分锚定标准（最高优先级，必须严格遵守）

严禁将所有评分集中在某一区间。必须根据实际质量给出分布在合理区间的评分。

| 区间 | 名称 | 含义 | 典型场景 |
|------|------|------|----------|
| 0.0-0.3 | 严重缺陷 | 根本性设计错误、核心功能缺失、破坏性回归、无法通过基本验证 | 核心模块未实现；关键API完全不可用；引入严重回归 |
| 0.3-0.6 | 部分满足 | 部分验收标准已满足，但存在明显功能缺陷或设计不足，需实质性返工 | 3个验收标准仅满足1个；已实现功能但有严重bug；架构方向正确但实现不完整 |
| 0.6-0.8 | 基本满足 | 核心功能正确实现，验收标准大部分满足，仅剩非关键性小问题 | 3个验收标准满足2个且第3个仅差细节；功能正确但缺少边界处理 |
| 0.8-1.0 | 完全满足 | 全部验收标准满足，代码质量高，无遗留问题 | 所有验收标准逐一通过；代码整洁无异味；测试覆盖充分 |

### 具体评分示例
- 0.15：核心功能完全缺失，代码无法运行
- 0.25：有代码但关键路径存在根本性设计错误
- 0.40：约一半验收标准满足，另一半存在实质缺陷
- 0.55：大部分功能框架已搭好，但关键细节缺失导致无法通过验证
- 0.65：核心功能正确，2-3个非关键问题需要修复
- 0.75：验收标准基本满足，仅剩代码风格或边界case问题
- 0.85：全部验收标准满足，代码质量好，可能有极微小改进空间
- 0.95：完美实现，无任何可挑剔之处

### 反集中化规则
- 禁止无理由给出 0.45-0.55 范围的"安全分"
- 禁止所有审查都给出相似分数（如连续多次 0.60-0.70）
- 如果质量有明显差异，评分必须有明显区分度（≥0.1的差距）
- 先判断落入哪个区间，再在区间内精确定位具体分数

## verdict 规则（与锚定区间严格对齐）
- "pass": score >= 0.8，完全满足区间
- "minor_issues": score >= 0.6 且 < 0.8，基本满足区间
- "major_issues": score >= 0.3 且 < 0.6，部分满足区间
- "critical": score < 0.3，严重缺陷区间
- "blocked": 无法继续，需要人工介入

## score_rationale（强制要求）
你必须在 score_rationale 字段中明确说明：
1. 评分落入的区间名称和范围
2. 支撑该区间的关键事实（逐条列出验收标准的满足情况）
3. 如果上一轮有审查反馈，必须说明本轮相对上轮的变化

格式："score_rationale": "[区间名](区间范围) - 事实1；事实2；事实3"
例如："score_rationale": "基本满足(0.6-0.8) - 验收标准1(收敛检测)已满足；验收标准2(CV集成)部分完成但_calc_cv仍为死代码；验收标准3(评分锚定)已满足"

【重要】你的回复必须是且仅是一个 JSON 对象，不要包含任何其他文字、解释或 markdown 代码块。
JSON 对象必须严格使用以下 key 名（不要使用 status/stage/checklist/recommendation 等替代名称）：

{
  "verdict": "pass|minor_issues|major_issues|critical|blocked",
  "score": 0.85,
  "summary": "审查总结",
  "score_rationale": "[区间名](区间范围) - 事实1；事实2",
  "issues": [
    {
      "severity": "minor|major|critical",
      "category": "correctness|security|performance|style|completeness",
      "description": "问题描述",
      "affected_files": ["file1.py"],
      "suggested_fix": "修复建议"
    }
  ],
  "corrective_actions": [
    {
      "action_id": "fix_xxx",
      "description": "修正描述",
      "prompt_template": "具体的修正指令，要详细到 Claude 可以直接执行",
      "priority": 1,
      "depends_on_actions": [],
      "timeout": 1800,
      "action_type": "claude_cli|operation",
      "executor_config": {}
    }
  ]
}

再次强调：只输出 JSON，不要输出任何其他内容。key 名必须是 verdict、score、summary、score_rationale、issues、corrective_actions。score_rationale 必须包含区间名、范围和逐条事实。
"""


class ReviewEngine:
    """Reviews phase execution results using Claude."""

    def __init__(
        self,
        claude_config: ClaudeConfig,
        limits_config: LimitsConfig,
        auto_config: AutoConfig,
        budget_tracker: BudgetTracker | None = None,
        working_dir: str | None = None,
        provider_config: Any | None = None,
        preferred_provider: str = "auto",
        phase_provider_overrides: dict[str, str] | None = None,
    ):
        self._claude_config = claude_config
        self._limits = limits_config
        self._auto_config = auto_config
        self._budget = budget_tracker
        self._working_dir = working_dir
        self._output_truncate_chars = 4000
        self._provider_config = provider_config
        self._preferred_provider = preferred_provider
        self._phase_provider_overrides = dict(phase_provider_overrides or {})

    def review_phase(
        self,
        phase: Phase,
        goal_text: str,
        task_outputs: dict[str, Any],
        task_results: dict[str, TaskResult] | None = None,
        task_contract: TaskContract | None = None,
        repo_profile: RepoProfile | None = None,
        verification_plan: VerificationPlan | None = None,
        architecture_contract: ArchitectureContract | None = None,
    ) -> ReviewResult:
        """Review a completed phase and return structured feedback."""
        hard_ok, hard_issues = self._run_hard_validation(
            phase,
            task_outputs,
            task_contract=task_contract,
            repo_profile=repo_profile,
            verification_plan=verification_plan,
            architecture_contract=architecture_contract,
        )

        prompt = self._build_review_prompt(phase, goal_text, task_outputs, architecture_contract)
        if hard_issues:
            prompt += (
                "\n\n# 自动验证结果\n"
                "以下问题由自动验证发现，请在审查中重点关注：\n"
                + "\n".join(f"- {issue}" for issue in hard_issues)
            )

        task_node = TaskNode(
            id="_review",
            prompt_template=prompt,
            timeout=900,
            model=self._auto_config.review_model,
            output_format="text",
            system_prompt=None,
            provider=self._preferred_provider,
            type="agent_cli",
            executor_config={"phase": "review"},
        )

        if self._provider_config is not None:
            result = run_agent_task(
                task=task_node,
                prompt=prompt,
                config=self._provider_config,
                limits=self._limits,
                budget_tracker=self._budget,
                working_dir=self._working_dir,
                on_progress=None,
                cli_provider=self._preferred_provider if self._preferred_provider in {"claude", "codex"} else None,
                phase_provider_overrides=self._phase_provider_overrides,
            )
        else:
            result = run_claude_task(
                task=task_node,
                prompt=prompt,
                claude_config=self._claude_config,
                limits=self._limits,
                budget_tracker=self._budget,
                working_dir=self._working_dir,
            )

        if not result.status or result.status.value != "success":
            logger.error("审查调用失败: %s", result.error)
            return ReviewResult(
                phase_id=phase.id,
                verdict=ReviewVerdict.MAJOR_ISSUES,
                score=0.5,
                summary=f"审查调用失败: {result.error or 'unknown error'}",
            )

        raw_output = result.output or ""
        review = self._parse_review(phase.id, raw_output)
        if not hard_ok:
            review.score = min(review.score, 0.59)
            if review.verdict == ReviewVerdict.PASS:
                review.verdict = ReviewVerdict.MAJOR_ISSUES
            review.summary = f"[硬验证失败] {'; '.join(hard_issues[:3])}\n{review.summary}"
            logger.warning("硬验证失败，审查结果降级: score<=0.59, verdict=%s", review.verdict.value)
        return review

    def _run_hard_validation(
        self,
        phase: Phase,
        task_outputs: dict[str, Any],
        task_contract: TaskContract | None = None,
        repo_profile: RepoProfile | None = None,
        verification_plan: VerificationPlan | None = None,
        architecture_contract: ArchitectureContract | None = None,
    ) -> tuple[bool, list[str]]:
        """执行硬验证：文件存在性、Git 冲突、严格重构边界。"""
        issues: list[str] = []
        strict = bool(task_contract and task_contract.strict_refactor_mode)
        state_patterns = set((task_contract.state_file_patterns if task_contract else []) or [])
        mentioned_files = self._extract_mentioned_files(task_outputs, state_patterns)

        if strict and verification_plan is not None and not verification_plan.commands:
            issues.append("严格重构缺少自动验证计划，无法形成确定性闭环")
        if architecture_contract is not None and verification_plan is not None:
            planned_commands = {item.command for item in verification_plan.commands}
            missing_obligations = [
                item.description
                for item in architecture_contract.verification_obligations
                if item.blocking and item.command_hint and item.command_hint not in planned_commands
            ]
            if missing_obligations:
                issues.append(f"缺少架构验证义务: {'; '.join(missing_obligations[:3])}")
        architecture_issues = self._validate_architecture_playbook_reporting(phase, task_outputs)
        issues.extend(architecture_issues)

        changed_files = self._git_changed_files()

        for fpath in mentioned_files:
            full_path = self._to_full_path(fpath, candidate_paths=changed_files)
            if not os.path.exists(full_path):
                issues.append(f"声称修改的文件不存在: {fpath}")

        for fpath in mentioned_files:
            if not fpath.endswith('.py'):
                continue
            full_path = self._to_full_path(fpath, candidate_paths=changed_files)
            if not os.path.exists(full_path):
                continue
            try:
                py_compile.compile(full_path, doraise=True)
            except py_compile.PyCompileError as exc:
                issues.append(f"Python 语法错误: {fpath}: {str(exc)[:200]}")

        issues.extend(self._validate_windows_command_wrappers(mentioned_files, changed_files))
        if strict:
            state_hits = [path for path in changed_files if self._is_state_file(path, state_patterns)]
            if task_contract and task_contract.forbid_state_file_edits and state_hits:
                issues.append(f"严格重构禁止修改编排器状态文件: {', '.join(state_hits[:5])}")

            phase_files = self._select_phase_candidate_files(
                mentioned_files=mentioned_files,
                changed_files=changed_files,
                phase=phase,
            )
            prod_files = [path for path in phase_files if self._is_prod_file(path)]
            if task_contract and task_contract.allowed_refactor_roots:
                outside = [path for path in prod_files if not self._is_allowed_path(path, task_contract.allowed_refactor_roots)]
                if outside:
                    issues.append(f"存在超出允许根路径的生产代码修改: {', '.join(outside[:5])}")

            if task_contract and task_contract.max_prod_files_per_iteration and len(prod_files) > task_contract.max_prod_files_per_iteration:
                issues.append(
                    f"严格重构本轮生产代码改动过大: {len(prod_files)} 个文件，超过上限 {task_contract.max_prod_files_per_iteration}"
                )

            if (
                task_contract
                and task_contract.require_guardrail_tests_before_service_moves
                and any('/service/' in path.replace('\\', '/').lower() or path.endswith('Service.java') for path in prod_files)
                and not any(self._is_test_file(path) for path in phase_files)
            ):
                issues.append("严格重构要求服务层改动至少伴随测试或验证代码改动")

            target_family = str(phase.metadata.get('target_service_family', '') or '').strip()
            if target_family:
                family_aliases = self._target_family_aliases(target_family)
                family_hits = {
                    Path(path).stem.replace('Test', '').replace('Tests', '')
                    for path in prod_files
                    if not any(alias in path.lower() for alias in family_aliases)
                    and any(token in path.lower() for token in ('service', 'controller'))
                }
                if len(family_hits) > max(0, (task_contract.max_service_families_per_phase if task_contract else 1) - 1):
                    issues.append("本阶段出现额外服务族/控制器族改动，疑似超出单切片重构范围")

        if self._working_dir:
            try:
                proc = subprocess.run(
                    ["git", "diff", "--check"],
                    cwd=self._working_dir,
                    capture_output=True,
                    text=True,
                    timeout=30,
                    encoding="utf-8",
                    errors="replace",
                )
                if proc.returncode != 0 and proc.stdout.strip():
                    conflict_lines = proc.stdout.strip().split('\n')[:5]
                    issues.append(f"Git 冲突标记检测: {'; '.join(conflict_lines)}")
            except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as exc:
                logger.debug("Git 冲突标记检测跳过: %s", exc)

        all_ok = len(issues) == 0
        if not all_ok:
            logger.warning("硬验证发现 %d 个问题: %s", len(issues), issues[:3])
        return all_ok, issues

    def _validate_architecture_playbook_reporting(
        self,
        phase: Phase,
        task_outputs: dict[str, Any],
    ) -> list[str]:
        metadata = phase.metadata or {}
        playbook_steps = metadata.get("architecture_playbook_steps") or []
        if not playbook_steps or not task_outputs:
            return []

        combined_text = "\n".join(str(output) for output in task_outputs.values() if output).lower()
        if not combined_text.strip():
            return []

        issues: list[str] = []
        required_labels = ["evidencerefs:", "rollbackrefs:"]
        cutover_gates = metadata.get("architecture_cutover_gates") or []
        if cutover_gates:
            required_labels.append("unmetcutovergates:")
        missing_labels = [label for label in required_labels if label not in combined_text]
        if missing_labels:
            issues.append(f"架构 playbook 交付缺少结构化回报字段: {', '.join(missing_labels)}")

        rollback_ids: list[str] = []
        evidence_refs: list[str] = []
        for step in playbook_steps:
            rollback_ids.extend(str(item).strip() for item in step.get("rollback_action_ids", []) if str(item).strip())
            evidence_refs.extend(str(item).strip() for item in step.get("evidence_required", []) if str(item).strip())

        missing_rollback_ids = [
            item for item in dict.fromkeys(rollback_ids)
            if item and item.lower() not in combined_text
        ]
        if missing_rollback_ids:
            issues.append(f"缺少架构回滚动作引用: {', '.join(missing_rollback_ids[:4])}")

        missing_evidence_refs = [
            item for item in list(dict.fromkeys(evidence_refs))[:6]
            if item and item.lower() not in combined_text
        ]
        if missing_evidence_refs:
            issues.append(f"缺少架构证据引用: {', '.join(missing_evidence_refs[:4])}")
        return issues

    def _extract_mentioned_files(self, task_outputs: dict[str, Any], state_patterns: set[str]) -> set[str]:
        file_pattern = re.compile(
            r'(?:修改|创建|编辑|写入|更新|changed?|modified?|created?|wrote|updated?)\s+[`"\']?([^\s`"\']+\.\w{1,10})[`"\']?',
            re.IGNORECASE,
        )
        mentioned_files: set[str] = set()
        for output in task_outputs.values():
            text = str(output) if output else ""
            for match in file_pattern.findall(text):
                normalized = self._normalize_reported_path(match)
                if not normalized:
                    continue
                if self._is_state_file(normalized, state_patterns):
                    continue
                mentioned_files.add(normalized)
        return mentioned_files

    def _select_phase_candidate_files(
        self,
        mentioned_files: set[str],
        changed_files: list[str],
        phase: Phase,
    ) -> list[str]:
        target_family = str(phase.metadata.get('target_service_family', '') or '').strip()
        family_aliases = self._target_family_aliases(target_family) if target_family else set()
        selected: list[str] = []
        seen: set[str] = set()

        def _add(path: str) -> None:
            normalized = path.replace('\\', '/').strip()
            if not normalized or normalized in seen:
                return
            seen.add(normalized)
            selected.append(normalized)

        for path in sorted(mentioned_files):
            _add(path)

        if family_aliases:
            for path in changed_files:
                normalized = path.replace('\\', '/').lower()
                if any(alias in normalized for alias in family_aliases):
                    _add(path)

        if selected:
            return selected
        return list(dict.fromkeys(path.replace('\\', '/') for path in changed_files))

    def _target_family_aliases(self, target_family: str) -> set[str]:
        lowered = target_family.lower().strip()
        aliases = {lowered}
        base = re.sub(r'(service|controller|test|tests)$', '', lowered)
        if base:
            aliases.add(base)
        return {alias for alias in aliases if alias}

    def _git_changed_files(self) -> list[str]:
        status_entries = self._git_status_entries()
        source_diff = self._diff_against_materialized_project_root()
        if status_entries:
            if source_diff is not None and all(status == "??" for status, _ in status_entries):
                return source_diff
            return [path for _, path in status_entries]
        return source_diff or []

    def _git_status_entries(self) -> list[tuple[str, str]]:
        if not self._working_dir:
            return []
        try:
            proc = subprocess.run(
                ["git", "status", "--short", "--untracked-files=all"],
                cwd=self._working_dir,
                capture_output=True,
                text=True,
                timeout=30,
                encoding="utf-8",
                errors="replace",
            )
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as exc:
            logger.debug("获取 changed files 跳过: %s", exc)
            return []

        if proc.returncode != 0:
            return []

        changed: list[tuple[str, str]] = []
        for line in proc.stdout.splitlines():
            if not line.strip():
                continue
            status = line[:2]
            path = line[3:].strip().replace('\\', '/')
            if ' -> ' in path:
                path = path.split(' -> ', 1)[1].strip()
            changed.append((status, path))
        return changed

    def _diff_against_materialized_project_root(self) -> list[str] | None:
        metadata = self._load_runtime_session_metadata()
        if not metadata:
            return None

        source_repo_raw = str(metadata.get("source_repo") or "").strip()
        project_root_raw = str(metadata.get("project_root") or source_repo_raw).strip()
        project_relative = str(metadata.get("project_relative_path") or ".").strip()
        if not source_repo_raw or not project_root_raw or project_relative in {"", "."}:
            return None

        source_repo = Path(source_repo_raw).resolve()
        project_root = Path(project_root_raw).resolve()
        working_dir = Path(self._working_dir or "").resolve()
        if project_root == source_repo or not project_root.exists() or not working_dir.exists():
            return None
        if self._project_subtree_is_tracked(source_repo, project_relative):
            return None

        source_snapshot = self._snapshot_directory(project_root)
        working_snapshot = self._snapshot_directory(working_dir)
        changed = sorted(
            path
            for path in set(source_snapshot) | set(working_snapshot)
            if source_snapshot.get(path) != working_snapshot.get(path)
        )
        return changed

    def _load_runtime_session_metadata(self) -> dict[str, Any] | None:
        if not self._working_dir:
            return None
        working_dir = Path(self._working_dir).resolve()
        for candidate in [working_dir, *working_dir.parents]:
            session_file = candidate / "state" / _RUNTIME_SESSION_FILE
            if not session_file.exists():
                continue
            try:
                payload = json.loads(session_file.read_text(encoding="utf-8"))
            except (OSError, ValueError, json.JSONDecodeError):
                return None
            return payload if isinstance(payload, dict) else None
        return None

    def _project_subtree_is_tracked(self, source_repo: Path, project_relative: str) -> bool:
        try:
            proc = subprocess.run(
                ["git", "ls-files", "--", project_relative],
                cwd=source_repo,
                capture_output=True,
                text=True,
                timeout=30,
                encoding="utf-8",
                errors="replace",
            )
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
            return False
        return proc.returncode == 0 and bool(proc.stdout.strip())

    def _snapshot_directory(self, root: Path) -> dict[str, str]:
        snapshot: dict[str, str] = {}
        if not root.exists():
            return snapshot

        for current_root, dirnames, filenames in os.walk(root):
            dirnames[:] = [name for name in dirnames if name.lower() not in _IGNORED_DIFF_DIRS]
            base = Path(current_root)
            for filename in filenames:
                path = base / filename
                rel_path = path.relative_to(root).as_posix()
                snapshot[rel_path] = self._hash_file(path)
        return snapshot

    def _hash_file(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(65536), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _is_state_file(self, path: str, state_patterns: set[str]) -> bool:
        normalized = path.replace('\\', '/').lower()
        candidates = {item.lower() for item in state_patterns}
        return any(normalized.endswith(item) for item in candidates)

    def _is_allowed_path(self, path: str, allowed_roots: list[str]) -> bool:
        normalized = self._normalize_reported_path(path).rstrip('/')
        for root in allowed_roots:
            normalized_root = root.replace('\\', '/').rstrip('/')
            if not normalized_root:
                continue
            if normalized == normalized_root or normalized.startswith(normalized_root + '/'):
                return True
        return False

    def _is_test_file(self, path: str) -> bool:
        normalized = path.replace('\\', '/').lower()
        return (
            '/src/test/' in normalized
            or normalized.endswith('test.java')
            or normalized.endswith('tests.java')
            or normalized.endswith('.spec.ts')
            or normalized.endswith('.test.ts')
            or normalized.endswith('.spec.js')
            or normalized.endswith('.test.js')
            or '/tests/' in normalized
        )

    def _is_prod_file(self, path: str) -> bool:
        normalized = path.replace('\\', '/').lower()
        if self._is_test_file(normalized):
            return False
        if normalized.endswith(('.md', '.txt', '.json', '.lock')):
            return False
        return normalized.endswith((
            '.java', '.kt', '.groovy', '.py', '.js', '.jsx', '.ts', '.tsx',
            '.xml', '.yml', '.yaml', '.properties', '.sql'
        ))

    def _normalize_reported_path(self, path: str) -> str:
        normalized = str(path or "").strip().strip('`"\'')
        if not normalized:
            return ""

        markdown_link = re.match(r"^\[[^\]]+\]\((.+?)\)?$", normalized)
        if markdown_link:
            normalized = markdown_link.group(1).strip()

        normalized = normalized.rstrip('.,);').replace('\\', '/')
        if not normalized:
            return ""

        candidate = Path(normalized)
        if candidate.is_absolute() and self._working_dir:
            try:
                return candidate.resolve().relative_to(Path(self._working_dir).resolve()).as_posix()
            except ValueError:
                return candidate.as_posix()
        return candidate.as_posix() if candidate.is_absolute() else normalized

    def _to_full_path(self, path: str, *, candidate_paths: list[str] | None = None) -> str:
        candidate = Path(path)
        if candidate.is_absolute():
            return str(candidate)
        if self._working_dir:
            direct_path = Path(self._working_dir) / candidate
            if direct_path.exists():
                return str(direct_path)
            if candidate_paths and len(candidate.parts) == 1:
                basename = candidate.name
                matches: list[str] = []
                seen: set[str] = set()
                for relative_path in candidate_paths:
                    relative_candidate = Path(relative_path.replace('\\', '/'))
                    if relative_candidate.name != basename:
                        continue
                    resolved = Path(self._working_dir) / relative_candidate
                    resolved_str = str(resolved)
                    if resolved.exists() and resolved_str not in seen:
                        matches.append(resolved_str)
                        seen.add(resolved_str)
                if len(matches) == 1:
                    return matches[0]
            return str(direct_path)
        return str(candidate)

    def _validate_windows_command_wrappers(self, mentioned_files: set[str], candidate_paths: list[str]) -> list[str]:
        issues: list[str] = []
        suspicious_inline_powershell = re.compile(
            r'powershell(?:\.exe)?\b[^\r\n"]*-Command\s+"[^"\r\n]*\'\'[^"\r\n]*"',
            re.IGNORECASE,
        )
        for fpath in sorted(mentioned_files):
            if not fpath.lower().endswith(('.cmd', '.bat')):
                continue
            full_path = self._to_full_path(fpath, candidate_paths=candidate_paths)
            if not os.path.exists(full_path):
                continue
            try:
                content = Path(full_path).read_text(encoding='utf-8', errors='replace')
            except OSError:
                continue
            for lineno, line in enumerate(content.splitlines(), start=1):
                if suspicious_inline_powershell.search(line):
                    issues.append(f"疑似错误的 PowerShell -Command 单引号转义: {fpath}:{lineno}")
                    break
        return issues

    def _build_review_prompt(
        self,
        phase: Phase,
        goal_text: str,
        task_outputs: dict[str, Any],
        architecture_contract: ArchitectureContract | None = None,
    ) -> str:
        sections = [
            _REVIEW_SYSTEM_PROMPT,
            f"# 总体目标\n{goal_text}",
            f"# 当前阶段: {phase.name}\n{phase.description}",
            "# 阶段目标\n" + "\n".join(f"- {o}" for o in phase.objectives),
            "# 验收标准\n" + "\n".join(f"- {c}" for c in phase.acceptance_criteria),
        ]

        if phase.metadata:
            target_family = phase.metadata.get('target_service_family')
            if target_family:
                sections.append(f"# 目标服务族\n- {target_family}")
            out_of_scope = phase.metadata.get('out_of_scope')
            if out_of_scope:
                sections.append("# 明确不处理范围\n" + "\n".join(f"- {item}" for item in out_of_scope))

        if architecture_contract is not None:
            sections.append(
                "# 架构合同\n"
                f"- 决策类型: {architecture_contract.decision_type or '未声明'}\n"
                f"- 选定方案: {architecture_contract.selected_option_id or '未声明'}\n"
                f"- 摘要: {architecture_contract.selected_summary or '未提供'}"
            )
            if architecture_contract.migration_waves:
                sections.append(
                    "# 迁移波次\n"
                    + "\n".join(
                        f"- [{item.wave_id}] {item.title}: {item.objective}"
                        for item in architecture_contract.migration_waves[:4]
                    )
                )
            if architecture_contract.role_deliberations:
                sections.append(
                    "# 架构师评审\n"
                    + "\n".join(
                        f"- {item.title}({item.stance}): {item.summary}"
                        for item in architecture_contract.role_deliberations[:5]
                    )
                )
            if architecture_contract.execution_playbook is not None:
                sections.append(
                    "# 执行 Playbook\n"
                    f"- 标题: {architecture_contract.execution_playbook.title}\n"
                    f"- 策略: {architecture_contract.execution_playbook.strategy or '未声明'}\n"
                    + (
                        "- 切流门槛: "
                        + "；".join(architecture_contract.execution_playbook.cutover_gates[:4])
                        + "\n"
                        if architecture_contract.execution_playbook.cutover_gates
                        else ""
                    )
                    + (
                        "- 回滚触发: "
                        + "；".join(architecture_contract.execution_playbook.rollback_triggers[:4])
                        + "\n"
                        if architecture_contract.execution_playbook.rollback_triggers
                        else ""
                    )
                    + "\n".join(
                        f"- [{item.step_id}] {item.title}: {item.objective}"
                        for item in architecture_contract.execution_playbook.steps[:5]
                    )
                )
            if architecture_contract.verification_obligations:
                sections.append(
                    "# 架构验证义务\n"
                    + "\n".join(
                        f"- {item.description}" + (f" (`{item.command_hint}`)" if item.command_hint else "")
                        for item in architecture_contract.verification_obligations
                    )
                )
            if architecture_contract.human_gates:
                sections.append(
                    "# 架构门禁\n"
                    + "\n".join(
                        f"- [{item.gate_id}] {item.reason}: {item.trigger_condition}"
                        for item in architecture_contract.human_gates
                    )
                )
            if architecture_contract.dissent_notes:
                sections.append("# 保留意见\n" + "\n".join(f"- {item}" for item in architecture_contract.dissent_notes))

        if task_outputs:
            output_lines = []
            for tid, output in task_outputs.items():
                if isinstance(output, str):
                    text = output[:self._output_truncate_chars]
                    if len(output) > self._output_truncate_chars:
                        text += f"\n... [截断，原始 {len(output)} 字符]"
                else:
                    text_repr = repr(output)
                    text = text_repr[:self._output_truncate_chars]
                    if len(text_repr) > self._output_truncate_chars:
                        text += f"\n... [截断，原始 {len(text_repr)} 字符]"
                output_lines.append(f"## 任务 {tid}\n{text}")
            sections.append("# 任务执行输出\n" + "\n\n".join(output_lines))

        sections.append(
            "请根据以上信息审查此阶段的执行结果。"
            "如果需要，请使用工具检查工作目录中的实际文件。"
            "返回严格 JSON 格式的审查结果。"
        )
        return "\n\n".join(sections)

    def _parse_review(self, phase_id: str, text: str) -> ReviewResult:
        logger.debug("Review 原始输出 (前500字符): %s", repr(text[:500]))
        try:
            data = robust_parse_json(text)
        except ValueError:
            logger.warning("无法解析审查结果 JSON，使用默认值。原始输出前500字符: %s", repr(text[:500]))
            data = {"verdict": "major_issues", "score": 0.5, "summary": text[:500]}

        verdict_str = (
            data.get("verdict")
            or data.get("review_result")
            or data.get("status")
            or data.get("result")
            or "major_issues"
        )
        verdict_str = str(verdict_str) if verdict_str else "major_issues"
        verdict_str = _VERDICT_MAP.get(verdict_str.lower(), verdict_str.lower())
        try:
            verdict = ReviewVerdict(verdict_str)
        except ValueError:
            logger.warning("未知 verdict '%s'，默认为 major_issues", verdict_str)
            verdict = ReviewVerdict.MAJOR_ISSUES

        raw_score = data.get("score") or data.get("rating") or data.get("grade")
        score = _DEFAULT_SCORES.get(verdict.value, 0.5) if raw_score is None else self._parse_score(raw_score)
        score = max(0.0, min(1.0, score))

        issues = [
            ReviewIssue(
                severity=item.get("severity", "minor"),
                category=item.get("category", "completeness"),
                description=item.get("description", ""),
                affected_files=item.get("affected_files", []),
                suggested_fix=item.get("suggested_fix", ""),
            )
            for item in data.get("issues", [])
        ]
        actions = [
            CorrectiveAction(
                action_id=item.get("action_id", f"fix_{index}"),
                description=item.get("description", ""),
                prompt_template=item.get("prompt_template", ""),
                priority=item.get("priority", 1),
                depends_on_actions=item.get("depends_on_actions", []),
                timeout=item.get("timeout", 1800),
                action_type=item.get("action_type", "claude_cli"),
                executor_config=item.get("executor_config"),
            )
            for index, item in enumerate(data.get("corrective_actions", []))
        ]

        score_rationale = str(data.get("score_rationale") or "")
        review = ReviewResult(
            phase_id=phase_id,
            verdict=verdict,
            score=score,
            summary=data.get("summary", ""),
            issues=issues,
            corrective_actions=actions,
            score_rationale=score_rationale,
        )

        # 评分-区间一致性校验：检查 score 是否与 score_rationale 声明的区间匹配
        score = self._validate_score_range_consistency(review)
        review.score = score

        logger.info(
            "阶段 '%s' 审查结果: verdict=%s score=%.2f issues=%d actions=%d",
            phase_id,
            verdict.value,
            score,
            len(issues),
            len(actions),
        )
        return review

    @staticmethod
    def _validate_score_range_consistency(review: ReviewResult) -> float:
        """校验 score 与 verdict / score_rationale 是否一致。

        verdict 是主裁决，score 必须首先落在 verdict 的法定区间内。
        score_rationale 仅用于进一步收紧分数，不得推翻 verdict。
        """
        score = review.score
        verdict_range = _VERDICT_SCORE_RANGES.get(review.verdict.value)
        if verdict_range is not None:
            verdict_low, verdict_high = verdict_range
            if not (verdict_low <= score <= verdict_high):
                adjusted = _DEFAULT_SCORES.get(review.verdict.value, score)
                logger.warning(
                    "评分-verdict 不一致: verdict=%s score=%.2f 不在 [%.1f-%.1f]，调整为 %.2f",
                    review.verdict.value,
                    score,
                    verdict_low,
                    verdict_high,
                    adjusted,
                )
                score = adjusted

        rationale = review.score_rationale
        if not rationale:
            return score

        # 从 rationale 中提取区间范围（如 "0.8-1.0" 或 "0.3~0.6"）
        range_pattern = re.compile(r'(\d+\.\d+)\s*[-~]\s*(\d+\.\d+)')
        match = range_pattern.search(rationale)
        if not match:
            return score

        try:
            low = float(match.group(1))
            high = float(match.group(2))
        except ValueError:
            return score

        if low <= score <= high:
            return score  # 一致，无需调整

        if score < low or score > high:
            verdict_low, verdict_high = verdict_range or (low, high)
            overlap_low = max(low, verdict_low)
            overlap_high = min(high, verdict_high)
            if overlap_low <= overlap_high:
                midpoint = round((overlap_low + overlap_high) / 2, 2)
            else:
                midpoint = _DEFAULT_SCORES.get(review.verdict.value, score)
            logger.warning(
                "评分-区间不一致: score=%.2f 但 rationale 声明区间 [%.1f-%.1f]，调整到中点 %.2f。rationale: %s",
                score, low, high, midpoint, rationale[:200],
            )
            return midpoint

        return score

    @staticmethod
    def _parse_score(raw: Any) -> float:
        if isinstance(raw, (int, float)):
            value = float(raw)
            if value > 1.0 and value >= 10.0:
                return value / 100.0
            return value
        text = str(raw).strip().rstrip('%')
        if str(raw).strip().endswith('%'):
            try:
                return float(text) / 100.0
            except ValueError:
                logger.warning("无法解析百分制 score '%s'，使用默认值 0.5", raw)
                return 0.5
        if '/' in text:
            parts = text.split('/')
            try:
                return float(parts[0].strip()) / float(parts[1].strip())
            except (ValueError, ZeroDivisionError, IndexError):
                pass
        try:
            value = float(text)
            if value > 1.0 and value >= 10.0:
                return value / 100.0
            return value
        except ValueError:
            logger.warning("无法解析 score '%s'，使用默认值 0.5", raw)
            return 0.5
