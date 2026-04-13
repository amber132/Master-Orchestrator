"""精确迭代控制器（Surgical Controller）。

一次只改一个问题，改完立刻运行验证命令，验证不过就把精确错误反馈给下一轮。
不依赖 AI 审查打分，用机器输出（pytest/编译错误）作为反馈信号。
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from .auto_model import (
    ContextNotebook,
    GoalStatus,
    NotebookEntry,
    Phase,
    PhaseStatus,
)
from .claude_cli import BudgetTracker, run_claude_task
from .config import ClaudeConfig, LimitsConfig
from .model import TaskNode, TaskResult, TaskStatus
from .verification_runner import (
    VerificationIssue,
    parse_verification_errors,
    run_targeted,
)

logger = logging.getLogger(__name__)

# 每个 issue 的最大重试次数
_MAX_ISSUE_ATTEMPTS = 3
# 单次修复任务的超时（秒）
_FIX_TASK_TIMEOUT = 300
# 单次修复任务的最大轮次
_FIX_TASK_MAX_TURNS = 30
# 每轮最多发现的新 issue 数
_MAX_NEW_ISSUES_PER_ROUND = 5
# notebook prompt 注入上限（字符）
_NOTEBOOK_PROMPT_MAX_CHARS = 8000


@dataclass
class SurgicalResult:
    """精确迭代的最终结果。"""
    status: GoalStatus
    total_iterations: int = 0
    total_cost_usd: float = 0.0
    issues_fixed: int = 0
    issues_remaining: int = 0
    notebook: ContextNotebook | None = None
    summary: str = ""


class SurgicalController:
    """单任务精确迭代控制器。

    核心循环：发现问题 → 取一个 → 单任务修复 → 运行验证 → 记录 notebook → 下一个。
    """

    def __init__(
        self,
        goal: str,
        working_dir: str,
        claude_config: ClaudeConfig,
        limits: LimitsConfig,
        verification_commands: list[str],
        *,
        budget_tracker: BudgetTracker | None = None,
        notebook: ContextNotebook | None = None,
        max_iterations: int = 50,
        deadline: datetime | None = None,
        execution_model: str = "",
    ):
        self._goal = goal
        self._working_dir = working_dir
        self._claude_config = claude_config
        self._limits = limits
        self._verification_commands = verification_commands
        self._budget = budget_tracker
        self._notebook = notebook or ContextNotebook(
            entries=[],
            goal=goal,
            verification_commands=verification_commands,
        )
        self._max_iterations = max_iterations
        self._deadline = deadline or datetime.now() + timedelta(hours=24)
        self._execution_model = execution_model or claude_config.default_model
        self._iteration = 0

    @property
    def notebook(self) -> ContextNotebook:
        return self._notebook

    @property
    def iteration_count(self) -> int:
        return self._iteration

    def execute(self) -> SurgicalResult:
        """主精确迭代循环。"""
        logger.info("[Surgical] 开始精确迭代模式, goal=%s", self._goal[:100])

        # 1. 运行验证，发现初始问题列表
        initial_result = self._run_verification()
        if initial_result.passed:
            logger.info("[Surgical] 初始验证已通过，无需修复")
            return SurgicalResult(
                status=GoalStatus.CONVERGED,
                summary="初始验证已通过，无需修复",
                notebook=self._notebook,
            )

        issues = parse_verification_errors(initial_result)
        if not issues:
            # 验证失败但无法解析出具体 issue，用通用 fallback
            issues = [VerificationIssue(
                file="",
                line=None,
                description=initial_result.full_output[:500],
                severity="error",
                tool="generic",
            )]

        logger.info("[Surgical] 发现 %d 个问题", len(issues))

        # 2. 按严重度排序：error > warning
        issues = self._prioritize(issues)

        # 3. 逐个修复
        issues_fixed = 0
        while issues and self._iteration < self._max_iterations:
            # 预算/截止时间检查
            if self._budget and self._budget.spent >= self._budget.limit:
                logger.warning("[Surgical] 预算耗尽: $%.4f", self._budget.spent)
                break
            if datetime.now() >= self._deadline:
                logger.warning("[Surgical] 截止时间到达")
                break

            issue = issues.pop(0)
            self._iteration += 1

            logger.info(
                "[Surgical] 迭代 %d: 修复 %s (file=%s, line=%s)",
                self._iteration, issue.description[:80],
                issue.file, issue.line,
            )

            # 修复前验证快照
            pre_result = self._run_verification()
            pre_output = pre_result.full_output

            # 构建 prompt 并执行
            prompt = self._build_fix_prompt(issue, pre_output)
            task_result = self._execute_fix(prompt)

            # 修复后验证
            post_result = self._run_verification()
            post_output = post_result.full_output

            # 记录到 notebook
            entry = NotebookEntry(
                iteration=self._iteration,
                target_file=issue.file,
                target_issue=issue.description,
                error_before=pre_output[:2000],
                fix_attempted=(task_result.output or "")[:500],
                verification_output=post_output[:2000],
                verification_passed=post_result.passed,
                learned=self._extract_learning(pre_output, post_output, task_result),
                cost_usd=task_result.cost_usd,
                duration_seconds=task_result.duration_seconds,
            )
            self._notebook.entries.append(entry)

            if post_result.passed:
                issues_fixed += 1
                logger.info("[Surgical] ✅ 迭代 %d 修复成功", self._iteration)
                # 全部通过，重新检查是否还有遗漏的问题
                issues = []
            else:
                # 检查是否是新错误（说明部分修复成功）
                new_issues = parse_verification_errors(post_result)
                if new_issues and self._errors_changed(pre_output, post_output):
                    logger.info(
                        "[Surgical] 迭代 %d 部分成功：旧错误已变化，发现 %d 个新问题",
                        self._iteration, len(new_issues),
                    )
                    # 将新 issue 加入队列（去重）
                    for ni in new_issues[:_MAX_NEW_ISSUES_PER_ROUND]:
                        if not any(
                            ni.description == existing.description and ni.file == existing.file
                            for existing in issues
                        ):
                            issues.append(ni)
                else:
                    # 相同错误，增加重试计数
                    issue.attempt += 1
                    issue.last_error = post_output[:300]
                    if issue.attempt < _MAX_ISSUE_ATTEMPTS:
                        logger.warning(
                            "[Surgical] 迭代 %d 未修复，重试 (%d/%d): %s",
                            self._iteration, issue.attempt, _MAX_ISSUE_ATTEMPTS,
                            issue.description[:80],
                        )
                        issues.append(issue)
                    else:
                        logger.warning(
                            "[Surgical] 迭代 %d 放弃 issue (已重试 %d 次): %s",
                            self._iteration, _MAX_ISSUE_ATTEMPTS,
                            issue.description[:80],
                        )

        # 4. 最终状态
        final_result = self._run_verification()
        issues_remaining = len(parse_verification_errors(final_result))

        total_cost = self._budget.spent if self._budget else 0.0
        if final_result.passed:
            status = GoalStatus.CONVERGED
            summary = f"精确迭代完成：修复了 {issues_fixed} 个问题，全部验证通过"
        elif issues_fixed > 0:
            status = GoalStatus.PARTIAL_SUCCESS
            summary = f"部分成功：修复了 {issues_fixed} 个问题，剩余 {issues_remaining} 个"
        else:
            status = GoalStatus.FAILED
            summary = f"修复失败：共 {self._iteration} 次迭代，剩余 {issues_remaining} 个问题"

        logger.info("[Surgical] 完成: %s (cost=$%.4f)", summary, total_cost)

        return SurgicalResult(
            status=status,
            total_iterations=self._iteration,
            total_cost_usd=total_cost,
            issues_fixed=issues_fixed,
            issues_remaining=issues_remaining,
            notebook=self._notebook,
            summary=summary,
        )

    def _run_verification(self):
        """运行验证命令，返回完整输出。"""
        if not self._verification_commands:
            # 没有验证命令，用最小默认：py_compile 检查所有 .py 文件
            import glob
            import os
            py_files = glob.glob(os.path.join(self._working_dir, "**/*.py"), recursive=True)
            # 跳过 __pycache__ 和测试文件
            py_files = [
                f for f in py_files
                if "__pycache__" not in f and "site-packages" not in f
            ][:50]  # 限制文件数量
            if py_files:
                file_list = " ".join(f'"{f}"' for f in py_files)
                return run_targeted(
                    [f"python -m py_compile {' '.join(py_files[:20])}"],
                    cwd=self._working_dir,
                )
            return run_targeted([], cwd=self._working_dir)

        return run_targeted(
            self._verification_commands,
            cwd=self._working_dir,
        )

    def _build_fix_prompt(self, issue: VerificationIssue, pre_verification: str) -> str:
        """构建精确修复 prompt。

        注入完整上下文：目标 issue、验证输出全文、notebook 中该文件的历史尝试。
        """
        parts = [
            f"# 目标\n{self._goal}\n",
            f"# 当前问题\n",
        ]
        if issue.file:
            parts.append(f"- 文件: {issue.file}")
        if issue.line:
            parts.append(f"- 行号: {issue.line}")
        parts.append(f"- 描述: {issue.description}")

        parts.append(f"\n# 验证输出（修复前）\n```\n{pre_verification[:6000]}\n```\n")

        # 注入 notebook 历史
        relevant = self._notebook.relevant_entries(target_file=issue.file, limit=3)
        if relevant:
            parts.append("# 该文件的历次修复尝试\n")
            for entry in relevant:
                status_icon = "✅" if entry.verification_passed else "❌"
                parts.append(
                    f"- 第 {entry.iteration} 轮 {status_icon}: "
                    f"修复={entry.fix_attempted[:150]}\n"
                    f"  结果={entry.verification_output[:150]}\n"
                    f"  经验={entry.learned[:150]}\n"
                )

        parts.append(
            "\n# 指令\n"
            "只修复上述问题，不要修改无关代码。\n"
            "修复后不需要运行验证，系统会自动验证。\n"
            "如果不确定如何修复，请说明原因。\n"
        )
        return "\n".join(parts)

    def _execute_fix(self, prompt: str) -> TaskResult:
        """通过 run_claude_task 执行单次修复任务。"""
        task_node = TaskNode(
            id=f"_surgical_{self._iteration}",
            prompt_template=prompt,
            timeout=_FIX_TASK_TIMEOUT,
            model=self._execution_model,
            output_format="text",
            max_turns=_FIX_TASK_MAX_TURNS,
        )
        return run_claude_task(
            task=task_node,
            prompt=prompt,
            claude_config=self._claude_config,
            limits=self._limits,
            budget_tracker=self._budget,
            working_dir=self._working_dir,
        )

    def _prioritize(self, issues: list[VerificationIssue]) -> list[VerificationIssue]:
        """按严重度排序：error > fail > warning，同级别保持原序。"""
        severity_order = {"error": 0, "fail": 1, "warning": 2}
        return sorted(issues, key=lambda i: severity_order.get(i.severity, 3))

    def _errors_changed(self, pre_output: str, post_output: str) -> bool:
        """判断错误是否发生了变化（即使没完全修复）。"""
        # 简单策略：输出不同即为变化
        return pre_output.strip() != post_output.strip()

    def _extract_learning(
        self,
        pre_output: str,
        post_output: str,
        task_result: TaskResult,
    ) -> str:
        """从迭代结果中提取一句话经验。"""
        if task_result.status == TaskStatus.SUCCESS:
            if post_output.strip() == "" or "0 failed" in post_output:
                return "修复成功"
            return "代码修改完成，但验证仍有问题"
        return f"任务状态={task_result.status.value}, 错误={task_result.error[:100] if task_result.error else 'unknown'}"
