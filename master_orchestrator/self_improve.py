"""自我迭代主控制器：发现 → 审批 → 执行 → 验证完整流程。

发现阶段用 IntrospectionEngine + ExternalSourceScanner，
执行阶段复用 AutonomousController。
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
from difflib import SequenceMatcher
from pathlib import Path

from .auto_model import (
    AutoConfig,
    GoalStatus,
    ImprovementPriority,
    ImprovementProposal,
    ImprovementStatus,
    QualityGate,
    SelfImproveState,
    load_self_improve_state,
    save_self_improve_state,
)
from .autonomous import AutonomousController
from .model import ControllerConfig
from .claude_cli import BudgetTracker
from .command_runtime import normalize_python_command
from .config import Config, DiscoveryConfig
from .external_source import ExternalSourceScanner
from .introspect import IntrospectionEngine
from .sanitizer import PromptSanitizer
from .search_provider import DiscoveredSource
from .store import Store
from .task_contract import DataRisk, TaskContract, TaskInputType, TaskType
from .workspace_manager import WorkspaceManager, WorkspaceSession

logger = logging.getLogger(__name__)
_FILE_SYNC_IGNORE_DIRS = {
    ".git",
    ".pytest_cache",
    "__pycache__",
    ".venv",
    "audit_logs",
    "orchestrator_runs",
    "simple_runs",
}
_FILE_SYNC_IGNORE_FILES = {
    "orchestrator_state.db",
    "orchestrator_state.db-shm",
    "orchestrator_state.db-wal",
    "task_cache.db",
    "metrics.jsonl",
    "self_improve_state.json",
    "budget_tracker.json",
}

_EXTERNAL_SCAN_MAX_PARALLEL = 4

# 优先级排序权重
_PRIORITY_ORDER = {
    ImprovementPriority.CRITICAL: 0,
    ImprovementPriority.HIGH: 1,
    ImprovementPriority.MEDIUM: 2,
    ImprovementPriority.LOW: 3,
}

# 标题相似度阈值（超过此值视为重复）
_DEDUP_SIMILARITY_THRESHOLD = 0.7

# stalled goal 检测参数
_STALLED_GOAL_SIMILARITY_THRESHOLD = 0.8  # 目标描述相似度阈值
_STALLED_GOAL_MIN_ROUNDS = 3              # 连续多少轮未修复视为 stalled

# 状态持久化文件名
_STATE_FILENAME = "self_improve_state.json"


def _title_similarity(a: str, b: str) -> float:
    """计算两个标题的相似度（0-1）。"""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def _dedup_proposals(proposals: list[ImprovementProposal]) -> list[ImprovementProposal]:
    """按标题相似度去重，保留优先级更高的。"""
    if not proposals:
        return []

    # 按优先级排序（高优先级在前），这样去重时保留高优先级的
    sorted_proposals = sorted(proposals, key=lambda p: _PRIORITY_ORDER.get(p.priority, 99))

    kept: list[ImprovementProposal] = []
    for proposal in sorted_proposals:
        is_dup = False
        for existing in kept:
            if _title_similarity(proposal.title, existing.title) >= _DEDUP_SIMILARITY_THRESHOLD:
                is_dup = True
                logger.debug("去重: '%s' 与 '%s' 相似", proposal.title, existing.title)
                break
        if not is_dup:
            kept.append(proposal)

    removed = len(proposals) - len(kept)
    if removed > 0:
        logger.info("去重移除了 %d 条重复提案", removed)
    return kept


def _sort_proposals(proposals: list[ImprovementProposal]) -> list[ImprovementProposal]:
    """按优先级排序。"""
    return sorted(proposals, key=lambda p: _PRIORITY_ORDER.get(p.priority, 99))


class SelfImproveController:
    """自我迭代主控制器。

    流程：
    1. 发现：内省引擎 + 外部文档扫描 → 改进提案
    2. 审批：交互式 CLI 或文件标记
    3. 执行前准备：质量门禁基线 + git 分支
    4. 执行：复用 AutonomousController
    5. 验证：重新跑质量门禁，失败则回滚
    """

    def __init__(
        self,
        config: Config,
        auto_config: AutoConfig,
        working_dir: str | Path,
        orchestrator_dir: str | Path,
        external_sources: list[str] | None = None,
        approval_mode: str = "interactive",
        approval_file: str | None = None,
        quality_gate_commands: list[str] | None = None,
        store: Store | None = None,
        log_file: str | None = None,
        skip_introspection: bool = False,
        skip_external: bool = False,
        discover_mode: bool = False,
        discover_keywords: list[str] | None = None,
        discover_rss_feeds: list[str] | None = None,
        discover_max_results: int = 20,
        discover_search_template: str = "{keyword} best practices",
        smart_discover: bool = False,
        failure_history: list[dict] | None = None,
        rate_limiter_state: dict | None = None,
        prev_failure_categories: dict[str, int] | None = None,
        resume_state: SelfImproveState | None = None,
        discovery_config: DiscoveryConfig | None = None,
        discoverer_factory=None,
        external_scanner_factory=None,
        goal_history: list[str] | None = None,
        prev_round_summary: dict | None = None,
        goal_outcomes: list[dict] | None = None,
        seed_proposals: list[ImprovementProposal] | None = None,
        preferred_provider: str = "auto",
        phase_provider_overrides: dict[str, str] | None = None,
    ):
        self._config = config
        self._auto_config = auto_config
        self._working_dir = Path(working_dir)
        self._orchestrator_dir = Path(orchestrator_dir).resolve()
        self._source_repo_dir = self._orchestrator_dir
        self._execution_repo_dir = self._source_repo_dir
        self._external_sources = external_sources or []
        self._approval_mode = approval_mode
        self._approval_file = approval_file
        self._quality_gate_commands = quality_gate_commands or []
        self._owns_store = store is None
        self._store = store or Store(config.checkpoint.db_path)
        self._log_file = log_file
        self._skip_introspection = skip_introspection
        self._skip_external = skip_external
        self._discover_mode = discover_mode
        self._discover_keywords = discover_keywords or []
        self._discover_rss_feeds = discover_rss_feeds or []
        self._discover_max_results = discover_max_results
        self._discover_search_template = discover_search_template
        self._smart_discover = smart_discover
        self._discovery_config = discovery_config or config.discovery
        self._failure_history = failure_history or []
        self._prev_failure_categories: dict[str, int] = prev_failure_categories or {}
        self._rate_limiter_state: dict | None = rate_limiter_state
        self._discoverer_factory = discoverer_factory
        self._external_scanner_factory = external_scanner_factory
        self._prev_round_summary = prev_round_summary

        # 持久化到 working_dir 下的 budget_tracker.json
        _budget_persist = str(self._working_dir / "budget_tracker.json")
        self._budget = BudgetTracker(
            config.claude.max_budget_usd,
            persist_path=_budget_persist,
            enforcement_mode=config.claude.budget_enforcement_mode,
        )
        self._sanitizer = PromptSanitizer()
        self._last_goal_state = None
        self._workspace_manager = WorkspaceManager(config.workspace)
        self._workspace_session: WorkspaceSession | None = None

        self._state_path = self._working_dir / _STATE_FILENAME

        # 断点续传：如果传入 resume_state 则恢复，否则尝试从文件加载，最后新建
        if resume_state is not None:
            self._state = resume_state
            logger.info("从传入的 resume_state 恢复会话 %s (status=%s)",
                        self._state.session_id, self._state.status)
        else:
            self._state = SelfImproveState(
                session_id=uuid.uuid4().hex[:12],
            )
        self._restore_workspace_session()

        # 跨轮次目标历史（优先从参数恢复，其次从持久化状态恢复）
        self._goal_history: list[str] = list(goal_history or self._state.goal_history)

        # 跨轮次目标执行结果（summary + success），用于 stalled goal 检测
        self._goal_outcomes: list[dict] = list(goal_outcomes or self._state.goal_outcomes)
        self._seed_proposals = list(seed_proposals or [])
        self._preferred_provider = preferred_provider
        self._phase_provider_overrides = dict(phase_provider_overrides or {})

    @property
    def state(self) -> SelfImproveState:
        return self._state

    def _save_state(self) -> None:
        """持久化当前状态。"""
        try:
            self._state.total_cost_usd = self._budget.spent
            save_self_improve_state(self._state, self._state_path)
        except Exception as e:
            logger.warning("状态保存失败: %s", e)

    def _determine_start_phase(self) -> int:
        """根据已持久化的状态判断应从哪个阶段开始。

        阶段编号：
          1 = 发现 (analyzing)
          2 = 审批 (awaiting_approval)
          3 = 执行前准备 (preparing)
          4 = 执行改进 (executing)
          5 = 验证 (verifying)

        返回值：应开始执行的阶段编号（1-5）。
        """
        status = self._state.status

        # 已完成或失败的状态不应该被续传，从头开始
        if status in ("completed", "failed"):
            logger.info("上次状态为 %s，从阶段 1 重新开始", status)
            return 1

        # verifying：阶段 4 已完成，从阶段 5 重新开始
        if status == "verifying":
            return 5

        # executing：阶段 3 已完成，从阶段 4 重新开始（AutonomousController 自己有断点续传）
        if status == "executing":
            return 4

        # preparing：阶段 2 已完成（approved_ids 已有数据），从阶段 3 重新开始
        if status == "preparing":
            return 3

        # awaiting_approval：阶段 1 已完成（proposals 已有数据），从阶段 2 重新开始
        if status == "awaiting_approval":
            if self._state.proposals:
                return 2
            # proposals 为空说明数据不完整，从头开始
            logger.warning("状态为 awaiting_approval 但 proposals 为空，从阶段 1 重新开始")
            return 1

        # analyzing 或其他未知状态：从阶段 1 开始
        return 1

    def _preflight_check(self) -> None:
        """启动前前置校验，失败抛 RuntimeError 含可操作提示。"""
        errors: list[str] = []

        # (1) Claude CLI 可用（含认证验证）
        claude_path = shutil.which("claude")
        if not claude_path:
            errors.append(
                "Claude CLI 未找到。请确保 'claude' 命令在 PATH 中。"
                " 安装方式: npm install -g @anthropic-ai/claude-code"
            )
        else:
            # 验证 CLI 实际可用（认证通过等）
            try:
                verify = subprocess.run(
                    [claude_path, "--version"],
                    capture_output=True, text=True, timeout=10,
                )
                if verify.returncode != 0:
                    errors.append(
                        f"Claude CLI 认证失败 (exit={verify.returncode})。"
                        " 请运行 'claude login' 完成认证"
                    )
                else:
                    logger.debug("preflight: claude CLI 校验通过: %s", verify.stdout.strip())
            except subprocess.TimeoutExpired:
                errors.append(
                    "Claude CLI 验证超时 (10s)。"
                    " 可能是 CLI 卡住或网络问题，请手动运行 'claude --version' 确认"
                )
            except Exception as exc:
                errors.append(
                    f"Claude CLI 验证异常: {exc}。"
                    " 请确认 CLI 安装正确并已认证"
                )

        # (2) orchestrator_dir 存在且可写
        if not self._orchestrator_dir.is_dir():
            errors.append(
                f"目标目录不存在: {self._orchestrator_dir}"
                " 请确认 -d 参数指向的项目路径正确"
            )
        else:
            # 检查写权限：尝试创建再删除临时文件
            try:
                probe = self._orchestrator_dir / f".preflight_{uuid.uuid4().hex[:6]}"
                probe.write_text("ok", encoding="utf-8")
                probe.unlink()
            except (PermissionError, OSError) as exc:
                errors.append(
                    f"目标目录不可写: {self._orchestrator_dir} ({exc})"
                    " 请检查目录权限或关闭占用进程后重试"
                )

        # (3) config.toml 存在（外部项目模式下降级为警告）
        config_path = self._orchestrator_dir / "config.toml"
        if not config_path.is_file():
            # 检查是否为外部项目（orchestrator_dir 不是编排器自身目录）
            _self_dir = Path(__file__).resolve().parent.parent
            is_external = self._orchestrator_dir.resolve() != _self_dir
            if is_external:
                logger.warning(
                    "外部项目未包含 config.toml: %s（将使用编排器默认配置）",
                    config_path,
                )
            else:
                errors.append(
                    f"配置文件不存在: {config_path}"
                    " 请确保项目根目录下有 config.toml"
                    "（可参考 config.toml.example 或从模板生成）"
                )

        if errors:
            separator = "\n  - "
            raise RuntimeError(
                f"前置校验失败（{len(errors)} 项）：{separator}{separator.join(errors)}"
            )

        logger.info("前置校验通过: claude=%s, dir=%s, config=%s",
                     claude_path, self._orchestrator_dir, config_path)

    def execute(self) -> SelfImproveState:
        """执行完整的自我迭代流程，支持断点续传。

        根据 self._state.status 判断上次中断的位置，跳过已完成的阶段。
        """
        self._preflight_check()
        start_phase = self._determine_start_phase()

        logger.info("=" * 60)
        logger.info("自我迭代系统启动")
        logger.info("会话 ID: %s", self._state.session_id)
        logger.info("编排器目录: %s", self._orchestrator_dir)
        logger.info("工作目录: %s", self._working_dir)
        if start_phase > 1:
            logger.info("断点续传: 从阶段 %d 恢复 (上次状态: %s)", start_phase, self._state.status)
        logger.info("=" * 60)

        try:
            # 阶段 1: 发现
            if start_phase <= 1:
                self._state.status = "analyzing"
                self._save_state()
                proposals = self._phase_discover()

                if not proposals:
                    logger.info("未发现任何改进提案，退出")
                    self._state.status = "completed"
                    self._save_state()
                    return self._state

                self._state.proposals = proposals
                self._state.status = "awaiting_approval"
                self._save_state()
            else:
                # 从持久化状态恢复 proposals
                proposals = self._state.proposals
                logger.info("跳过阶段 1 (发现): 已有 %d 条提案", len(proposals))

            # 阶段 2: 审批
            if start_phase <= 2:
                self._save_state()
                approved = self._phase_approve(proposals)

                if not approved:
                    logger.info("没有提案被批准，退出")
                    self._state.status = "completed"
                    self._save_state()
                    return self._state

                self._state.approved_ids = [p.proposal_id for p in approved]
                for p in proposals:
                    if p.proposal_id in self._state.approved_ids:
                        p.status = ImprovementStatus.APPROVED
                    else:
                        p.status = ImprovementStatus.REJECTED
                        self._state.rejected_ids.append(p.proposal_id)
                self._state.status = "preparing"
                self._save_state()
            else:
                # 从持久化状态恢复 approved 列表
                approved_set = set(self._state.approved_ids)
                approved = [p for p in proposals if p.proposal_id in approved_set]
                logger.info("跳过阶段 2 (审批): 已批准 %d 条提案", len(approved))

            # 阶段 3: 执行前准备
            if start_phase <= 3:
                pre_test_ok = self._phase_prepare()
                self._state.pre_test_passed = pre_test_ok
                self._state.status = "executing"
                self._save_state()
            else:
                logger.info("跳过阶段 3 (准备): pre_test_passed=%s", self._state.pre_test_passed)

            # 阶段 4: 执行改进
            if start_phase <= 4:
                for p in approved:
                    p.status = ImprovementStatus.EXECUTING
                self._save_state()

                exec_ok = self._phase_execute(approved)
                self._state.status = "verifying"
                self._save_state()
            else:
                # 从阶段 5 恢复时，视为执行已成功（否则不会进入 verifying）
                exec_ok = True
                logger.info("跳过阶段 4 (执行): 从验证阶段恢复")

            # 阶段 5: 验证
            post_test_ok = self._phase_verify()
            self._state.post_test_passed = post_test_ok

            if not exec_ok:
                # 检查是否有实际代码变更（git diff）
                has_changes = self._has_uncommitted_changes()
                if has_changes:
                    # 有代码变更但未收敛，回滚
                    self._rollback()
                    for p in approved:
                        p.status = ImprovementStatus.ROLLED_BACK
                    logger.warning("执行阶段失败，已回滚代码变更")
                else:
                    # 无代码变更（如目标分解失败），无需回滚
                    for p in approved:
                        p.status = ImprovementStatus.FAILED
                    logger.warning("执行阶段失败（无代码变更），跳过回滚")
                self._state.status = "failed"
            elif post_test_ok or not self._quality_gate_commands:
                # 将 workspace 中的改进合并回源仓库
                self._merge_workspace_to_source()
                self._state.status = "completed"
                for p in approved:
                    p.status = ImprovementStatus.COMPLETED
                logger.info("自我迭代完成，所有验证通过")
            else:
                # 测试失败，回滚
                self._rollback()
                self._state.status = "failed"
                for p in approved:
                    p.status = ImprovementStatus.ROLLED_BACK
                logger.warning("验证失败，已回滚到改进前状态")

        except KeyboardInterrupt:
            logger.warning("用户中断")
            self._state.status = "failed"
        except Exception as e:
            logger.exception("自我迭代异常: %s", e)
            self._state.status = "failed"
        finally:
            # 确保最终状态持久化（必须在 finally 内，否则异常时丢失）
            self._state.total_cost_usd = self._budget.spent
            self._save_state()
            self._print_summary()

            # 关闭自己创建的 Store 实例
            if self._owns_store:
                try:
                    self._store.close()
                except Exception as e:
                    logger.warning("Failed to close store: %s", e)

        return self._state

    # ── 阶段 1: 发现 ──

    def _phase_discover(self) -> list[ImprovementProposal]:
        """运行内省引擎和外部文档扫描，合并去重。"""
        logger.info("─── 阶段 1: 发现 ───")
        all_proposals: list[ImprovementProposal] = list(self._seed_proposals)
        if self._seed_proposals:
            logger.info("计划文件种子提案: %d 条", len(self._seed_proposals))
        discovered_sources: list[DiscoveredSource] = []

        if self._discover_mode or (not self._skip_external and self._external_sources):
            self._ensure_workspace_session()

        # 自动发现博文（在外部扫描之前，将发现的 URL 合并到 external_sources）
        if self._discover_mode:
            from .discover import ArticleDiscoverer

            discoverer_kwargs = dict(
                orchestrator_dir=self._orchestrator_dir,
                working_dir=self._working_dir,
                extra_keywords=self._discover_keywords,
                extra_rss_feeds=self._discover_rss_feeds,
                max_results=self._discover_max_results,
                search_template=self._discover_search_template,
                claude_config=self._config.claude,
                limits_config=self._config.limits,
                budget_tracker=self._budget,
                smart_mode=self._smart_discover,
                discovery_config=self._discovery_config,
                evidence_dir=self._workspace_session.layout.evidence if self._workspace_session else None,
            )
            discoverer = (
                self._discoverer_factory(**discoverer_kwargs)
                if self._discoverer_factory
                else ArticleDiscoverer(**discoverer_kwargs)
            )
            try:
                discovered_sources = discoverer.discover()
            except Exception as e:
                logger.warning("自动发现失败: %s", e)
                discovered_sources = []
            logger.info("自动发现: %d 个来源", len(discovered_sources))
            # 合并到外部源列表（去重）
            existing = {
                item.url if isinstance(item, DiscoveredSource) else item
                for item in self._external_sources
            }
            for source in discovered_sources:
                if source.url not in existing and source.canonical_url not in existing:
                    self._external_sources.append(source)
                    existing.add(source.url)
                    existing.add(source.canonical_url)

        # 内省引擎
        if not self._skip_introspection:
            engine = IntrospectionEngine(
                claude_config=self._config.claude,
                limits_config=self._config.limits,
                budget_tracker=self._budget,
                orchestrator_dir=self._orchestrator_dir,
                working_dir=self._working_dir,
                store=self._store,
            )
            try:
                introspect_proposals = engine.analyze()
            except Exception as e:
                logger.warning("内省引擎失败: %s", e)
                introspect_proposals = []
            all_proposals.extend(introspect_proposals)
            logger.info("内省引擎: %d 条提案", len(introspect_proposals))

        # 外部文档扫描
        if not self._skip_external and self._external_sources:
            scanner_kwargs = dict(
                claude_config=self._config.claude,
                limits_config=self._config.limits,
                budget_tracker=self._budget,
                orchestrator_dir=self._orchestrator_dir,
                evidence_dir=self._workspace_session.layout.evidence if self._workspace_session else None,
                sogou_cookie_header=self._discovery_config.sogou_cookie_header,
                sogou_cookie_file=self._discovery_config.sogou_cookie_file,
                sogou_storage_state_path=self._discovery_config.sogou_storage_state_path,
                focus_keywords=self._discover_keywords,
                max_parallel=min(self._config.orchestrator.max_parallel, _EXTERNAL_SCAN_MAX_PARALLEL),
            )
            scanner = (
                self._external_scanner_factory(**scanner_kwargs)
                if self._external_scanner_factory
                else ExternalSourceScanner(**scanner_kwargs)
            )
            try:
                external_proposals = scanner.scan(self._external_sources)
            except Exception as e:
                logger.warning("外部扫描失败: %s", e)
                external_proposals = []
            all_proposals.extend(external_proposals)
            logger.info("外部扫描: %d 条提案", len(external_proposals))

        # 合并去重 + 排序
        deduped = _dedup_proposals(all_proposals)
        # 过滤掉空描述的低质量提案
        quality_proposals = [p for p in deduped if p.description and p.description.strip()]
        filtered_count = len(deduped) - len(quality_proposals)
        if filtered_count > 0:
            logger.info("过滤掉 %d 条空描述提案", filtered_count)
        sorted_proposals = _sort_proposals(quality_proposals)

        logger.info("发现阶段完成: %d 条提案（去重前 %d 条）", len(sorted_proposals), len(all_proposals))
        return sorted_proposals

    # ── 阶段 2: 审批 ──

    def _phase_approve(self, proposals: list[ImprovementProposal]) -> list[ImprovementProposal]:
        """根据审批模式获取用户批准。"""
        logger.info("─── 阶段 2: 审批 ───")

        if self._approval_mode == "auto":
            # 自动审批模式：限制最多 15 条，优先高优先级（已排序）
            max_auto = 15
            approved = list(proposals[:max_auto])
            if len(proposals) > max_auto:
                logger.info("自动审批模式: 批准前 %d 条提案（共 %d 条，跳过 %d 条低优先级）",
                            max_auto, len(proposals), len(proposals) - max_auto)
            else:
                logger.info("自动审批模式: 批准全部 %d 条提案", len(approved))
            return approved
        elif self._approval_mode == "file":
            return self._approve_via_file(proposals)
        else:
            return self._approve_interactive(proposals)

    def _approve_interactive(self, proposals: list[ImprovementProposal]) -> list[ImprovementProposal]:
        """交互式 CLI 审批。"""
        print("\n" + "=" * 60)
        print("  改进提案审批")
        print("=" * 60 + "\n")

        for i, p in enumerate(proposals, 1):
            priority_tag = p.priority.value.upper()
            print(f"[{i}] [{priority_tag}] {p.title}")
            print(f"    问题: {p.description[:120]}")
            if p.rationale:
                print(f"    理由: {p.rationale[:120]}")
            files_str = ", ".join(p.affected_files[:5]) if p.affected_files else "未指定"
            print(f"    影响: {files_str} | 复杂度: {p.estimated_complexity}")
            if p.evidence:
                print(f"    证据: {p.evidence[:100]}")
            if p.source_provider or p.source_score or p.evidence_path:
                print(
                    "    来源: {} | 分数: {:.2f} | 证据文件: {}".format(
                        p.source_provider or "manual",
                        p.source_score,
                        p.evidence_path or "未落盘",
                    )
                )
            print()

        print("请输入要批准的编号 (例: 1,3,5 / a=全部 / n=拒绝全部 / q=退出): ", end="", flush=True)

        try:
            user_input = input().strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\n已取消")
            return []

        if user_input == "q":
            return []
        if user_input == "n":
            return []
        if user_input == "a":
            return list(proposals)

        # 解析编号
        approved: list[ImprovementProposal] = []
        for part in user_input.split(","):
            part = part.strip()
            try:
                idx = int(part) - 1
                if 0 <= idx < len(proposals):
                    approved.append(proposals[idx])
            except ValueError:
                logger.warning("忽略无效输入: %s", part)

        logger.info("用户批准了 %d / %d 条提案", len(approved), len(proposals))
        return approved

    def _approve_via_file(self, proposals: list[ImprovementProposal]) -> list[ImprovementProposal]:
        """文件标记审批：写入 markdown 文件，轮询等待修改。"""
        file_path = Path(self._approval_file) if self._approval_file else (self._working_dir / "proposals.md")

        # 写入提案文件
        lines = ["# 改进提案审批\n"]
        lines.append("在要批准的提案前打勾 `[x]`，然后保存文件。\n\n")

        for p in proposals:
            priority_tag = p.priority.value.upper()
            lines.append(f"- [ ] **[{priority_tag}]** {p.title} (`{p.proposal_id}`)")
            lines.append(f"  - {p.description[:200]}")
            if p.affected_files:
                lines.append(f"  - 影响文件: {', '.join(p.affected_files[:5])}")
            if p.source_provider or p.source_score or p.evidence_path:
                lines.append(
                    f"  - 来源: {p.source_provider or 'manual'} | 分数: {p.source_score:.2f} | 证据文件: {p.evidence_path or '未落盘'}"
                )
            lines.append("")

        file_path.write_text("\n".join(lines), encoding="utf-8")
        logger.info("提案文件已写入: %s", file_path)
        print(f"\n提案已写入 {file_path}，请编辑后保存。等待中...", flush=True)

        # 轮询等待文件修改
        try:
            initial_mtime = file_path.stat().st_mtime
        except (OSError, FileNotFoundError) as e:
            logger.error("无法读取文件状态 %s: %s", file_path, e)
            return []
        deadline = time.time() + 3600  # 1小时超时
        while time.time() < deadline:
            time.sleep(2)
            try:
                current_mtime = file_path.stat().st_mtime
                if current_mtime > initial_mtime:
                    break
            except OSError:
                continue
        else:
            # 超时未修改
            logger.warning("文件审批超时 (3600s)，拒绝所有提案")
            return []

        # 解析已批准的提案
        try:
            content = file_path.read_text(encoding="utf-8")
        except (OSError, FileNotFoundError, PermissionError) as e:
            logger.error("无法读取提案文件 %s: %s", file_path, e)
            return []
        approved_ids: set[str] = set()
        for line in content.split("\n"):
            # 匹配 [x] 或 [X] 标记
            match = re.search(r"- \[x\].*\(`(\w+)`\)", line, re.IGNORECASE)
            if match:
                approved_ids.add(match.group(1))

        approved = [p for p in proposals if p.proposal_id in approved_ids]
        logger.info("文件审批: %d / %d 条提案被批准", len(approved), len(proposals))
        return approved

    # ── 阶段 3: 执行前准备 ──

    def _phase_prepare(self) -> bool:
        """创建隔离工作区并运行质量门禁基线。"""
        logger.info("─── 阶段 3: 执行前准备 ───")

        self._ensure_workspace_session()
        self._sync_source_to_workspace()
        self._record_pre_commit_hash()

        # 运行质量门禁基线
        if not self._quality_gate_commands:
            logger.info("未配置质量门禁，跳过基线测试")
            return True

        return self._run_quality_gate("执行前基线")

    def _check_stalled_goal(
        self,
        current_goal_summary: str,
        similarity_threshold: float = _STALLED_GOAL_SIMILARITY_THRESHOLD,
        min_rounds: int = _STALLED_GOAL_MIN_ROUNDS,
    ) -> bool:
        """检测连续 N 轮目标描述相似度 > threshold 但均未修复的停滞状态。

        从最近的 goal_outcomes 往前扫描，要求：
        1. 连续 min_rounds 轮（含当前轮）的目标描述两两相似度 > threshold
        2. 这些轮次的执行结果均为失败（success=False）

        Args:
            current_goal_summary: 当前轮次的目标摘要（前 200 字符）
            similarity_threshold: 相似度阈值（默认 0.8）
            min_rounds: 最少连续轮次（默认 3）

        Returns:
            True 表示检测到 stalled goal
        """
        # 需要至少 min_rounds - 1 条历史记录（加上当前轮 = min_rounds）
        needed = min_rounds - 1
        if len(self._goal_outcomes) < needed:
            return False

        # 取最近 needed 条记录，全部必须失败
        recent = self._goal_outcomes[-needed:]
        if any(entry.get("success", False) for entry in recent):
            return False

        # 检查所有历史摘要与当前目标的相似度
        for entry in recent:
            hist_summary = entry.get("summary", "")
            similarity = SequenceMatcher(
                None, current_goal_summary.lower(), hist_summary.lower()
            ).ratio()
            if similarity < similarity_threshold:
                return False

        return True

    # ── 阶段 4: 执行改进 ──

    def _phase_execute(self, approved: list[ImprovementProposal]) -> bool:
        """将批准的提案组装为 goal，复用 AutonomousController 执行。返回是否成功。"""
        logger.info("─── 阶段 4: 执行改进 ───")

        # 组装 goal 文本
        goal_parts = ["对编排器自身代码执行以下改进：\n"]
        for i, p in enumerate(approved, 1):
            goal_parts.append(f"{i}. [{p.priority.value.upper()}] {p.title}")
            if p.description:
                # 截断过长描述，避免 goal 文本爆炸
                # 清洗外部来源的描述内容

                sanitized_desc = self._sanitizer.sanitize(p.description, max_length=500)

                desc = sanitized_desc.cleaned_text[:300]
                goal_parts.append(f"   描述: {desc}")
            if p.affected_files:
                goal_parts.append(f"   影响文件: {', '.join(p.affected_files)}")
            goal_parts.append("")

        goal_parts.append(
            "注意事项：\n"
            "- 每个改进都要确保代码可以正常运行\n"
            "- 保持现有 API 兼容性\n"
            "- 添加必要的错误处理\n"
            "- 不要破坏现有功能"
        )

        # 注入上一轮失败上下文，帮助本轮避免重复错误
        if self._failure_history:
            goal_parts.append("\n\n## 前序轮次失败记录（务必避免重复这些错误）\n")
            for fh in self._failure_history:
                goal_parts.append(f"- 第 {fh['round']} 轮: 状态={fh['status']}")
                if fh.get("failure_stage"):
                    goal_parts.append(f"  失败阶段: {fh['failure_stage']}")
                if fh.get("error_detail"):
                    goal_parts.append(f"  错误详情: {fh['error_detail'][:200]}")
                if fh.get("hint"):
                    goal_parts.append(f"  修正建议: {fh['hint']}")

        # 注入前轮 goal_state.failure_categories 统计，帮助本轮定位高发失败模式
        if self._prev_failure_categories:
            goal_parts.append("\n\n## 前轮失败诊断\n")
            goal_parts.append("前序轮次的结构化失败分类统计（category → 出现次数）：\n")
            for cat, count in sorted(
                self._prev_failure_categories.items(), key=lambda x: x[1], reverse=True
            ):
                goal_parts.append(f"- **{cat}**: {count} 次")
            goal_parts.append(
                "\n请针对上述高频失败类别，优先从不同角度或方法来解决问题，"
                "避免重复相同的修复策略。"
            )

        # 注入前轮运行摘要（score_trend / iterations / phases），帮助本轮了解历史进展
        if self._prev_round_summary:
            goal_parts.append("\n\n## 前轮运行摘要\n")
            summary = self._prev_round_summary
            # 分数趋势
            score_trend = summary.get("score_trend")
            if score_trend:
                trend_str = " → ".join(f"{s:.2f}" for s in score_trend[-10:])
                goal_parts.append(f"**分数趋势**: {trend_str}")
            # 迭代统计
            total_iter = summary.get("total_iterations", 0)
            if total_iter:
                goal_parts.append(f"**总迭代次数**: {total_iter}")
            # 阶段状态
            phases = summary.get("phases")
            if phases:
                goal_parts.append("**阶段完成情况**:")
                for ph in phases:
                    name = ph.get("name", "?")
                    status = ph.get("status", "?")
                    best_score = ph.get("best_score")
                    score_info = f" (最高分={best_score:.2f})" if best_score else ""
                    goal_parts.append(f"  - {name}: {status}{score_info}")
            # 失败分类
            failure_cats = summary.get("failure_categories")
            if failure_cats:
                goal_parts.append("**失败分类统计**:")
                for cat, cnt in sorted(failure_cats.items(), key=lambda x: x[1], reverse=True)[:5]:
                    goal_parts.append(f"  - {cat}: {cnt} 次")
            goal_parts.append(
                "\n请参考上述运行摘要，调整改进策略。"
                "如果分数趋势停滞或下降，请尝试完全不同的方法。"
            )

        goal_text = "\n".join(goal_parts)

        # stalled goal 检测：连续 N 轮相似目标均未修复
        goal_summary = goal_text[:200]
        if self._check_stalled_goal(goal_summary):
            self._state.stalled_goal = True
            logger.warning(
                'stalled_goal 检测: 连续 %d 轮相似目标均未修复，标记 stalled_goal=True',
                _STALLED_GOAL_MIN_ROUNDS,
            )
            self._save_state()

        # 跨轮次目标去重检测
        similar_count = 0
        for hist in reversed(self._goal_history):
            if SequenceMatcher(None, goal_summary, hist).ratio() >= 0.7:
                similar_count += 1
            else:
                break
        if similar_count >= 3:
            self._state.status = 'skipped_duplicate_goal'
            logger.warning(
                '连续 %d 次相同目标，自动跳过并报告人工干预',
                similar_count,
            )
            self._save_state()
            return False
        if similar_count >= 1:
            goal_parts.append(
                '\n\n## 策略切换提示\n'
                f'前 {similar_count} 轮用相同策略失败，'
                '请采用完全不同的修复方法（例如：换用不同的代码结构、'
                '换用不同的设计模式、先写测试再改实现）'
            )
            goal_text = "\n".join(goal_parts)

        # 配置 AutonomousController
        quality_gate = QualityGate()
        if self._quality_gate_commands:
            quality_gate = QualityGate(commands=self._quality_gate_commands, enabled=True)

        exec_config = AutoConfig(
            max_hours=self._auto_config.max_hours,
            max_total_iterations=self._auto_config.max_total_iterations,
            decomposition_model=self._auto_config.decomposition_model,
            review_model=self._auto_config.review_model,
            execution_model=self._auto_config.execution_model,
            quality_gate=quality_gate,
        )

        controller = AutonomousController(ControllerConfig(
            goal=goal_text,
            working_dir=str(self._execution_repo_dir),
            config=self._config,
            auto_config=exec_config,
            store=self._store,
            log_file=self._log_file,
            preferred_provider=self._preferred_provider,
            phase_provider_overrides=self._phase_provider_overrides,
        ))

        # 恢复跨轮次限流状态
        if self._rate_limiter_state:
            try:
                controller._rate_limiter.restore_state(self._rate_limiter_state)
            except (AttributeError, Exception):
                pass

        state = controller.execute()
        self._state.execution_goal_id = state.goal_id
        self._last_goal_state = state  # 保留用于失败诊断提取

        # 提取最新限流状态，供下一轮使用
        try:
            self._rate_limiter_state = controller._rate_limiter.get_state()
        except (AttributeError, Exception):
            pass

        # 同步花费：取 state 和 controller 内部预算的较大值，避免遗漏
        controller_spent = 0.0
        try:
            controller_spent = controller._budget.spent
        except (AttributeError, Exception):
            pass
        actual_spent = max(state.total_cost_usd, controller_spent)
        self._budget.add_spent(actual_spent)
        # 确保状态文件中的花费也同步更新
        self._state.total_cost_usd = self._budget.spent
        logger.info(
            '预算同步: state.cost=%.4f controller.budget=%.4f synced=%.4f',
            state.total_cost_usd, controller_spent, actual_spent,
        )

        # 记录本轮 goal 摘要到历史（无论执行成功或失败）
        self._goal_history.append(goal_summary)
        self._state.goal_history = list(self._goal_history)

        # 记录本轮目标执行结果（用于 stalled goal 检测）
        exec_success = state.status in (GoalStatus.CONVERGED, GoalStatus.PARTIAL_SUCCESS)
        self._goal_outcomes.append({"summary": goal_summary, "success": exec_success})
        self._state.goal_outcomes = list(self._goal_outcomes)
        self._save_state()

        if state.status in (GoalStatus.CONVERGED, GoalStatus.PARTIAL_SUCCESS):
            logger.info("改进执行成功 (状态: %s)", state.status.value)
            return True
        else:
            logger.warning("改进执行状态: %s", state.status.value)
            return False

    @property
    def last_goal_state(self):
        """返回最近一次执行的 GoalState，用于提取失败诊断信息。"""
        return getattr(self, '_last_goal_state', None)

    def get_rate_limiter_state(self) -> dict | None:
        """返回当前限流器状态，供跨轮次持久化。"""
        return self._rate_limiter_state

    @property
    def goal_outcomes(self) -> list[dict]:
        """返回跨轮次目标执行结果，供 CLI 轮间传递。"""
        return self._goal_outcomes

    # ── 阶段 5: 验证 ──

    def _phase_verify(self) -> bool:
        """重新运行质量门禁，验证改进结果。"""
        logger.info("─── 阶段 5: 验证 ───")

        if not self._quality_gate_commands:
            logger.info("未配置质量门禁，跳过验证")
            return True

        return self._run_quality_gate("执行后验证")

    def _run_quality_gate(self, label: str) -> bool:
        """执行质量门禁命令，返回是否全部通过。"""
        logger.info("运行质量门禁 (%s): %d 个命令", label, len(self._quality_gate_commands))
        all_passed = True

        for cmd_str in self._quality_gate_commands:
            rendered_command = normalize_python_command(self._render_quality_gate_command(cmd_str))
            logger.info("  命令: %s", rendered_command)
            try:
                proc = subprocess.run(
                    rendered_command,
                    shell=True,
                    cwd=str(self._execution_repo_dir),
                    capture_output=True,
                    text=True,
                    timeout=300,
                    encoding="utf-8",
                    errors="replace",
                )
                if proc.returncode == 0:
                    logger.info("  [OK] 通过: %s", cmd_str)
                else:
                    logger.warning("  [X] 失败: %s (exit=%d)", cmd_str, proc.returncode)
                    if proc.stderr:
                        logger.warning("    stderr: %s", proc.stderr[-500:])
                    all_passed = False
            except subprocess.TimeoutExpired:
                logger.warning("  [X] 超时: %s", cmd_str)
                all_passed = False
            except Exception as e:
                logger.warning("  [X] 异常: %s — %s", cmd_str, e)
                all_passed = False

        logger.info("质量门禁 (%s): %s", label, "全部通过" if all_passed else "存在失败")
        return all_passed

    def _render_quality_gate_command(self, cmd_str: str) -> str:
        return str(cmd_str).format(
            workspace_dir=str(self._execution_repo_dir),
            source_repo=str(self._source_repo_dir),
        )

    def _has_uncommitted_changes(self) -> bool:
        """检查工作目录是否有未提交的代码变更（含 staged 和 unstaged）。"""
        try:
            # unstaged changes
            unstaged = subprocess.run(
                ["git", "diff", "--stat"],
                cwd=str(self._execution_repo_dir),
                capture_output=True, text=True, timeout=10,
            )
            # staged changes
            staged = subprocess.run(
                ["git", "diff", "--cached", "--stat"],
                cwd=str(self._execution_repo_dir),
                capture_output=True, text=True, timeout=10,
            )
            return bool(unstaged.stdout.strip() or staged.stdout.strip())
        except Exception:
            return False

    def _rollback_branch_fallback(self) -> None:
        """方案 B 回滚：切换回原分支。"""
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=str(self._orchestrator_dir),
            capture_output=True, text=True, timeout=10,
        )
        current_branch = result.stdout.strip()
        if current_branch == self._state.git_branch:
            subprocess.run(
                ["git", "checkout", "-"],
                cwd=str(self._orchestrator_dir),
                capture_output=True, text=True, timeout=30,
            )
            logger.info("已切换回之前的分支")

    def _rollback(self) -> None:
        """回滚到改进前的 git 状态。

        策略：
        1. 如使用隔离 worktree，优先移除隔离工作区，保持源仓库不变
        2. 仅在未启用隔离时，回退到旧的源仓库 reset/checkout 策略
        """
        if self._workspace_session and self._workspace_session.layout.workspace != self._source_repo_dir:
            self._discard_isolated_workspace()
            return

        if not self._state.git_branch:
            logger.warning("无 git 分支信息，无法回滚")
            return

        logger.info("回滚: 恢复到改进前状态")
        try:
            if self._state.pre_commit_hash:
                # 方案 A：直接 reset 到改进前的 commit
                # 先验证 commit 是否存在（可能被 force-push 删除）
                verify = subprocess.run(
                    ["git", "cat-file", "-e", self._state.pre_commit_hash],
                    cwd=str(self._orchestrator_dir),
                    capture_output=True, timeout=10,
                )
                if verify.returncode != 0:
                    logger.warning(
                        "pre_commit_hash %s 不存在于仓库，回退到方案 B",
                        self._state.pre_commit_hash[:8],
                    )
                    # 降级到方案 B
                    self._rollback_branch_fallback()
                else:
                    subprocess.run(
                        ["git", "reset", "--hard", self._state.pre_commit_hash],
                        cwd=str(self._execution_repo_dir),
                        capture_output=True, text=True, timeout=30,
                        check=True,
                    )
                    logger.info("已 reset 到改进前 commit: %s", self._state.pre_commit_hash[:8])
            else:
                # 方案 B：回退到原分支（兜底）
                self._rollback_branch_fallback()

            # 清理未跟踪的文件
            subprocess.run(
                ["git", "clean", "-fd"],
                cwd=str(self._execution_repo_dir),
                capture_output=True, text=True, timeout=30,
            )
        except subprocess.CalledProcessError as e:
            logger.error("回滚 git reset 失败 (exit %d): %s", e.returncode, e.stderr)
        except Exception as e:
            logger.error("回滚失败: %s", e)

    def _print_summary(self) -> None:
        """打印最终摘要。"""
        s = self._state
        total = len(s.proposals)
        approved = len(s.approved_ids)
        rejected = len(s.rejected_ids)

        lines = [
            "",
            "=" * 60,
            "  自我迭代完成",
            f"  会话: {s.session_id}",
            f"  状态: {s.status}",
            f"  提案: {total} 条 (批准 {approved}, 拒绝 {rejected})",
            f"  执行前测试: {'通过' if s.pre_test_passed else '未通过/跳过'}",
            f"  执行后测试: {'通过' if s.post_test_passed else '未通过/跳过'}",
            f"  总花费: ${s.total_cost_usd:.2f}",
        ]

        if s.git_branch:
            lines.append(f"  Git 分支: {s.git_branch}")
        if s.runtime_dir:
            lines.append(f"  Runtime: {s.runtime_dir}")
        if s.workspace_dir:
            lines.append(f"  Workspace: {s.workspace_dir}")
        if s.handoff_dir:
            lines.append(f"  Handoff: {s.handoff_dir}")

        lines.append("=" * 60)
        sys.stderr.write("\n".join(lines) + "\n")
        sys.stderr.flush()

    def _self_improve_contract(self) -> TaskContract:
        """构建 self-improve 任务的 TaskContract。"""
        return TaskContract(
            source_goal="self improve",
            normalized_goal=f"self-improve-{self._state.session_id}",
            input_type=TaskInputType.NATURAL_LANGUAGE,
            task_type=TaskType.REFACTOR,
            data_risk=DataRisk.NONE,
            affected_areas=["backend"],
            document_paths=[],
        )

    def _restore_workspace_session(self) -> None:
        """从 runtime_dir 恢复隔离工作区会话。"""
        runtime_dir = getattr(self._state, "runtime_dir", "")
        if not runtime_dir:
            return
        try:
            session = self._workspace_manager.load_session(runtime_dir)
        except Exception as exc:
            logger.warning("恢复隔离工作区失败: %s", exc)
            return
        self._apply_workspace_session(session)

    def _ensure_workspace_session(self) -> None:
        """确保工作区会话存在，不存在则创建。"""
        if self._workspace_session is not None:
            return
        session = self._workspace_manager.create_session(self._source_repo_dir, self._self_improve_contract())
        self._apply_workspace_session(session)

    def _apply_workspace_session(self, session: WorkspaceSession) -> None:
        """应用工作区会话到控制器状态。"""
        self._workspace_session = session
        self._execution_repo_dir = session.layout.workspace.resolve()
        self._state.runtime_dir = str(session.layout.root)
        self._state.workspace_dir = str(session.layout.workspace)
        self._state.handoff_dir = str(session.layout.handoff)
        self._state.branch_names = list(session.branch_names)
        if session.primary_branch:
            self._state.git_branch = session.primary_branch

    def _record_pre_commit_hash(self) -> None:
        """记录改进前的 commit hash。"""
        try:
            hash_result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=str(self._source_repo_dir),
                capture_output=True,
                text=True,
                timeout=10,
            )
            if hash_result.returncode == 0:
                self._state.pre_commit_hash = hash_result.stdout.strip()
                logger.info(
                    "准备隔离工作区: %s (基于 %s)",
                    self._state.git_branch or "无分支",
                    self._state.pre_commit_hash[:8],
                )
        except Exception as exc:
            logger.warning("记录改进前 commit 失败: %s", exc)

    def _sync_source_to_workspace(self) -> None:
        """将源仓库的最新文件同步到 workspace（包括已修改和未跟踪的文件）。

        git worktree 基于源仓库的 HEAD 创建，但源仓库中可能有：
        - 已修改但未提交的已跟踪文件（git worktree 不会包含这些修改）
        - 新增的未跟踪文件（create_session 只在创建时同步一次）

        此方法确保 workspace 拥有与源仓库完全一致的最新内容。
        """
        if not self._workspace_session:
            return
        if self._execution_repo_dir == self._source_repo_dir:
            return  # 非隔离模式，无需同步

        workspace = self._execution_repo_dir
        source = self._source_repo_dir

        logger.info("同步源仓库最新文件到 workspace")

        # 同步已修改的已跟踪文件（unstaged + staged）
        try:
            dirty_count = 0
            for cmd in [["git", "diff", "--name-only"], ["git", "diff", "--cached", "--name-only"]]:
                result = subprocess.run(
                    cmd,
                    cwd=str(source),
                    capture_output=True, text=True, timeout=10,
                )
                for rel_path in result.stdout.strip().splitlines():
                    rel_path = rel_path.strip()
                    if not rel_path:
                        continue
                    src = source / rel_path
                    dst = workspace / rel_path
                    if src.is_file():
                        dst.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(src, dst)
                        dirty_count += 1
            if dirty_count:
                logger.info("同步 %d 个已修改文件到 workspace", dirty_count)
        except Exception as exc:
            logger.warning("同步已修改文件失败: %s", exc)

        # 同步未跟踪文件（workspace 创建后源仓库可能新增了未跟踪文件）
        try:
            result = subprocess.run(
                ["git", "ls-files", "--others", "--exclude-standard"],
                cwd=str(source),
                capture_output=True, text=True, timeout=10,
            )
            untracked = result.stdout.strip().splitlines() if result.stdout.strip() else []
            count = 0
            for rel_path in untracked:
                rel_path = rel_path.strip()
                if not rel_path:
                    continue
                src = source / rel_path
                dst = workspace / rel_path
                if src.is_file():
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src, dst)
                    count += 1
            if count:
                logger.info("同步 %d 个未跟踪文件到 workspace", count)
        except Exception as exc:
            logger.warning("同步未跟踪文件失败: %s", exc)

    def _merge_workspace_to_source(self) -> None:
        """将 workspace 中的改进合并回源仓库。

        策略：
        1. 在 workspace 中提交所有变更
        2. 尝试 git merge 将 workspace 分支合并到源仓库
        3. 若 merge 失败（冲突等），回退到文件级复制
        """
        if not self._workspace_session:
            return
        if self._execution_repo_dir == self._source_repo_dir:
            return  # 非隔离模式，改动已在源仓库中

        workspace = self._execution_repo_dir
        source = self._source_repo_dir

        logger.info("合并 workspace 改进到源仓库")

        # 1. 在 workspace 中提交所有变更
        committed = False
        try:
            subprocess.run(
                ["git", "add", "-A"],
                cwd=str(workspace),
                capture_output=True, text=True, timeout=30,
            )
            commit_result = subprocess.run(
                ["git", "commit", "-m", f"self-improve: {self._state.session_id}"],
                cwd=str(workspace),
                capture_output=True, text=True, timeout=30,
            )
            if commit_result.returncode == 0:
                committed = True
                logger.info("workspace 变更已提交")
            else:
                logger.info("workspace 无变更需要提交")
        except Exception as exc:
            logger.warning("workspace 提交失败: %s", exc)

        # 2. 尝试 git merge
        branch = self._workspace_session.primary_branch
        if committed and branch:
            try:
                merge_result = subprocess.run(
                    ["git", "merge", branch, "--no-edit"],
                    cwd=str(source),
                    capture_output=True, text=True, timeout=60,
                )
                if merge_result.returncode == 0:
                    logger.info("成功合并 workspace 分支 %s 到源仓库", branch)
                    return
                else:
                    logger.warning("git merge 失败（exit=%d）: %s", merge_result.returncode, (merge_result.stderr or "")[:500])
                    subprocess.run(
                        ["git", "merge", "--abort"],
                        cwd=str(source),
                        capture_output=True, text=True, timeout=10,
                    )
            except Exception as exc:
                logger.warning("git merge 异常: %s", exc)

        # 3. 回退到文件级同步：复制 workspace 中的项目文件
        logger.info("回退到文件级同步")
        count = 0
        for current_root, dirnames, filenames in os.walk(workspace):
            current_path = Path(current_root)
            rel_root = current_path.relative_to(workspace)
            dirnames[:] = [name for name in dirnames if name not in _FILE_SYNC_IGNORE_DIRS]

            for filename in filenames:
                if filename in _FILE_SYNC_IGNORE_FILES:
                    continue
                src_file = current_path / filename
                if not src_file.is_file():
                    continue
                dst_file = source / rel_root / filename
                dst_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src_file, dst_file)
                count += 1
        if count:
            logger.info("文件级同步: 复制了 %d 个文件到源仓库", count)
        else:
            logger.info("文件级同步: 无文件需要复制")

    def _discard_isolated_workspace(self) -> None:
        """回滚时丢弃隔离工作区。"""
        assert self._workspace_session is not None
        workspace = self._workspace_session.layout.workspace
        logger.info("回滚: 丢弃隔离工作区 %s", workspace)
        try:
            subprocess.run(
                ["git", "worktree", "remove", "--force", str(workspace)],
                cwd=str(self._source_repo_dir),
                capture_output=True,
                text=True,
                timeout=30,
                check=True,
            )
        except Exception as exc:
            logger.warning("git worktree remove 失败，回退到目录删除: %s", exc)
            shutil.rmtree(workspace, ignore_errors=True)
