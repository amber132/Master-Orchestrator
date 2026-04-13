"""插件注册系统：支持统一 provider 路由和可扩展的任务执行器。"""

from __future__ import annotations

from typing import Any, Dict, Protocol, Type, runtime_checkable

from .model import TaskNode, TaskResult


@runtime_checkable
class TaskExecutor(Protocol):
    """任务执行器协议：所有执行器必须实现此接口"""
    
    def execute(
        self,
        task: TaskNode,
        prompt: str,
        claude_config: Any,
        limits: Any,
        budget_tracker: Any,
        working_dir: str | None,
        on_progress: Any,
        audit_logger: Any = None,
        rate_limiter: Any = None,
    ) -> TaskResult:
        """
        执行任务并返回结果
        
        Args:
            task: 任务节点
            prompt: 渲染后的 prompt
            claude_config: 兼容历史命名。当前传入完整 Config 或 provider-specific config
            limits: 限制配置
            budget_tracker: 预算追踪器
            working_dir: 工作目录
            on_progress: 进度回调
            audit_logger: 审计日志记录器（可选）
            rate_limiter: 速率限制器（可选）
            
        Returns:
            TaskResult: 执行结果
        """
        ...


class ClaudeCliExecutor:
    """Claude CLI 执行器（默认）"""
    
    def execute(
        self,
        task: TaskNode,
        prompt: str,
        claude_config: Any,
        limits: Any,
        budget_tracker: Any,
        working_dir: str | None,
        on_progress: Any,
        audit_logger: Any = None,
        rate_limiter: Any = None,
    ) -> TaskResult:
        """通过 Claude CLI 执行任务"""
        from .claude_cli import run_claude_task

        resolved_config = getattr(claude_config, "claude", claude_config)
        return run_claude_task(
            task=task,
            prompt=prompt,
            claude_config=resolved_config,
            limits=limits,
            budget_tracker=budget_tracker,
            working_dir=working_dir,
            on_progress=on_progress,
            audit_logger=audit_logger,
            rate_limiter=rate_limiter,
        )


class CodexCliExecutor:
    """Codex CLI 执行器。"""

    def execute(
        self,
        task: TaskNode,
        prompt: str,
        claude_config: Any,
        limits: Any,
        budget_tracker: Any,
        working_dir: str | None,
        on_progress: Any,
        audit_logger: Any = None,
        rate_limiter: Any = None,
    ) -> TaskResult:
        from .codex_cli import run_codex_task

        resolved_config = getattr(claude_config, "codex", claude_config)
        result = run_codex_task(
            task=task,
            prompt=prompt,
            codex_config=resolved_config,
            limits=limits,
            budget_tracker=budget_tracker,
            working_dir=working_dir,
            on_progress=on_progress,
            audit_logger=audit_logger,
            rate_limiter=rate_limiter,
        )
        result.provider_used = "codex"
        return result


class AgentCliExecutor:
    """统一 provider 执行器，内部路由到 Claude 或 Codex。"""

    def execute(
        self,
        task: TaskNode,
        prompt: str,
        claude_config: Any,
        limits: Any,
        budget_tracker: Any,
        working_dir: str | None,
        on_progress: Any,
        audit_logger: Any = None,
        rate_limiter: Any = None,
    ) -> TaskResult:
        from .agent_cli import run_agent_task

        return run_agent_task(
            task=task,
            prompt=prompt,
            config=claude_config,
            limits=limits,
            budget_tracker=budget_tracker,
            working_dir=working_dir,
            on_progress=on_progress,
            audit_logger=audit_logger,
            rate_limiter=rate_limiter,
        )


class PluginRegistry:
    """插件注册表：管理所有任务执行器"""
    
    _executors: Dict[str, Type[TaskExecutor]] = {}
    _initialized = False
    
    @classmethod
    def _ensure_initialized(cls):
        """确保注册表已初始化"""
        if not cls._initialized:
            from .analysis_executor import AnalysisExecutor
            from .operation_executor import OperationExecutor

            cls.register("agent_cli", AgentCliExecutor)
            cls.register("claude_cli", ClaudeCliExecutor)
            cls.register("codex_cli", CodexCliExecutor)
            cls.register("analysis", AnalysisExecutor)
            cls.register("operation", OperationExecutor)
            cls._initialized = True
    
    @classmethod
    def register(cls, type_name: str, executor_cls: Type[TaskExecutor]):
        """
        注册执行器
        
        Args:
            type_name: 执行器类型名称
            executor_cls: 执行器类（必须实现 TaskExecutor 协议）
        """
        if not isinstance(executor_cls, type):
            raise TypeError(f"executor_cls 必须是类，而非 {type(executor_cls)}")
        
        # 检查是否实现了 TaskExecutor 协议
        if not hasattr(executor_cls, "execute"):
            raise TypeError(f"{executor_cls.__name__} 必须实现 execute() 方法")
        
        cls._executors[type_name] = executor_cls
    
    @classmethod
    def get_executor(cls, type_name: str) -> TaskExecutor:
        """
        获取执行器实例
        
        Args:
            type_name: 执行器类型名称
            
        Returns:
            TaskExecutor: 执行器实例
            
        Raises:
            KeyError: 如果执行器类型未注册
        """
        cls._ensure_initialized()
        
        if type_name not in cls._executors:
            raise KeyError(
                f"未注册的执行器类型: {type_name}。"
                f"已注册类型: {list(cls._executors.keys())}"
            )
        
        executor_cls = cls._executors[type_name]
        return executor_cls()
    
    @classmethod
    def list_types(cls) -> list[str]:
        """列出所有已注册的执行器类型"""
        cls._ensure_initialized()
        return list(cls._executors.keys())
