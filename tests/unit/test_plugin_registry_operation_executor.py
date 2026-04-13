from codex_orchestrator.analysis_executor import AnalysisExecutor
from codex_orchestrator.operation_executor import OperationExecutor
from codex_orchestrator.plugin_registry import PluginRegistry


def test_plugin_registry_exposes_analysis_executor() -> None:
    executor = PluginRegistry.get_executor("analysis")

    assert isinstance(executor, AnalysisExecutor)
    assert "analysis" in PluginRegistry.list_types()


def test_plugin_registry_exposes_operation_executor() -> None:
    executor = PluginRegistry.get_executor("operation")

    assert isinstance(executor, OperationExecutor)
    assert "operation" in PluginRegistry.list_types()
