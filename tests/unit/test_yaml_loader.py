from __future__ import annotations

from pathlib import Path

import pytest


def test_load_yaml_dag(tmp_path: Path):
    """从 YAML 文件加载 DAG。"""
    yaml_content = """\
name: test-yaml-dag
max_parallel: 5

tasks:
  scan:
    prompt: "扫描代码库"
    model: haiku
    read_only: true
    is_critical: false

  fix:
    prompt: "修复发现的问题"
    depends_on: [scan]
    model: sonnet
    is_critical: true
"""
    yaml_file = tmp_path / "test.yaml"
    yaml_file.write_text(yaml_content, encoding="utf-8")

    from claude_orchestrator.dag_loader import load_dag
    dag = load_dag(str(yaml_file))

    assert dag.name == "test-yaml-dag"
    assert dag.max_parallel == 5
    assert "scan" in dag.tasks
    assert "fix" in dag.tasks
    assert dag.tasks["scan"].read_only is True
    assert dag.tasks["fix"].is_critical is True
    assert dag.tasks["fix"].depends_on == ["scan"]


def test_load_yml_extension(tmp_path: Path):
    """.yml 扩展名也能正确加载。"""
    yaml_content = """\
name: yml-test
tasks:
  t1:
    prompt: "test"
"""
    yml_file = tmp_path / "test.yml"
    yml_file.write_text(yaml_content, encoding="utf-8")

    from claude_orchestrator.dag_loader import load_dag
    dag = load_dag(str(yml_file))
    assert dag.name == "yml-test"


def test_yaml_dag_validation(tmp_path: Path):
    """YAML DAG 也经过依赖校验。"""
    yaml_content = """\
name: bad-deps
tasks:
  t1:
    prompt: "test"
    depends_on: [nonexistent]
"""
    yaml_file = tmp_path / "bad.yaml"
    yaml_file.write_text(yaml_content, encoding="utf-8")

    from claude_orchestrator.dag_loader import load_dag
    from claude_orchestrator.exceptions import DAGValidationError
    with pytest.raises(DAGValidationError):
        load_dag(str(yaml_file))


def test_yaml_empty_tasks(tmp_path: Path):
    """没有任务时报错。"""
    yaml_content = """\
name: empty
tasks: {}
"""
    yaml_file = tmp_path / "empty.yaml"
    yaml_file.write_text(yaml_content, encoding="utf-8")

    from claude_orchestrator.dag_loader import load_dag
    from claude_orchestrator.exceptions import DAGLoadError
    with pytest.raises(DAGLoadError):
        load_dag(str(yaml_file))
