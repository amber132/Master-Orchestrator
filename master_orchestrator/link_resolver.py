"""Link resolver for task data dependencies.

This module provides utilities to resolve data links between tasks,
extracting values from upstream task outputs and injecting them into
downstream task contexts.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from master_orchestrator.model import TaskNode

logger = logging.getLogger(__name__)


def _extract_value(data: Any, path: str) -> Any:
    """Extract value from nested data structure using path notation.

    Supports:
    - Dot notation: "result.user_id"
    - Array indexing: "items[0].name"
    - Nested paths: "data.users[0].profile.name"

    Args:
        data: The data structure to extract from
        path: The path expression

    Returns:
        The extracted value

    Raises:
        KeyError: If path does not exist
        IndexError: If array index is out of bounds
        TypeError: If path traversal fails due to type mismatch
    """
    if not path:
        return data

    current = data

    # Split path into segments, handling array indices
    # e.g., "data.users[0].name" -> ["data", "users", "[0]", "name"]
    segments = re.split(r'\.|\[', path)

    for segment in segments:
        if not segment:
            continue

        # Handle array index: "[0]" -> "0"
        if segment.endswith(']'):
            index_str = segment[:-1]
            try:
                index = int(index_str)
                current = current[index]
            except (ValueError, TypeError, KeyError, IndexError) as e:
                raise KeyError(f"Failed to access index [{index_str}] in path '{path}': {e}") from e
        else:
            # Handle dict key
            if isinstance(current, dict):
                if segment not in current:
                    raise KeyError(f"Key '{segment}' not found in path '{path}'")
                current = current[segment]
            else:
                raise TypeError(f"Cannot access key '{segment}' on non-dict type {type(current).__name__} in path '{path}'")

    return current


def resolve_links(task: TaskNode, outputs: dict[str, Any]) -> dict[str, Any]:
    """Resolve data links from upstream task outputs.

    Args:
        task: The task node with link definitions
        outputs: Dictionary mapping task_id -> task output data

    Returns:
        Dictionary mapping input_key -> resolved value

    Example:
        >>> task = TaskNode(
        ...     id="task2",
        ...     prompt_template="Process user {user_id}",
        ...     links=[
        ...         LinkMapping(
        ...             upstream_task="task1",
        ...             output_path="result.user_id",
        ...             input_key="user_id"
        ...         )
        ...     ]
        ... )
        >>> outputs = {"task1": {"result": {"user_id": 123}}}
        >>> resolve_links(task, outputs)
        {'user_id': 123}
    """
    resolved: dict[str, Any] = {}

    for link in task.links:
        upstream_id = link.upstream_task
        output_path = link.output_path
        input_key = link.input_key

        # Check if upstream task output exists
        if upstream_id not in outputs:
            logger.warning(
                f"Task '{task.id}': upstream task '{upstream_id}' not found in outputs, "
                f"skipping link for input_key '{input_key}'"
            )
            continue

        upstream_output = outputs[upstream_id]

        # Extract value from upstream output
        try:
            value = _extract_value(upstream_output, output_path)
            resolved[input_key] = value
            logger.debug(
                f"Task '{task.id}': resolved link '{input_key}' = {value!r} "
                f"from '{upstream_id}.{output_path}'"
            )
        except (KeyError, IndexError, TypeError) as e:
            logger.error(
                f"Task '{task.id}': failed to extract '{output_path}' from "
                f"upstream task '{upstream_id}': {e}"
            )
            # Continue processing other links even if one fails
            continue

    return resolved


def inject_link_context(prompt: str, resolved: dict[str, Any]) -> str:
    """Inject resolved link data into prompt.

    Appends a section with upstream data in JSON format to the end of the prompt.

    Args:
        prompt: The original prompt template
        resolved: Dictionary of resolved link values

    Returns:
        The prompt with injected context

    Example:
        >>> prompt = "Process the user data"
        >>> resolved = {"user_id": 123, "username": "alice"}
        >>> inject_link_context(prompt, resolved)
        'Process the user data\\n\\n## 上游数据\\n{"user_id": 123, "username": "alice"}'
    """
    if not resolved:
        return prompt

    # Serialize resolved data to JSON
    try:
        json_data = json.dumps(resolved, ensure_ascii=False, indent=2)
    except (TypeError, ValueError) as e:
        logger.error(f"Failed to serialize resolved data to JSON: {e}")
        # Fallback to repr if JSON serialization fails
        json_data = repr(resolved)

    # Append context section
    context_section = f"\n\n## 上游数据\n{json_data}"
    return prompt + context_section
