"""读取工作流 JSON 文件。"""

import json

from .errors import WorkflowDefinitionError, WorkflowInputError


def load_workflow_file(path: str):
    """读取并解析 JSON 文件。

    文件不可读抛 WorkflowInputError（INPUT_READ_ERROR）；
    JSON 非法抛 WorkflowDefinitionError（INVALID_JSON）。
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read()
    except OSError as exc:
        raise WorkflowInputError(f"无法读取文件 {path}: {exc}") from exc
    return parse_workflow_json(raw)


def parse_workflow_json(raw: str):
    """解析 JSON 字符串，非法时抛 INVALID_JSON。"""
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise WorkflowDefinitionError(
            "INVALID_JSON", f"JSON 解析失败（第 {exc.lineno} 行第 {exc.colno} 列）"
        ) from exc
