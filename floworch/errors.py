"""Flow Orch 的错误类型与错误码。"""

from __future__ import annotations


class WorkflowDefinitionError(Exception):
    """工作流定义无效（或输入不可读）时抛出的唯一异常类型。

    ``code`` 为稳定错误码，``message`` 面向用户描述问题。
    """

    INVALID_JSON = "INVALID_JSON"
    INVALID_SCHEMA = "INVALID_SCHEMA"
    DUPLICATE_TASK_ID = "DUPLICATE_TASK_ID"
    UNKNOWN_DEPENDENCY = "UNKNOWN_DEPENDENCY"
    DEPENDENCY_CYCLE = "DEPENDENCY_CYCLE"
    INVALID_RETRY_POLICY = "INVALID_RETRY_POLICY"
    INVALID_ARGS = "INVALID_ARGS"
    INPUT_READ_ERROR = "INPUT_READ_ERROR"

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")
