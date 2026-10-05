"""Flow Orch 错误类型与错误码。"""


class FlowOrchError(Exception):
    """所有 floworch 异常的基类。"""


class WorkflowDefinitionError(FlowOrchError):
    """工作流定义无效。

    code 取值：INVALID_JSON、INVALID_SCHEMA、DUPLICATE_TASK_ID、
    UNKNOWN_DEPENDENCY、DEPENDENCY_CYCLE、INVALID_RETRY_POLICY、
    INVALID_TIMEOUT_POLICY、INVALID_RUN_TIMEOUT_POLICY、
    INVALID_CONCURRENCY_POLICY、INVALID_PRIORITY_POLICY、INVALID_ARGS。
    """

    def __init__(self, code, message=""):
        self.code = code
        self.message = message or code
        super().__init__(f"{code}: {self.message}")


class WorkflowInputError(FlowOrchError):
    """输入文件不可读，code 固定为 INPUT_READ_ERROR。"""

    def __init__(self, message=""):
        self.code = "INPUT_READ_ERROR"
        self.message = message or self.code
        super().__init__(f"{self.code}: {self.message}")


class TaskExecutionError(FlowOrchError):
    """内置任务执行失败，结果中 error.code 固定为 BUILTIN_TASK_FAILED。"""

    def __init__(self, message=""):
        self.code = "BUILTIN_TASK_FAILED"
        self.message = message or self.code
        super().__init__(f"{self.code}: {self.message}")
