"""Flow Orch：DAG 工作流编排引擎。

公开入口：
- run_workflow: 校验并执行工作流定义，返回 RunResult
- WorkflowDefinitionError / WorkflowInputError: 定义与输入异常
- RunResult / TaskResult / CallbackEvent: 数据模型
- RunTrace / AttemptRecord: collect_trace 开启后的内存运行轨迹
"""

from .engine import run_workflow
from .errors import (
    FlowOrchError,
    TaskExecutionError,
    WorkflowDefinitionError,
    WorkflowInputError,
)
from .models import (
    AttemptRecord,
    CallbackEvent,
    EventCallback,
    RunResult,
    RunTrace,
    TaskDef,
    TaskResult,
)

__all__ = [
    "run_workflow",
    "FlowOrchError",
    "WorkflowDefinitionError",
    "WorkflowInputError",
    "TaskExecutionError",
    "RunResult",
    "TaskResult",
    "TaskDef",
    "CallbackEvent",
    "EventCallback",
    "RunTrace",
    "AttemptRecord",
]
