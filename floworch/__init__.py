"""Flow Orch：DAG 工作流编排引擎。

公开入口：
- run_workflow: 校验并执行工作流定义，返回 RunResult
- WorkflowDefinitionError / WorkflowInputError: 定义与输入异常
- RunResult / TaskResult / CallbackEvent: 数据模型
"""

from .engine import run_workflow
from .errors import (
    FlowOrchError,
    TaskExecutionError,
    WorkflowDefinitionError,
    WorkflowInputError,
)
from .models import CallbackEvent, EventCallback, RunResult, TaskDef, TaskResult

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
]
