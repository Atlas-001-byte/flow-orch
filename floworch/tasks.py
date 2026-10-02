"""内置任务执行逻辑：value 与 fail_now。"""

from typing import Any

from .errors import TaskExecutionError
from .models import TaskDef, TaskResult


def run_once(task: TaskDef, results: dict[str, TaskResult]) -> Any:
    """执行一次任务体，成功返回输出，失败抛 TaskExecutionError。"""
    if task.task_type == "value":
        if "value" in task.args:
            return task.args["value"]
        return results[task.args["ref"]].output
    if task.task_type == "fail_now":
        raise TaskExecutionError(f"任务 {task.id} 配置为 fail_now，执行即失败")
    # validator 已保证 task_type 合法，正常不可达。
    raise TaskExecutionError(f"任务 {task.id} 的类型不受支持: {task.task_type}")
