"""内置任务执行逻辑：value、fail_now 与 sleep。"""

import threading
from typing import Any, Optional

from .errors import TaskExecutionError
from .models import TaskDef, TaskResult


def run_once(
    task: TaskDef,
    results: dict[str, TaskResult],
    cancel: Optional[threading.Event] = None,
) -> Any:
    """执行一次任务体，成功返回输出，失败抛 TaskExecutionError。

    cancel 被设置时，可中断等待的任务体应立即结束等待（sleep 任务
    直接抛 TaskExecutionError，由上层按超时处理）。
    """
    if task.task_type == "value":
        if "value" in task.args:
            return task.args["value"]
        return results[task.args["ref"]].output
    if task.task_type == "fail_now":
        raise TaskExecutionError(f"任务 {task.id} 配置为 fail_now，执行即失败")
    if task.task_type == "sleep":
        seconds = float(task.args["seconds"])
        if seconds > 0:
            if cancel is None:
                cancel = threading.Event()
            # Event.wait 被 set 时立即返回 True，sleep 等待随超时中止。
            if cancel.wait(seconds):
                raise TaskExecutionError(f"任务 {task.id} 的 sleep 等待被中止")
        return task.args["output"]
    # validator 已保证 task_type 合法，正常不可达。
    raise TaskExecutionError(f"任务 {task.id} 的类型不受支持: {task.task_type}")
