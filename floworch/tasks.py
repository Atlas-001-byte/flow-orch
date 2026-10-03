"""内置任务执行逻辑：value、fail_now 与 sleep。"""

import threading
import time
from typing import Any, Optional

from .errors import TaskExecutionError
from .models import TaskDef, TaskResult


def run_once(
    task: TaskDef,
    results: dict[str, TaskResult],
    cancel_event: Optional[threading.Event] = None,
) -> Any:
    """执行一次任务体，成功返回输出，失败抛 TaskExecutionError。

    cancel_event 被设置时，可中断等待中的任务体（用于超时中止）；
    普通任务不使用该事件。
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
            if cancel_event is None:
                time.sleep(seconds)
            else:
                # Event.wait 可被超时中止信号立即打断，不会睡满原定秒数。
                cancel_event.wait(seconds)
        return task.args["output"]
    # validator 已保证 task_type 合法，正常不可达。
    raise TaskExecutionError(f"任务 {task.id} 的类型不受支持: {task.task_type}")
