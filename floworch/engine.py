"""DAG 调度引擎：并发执行、失败重试与下游跳过。"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timezone
from typing import Optional

from .errors import TaskExecutionError
from .models import CallbackEvent, EventCallback, RunResult, TaskDef, TaskResult
from .tasks import run_once
from .validator import build_tasks


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _retry_wait_seconds(task: TaskDef, attempt: int) -> float:
    """第 attempt 次失败后、准备第 attempt+1 次尝试前的实际等待秒数。

    基础 delay 乘以 multiplier 的 attempt-1 次方；配置上限后取计算值
    与上限的较小值。delay 为 0 时结果恒为 0（multiplier 已由校验
    保证为有限值，不会产生 NaN/inf）。
    """
    delay = task.retry_delay_seconds * (
        task.retry_backoff_multiplier ** (attempt - 1)
    )
    if task.max_retry_delay_seconds is not None:
        delay = min(delay, task.max_retry_delay_seconds)
    return delay


def _run_attempt(task: TaskDef, results: dict[str, TaskResult]):
    """执行一次任务体，返回 ("success", output) 或 ("failed", code, message)。

    timeout_seconds 为 None 时直接执行；为 0 时不执行任务体立即超时；
    否则在独立线程中运行并以 join 截止，到点未结束则设置取消事件中止
    可中断等待（如 sleep），本次尝试记 TASK_TIMEOUT。超时后任务体线程
    可能仍在运行，但其迟到结果一律忽略，不会误报成功。
    """
    timeout = task.timeout_seconds
    if timeout is None:
        try:
            return ("success", run_once(task, results))
        except TaskExecutionError as exc:
            return ("failed", exc.code, exc.message)

    if timeout == 0:
        return (
            "failed",
            "TASK_TIMEOUT",
            f"任务 {task.id} 的 timeout_seconds 为 0，未执行任务体即超时",
        )

    cancel = threading.Event()
    box: dict = {}

    def target() -> None:
        try:
            box["output"] = run_once(task, results, cancel)
        except TaskExecutionError as exc:
            box["error"] = exc

    worker = threading.Thread(target=target, daemon=True)
    worker.start()
    worker.join(timeout)
    if worker.is_alive():
        # 截止：立即打断可中断的等待；线程迟到的任何结果都被丢弃。
        cancel.set()
        return (
            "failed",
            "TASK_TIMEOUT",
            f"任务 {task.id} 的本次尝试在 {timeout} 秒内未完成",
        )
    if "error" in box:
        exc = box["error"]
        return ("failed", exc.code, exc.message)
    return ("success", box["output"])


def run_workflow(workflow: dict, callback: Optional[EventCallback] = None) -> RunResult:
    """校验并执行一个工作流定义。

    workflow 为已解析的 JSON 对象，含 name、tasks。定义无效抛
    WorkflowDefinitionError；返回 RunResult 汇总每个任务的结果。
    相互独立的任务在线程池中并发执行。
    """
    tasks: list[TaskDef] = build_tasks(workflow)
    name = workflow["name"]

    # 结果按定义顺序输出；先建好槽位，任务完成后按 id 回填。
    results: dict[str, TaskResult] = {}
    remaining_deps = {task.id: len(task.depends_on) for task in tasks}
    dependents: dict[str, list[str]] = {task.id: [] for task in tasks}
    for task in tasks:
        for dep in task.depends_on:
            dependents[dep].append(task.id)

    by_id = {task.id: task for task in tasks}
    lock = threading.Lock()  # 保护回调与 results 的串行可见性

    def emit(
        event: str, task_id: str, attempt: int, wait_seconds: Optional[float] = None
    ) -> None:
        if callback is not None:
            callback(
                CallbackEvent(event, task_id, attempt, _utc_now_iso(), wait_seconds)
            )

    def execute_task(task: TaskDef) -> TaskResult:
        """在工作线程中执行单个任务，含重试循环。"""
        for attempt in range(1, task.max_attempts + 1):
            with lock:
                emit("task_started", task.id, attempt)
            outcome = _run_attempt(task, results)
            if outcome[0] == "failed":
                _, code, message = outcome
                if attempt < task.max_attempts:
                    # 等待发生在 task_retrying 之后、下一次 task_started 之前。
                    wait_seconds = _retry_wait_seconds(task, attempt)
                    with lock:
                        emit("task_retrying", task.id, attempt, wait_seconds)
                    if wait_seconds > 0:
                        time.sleep(wait_seconds)
                    continue
                with lock:
                    emit("task_failed", task.id, attempt)
                return TaskResult(
                    status="failed",
                    attempts=attempt,
                    output=None,
                    error={"code": code, "message": message},
                )
            with lock:
                emit("task_succeeded", task.id, attempt)
            return TaskResult(status="success", attempts=attempt, output=outcome[1])
        # 理论上不可达（max_attempts >= 1 已由校验保证）。
        raise RuntimeError(f"任务 {task.id} 的重试循环异常退出")

    def skip_task(task: TaskDef) -> TaskResult:
        with lock:
            emit("task_skipped", task.id, 0)
        return TaskResult(status="skipped", attempts=0, output=None, error=None)

    ready = [task.id for task in tasks if not task.depends_on]
    max_workers = max(1, len(tasks))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {}

        def submit(tid: str) -> None:
            task = by_id[tid]
            deps_succeeded = all(
                results[dep].status == "success" for dep in task.depends_on
            )
            if deps_succeeded:
                futures[pool.submit(execute_task, task)] = tid
            else:
                # 依赖中有 failed/skipped：本任务及（稍后）其下游一律跳过。
                results[tid] = skip_task(task)
                for child in dependents[tid]:
                    remaining_deps[child] -= 1
                    if remaining_deps[child] == 0:
                        ready.append(child)

        while True:
            while ready:
                submit(ready.pop())

            # 跳过的任务可能在不经过线程池的情况下连锁放行下游。
            if not futures:
                break

            done, _ = wait(set(futures), return_when="FIRST_COMPLETED")
            for future in done:
                tid = futures.pop(future)
                results[tid] = future.result()
                for child in dependents[tid]:
                    remaining_deps[child] -= 1
                    if remaining_deps[child] == 0:
                        ready.append(child)

    ordered = {task.id: results[task.id] for task in tasks}
    overall_failed = any(r.status == "failed" for r in ordered.values())
    return RunResult(
        run_id=name,
        status="failed" if overall_failed else "success",
        results=ordered,
    )
