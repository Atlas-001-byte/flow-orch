"""DAG 调度与任务执行。"""

from __future__ import annotations

import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from .definition import TaskDefinition, WorkflowDefinition, parse_definition

EventCallback = Callable[[dict[str, Any]], None]

BUILTIN_TASK_FAILED = "BUILTIN_TASK_FAILED"


@dataclass
class TaskResult:
    status: str  # "succeeded" | "failed" | "skipped"
    attempts: int
    output: Any = None
    error: dict[str, str] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "attempts": self.attempts,
            "output": self.output,
            "error": self.error,
        }


@dataclass
class RunResult:
    run_id: str
    status: str  # "succeeded" | "failed"
    results: dict[str, TaskResult]

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "results": {tid: r.to_dict() for tid, r in self.results.items()},
        }


class _BuiltinTaskError(Exception):
    """内置任务执行失败。"""


def run_workflow(
    definition: str | dict[str, Any],
    on_event: EventCallback | None = None,
) -> RunResult:
    """校验并执行工作流，返回 RunResult。

    定义无效时抛出 WorkflowDefinitionError；执行失败不抛异常，
    体现在 RunResult.status 与各任务结果中。
    """
    defn = parse_definition(definition)
    return _execute(defn, on_event)


def _execute(defn: WorkflowDefinition, on_event: EventCallback | None) -> RunResult:
    emit_lock = threading.Lock()

    def emit(event: str, task_id: str, attempt: int) -> None:
        if on_event is None:
            return
        payload = {
            "event": event,
            "task_id": task_id,
            "attempt": attempt,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        with emit_lock:
            on_event(payload)

    tasks_by_id = {t.id: t for t in defn.tasks}
    dependents: dict[str, list[str]] = {t.id: [] for t in defn.tasks}
    unresolved: dict[str, set[str]] = {}
    for task in defn.tasks:
        unresolved[task.id] = set(task.depends_on)
        for dep in task.depends_on:
            dependents[dep].append(task.id)

    results: dict[str, TaskResult] = {}
    ready = [t.id for t in defn.tasks if not t.depends_on]
    running = {}

    with ThreadPoolExecutor(max_workers=max(1, min(32, len(defn.tasks)))) as pool:
        while ready or running:
            for tid in ready:
                future = pool.submit(_run_task, tasks_by_id[tid], results, emit)
                running[future] = tid
            ready = []

            done, _ = wait(running, return_when=FIRST_COMPLETED)
            resolved_queue = []
            for future in done:
                tid = running.pop(future)
                results[tid] = future.result()
                resolved_queue.append(tid)

            # 传播完成状态：全部依赖成功则就绪，任一依赖失败/跳过则跳过。
            while resolved_queue:
                tid = resolved_queue.pop()
                for downstream in dependents[tid]:
                    if downstream in results:
                        continue
                    unresolved[downstream].discard(tid)
                    if unresolved[downstream]:
                        continue
                    deps = tasks_by_id[downstream].depends_on
                    if all(results[d].status == "succeeded" for d in deps):
                        ready.append(downstream)
                    else:
                        emit("task_skipped", downstream, 0)
                        results[downstream] = TaskResult("skipped", 0, None, None)
                        resolved_queue.append(downstream)

    status = (
        "succeeded"
        if all(r.status == "succeeded" for r in results.values())
        else "failed"
    )
    return RunResult(run_id=defn.name, status=status, results=results)


def _run_task(
    task: TaskDefinition,
    results: dict[str, TaskResult],
    emit: Callable[[str, str, int], None],
) -> TaskResult:
    for attempt in range(1, task.max_attempts + 1):
        emit("task_started", task.id, attempt)
        try:
            output = _execute_builtin(task, results)
        except Exception as exc:
            if attempt < task.max_attempts:
                emit("task_retrying", task.id, attempt)
                if task.retry_delay_seconds > 0:
                    time.sleep(task.retry_delay_seconds)
                continue
            emit("task_failed", task.id, attempt)
            return TaskResult(
                "failed",
                attempt,
                None,
                {"code": BUILTIN_TASK_FAILED, "message": str(exc)},
            )
        emit("task_succeeded", task.id, attempt)
        return TaskResult("succeeded", attempt, output, None)
    raise AssertionError("unreachable")


def _execute_builtin(task: TaskDefinition, results: dict[str, TaskResult]) -> Any:
    if task.task_type == "value":
        if "value" in task.args:
            return task.args["value"]
        ref = task.args["ref"]
        return results[ref].output
    if task.task_type == "fail_now":
        message = task.args.get("message") or f"任务 '{task.id}' 按定义失败（fail_now）"
        raise _BuiltinTaskError(message)
    raise AssertionError(f"未知 task_type：{task.task_type}")
