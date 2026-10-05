"""DAG 调度引擎：并发执行、并发上限、失败重试、单次尝试超时、工作流总时限与下游跳过。"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timezone
from typing import Any, Optional

from .errors import TaskExecutionError
from .models import CallbackEvent, EventCallback, RunResult, TaskDef, TaskResult
from .tasks import run_once
from .validator import build_tasks

WORKFLOW_TIMEOUT_CODE = "WORKFLOW_TIMEOUT"


class _AttemptTimeout(Exception):
    """单次尝试超过 timeout_seconds；结果 error.code 固定为 TASK_TIMEOUT。"""

    code = "TASK_TIMEOUT"

    def __init__(self, task_id: str, attempt: int, timeout_seconds: float):
        self.message = (
            f"任务 {task_id} 第 {attempt} 次尝试超过 {timeout_seconds} 秒未完成"
        )
        super().__init__(self.message)


class _RunTimedOut(Exception):
    """工作流总时限已到，尝试不再继续；任务结果统一记 WORKFLOW_TIMEOUT。"""


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


def _run_attempt(
    task: TaskDef,
    results: dict[str, TaskResult],
    attempt: int,
    run_deadline: Optional[float] = None,
) -> Any:
    """执行一次任务体，成功返回输出。

    失败抛 TaskExecutionError；超过任务 timeout_seconds 抛 _AttemptTimeout；
    工作流总时限先到抛 _RunTimedOut。两者都未配置时保持同步直调；配置任一
    时限时在守护线程中执行任务体，join 时长取两者剩余的较小值（总时限为 0
    时由调用方在进入前拦截）。到点未完成即中止可中断等待（sleep）并遗弃
    任务体线程（守护线程，不阻塞进程退出），晚到的结果不会被采纳，因此
    不可能误报成功。timeout_seconds 为 0 时不执行任务体，立即判任务超时。
    """
    timeout = task.timeout_seconds

    run_remaining = (
        max(0.0, run_deadline - time.monotonic()) if run_deadline is not None else None
    )
    if run_remaining is not None and run_remaining <= 0:
        raise _RunTimedOut

    if timeout is None and run_deadline is None:
        return run_once(task, results)

    if timeout is not None and timeout <= 0:
        raise _AttemptTimeout(task.id, attempt, timeout)
    if timeout is None:
        join_seconds = run_remaining
        run_bound_fires_first = True
    elif run_remaining is None:
        join_seconds = timeout
        run_bound_fires_first = False
    else:
        # 两者重合时按总时限到期处理（WORKFLOW_TIMEOUT 优先于 TASK_TIMEOUT）。
        run_bound_fires_first = run_remaining <= timeout
        join_seconds = run_remaining if run_bound_fires_first else timeout

    cancel = threading.Event()
    holder: dict[str, Any] = {}

    def worker() -> None:
        try:
            holder["output"] = run_once(task, results, cancel)
        except BaseException as exc:  # noqa: BLE001 - 交回主线程按原语义抛出
            holder["exc"] = exc

    runner = threading.Thread(
        target=worker, name=f"attempt-{task.id}-{attempt}", daemon=True
    )
    runner.start()
    runner.join(join_seconds)
    if runner.is_alive():
        # 到截止仍未完成：中止可中断等待（sleep），不再 join 该线程。
        cancel.set()
        if run_bound_fires_first:
            raise _RunTimedOut
        raise _AttemptTimeout(task.id, attempt, timeout)
    if "exc" in holder:
        raise holder["exc"]
    return holder["output"]


def run_workflow(workflow: dict, callback: Optional[EventCallback] = None) -> RunResult:
    """校验并执行一个工作流定义。

    workflow 为已解析的 JSON 对象，含 name、tasks，可选 max_concurrency 与
    顶层 timeout_seconds。定义无效抛 WorkflowDefinitionError；返回 RunResult
    汇总每个任务的结果。相互独立的任务在线程池中并发执行；配置
    max_concurrency 时，同时占用额度的任务不超过该值——额度自 task_started
    起占用，重试等待期间不释放，直到 task_succeeded、task_failed 或
    task_skipped 才释放。

    配置顶层 timeout_seconds 后，总时限从校验完成、执行开始时起算：到期不再
    启动新任务/新尝试/重试等待，等待中的 sleep 立即结束；已成功任务保留原
    输出，其余未进入终态的任务统一记 failed、error.code 为 WORKFLOW_TIMEOUT，
    整体 status 为 failed，并补发一次 run_timed_out 回调。省略时不限总时长，
    其余语义完全不变。
    """
    tasks, max_concurrency, run_timeout = build_tasks(workflow)
    name = workflow["name"]

    # 总时限从校验完成、执行开始时起算（单调时钟，不受系统时钟回调影响）。
    run_started = time.monotonic()
    run_deadline = (
        run_started + run_timeout if run_timeout is not None else None
    )

    def expired() -> bool:
        return run_deadline is not None and time.monotonic() >= run_deadline

    # 跨线程共享的"运行已超时"标志：无论主调度循环的 wait 先到点，还是某个
    # 工作线程在重试等待/尝试边界先发现到期，都置位它，保证收口口径唯一——
    # 不会出现线程已返回 WORKFLOW_TIMEOUT 而主循环把它当普通完成收走、
    # 最终漏发 run_timed_out 的竞态。
    timed_out_event = threading.Event()

    def mark_timed_out_if_expired() -> bool:
        if expired():
            timed_out_event.set()
            return True
        return False

    timeout_error = {
        "code": WORKFLOW_TIMEOUT_CODE,
        "message": (
            f"工作流总时限 {run_timeout} 秒已到，任务在终止前未进入终态"
            if run_timeout is not None
            else "工作流运行超过总时限"
        ),
    }

    def timeout_result(attempts: int) -> TaskResult:
        return TaskResult(
            status="failed",
            attempts=attempts,
            output=None,
            error=dict(timeout_error),
        )

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
        """在工作线程中执行单个任务，含重试循环。

        总时限到期时不补发任何任务终态事件，直接返回 WORKFLOW_TIMEOUT 结果；
        attempts 为已经实际发出 task_started 的尝试次数。
        """
        attempts_started = 0
        for attempt in range(1, task.max_attempts + 1):
            if mark_timed_out_if_expired():
                return timeout_result(attempts_started)
            attempts_started = attempt
            with lock:
                emit("task_started", task.id, attempt)
            try:
                output = _run_attempt(task, results, attempt, run_deadline)
            except _RunTimedOut:
                timed_out_event.set()
                return timeout_result(attempts_started)
            except (TaskExecutionError, _AttemptTimeout) as exc:
                if mark_timed_out_if_expired():
                    return timeout_result(attempts_started)
                if attempt < task.max_attempts:
                    # 等待发生在 task_retrying 之后、下一次 task_started 之前。
                    wait_seconds = _retry_wait_seconds(task, attempt)
                    with lock:
                        emit("task_retrying", task.id, attempt, wait_seconds)
                    if wait_seconds > 0:
                        if run_deadline is not None:
                            # 总时限先到则等待立即结束；否则等满退避秒数。
                            # 未 set 的 Event.wait 即可限时等待的 sleep。
                            run_remaining = max(
                                0.0, run_deadline - time.monotonic()
                            )
                            threading.Event().wait(min(wait_seconds, run_remaining))
                            if mark_timed_out_if_expired():
                                return timeout_result(attempts_started)
                        else:
                            time.sleep(wait_seconds)
                    continue
                with lock:
                    emit("task_failed", task.id, attempt)
                return TaskResult(
                    status="failed",
                    attempts=attempt,
                    output=None,
                    error={"code": exc.code, "message": exc.message},
                )
            else:
                # 尝试在时限内正常返回即为成功；不再复查是否已临界到期，
                # 否则会把时限内完成的成功误记为 WORKFLOW_TIMEOUT。
                with lock:
                    emit("task_succeeded", task.id, attempt)
                return TaskResult(status="success", attempts=attempt, output=output)
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
            # 并发额度由在途 future 数体现：任务自提交（task_started）起
            # 占用一份额度，重试等待也在其 future 内，直到终态事件后
            # future 完成才释放。skipped 任务不进线程池，不占额度。
            # 到点后不再放行 ready、不再提交新任务（时钟闸门）。
            while ready and not expired() and (
                max_concurrency is None or len(futures) < max_concurrency
            ):
                submit(ready.pop())

            if not futures:
                # 无在途任务：全部已进入终态则正常收口；否则（仍有 ready
                # 或结果未收齐）只可能是到点后无法再启动，按超时收口。
                if (
                    run_deadline is not None
                    and expired()
                    and (ready or len(results) < len(tasks))
                ):
                    timed_out_event.set()
                break

            if run_deadline is None:
                done, _ = wait(set(futures), return_when="FIRST_COMPLETED")
            else:
                # 最多阻塞到总时限截止；超时返回空集即运行到期。
                done, _ = wait(
                    set(futures),
                    timeout=max(0.0, run_deadline - time.monotonic()),
                    return_when="FIRST_COMPLETED",
                )
                if not done:
                    timed_out_event.set()
                    break

            for future in done:
                tid = futures.pop(future)
                # 工作线程若先感知到期会把 timed_out_event 置位并返回
                # WORKFLOW_TIMEOUT；到点前完成的仍是原终态结果。
                results[tid] = future.result()
                for child in dependents[tid]:
                    remaining_deps[child] -= 1
                    if remaining_deps[child] == 0:
                        ready.append(child)

            if timed_out_event.is_set():
                break

        if timed_out_event.is_set():
            # 临界时刻已完成的在途任务采纳其终态结果（终态事件已发过）；
            # 仍在途的任务会在尝试 join / 重试等待处感知到期并很快返回
            # WORKFLOW_TIMEOUT（任务体守护线程被遗弃，不阻塞线程池收口）。
            while futures:
                done, _ = wait(set(futures), return_when="FIRST_COMPLETED")
                for future in done:
                    tid = futures.pop(future)
                    results[tid] = future.result()

    if timed_out_event.is_set():
        # 从未进入执行/跳过流程的任务：attempts 为 0，得到同一 WORKFLOW_TIMEOUT。
        for task in tasks:
            if task.id not in results:
                results[task.id] = timeout_result(0)
        with lock:
            emit("run_timed_out", "", 0, None)

    ordered = {task.id: results[task.id] for task in tasks}
    overall_failed = timed_out_event.is_set() or any(
        r.status == "failed" for r in ordered.values()
    )
    return RunResult(
        run_id=name,
        status="failed" if overall_failed else "success",
        results=ordered,
    )
