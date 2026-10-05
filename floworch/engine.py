"""DAG 调度引擎：并发执行、并发上限、失败重试、单次尝试超时、工作流总时限与下游跳过。"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timezone
from typing import Any, Optional

from .errors import TaskExecutionError
from .models import (
    AttemptRecord,
    CallbackEvent,
    EventCallback,
    RunResult,
    RunTrace,
    TaskDef,
    TaskResult,
)
from .tasks import run_once
from .validator import build_tasks

# 工作流总时限到期后，未进入终态的任务统一使用的错误码。
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
    """工作流总时限到期：当前尝试或重试等待立即中止。"""


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
    deadline: Optional[float],
) -> Any:
    """执行一次任务体，成功返回输出。

    失败抛 TaskExecutionError；超过任务级 timeout_seconds 抛 _AttemptTimeout；
    到达工作流总时限截止抛 _RunTimedOut。未配置任何超时时保持同步直调；
    任务级 timeout_seconds 为 0 时不执行任务体，立即判超时。超时或总时限
    到期后任务体线程被遗弃（守护线程，不阻塞进程退出），其晚到的结果不会
    被采纳，因此不可能误报成功。
    """
    timeout = task.timeout_seconds

    if deadline is not None and time.monotonic() >= deadline:
        raise _RunTimedOut
    if timeout is not None and timeout <= 0:
        raise _AttemptTimeout(task.id, attempt, timeout)

    # 既未配置任务级超时也未配置总时限：同步直调，保持原有调用路径。
    if timeout is None and deadline is None:
        return run_once(task, results)

    # 每次尝试单独持有取消事件：到任一截止未完成都会 set 它，
    # 使可中断的 sleep 立即结束；不复用，避免污染后续重试。
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
    if timeout is not None and deadline is not None:
        # 两个截止同时存在时取较早者，保证 sleep 在总时限点也能立即结束。
        join_seconds: Optional[float] = min(
            timeout, max(0.0, deadline - time.monotonic())
        )
    elif timeout is not None:
        join_seconds = timeout
    else:
        join_seconds = max(0.0, deadline - time.monotonic())
    runner.join(join_seconds)
    # 先判总时限：join 等到截止点即到期，与任务体线程是否恰在此微窗口内
    # 结束无关——其晚到结果一律不采纳（与任务级超时的遗弃策略一致）。
    if deadline is not None and time.monotonic() >= deadline:
        cancel.set()
        raise _RunTimedOut
    if runner.is_alive():
        # 任务级超时到截止仍未完成：中止可中断等待（sleep），不再 join。
        cancel.set()
        raise _AttemptTimeout(task.id, attempt, timeout)
    if "exc" in holder:
        raise holder["exc"]
    return holder["output"]


def run_workflow(
    workflow: dict,
    callback: Optional[EventCallback] = None,
    collect_trace: bool = False,
) -> RunResult:
    """校验并执行一个工作流定义。

    workflow 为已解析的 JSON 对象，含 name、tasks，可选 max_concurrency 与
    顶层 timeout_seconds。定义无效抛 WorkflowDefinitionError；返回 RunResult
    汇总每个任务的结果。相互独立的任务在线程池中并发执行；配置
    max_concurrency 时，同时占用额度的任务不超过该值——额度自 task_started
    起占用，重试等待期间不释放，直到 task_succeeded、task_failed 或
    task_skipped 才释放。

    配置顶层 timeout_seconds 后，从校验完成、执行开始时起算总时限：到期不再
    启动新任务、新尝试或重试等待，正在等待的 sleep 立即结束；已成功任务保留
    原输出，其余未进入终态的任务统一记 failed（error.code 为 WORKFLOW_TIMEOUT，
    attempts 为已经实际开始的尝试次数），整体 status 为 failed。

    collect_trace 为 True 时，RunResult.trace 为本次运行的内存轨迹
    （RunTrace：起止时间、按发出顺序与回调逐条一致的事件、每次已开始
    尝试的记录），正常结束与超时收口均返回完整轨迹；为 False 时
    trace 为 None，行为与此前完全一致。
    """
    tasks, max_concurrency, run_timeout = build_tasks(workflow)
    name = workflow["name"]

    # 轨迹起点与总时限同一基准：校验完成、执行开始。
    trace_started_at = _utc_now_iso()
    trace_events: list[CallbackEvent] = []
    trace_attempts: list[AttemptRecord] = []

    # 总时限从校验完成、执行开始时起算；省略时 deadline 为 None（不限时）。
    deadline: Optional[float] = (
        None if run_timeout is None else time.monotonic() + run_timeout
    )

    # 结果按定义顺序输出；先建好账本，任务完成后按 id 回填。
    results: dict[str, TaskResult] = {}
    remaining_deps = {task.id: len(task.depends_on) for task in tasks}
    dependents: dict[str, list[str]] = {task.id: [] for task in tasks}
    for task in tasks:
        for dep in task.depends_on:
            dependents[dep].append(task.id)

    by_id = {task.id: task for task in tasks}
    # 保护回调串行可见性、results 回填、到期状态与调度账本。
    state_lock = threading.Lock()
    # 总时限到期事件：set 后唤醒重试等待，并让调度循环立即收口。
    timed_out = threading.Event()
    timeout_announced = False

    def emit(
        event: str, task_id: str, attempt: int, wait_seconds: Optional[float] = None
    ) -> None:
        # 所有调用点均持有 state_lock，轨迹事件因此与回调同序、逐条一致。
        if callback is None and not collect_trace:
            return
        callback_event = CallbackEvent(
            event, task_id, attempt, _utc_now_iso(), wait_seconds
        )
        if collect_trace:
            trace_events.append(callback_event)
        if callback is not None:
            callback(callback_event)

    def build_trace() -> Optional[RunTrace]:
        if not collect_trace:
            return None
        return RunTrace(
            started_at=trace_started_at,
            finished_at=_utc_now_iso(),
            events=trace_events,
            attempts=trace_attempts,
        )

    def record_attempt(
        task_id: str,
        attempt: int,
        started_at: str,
        start_monotonic: float,
        outcome: str,
        error: Optional[dict[str, Any]],
    ) -> None:
        """为一次已开始的尝试补全并登记轨迹记录（重试等待不计入耗时）。"""
        if not collect_trace:
            return
        record = AttemptRecord(
            task_id=task_id,
            attempt=attempt,
            started_at=started_at,
            finished_at=_utc_now_iso(),
            duration_seconds=time.monotonic() - start_monotonic,
            outcome=outcome,
            error=error,
        )
        with state_lock:
            trace_attempts.append(record)

    def announce_timeout() -> None:
        """宣布总时限到期（run_timed_out 只发一次）。调用方须持有 state_lock。"""
        nonlocal timeout_announced
        if not timeout_announced:
            timeout_announced = True
            timed_out.set()
            emit("run_timed_out", "", 0, None)

    def timeout_error() -> dict[str, Any]:
        return {
            "code": WORKFLOW_TIMEOUT_CODE,
            "message": f"工作流运行超过总时限 {run_timeout} 秒",
        }

    # timeout_seconds 为 0（理论上 deadline 已过同样如此）：不执行任何
    # 任务体，所有任务 attempts 为 0、得到同一个 WORKFLOW_TIMEOUT 错误。
    if deadline is not None and time.monotonic() >= deadline:
        with state_lock:
            announce_timeout()
        shared_error = timeout_error()
        ordered = {
            task.id: TaskResult(
                status="failed", attempts=0, output=None, error=dict(shared_error)
            )
            for task in tasks
        }
        return RunResult(
            run_id=name, status="failed", results=ordered, trace=build_trace()
        )

    def execute_task(task: TaskDef) -> TaskResult:
        """在工作线程中执行单个任务，含重试循环。"""

        def fail_by_timeout(attempts: int) -> TaskResult:
            with state_lock:
                announce_timeout()
            return TaskResult(
                status="failed",
                attempts=attempts,
                output=None,
                error=timeout_error(),
            )

        for attempt in range(1, task.max_attempts + 1):
            # 到期后不再启动新尝试：attempts 只计已经实际开始的次数。
            # 锁内同时看宣布事件与原始时钟，跨过截止点就由本线程原子宣布，
            # 避免 deadline 已过、主循环尚未唤醒时多发出一次 task_started。
            with state_lock:
                expired = timed_out.is_set() or (
                    deadline is not None and time.monotonic() >= deadline
                )
                if expired:
                    announce_timeout()
                    return TaskResult(
                        status="failed",
                        attempts=attempt - 1,
                        output=None,
                        error=timeout_error(),
                    )
                emit("task_started", task.id, attempt)
            # 轨迹只记 task_started 之后的尝试：起点取事件发出之后。
            attempt_started_at = _utc_now_iso()
            attempt_start = time.monotonic()
            try:
                output = _run_attempt(task, results, attempt, deadline)
            except _RunTimedOut:
                record_attempt(
                    task.id,
                    attempt,
                    attempt_started_at,
                    attempt_start,
                    "workflow_timeout",
                    timeout_error(),
                )
                return fail_by_timeout(attempt)
            except (TaskExecutionError, _AttemptTimeout) as exc:
                # _AttemptTimeout 记 task_timeout，TaskExecutionError 记
                # task_failed；两类错误的重试与终态处理保持原有同一口径。
                record_attempt(
                    task.id,
                    attempt,
                    attempt_started_at,
                    attempt_start,
                    "task_timeout"
                    if isinstance(exc, _AttemptTimeout)
                    else "task_failed",
                    {"code": exc.code, "message": exc.message},
                )
                if attempt < task.max_attempts:
                    # 等待发生在 task_retrying 之后、下一次 task_started 之前。
                    wait_seconds = _retry_wait_seconds(task, attempt)
                    with state_lock:
                        emit("task_retrying", task.id, attempt, wait_seconds)
                    if wait_seconds > 0:
                        # 总时限到期立即结束重试等待（未配置总时限时
                        # timed_out 永不 set，等价于原来的 time.sleep）。
                        timed_out.wait(wait_seconds)
                    # 等待结束（含 0 秒等待）：锁内按事件与时钟裁定，
                    # 跨过截止点就由本线程原子宣布，不再进入下一次尝试。
                    with state_lock:
                        expired = timed_out.is_set() or (
                            deadline is not None and time.monotonic() >= deadline
                        )
                        if expired:
                            announce_timeout()
                            return TaskResult(
                                status="failed",
                                attempts=attempt,
                                output=None,
                                error=timeout_error(),
                            )
                    continue
                with state_lock:
                    emit("task_failed", task.id, attempt)
                return TaskResult(
                    status="failed",
                    attempts=attempt,
                    output=None,
                    error={"code": exc.code, "message": exc.message},
                )
            else:
                record_attempt(
                    task.id,
                    attempt,
                    attempt_started_at,
                    attempt_start,
                    "success",
                    None,
                )
                with state_lock:
                    emit("task_succeeded", task.id, attempt)
                return TaskResult(status="success", attempts=attempt, output=output)
        # 理论上不可达（max_attempts >= 1 已由校验保证）。
        raise RuntimeError(f"任务 {task.id} 的重试循环异常退出")

    def skip_task(task: TaskDef) -> TaskResult:
        with state_lock:
            emit("task_skipped", task.id, 0)
        return TaskResult(status="skipped", attempts=0, output=None, error=None)

    ready = [task.id for task in tasks if not task.depends_on]
    max_workers = max(1, len(tasks))

    def pop_next_ready() -> str:
        """取下一个应启动的就绪任务：priority 数值越大越优先。

        priority 相同的任务保持既有相对启动顺序——ready 仍按原方式入列，
        这里在最高优先级候选中取最靠后者，与历史的 ready.pop() 口径一致。
        """
        best_index = len(ready) - 1
        best_priority = by_id[ready[best_index]].priority
        for index in range(len(ready) - 2, -1, -1):
            priority = by_id[ready[index]].priority
            if priority > best_priority:
                best_priority = priority
                best_index = index
        return ready.pop(best_index)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures: dict[Any, str] = {}

        def release_children(tid: str) -> None:
            for child in dependents[tid]:
                remaining_deps[child] -= 1
                if remaining_deps[child] == 0:
                    ready.append(child)

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
                release_children(tid)

        while True:
            with state_lock:
                if deadline is not None and time.monotonic() >= deadline:
                    announce_timeout()

            # 到期后不再启动新任务：就绪队列不再提交，只等在途任务收口。
            if not timed_out.is_set():
                # 并发额度由在途 future 数体现：任务自提交（task_started）起
                # 占用一份额度，重试等待也在其 future 内，直到终态事件后
                # future 完成才释放。skipped 任务不进线程池，不占额度。
                while ready and (
                    max_concurrency is None or len(futures) < max_concurrency
                ):
                    submit(pop_next_ready())

            # 跳过的任务可能在不经过线程池的情况下连锁放行下游。
            if not futures:
                break

            if deadline is not None and not timed_out.is_set():
                wait_timeout = max(0.0, deadline - time.monotonic())
            else:
                # 已到期：在途尝试自带截止、重试等待已被唤醒，很快都会结束。
                wait_timeout = None

            done, _ = wait(
                set(futures),
                timeout=wait_timeout,
                return_when="FIRST_COMPLETED",
            )
            if not done:
                # 等待达到总时限截止仍无任务完成：宣布到期后收一轮在途任务。
                with state_lock:
                    if deadline is not None and time.monotonic() >= deadline:
                        announce_timeout()
                continue
            for future in done:
                tid = futures.pop(future)
                results[tid] = future.result()
                release_children(tid)

    # 到期收口：未进入终态的任务（未提交或未回填）统一记 WORKFLOW_TIMEOUT，
    # attempts 为 0；此前已 success/failed/skipped 的结果原样保留。
    if timed_out.is_set():
        shared_error = timeout_error()
        for task in tasks:
            if task.id not in results:
                results[task.id] = TaskResult(
                    status="failed",
                    attempts=0,
                    output=None,
                    error=dict(shared_error),
                )
        overall_failed = True
    else:
        overall_failed = any(
            results[task.id].status == "failed" for task in tasks
        )

    ordered = {task.id: results[task.id] for task in tasks}
    return RunResult(
        run_id=name,
        status="failed" if overall_failed else "success",
        results=ordered,
        trace=build_trace(),
    )
