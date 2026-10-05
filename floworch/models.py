"""数据模型：任务定义、任务结果、运行结果与回调事件。"""

from dataclasses import dataclass
from typing import Any, Callable, Optional


@dataclass(frozen=True)
class TaskDef:
    """单个任务的静态定义。

    retry_backoff_multiplier 缺省为 1（固定等待）；max_retry_delay_seconds
    缺省为 None，表示退避后的等待不设上限；timeout_seconds 缺省为 None，
    表示每次尝试不限时。
    """

    id: str
    task_type: str
    depends_on: tuple[str, ...]
    max_attempts: int
    retry_delay_seconds: float
    args: dict[str, Any]
    retry_backoff_multiplier: float = 1.0
    max_retry_delay_seconds: Optional[float] = None
    timeout_seconds: Optional[float] = None


@dataclass
class TaskResult:
    """单个任务的运行结果。

    status 为 success、failed 或 skipped；skipped 时 attempts 为 0，
    output 与 error 均为 None。工作流总时限到期时尚未进入终态的任务
    记为 failed：attempts 为已实际开始的尝试次数（从未开始为 0），
    output 为 None，error.code 为 WORKFLOW_TIMEOUT。
    """

    status: str
    attempts: int
    output: Any = None
    error: Optional[dict[str, Any]] = None


@dataclass
class RunResult:
    """整个工作流的运行结果，run_id 取工作流 name。"""

    run_id: str
    status: str  # success | failed
    results: dict[str, TaskResult]


@dataclass(frozen=True)
class CallbackEvent:
    """回调事件。

    event 为 task_started、task_retrying、task_succeeded、task_failed、
    task_skipped 或 run_timed_out；timestamp 为 UTC ISO 8601 字符串。
    wait_seconds 仅 task_retrying 携带本次重试的实际等待秒数，
    其余事件均为 None。run_timed_out 在工作流总时限到期收口时发出
    一次：task_id 为空字符串、attempt 为 0、wait_seconds 为 None；
    到期时未进入终态的任务不再补发任何任务终态事件。
    """

    event: str
    task_id: str
    attempt: int
    timestamp: str
    wait_seconds: Optional[float] = None


# 回调函数：接收一个 CallbackEvent，无返回值。
# 依赖任务并发执行时，回调可能在不同工作线程中被调用。
EventCallback = Callable[[CallbackEvent], None]
