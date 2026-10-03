"""工作流定义校验：结构、重试策略、依赖关系与参数。"""

import math
from typing import Any, Optional

from .errors import WorkflowDefinitionError
from .models import TaskDef

KNOWN_TASK_TYPES = ("value", "fail_now", "sleep")


def _err(code: str, message: str) -> WorkflowDefinitionError:
    return WorkflowDefinitionError(code, message)


def _is_real_number(value: Any) -> bool:
    # bool 是 int 的子类，需显式排除。
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def build_tasks(data: Any) -> tuple[list[TaskDef], Optional[int]]:
    """校验顶层工作流定义并返回 (有序 TaskDef 列表, max_concurrency)。

    max_concurrency 省略时为 None，表示不限制同时执行的任务数。
    任何定义问题都抛 WorkflowDefinitionError，绝不产生其他异常类型。
    """
    if not isinstance(data, dict):
        raise _err("INVALID_SCHEMA", "工作流定义必须是 JSON 对象")

    name = data.get("name")
    if not isinstance(name, str) or not name:
        raise _err("INVALID_SCHEMA", "name 必须是非空字符串")

    tasks_raw = data.get("tasks")
    if not isinstance(tasks_raw, list):
        raise _err("INVALID_SCHEMA", "tasks 必须是数组")

    max_concurrency = _validate_concurrency_policy(data)

    tasks: list[TaskDef] = []
    seen_ids: set[str] = set()

    for index, raw in enumerate(tasks_raw):
        if not isinstance(raw, dict):
            raise _err("INVALID_SCHEMA", f"第 {index} 个任务必须是 JSON 对象")

        task_id = raw.get("id")
        if not isinstance(task_id, str) or not task_id:
            raise _err("INVALID_SCHEMA", f"第 {index} 个任务的 id 必须是非空字符串")
        if task_id in seen_ids:
            raise _err("DUPLICATE_TASK_ID", f"任务 id 重复: {task_id}")
        seen_ids.add(task_id)

        task_type = raw.get("task_type")
        if not isinstance(task_type, str) or not task_type:
            raise _err("INVALID_SCHEMA", f"任务 {task_id} 的 task_type 必须是非空字符串")
        if task_type not in KNOWN_TASK_TYPES:
            raise _err(
                "INVALID_SCHEMA",
                f"任务 {task_id} 的 task_type 未知: {task_type}",
            )

        depends_on = _validate_depends_on(task_id, raw.get("depends_on", []))
        (
            max_attempts,
            retry_delay,
            backoff_multiplier,
            max_retry_delay,
        ) = _validate_retry_policy(task_id, raw)
        args = _validate_args_type(task_id, raw.get("args", {}))
        timeout_seconds = _validate_timeout_policy(task_id, raw)

        tasks.append(
            TaskDef(
                id=task_id,
                task_type=task_type,
                depends_on=tuple(depends_on),
                max_attempts=max_attempts,
                retry_delay_seconds=retry_delay,
                args=args,
                retry_backoff_multiplier=backoff_multiplier,
                max_retry_delay_seconds=max_retry_delay,
                timeout_seconds=timeout_seconds,
            )
        )

    _validate_dependency_references(tasks)
    _validate_no_cycle(tasks)
    _validate_task_args(tasks)

    return tasks, max_concurrency


def _validate_concurrency_policy(data: dict) -> Optional[int]:
    """max_concurrency 省略时为 None（不限并发）；否则必须是 >= 1 的整数。

    仅接受 JSON 整数：布尔值与 2.0 这类浮点一律拒绝。
    """
    if "max_concurrency" not in data:
        return None
    value = data["max_concurrency"]
    if not isinstance(value, int) or isinstance(value, bool):
        raise _err(
            "INVALID_CONCURRENCY_POLICY",
            "max_concurrency 必须是整数",
        )
    if value < 1:
        raise _err(
            "INVALID_CONCURRENCY_POLICY",
            "max_concurrency 不能小于 1",
        )
    return value


def _validate_depends_on(task_id: str, value: Any) -> list[str]:
    if not isinstance(value, list):
        raise _err("INVALID_SCHEMA", f"任务 {task_id} 的 depends_on 必须是数组")
    deps: list[str] = []
    for dep in value:
        if not isinstance(dep, str) or not dep:
            raise _err(
                "INVALID_SCHEMA",
                f"任务 {task_id} 的 depends_on 只能包含非空字符串",
            )
        if dep in deps:
            raise _err(
                "INVALID_SCHEMA",
                f"任务 {task_id} 的 depends_on 中依赖重复: {dep}",
            )
        deps.append(dep)
    return deps


def _validate_retry_policy(
    task_id: str, raw: dict
) -> tuple[int, float, float, Optional[float]]:
    max_attempts = raw.get("max_attempts", 1)
    # 仅接受整数次（1.0 这类浮点在 JSON 中若写为 1.0 也拒绝，避免歧义）。
    if not isinstance(max_attempts, int) or isinstance(max_attempts, bool):
        raise _err(
            "INVALID_RETRY_POLICY",
            f"任务 {task_id} 的 max_attempts 必须是整数",
        )
    if max_attempts < 1:
        raise _err(
            "INVALID_RETRY_POLICY",
            f"任务 {task_id} 的 max_attempts 不能小于 1",
        )

    retry_delay = raw.get("retry_delay_seconds", 0)
    if not _is_real_number(retry_delay) or not math.isfinite(retry_delay):
        raise _err(
            "INVALID_RETRY_POLICY",
            f"任务 {task_id} 的 retry_delay_seconds 必须是有限数值",
        )
    if retry_delay < 0:
        raise _err(
            "INVALID_RETRY_POLICY",
            f"任务 {task_id} 的 retry_delay_seconds 不能小于 0",
        )

    # 省略 multiplier 时默认 1（固定等待，与历史行为完全一致）。
    backoff_multiplier = raw.get("retry_backoff_multiplier", 1)
    if not _is_real_number(backoff_multiplier) or not math.isfinite(backoff_multiplier):
        raise _err(
            "INVALID_RETRY_POLICY",
            f"任务 {task_id} 的 retry_backoff_multiplier 必须是有限数值",
        )
    if backoff_multiplier < 1:
        raise _err(
            "INVALID_RETRY_POLICY",
            f"任务 {task_id} 的 retry_backoff_multiplier 不能小于 1",
        )

    # 省略上限时为 None，退避结果不截断。
    max_retry_delay: Optional[float] = None
    if "max_retry_delay_seconds" in raw:
        max_retry_delay = raw["max_retry_delay_seconds"]
        if not _is_real_number(max_retry_delay) or not math.isfinite(max_retry_delay):
            raise _err(
                "INVALID_RETRY_POLICY",
                f"任务 {task_id} 的 max_retry_delay_seconds 必须是有限数值",
            )
        if max_retry_delay < 0:
            raise _err(
                "INVALID_RETRY_POLICY",
                f"任务 {task_id} 的 max_retry_delay_seconds 不能小于 0",
            )
        max_retry_delay = float(max_retry_delay)

    return (
        max_attempts,
        float(retry_delay),
        float(backoff_multiplier),
        max_retry_delay,
    )


def _validate_args_type(task_id: str, value: Any) -> dict:
    if not isinstance(value, dict):
        raise _err("INVALID_ARGS", f"任务 {task_id} 的 args 必须是 JSON 对象")
    return value


def _validate_timeout_policy(task_id: str, raw: dict) -> Optional[float]:
    """timeout_seconds 省略时为 None（不限时）；否则必须是 >= 0 的有限数值。"""
    if "timeout_seconds" not in raw:
        return None
    timeout = raw["timeout_seconds"]
    if not _is_real_number(timeout) or not math.isfinite(timeout):
        raise _err(
            "INVALID_TIMEOUT_POLICY",
            f"任务 {task_id} 的 timeout_seconds 必须是有限数值",
        )
    if timeout < 0:
        raise _err(
            "INVALID_TIMEOUT_POLICY",
            f"任务 {task_id} 的 timeout_seconds 不能小于 0",
        )
    return float(timeout)


def _validate_dependency_references(tasks: list[TaskDef]) -> None:
    known = {task.id for task in tasks}
    for task in tasks:
        for dep in task.depends_on:
            if dep not in known:
                raise _err(
                    "UNKNOWN_DEPENDENCY",
                    f"任务 {task.id} 依赖了不存在的任务: {dep}",
                )


def _validate_no_cycle(tasks: list[TaskDef]) -> None:
    # Kahn 拓扑排序；处理完后仍有剩余节点即存在环（含自依赖）。
    graph = {task.id: set(task.depends_on) for task in tasks}
    processed: set[str] = set()
    while True:
        leaves = [tid for tid, deps in graph.items() if tid not in processed and not (deps - processed)]
        if not leaves:
            break
        processed.update(leaves)
    if len(processed) != len(graph):
        remaining = sorted(set(graph) - processed)
        raise _err(
            "DEPENDENCY_CYCLE",
            f"依赖关系中存在环，涉及任务: {', '.join(remaining)}",
        )


def _validate_task_args(tasks: list[TaskDef]) -> None:
    by_id = {task.id: task for task in tasks}
    for task in tasks:
        if task.task_type == "value":
            has_value = "value" in task.args
            has_ref = "ref" in task.args
            if not has_value and not has_ref:
                raise _err(
                    "INVALID_ARGS",
                    f"任务 {task.id} 的 value 类型要求 args.value 或 args.ref",
                )
            if has_ref:
                ref = task.args["ref"]
                if not isinstance(ref, str) or not ref:
                    raise _err(
                        "INVALID_ARGS",
                        f"任务 {task.id} 的 args.ref 必须是非空字符串",
                    )
                if ref not in task.depends_on:
                    raise _err(
                        "INVALID_ARGS",
                        f"任务 {task.id} 的 ref 目标 {ref} 必须出现在 depends_on 中",
                    )
        # fail_now 对 args 无额外要求。
        if task.task_type == "sleep":
            if "seconds" not in task.args or "output" not in task.args:
                raise _err(
                    "INVALID_ARGS",
                    f"任务 {task.id} 的 sleep 类型要求同时提供 args.seconds 与 args.output",
                )
            seconds = task.args["seconds"]
            if not _is_real_number(seconds) or not math.isfinite(seconds):
                raise _err(
                    "INVALID_ARGS",
                    f"任务 {task.id} 的 args.seconds 必须是有限数值",
                )
            if seconds < 0:
                raise _err(
                    "INVALID_ARGS",
                    f"任务 {task.id} 的 args.seconds 不能小于 0",
                )
