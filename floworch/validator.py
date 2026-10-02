"""工作流定义校验：结构、重试策略、依赖关系与参数。"""

import math
from typing import Any

from .errors import WorkflowDefinitionError
from .models import TaskDef

KNOWN_TASK_TYPES = ("value", "fail_now")


def _err(code: str, message: str) -> WorkflowDefinitionError:
    return WorkflowDefinitionError(code, message)


def _is_real_number(value: Any) -> bool:
    # bool 是 int 的子类，需显式排除。
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def build_tasks(data: Any) -> list[TaskDef]:
    """校验顶层工作流定义并返回有序 TaskDef 列表。

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
        retry_policy = _validate_retry_policy(task_id, raw)
        args = _validate_args_type(task_id, raw.get("args", {}))

        tasks.append(
            TaskDef(
                id=task_id,
                task_type=task_type,
                depends_on=tuple(depends_on),
                max_attempts=retry_policy[0],
                retry_delay_seconds=retry_policy[1],
                retry_backoff_multiplier=retry_policy[2],
                max_retry_delay_seconds=retry_policy[3],
                args=args,
            )
        )

    _validate_dependency_references(tasks)
    _validate_no_cycle(tasks)
    _validate_task_args(tasks)

    return tasks


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


def _validate_retry_policy(task_id: str, raw: dict) -> tuple[int, float, float, float | None]:
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

    multiplier = raw.get("retry_backoff_multiplier", 1)
    if not _is_real_number(multiplier) or not math.isfinite(multiplier):
        raise _err(
            "INVALID_RETRY_POLICY",
            f"任务 {task_id} 的 retry_backoff_multiplier 必须是有限数值",
        )
    if multiplier < 1:
        raise _err(
            "INVALID_RETRY_POLICY",
            f"任务 {task_id} 的 retry_backoff_multiplier 不能小于 1",
        )

    max_delay = raw.get("max_retry_delay_seconds")
    if max_delay is not None:
        if not _is_real_number(max_delay) or not math.isfinite(max_delay):
            raise _err(
                "INVALID_RETRY_POLICY",
                f"任务 {task_id} 的 max_retry_delay_seconds 必须是有限数值",
            )
        if max_delay < 0:
            raise _err(
                "INVALID_RETRY_POLICY",
                f"任务 {task_id} 的 max_retry_delay_seconds 不能小于 0",
            )
        max_delay = float(max_delay)

    return max_attempts, float(retry_delay), float(multiplier), max_delay


def _validate_args_type(task_id: str, value: Any) -> dict:
    if not isinstance(value, dict):
        raise _err("INVALID_ARGS", f"任务 {task_id} 的 args 必须是 JSON 对象")
    return value


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
