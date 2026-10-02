"""工作流定义的解析与校验。

校验失败只抛出 :class:`WorkflowDefinitionError`。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from .errors import WorkflowDefinitionError as Err

TASK_TYPES = ("value", "fail_now")


@dataclass(frozen=True)
class TaskDefinition:
    id: str
    depends_on: tuple[str, ...] = ()
    max_attempts: int = 1
    retry_delay_seconds: float = 0.0
    task_type: str = "value"
    args: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class WorkflowDefinition:
    name: str
    tasks: tuple[TaskDefinition, ...]


def parse_definition(source: str | dict[str, Any]) -> WorkflowDefinition:
    """把 JSON 文本或已解析的 dict 校验为 WorkflowDefinition。"""
    if isinstance(source, str):
        try:
            data = json.loads(source)
        except json.JSONDecodeError as exc:
            raise Err(Err.INVALID_JSON, f"输入不是合法 JSON：{exc}") from exc
    elif isinstance(source, dict):
        data = source
    else:
        raise Err(Err.INVALID_SCHEMA, "工作流定义必须是 JSON 对象")

    if not isinstance(data, dict):
        raise Err(Err.INVALID_SCHEMA, "工作流定义必须是 JSON 对象")

    name = data.get("name")
    if not isinstance(name, str) or not name.strip():
        raise Err(Err.INVALID_SCHEMA, "字段 'name' 必须是非空字符串")

    raw_tasks = data.get("tasks")
    if not isinstance(raw_tasks, list):
        raise Err(Err.INVALID_SCHEMA, "字段 'tasks' 必须是数组")

    tasks: list[TaskDefinition] = []
    seen_ids: set[str] = set()
    for index, raw in enumerate(raw_tasks):
        tasks.append(_parse_task(raw, index, seen_ids))

    task_ids = {t.id for t in tasks}
    for task in tasks:
        for dep in task.depends_on:
            if dep not in task_ids:
                raise Err(
                    Err.UNKNOWN_DEPENDENCY,
                    f"任务 '{task.id}' 依赖未知任务 '{dep}'",
                )

    _check_cycle(tasks)
    return WorkflowDefinition(name=name, tasks=tuple(tasks))


def _parse_task(raw: Any, index: int, seen_ids: set[str]) -> TaskDefinition:
    where = f"tasks[{index}]"
    if not isinstance(raw, dict):
        raise Err(Err.INVALID_SCHEMA, f"{where} 必须是对象")

    task_id = raw.get("id")
    if not isinstance(task_id, str) or not task_id.strip():
        raise Err(Err.INVALID_SCHEMA, f"{where}.id 必须是非空字符串")
    if task_id in seen_ids:
        raise Err(Err.DUPLICATE_TASK_ID, f"任务 id '{task_id}' 重复")
    seen_ids.add(task_id)

    depends_on = raw.get("depends_on", [])
    if not isinstance(depends_on, list) or not all(
        isinstance(d, str) for d in depends_on
    ):
        raise Err(Err.INVALID_SCHEMA, f"任务 '{task_id}' 的 depends_on 必须是字符串数组")
    if len(set(depends_on)) != len(depends_on):
        raise Err(Err.INVALID_SCHEMA, f"任务 '{task_id}' 的 depends_on 存在重复项")

    max_attempts = raw.get("max_attempts", 1)
    if isinstance(max_attempts, bool) or not isinstance(max_attempts, int) or max_attempts < 1:
        raise Err(
            Err.INVALID_RETRY_POLICY,
            f"任务 '{task_id}' 的 max_attempts 必须是不小于 1 的整数",
        )

    retry_delay = raw.get("retry_delay_seconds", 0)
    if (
        isinstance(retry_delay, bool)
        or not isinstance(retry_delay, (int, float))
        or retry_delay < 0
    ):
        raise Err(
            Err.INVALID_RETRY_POLICY,
            f"任务 '{task_id}' 的 retry_delay_seconds 必须是不小于 0 的数字",
        )

    task_type = raw.get("task_type")
    if task_type not in TASK_TYPES:
        raise Err(
            Err.INVALID_SCHEMA,
            f"任务 '{task_id}' 的 task_type 必须是 {list(TASK_TYPES)} 之一",
        )

    args = raw.get("args", {})
    if not isinstance(args, dict):
        raise Err(Err.INVALID_SCHEMA, f"任务 '{task_id}' 的 args 必须是对象")

    _validate_args(task_id, task_type, args, depends_on)

    return TaskDefinition(
        id=task_id,
        depends_on=tuple(depends_on),
        max_attempts=max_attempts,
        retry_delay_seconds=float(retry_delay),
        task_type=task_type,
        args=dict(args),
    )


def _validate_args(task_id: str, task_type: str, args: dict, depends_on: list[str]) -> None:
    if task_type == "value":
        has_value = "value" in args
        has_ref = "ref" in args
        if has_value == has_ref:
            raise Err(
                Err.INVALID_ARGS,
                f"任务 '{task_id}'（value）的 args 必须且只能包含 'value' 或 'ref' 之一",
            )
        if has_ref:
            ref = args["ref"]
            if not isinstance(ref, str) or not ref:
                raise Err(Err.INVALID_ARGS, f"任务 '{task_id}' 的 ref 必须是非空字符串")
            if ref not in depends_on:
                raise Err(
                    Err.INVALID_ARGS,
                    f"任务 '{task_id}' 的 ref 目标 '{ref}' 必须出现在其 depends_on 中",
                )


def _check_cycle(tasks: list[TaskDefinition]) -> None:
    """Kahn 拓扑排序；排不完说明有环。"""
    remaining: dict[str, set[str]] = {t.id: set(t.depends_on) for t in tasks}
    ready = [tid for tid, deps in remaining.items() if not deps]
    resolved = 0
    while ready:
        tid = ready.pop()
        resolved += 1
        for other, deps in remaining.items():
            if tid in deps:
                deps.discard(tid)
                if not deps:
                    ready.append(other)
    if resolved != len(remaining):
        cycle = sorted(tid for tid, deps in remaining.items() if deps)
        raise Err(Err.DEPENDENCY_CYCLE, f"任务依赖存在环，涉及任务：{cycle}")
