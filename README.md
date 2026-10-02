# Flow Orch

DAG 工作流编排引擎：依赖调度、重试策略与运行可观测。

## 范围

本仓库从零开始实现上述方向的可用工具，不依赖外部同类实现。仅使用 Python 标准库（3.11+），不含持久化与远程执行。

## 使用

### 命令行

```bash
python -m floworch <workflow.json>
```

输出 JSON（`run_id`、`status`、`results`）到 stdout。退出码：

- `0`：全部任务成功
- `1`：执行失败（结果仍输出到 stdout）
- `2`：定义无效或输入不可读（错误输出到 stderr）

### Python API

```python
from floworch import run_workflow

result = run_workflow(definition, on_event=events.append)  # definition 为 dict 或 JSON 文本
print(result.run_id, result.status)  # run_id 取定义中的 name
```

## 工作流定义

```json
{
  "name": "demo",
  "tasks": [
    {"id": "a", "task_type": "value", "args": {"value": 1}},
    {"id": "b", "depends_on": ["a"], "task_type": "value", "args": {"ref": "a"},
     "max_attempts": 3, "retry_delay_seconds": 0.5},
    {"id": "c", "task_type": "fail_now", "args": {}}
  ]
}
```

任务字段：

| 字段 | 说明 | 默认 |
| --- | --- | --- |
| `id` | 非空字符串，全局唯一 | 必填 |
| `depends_on` | 依赖任务 id 数组，无重复、无环 | `[]` |
| `task_type` | `value` 或 `fail_now` | 必填 |
| `args` | 对象；`value` 任务须且只须含 `value`（字面量）或 `ref`（依赖任务 id，须出现在 `depends_on` 中） | `{}` |
| `max_attempts` | 整数 ≥ 1 | `1` |
| `retry_delay_seconds` | 数字 ≥ 0，失败后等待再重试 | `0` |

## 执行语义

- 依赖全部成功才执行；任一依赖失败或被跳过，则该任务为 `skipped`（`attempts` 为 0，`output`、`error` 为 null）。
- 相互独立的任务并发执行。
- 失败后等待 `retry_delay_seconds` 再重试，最多 `max_attempts` 次。
- `value` 任务输出解析值（`value` 字面量或 `ref` 指向任务的输出）；`fail_now` 任务总是失败，耗尽重试后 `error.code` 为 `BUILTIN_TASK_FAILED`。
- 回调 `on_event` 收到 `task_started`、`task_retrying`、`task_succeeded`、`task_failed`、`task_skipped` 事件，各含 `task_id`、`attempt` 与 UTC 时间戳。

## 定义错误

定义无效时仅抛出 `WorkflowDefinitionError`，错误码：`INVALID_JSON`、`INVALID_SCHEMA`、`DUPLICATE_TASK_ID`、`UNKNOWN_DEPENDENCY`、`DEPENDENCY_CYCLE`、`INVALID_RETRY_POLICY`、`INVALID_ARGS`；输入文件不可读为 `INPUT_READ_ERROR`。

## 约定

- 公开行为以 README 与源码为准。
- 后续需求在此基线上增量实现。
