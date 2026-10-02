# Flow Orch

DAG 工作流编排引擎：依赖调度、重试策略与运行可观测。零第三方依赖，Python 3.11+ 标准库实现。

## 范围

本仓库从零开始实现上述方向的可用工具，不依赖外部同类实现，不包含持久化或远程执行。

## 用法

CLI 读取工作流 JSON 文件：

```bash
python -m floworch workflow.json
```

Python API：

```python
from floworch import run_workflow

result = run_workflow({
    "name": "demo",
    "tasks": [
        {"id": "a", "task_type": "value", "args": {"value": 1}},
        {"id": "b", "task_type": "value", "depends_on": ["a"], "args": {"ref": "a"}},
    ],
})
print(result.run_id, result.status, result.results)
```

`run_workflow(workflow, callback=None)` 返回 `RunResult`，定义无效时抛
`WorkflowDefinitionError`。回调可选，接收 `CallbackEvent`（独立任务并发执行时
回调可能来自不同工作线程）。

## 工作流定义

顶层字段：

| 字段 | 说明 |
| --- | --- |
| `name` | 非空字符串，作为 `run_id` |
| `tasks` | 任务数组，可为空 |

任务字段：

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `id` | — | 非空字符串，全工作流唯一 |
| `task_type` | — | `value` 或 `fail_now` |
| `depends_on` | `[]` | 所依赖任务的 id 数组，不可重复 |
| `max_attempts` | `1` | 最大尝试次数，整数且 ≥ 1 |
| `retry_delay_seconds` | `0` | 失败后重试前的等待秒数，数值且 ≥ 0 |
| `args` | `{}` | 任务参数（JSON 对象） |

## 任务类型

- **value**：取 `args.value` 的原值作为输出；或写 `args.ref`（必须出现在
  `depends_on` 中），输出所引用依赖任务的输出。二选一，至少提供一个。
- **fail_now**：执行即失败，耗尽重试后 `error.code` 为 `BUILTIN_TASK_FAILED`。

## 执行语义

- 依赖全部成功后任务才可执行；相互独立的任务在线程池中并发执行。
- 失败后等待 `retry_delay_seconds` 再重试，最多 `max_attempts` 次；
  最后一次尝试失败不再等待。
- 任一依赖为 failed/skipped 时，任务标记为 skipped，下游连锁跳过。
- skipped 任务 `attempts` 为 0，`output`、`error` 为 null。

## 输出与退出码

成功时 stdout 输出整体结果并退出 **0**；任务最终失败时仍输出完整结果并退出 **1**；
定义或输入错误报告到 stderr 并退出 **2**：

```json
{
  "run_id": "demo",
  "status": "success",
  "results": {
    "a": {"status": "success", "attempts": 1, "output": 1, "error": null}
  }
}
```

每个任务结果含 `status`（success/failed/skipped）、`attempts`、`output`、`error`。

## 错误码

`WorkflowDefinitionError.code`（退出码 2）：

| code | 触发条件 |
| --- | --- |
| `INVALID_JSON` | 文件内容不是合法 JSON |
| `INVALID_SCHEMA` | 顶层结构、name/tasks、id、task_type、depends_on 结构问题 |
| `DUPLICATE_TASK_ID` | 任务 id 重复 |
| `UNKNOWN_DEPENDENCY` | depends_on 引用了不存在的任务 |
| `DEPENDENCY_CYCLE` | 依赖图有环（含自依赖） |
| `INVALID_RETRY_POLICY` | max_attempts 非整数或 < 1；delay 非有限数值或 < 0 |
| `INVALID_ARGS` | args 不是对象；value 任务缺 value/ref；ref 非法或未在 depends_on 中 |

`WorkflowInputError`（退出码 2）：文件不可读时 `INPUT_READ_ERROR`。

## 回调事件

`task_started`（每次尝试开始）、`task_retrying`（失败后、等待重试前）、
`task_succeeded`、`task_failed`、`task_skipped`。事件带 `task_id`、`attempt`
（skipped 为 0）和 UTC ISO 8601 `timestamp`。

## 状态

Python 3.11 DAG 执行功能已实现：校验、并发调度、重试与回调可直接使用。

## 约定

- 公开行为以 README 与源码为准。
- 后续需求在此基线上增量实现。
