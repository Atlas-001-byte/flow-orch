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
| `max_concurrency` | 可选，允许同时执行的任务数上限，JSON 整数且 ≥ 1（布尔不被接受）；省略时不限制并发 |

任务字段：

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `id` | — | 非空字符串，全工作流唯一 |
| `task_type` | — | `value`、`fail_now` 或 `sleep` |
| `depends_on` | `[]` | 所依赖任务的 id 数组，不可重复 |
| `max_attempts` | `1` | 最大尝试次数，整数且 ≥ 1 |
| `retry_delay_seconds` | `0` | 失败后重试前的基础等待秒数，数值且 ≥ 0 |
| `retry_backoff_multiplier` | `1` | 指数退避乘数，有限数值且 ≥ 1（布尔不算数值） |
| `max_retry_delay_seconds` | 无上限 | 单次重试等待上限，有限数值且 ≥ 0（布尔不算数值） |
| `timeout_seconds` | 不限时 | 每次尝试从开始到结束的时限，有限数值且 ≥ 0（布尔不算数值）；`0` 表示不执行任务体立即超时 |
| `args` | `{}` | 任务参数（JSON 对象） |

## 任务类型

- **value**：取 `args.value` 的原值作为输出；或写 `args.ref`（必须出现在
  `depends_on` 中），输出所引用依赖任务的输出。二选一，至少提供一个。
- **fail_now**：执行即失败，耗尽重试后 `error.code` 为 `BUILTIN_TASK_FAILED`。
- **sleep**：等待 `args.seconds` 秒（有限数值且 ≥ 0，布尔不算数值）后返回
  `args.output` 的原值（任意 JSON 值）。`seconds` 与 `output` 必须同时提供。
  若本任务配置了 `timeout_seconds`，到截止仍未等满时等待立即结束并按超时处理，
  不会阻塞到原定秒数走完。

## 执行语义

- 依赖全部成功后任务才可执行；相互独立的任务在线程池中并发执行。
- 配置 `max_concurrency` 后，依赖就绪的任务仅在并发额度未满时启动：
  额度从 `task_started` 起占用，到 `task_succeeded`、`task_failed` 或
  `task_skipped` 才释放，重试等待期间也占用；额度释放后才能提交其他
  就绪任务。省略时不限制并发，行为与之前一致。
- 失败后按下列规则等待再重试，最多 `max_attempts` 次；
  第 n 次失败、准备第 n+1 次尝试前的等待秒数为
  `retry_delay_seconds * retry_backoff_multiplier ** (n - 1)`；省略
  multiplier 时固定等待 `retry_delay_seconds`。配置
  `max_retry_delay_seconds` 后等待取计算值与上限的较小值，未配置则不截断；
  `retry_delay_seconds` 为 0 时所有等待均为 0。最后一次尝试失败不再等待。
- 等待发生在 `task_retrying` 事件之后、下一次 `task_started` 之前。
- 任一依赖为 failed/skipped 时，任务标记为 skipped，下游连锁跳过。
- skipped 任务 `attempts` 为 0，`output`、`error` 为 null。
- 配置 `timeout_seconds` 后，每次尝试从开始单独计时：截止前成功则输出不变；
  到截止仍未完成时本次尝试立即中止并计入 `attempts`，`output` 为 null，
  `error.code` 为 `TASK_TIMEOUT`、`message` 非空，随后与普通失败一样按
  `max_attempts` 与退避字段重试，额度耗尽后任务 status 为 failed。
  `timeout_seconds` 为 0 时不执行任务体、立即判超时。省略时不限时，
  其他任务默认行为不变。

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
| `INVALID_RETRY_POLICY` | max_attempts 非整数或 < 1；delay 非有限数值或 < 0；multiplier 非有限数值、< 1 或为布尔；max delay 非有限数值、< 0 或为布尔 |
| `INVALID_TIMEOUT_POLICY` | timeout_seconds 非有限数值、< 0 或为布尔 |
| `INVALID_CONCURRENCY_POLICY` | max_concurrency 非整数、为布尔或 < 1 |
| `INVALID_ARGS` | args 不是对象；value 任务缺 value/ref；ref 非法或未在 depends_on 中；sleep 任务缺 seconds/output，或 seconds 非有限数值、< 0 或为布尔 |

`WorkflowInputError`（退出码 2）：文件不可读时 `INPUT_READ_ERROR`。

## 回调事件

`task_started`（每次尝试开始）、`task_retrying`（失败后、等待重试前）、
`task_succeeded`、`task_failed`、`task_skipped`。事件带 `task_id`、`attempt`
（skipped 为 0）和 UTC ISO 8601 `timestamp`。`task_retrying` 额外携带
`wait_seconds`（按退避规则算出的本次实际等待秒数，可能为 0），其余事件
`wait_seconds` 为 null。

## 状态

Python 3.11 DAG 执行功能已实现：校验、并发调度、重试与回调可直接使用。

## 约定

- 公开行为以 README 与源码为准。
- 后续需求在此基线上增量实现。
