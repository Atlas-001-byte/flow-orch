"""命令行入口：python -m floworch [--trace] <workflow.json>

退出码：0 成功；1 工作流执行失败（结果仍打印）；2 定义或输入错误。
--trace 时 stdout JSON 额外携带 trace 字段（内存运行轨迹）。
"""

import argparse
import json
import sys
from dataclasses import asdict
from typing import Optional

from .engine import run_workflow
from .errors import WorkflowDefinitionError, WorkflowInputError
from .loader import load_workflow_file


def _task_result_to_dict(result) -> dict:
    return {
        "status": result.status,
        "attempts": result.attempts,
        "output": result.output,
        "error": result.error,
    }


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m floworch",
        description="执行 JSON 定义的 DAG 工作流",
    )
    parser.add_argument("workflow_file", help="工作流 JSON 文件路径")
    parser.add_argument(
        "--trace",
        action="store_true",
        help="在 stdout JSON 中附带本次运行的内存轨迹（trace 字段）",
    )
    args = parser.parse_args(argv)

    try:
        workflow = load_workflow_file(args.workflow_file)
        run_result = run_workflow(workflow, collect_trace=args.trace)
    except (WorkflowInputError, WorkflowDefinitionError) as exc:
        print(
            json.dumps({"error": {"code": exc.code, "message": exc.message}}, ensure_ascii=False),
            file=sys.stderr,
        )
        return 2

    payload = {
        "run_id": run_result.run_id,
        "status": run_result.status,
        "results": {
            task_id: _task_result_to_dict(result)
            for task_id, result in run_result.results.items()
        },
    }
    if args.trace:
        payload["trace"] = asdict(run_result.trace)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 1 if run_result.status == "failed" else 0


if __name__ == "__main__":
    sys.exit(main())
