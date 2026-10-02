"""命令行入口：python -m floworch <workflow.json>

退出码：0 成功；1 执行失败；2 定义无效或输入不可读。
"""

from __future__ import annotations

import json
import sys

from .errors import WorkflowDefinitionError
from .executor import run_workflow


def _error_exit(exc: WorkflowDefinitionError) -> int:
    payload = {"error": {"code": exc.code, "message": exc.message}}
    print(json.dumps(payload, ensure_ascii=False), file=sys.stderr)
    return 2


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 1:
        print("用法：python -m floworch <workflow.json>", file=sys.stderr)
        return 2

    path = args[0]
    try:
        with open(path, "r", encoding="utf-8") as fh:
            text = fh.read()
    except OSError as exc:
        return _error_exit(
            WorkflowDefinitionError(
                WorkflowDefinitionError.INPUT_READ_ERROR,
                f"无法读取输入文件 '{path}'：{exc}",
            )
        )

    try:
        result = run_workflow(text)
    except WorkflowDefinitionError as exc:
        return _error_exit(exc)

    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    return 0 if result.status == "succeeded" else 1


if __name__ == "__main__":
    sys.exit(main())
