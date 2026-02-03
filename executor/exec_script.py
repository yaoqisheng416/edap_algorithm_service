# -*- coding: utf-8 -*-
import traceback


def exec_script(script_content: str, data, context):
    """
    执行 MySQL 脚本内容，必须定义 run(data) 函数
    返回 list[tuple]
    """
    local_env = {}
    exec(script_content, {}, local_env)
    if "run" not in local_env:
        raise RuntimeError("Script does not define run(data)")
    result = local_env["run"](data)
    final_result = []
    for row in result:
        if isinstance(row, dict):
            final_result.append(tuple(row[k] for k in sorted(row.keys())))
        elif isinstance(row, (list, tuple)):
            final_result.append(tuple(row))
        else:
            raise RuntimeError(f"Unsupported row type: {type(row)}")
    return final_result

