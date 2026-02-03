# -*- coding: utf-8 -*-
def exec_script(script: str, data, context: dict):
    """
    执行算法脚本
    约定：
      - 脚本必须定义 run(data, context) -> dict
    """
    exec_globals = {}
    exec_locals = {}

    # 执行脚本
    exec(script, exec_globals, exec_locals)

    if "run" not in exec_locals:
        raise RuntimeError("script must define function run(data, context)")

    run_func = exec_locals["run"]

    result = run_func(data, context)

    if not isinstance(result, dict):
        raise RuntimeError("script return value must be dict")

    return result
