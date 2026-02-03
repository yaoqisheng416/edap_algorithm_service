# -*- coding: utf-8 -*-
def run_script_safe(script_code, input_data, context):
    """
    执行脚本安全沙箱：
    - input_data: ClickHouse 查询结果
    - context: dict, 包含 order_no, cell_id, test_item
    返回 output_data
    """
    # 只允许访问 input_data, context, output_data
    local_env = {
        "input_data": input_data,
        "context": context,
        "output_data": None
    }

    # 安全执行 exec（不允许内置函数访问）
    exec(script_code, {"__builtins__": {}}, local_env)

    output_data = local_env.get("output_data")
    if output_data is None:
        raise Exception("Script did not produce output_data")

    return output_data
