# -*- coding: utf-8 -*-
class GenericStrategy:
    """
    通用策略：
    - 不关心 test_item
    - 不关心表结构
    - insert_sql 决定列顺序
    """

    def build_insert_values(self, result, query_data):
        """
        result: 脚本 run(data) 的返回
        支持：
        - dict
        - list[dict]
        - list[tuple]
        """

        if result is None:
            return []

        # 单条 dict
        if isinstance(result, dict):
            return [tuple(result.values())]

        # 多条
        values = []
        for row in result:
            if isinstance(row, dict):
                values.append(tuple(row.values()))
            elif isinstance(row, (list, tuple)):
                values.append(tuple(row))
            else:
                raise Exception(f"Unsupported result row type: {type(row)}")

        return values



