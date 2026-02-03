# -*- coding: utf-8 -*-
class GenericStrategy:

    def __init__(self, cfg: dict):
        self.cfg = cfg

    def query(self, ck, task):
        sql = self.cfg["query_sql"].format(
            order_no=task["order_no"],
            cell_id=task["cell_id"]
        )
        return self.ck_query_rows(ck, sql)

    @staticmethod
    def ck_query_rows(ck, sql):
        res = ck.query(sql)
        if isinstance(res, list):
            return res
        return res.result_rows

    def insert(self, ck, result: dict, task):
        values = [
            tuple(task[k] if k in task else result[k]
                  for k in self.cfg["insert_cols"])
        ]

        ck.insert(
            self.cfg["insert_table"],
            values,
            self.cfg["insert_cols"]
        )
