# -*- coding: utf-8 -*-

import clickhouse_connect
import requests

from config import *


class ClickHouseClient:
    def __init__(self):
        self.url = f"http://{CK_HOST}:{CK_PORT}/?database={CK_DB}"
        self.auth = (CK_USER, CK_PASSWORD)

    def query(self, sql: str):
        """返回 list[tuple]"""
        try:
            resp = requests.post(self.url, data=sql, auth=self.auth)
            resp.raise_for_status()
            text = resp.text.strip()
            if not text:
                return []
            rows = [tuple(line.split('\t')) for line in text.split('\n')]
            return rows
        except Exception as e:
            raise RuntimeError(f"CK query failed, sql={sql}\n{e}")

    def insert(self, table: str, columns: list, values: list):
        if not values:
            return
        try:
            vals_sql = []
            for row in values:
                vals_sql.append(
                    "(" + ",".join(
                        f"'{str(v)}'" if v is not None else "NULL"
                        for v in row
                    ) + ")"
                )
            sql = f"INSERT INTO {table} ({','.join(columns)}) VALUES " + ",".join(vals_sql)
            resp = requests.post(self.url, data=sql, auth=self.auth)
            resp.raise_for_status()
        except Exception as e:
            raise RuntimeError(f"CK insert failed, table={table}, columns={columns}\n{e}")