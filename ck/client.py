# -*- coding: utf-8 -*-
import clickhouse_connect


class ClickHouseClient(object):
    def __init__(self, host, port, user, password, database):
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database
        )

    def query(self, sql):
        return self.client.query(sql).result_rows

    def insert(self, table, rows, columns):
        self.client.insert(table, rows, column_names=columns)
