# -*- coding: utf-8 -*-
import clickhouse_connect

client = clickhouse_connect.get_client(
    host='10.6.34.24',
    port=8123,
    username='default',
    password='default',
    database='dwm'
)

print(client.query("SELECT 1").result_rows)
