# -*- coding: utf-8 -*-
from mysql.db import get_mysql_conn


def get_script(test_item):
    conn = get_mysql_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT script_content FROM algo_script WHERE test_item=%s AND enabled=1",
        (test_item,)
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row or not row[0]:
        raise Exception(f"Script not found for test_item={test_item}")

    return row[0]

