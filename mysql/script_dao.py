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
    return row[0] if row else None
