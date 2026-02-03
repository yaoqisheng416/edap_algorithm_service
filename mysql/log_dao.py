# -*- coding: utf-8 -*-
from mysql.db import ensure_detail_table, get_detail_table


def create_main_log(conn, msg, task):
    sql = """
    INSERT INTO algo_log_main
    (kafka_topic, kafka_partition, kafka_offset,
     order_no, cell_id, test_item,
     status, start_time)
    VALUES (%s,%s,%s,%s,%s,%s,'INIT',NOW())
    """
    cur = conn.cursor()
    cur.execute(sql, (
        msg.topic, msg.partition, msg.offset,
        task["order_no"], task["cell_id"], task["test_item"]
    ))
    conn.commit()
    return cur.lastrowid


def finish_main_log(conn, log_id, status, error=None):
    sql = """
    UPDATE algo_log_main
    SET status=%s,
        end_time=NOW(),
        total_cost_ms=TIMESTAMPDIFF(MICROSECOND, start_time, NOW())/1000,
        error_message=%s
    WHERE id=%s
    """
    conn.cursor().execute(sql, (status, error, log_id))
    conn.commit()


def insert_detail_log(
    conn, log_id,
    stage, action,
    level, status,
    start_time, end_time,
    message=None
):
    table = get_detail_table()
    ensure_detail_table(conn, table)

    cost_ms = int((end_time - start_time).total_seconds() * 1000)

    sql = f"""
    INSERT INTO {table}
    (log_id, stage, action, level, status,
     message, start_time, end_time, cost_ms)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    conn.cursor().execute(sql, (
        log_id, stage, action, level, status,
        message, start_time, end_time, cost_ms
    ))
    conn.commit()


def exists_main_log(conn, msg) -> bool:
    """
    判断 Kafka 消息是否已经处理过（成功或失败都不再执行）
    """
    sql = """
    SELECT status
    FROM algo_log_main
    WHERE kafka_topic=%s
      AND kafka_partition=%s
      AND kafka_offset=%s
    LIMIT 1
    """
    cur = conn.cursor()
    cur.execute(sql, (msg.topic, msg.partition, msg.offset))
    row = cur.fetchone()

    if not row:
        return False

    status = row[0]
    return status in ("SUCCESS", "FAIL")


def update_main_log(conn, log_id: int, status: str, error: str | None = None):
    """
    结束主日志
    """
    finish_main_log(conn, log_id, status, error)


def insert_main_log(conn, msg, task) -> int:
    """
    创建主日志，返回 log_id
    """
    return create_main_log(conn, msg, task)
