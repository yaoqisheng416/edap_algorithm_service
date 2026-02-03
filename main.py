# -*- coding: utf-8 -*-
import traceback
from datetime import datetime
from executor.worker_pool import run_worker, init_worker_pool
from mysql.db import get_mysql_conn
from mysql.log_dao import insert_detail_log, exists_main_log, update_main_log, \
    insert_main_log
from kafka_client.consumer import get_consumer
from ck.client import ClickHouseClient
from mysql.script_dao import get_script
from config import *
from strategy_factory import get_strategy


def main():
    consumer = get_consumer()
    db_conn = get_mysql_conn()
    ck = ClickHouseClient()
    # 1️⃣ 初始化进程池（只一次）
    init_worker_pool(WORKER_NUM)

    for msg in consumer:

        if exists_main_log(db_conn, msg):
            continue

        task = msg.value

        log_id = insert_main_log(db_conn, msg, task)

        try:
            # 1️⃣ 通用策略
            strategy = get_strategy(task.get("test_item"))

            # 2️⃣ 脚本来自 MySQL
            script = get_script(task.get("test_item"))

            # 3️⃣ 查询
            t1 = datetime.now()
            data = ck.query(task["query_sql"])
            t2 = datetime.now()
            insert_detail_log(
                db_conn, log_id,
                stage="CK", action="query",
                level="INFO", status="SUCCESS",
                start_time=t1, end_time=t2
            )

            # 4️⃣ 执行脚本
            t1 = datetime.now()
            res = run_worker({
                "script": script,
                "data": data,
                "context": task
            })
            t2 = datetime.now()

            if res["status"] != "SUCCESS":
                insert_detail_log(
                    db_conn, log_id,
                    stage="EXEC", action="algorithm",
                    level="ERROR", status="FAIL",
                    start_time=t1, end_time=t2,
                    message=res["error"]
                )
                raise Exception(res["error"])

            insert_detail_log(
                db_conn, log_id,
                stage="EXEC", action="algorithm",
                level="INFO", status="SUCCESS",
                start_time=t1, end_time=t2
            )

            # 5️⃣ 插入（HTTP 原生 SQL）
            t1 = datetime.now()
            insert_values = strategy.build_insert_values(res["result"], data)
            # print("insert_values", insert_values)
            ck.insert(
                table=task["insert_table"],
                columns=task["insert_cols"],
                values=insert_values
            )
            t2 = datetime.now()

            insert_detail_log(
                db_conn, log_id,
                stage="CK", action="insert",
                level="INFO", status="SUCCESS",
                start_time=t1, end_time=t2
            )

            update_main_log(db_conn, log_id, "SUCCESS")

        except Exception:
            error_msg = traceback.format_exc()
            update_main_log(db_conn, log_id, "FAIL", error_msg)


if __name__ == "__main__":
    main()

