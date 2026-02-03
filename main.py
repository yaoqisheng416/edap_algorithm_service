# -*- coding: utf-8 -*-
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
    ck = ClickHouseClient(
        CK_HOST, CK_PORT, CK_USER, CK_PASSWORD, CK_DB
    )
    # 1️⃣ 初始化进程池（只一次）
    init_worker_pool(WORKER_NUM)

    for msg in consumer:

        if exists_main_log(db_conn, msg):
            continue

        task = msg.value

        # 1️⃣ 主日志
        log_id = insert_main_log(db_conn, msg, task)

        try:
            strategy = get_strategy(task["test_item"])
            script = get_script(task["test_item"])

            # 2️⃣ 查询
            t1 = datetime.now()
            data = strategy.query(ck, task)
            t2 = datetime.now()
            insert_detail_log(
                db_conn, log_id,
                stage="CK", action="query",
                level="INFO", status="SUCCESS",
                start_time=t1, end_time=t2
            )

            # 3️⃣ 执行
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

            # 4️⃣ 插入结果
            t1 = datetime.now()
            strategy.insert(ck, res["result"], task)
            t2 = datetime.now()
            insert_detail_log(
                db_conn, log_id,
                stage="CK", action="insert",
                level="INFO", status="SUCCESS",
                start_time=t1, end_time=t2
            )

            # 5️⃣ 成功
            update_main_log(db_conn, log_id, "SUCCESS")

        except Exception as e:
            update_main_log(db_conn, log_id, "FAIL", str(e))


if __name__ == "__main__":
    main()

