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

        try:
            # 跳过已处理消息
            if exists_main_log(db_conn, msg):
                continue

            task = msg.value

            # 1️⃣ 主日志
            log_id = insert_main_log(db_conn, msg, task)

            # 2️⃣ 获取策略，不校验 test_item 是否存在，容错处理
            try:
                strategy = get_strategy(task.get("test_item"))
            except Exception as e:
                insert_detail_log(
                    db_conn, log_id,
                    stage="STRATEGY", action="get_strategy",
                    level="ERROR", status="FAIL",
                    start_time=datetime.utcnow(), end_time=datetime.utcnow(),
                    message=str(e)
                )
                update_main_log(db_conn, log_id, "FAIL", str(e))
                continue

            # 3️⃣ 获取脚本
            try:
                script = get_script(task.get("test_item"))
                if not script:
                    raise Exception(f"No script found for test_item={task.get('test_item')}")
            except Exception as e:
                insert_detail_log(
                    db_conn, log_id,
                    stage="SCRIPT", action="get_script",
                    level="ERROR", status="FAIL",
                    start_time=datetime.utcnow(), end_time=datetime.utcnow(),
                    message=str(e)
                )
                update_main_log(db_conn, log_id, "FAIL", str(e))
                continue

            # 4️⃣ 查询（SQL 来自 Kafka）
            t1 = datetime.utcnow()
            try:
                data = ck.query(task["query_sql"])
            except Exception as e:
                insert_detail_log(
                    db_conn, log_id,
                    stage="CK", action="query",
                    level="ERROR", status="FAIL",
                    start_time=t1, end_time=datetime.utcnow(),
                    message=str(e)
                )
                update_main_log(db_conn, log_id, "FAIL", f"CK query failed: {e}")
                continue
            t2 = datetime.utcnow()
            insert_detail_log(
                db_conn, log_id,
                stage="CK", action="query",
                level="INFO", status="SUCCESS",
                start_time=t1, end_time=t2
            )

            # 5️⃣ 执行处理脚本
            t1 = datetime.utcnow()
            try:
                res = run_worker({
                    "script": script,
                    "data": data,
                    "context": task
                })
                if res.get("status") != "SUCCESS":
                    raise Exception(res.get("error", "Unknown error in script"))
            except Exception as e:
                insert_detail_log(
                    db_conn, log_id,
                    stage="EXEC", action="algorithm",
                    level="ERROR", status="FAIL",
                    start_time=t1, end_time=datetime.utcnow(),
                    message=str(e)
                )
                update_main_log(db_conn, log_id, "FAIL", str(e))
                continue
            t2 = datetime.utcnow()
            insert_detail_log(
                db_conn, log_id,
                stage="EXEC", action="algorithm",
                level="INFO", status="SUCCESS",
                start_time=t1, end_time=t2
            )

            # 6️⃣ 插入结果（表名、列名来自 Kafka 消息）
            t1 = datetime.utcnow()
            try:
                insert_values = strategy.build_insert_values(res["result"], data)
                if insert_values:
                    ck.insert(
                        table=task.get("insert_table"),
                        columns=task.get("insert_cols"),
                        values=insert_values
                    )
            except Exception as e:
                insert_detail_log(
                    db_conn, log_id,
                    stage="CK", action="insert",
                    level="ERROR", status="FAIL",
                    start_time=t1, end_time=datetime.utcnow(),
                    message=str(e)
                )
                update_main_log(db_conn, log_id, "FAIL", str(e))
                continue
            t2 = datetime.utcnow()
            insert_detail_log(
                db_conn, log_id,
                stage="CK", action="insert",
                level="INFO", status="SUCCESS",
                start_time=t1, end_time=t2
            )

            # 7️⃣ 成功
            update_main_log(db_conn, log_id, "SUCCESS")

        except Exception:
            # 捕获整个循环的未处理异常，保证服务不中断
            error_msg = traceback.format_exc()
            try:
                update_main_log(db_conn, log_id, "FAIL", error_msg)
            except Exception:
                # 即使更新日志也失败，也不抛异常
                pass
            continue


if __name__ == "__main__":
    main()

