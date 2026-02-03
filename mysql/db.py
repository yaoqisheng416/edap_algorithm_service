# -*- coding: utf-8 -*-
import pymysql
from config import *
from datetime import datetime


def get_detail_table():
    return f"algo_log_detail_{datetime.now():%y%m}"


def get_mysql_conn():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset="utf8"
    )


def ensure_detail_table(conn, table_name):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        log_id BIGINT NOT NULL,
        stage VARCHAR(50) NOT NULL,
        action VARCHAR(100) NOT NULL,
        level VARCHAR(10) NOT NULL,
        status VARCHAR(20) NOT NULL,
        message TEXT,
        start_time DATETIME NOT NULL,
        end_time DATETIME NOT NULL,
        cost_ms BIGINT NOT NULL,
        create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        KEY idx_log_id (log_id)
    ) ENGINE=InnoDB;
    """
    conn.cursor().execute(sql)
    conn.commit()
