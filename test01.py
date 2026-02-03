# -*- coding: utf-8 -*-
from kafka import KafkaProducer
import json

# Kafka 配置
KAFKA_BOOTSTRAP = ["192.168.5.167:9092"]
KAFKA_TOPIC = "TEST_TASK"

# 初始化 producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# 测试消息
# msg ={
#     "order_no": "001",
#     "cell_id":" 001",
#   "test_item": "test01",
#   "query_sql": "select id,entrust_order_no,detection_task_no from xwd_mart.python_test_source where id = '009521273fa2712365fd692342e36e21'",
#   "insert_sql": "INSERT INTO xwd_mart_fix.python_test_source (id, entrust_order_no, detection_task_no) VALUES (?, ?, ?)"
# }
msg = {
    "order_no": "001",
    "cell_id": " 001",
    "test_item": "test01",
    "query_sql": "select id,entrust_order_no,detection_task_no from xwd_mart.python_test_source where id = '009521273fa2712365fd692342e36e21'",
    "insert_table": "xwd_mart_fix.python_test_source",
    "insert_cols": ["id", "entrust_order_no", "detection_task_no"]
}
#     {
#     "query_sql": "select id,entrust_order_no,detection_task_no from xwd_mart.python_test_source where id = '009521273fa2712365fd692342e36e21';",
#     "insert_sql": "INSERT INTO xwd_mart_fix.python_test_source (id, entrust_order_no, detection_task_no) VALUES('', '', '');",
#     "test_item": "capacity1"
# }

# 发送消息
producer.send(KAFKA_TOPIC, msg)
producer.flush()  # 确保立即发送
print("消息发送成功:", msg)


