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
msg = {
    "order_no": "ORD001",
    "cell_id": "CELL01",
    "test_item": "capacity"
}

# 发送消息
producer.send(KAFKA_TOPIC, msg)
producer.flush()  # 确保立即发送
print("消息发送成功:", msg)


