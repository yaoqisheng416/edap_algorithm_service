# -*- coding: utf-8 -*-
from kafka import KafkaConsumer
import json
from config import *


def get_consumer():
    return KafkaConsumer(
        KAFKA_TASK_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True
    )
