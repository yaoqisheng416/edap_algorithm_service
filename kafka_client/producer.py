# -*- coding: utf-8 -*-
from kafka import KafkaProducer
import json
from config import *

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def send(topic, msg):
    producer.send(topic, msg)
    producer.flush()
