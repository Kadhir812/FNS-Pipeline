import json
from kafka import KafkaProducer
from .config import KAFKA_BROKER, TOPIC


def create_producer():
    print(f"[DEBUG] Kafka broker used: {KAFKA_BROKER}")
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def publish_article(producer, payload):
    producer.send(TOPIC, value=payload)


def flush(producer):
    producer.flush()
