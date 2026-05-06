from kafka import KafkaProducer
import json
import time
from common.logging import get_logger

logger = get_logger(__name__)
producer = None


def get_producer():
    global producer
    if producer is None:
        while True:
            try:
                producer = KafkaProducer(
                    bootstrap_servers='kafka:9092',
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                )
                logger.info("Kafka producer initialized")
                break
            except Exception as exc:
                logger.warning("Kafka producer init failed, retrying", extra={"error": str(exc)})
                time.sleep(5)
    return producer


def publish(topic, event):
    producer = get_producer()
    logger.info("publishing event to Kafka", extra={"topic": topic, "event_id": event.get("event_id"), "order_id": event.get("order_id")})
    producer.send(topic, event)
    producer.flush()
    logger.info("event published to Kafka", extra={"topic": topic, "event_id": event.get("event_id"), "order_id": event.get("order_id")})
