from kafka import KafkaProducer
import json
import time
from common.logging import get_logger
from common.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONNECT_ATTEMPTS

logger = get_logger(__name__)
producer = None


def get_producer():
    global producer
    if producer is None:
        for attempt in range(1, KAFKA_CONNECT_ATTEMPTS + 1):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    acks='all',
                    retries=5,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                )
                logger.info("Kafka producer initialized")
                return producer
            except Exception as exc:
                logger.warning(
                    "Kafka producer init failed",
                    extra={"error": str(exc), "attempt": attempt},
                )
                if attempt < KAFKA_CONNECT_ATTEMPTS:
                    time.sleep(1)
        raise RuntimeError("Kafka producer unavailable")
    return producer


def publish(topic, event):
    global producer
    producer = get_producer()
    logger.info("publishing event to Kafka", extra={"topic": topic, "event_id": event.get("event_id"), "order_id": event.get("order_id")})
    try:
        producer.send(topic, event).get(timeout=10)
    except Exception:
        producer.close(timeout=1)
        producer = None
        raise
    logger.info("event published to Kafka", extra={"topic": topic, "event_id": event.get("event_id"), "order_id": event.get("order_id")})
