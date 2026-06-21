from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import random
import time
import redis
from common.logging import get_logger
from common.context import set_correlation_id
from common.config import KAFKA_BOOTSTRAP_SERVERS, REDIS_URL
from common.events import create_event, create_rejected_event, validate_event
from common.kafka_client import publish

logger = get_logger(__name__)


def create_consumer():
    while True:
        try:
            return KafkaConsumer(
                'orders',
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda x: json.loads(x.decode()),
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                group_id='payment-service-group',
            )
        except NoBrokersAvailable:
            time.sleep(5)


def create_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode(),
                acks='all',
                retries=5,
            )
        except NoBrokersAvailable:
            time.sleep(5)


consumer = create_consumer()
producer = create_producer()
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
logger.info("payment service started and listening for orders")

for msg in consumer:
    event = msg.value
    set_correlation_id(event.get('correlation_id'))
    logger.info("received order event", extra={'event': event})

    try:
        validate_event(event, {'ORDER_CREATED'})
        payload = event['payload']
        cache_key = f"payment-result:{event['event_id']}"
        cached_result = redis_client.get(cache_key)
        if cached_result:
            result = json.loads(cached_result)
            logger.info("reusing payment result", extra={'event_id': event['event_id']})
        else:
            success = random.choice([True, False])
            result = create_event(
                'PAYMENT_SUCCESS' if success else 'PAYMENT_FAILED',
                {'order_id': payload['order_id']},
                event.get('correlation_id'),
                causation_id=event['event_id'],
            )
            redis_client.set(cache_key, json.dumps(result))
        producer.send('payments', result).get(timeout=10)
        consumer.commit()
        logger.info("payment result published", extra={'result': result})
    except (KeyError, TypeError, ValueError) as exc:
        try:
            publish('orders-dlq', create_rejected_event('orders', event, exc))
        except Exception:
            logger.exception("order dead-letter publish failed")
            time.sleep(2)
            continue
        consumer.commit()
        logger.error("order event rejected", extra={'error': str(exc), 'event': event})
    except Exception:
        logger.exception("order event handling failed", extra={'event': event})
        time.sleep(2)
