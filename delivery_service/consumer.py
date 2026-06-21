from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import time
from common.logging import get_logger
from common.context import set_correlation_id
from common.config import KAFKA_BOOTSTRAP_SERVERS
from common.events import create_rejected_event, validate_event
from common.kafka_client import publish

logger = get_logger(__name__)


def create_consumer():
    while True:
        try:
            return KafkaConsumer(
                'payments',
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda x: json.loads(x.decode()),
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                group_id='delivery-service-group',
            )
        except NoBrokersAvailable:
            time.sleep(5)


consumer = create_consumer()
logger.info("delivery service started and listening for payment events")

for msg in consumer:
    event = msg.value
    set_correlation_id(event.get('correlation_id'))
    logger.info("received payment event", extra={'event': event})

    try:
        validate_event(event, {'PAYMENT_SUCCESS', 'PAYMENT_FAILED'})
        order_id = event['payload']['order_id']
        if event['event_type'] == 'PAYMENT_SUCCESS':
            logger.info("delivery assigned", extra={'order_id': order_id})
        else:
            logger.info("delivery skipped", extra={'order_id': order_id})
        consumer.commit()
    except (KeyError, TypeError, ValueError) as exc:
        try:
            publish('payments-dlq', create_rejected_event('payments', event, exc))
        except Exception:
            logger.exception("payment dead-letter publish failed")
            time.sleep(2)
            continue
        consumer.commit()
        logger.error("payment event rejected", extra={'error': str(exc), 'event': event})
    except Exception:
        logger.exception("payment event handling failed", extra={'event': event})
        time.sleep(2)
