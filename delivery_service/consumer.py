from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import time
from common.logging import get_logger
from common.context import set_correlation_id

logger = get_logger(__name__)


def create_consumer():
    while True:
        try:
            return KafkaConsumer(
                'payments',
                bootstrap_servers='kafka:9092',
                value_deserializer=lambda x: json.loads(x.decode()),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
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

    if event['event_type'] == 'PAYMENT_SUCCESS':
        logger.info("delivery assigned", extra={'order_id': event['order_id']})
    elif event['event_type'] == 'PAYMENT_FAILED':
        logger.info("delivery skipped", extra={'order_id': event['order_id']})
    else:
        logger.info("unhandled payment event type", extra={'event_type': event['event_type'], 'order_id': event['order_id']})
