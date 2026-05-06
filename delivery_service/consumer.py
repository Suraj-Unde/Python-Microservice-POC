from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import time


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

for msg in consumer:
    event = msg.value
    if event['event_type'] == 'PAYMENT_SUCCESS':
        print('Delivery assigned for order', event['order_id'])
    elif event['event_type'] == 'PAYMENT_FAILED':
        print('Delivery skipped for order', event['order_id'])
