from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import random
import time


def create_consumer():
    while True:
        try:
            return KafkaConsumer(
                'orders',
                bootstrap_servers='kafka:9092',
                value_deserializer=lambda x: json.loads(x.decode()),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='payment-service-group',
            )
        except NoBrokersAvailable:
            time.sleep(5)


def create_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode(),
            )
        except NoBrokersAvailable:
            time.sleep(5)


consumer = create_consumer()
producer = create_producer()
processed = set()

for msg in consumer:
    event = msg.value
    if event['event_id'] in processed:
        continue

    processed.add(event['event_id'])

    if event['event_type'] == 'ORDER_CREATED':
        success = random.choice([True, False])
        result = {
            'event_type': 'PAYMENT_SUCCESS' if success else 'PAYMENT_FAILED',
            'order_id': event['order_id'],
            'event_id': event['event_id'],
        }
        producer.send('payments', result)
        producer.flush()
        print('Payment result:', result)
