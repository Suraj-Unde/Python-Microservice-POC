from kafka import KafkaProducer
import json
import logging
import time

logging.basicConfig(level=logging.INFO)
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
                break
            except Exception as exc:
                logging.warning('Kafka producer init failed, retrying: %s', exc)
                time.sleep(5)
    return producer


def publish(topic, event):
    producer = get_producer()
    producer.send(topic, event)
    producer.flush()
