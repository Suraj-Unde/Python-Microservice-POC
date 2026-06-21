import json
import time

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from sqlalchemy.exc import IntegrityError

from common.config import KAFKA_BOOTSTRAP_SERVERS
from common.context import set_correlation_id
from common.events import create_rejected_event, validate_event
from common.kafka_client import publish
from common.logging import get_logger
from order_service.db import SessionLocal
from order_service.models import Order, ProcessedEvent

logger = get_logger(__name__)


def create_consumer():
    while True:
        try:
            return KafkaConsumer(
                "payments",
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda value: json.loads(value.decode()),
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                group_id="order-status-group",
            )
        except NoBrokersAvailable:
            logger.warning("Kafka unavailable; retrying")
            time.sleep(5)


consumer = create_consumer()
logger.info("order status worker started")

for message in consumer:
    event = message.value
    set_correlation_id(event.get("correlation_id"))

    try:
        validate_event(event, {"PAYMENT_SUCCESS", "PAYMENT_FAILED"})
        event_type = event["event_type"]
        payload = event["payload"]
        with SessionLocal.begin() as db:
            db.add(ProcessedEvent(event_id=event["event_id"], event_type=event_type))
            order = db.get(Order, payload["order_id"])
            if order is None:
                raise ValueError(f"order {payload['order_id']} does not exist")
            order.status = "COMPLETED" if event_type == "PAYMENT_SUCCESS" else "CANCELLED"
        consumer.commit()
        logger.info(
            "order status updated",
            extra={"order_id": payload["order_id"], "status": order.status},
        )
    except IntegrityError:
        consumer.commit()
        logger.info("skipping processed payment event", extra={"event_id": event["event_id"]})
    except (KeyError, TypeError, ValueError) as exc:
        try:
            publish("payments-dlq", create_rejected_event("payments", event, exc))
        except Exception:
            logger.exception("payment dead-letter publish failed")
            time.sleep(2)
            continue
        consumer.commit()
        logger.error("payment event rejected", extra={"error": str(exc), "event": event})
    except Exception:
        logger.exception("payment event handling failed", extra={"event": event})
        time.sleep(2)
