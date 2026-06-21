import time
from datetime import datetime, timezone

from sqlalchemy import select

from common.kafka_client import publish
from common.logging import get_logger
from order_service.db import SessionLocal
from order_service.models import OutboxEvent

logger = get_logger(__name__)


def publish_next():
    with SessionLocal.begin() as db:
        event = db.execute(
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.created_at)
            .with_for_update(skip_locked=True)
            .limit(1)
        ).scalar_one_or_none()
        if event is None:
            return False

        publish(event.topic, event.payload)
        event.published_at = datetime.now(timezone.utc)
        logger.info(
            "outbox event published",
            extra={"event_id": event.event_id, "topic": event.topic},
        )
        return True


def run():
    logger.info("outbox worker started")
    while True:
        try:
            if not publish_next():
                time.sleep(1)
        except Exception:
            logger.exception("outbox publish failed")
            time.sleep(2)


if __name__ == "__main__":
    run()
