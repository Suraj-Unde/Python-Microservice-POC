from sqlalchemy.orm import declarative_base
from datetime import datetime, timezone

from sqlalchemy import JSON, Column, DateTime, Integer, String

Base = declarative_base()

class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True)
    status = Column(String(32), nullable=False, index=True)


class ProcessedEvent(Base):
    __tablename__ = "processed_events"
    event_id = Column(String(36), primary_key=True)
    event_type = Column(String(64), nullable=False)


class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    event_id = Column(String(36), primary_key=True)
    topic = Column(String(128), nullable=False, index=True)
    payload = Column(JSON, nullable=False)
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    published_at = Column(DateTime(timezone=True), nullable=True, index=True)
