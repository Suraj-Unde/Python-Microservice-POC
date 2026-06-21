from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from uuid import uuid4
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST
from sqlalchemy import text
from common.events import create_event
from common.logging import Span, get_logger
from common.context import set_correlation_id, get_correlation_id
from order_service.db import SessionLocal, engine
from order_service.models import Base, Order, OutboxEvent

Base.metadata.create_all(bind=engine)

app = FastAPI()
logger = get_logger(__name__)

order_created_counter = Counter(
    "order_service_orders_created_total",
    "Total number of orders created",
)


@app.middleware("http")
async def add_correlation_id(request: Request, call_next):
    cid = request.headers.get("X-Correlation-ID") or str(uuid4())
    set_correlation_id(cid)
    response = await call_next(request)
    response.headers["X-Correlation-ID"] = cid
    return response


class OrderCreate(BaseModel):
    item: str = Field(min_length=1, max_length=200)


@app.get("/metrics")
def metrics():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
def health():
    try:
        with SessionLocal() as db:
            db.execute(text("SELECT 1"))
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return {"status": "ok"}


@app.get("/orders/{order_id}")
def get_order(order_id: int):
    with SessionLocal() as db:
        db_order = db.get(Order, order_id)
        if db_order is None:
            raise HTTPException(status_code=404, detail="Order not found")
        return {"order_id": db_order.id, "status": db_order.status}


@app.post('/orders', status_code=202)
def create_order(order: OrderCreate):
    with Span("create_order", logger):
        order_created_counter.inc()
        db = SessionLocal()
        try:
            logger.info(
                "starting order creation",
                extra={"order_item": order.item},
            )

            db_order = Order(status='PENDING')
            db.add(db_order)
            db.flush()

            event = create_event(
                "ORDER_CREATED",
                {"order_id": db_order.id, "item": order.item},
                get_correlation_id(),
            )
            db.add(
                OutboxEvent(
                    event_id=event["event_id"],
                    topic="orders",
                    payload=event,
                )
            )
            db.commit()

            logger.info(
                "order and outbox event committed",
                extra={
                    'order_id': db_order.id,
                    'event_id': event['event_id'],
                    'correlation_id': event['correlation_id'],
                },
            )

            logger.info(
                "order created",
                extra={'order_id': db_order.id, 'status': db_order.status},
            )
            return {'order_id': db_order.id, 'status': db_order.status}
        finally:
            db.close()
