from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
from uuid import uuid4
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from common.kafka_client import publish
from common.logging import Span, get_logger
from common.context import set_correlation_id, get_correlation_id
from order_service.db import SessionLocal, engine
from order_service.models import Base, Order

Base.metadata.create_all(bind=engine)

app = FastAPI()
logger = get_logger(__name__)

order_created_counter = Counter(
    "order_service_orders_created_total",
    "Total number of orders created",
)
order_publish_success_counter = Counter(
    "order_service_order_publish_success_total",
    "Total number of successfully published order events",
)
order_publish_failure_counter = Counter(
    "order_service_order_publish_failure_total",
    "Total number of failed order event publishes",
)
order_processing_histogram = Histogram(
    "order_service_order_processing_seconds",
    "Time spent processing order creation requests",
)


@app.middleware("http")
async def add_correlation_id(request: Request, call_next):
    cid = request.headers.get("X-Correlation-ID") or str(uuid4())
    set_correlation_id(cid)
    response = await call_next(request)
    response.headers["X-Correlation-ID"] = cid
    return response


class OrderCreate(BaseModel):
    item: str | None = None


@app.get("/metrics")
def metrics():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post('/orders')
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
            db.commit()
            db.refresh(db_order)

            event = {
                'event_type': 'ORDER_CREATED',
                'order_id': db_order.id,
                'event_id': str(uuid4()),
                'item': order.item,
                'correlation_id': get_correlation_id(),
            }

            logger.info(
                "publishing order event",
                extra={
                    'order_id': db_order.id,
                    'event_id': event['event_id'],
                    'correlation_id': event['correlation_id'],
                },
            )

            with order_processing_histogram.time():
                try:
                    publish('orders', event)
                except Exception as exc:
                    order_publish_failure_counter.inc()
                    db_order.status = 'FAILED'
                    db.add(db_order)
                    db.commit()
                    logger.exception(
                        "order publish failed",
                        extra={'order_id': db_order.id, 'event_id': event['event_id']},
                    )
                    raise HTTPException(status_code=500, detail="Order publish failed")
                else:
                    order_publish_success_counter.inc()
                    logger.info(
                        "order published successfully",
                        extra={'order_id': db_order.id, 'event_id': event['event_id']},
                    )

            logger.info(
                "order created",
                extra={'order_id': db_order.id, 'status': db_order.status},
            )
            return {'order_id': db_order.id, 'status': db_order.status}
        finally:
            db.close()
