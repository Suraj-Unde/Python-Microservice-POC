from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from uuid import uuid4
from common.kafka_client import publish
from order_service.db import SessionLocal, engine
from order_service.models import Base, Order

Base.metadata.create_all(bind=engine)

app = FastAPI()

class OrderCreate(BaseModel):
    item: str | None = None

@app.post('/orders')
def create_order(order: OrderCreate):
    db = SessionLocal()
    try:
        db_order = Order(status='PENDING')
        db.add(db_order)
        db.commit()
        db.refresh(db_order)

        event = {
            'event_type': 'ORDER_CREATED',
            'order_id': db_order.id,
            'event_id': str(uuid4()),
            'item': order.item,
        }

        try:
            publish('orders', event)
        except Exception as exc:
            db_order.status = 'FAILED'
            db.add(db_order)
            db.commit()
            raise HTTPException(status_code=500, detail=str(exc))

        return {'order_id': db_order.id, 'status': db_order.status}
    finally:
        db.close()
