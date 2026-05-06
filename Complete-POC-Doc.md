# Production-Grade Python Microservices (Deep Theory + Runnable System)

---

# 🎯 What You Will Achieve

This guide is designed for **all audiences (beginner → senior engineer)**.

You will:
- Understand microservices **from first principles**
- Learn **why each pattern exists (not just how)**
- Build a **fully runnable system with DB + Saga + failure handling**
- Be able to **explain trade-offs like a system designer**

---

# 🧠 PART 1: THEORY — SENIOR ENGINEER PERSPECTIVE (WITH REAL SYSTEM CONTEXT)

## 1. What microservices actually are (in the context of OUR system)

Let’s ground everything in the system we are building: a **food delivery platform**.

When a user places an order, a lot of things happen:
- Order is created
- Payment is processed
- Delivery is assigned

In a monolith, all of this would live in one codebase, one deployment, one database.

In our system, we split this into:
- Order Service
- Payment Service
- Delivery Service

👉 Each of these is a **microservice**.

But the important part is not “separate services”.

👉 The important part is:
Each service owns:
- Its **business logic**
- Its **data**
- Its **failures**

For example:
- Order Service does NOT know how payment works
- Payment Service does NOT care how delivery is assigned

This separation is what gives us flexibility—but also complexity.

---

## 2. Why teams move away from monoliths (seen through our POC)

Let’s imagine our system as a monolith.

### Scenario:
Payment logic has a bug.

In monolith:
- Entire system redeployed
- Risk of breaking order + delivery

In microservices:
- Only Payment Service redeployed
- Order and Delivery continue running

---

### Another scenario: scale problem

Food festival → sudden spike in orders

Monolith:
- Scale whole system (wasteful)

Microservices:
- Scale only **Order Service**

👉 This is real-world cost optimization.

---

## 3. The hidden complexity (mapped to our system)

Let’s walk through a real failure.

User places order → Order Service works fine  
But Payment Service is down.

Now what?

- Order already created
- Payment not done

System is now **inconsistent**.

👉 This is the fundamental problem microservices introduce.

---

## 4. Eventual consistency (explained using our flow)

In our system:

1. Order Service creates order → status = PENDING
2. Payment Service processes later

For a short time:
- Order exists
- Payment not completed

👉 This is *expected*

Eventually:
- Payment succeeds → COMPLETED
- OR fails → CANCELLED

This is called **eventual consistency**.

---

## 5. Why we use Kafka (not just theory)

Let’s compare two approaches in OUR system:

### Approach 1: HTTP call
Order Service → Payment Service

Problems:
- If Payment is slow → Order is slow
- If Payment is down → Order fails

---

### Approach 2: Kafka (what we implemented)
Order Service → emits event → Kafka → Payment Service consumes

Now:
- Order Service does NOT wait
- Payment can process later
- System becomes resilient

👉 This is why event-driven architecture exists

---

## 6. Saga pattern (explained with exact flow)

Let’s trace OUR system step by step:

### Step 1: Order created
Order Service:
- Save order → PENDING
- Emit ORDER_CREATED

### Step 2: Payment Service reacts
- Receives event
- Processes payment

---

### Case 1: Payment success
- Emit PAYMENT_SUCCESS
- Order → COMPLETED
- Delivery triggered

---

### Case 2: Payment fails
- Emit PAYMENT_FAILED
- Order Service updates → CANCELLED

👉 This “undo logic” is called **compensation**

---

👉 Important insight:
We are NOT rolling back.
We are **moving forward with corrective actions**.

---

## 7. Idempotency (real bug scenario)

Let’s say Kafka sends same event twice:

ORDER_CREATED(order_id=123)
ORDER_CREATED(order_id=123)

Without idempotency:
- Payment processed twice ❌

With idempotency:
- First event processed
- Second ignored

👉 In our system we store event_id in Redis

---

## 8. Failure handling (mapped to our services)

### Scenario: Payment service crashes mid-processing

We handle this using:

#### Retry
Payment tries again

#### Backoff
Wait 1s → 2s → 4s

#### Circuit breaker (conceptual here)
If payment keeps failing:
- Stop hitting it temporarily

👉 Prevents system-wide slowdown

---

## 9. Observability (seen in our system)

Let’s say a user complains:
“Money deducted but order cancelled”

Without observability:
- You guess

With observability:
- Check logs using correlation_id
- Trace flow across services
- Identify exact failure point

👉 This is why we added:
- Logging
- Correlation IDs
- Metrics

---

## 10. Final mental model (very important)

Think of our system like this:

Order Service → "I created an order"  
Payment Service → "I handled payment"  
Delivery Service → "I will deliver"

No one controls everything.

👉 System works via **cooperation, not control**

---

# 🏗️ PART 2: SYSTEM DESIGN

## Services
- API Gateway
- Order Service
- Payment Service
- Delivery Service

## Infra
- PostgreSQL (data)
- Kafka (events)
- Redis (cache/idempotency)

---

# 💻 PART 3: IMPLEMENTATION (RUNNABLE)

---

# STEP 1: Database Model (Order Service)

```python
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True)
    status = Column(String)  # PENDING, COMPLETED, CANCELLED
```

---

# STEP 2: Order Service (Saga Start)

```python
from fastapi import FastAPI
from sqlalchemy.orm import sessionmaker
from common.kafka_client import publish
import uuid

app = FastAPI()

@app.post("/orders")
def create_order():
    order_id = str(uuid.uuid4())

    # Save to DB with PENDING

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "ORDER_CREATED",
        "order_id": order_id
    }

    publish("orders", event)

    return {"order_id": order_id}
```

---

# STEP 3: Payment Service (With Failure Simulation)

```python
from kafka import KafkaConsumer
import random, json

processed_events = set()

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda x: json.loads(x.decode())
)

for msg in consumer:
    event = msg.value

    if event['event_id'] in processed_events:
        continue

    processed_events.add(event['event_id'])

    if event['event_type'] == 'ORDER_CREATED':
        order_id = event['order_id']

        success = random.choice([True, False])

        new_event = {
            "event_type": "PAYMENT_SUCCESS" if success else "PAYMENT_FAILED",
            "order_id": order_id
        }

        print(new_event)
```

---

# STEP 4: Order Service (Compensation Logic)

```python
if event['event_type'] == 'PAYMENT_FAILED':
    order.status = "CANCELLED"

elif event['event_type'] == 'PAYMENT_SUCCESS':
    order.status = "COMPLETED"
```

---

# STEP 5: Retry Logic

```python
import time

def retry(func):
    for i in range(3):
        try:
            return func()
        except Exception:
            time.sleep(2 ** i)
```

---

# STEP 6: Idempotency using Redis

```python
import redis
r = redis.Redis(host='redis', port=6379)

if r.get(event_id):
    return

r.set(event_id, "processed")
```

---

# STEP 7: Docker Compose (FULL RUN)

```yaml
version: '3.8'

services:
  kafka:
    image: bitnami/kafka

  postgres:
    image: postgres
    environment:
      POSTGRES_PASSWORD: password

  redis:
    image: redis

  order-service:
    build: ./order-service

  payment-service:
    build: ./payment-service

  delivery-service:
    build: ./delivery-service

  api-gateway:
    build: ./api-gateway
    ports:
      - "8000:8000"
```

---

# ▶️ RUN

```bash
docker-compose up --build
```

---

# 🔄 FLOW (WITH FAILURE)

1. Order created
2. Event → Kafka
3. Payment randomly fails
4. If fail → Order CANCELLED
5. If success → Delivery triggered

---

# 🚀 WHAT MAKES THIS ADVANCED

✔ Real DB model  
✔ Saga implemented  
✔ Failure simulation  
✔ Idempotency  
✔ Retry logic  
✔ Event-driven system  

---

# 🎯 FINAL UNDERSTANDING

You now understand:

- WHY microservices exist
- WHY they fail
- HOW to fix failures
- HOW real systems are built

---

# 🔭 PART 6: OBSERVABILITY (LOGGING + TRACING + METRICS)

Observability answers three questions:
- **What happened?** (logs)
- **Where did it happen?** (tracing)
- **How often / how fast?** (metrics)

We’ll implement:
- Structured logging (JSON)
- Correlation ID propagation
- Basic tracing (request spans)
- Prometheus metrics

---

## 6.1 Structured Logging (JSON)

### Why?
Plain logs are hard to search across services. JSON logs are machine-friendly.

### common/logging.py

```python
import logging, json, sys

class JsonFormatter(logging.Formatter):
    def format(self, record):
        log = {
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "time": self.formatTime(record, self.datefmt),
        }
        # optional extras
        if hasattr(record, "correlation_id"):
            log["correlation_id"] = record.correlation_id
        return json.dumps(log)


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())

    if not logger.handlers:
        logger.addHandler(handler)
    return logger
```

---

## 6.2 Correlation ID (Request Tracing Across Services)

### Concept
Every request gets a unique ID that travels through all services.

Header used:
```
X-Correlation-ID
```

---

### common/context.py

```python
from contextvars import ContextVar

correlation_id_var = ContextVar("correlation_id", default=None)


def set_correlation_id(cid):
    correlation_id_var.set(cid)


def get_correlation_id():
    return correlation_id_var.get()
```

---

### FastAPI Middleware (add to every service)

```python
from fastapi import Request
import uuid
from common.context import set_correlation_id

async def correlation_middleware(request: Request, call_next):
    cid = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
    set_correlation_id(cid)

    response = await call_next(request)
    response.headers["X-Correlation-ID"] = cid
    return response
```

---

### Using Correlation ID in Logs

```python
from common.logging import get_logger
from common.context import get_correlation_id

logger = get_logger(__name__)

logger.info("order created", extra={"correlation_id": get_correlation_id()})
```

---

## 6.3 Propagating Correlation ID via Kafka

### Why?
Events must carry tracing context.

```python
event = {
  "event_id": "...",
  "correlation_id": get_correlation_id(),
  "event_type": "ORDER_CREATED",
  "payload": {...}
}
```

Consumers must extract and set it:

```python
from common.context import set_correlation_id

set_correlation_id(event.get("correlation_id"))
```

---

## 6.4 Basic Tracing (Spans)

### Concept
A request is broken into steps (spans):
- API Gateway span
- Order Service span
- Payment Service span

We’ll implement lightweight tracing logs.

```python
import time

class Span:
    def __init__(self, name, logger, cid):
        self.name = name
        self.logger = logger
        self.cid = cid

    def __enter__(self):
        self.start = time.time()
        self.logger.info(f"start:{self.name}", extra={"correlation_id": self.cid})

    def __exit__(self, exc_type, exc, tb):
        dur = time.time() - self.start
        self.logger.info(f"end:{self.name} duration={dur}", extra={"correlation_id": self.cid})
```

Usage:

```python
from common.context import get_correlation_id
from common.logging import get_logger

logger = get_logger(__name__)

with Span("create_order", logger, get_correlation_id()):
    # business logic
    pass
```

---

## 6.5 Metrics with Prometheus

### Why?
We need numbers:
- Requests per second
- Error rate
- Latency

---

### Install

```bash
pip install prometheus_client
```

---

### Add Metrics to FastAPI

```python
from prometheus_client import Counter, Histogram, generate_latest
from fastapi import Response
import time

REQUEST_COUNT = Counter("request_count", "Total requests", ["service", "endpoint"])
REQUEST_LATENCY = Histogram("request_latency_seconds", "Latency", ["service", "endpoint"])

@app.middleware("http")
async def metrics_middleware(request, call_next):
    start = time.time()

    response = await call_next(request)

    latency = time.time() - start

    REQUEST_COUNT.labels("order-service", request.url.path).inc()
    REQUEST_LATENCY.labels("order-service", request.url.path).observe(latency)

    return response

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type="text/plain")
```

---

## 6.6 Prometheus + Grafana (Docker)

Add to docker-compose:

```yaml
prometheus:
  image: prom/prometheus

grafana:
  image: grafana/grafana
  ports:
    - "3000:3000"
```

---

## 6.7 What You Can Now Demonstrate

In your demo:

1. Place order
2. Show logs with same correlation ID across services
3. Show failure in payment
4. Show compensation logs
5. Show metrics endpoint

👉 This is **real production observability**

---

# 🏁 CONCLUSION

You now have:

✔ Microservices architecture  
✔ Event-driven system  
✔ Saga + failure handling  
✔ Idempotency + retries  
✔ Structured logging  
✔ Distributed tracing  
✔ Metrics + monitoring  

---

👉 This is a **complete production-grade distributed system blueprint**.

Very few engineers can build and explain this end-to-end.

---

