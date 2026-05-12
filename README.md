# Microservices Food Delivery System (Python)

A production-style event-driven microservices system built in Python to demonstrate:

- Microservices architecture
- Event-driven communication using Kafka
- Saga pattern for distributed transactions
- Failure handling and compensation
- Observability fundamentals

## Why this project?

Most tutorials explain microservices in isolation.
This project answers the question:

> “What actually happens when things fail in a distributed system?”

Using a real-world food delivery scenario, it demonstrates:

- Order creation
- Payment processing
- Delivery assignment
- Failure recovery using Saga

## Architecture Overview

Key principles:

- Each service owns its logic and database
- Services communicate via Kafka events
- The system is eventually consistent

## End-to-End Flow

1. Client places an order
2. Order Service creates the order with status `PENDING`
3. Order Service publishes an event to Kafka
4. Payment Service consumes the event and processes payment
5. Delivery Service reacts to payment success and assigns delivery

## Saga Pattern

### Happy Path

- Order → Payment → Delivery
- Everything succeeds

### Failure Handling

- Payment fails
- Order is cancelled via compensation
- Delivery is skipped

## Tech Stack

- Python (FastAPI)
- Kafka (event streaming)
- PostgreSQL (per-service DB)
- Redis (idempotency - optional)
- Docker & Docker Compose

## Services

| Service          | Responsibility                          |
| ---------------- | ----------------------------------------|
| API Gateway      | Entry point for clients                  |
| Order Service    | Creates orders and starts the Saga       |
| Payment Service  | Processes payment events                 |
| Delivery Service | Assigns delivery after payment success   |

## How to Run

1. Clone the repository

```bash
git clone <your-repo-url>
cd Python-Microservice-POC
```

2. Start the system

```bash
docker-compose up --build
```
3. To rebuild and restart a single service after code changes
   
 ```bash
docker-compose up -d --no-deps --build <service-name>
 ```
Example:

 ```bash
docker-compose up -d --no-deps --build order-service
 ```

4. Trigger the flow

```bash
curl -X POST http://localhost:8000/place-order
```

## What to Observe

- Order created by the Order Service
- Payment randomly succeeds or fails
- Delivery assigned on payment success
- Order cancelled when payment fails
- Structured logs with `correlation_id` across order/payment/delivery services
- Kafka event flow from `ORDER_CREATED` → `PAYMENT_SUCCESS` / `PAYMENT_FAILED` → delivery action

## Failure Simulation

The Payment Service randomly returns:

- SUCCESS
- FAILURE

This demonstrates:

- Real-world unpredictability
- System resilience

## Observability (Basic)

- Service logs across components
- Event tracing through the flow
- Structured JSON logs in the Order Service, Payment Service, and Delivery Service
- Correlation ID propagation via `X-Correlation-ID`
- Kafka event metadata in logs for better tracing

Order Service now exposes Prometheus metrics at `/metrics`, including:

- total orders created
- Kafka publish success/failure counts
- order processing latency

Payment and Delivery services now log:

- startup and consumer readiness
- received Kafka events
- event handling decisions
- delivery assignment or skip actions

Use `docker-compose logs -f order_service payment_service delivery_service` to follow the full saga execution.
- Kafka publish success/failure counts
- order processing latency

Why Prometheus?

- It collects runtime metrics automatically
- It lets you monitor service behavior over time
- It helps answer questions like “how many orders are processed” and “how long order creation takes”

Potential extensions:

- Grafana for dashboarding
- OpenTelemetry for distributed traces

## Future Improvements

- Add OpenTelemetry tracing
- Add JWT authentication
- Add Kubernetes deployment
- Add retry logic and DLQ (Dead Letter Queue)
- Add a UI dashboard

## What this project demonstrates

- Real-world distributed system behavior
- Event-driven design
- Failure handling using Saga
- Production thinking, not just code
