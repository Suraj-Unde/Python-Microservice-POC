<<<<<<< HEAD
# Event-Driven Food Delivery POC
=======
# Microservices Food Delivery System (Python)
>>>>>>> ce10b2b6cf53bfc17e929452a27e9141f05f6535

A small Python microservices example that demonstrates an eventually consistent
order saga with FastAPI, Kafka, PostgreSQL, Redis, and Docker Compose.

## Architecture

<<<<<<< HEAD
```text
Client
  |
  v
API Gateway ---> Order API ---> PostgreSQL (orders + outbox)
                                    |               ^
                               Outbox worker        | Order status worker
                                    |               |
                              ORDER_CREATED         | payment outcome
                                    v               |
                                  Kafka ------------+
                                    |
                                    +--> Payment worker ---> Redis
                                    |
                                    +--> Delivery worker
=======
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
>>>>>>> ce10b2b6cf53bfc17e929452a27e9141f05f6535
```

The Order API, outbox worker, and order status worker are separate processes in
the same bounded context. They are the only components that access order data.

## Saga Flow

1. `POST /place-order` atomically creates a `PENDING` order and outbox row.
2. The outbox worker publishes `ORDER_CREATED` and marks the row published.
3. The Payment worker chooses a simulated result and publishes either
   `PAYMENT_SUCCESS` or `PAYMENT_FAILED`.
4. The Order status worker changes the order to `COMPLETED` or `CANCELLED`.
5. The Delivery worker assigns delivery only after `PAYMENT_SUCCESS`.

Every event uses a versioned envelope with `event_id`, `event_type`,
`schema_version`, `occurred_at`, `correlation_id`, `causation_id`, and `payload`.

## Reliability Guarantees

- Consumers disable auto-commit and commit offsets after successful handling.
- Order creation and its outbox event commit in one PostgreSQL transaction.
- Redis stores the payment decision by order event ID. A redelivered event
  republishes the same outcome instead of charging differently.
- The Order worker records processed payment event IDs in PostgreSQL, making
  order status updates idempotent.
- Kafka producers request `acks=all`, retry transient sends, and wait for broker
  acknowledgement.
- The gateway has a bounded upstream timeout and returns `503` when the Order
  API is unavailable.
- Compose waits for PostgreSQL and the Order API health check before starting
  dependent Order processes.
- Invalid contract events are published to `orders-dlq` or `payments-dlq` before
  their source offset is committed.
- Application containers run as an unprivileged user.

This remains a learning POC, not a production payment platform. The remaining
risks and recommended delivery order are in
[Complete-POC-Doc.md](Complete-POC-Doc.md).

## Run

```bash
docker compose up --build
```

Create and inspect an order:

```bash
curl -i -X POST http://localhost:8000/place-order \
  -H "Content-Type: application/json" \
  -d '{"item":"standard meal"}'
curl -i http://localhost:8000/orders/1
```

<<<<<<< HEAD
The first response is normally `PENDING`. Poll the second endpoint until it is
`COMPLETED` or `CANCELLED`.
=======
## What to Observe
>>>>>>> ce10b2b6cf53bfc17e929452a27e9141f05f6535

Useful commands:

<<<<<<< HEAD
```bash
docker compose logs -f api-gateway order-service order-outbox-worker order-status-worker payment-service delivery-service
docker compose up -d --no-deps --build order-service order-outbox-worker order-status-worker
docker compose down
```
=======
## Failure Simulation
>>>>>>> ce10b2b6cf53bfc17e929452a27e9141f05f6535

Order Service exposes database-aware `/health` and `/metrics` internally. The
gateway exposes liveness at `/health` and dependency readiness at `/ready`.

<<<<<<< HEAD
Run the contract tests with:
=======
- SUCCESS
- FAILURE
>>>>>>> ce10b2b6cf53bfc17e929452a27e9141f05f6535

```bash
python -m unittest discover -s tests -v
```

## Configuration

<<<<<<< HEAD
| Variable | Used by | Default |
| --- | --- | --- |
| `ORDER_SERVICE_URL` | Gateway | `http://order-service:8000` |
| `UPSTREAM_TIMEOUT_SECONDS` | Gateway | `5` |
| `DATABASE_URL` | Order processes | Compose PostgreSQL URL |
| `KAFKA_BOOTSTRAP_SERVERS` | Event producers/consumers | `kafka:9092` |
| `KAFKA_CONNECT_ATTEMPTS` | Order event producer | `5` |
| `REDIS_URL` | Payment worker | `redis://redis:6379/0` |

The credentials and exposed infrastructure ports in Compose are development
defaults only. Use secrets and private networks in a real environment.
=======
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
>>>>>>> ce10b2b6cf53bfc17e929452a27e9141f05f6535
