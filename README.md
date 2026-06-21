# Event-Driven Food Delivery POC

A runnable Python microservices example demonstrating an eventually consistent
order saga with FastAPI, Kafka, PostgreSQL, Redis, and Docker Compose.

This is an educational POC. It implements several reliability patterns, but it
is not a production payment platform.

## Architecture

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
```

The Order API, outbox worker, and order status worker are separate processes in
the same service boundary. They are the only components that access order data.

## Components

| Component | Responsibility | State |
| --- | --- | --- |
| API Gateway | Public API, validation, routing, timeout handling | None |
| Order API | Create and query orders; write outbox events | PostgreSQL |
| Outbox worker | Publish committed order events to Kafka | PostgreSQL |
| Order status worker | Apply payment outcomes to orders | PostgreSQL |
| Payment worker | Simulate a stable payment outcome | Redis |
| Delivery worker | React to successful payments | Logs only |
| Kafka | Transport order and payment events | Kafka log |

## Saga Flow

1. The client submits an order through `POST /place-order`.
2. The Order API atomically stores a `PENDING` order and an outbox event.
3. The outbox worker publishes `ORDER_CREATED` and marks the row published.
4. The Payment worker chooses a simulated success or failure and stores that
   decision in Redis.
5. The Payment worker publishes `PAYMENT_SUCCESS` or `PAYMENT_FAILED`.
6. The Order status worker changes the order to `COMPLETED` or `CANCELLED`.
7. The Delivery worker assigns delivery only after successful payment.

Payment results are intentionally random. Redis ensures that Kafka redelivery
reuses the original decision instead of producing a different result.

## Reliability Features

- Transactional outbox for atomic order and event persistence.
- Manual Kafka offset commits after successful processing.
- Idempotent order updates using PostgreSQL `processed_events` records.
- Stable payment decisions keyed by source event ID in Redis.
- Kafka producer acknowledgements and bounded connection retries.
- Versioned event envelopes with UUID and timestamp validation.
- `orders-dlq` and `payments-dlq` topics for contract-invalid events.
- Correlation IDs propagated through HTTP, events, and structured logs.
- Gateway timeout and HTTP `503` handling for Order API outages.
- Database-aware readiness and Compose dependency health checks.
- Non-root application containers.

## Prerequisites

- Docker Desktop with the Linux container engine running
- Docker Compose v2 (`docker compose`)
- Python 3.10+ only when running tests outside Docker

## Quick Start

Start the complete environment:

```bash
docker compose up --build
```

Wait until `order-service` is healthy, then create an order:

```bash
curl -i -X POST http://localhost:8000/place-order \
  -H "Content-Type: application/json" \
  -H "X-Correlation-ID: demo-order-1" \
  -d '{"item":"standard meal"}'
```

The API returns HTTP `202` with a response similar to:

```json
{"order_id":1,"status":"PENDING"}
```

Query the order until the asynchronous saga finishes:

```bash
curl -i http://localhost:8000/orders/1
```

The final status will be `COMPLETED` or `CANCELLED`.

## Public API

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/place-order` | Create an order; item length must be 1-200 characters |
| `GET` | `/orders/{order_id}` | Read the current order status |
| `GET` | `/health` | Gateway liveness |
| `GET` | `/ready` | Gateway and Order API readiness |

FastAPI documentation is available at `http://localhost:8000/docs`.

## Event Contract

Domain events use this versioned envelope:

```json
{
  "event_id": "UUID",
  "event_type": "ORDER_CREATED",
  "schema_version": 1,
  "occurred_at": "ISO-8601 timestamp",
  "correlation_id": "request correlation ID",
  "causation_id": null,
  "payload": {
    "order_id": 1,
    "item": "standard meal"
  }
}
```

Payment events set `causation_id` to the source order event ID. Structurally
invalid event objects are wrapped as `EVENT_REJECTED` and sent to a DLQ before
the source offset is committed.

## Observability

Follow the complete saga using structured logs:

```bash
docker compose logs -f api-gateway order-service order-outbox-worker order-status-worker payment-service delivery-service
```

Order Service exposes these internal endpoints:

- `/health`: verifies PostgreSQL connectivity.
- `/metrics`: exposes Prometheus metrics, including created order count.

Use the same `X-Correlation-ID` value to find one request across services.

## Tests and Validation

Run event contract tests locally:

```bash
python -m unittest discover -s tests -v
```

Validate the Compose configuration:

```bash
docker compose config --quiet
```

## Configuration

| Variable | Used by | Default |
| --- | --- | --- |
| `ORDER_SERVICE_URL` | API Gateway | `http://order-service:8000` |
| `UPSTREAM_TIMEOUT_SECONDS` | API Gateway | `5` |
| `DATABASE_URL` | Order processes | PostgreSQL Compose URL |
| `KAFKA_BOOTSTRAP_SERVERS` | Producers and consumers | `kafka:9092` |
| `KAFKA_CONNECT_ATTEMPTS` | Shared Kafka producer | `5` |
| `REDIS_URL` | Payment worker | `redis://redis:6379/0` |

The credentials and infrastructure ports in `docker-compose.yml` are local
development defaults. Use managed secrets and private networks outside a POC.

## Common Operations

Rebuild the Order processes after changing Order Service code:

```bash
docker compose up -d --no-deps --build order-service order-outbox-worker order-status-worker
```

Stop services while preserving data:

```bash
docker compose down
```

Delete all local PostgreSQL, Redis, and Kafka data:

```bash
docker compose down -v
```

The last command is destructive and is useful when resetting old POC event or
database state after a contract change.

## Troubleshooting

### Docker cannot connect to the Linux engine

Start Docker Desktop and confirm it is using Linux containers, then run:

```bash
docker info
docker compose up --build
```

### An order remains `PENDING`

Inspect the outbox and consumer logs:

```bash
docker compose logs order-outbox-worker payment-service order-status-worker
```

Confirm Kafka and Redis are running with `docker compose ps`.

### A consumer repeatedly rejects an event

Check `orders-dlq` or `payments-dlq`. Malformed JSON that fails before envelope
validation is a known limitation and still requires operational handling.

## Known Limitations

- Payment state is a Redis decision cache, not an authoritative payment ledger.
- Delivery assignment is logged but not persisted.
- Published outbox rows do not yet have an archival or retention job.
- Retry topics, bounded exponential backoff, and DLQ replay tooling are absent.
- Database schema creation uses SQLAlchemy `create_all` instead of migrations.
- Authentication, authorization, rate limiting, tracing, and dashboards are not
  implemented.
- Kafka, Redis, and PostgreSQL expose development ports and credentials.

See [Complete-POC-Doc.md](Complete-POC-Doc.md) for the architecture review,
trade-offs, and prioritized production roadmap.
