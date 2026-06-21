# Event-Driven Food Delivery POC

A small Python microservices example that demonstrates an eventually consistent
order saga with FastAPI, Kafka, PostgreSQL, Redis, and Docker Compose.

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

The first response is normally `PENDING`. Poll the second endpoint until it is
`COMPLETED` or `CANCELLED`.

Useful commands:

```bash
docker compose logs -f api-gateway order-service order-outbox-worker order-status-worker payment-service delivery-service
docker compose up -d --no-deps --build order-service order-outbox-worker order-status-worker
docker compose down
```

Order Service exposes database-aware `/health` and `/metrics` internally. The
gateway exposes liveness at `/health` and dependency readiness at `/ready`.

Run the contract tests with:

```bash
python -m unittest discover -s tests -v
```

## Configuration

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
