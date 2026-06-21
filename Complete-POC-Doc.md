# Architecture Review and Improvement Record

## Scope

This repository is an educational, choreography-based saga. It demonstrates
service boundaries and asynchronous state transitions; it does not claim the
operational guarantees of a production payment platform.

## Components and Ownership

| Component | Responsibility | Durable state |
| --- | --- | --- |
| API Gateway | Public routing, timeout, correlation propagation | None |
| Order API | Atomically create orders and outbox rows | PostgreSQL |
| Outbox worker | Relay committed order events to Kafka | PostgreSQL |
| Order status worker | Apply payment outcomes to orders | PostgreSQL |
| Payment worker | Simulate a stable payment result | Redis decision cache |
| Delivery worker | React to successful payment | None (logs only) |
| Kafka | Transport order and payment events | Kafka log |

The Order API, outbox worker, and status worker deploy separately but form one
service boundary. No other service accesses the orders database.

## Improvements Applied

### Complete saga compensation

Previously, payment failure was only logged by Delivery, so orders remained
`PENDING`. The new Order status worker consumes payment events and moves orders
to `COMPLETED` or `CANCELLED`. Its `processed_events` table prevents duplicate
events from applying twice.

### Safer event consumption

Consumers now use manual offset commits. Payment commits after its result is
acknowledged by Kafka; the Order worker commits after its database transaction;
Delivery commits after handling. This gives at-least-once processing rather than
silently acknowledging work before it finishes.

Payment decisions are stored in Redis under the source event ID. If a crash
occurs after publishing but before committing the Kafka offset, the retry emits
the same payment event and outcome. Each derived event has a new `event_id` and
uses `causation_id` to point to its source.

### Transactional outbox

Order creation and `ORDER_CREATED` persistence now happen in one PostgreSQL
transaction. A separate relay locks pending rows with `SKIP LOCKED`, publishes
them, and records `published_at`. A crash after Kafka acknowledgement but before
the database update can still duplicate an event, so consumers remain
idempotent.

### Versioned contracts and rejection

All domain events share a validated version 1 envelope. Structurally invalid
events are wrapped as `EVENT_REJECTED` and sent to `orders-dlq` or
`payments-dlq`; source offsets commit only after the dead-letter publish is
acknowledged. Contract unit tests cover creation, versions, required fields, and
rejection context.

### Gateway resilience

The gateway now:

- bounds calls to the Order API with a timeout;
- maps connection failures to HTTP `503`;
- preserves upstream status codes and correlation IDs;
- exposes order lookup and a health endpoint.

### Configuration and deployment

Service URLs, Kafka brokers, Redis, database URL, and timeout are environment
configured. PostgreSQL readiness gates the Order processes. Fixed container
names were removed so Compose services can be scaled without name collisions.
Application images run as a non-root user.

## Important Remaining Risks

### 1. Make payment state authoritative

Redis currently provides a durable-enough decision cache for the demo, but it is
not a payment ledger. A real Payment Service needs its own database with a unique
idempotency key, provider transaction ID, amount/currency, explicit state
machine, audit history, and reconciliation process.

### 2. Persist delivery work

Delivery assignment is currently only a log statement. Add a Delivery-owned
database and idempotency table before integrating an external courier provider.

### 3. Complete dead-letter operations

Contract-invalid JSON objects go to dead-letter topics, but malformed JSON can
still fail during deserialization and transient failures retry without bounded
backoff. Add byte-level decode handling, retry topics, DLQ alerts, retention
policy, and an audited replay tool.

Published outbox rows also need a retention or archival job so the table does
not grow without bound.

### 4. Govern contract evolution

The code now validates a shared envelope. Move the contract to JSON Schema or
Avro and enforce backward/forward compatibility in CI before adding version 2.

### 5. Improve operations and security

- Replace development credentials with managed secrets.
- Keep Kafka, Redis, and PostgreSQL off public host ports.
- Add authentication, authorization, rate limiting, and request-size limits.
- Add readiness probes for dependencies and graceful consumer shutdown.
- Export metrics from every process and adopt OpenTelemetry traces.
- Pin images by digest, scan dependencies, and use read-only root filesystems.
- Add database migrations (Alembic); do not use `create_all` for production.

## Recommended Delivery Order

1. Migration tooling, especially before changing existing tables.
2. Payment ledger and provider idempotency.
3. Retry topics, malformed-message handling, and DLQ operations.
4. Delivery persistence.
5. Authentication, secrets, tracing, dashboards, and deployment manifests.

This order tackles data loss and duplicate financial effects before adding more
platform machinery.
