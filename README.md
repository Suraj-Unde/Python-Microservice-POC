# 🚀 Microservices Food Delivery System (Python)

A production-style event-driven microservices system built in Python to demonstrate:

- Microservices architecture
- Event-driven communication using Kafka
- Saga pattern for distributed transactions
- Failure handling and compensation
- Observability fundamentals

## 🧠 Why this project?

Most tutorials explain microservices in isolation.
This project answers the question:

> “What actually happens when things fail in a distributed system?”

Using a real-world food delivery scenario, it demonstrates:

- Order creation
- Payment processing
- Delivery assignment
- Failure recovery using Saga

## 🏗️ Architecture Overview

Key principles:

- Each service owns its logic and database
- Services communicate via Kafka events
- The system is eventually consistent

## 🔄 End-to-End Flow

1. Client places an order
2. Order Service creates the order with status `PENDING`
3. Order Service publishes an event to Kafka
4. Payment Service consumes the event and processes payment
5. Delivery Service reacts to payment success and assigns delivery

## 🔁 Saga Pattern

### ✅ Happy Path

- Order → Payment → Delivery
- Everything succeeds

### ❌ Failure Handling

- Payment fails
- Order is cancelled via compensation
- Delivery is skipped

## ⚙️ Tech Stack

- Python (FastAPI)
- Kafka (event streaming)
- PostgreSQL (per-service DB)
- Redis (idempotency - optional)
- Docker & Docker Compose

## 📦 Services

| Service          | Responsibility                          |
| ---------------- | ----------------------------------------|
| API Gateway      | Entry point for clients                  |
| Order Service    | Creates orders and starts the Saga       |
| Payment Service  | Processes payment events                 |
| Delivery Service | Assigns delivery after payment success   |

## ▶️ How to Run

1. Clone the repository

```bash
git clone <your-repo-url>
cd Microservice-POC
```

2. Start the system

```bash
docker-compose up --build
```

3. Trigger the flow

```bash
curl -X POST http://localhost:8000/place-order
```

## 🔍 What to Observe

- Order created by the Order Service
- Payment randomly succeeds or fails
- Delivery assigned on payment success
- Order cancelled when payment fails

## 🧪 Failure Simulation

The Payment Service randomly returns:

- ✅ SUCCESS
- ❌ FAILURE

This demonstrates:

- Real-world unpredictability
- System resilience

## 🔎 Observability (Basic)

- Service logs across components
- Event tracing through the flow

Potential extensions:

- Prometheus
- Grafana
- OpenTelemetry

## 🚀 Future Improvements

- Add OpenTelemetry tracing
- Add JWT authentication
- Add Kubernetes deployment
- Add retry logic and DLQ (Dead Letter Queue)
- Add a UI dashboard

## 🎯 What this project demonstrates

- Real-world distributed system behavior
- Event-driven design
- Failure handling using Saga
- Production thinking, not just code
