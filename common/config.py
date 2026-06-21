import os


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:password@postgres:5432/orders"
)
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
ORDER_SERVICE_URL = os.getenv("ORDER_SERVICE_URL", "http://order-service:8000")
UPSTREAM_TIMEOUT_SECONDS = float(os.getenv("UPSTREAM_TIMEOUT_SECONDS", "5"))
KAFKA_CONNECT_ATTEMPTS = int(os.getenv("KAFKA_CONNECT_ATTEMPTS", "5"))
