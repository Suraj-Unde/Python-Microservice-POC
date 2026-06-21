FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

WORKDIR /app

COPY payment_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY payment_service .
COPY common ./common

RUN useradd --create-home appuser && chown -R appuser:appuser /app
USER appuser

CMD ["python", "consumer.py"]
