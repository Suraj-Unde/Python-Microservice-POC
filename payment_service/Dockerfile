FROM python:3.10-slim

WORKDIR /app

COPY payment_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY payment_service .
COPY common ./common

CMD ["python", "consumer.py"]