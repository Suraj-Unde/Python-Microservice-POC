FROM python:3.10

WORKDIR /app

COPY delivery_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY delivery_service .
COPY common ./common

CMD ["python", "consumer.py"]