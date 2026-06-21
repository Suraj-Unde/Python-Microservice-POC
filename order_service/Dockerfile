FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

WORKDIR /app

COPY order_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY order_service order_service
COPY common ./common

RUN useradd --create-home appuser && chown -R appuser:appuser /app
USER appuser

CMD ["uvicorn", "order_service.app:app", "--host", "0.0.0.0", "--port", "8000"]
