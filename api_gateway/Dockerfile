FROM python:3.10-slim

WORKDIR /app

COPY api_gateway/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api_gateway .
COPY common ./common


CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]