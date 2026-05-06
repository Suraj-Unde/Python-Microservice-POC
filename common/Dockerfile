FROM python:3.10

WORKDIR /app
COPY ../common ./common
COPY . .

RUN pip install fastapi uvicorn sqlalchemy psycopg2-binary kafka-python

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]