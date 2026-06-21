from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import requests
from common.config import ORDER_SERVICE_URL, UPSTREAM_TIMEOUT_SECONDS

app = FastAPI()


class OrderCreate(BaseModel):
    item: str = Field(min_length=1, max_length=200)

def proxy(request: Request, method: str, path: str, json=None):
    correlation_id = request.headers.get("X-Correlation-ID")
    headers = {"X-Correlation-ID": correlation_id} if correlation_id else {}
    try:
        response = requests.request(
            method,
            f"{ORDER_SERVICE_URL}{path}",
            json=json,
            headers=headers,
            timeout=UPSTREAM_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        raise HTTPException(status_code=503, detail="Order service unavailable") from exc

    response_headers = {}
    if response.headers.get("X-Correlation-ID"):
        response_headers["X-Correlation-ID"] = response.headers["X-Correlation-ID"]
    try:
        content = response.json()
    except ValueError:
        content = {"detail": "Invalid response from order service"}
    return JSONResponse(content, status_code=response.status_code, headers=response_headers)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/ready")
def ready(request: Request):
    return proxy(request, "GET", "/health")


@app.post("/place-order")
def place_order(order: OrderCreate, request: Request):
    return proxy(request, "POST", "/orders", json=order.model_dump())


@app.get("/orders/{order_id}")
def get_order(order_id: int, request: Request):
    return proxy(request, "GET", f"/orders/{order_id}")
