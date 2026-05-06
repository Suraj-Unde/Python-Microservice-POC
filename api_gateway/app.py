from fastapi import FastAPI
import requests

app = FastAPI()

@app.post("/place-order")
def place_order():
    res = requests.post(
        "http://order-service:8000/orders",
        json={"item": "standard meal"}
    )
    return res.json()