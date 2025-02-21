from fastapi import FastAPI
import requests

app = FastAPI()

USER_SERVICE_URL = "http://localhost:8001/users"
PRODUCT_SERVICE_URL = "http://localhost:8002/products"
ORDER_SERVICE_URL = "http://localhost:8003/orders"

@app.post("/create_order/")
def create_order(user_id: int, product_id: int):
    user = requests.get(f"{USER_SERVICE_URL}/{user_id}").json()
    product = requests.get(f"{PRODUCT_SERVICE_URL}/{product_id}").json()

    if user and product:
        response = requests.post(ORDER_SERVICE_URL, json={"user_id": user_id, "product_id": product_id})
        return response.json()
    return {"error": "User or Product not found"}