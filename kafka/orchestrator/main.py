from fastapi import FastAPI, HTTPException
import uvicorn
from pydantic import BaseModel
import threading
import os
import uuid
from dotenv import load_dotenv
from orchestrator import OrderSagaOrchestrator
from saga_state import saga_store
from kafka_config import publish_message, TOPIC_START_ORDER_SAGA

load_dotenv()

app = FastAPI()
orchestrator = OrderSagaOrchestrator()

class OrderRequest(BaseModel):
    product_id: int
    owner_id: int
    quantity: int

@app.on_event("startup")
def startup_event():
    """Start the orchestrator on application startup"""
    threading.Thread(target=orchestrator.start, daemon=True).start()

@app.post("/orders")
async def create_order(order: OrderRequest):
    """Create a new order and start the saga"""
    saga_id = str(uuid.uuid4())
    message = order.dict()
    message["saga_id"] = saga_id
    
    publish_message(TOPIC_START_ORDER_SAGA, message)
    print("================================================")
    return {"message": "Order processing started", "saga_id": saga_id}

@app.get("/sagas/{saga_id}")
async def get_saga(saga_id: str):
    """Get the current state of a saga"""
    if saga_id not in saga_store:
        raise HTTPException(status_code=404, detail="Saga not found")
    
    return saga_store[saga_id].to_dict()

@app.get("/sagas")
async def get_all_sagas():
    """Get all sagas"""
    return [saga.to_dict() for saga in saga_store.values()]

if __name__ == "__main__":
    port = int(os.getenv("ORCHESTRATOR_PORT", 8003))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True) 