import uvicorn
from fastapi import FastAPI
import routes
import mqlistener
import database
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Saga Orchestrator", description="Orchestrates distributed transactions across microservices")
app.include_router(routes.router)

@app.on_event("startup")
def startup_db_client():
    database.init_db()
    mqlistener.start_listener_thread()

@app.get("/")
def read_root():
    return {
        "message": "Saga Orchestrator API",
        "endpoints": [
            "/sagas - List all sagas",
            "/sagas/{saga_id} - Get saga details",
            "/sagas/order/{order_id} - Get saga by order ID",
            "/orders - Create a new order and start saga"
        ]
    }

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8003))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True) 