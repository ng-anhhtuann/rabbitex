from fastapi import FastAPI
import models
import models
from database import engine
import routes
from mqlistener import start_listener
import threading

app = FastAPI()

@app.on_event("startup")
def startup_event():
    listener_thread = threading.Thread(target=start_listener, daemon=True)
    listener_thread.start()
    
models.Base.metadata.create_all(bind=engine)

app.include_router(routes.router)
