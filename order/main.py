from fastapi import FastAPI
import models
from database import engine
import routes
from mqlistener import start_listener
import threading

app = FastAPI()

@app.on_event("startup")
def startup_event():
    start_listener()
    
models.Base.metadata.create_all(bind=engine)

app.include_router(routes.router)
