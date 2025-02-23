from fastapi import FastAPI
import models
from database import engine
import routes
import threading
from topiclistener import start_listener

app = FastAPI()

@app.on_event("startup")
def startup_event():
    threading.Thread(target=start_listener, daemon=True).start()

models.Base.metadata.create_all(bind=engine)
app.include_router(routes.router)