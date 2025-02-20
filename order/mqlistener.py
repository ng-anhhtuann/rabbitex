from rabbitmq import consume_message
from sqlalchemy.orm import Session
import models, database

QUEUE_UPDATE = "order.update"

def update_order_status(data):
    
    db = database.SessionLocal()
    order = db.query(models.Order).filter(models.Order.id == data["order_id"]).first()
    
    if order:
        order.status = data["status"]
        db.commit()
    db.close()

import threading

def start_listener():
    thread = threading.Thread(target=consume_message, args=(QUEUE_UPDATE, update_order_status))
    thread.daemon = True
    thread.start()
