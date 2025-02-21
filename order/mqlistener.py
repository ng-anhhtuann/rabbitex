from rabbitmq import consume_message
import models, database
import threading

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
    # consume_message("order.update", update_order_status)
    thread = threading.Thread(target=consume_message, args=(QUEUE_UPDATE, update_order_status), daemon=True)
    
    thread.start()
    
    # thread.join()
