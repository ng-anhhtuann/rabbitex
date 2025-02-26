from kafkonfig import consume_kafka
import models, database
import threading

TOPIC_ORDER_STATUS = "ORDER_CHECK"

def update_order_status(data):
    print("===== UPDATE ORDER STATUS")
    print(data)
    db = database.SessionLocal()
    
    order = db.query(models.Order).filter(models.Order.id == data["order_id"]).first()

    if order:
        order.status = data["status"]
        db.commit()
    db.close()

import threading

def start_listener():
    queue_callbacks = {
        TOPIC_ORDER_STATUS: update_order_status
    }
    consume_kafka(queue_callbacks.keys(), queue_callbacks)
