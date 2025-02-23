from rabbitmq import consume_queues
import models, database
import threading

QUEUE_UPDATE = "order.update"
QUEUE_UPDATE_SUCCESS = "order.update.success"

def update_order_status(data):
    print("UPDATE ORDER STATUS")
    print(data)
    db = database.SessionLocal()
    
    order = db.query(models.Order).filter(models.Order.id == data["order_id"]).first()

    if order:
        order.status = data["status"]
        db.commit()
    db.close()

import threading

def start_listener():
    # consume_queues("ORDER_UPDATE", update_order_status)
    
    # thread = threading.Thread(target=consume_queues, args=(QUEUE_UPDATE, update_order_status), daemon=True)
    # thread.start()
    # thread.join()
    
    queue_callbacks = {
        QUEUE_UPDATE_SUCCESS: update_order_status
    }
    consume_queues(queue_callbacks.keys(), queue_callbacks)
