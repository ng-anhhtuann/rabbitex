from kafkonfig import consume_queues
import models, database
import threading

TOPICUPDATE = "ORDER_UPDATE"
def update_order_status(data):
    db = database.SessionLocal()
    order = db.query(models.Order).filter(models.Order.id == data["order_id"]).first()
    print(data)
    if order:
        order.status = data["status"]
        db.commit()
    db.close()


def start_listener():
    # consume_queues("ORDER_UPDATE", update_order_status)
    thread = threading.Thread(target=consume_queues, args=(TOPICUPDATE, update_order_status), daemon=True)
    
    thread.start()
    
    # thread.join()
