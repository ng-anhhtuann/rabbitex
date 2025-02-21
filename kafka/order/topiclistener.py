from kafkonfig import consume_message
import models, database
import threading

TOPICUPDATE = "order.update"
def update_order_status(data):
    db = database.SessionLocal()
    order = db.query(models.Order).filter(models.Order.id == data["order_id"]).first()
    print(data)
    if order:
        order.status = data["status"]
        db.commit()
    db.close()


def start_listener():
    # consume_message("order.update", update_order_status)
    thread = threading.Thread(target=consume_message, args=(TOPICUPDATE, update_order_status), daemon=True)
    
    thread.start()
    
    # thread.join()
