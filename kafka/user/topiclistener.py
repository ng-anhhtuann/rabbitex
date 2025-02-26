from kafkonfig import consume_kafka, publish_kafka
import models, database
import threading

TOPIC_ORDER_STATUS = "ORDER_CHECK"
TOPIC_BALANCE_CHECK = "BALANCE_CHECK"

def process_payment(data):
    print("===== CHECK INSUFFICIENT BALANCE")
    print(data)
    db = database.SessionLocal()
    user = db.query(models.User).filter(models.User.id == data["owner_id"]).first()

    message = {
        "order_id": data["order_id"],
        "product_id": data.get("product_id"),  
        "quantity": data.get("quantity")       
    }

    if not user or user.balance < data["amount"]:
        message["status"] = "FAILED"
    else:
        user.balance -= data["amount"]
        db.commit()
        
        message["status"] = "SUCCESS"
        message["product_id"] = data["product_id"]
        
    publish_kafka(TOPIC_ORDER_STATUS, message)  
    
    db.close()
    
def start_listener():
    queue_callbacks = {
        TOPIC_BALANCE_CHECK: process_payment
    }
    consume_kafka(queue_callbacks.keys(), queue_callbacks)