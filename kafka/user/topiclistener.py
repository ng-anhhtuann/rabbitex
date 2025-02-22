from kafkonfig import consume_queues, publish_default
import models, database
import threading

TOPICUPDATE = "ORDER_UPDATE"
TOPIC_PROCESS = "PAYMENT_PROCESS"

def process_payment(data):
    db = database.SessionLocal()
    user = db.query(models.User).filter(models.User.id == data["owner_id"]).first()
    print("CHECK INSUFFICIENT BALANCE")
    print(data)
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
        
    publish_default(TOPICUPDATE, message)  
    
    db.close()
    
def start_listener():
    # consume_queues(TOPIC_PROCESS, process_payment)
    thread = threading.Thread(target=consume_queues, args=(TOPIC_PROCESS, process_payment), daemon=True)
    
    thread.start()
    
    # thread.join()