from kafkonfig import consume_message, publish_message
import models, database
import threading

TOPICUPDATE = "order.update"
TOPIC_PROCESS = "user.payment"

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
        
    publish_message(TOPICUPDATE, message)  
    
    db.close()
    
def start_listener():
    # consume_message(TOPIC_PROCESS, process_payment)
    thread = threading.Thread(target=consume_message, args=(TOPIC_PROCESS, process_payment), daemon=True)
    
    thread.start()
    
    # thread.join()