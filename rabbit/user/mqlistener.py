from rabbitmq import consume_queues, publish_default
import models, database
import threading

QUEUE_UPDATE = "order.update"
QUEUE_PROCESS = "payment.process"

def process_payment(data):
    print("CHECK INSUFFICIENT BALANCE")
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
        
    publish_default(QUEUE_UPDATE, message)  
    
    db.close()
    
def start_listener():
    # consume_queues(QUEUE_PROCESS, process_payment)
    
    # thread = threading.Thread(target=consume_queues, args=(QUEUE_PROCESS, process_payment), daemon=True)
    # thread.start()    
    # thread.join()
    
    queue_callbacks = {
        QUEUE_PROCESS: process_payment,
    }
    consume_queues(queue_callbacks.keys(), queue_callbacks)