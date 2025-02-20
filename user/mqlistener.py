from rabbitmq import consume_message, publish_message
from sqlalchemy.orm import Session
import models, database

QUEUE_UPDATE = "order.update"
QUEUE_PROCESS = "user.payment"

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
        
    publish_message(QUEUE_UPDATE, message)  
    
    db.close()
    
def start_listener():
    consume_message(QUEUE_PROCESS, process_payment)
