from rabbitmq import consume_queues, publish_default, publish_fanout, consume_fanout
import models, database

# QUEUE_UPDATE = "order.update"
# QUEUE_PROCESS = "payment.process"

EXCHANGE_ORDER = "EX_ORDER"
EXCHANGE_PRODUCT = "EX_PRODUCT"
EXCHANGE_USER = "EX_USER"

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
        
    publish_fanout(EXCHANGE_USER, message)  
    
    db.close()
    
def start_listener():
    # Default
    # queue_callbacks = {
    #     QUEUE_PROCESS: process_payment,
    # }
    # consume_queues(queue_callbacks.keys(), queue_callbacks)
    
    # Fanout
    exchange_callbacks = {
        EXCHANGE_PRODUCT: process_payment
    }
    consume_fanout(exchange_callbacks)