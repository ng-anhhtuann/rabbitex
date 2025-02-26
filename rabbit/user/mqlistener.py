from rabbitmq import consume_default, publish_default, publish_fanout, consume_fanout, consume_topic, publish_topic
import models, database

# @Default
# QUEUE_UPDATE = "order.update"
# QUEUE_PROCESS = "payment.process"

# @Fanout
# EXCHANGE_ORDER = "EX_ORDER"
# EXCHANGE_PRODUCT = "EX_PRODUCT"
# EXCHANGE_USER = "EX_USER"

# @Topic
TOPIC_USER = "user.check"
TOPIC_UPDATE = "update.*"

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
        
    # publish_default(QUEUE_UPDATE, message)
    # publish_fanout(EXCHANGE_USER, message)  
    publish_topic(TOPIC_UPDATE, message)  
    
    db.close()
    
def start_listener():
    # Default
    # queue_callbacks = {
    #     QUEUE_PROCESS: process_payment,
    # }
    # consume_default(queue_callbacks.keys(), queue_callbacks)
    
    # Fanout
    # exchange_callbacks = {
    #     EXCHANGE_PRODUCT: process_payment
    # }
    # consume_fanout(exchange_callbacks)
    topic_callbacks = {
        TOPIC_USER: process_payment
    }
    consume_topic(topic_callbacks)