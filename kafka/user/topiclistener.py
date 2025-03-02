from kafkonfig import consume_kafka, publish_kafka, consume_kafka_rpc
import models, database
import threading

# Original topics
TOPIC_ORDER_STATUS = "ORDER_CHECK"
TOPIC_BALANCE_CHECK = "BALANCE_CHECK"

# Saga orchestration topics
TOPIC_PROCESS_PAYMENT = "PROCESS_PAYMENT"
TOPIC_REFUND_PAYMENT = "REFUND_PAYMENT"
TOPIC_PAYMENT_PROCESSED = "PAYMENT_PROCESSED"
TOPIC_PAYMENT_FAILED = "PAYMENT_FAILED"
TOPIC_PAYMENT_REFUNDED = "PAYMENT_REFUNDED"

def process_payment(data):
    """Legacy method for RPC-style communication"""
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
        
    # publish_kafka(TOPIC_ORDER_STATUS, message)  
    
    db.close()

    # @RPC    
    return message

# Saga pattern handlers
def handle_process_payment(data):
    """Handle process payment command from orchestrator"""
    print("===== PROCESSING PAYMENT (SAGA)")
    print(data)
    
    saga_id = data.get("saga_id")
    order_id = data.get("order_id")
    owner_id = data.get("owner_id")
    amount = data.get("amount")
    
    db = database.SessionLocal()
    
    user = db.query(models.User).filter(models.User.id == owner_id).first()
    
    # Check if user exists and has sufficient balance
    if not user or user.balance < amount:
        # Publish payment failed event
        publish_kafka(TOPIC_PAYMENT_FAILED, {
            "saga_id": saga_id,
            "order_id": order_id,
            "owner_id": owner_id,
            "reason": "Insufficient balance or invalid user"
        })
    else:
        # Process payment
        user.balance -= amount
        db.commit()
        
        # Publish payment processed event
        publish_kafka(TOPIC_PAYMENT_PROCESSED, {
            "saga_id": saga_id,
            "order_id": order_id,
            "owner_id": owner_id,
            "amount": amount
        })
    
    db.close()

def handle_refund_payment(data):
    """Handle refund payment command from orchestrator (compensation)"""
    print("===== REFUNDING PAYMENT (SAGA COMPENSATION)")
    print(data)
    
    saga_id = data.get("saga_id")
    order_id = data.get("order_id")
    owner_id = data.get("owner_id")
    amount = data.get("amount")
    
    db = database.SessionLocal()
    
    user = db.query(models.User).filter(models.User.id == owner_id).first()
    
    if user:
        # Refund payment
        user.balance += amount
        db.commit()
    
    # Publish payment refunded event
    publish_kafka(TOPIC_PAYMENT_REFUNDED, {
        "saga_id": saga_id,
        "order_id": order_id,
        "owner_id": owner_id,
        "amount": amount
    })
    
    db.close()
    
def start_listener():
    # Legacy RPC-style listener
    # consume_kafka_rpc(TOPIC_BALANCE_CHECK, process_payment)
    
    # Saga pattern listeners
    queue_callbacks = {
        TOPIC_PROCESS_PAYMENT: handle_process_payment,
        TOPIC_REFUND_PAYMENT: handle_refund_payment
    }
    consume_kafka(queue_callbacks.keys(), queue_callbacks)