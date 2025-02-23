from kafkonfig import consume_kafka, publish_kafka
import models, database
import threading

TOPIC_ORDER_CREATE = "order.create"
TOPIC_ORDER_UPDATE = "order.update"
TOPIC_PAYMENT_PROCESS = "payment.process"

def check_stock(data):    
    print("===== CHECK AVAILABLE STOCK")
    print(data)
    db = database.SessionLocal()
    product = db.query(models.Product).filter(models.Product.id == data["product_id"]).first()

    if not product or product.stock < data["quantity"]:
        publish_kafka(TOPIC_ORDER_UPDATE, {"order_id": data["order_id"], "status": "FAILED"})
    else:
        total_price = product.price * data["quantity"]
        data["amount"] = total_price
        publish_kafka(TOPIC_PAYMENT_PROCESS, data)  
    
    db.close()

def update_stock(data):
    print("===== UPDATE STOCK DEDUCTION")
    print(data)
    db = database.SessionLocal()    
    if data["status"] == "SUCCESS":
        product = db.query(models.Product).filter(models.Product.id == data["product_id"]).first()

        if product:
            product.stock -= data["quantity"]
            db.commit()
        
    db.close()

def start_listener():
    queue_callbacks = {
        TOPIC_ORDER_CREATE: check_stock,
        TOPIC_ORDER_UPDATE: update_stock
    }
    consume_kafka(queue_callbacks.keys(), queue_callbacks)