from kafkonfig import consume_kafka, publish_kafka
import models, database
import threading

TOPIC_STOCK_CHECK = "STOCK_CHECK"
TOPIC_ORDER_STATUS = "ORDER_CHECK"
TOPIC_BALANCE_CHECK = "BALANCE_CHECK"

def check_stock(data):    
    print("===== CHECK AVAILABLE STOCK")
    print(data)
    db = database.SessionLocal()
    product = db.query(models.Product).filter(models.Product.id == data["product_id"]).first()

    if not product or product.stock < data["quantity"]:
        publish_kafka(TOPIC_ORDER_STATUS, {"order_id": data["order_id"], "status": "FAILED"})
    else:
        total_price = product.price * data["quantity"]
        data["amount"] = total_price
        publish_kafka(TOPIC_BALANCE_CHECK, data)  
    
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
        TOPIC_STOCK_CHECK: check_stock,
        TOPIC_ORDER_STATUS: update_stock
    }
    consume_kafka(queue_callbacks.keys(), queue_callbacks)