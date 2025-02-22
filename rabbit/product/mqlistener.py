from rabbitmq import consume_queues, publish_default
import models, database
import threading

QUEUE_UPDATE = "order.update"
QUEUE_START = "order.create"
QUEUE_PROCESS = "payment.process"

def check_stock(data):    
    print("CHECK AVAILABLE STOCK")
    print(data)
    db = database.SessionLocal()
    product = db.query(models.Product).filter(models.Product.id == data["product_id"]).first()

    if not product or product.stock < data["quantity"]:
        publish_default(QUEUE_UPDATE, {"order_id": data["order_id"], "status": "FAILED"})
    else:
        total_price = product.price * data["quantity"]
        data["amount"] = total_price
        publish_default(QUEUE_PROCESS, data)  
    
    db.close()

def update_stock(data):
    print("UPDATE STOCK DEDUCTION")
    print(data)
    db = database.SessionLocal()    
    if (data["status"] == "SUCCESS"):
        product = db.query(models.Product).filter(models.Product.id == data["product_id"]).first()

        if product:
            product.stock -= data["quantity"]
            db.commit()
    
    db.close()

def start_listener():
    # consume_queues(QUEUE_START, check_stock)
    # consume_queues(QUEUE_UPDATE, update_stock)
    
    # thread1 = threading.Thread(target=consume_queues, args=(QUEUE_START, check_stock), daemon=True)
    # thread2 = threading.Thread(target=consume_queues, args=(QUEUE_UPDATE, update_stock), daemon=True)
    # thread1.start()
    # thread2.start()
    # thread1.join()
    # thread2.join()
    
    queue_callbacks = {
        QUEUE_START: check_stock,
        QUEUE_UPDATE: update_stock
    }
    consume_queues(queue_callbacks.keys(), queue_callbacks)


