from kafkonfig import consume_queues, publish_default
import models, database
import threading

TOPICUPDATE = "ORDER_UPDATE"
TOPICSTART = "ORDER_CREATE"

def check_stock(data):
    db = database.SessionLocal()
    product = db.query(models.Product).filter(models.Product.id == data["product_id"]).first()
    print("CHECK AVAILABLE STOCK")
    print(data)
    if not product or product.stock < data["quantity"]:
        publish_default("ORDER_UPDATE", {"order_id": data["order_id"], "status": "FAILED"})
    else:
        total_price = product.price * data["quantity"]
        data["amount"] = total_price
        publish_default("PAYMENT_PROCESS", data)  
    
    db.close()

def update_stock(data):
    db = database.SessionLocal()    
    print("UPDATE STOCK DEDUCTION")
    print(data)
    product = db.query(models.Product).filter(models.Product.id == data["product_id"]).first()

    if product:
        product.stock -= data["quantity"]
        db.commit()
    
    db.close()


def start_listener():
    # consume_queues(TOPICSTART, check_stock)
    # consume_queues(TOPICUPDATE, update_stock)
    thread1 = threading.Thread(target=consume_queues, args=(TOPICSTART, check_stock), daemon=True)
    thread2 = threading.Thread(target=consume_queues, args=(TOPICUPDATE, update_stock), daemon=True)

    thread1.start()
    thread2.start()

    # thread1.join()
    # thread2.join()


