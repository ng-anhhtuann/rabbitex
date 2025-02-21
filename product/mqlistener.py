from rabbitmq import consume_message, publish_message
import models, database
import threading

QUEUE_UPDATE = "order.update"
QUEUE_START = "order.created"

def check_stock(data):
    db = database.SessionLocal()
    product = db.query(models.Product).filter(models.Product.id == data["product_id"]).first()
    print("CHECK AVAILABLE STOCK")
    print(data)
    if not product or product.stock < data["quantity"]:
        publish_message("order.update", {"order_id": data["order_id"], "status": "FAILED"})
    else:
        total_price = product.price * data["quantity"]
        data["amount"] = total_price
        publish_message("user.payment", data)  
    
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
    # consume_message(QUEUE_START, check_stock)
    # consume_message(QUEUE_UPDATE, update_stock)
    thread1 = threading.Thread(target=consume_message, args=(QUEUE_START, check_stock), daemon=True)
    thread2 = threading.Thread(target=consume_message, args=(QUEUE_UPDATE, update_stock), daemon=True)

    thread1.start()
    thread2.start()

    # thread1.join()
    # thread2.join()


