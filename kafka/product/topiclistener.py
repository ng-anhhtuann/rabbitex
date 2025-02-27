from kafkonfig import consume_kafka, publish_kafka, publish_kafka_rpc, consume_kafka_rpc
import models, database
import time

TOPIC_STOCK_CHECK = "STOCK_CHECK"
TOPIC_ORDER_STATUS = "ORDER_CHECK"
TOPIC_BALANCE_CHECK = "BALANCE_CHECK"

RPC_REPLY = "PRODUCT_REPLY"

def check_stock(data):    
    print("===== CHECK AVAILABLE STOCK")
    print(data)
    db = database.SessionLocal()
    product = db.query(models.Product).filter(models.Product.id == data["product_id"]).first()
    response = None
    
    # time.sleep(1)
    
    if not product or product.stock < data["quantity"] or data["quantity"] <= 0:
        # publish_kafka(TOPIC_ORDER_STATUS, {"order_id": data["order_id"], "status": "FAILED"})
        response = publish_kafka_rpc(TOPIC_ORDER_STATUS, RPC_REPLY, {"order_id": data["order_id"], "status": "FAILED"})
    else:
        total_price = product.price * data["quantity"]
        data["amount"] = total_price
        # publish_kafka(TOPIC_BALANCE_CHECK, data)
        
        # @RPC
        response_balance = publish_kafka_rpc(TOPIC_BALANCE_CHECK, RPC_REPLY, data)
        if response_balance.get("status") != "SUCCESS":
            update_message = {
                "order_id": data["order_id"],
                "status": "FAILED"
            }
            response = publish_kafka_rpc(TOPIC_ORDER_STATUS, RPC_REPLY, update_message)
            return response_balance
        else:
            product.stock -= data["quantity"]
            db.commit()
            update_message = {
                "order_id": data["order_id"],
                "status": "SUCCESS"
            }
            response = publish_kafka_rpc(TOPIC_ORDER_STATUS, RPC_REPLY, update_message)
            return response
        # End @RPC
    db.close()
    return response

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
    # queue_callbacks = {
    #     TOPIC_STOCK_CHECK: check_stock,
    #     TOPIC_ORDER_STATUS: update_stock
    # }
    # consume_kafka(queue_callbacks.keys(), queue_callbacks)
    
    consume_kafka_rpc(TOPIC_STOCK_CHECK, check_stock)