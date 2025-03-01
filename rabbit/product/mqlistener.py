from rabbitmq import consume_default, publish_default, publish_fanout, consume_fanout, consume_topic, publish_topic, publish_rpc, consume_rpc
import models, database
import time

# @Default
# QUEUE_UPDATE = "order.update"
# QUEUE_START = "order.create"
# QUEUE_PROCESS = "payment.process"
# QUEUE_UPDATE_SUCCESS = "order.update.success"

# @Fanout
# EXCHANGE_ORDER = "EX_ORDER"
# EXCHANGE_PRODUCT = "EX_PRODUCT"
# EXCHANGE_USER = "EX_USER"

# @Topic
# TOPIC_ORDER = "order.create"
# TOPIC_USER = "user.check"
# TOPIC_UPDATE_STOCK = "update.stock"
# TOPIC_UPDATE = "update.*"

# @RPC
QUEUE_CREATE = "order_create"
QUEUE_STOCK_UPDATE = "product_stock_update"
QUEUE_ORDER_STATUS_UPDATE = "order_status_update"
QUEUE_BALANCE = "user_check"

def check_stock(data):    
    print("CHECK AVAILABLE STOCK")
    print(data)
    db = database.SessionLocal()
    product = db.query(models.Product).filter(models.Product.id == data["product_id"]).first()
    response = None
    
    # time.sleep(2)
    
    if not product or product.stock < data["quantity"] or data["quantity"] <= 0:
        # publish_default(QUEUE_UPDATE, {"order_id": data["order_id"], "status": "FAILED"})
        # publish_fanout(EXCHANGE_USER, {"order_id": data["order_id"], "status": "FAILED"})
        # publish_topic(QUEUE_UPDATE, {"order_id": data["order_id"], "status": "FAILED"})
        
        # @RPC
        response = publish_rpc(QUEUE_ORDER_STATUS_UPDATE, {
            "order_id": data["order_id"],
            "status": "FAILED"
        })
        # End @RPC
    
    else:
        total_price = product.price * data["quantity"]
        data["amount"] = total_price
        # publish_default(QUEUE_PROCESS, data)  
        # publish_fanout(EXCHANGE_PRODUCT, data)  
        # publish_topic(TOPIC_USER, data)  
        
        # @RPC
        response_balance = publish_rpc(QUEUE_BALANCE, data)
        if response_balance.get("status") != "SUCCESS":
            update_message = {
                "order_id": data["order_id"],
                "status": "FAILED"
            }
            response = publish_rpc(QUEUE_ORDER_STATUS_UPDATE, update_message)
            return response_balance
        else:
            product.stock -= data["quantity"]
            db.commit()
            update_message = {
                "order_id": data["order_id"],
                "status": "SUCCESS"
            }
            response = publish_rpc(QUEUE_ORDER_STATUS_UPDATE, update_message)
            return response
        # End @RPC
    db.close()
    return response

def update_stock(data):
    print("UPDATE STOCK DEDUCTION")
    print(data)
    db = database.SessionLocal()    
    if (data["status"] == "SUCCESS"):
        product = db.query(models.Product).filter(models.Product.id == data["product_id"]).first()

        if product:
            product.stock -= data["quantity"]
            db.commit()
    
        # publish_default(QUEUE_UPDATE_SUCCESS, data)  
    db.close()
    
def start_listener():
    # Default
    # queue_callbacks = {
    #     QUEUE_START: check_stock,
    #     QUEUE_UPDATE: update_stock
    # }
    # consume_default(queue_callbacks.keys(), queue_callbacks)
    
    # Fanout
    # exchange_callbacks = {
    #     EXCHANGE_ORDER: check_stock,
    #     EXCHANGE_USER: update_stock
    # }
    # consume_fanout(exchange_callbacks)
    
    # Topic
    # topic_callbacks = {
    #     TOPIC_ORDER: check_stock,
    #     TOPIC_UPDATE_STOCK: update_stock
    # }
    # consume_topic(topic_callbacks)
    
    # RPC
    rpc_callbacks = {
        QUEUE_CREATE: check_stock
    }
    consume_rpc(rpc_callbacks)