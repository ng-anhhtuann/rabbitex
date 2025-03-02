from kafkonfig import consume_kafka, publish_kafka, publish_kafka_rpc, consume_kafka_rpc
import models, database
import time

# Original topics
TOPIC_STOCK_CHECK = "STOCK_CHECK"
TOPIC_ORDER_STATUS = "ORDER_CHECK"
TOPIC_BALANCE_CHECK = "BALANCE_CHECK"
RPC_REPLY = "PRODUCT_REPLY"

# Saga orchestration topics
TOPIC_RESERVE_PRODUCT = "RESERVE_PRODUCT"
TOPIC_RELEASE_PRODUCT = "RELEASE_PRODUCT"
TOPIC_PRODUCT_RESERVED = "PRODUCT_RESERVED"
TOPIC_PRODUCT_RESERVATION_FAILED = "PRODUCT_RESERVATION_FAILED"
TOPIC_PRODUCT_RELEASED = "PRODUCT_RELEASED"
TOPIC_CONFIRM_PRODUCT_DEDUCTION = "CONFIRM_PRODUCT_DEDUCTION"

def check_stock(data):    
    """Legacy method for RPC-style communication"""
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
    """Legacy method for updating stock"""
    print("===== UPDATE STOCK DEDUCTION")
    print(data)
    db = database.SessionLocal()    
    if data["status"] == "SUCCESS":
        product = db.query(models.Product).filter(models.Product.id == data["product_id"]).first()

        if product:
            product.stock -= data["quantity"]
            db.commit()
        
    db.close()

# Saga pattern handlers
def handle_reserve_product(data):
    """Handle reserve product command from orchestrator"""
    print("===== RESERVING PRODUCT (SAGA)")
    print(data)
    
    saga_id = data.get("saga_id")
    order_id = data.get("order_id")
    product_id = data.get("product_id")
    quantity = data.get("quantity")
    
    db = database.SessionLocal()
    
    product = db.query(models.Product).filter(models.Product.id == product_id).first()
    
    if not product or product.stock < quantity or quantity <= 0:
        publish_kafka(TOPIC_PRODUCT_RESERVATION_FAILED, {
            "saga_id": saga_id,
            "order_id": order_id,
            "product_id": product_id,
            "reason": "Insufficient stock or invalid product"
        })
    else:
        total_price = product.price * quantity
        
        publish_kafka(TOPIC_PRODUCT_RESERVED, {
            "saga_id": saga_id,
            "order_id": order_id,
            "product_id": product_id,
            "quantity": quantity,
            "amount": total_price
        })
    
    db.close()

def handle_release_product(data):
    """Handle release product command from orchestrator (compensation)"""
    print("===== RELEASING PRODUCT (SAGA COMPENSATION)")
    print(data)
    
    saga_id = data.get("saga_id")
    order_id = data.get("order_id")
    product_id = data.get("product_id")
    quantity = data.get("quantity")
    
    publish_kafka(TOPIC_PRODUCT_RELEASED, {
        "saga_id": saga_id,
        "order_id": order_id,
        "product_id": product_id,
        "quantity": quantity
    })

def handle_confirm_product_deduction(data):
    """Handle confirm product deduction command from orchestrator"""
    print("===== CONFIRMING PRODUCT DEDUCTION (SAGA)")
    print(data)
    
    saga_id = data.get("saga_id")
    order_id = data.get("order_id")
    product_id = data.get("product_id")
    quantity = data.get("quantity")
    
    db = database.SessionLocal()
    
    product = db.query(models.Product).filter(models.Product.id == product_id).first()
    
    if product:
        # Actually deduct the stock now that payment is confirmed
        product.stock -= quantity
        db.commit()
        print(f"Stock deducted: Product {product_id} new stock: {product.stock}")
    else:
        print(f"Warning: Product {product_id} not found for stock deduction")
    
    db.close()

def start_listener():
    # Legacy RPC-style listener
    # consume_kafka_rpc(TOPIC_STOCK_CHECK, check_stock)
    
    # Saga pattern listeners
    queue_callbacks = {
        TOPIC_RESERVE_PRODUCT: handle_reserve_product,
        TOPIC_RELEASE_PRODUCT: handle_release_product,
        TOPIC_CONFIRM_PRODUCT_DEDUCTION: handle_confirm_product_deduction
    }
    consume_kafka(queue_callbacks.keys(), queue_callbacks)