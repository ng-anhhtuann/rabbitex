from kafkonfig import consume_kafka, consume_kafka_rpc, publish_kafka
import models, database
import threading

# Original topics
TOPIC_ORDER_STATUS = "ORDER_CHECK"

# Saga orchestration topics
TOPIC_CREATE_ORDER = "CREATE_ORDER"
TOPIC_COMPLETE_ORDER = "COMPLETE_ORDER"
TOPIC_CANCEL_ORDER = "CANCEL_ORDER"
TOPIC_ORDER_CREATED = "ORDER_CREATED"
TOPIC_ORDER_COMPLETED = "ORDER_COMPLETED"
TOPIC_ORDER_CANCELLED = "ORDER_CANCELLED"

def update_order_status(data):
    """Legacy method for RPC-style communication"""
    print("===== UPDATE ORDER STATUS")
    print(data)
    db = database.SessionLocal()
    
    order = db.query(models.Order).filter(models.Order.id == data["order_id"]).first()

    if order:
        order.status = data["status"]
        db.commit()
        
        # @RPC
        db.refresh(order)

        order_data = {
            "id": data["order_id"],
            "owner_id": order.owner_id,
            "product_id": order.product_id,
            "quantity": order.quantity,
            "status": order.status
        }
    else:
        order_data = data
        # End @RPC
        
    db.close()
    return order_data

# Saga pattern handlers
def handle_create_order(data):
    """Handle create order command from orchestrator"""
    print("===== CREATING ORDER (SAGA)")
    print(data)
    
    saga_id = data.get("saga_id")
    product_id = data.get("product_id")
    owner_id = data.get("owner_id")
    quantity = data.get("quantity")
    
    db = database.SessionLocal()
    
    # Create new order
    db_order = models.Order(
        product_id=product_id,
        owner_id=owner_id,
        quantity=quantity,
        status="PROCESSING"
    )
    db.add(db_order)
    db.commit()
    db.refresh(db_order)
    
    publish_kafka(TOPIC_ORDER_CREATED, {
        "saga_id": saga_id,
        "order_id": db_order.id,
        "product_id": product_id,
        "owner_id": owner_id,
        "quantity": quantity,
        "status": "PROCESSING"
    })
    
    db.close()

def handle_complete_order(data):
    """Handle complete order command from orchestrator"""
    print("===== COMPLETING ORDER (SAGA)")
    print(data)
    
    saga_id = data.get("saga_id")
    order_id = data.get("order_id")
    status = data.get("status")
    
    db = database.SessionLocal()
    
    order = db.query(models.Order).filter(models.Order.id == order_id).first()
    
    if order:
        order.status = status
        db.commit()
        
        publish_kafka(TOPIC_ORDER_COMPLETED, {
            "saga_id": saga_id,
            "order_id": order_id,
            "status": status
        })
    
    db.close()

def handle_cancel_order(data):
    """Handle cancel order command from orchestrator (compensation)"""
    print("===== CANCELLING ORDER (SAGA COMPENSATION)")
    print(data)
    
    saga_id = data.get("saga_id")
    order_id = data.get("order_id")
    
    db = database.SessionLocal()
    
    order = db.query(models.Order).filter(models.Order.id == order_id).first()
    
    if order:
        order.status = "CANCELLED"
        db.commit()
        
        publish_kafka(TOPIC_ORDER_CANCELLED, {
            "saga_id": saga_id,
            "order_id": order_id,
            "status": "CANCELLED"
        })
    
    db.close()

def start_listener():
    # Legacy RPC-style listener
    # consume_kafka_rpc(TOPIC_ORDER_STATUS, update_order_status)
    
    # Saga pattern listeners
    queue_callbacks = {
        TOPIC_CREATE_ORDER: handle_create_order,
        TOPIC_COMPLETE_ORDER: handle_complete_order,
        TOPIC_CANCEL_ORDER: handle_cancel_order
    }
    consume_kafka(queue_callbacks.keys(), queue_callbacks)
