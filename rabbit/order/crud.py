from sqlalchemy.orm import Session
import models, schemas
from rabbitmq import publish_default, publish_fanout, publish_topic, publish_rpc

# @Default
# QUEUE_CREATE = "order.create"

# @Fanout
# EXCHANGE_ORDER = "EX_ORDER"

# @Topic
# TOPIC_ORDER = "order.create.#"

# @RPC
# QUEUE_CREATE = "order_create"

# New Orchestrator Queue
QUEUE_ORCHESTRATE_ORDER = "orchestrate_order"

def get_orders(db: Session, skip: int = 0, limit: int = 10):
    return db.query(models.Order).offset(skip).limit(limit).all()

def get_order(db: Session, order_id: int):
    return db.query(models.Order).filter(models.Order.id == order_id).first()

def create_order(db: Session, order: schemas.OrderCreate):
    db_order = models.Order(
        product_id=order.product_id, owner_id=order.owner_id,
        quantity=order.quantity, status="PROCESSING"
    )
    db.add(db_order)
    db.commit()
    db.refresh(db_order)
    
    message = {
        "order_id": db_order.id,
        "product_id": order.product_id,
        "quantity": order.quantity,
        "owner_id": order.owner_id
    }
    
    # publish_default(QUEUE_CREATE, message)
    # publish_fanout(EXCHANGE_ORDER, message)
    # publish_topic(TOPIC_ORDER, message)
    # return db_order
    
    # Old direct RPC to product service
    # response = publish_rpc(QUEUE_CREATE, message)
    
    # New RPC to orchestrator service
    response = publish_rpc(QUEUE_ORCHESTRATE_ORDER, message)
    print("DATA RESPONSE")
    print(response)

    return {
        "id": db_order.id,
        "product_id": db_order.product_id,
        "owner_id": db_order.owner_id,
        "quantity": db_order.quantity,
        "status": response.get("status") 
    }

def delete_order(db: Session, order_id: int):
    db_order = db.query(models.Order).filter(models.Order.id == order_id).first()
    if db_order:
        db.delete(db_order)
        db.commit()
    return db_order
