from sqlalchemy.orm import Session
import models, schemas
from kafkonfig import publish_kafka, publish_kafka_rpc, publish_kafka_sync

TOPIC_STOCK_CHECK = "STOCK_CHECK"
RPC_REPLY = "ORDER_RPC_REPLY"

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
    # publish_kafka(TOPIC_STOCK_CHECK, message)
    # return db_order
    
    # @RPC
    response = publish_kafka_rpc(TOPIC_STOCK_CHECK, RPC_REPLY, message)
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
