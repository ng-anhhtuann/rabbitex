from sqlalchemy.orm import Session
import models, schemas
from rabbitmq import publish_default, publish_fanout, publish_topic

# @Default
# QUEUE_UPDATE = "order.create"

# @Fanout
# EXCHANGE_ORDER = "EX_ORDER"

# @Topic
TOPIC_ORDER = "order.create.#"

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
    
    # publish_default(QUEUE_UPDATE, message)
    # publish_fanout(EXCHANGE_ORDER, message)
    publish_topic(TOPIC_ORDER, message)
    
    return db_order


def delete_order(db: Session, order_id: int):
    db_order = db.query(models.Order).filter(models.Order.id == order_id).first()
    if db_order:
        db.delete(db_order)
        db.commit()
    return db_order
