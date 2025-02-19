from sqlalchemy.orm import Session
import models, schemas

def get_orders(db: Session, skip: int = 0, limit: int = 10):
    return db.query(models.Order).offset(skip).limit(limit).all()

def get_order(db: Session, order_id: int):
    return db.query(models.Order).filter(models.Order.id == order_id).first()

def create_order(db: Session, order: schemas.OrderCreate):
    db_order = models.Order(product_name=order.product_name, quantity=order.quantity)
    db.add(db_order)
    db.commit()
    db.refresh(db_order)
    return db_order

def delete_order(db: Session, order_id: int):
    db_order = db.query(models.Order).filter(models.Order.id == order_id).first()
    if db_order:
        db.delete(db_order)
        db.commit()
    return db_order
