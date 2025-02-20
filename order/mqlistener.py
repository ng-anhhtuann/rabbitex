from rabbitmq import consume_message
from sqlalchemy.orm import Session
import models, database

def update_order_status(data):
    db = database.SessionLocal()
    order = db.query(models.Order).filter(models.Order.id == data["order_id"]).first()
    print(data)
    if order:
        order.status = data["status"]
        db.commit()
    db.close()


def start_listener():
    consume_message("order.update", update_order_status)
