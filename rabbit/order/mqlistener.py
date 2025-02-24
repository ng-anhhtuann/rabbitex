from rabbitmq import consume_queues, consume_fanout
import models, database

# QUEUE_UPDATE = "order.update"
# QUEUE_UPDATE_SUCCESS = "order.update.success"

EXCHANGE_ORDER = "EX_ORDER"
EXCHANGE_PRODUCT = "EX_PRODUCT"
EXCHANGE_USER = "EX_USER"

def update_order_status(data):
    print("UPDATE ORDER STATUS")
    print(data)
    db = database.SessionLocal()
    
    order = db.query(models.Order).filter(models.Order.id == data["order_id"]).first()

    if order:
        order.status = data["status"]
        db.commit()
    db.close()

def start_listener():
    # Default
    # queue_callbacks = {
    #     QUEUE_UPDATE: update_order_status
    # }
    # consume_queues(queue_callbacks.keys(), queue_callbacks)
    
    exchange_callbacks = {
        EXCHANGE_USER: update_order_status
    }
    consume_fanout(exchange_callbacks)
