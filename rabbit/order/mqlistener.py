from rabbitmq import consume_default, consume_fanout, consume_topic
import models, database

# @Default
# QUEUE_UPDATE = "order.update"
# QUEUE_UPDATE_SUCCESS = "order.update.success"

# @Fanout
# EXCHANGE_USER = "EX_USER"

TOPIC_UPDATE = "update.status"

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
    # consume_default(queue_callbacks.keys(), queue_callbacks)
    
    # exchange_callbacks = {
    #     EXCHANGE_USER: update_order_status
    # }
    # consume_fanout(exchange_callbacks)
    
    topic_callbacks = {
        TOPIC_UPDATE: update_order_status
    }
    consume_topic(topic_callbacks)
