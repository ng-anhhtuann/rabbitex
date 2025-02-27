from kafkonfig import consume_kafka, consume_kafka_rpc
import models, database
import threading

TOPIC_ORDER_STATUS = "ORDER_CHECK"

def update_order_status(data):
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

import threading

def start_listener():
    # queue_callbacks = {
    #     TOPIC_ORDER_STATUS: update_order_status
    # }
    # consume_kafka(queue_callbacks.keys(), queue_callbacks)
    consume_kafka_rpc(TOPIC_ORDER_STATUS, update_order_status)
