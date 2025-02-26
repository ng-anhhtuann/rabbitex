from rabbitmq import consume_default, consume_fanout, consume_topic, consume_rpc
import models, database

# @Default
# QUEUE_UPDATE = "order.update"
# QUEUE_UPDATE_SUCCESS = "order.update.success"

# @Fanout
# EXCHANGE_USER = "EX_USER"

# @Topic
# TOPIC_UPDATE = "update.status"

# @RPC
QUEUE_ORDER_STATUS_UPDATE = "order_status_update"

def update_order_status(data):
    print("UPDATE ORDER STATUS")
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

def start_listener():
    # Default
    # queue_callbacks = {
    #     QUEUE_UPDATE: update_order_status
    # }
    # consume_default(queue_callbacks.keys(), queue_callbacks)
    
    # Fanout
    # exchange_callbacks = {
    #     EXCHANGE_USER: update_order_status
    # }
    # consume_fanout(exchange_callbacks)
    
    # Topic
    # topic_callbacks = {
    #     TOPIC_UPDATE: update_order_status
    # }
    # consume_topic(topic_callbacks)
    
    # Rpc
    rpc_callbacks = {
        QUEUE_ORDER_STATUS_UPDATE: update_order_status
    }
    consume_rpc(rpc_callbacks)
