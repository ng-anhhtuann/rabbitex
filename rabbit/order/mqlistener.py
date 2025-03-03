from rabbitmq import consume_default, consume_fanout, consume_topic, consume_rpc
import models, database
import os
from dotenv import load_dotenv
import threading
import pika
import json
from pika.exchange_type import ExchangeType
import time

load_dotenv()

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
SAGA_EXCHANGE = "SAGA_EXCHANGE"

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

# New saga pattern functions
def publish_saga_event(saga_id, action, payload, correlation_id, compensating=False):
    """Publish a saga event"""
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    
    # Ensure the exchange exists
    channel.exchange_declare(exchange=SAGA_EXCHANGE, exchange_type=ExchangeType.topic, durable=True)
    
    # Determine result status
    result_status = "success" if payload.get("status") == "SUCCESS" else "failed"
    
    # Create routing key in format: saga.{sagaId}.event.{service}.{action}.{result}
    routing_key = f"saga.{saga_id}.event.order.{action}.{result_status}"
    
    # Create message with metadata
    message = {
        "sagaId": saga_id,
        "correlationId": correlation_id,
        "payload": payload,
        "timestamp": str(time.time()),
        "service": "order",
        "action": action,
        "result": result_status,
        "compensating": compensating
    }
    
    # Publish message
    channel.basic_publish(
        exchange=SAGA_EXCHANGE,
        routing_key=routing_key,
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
            correlation_id=correlation_id
        )
    )
    
    connection.close()

def handle_update_status_command(data):
    """Handle update status command from saga orchestrator"""
    print("SAGA: UPDATE ORDER STATUS COMMAND")
    print(data)
    
    saga_id = data["sagaId"]
    correlation_id = data["correlationId"]
    payload = data["payload"]
    
    db = database.SessionLocal()
    order = db.query(models.Order).filter(models.Order.id == payload["order_id"]).first()
    
    if not order:
        db.close()
        response = {
            "status": "FAILED", 
            "reason": "Order not found",
            "payload": payload
        }
    else:
        try:
            order.status = payload["status"]
            db.commit()
            db.refresh(order)
            
            response = {
                "status": "SUCCESS",
                "order_id": order.id,
                "owner_id": order.owner_id,
                "product_id": order.product_id,
                "quantity": order.quantity,
                "order_status": order.status,
                "payload": payload
            }
            db.close()
        except Exception as e:
            db.rollback()
            db.close()
            response = {
                "status": "FAILED", 
                "reason": str(e),
                "payload": payload
            }
    
    # Publish the result as an event
    publish_saga_event(saga_id, "update_status", response, correlation_id)
    
    return response

def consume_saga_commands():
    """Consume saga commands for the order service"""
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    
    # Ensure the exchange exists
    channel.exchange_declare(exchange=SAGA_EXCHANGE, exchange_type=ExchangeType.topic, durable=True)
    
    # Create a queue for this service
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    
    # Bind to command routing patterns for this service
    pattern = "saga.*.command.order.*"
    channel.queue_bind(
        exchange=SAGA_EXCHANGE,
        queue=queue_name,
        routing_key=pattern
    )
    
    def on_message(ch, method, properties, body):
        data = json.loads(body)
        
        # Extract action from routing key
        routing_parts = method.routing_key.split('.')
        if len(routing_parts) >= 5:
            action = routing_parts[4]
            
            # Call the appropriate handler
            if action == "update_status":
                handle_update_status_command(data)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=on_message,
        auto_ack=False
    )
    
    print("[*] Order service waiting for saga commands. To exit press CTRL+C")
    channel.start_consuming()

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
    
    # Start RPC consumer in a thread
    rpc_thread = threading.Thread(target=lambda: consume_rpc(rpc_callbacks), daemon=True)
    rpc_thread.start()
    
    # Start saga command consumer in the main thread
    consume_saga_commands()
