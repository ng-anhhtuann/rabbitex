from rabbitmq import consume_default, publish_default, publish_fanout, consume_fanout, consume_topic, publish_topic, publish_rpc, consume_rpc
import models, database
import time
import os
from dotenv import load_dotenv
import threading
import pika
import json
from pika.exchange_type import ExchangeType

load_dotenv()

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
SAGA_EXCHANGE = "SAGA_EXCHANGE"

# @Default
# QUEUE_UPDATE = "order.update"
# QUEUE_START = "order.create"
# QUEUE_PROCESS = "payment.process"
# QUEUE_UPDATE_SUCCESS = "order.update.success"

# @Fanout
# EXCHANGE_ORDER = "EX_ORDER"
# EXCHANGE_PRODUCT = "EX_PRODUCT"
# EXCHANGE_USER = "EX_USER"

# @Topic
# TOPIC_ORDER = "order.create"
# TOPIC_USER = "user.check"
# TOPIC_UPDATE_STOCK = "update.stock"
# TOPIC_UPDATE = "update.*"

# @RPC
QUEUE_CREATE = "order_create"
QUEUE_STOCK_UPDATE = "product_stock_update"
QUEUE_ORDER_STATUS_UPDATE = "order_status_update"
QUEUE_BALANCE = "user_check"

# New Orchestrator RPC Queues
QUEUE_CHECK_STOCK = "product_stock_check"
QUEUE_UPDATE_STOCK = "product_stock_update"

def check_stock(data):    
    print("CHECK AVAILABLE STOCK")
    print(data)
    db = database.SessionLocal()
    product = db.query(models.Product).filter(models.Product.id == data["product_id"]).first()
    response = None
    
    # time.sleep(2)
    
    if not product or product.stock < data["quantity"] or data["quantity"] <= 0:
        # publish_default(QUEUE_UPDATE, {"order_id": data["order_id"], "status": "FAILED"})
        # publish_fanout(EXCHANGE_USER, {"order_id": data["order_id"], "status": "FAILED"})
        # publish_topic(QUEUE_UPDATE, {"order_id": data["order_id"], "status": "FAILED"})
        
        # @RPC
        response = publish_rpc(QUEUE_ORDER_STATUS_UPDATE, {
            "order_id": data["order_id"],
            "status": "FAILED"
        })
        # End @RPC
    
    else:
        total_price = product.price * data["quantity"]
        data["amount"] = total_price
        # publish_default(QUEUE_PROCESS, data)  
        # publish_fanout(EXCHANGE_PRODUCT, data)  
        # publish_topic(TOPIC_USER, data)  
        
        # @RPC
        response_balance = publish_rpc(QUEUE_BALANCE, data)
        if response_balance.get("status") != "SUCCESS":
            update_message = {
                "order_id": data["order_id"],
                "status": "FAILED"
            }
            response = publish_rpc(QUEUE_ORDER_STATUS_UPDATE, update_message)
            return response_balance
        else:
            product.stock -= data["quantity"]
            db.commit()
            update_message = {
                "order_id": data["order_id"],
                "status": "SUCCESS"
            }
            response = publish_rpc(QUEUE_ORDER_STATUS_UPDATE, update_message)
            return response
        # End @RPC
    db.close()
    return response

def update_stock(data):
    print("UPDATE STOCK DEDUCTION")
    print(data)
    db = database.SessionLocal()    
    if (data["status"] == "SUCCESS"):
        product = db.query(models.Product).filter(models.Product.id == data["product_id"]).first()

        if product:
            product.stock -= data["quantity"]
            db.commit()
    
        # publish_default(QUEUE_UPDATE_SUCCESS, data)  
    db.close()
    
# New functions for orchestrator
def check_stock_for_orchestrator(data):
    print("CHECK STOCK FOR ORCHESTRATOR")
    print(data)
    db = database.SessionLocal()
    product = db.query(models.Product).filter(models.Product.id == data["product_id"]).first()
    
    if not product or product.stock < data["quantity"] or data["quantity"] <= 0:
        db.close()
        return {"status": "FAILED", "reason": "Insufficient stock"}
    
    total_price = product.price * data["quantity"]
    db.close()
    
    return {
        "status": "SUCCESS", 
        "total_price": total_price,
        "product_id": data["product_id"],
        "quantity": data["quantity"]
    }

def update_stock_for_orchestrator(data):
    print("UPDATE STOCK FOR ORCHESTRATOR")
    print(data)
    db = database.SessionLocal()
    product = db.query(models.Product).filter(models.Product.id == data["product_id"]).first()
    
    if not product or product.stock < data["quantity"]:
        db.close()
        return {"status": "FAILED", "reason": "Insufficient stock"}
    
    product.stock -= data["quantity"]
    db.commit()
    db.close()
    
    return {"status": "SUCCESS"}

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
    routing_key = f"saga.{saga_id}.event.product.{action}.{result_status}"
    
    # Create message with metadata
    message = {
        "sagaId": saga_id,
        "correlationId": correlation_id,
        "payload": payload,
        "timestamp": str(time.time()),
        "service": "product",
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

def handle_check_stock_command(data):
    """Handle check stock command from saga orchestrator"""
    print("SAGA: CHECK STOCK COMMAND")
    print(data)
    
    saga_id = data["sagaId"]
    correlation_id = data["correlationId"]
    payload = data["payload"]
    
    db = database.SessionLocal()
    product = db.query(models.Product).filter(models.Product.id == payload["product_id"]).first()
    
    if not product or product.stock < payload["quantity"] or payload["quantity"] <= 0:
        db.close()
        response = {
            "status": "FAILED", 
            "reason": "Insufficient stock",
            "payload": payload
        }
    else:
        total_price = product.price * payload["quantity"]
        db.close()
        response = {
            "status": "SUCCESS", 
            "total_price": total_price,
            "payload": payload
        }
    
    # Publish the result as an event
    publish_saga_event(saga_id, "check_stock", response, correlation_id)
    
    return response

def handle_update_stock_command(data):
    """Handle update stock command from saga orchestrator"""
    print("SAGA: UPDATE STOCK COMMAND")
    print(data)
    
    saga_id = data["sagaId"]
    correlation_id = data["correlationId"]
    payload = data["payload"]
    compensating = data.get("compensating", False)
    
    db = database.SessionLocal()
    product = db.query(models.Product).filter(models.Product.id == payload["product_id"]).first()
    
    if not product or (not compensating and product.stock < payload["quantity"]):
        db.close()
        response = {
            "status": "FAILED", 
            "reason": "Insufficient stock",
            "payload": payload
        }
    else:
        if compensating:
            # Compensating transaction - add stock back
            product.stock += payload["quantity"]
        else:
            # Normal transaction - reduce stock
            product.stock -= payload["quantity"]
            
        db.commit()
        db.close()
        response = {
            "status": "SUCCESS",
            "payload": payload
        }
    
    # Publish the result as an event
    publish_saga_event(saga_id, "update_stock", response, correlation_id, compensating)
    
    return response

def consume_saga_commands():
    """Consume saga commands for the product service"""
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    
    # Ensure the exchange exists
    channel.exchange_declare(exchange=SAGA_EXCHANGE, exchange_type=ExchangeType.topic, durable=True)
    
    # Create a queue for this service
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    
    # Bind to command routing patterns for this service
    pattern = "saga.*.command.product.*"
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
            if action == "check_stock":
                handle_check_stock_command(data)
            elif action == "update_stock":
                handle_update_stock_command(data)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=on_message,
        auto_ack=False
    )
    
    print("[*] Product service waiting for saga commands. To exit press CTRL+C")
    channel.start_consuming()
    
def start_listener():
    # Default
    # queue_callbacks = {
    #     QUEUE_START: check_stock,
    #     QUEUE_UPDATE: update_stock
    # }
    # consume_default(queue_callbacks.keys(), queue_callbacks)
    
    # Fanout
    # exchange_callbacks = {
    #     EXCHANGE_ORDER: check_stock,
    #     EXCHANGE_USER: update_stock
    # }
    # consume_fanout(exchange_callbacks)
    
    # Topic
    # topic_callbacks = {
    #     TOPIC_ORDER: check_stock,
    #     TOPIC_UPDATE_STOCK: update_stock
    # }
    # consume_topic(topic_callbacks)
    
    # RPC
    rpc_callbacks = {
        QUEUE_CREATE: check_stock,
        QUEUE_CHECK_STOCK: check_stock_for_orchestrator,
        QUEUE_UPDATE_STOCK: update_stock_for_orchestrator
    }
    
    # Start RPC consumer in a thread
    rpc_thread = threading.Thread(target=lambda: consume_rpc(rpc_callbacks), daemon=True)
    rpc_thread.start()
    
    # Start saga command consumer in the main thread
    consume_saga_commands()