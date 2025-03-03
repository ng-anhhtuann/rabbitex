from rabbitmq import consume_default, publish_default, publish_fanout, consume_fanout, consume_topic, publish_topic, consume_rpc
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
# QUEUE_PROCESS = "payment.process"

# @Fanout
# EXCHANGE_ORDER = "EX_ORDER"
# EXCHANGE_PRODUCT = "EX_PRODUCT"
# EXCHANGE_USER = "EX_USER"

# @Topic
# TOPIC_USER = "user.check"
# TOPIC_UPDATE = "update.*"

# @RPC
QUEUE_BALANCE = "user_check"

# New Orchestrator RPC Queues
QUEUE_CHECK_BALANCE = "user_balance_check"
QUEUE_UPDATE_BALANCE = "user_balance_update"

def process_payment(data):
    print("CHECK INSUFFICIENT BALANCE")
    print(data)
    db = database.SessionLocal()
    user = db.query(models.User).filter(models.User.id == data["owner_id"]).first()

    message = {
        "order_id": data["order_id"],
        "product_id": data.get("product_id"),  
        "quantity": data.get("quantity")       
    }

    if not user or user.balance < data["amount"]:
        message["status"] = "FAILED"
    else:
        user.balance -= data["amount"]
        db.commit()
        
        message["status"] = "SUCCESS"
        message["product_id"] = data["product_id"]
        
    # publish_default(QUEUE_UPDATE, message)
    # publish_fanout(EXCHANGE_USER, message)  
    # publish_topic(TOPIC_UPDATE, message)  
    
    db.close()
    
    # @RPC
    return message

# New functions for orchestrator
def check_balance_for_orchestrator(data):
    print("CHECK BALANCE FOR ORCHESTRATOR")
    print(data)
    db = database.SessionLocal()
    user = db.query(models.User).filter(models.User.id == data["owner_id"]).first()
    
    if not user or user.balance < data["amount"]:
        db.close()
        return {"status": "FAILED", "reason": "Insufficient balance"}
    
    db.close()
    return {"status": "SUCCESS"}

def update_balance_for_orchestrator(data):
    print("UPDATE BALANCE FOR ORCHESTRATOR")
    print(data)
    db = database.SessionLocal()
    user = db.query(models.User).filter(models.User.id == data["owner_id"]).first()
    
    if not user:
        db.close()
        return {"status": "FAILED", "reason": "User not found"}
    
    # Handle compensation transactions (refunds)
    if data.get("is_compensation", False):
        user.balance -= data["amount"]  # For refunds, amount is negative
    else:
        # Regular deduction
        if user.balance < data["amount"]:
            db.close()
            return {"status": "FAILED", "reason": "Insufficient balance"}
        user.balance -= data["amount"]
    
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
    routing_key = f"saga.{saga_id}.event.user.{action}.{result_status}"
    
    # Create message with metadata
    message = {
        "sagaId": saga_id,
        "correlationId": correlation_id,
        "payload": payload,
        "timestamp": str(time.time()),
        "service": "user",
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

def handle_check_balance_command(data):
    """Handle check balance command from saga orchestrator"""
    print("SAGA: CHECK BALANCE COMMAND")
    print(data)
    
    saga_id = data["sagaId"]
    correlation_id = data["correlationId"]
    payload = data["payload"]
    
    db = database.SessionLocal()
    user = db.query(models.User).filter(models.User.id == payload["owner_id"]).first()
    
    if not user or user.balance < payload["amount"]:
        db.close()
        response = {
            "status": "FAILED", 
            "reason": "Insufficient balance",
            "payload": payload
        }
    else:
        db.close()
        response = {
            "status": "SUCCESS",
            "payload": payload
        }
    
    # Publish the result as an event
    publish_saga_event(saga_id, "check_balance", response, correlation_id)
    
    return response

def handle_update_balance_command(data):
    """Handle update balance command from saga orchestrator"""
    print("SAGA: UPDATE BALANCE COMMAND")
    print(data)
    
    saga_id = data["sagaId"]
    correlation_id = data["correlationId"]
    payload = data["payload"]
    compensating = data.get("compensating", False)
    
    db = database.SessionLocal()
    user = db.query(models.User).filter(models.User.id == payload["owner_id"]).first()
    
    if not user:
        db.close()
        response = {
            "status": "FAILED", 
            "reason": "User not found",
            "payload": payload
        }
    else:
        try:
            if compensating:
                # Compensating transaction - refund
                user.balance += abs(payload["amount"])
            else:
                # Normal transaction - deduct
                if user.balance < payload["amount"]:
                    db.close()
                    response = {
                        "status": "FAILED", 
                        "reason": "Insufficient balance",
                        "payload": payload
                    }
                    publish_saga_event(saga_id, "update_balance", response, correlation_id, compensating)
                    return response
                    
                user.balance -= payload["amount"]
                
            db.commit()
            db.close()
            response = {
                "status": "SUCCESS",
                "payload": payload
            }
        except Exception as e:
            db.rollback()
            db.close()
            response = {
                "status": "FAILED", 
                "reason": str(e),
                "payload": payload
            }
    
    # Publish the result as an event
    publish_saga_event(saga_id, "update_balance", response, correlation_id, compensating)
    
    return response

def consume_saga_commands():
    """Consume saga commands for the user service"""
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    
    # Ensure the exchange exists
    channel.exchange_declare(exchange=SAGA_EXCHANGE, exchange_type=ExchangeType.topic, durable=True)
    
    # Create a queue for this service
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    
    # Bind to command routing patterns for this service
    pattern = "saga.*.command.user.*"
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
            if action == "check_balance":
                handle_check_balance_command(data)
            elif action == "update_balance":
                handle_update_balance_command(data)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=on_message,
        auto_ack=False
    )
    
    print("[*] User service waiting for saga commands. To exit press CTRL+C")
    channel.start_consuming()
    
def start_listener():
    # Default
    # queue_callbacks = {
    #     QUEUE_PROCESS: process_payment,
    # }
    # consume_default(queue_callbacks.keys(), queue_callbacks)
    
    # Fanout
    # exchange_callbacks = {
    #     EXCHANGE_PRODUCT: process_payment
    # }
    # consume_fanout(exchange_callbacks)
    
    # Topic
    # topic_callbacks = {
    #     TOPIC_USER: process_payment
    # }
    # consume_topic(topic_callbacks)
    
    # RPC
    rpc_callbacks = {
        QUEUE_BALANCE: process_payment,
        QUEUE_CHECK_BALANCE: check_balance_for_orchestrator,
        QUEUE_UPDATE_BALANCE: update_balance_for_orchestrator
    }
    
    # Start RPC consumer in a thread
    rpc_thread = threading.Thread(target=lambda: consume_rpc(rpc_callbacks), daemon=True)
    rpc_thread.start()
    
    # Start saga command consumer in the main thread
    consume_saga_commands()