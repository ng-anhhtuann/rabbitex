import pika
import json
from pika.exchange_type import ExchangeType
import uuid
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
SAGA_EXCHANGE = "SAGA_EXCHANGE"

def get_channel():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    return connection, connection.channel()

def publish_saga_command(saga_id, service, action, payload, correlation_id=None, compensating=False):
    """
    Publish a command to a service as part of a saga
    
    Args:
        saga_id: Unique identifier for the saga
        service: Target service (e.g., 'product', 'user')
        action: Action to perform (e.g., 'check_stock', 'update_balance')
        payload: Command data
        correlation_id: Optional correlation ID for tracking
        compensating: Whether this is a compensating transaction
    """
    if correlation_id is None:
        correlation_id = str(uuid.uuid4())
        
    connection, channel = get_channel()
    
    # Ensure the exchange exists
    channel.exchange_declare(exchange=SAGA_EXCHANGE, exchange_type=ExchangeType.topic, durable=True)
    
    # Create routing key in format: saga.{sagaId}.command.{service}.{action}
    routing_key = f"saga.{saga_id}.command.{service}.{action}"
    
    # Create message with metadata
    message = {
        "sagaId": saga_id,
        "correlationId": correlation_id,
        "payload": payload,
        "timestamp": datetime.utcnow().isoformat(),
        "service": service,
        "action": action,
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
    return correlation_id

def consume_saga_events(service_name, event_handlers, saga_id=None):
    """
    Consume saga events for a specific service
    
    Args:
        service_name: Name of this service
        event_handlers: Dictionary mapping action names to handler functions
        saga_id: Optional saga ID to filter for (None means listen to all sagas)
    """
    connection, channel = get_channel()
    
    # Ensure the exchange exists
    channel.exchange_declare(exchange=SAGA_EXCHANGE, exchange_type=ExchangeType.topic, durable=True)
    
    # Create a queue for this service
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    
    # Bind to command routing patterns
    if saga_id:
        # Listen only to commands for this specific saga
        pattern = f"saga.{saga_id}.command.{service_name}.*"
    else:
        # Listen to all commands for this service
        pattern = f"saga.*.command.{service_name}.*"
        
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
            
            if action in event_handlers:
                # Call the appropriate handler
                result = event_handlers[action](data)
                
                # Publish the result as an event
                publish_saga_event(
                    data["sagaId"],
                    service_name,
                    action,
                    result,
                    properties.correlation_id,
                    data.get("compensating", False)
                )
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=on_message,
        auto_ack=False
    )
    
    print(f"[*] {service_name} service waiting for saga commands. To exit press CTRL+C")
    channel.start_consuming()

def publish_saga_event(saga_id, service, action, payload, correlation_id, compensating=False):
    """
    Publish an event as part of a saga
    
    Args:
        saga_id: Unique identifier for the saga
        service: Source service (e.g., 'product', 'user')
        action: Action performed (e.g., 'check_stock', 'update_balance')
        payload: Event data including status
        correlation_id: Correlation ID for tracking
        compensating: Whether this was a compensating transaction
    """
    connection, channel = get_channel()
    
    # Ensure the exchange exists
    channel.exchange_declare(exchange=SAGA_EXCHANGE, exchange_type=ExchangeType.topic, durable=True)
    
    # Determine result status
    result_status = "success" if payload.get("status") == "SUCCESS" else "failed"
    
    # Create routing key in format: saga.{sagaId}.event.{service}.{action}.{result}
    routing_key = f"saga.{saga_id}.event.{service}.{action}.{result_status}"
    
    # Create message with metadata
    message = {
        "sagaId": saga_id,
        "correlationId": correlation_id,
        "payload": payload,
        "timestamp": datetime.utcnow().isoformat(),
        "service": service,
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

def consume_saga_orchestrator(event_handlers):
    """
    Consume all saga events for the orchestrator
    
    Args:
        event_handlers: Dictionary mapping patterns to handler functions
    """
    connection, channel = get_channel()
    
    # Ensure the exchange exists
    channel.exchange_declare(exchange=SAGA_EXCHANGE, exchange_type=ExchangeType.topic, durable=True)
    
    # Create a queue for the orchestrator
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    
    # Bind to all event routing patterns
    pattern = "saga.*.event.*.*.*"
    channel.queue_bind(
        exchange=SAGA_EXCHANGE,
        queue=queue_name,
        routing_key=pattern
    )
    
    def on_message(ch, method, properties, body):
        data = json.loads(body)
        
        # Extract parts from routing key
        routing_parts = method.routing_key.split('.')
        if len(routing_parts) >= 6:
            saga_id = routing_parts[1]
            service = routing_parts[3]
            action = routing_parts[4]
            result = routing_parts[5]
            
            # Create pattern for handler lookup
            pattern = f"{service}.{action}.{result}"
            
            if pattern in event_handlers:
                # Call the appropriate handler
                event_handlers[pattern](saga_id, data)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=on_message,
        auto_ack=False
    )
    
    print("[*] Orchestrator waiting for saga events. To exit press CTRL+C")
    channel.start_consuming() 