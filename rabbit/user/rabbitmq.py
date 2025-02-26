import pika
import json
from pika.exchange_type import ExchangeType
import uuid

RABBITMQ_HOST = "localhost"
EXCHANGE = "EXCHANGE"

def get_channel():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    return connection, connection.channel()

def publish_default(queue_name, message):
    connection, channel = get_channel()
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_publish(exchange="", routing_key=queue_name, body=json.dumps(message))
    connection.close()

def publish_direct(routing_key, message):
    connection, channel = get_channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type=ExchangeType.direct, durable=True)
    channel.basic_publish(exchange=EXCHANGE, routing_key=routing_key, body=json.dumps(message))
    connection.close()

def publish_topic(routing_key, message):
    connection, channel = get_channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type=ExchangeType.topic, durable=True)
    channel.basic_publish(exchange=EXCHANGE, routing_key=routing_key, body=json.dumps(message))
    connection.close()

def publish_fanout(exchange_name, message):
    connection, channel = get_channel()
    channel.exchange_declare(exchange=exchange_name, exchange_type=ExchangeType.fanout, durable=True)
    channel.basic_publish(exchange=exchange_name, routing_key="", body=json.dumps(message))
    connection.close()

def publish_rpc(queue_name, message):
    connection, channel = get_channel()

    result = channel.queue_declare(queue="", exclusive=True)
    callback_queue = result.method.queue

    correlation_id = str(uuid.uuid4())
    response = None

    def on_response(ch, method, properties, body):
        nonlocal response
        if properties.correlation_id == correlation_id:
            response = json.loads(body)
            ch.stop_consuming() 

    channel.basic_consume(queue=callback_queue, on_message_callback=on_response, auto_ack=True)

    properties = pika.BasicProperties(
        reply_to=callback_queue,  
        correlation_id=correlation_id
    )

    channel.basic_publish(exchange="", routing_key=queue_name, body=json.dumps(message), properties=properties)
    channel.start_consuming()

    connection.close()
    return response 

def consume_default(queue_names, callback_map):
    connection, channel = get_channel()

    def on_message(ch, method, properties, body):
        queue_name = method.routing_key
        data = json.loads(body)

        if queue_name in callback_map:
            callback_map[queue_name](data)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    for queue_name in queue_names:
        channel.queue_declare(queue=queue_name, durable=True)
        channel.basic_consume(queue=queue_name, on_message_callback=on_message)

    channel.start_consuming()
    
def consume_fanout(exchange_callback_map):
    connection, channel = get_channel()
    
    for exchange_name, callback in exchange_callback_map.items():
        channel.exchange_declare(exchange=exchange_name, exchange_type=ExchangeType.fanout, durable=True)
        
        result = channel.queue_declare(queue="", durable=True)
        queue_name = result.method.queue
        
        channel.queue_bind(exchange=exchange_name, queue=queue_name)
        
        def on_message(ch, method, properties, body, callback=callback):
            data = json.loads(body)
            callback(data)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        
        channel.basic_consume(
            queue=queue_name,
            on_message_callback=on_message,
            auto_ack=False
        )
    
    channel.start_consuming()
    
def consume_topic(routing_callback_map):
    connection, channel = get_channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type=ExchangeType.topic, durable=True)
    
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    
    for routing_key, callback in routing_callback_map.items():
        channel.queue_bind(exchange=EXCHANGE, queue=queue_name, routing_key=routing_key)
        
        def on_message(ch, method, properties, body, callback=callback):
            data = json.loads(body)
            callback(data)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        
        channel.basic_consume(
            queue=queue_name,
            on_message_callback=on_message,
            auto_ack=True
        )
    
    channel.start_consuming()
    
def consume_rpc(callback_map):
    connection, channel = get_channel()

    for queue_name in callback_map.keys():
        channel.queue_declare(queue=queue_name, durable=True)

    def on_request(ch, method, properties, body):
        queue_name = method.routing_key
        data = json.loads(body)

        if queue_name in callback_map:
            response = callback_map[queue_name](data)  

            ch.basic_publish(
                exchange="",
                routing_key=properties.reply_to,  
                properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                body=json.dumps(response)
            )

        ch.basic_ack(delivery_tag=method.delivery_tag)

    for queue_name in callback_map.keys():
        channel.basic_consume(queue=queue_name, on_message_callback=on_request)

    channel.start_consuming()