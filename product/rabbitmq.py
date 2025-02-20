import pika
import json

RABBITMQ_HOST = "localhost"
EXCHANGE_NAME = "rabbitex"

def publish_message(queue_name, message):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=True)

    channel.queue_declare(queue=queue_name, durable=True)

    channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name, routing_key=queue_name)

    channel.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=queue_name,
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2)
    )
    connection.close()

def consume_message(queue_name, callback):
    def on_message(ch, method, properties, body):
        data = json.loads(body)
        callback(data)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    
    channel.basic_consume(queue=queue_name, on_message_callback=on_message, auto_ack=False)
    
    channel.start_consuming()
    