import pika
import json

RABBITMQ_HOST = "localhost"

def publish_message(queue_name, message):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    
    channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        body=json.dumps(message)
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
    
    channel.basic_consume(queue=queue_name, on_message_callback=on_message)
    print(f" [*] Listening for messages on {queue_name}. To exit, press CTRL+C")
    
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print(f" [!] Stopped listening to {queue_name}")
        connection.close()
