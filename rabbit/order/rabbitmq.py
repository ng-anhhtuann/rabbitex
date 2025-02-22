import pika
import json
from pika.exchange_type import ExchangeType

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

def publish_fanout(exchange, message):
    connection, channel = get_channel()
    channel.exchange_declare(exchange=exchange, exchange_type=ExchangeType.fanout, durable=True)
    channel.basic_publish(exchange=exchange, routing_key="", body=json.dumps(message))
    connection.close()

def publish_headers(headers, message):
    connection, channel = get_channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type=ExchangeType.headers, durable=True)
    properties = pika.BasicProperties(headers=headers)
    channel.basic_publish(exchange=EXCHANGE, routing_key="", body=json.dumps(message), properties=properties)
    connection.close()

def consume_queues(queue_names, callback_map):
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