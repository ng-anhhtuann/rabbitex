from kafka import KafkaProducer, KafkaConsumer
import json
import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
CONSUMER_GROUP = "SAGA_ORCHESTRATOR"
UTF8 = "utf-8"

# Topics for commands
TOPIC_CREATE_ORDER = "CREATE_ORDER"
TOPIC_RESERVE_PRODUCT = "RESERVE_PRODUCT"
TOPIC_PROCESS_PAYMENT = "PROCESS_PAYMENT"
TOPIC_COMPLETE_ORDER = "COMPLETE_ORDER"
TOPIC_START_ORDER_SAGA = "START_ORDER_SAGA"
TOPIC_CONFIRM_PRODUCT_DEDUCTION = "CONFIRM_PRODUCT_DEDUCTION"

# Topics for compensations
TOPIC_CANCEL_ORDER = "CANCEL_ORDER"
TOPIC_RELEASE_PRODUCT = "RELEASE_PRODUCT"
TOPIC_REFUND_PAYMENT = "REFUND_PAYMENT"

# Topics for events/responses
TOPIC_ORDER_CREATED = "ORDER_CREATED"
TOPIC_PRODUCT_RESERVED = "PRODUCT_RESERVED"
TOPIC_PRODUCT_RESERVATION_FAILED = "PRODUCT_RESERVATION_FAILED"
TOPIC_PAYMENT_PROCESSED = "PAYMENT_PROCESSED"
TOPIC_PAYMENT_FAILED = "PAYMENT_FAILED"
TOPIC_ORDER_COMPLETED = "ORDER_COMPLETED"
TOPIC_ORDER_CANCELLED = "ORDER_CANCELLED"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode(UTF8),
)

def publish_message(topic, message):
    """Publish a message to a Kafka topic"""
    producer.send(topic, value=message)
    producer.flush()

def consume_messages(topics, callback_map):
    """Consume messages from Kafka topics"""
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode(UTF8)),
        group_id=CONSUMER_GROUP,
    )

    for message in consumer:
        topic = message.topic
        data = message.value

        if topic in callback_map:
            callback_map[topic](data) 