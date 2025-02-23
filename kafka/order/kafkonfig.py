from kafka import KafkaProducer, KafkaConsumer
import json

KAFKA_BROKER = "localhost:9092"
# EACH CONSUMER INDEPENDENTLY RECEIVE THE SAME DATA
# SAME GROUP -> LOAD BALANCING
# NOT SAME -> SIMPLE PUB/SUB
CONSUME_GROUP = "DEMO_ORDER"
UTF8 = "utf-8"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode(UTF8),
)

def publish_kafka(topic, message):
    producer.send(topic, value=message)
    producer.flush()

def consume_kafka(topics, callback_map):
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode(UTF8)),
        group_id=CONSUME_GROUP, 
    )

    for message in consumer:
        topic = message.topic
        data = message.value

        if topic in callback_map:
            callback_map[topic](data)