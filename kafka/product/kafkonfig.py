from confluent_kafka import Producer, Consumer
import json

KAFKA_BROKER = "localhost:9092"
KAFKA_GROUP_ID = "partition1"

producer = Producer({"bootstrap.servers": KAFKA_BROKER})

consumer = Consumer({
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest"
})

def publish_default(topic, message):
    producer.produce(topic, key="key", value=json.dumps(message))
    producer.flush()

def consume_queues(topic, callback):
    consumer.subscribe([topic])

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"====================Error: {msg.error()}")
            continue

        data = json.loads(msg.value().decode("utf-8"))
        callback(data)

        consumer.commit()
