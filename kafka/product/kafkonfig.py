from kafka import KafkaProducer, KafkaConsumer
import json
import uuid

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
            
def publish_kafka_rpc(request_topic, reply_topic, message):

    corr_id = str(uuid.uuid4())
    headers = [
        ('correlation_id', corr_id.encode(UTF8)),
        ('reply_topic', reply_topic.encode(UTF8))
    ]
    
    producer.send(request_topic, value=message, headers=headers)
    producer.flush()
    
    consumer = KafkaConsumer(
        reply_topic,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode(UTF8))
    )
    
    for msg in consumer:
        msg_headers = {k: v.decode(UTF8) for k, v in msg.headers} if msg.headers else {}
        if msg_headers.get('correlation_id') == corr_id:
            consumer.close()
            return msg.value

def consume_kafka_rpc(request_topic, callback):
    consumer = KafkaConsumer(
        request_topic,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode(UTF8)),
        group_id=CONSUME_GROUP
    )
    
    for msg in consumer:
        msg_headers = {k: v.decode(UTF8) for k, v in msg.headers} if msg.headers else {}
        corr_id = msg_headers.get('correlation_id')
        reply_topic = msg_headers.get('reply_topic')
        data = msg.value
        
        response = callback(data)
        response_headers = [('correlation_id', corr_id.encode(UTF8))] if corr_id else []
        
        if reply_topic:
            producer.send(reply_topic, value=response, headers=response_headers)
            producer.flush()
            
            
def publish_kafka_sync(topic, message, timeout=5):
    future = producer.send(topic, value=message)
    record_metadata = future.get(timeout=timeout)
    return record_metadata