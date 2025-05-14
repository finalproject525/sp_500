from kafka import KafkaProducer
import json

def create_producer(broker):
    return KafkaProducer(
        bootstrap_servers = broker,
        value_serializer = lambda v:json.dumps(v).encode('utf-8')
        
    )

def send_messages(producer, topic, messages):
    for message in messages:
        producer.send(topic, value=message)
    producer.flush()