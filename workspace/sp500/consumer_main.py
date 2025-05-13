from config import BROKER, TOPIC, BATCH_SIZE
from consumer.kafka_consumer import KafkaBatchConsumer

def main():
    consumer = KafkaBatchConsumer(
        brokers=BROKER,
        topic=TOPIC,
        batch_size=BATCH_SIZE,
        flush_interval=10,
        idle_timeout=20
    )
    consumer.consume()

if __name__ == "__main__":
    main()