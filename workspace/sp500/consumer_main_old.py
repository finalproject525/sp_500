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




from consumer.spark_consumer import KafkaSparkConsumer

if __name__ == "__main__":
    consumer = KafkaSparkConsumer(
        kafka_bootstrap_servers=BROKER,
        kafka_topic=TOPIC,
        checkpoint_dir="/project/workspace/sp500/spark_output/_checkpoints/",
        output_path="/project/workspace/sp500/spark_output/json/"
    )

    consumer.start_stream()
