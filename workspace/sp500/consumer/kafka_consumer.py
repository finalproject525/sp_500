import json
import time
from kafka import KafkaConsumer
from services.aws.S3Uploader import S3Uploader,LocalUploader
from config import AWS_S3_BUCKET,SAVE_LOCAL


class KafkaBatchConsumer:
    def __init__(self, brokers, topic, batch_size=100, flush_interval=10, idle_timeout=20, group_id="yfinance-group"):
        self.brokers = brokers
        self.topic = topic
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.idle_timeout = idle_timeout
        self.group_id = group_id
        self.consumer = self._create_consumer()

    def _create_consumer(self):
        return KafkaConsumer(
            self.topic,
            bootstrap_servers=self.brokers,
            group_id=self.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8"))
        )

    def _handle_batch(self, messages):
        if SAVE_LOCAL == False:
            uploader = S3Uploader(bucket_name=AWS_S3_BUCKET)
            
        else :
            uploader = LocalUploader(output_dir="test_output")

        uploader.upload_json(messages, prefix="yfinance-data")

    def consume(self):
        buffer = []
        last_flush_time = time.time()
        last_message_time = time.time()
        print("🟢 Kafka consumer started with timeout handling...")

        try:
            while True:
                records = self.consumer.poll(timeout_ms=1000)
                any_new_message = False

                for tp, messages in records.items():
                    for message in messages:
                        buffer.append(message.value)
                        any_new_message = True

                now = time.time()

                if any_new_message:
                    last_message_time = now

                if len(buffer) >= self.batch_size:
                    print(f"📦 Batch size reached: flushing {len(buffer)} messages")
                    self._handle_batch(buffer)
                    buffer.clear()
                    last_flush_time = now

                elif buffer and (now - last_flush_time >= self.flush_interval):
                    print(f"⏱ Timeout reached: flushing {len(buffer)} messages")
                    self._handle_batch(buffer)
                    buffer.clear()
                    last_flush_time = now

                if now - last_message_time >= self.idle_timeout:
                    print(f"🛑 No new messages for {self.idle_timeout}s. Exiting.")
                    break

        finally:
            if buffer:
                print(f"📤 Final flush of {len(buffer)} remaining messages")
                self._handle_batch(buffer)





