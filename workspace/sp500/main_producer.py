
import threading
import queue
import time
import json
from kafka import KafkaProducer
from finance.functions import get_sp500_symbol
from finance.YahooFinance import YahooBatchFinanceClient  
from config import TEST,SYMBOLS_TEST,BROKER,TOPIC,PERIOD,INTERVAL,API_BATCH_SIZE
#from producer.kafka_producer import create_producer,send_messages 




def fetch_batches(symbols, data_queue, batch_size, period, interval):
    client = YahooBatchFinanceClient(symbols=symbols, batch_size=batch_size, period=period, interval=interval)
    
    for batch in client._chunk_list(symbols, batch_size):
        client_batch = YahooBatchFinanceClient(
            symbols=batch,
            batch_size=batch_size,
            period=period,
            interval=interval
        )
        client_batch.fetch_all()
        batch_records = client_batch.to_dict_records()
        print(f"📥 Got batch with {len(batch_records)} records")
        data_queue.put(batch_records)




def send_batches(producer, topic, data_queue):
    while True:
        try:
            messages = data_queue.get(timeout=30)  
            send_messages(producer, topic, messages)
            print(f"📤 Sent batch of {len(messages)} messages")
        except queue.Empty:
            print("✅ Queue is empty, done sending.")
            break


def create_producer(broker):
    return KafkaProducer(
        bootstrap_servers = broker,
        value_serializer = lambda v:json.dumps(v).encode('utf-8')
        
    )

def send_messages(producer, topic, messages):
    for message in messages:
        producer.send(topic, value=message)
    producer.flush()


if __name__ == "__main__":

    data_queue = queue.Queue()

    if TEST == True:
        symbols = SYMBOLS_TEST  # List of stock symbols
    else :
        symbols = get_sp500_symbol()['Symbol'].to_list()  # List of stock symbols

 
    data_queue = queue.Queue()
    producer = create_producer(BROKER)

    # Threads
    fetcher_thread = threading.Thread(target=fetch_batches, args=(symbols, data_queue, API_BATCH_SIZE, PERIOD, INTERVAL))
    sender_thread = threading.Thread(target=send_batches, args=(producer, TOPIC, data_queue))

    fetcher_thread.start()
    sender_thread.start()

    fetcher_thread.join()
    sender_thread.join()
        