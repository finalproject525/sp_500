

from finance.functions import get_sp500_symbol
from finance.YahooFinance import YahooBatchFinanceClient  
from config import TEST,SYMBOLS_TEST,BROKER,TOPIC,PERIOD,INTERVAL
from producer.kafka_producer import create_producer,send_messages 

if __name__ == "__main__":

    if TEST == True:
        symbols = SYMBOLS_TEST  # List of stock symbols
    else :
        symbols = get_sp500_symbol()['Symbol'].to_list()  # List of stock symbols

 
    client = YahooBatchFinanceClient(symbols, period=PERIOD, interval=INTERVAL)
    client.fetch_all()  
    messages = client.to_dict_records()
   
    producer = create_producer(BROKER)
    send_messages(producer, TOPIC, messages)
        