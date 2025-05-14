from finance.functions import get_sp500_symbol
TEST = True

SYMBOLS_TEST = ["AAPL", "MSFT", "GOOG","TSLA","NVDA","META","AMZN"]





################ yfinance api config#############################
PERIOD = "1d"
INTERVAL = "60m" 
API_BATCH_SIZE = 50

################ Kafka Config ###################################
TOPIC = 'yfinance-data'
BROKER = ['course-kafka:9092']
BATCH_SIZE = 100
USE_DYNAMIC_GROUP = False

################### AWS #########################################

SAVE_LOCAL = False
AWS_S3_BUCKET = "final-de-project-sp500"


