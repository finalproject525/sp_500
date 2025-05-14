from pyspark.sql import SparkSession

# Création de la session Spark
spark = SparkSession.builder \
    .appName("KafkaStreamPreview") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

# Lecture du stream Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:9092")\
    .option("subscribe", "yfinance-data") \
    .option("startingOffsets", "earliest") \
    .load()
v
# Conversion des messages (clé/valeur) de bytes → string
df_parsed = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Écriture dans la console (stream)
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
