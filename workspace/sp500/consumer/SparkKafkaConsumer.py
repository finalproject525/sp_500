from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType
from services.aws.S3Uploader import S3Uploader
from config import USE_DYNAMIC_GROUP

class SparkKafkaConsumer:
    def __init__(self, kafka_bootstrap_servers, topic, s3_bucket, json_prefix="json", parquet_prefix="parquet"):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.topic = topic
        self.s3_bucket = s3_bucket
        self.json_prefix = json_prefix
        self.parquet_prefix = parquet_prefix
        self.spark = self._create_spark_session()
        self.uploader = S3Uploader(bucket_name=self.s3_bucket)

    def _create_spark_session(self):
        return SparkSession.builder \
            .appName("KafkaSparkToS3") \
            .master("local[*]") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
            .getOrCreate()

    def _parse_kafka_stream(self):
        schema = StructType() \
            .add("Open", StringType()) \
            .add("High", StringType()) \
            .add("Low", StringType()) \
            .add("Close", StringType()) \
            .add("Volume", StringType()) \
            .add("symbol", StringType()) \
            .add("timestamp", StringType()) \
            .add("datetime", StringType())

        raw_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.topic) \
            .option("startingOffsets", "earliest") \
            .load()

        parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")

        return parsed_df

    def _write_batch_to_s3_old(self, batch_df, batch_id):
        if batch_df.isEmpty():
            print(f"⚠️ Batch {batch_id} is empty — skipping S3 upload.")
            return

        pd_df = batch_df.toPandas()
        records = pd_df.to_dict(orient="records")

        print(f"📤 Sending batch {batch_id} to S3")
        self.uploader.upload_json(records, prefix=self.json_prefix)
        self.uploader.upload_dataframe_as_parquet(pd_df, s3_key=self._make_parquet_key(batch_id))

        
    def _write_batch_to_s3(self, batch_df, batch_id):
    # Vérifie si le batch est vide
        if batch_df.rdd.isEmpty():
            print(f"⚠️ Batch {batch_id} is empty — skipping S3 upload.")
            return

        # Affiche le nombre de lignes du batch
        row_count = batch_df.count()
        print(f"📊 Batch {batch_id} has {row_count} rows.")

        # Convertit en DataFrame pandas
        pd_df = batch_df.toPandas()

        # Conversion en liste de dictionnaires pour le JSON
        records = pd_df.to_dict(orient="records")

        print(f"📤 Sending batch {batch_id} to S3")

        # Upload vers S3 : JSON + Parquet
        self.uploader.upload_json(records, prefix=self.json_prefix)
        self.uploader.upload_dataframe_as_parquet(
            pd_df,
            prefix=self.parquet_prefix
        )


    def _make_parquet_key(self, batch_id):
        return f"{self.parquet_prefix}/batch_{batch_id}.parquet"

    def start(self):
        df = self._parse_kafka_stream()

        query = df.writeStream \
            .foreachBatch(self._write_batch_to_s3) \
            .outputMode("append") \
            .start()

        query.awaitTermination()
