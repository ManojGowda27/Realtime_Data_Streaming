import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from time import sleep

def start_streaming(spark):
    topic = 'customers_review'
    
    print("Reading from Kafka...")
    
    # 1. Read Stream from Kafka (Local Docker Config)
    df = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "broker:29092") # Use broker:29092 inside Docker
          .option("subscribe", topic)
          .option("startingOffsets", "earliest")
          .load())

    # 2. Define Schema
    schema = StructType([
        StructField("review_id", StringType()),
        StructField("user_id", StringType()),
        StructField("business_id", StringType()),
        StructField("stars", FloatType()),
        StructField("date", StringType()), 
        StructField("text", StringType())
    ])

    # 3. Parse JSON
    stream_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # 4. Add Watermark (Convert string date to Timestamp)
    stream_df = stream_df.withColumn("timestamp", col("date").cast(TimestampType())) \
                         .withWatermark("timestamp", "10 minutes")

    # 5. Output to CONSOLE (Debug Mode)
    # We use this to verify data is actually flowing before sending back to Kafka
    query = (stream_df.writeStream
             .outputMode("append")
             .format("console")
             .option("truncate", "false")
             .start())
             
    query.awaitTermination()

if __name__ == "__main__":
    spark_conn = SparkSession.builder.appName("YelpKafkaStream").getOrCreate()
    # No try/except needed for basic testing
    spark_conn.sparkContext.setLogLevel("WARN")
    start_streaming(spark_conn)