from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# Initialize Spark Session
spark = SparkSession.builder.appName("KafkaSparkConsumer").getOrCreate()

# Read stream from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "server-logs") \
    .load()

# Convert Kafka value from bytes to String
logs_df = df.selectExpr("CAST(value AS STRING) as log_message")

# Split log message into columns (assuming "timestamp - level - message" format)
logs_df = logs_df.withColumn("timestamp", expr("split(log_message, ' - ')[0]")) \
                 .withColumn("log_level", expr("split(log_message, ' - ')[1]")) \
                 .withColumn("message", expr("split(log_message, ' - ')[2]")) \
                 .drop("log_message")

# Print structured logs to console with a 5-second update interval
query = logs_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .trigger(processingTime="5 seconds") \  # Update every 5 seconds
    .start()

query.awaitTermination()
