from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

# Initialize SparkSession with the correct configuration
spark = SparkSession.builder \
    .appName("KafkaRDDConsumer") \
    .config("spark.sql.streaming.kafka.useDeprecatedOffsetFetching", "false") \
    .getOrCreate()

# Read stream from Kafka
kafkaStreamDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "server-logs") \
    .load()

# Extract the 'value' column (which contains the log message) from the Kafka stream
kafkaRDD = kafkaStreamDF.selectExpr("CAST(value AS STRING)")

# Split the log message into separate fields (log level as an example)
logLevelsDF = kafkaRDD.select(split(col("value"), " - ").getItem(1).alias("logLevel"))

# Group by the log level and count occurrences
logLevelCounts = logLevelsDF.groupBy("logLevel").count()

# Output the result to the console
query = logLevelCounts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Await termination
query.awaitTermination()
