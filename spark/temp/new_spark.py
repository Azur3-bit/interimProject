from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, window
from kafka import KafkaConsumer

# Kafka configuration
kafka_broker = 'localhost:9092'  # Replace with your Kafka broker address
kafka_topic = 'server-logs'

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaLogAnalyzer") \
    .getOrCreate()

# Function to parse log entries
def parse_log_entry(log_entry):
    parts = log_entry.split(" - ")
    timestamp = parts[0].replace("Produced: ", "")
    log_level = parts[1]
    message = parts[2]
    return timestamp, log_level, message

# Kafka consumer function
def consume_logs():
    # Create a Kafka consumer
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=[kafka_broker],
        auto_offset_reset='earliest',  # Start reading from the earliest message
        group_id='log_analyzer_group',  # Consumer group ID
        value_deserializer=lambda x: x.decode('utf-8')  # Decode messages as UTF-8
    )

    logs = []
    try:
        for message in consumer:
            # Append the log message to the list
            log_message = message.value
            logs.append(log_message)
            print("Consumed: {}".format(log_message))

            # Analyze logs periodically (e.g., after every 10 messages)
            if len(logs) % 10 == 0:
                df = spark.createDataFrame([parse_log_entry(log) for log in logs], ["timestamp", "log_level", "message"])
                df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

                # Calculate log level counts
                log_level_counts = df.groupBy("log_level").count()
                print("\nLog Level Counts:")
                log_level_counts.show()

                # Calculate average time intervals between log messages for each log level
                log_level_intervals = df.withColumn("window", window(col("timestamp"), "1 minute")).groupBy("log_level", "window").count()
                log_level_intervals.show()

    except KeyboardInterrupt:
        print("Consumption interrupted by user.")
    finally:
        consumer.close()

# Main function
if __name__ == "__main__":
    consume_logs()



#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 kafka_spark_log_analyzer.py

