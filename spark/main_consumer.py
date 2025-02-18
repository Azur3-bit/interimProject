from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, window
from kafka import KafkaConsumer
import logging


logging.getLogger("org.apache.spark").setLevel(logging.ERROR)
logging.getLogger("akka").setLevel(logging.ERROR)


kafka_broker = 'localhost:9092' 
kafka_topic = 'server-logs'


spark = SparkSession.builder \
    .appName("KafkaLogAnalyzer") \
    .getOrCreate()


def parse_log_entry(log_entry):
    parts = log_entry.split(" - ")
    timestamp = parts[0].replace("Produced: ", "")
    log_level = parts[1]
    message = parts[2]
    return timestamp, log_level, message

def consume_logs():
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=[kafka_broker],
        auto_offset_reset='earliest', 
        value_deserializer=lambda x: x.decode('utf-8') 
    )

    logs = []
    try:
        for message in consumer:
            log_message = message.value
            logs.append(log_message)
            print("Consumed: {}".format(log_message))

            if len(logs) % 10 == 0:
                df = spark.createDataFrame([parse_log_entry(log) for log in logs], ["timestamp", "log_level", "message"])
                df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

                log_level_counts = df.groupBy("log_level").count()
                print("\nLog Level Counts:")
                log_level_counts.show()

                log_level_intervals = df.withColumn("window", window(col("timestamp"), "1 minute")).groupBy("log_level", "window").count()
                log_level_intervals.show()

    except KeyboardInterrupt:
        print("Consumption interrupted by user.")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_logs()
