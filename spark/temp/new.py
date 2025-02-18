from kafka import KafkaConsumer
from collections import defaultdict
from datetime import datetime

# Kafka configuration
kafka_broker = 'localhost:9092'  # Replace with your Kafka broker address
kafka_topic = 'server-logs'

# Function to parse log entries
def parse_log_entry(log_entry):
    parts = log_entry.split(" - ")
    timestamp = datetime.strptime(parts[0].replace("Produced: ", ""), "%Y-%m-%d %H:%M:%S")
    log_level = parts[1]
    message = parts[2]
    return timestamp, log_level, message

# Analyze log data
def analyze_logs(logs):
    log_level_counts = defaultdict(int)
    log_level_timestamps = defaultdict(list)

    for log in logs:
        timestamp, log_level, _ = parse_log_entry(log)
        log_level_counts[log_level] += 1
        log_level_timestamps[log_level].append(timestamp)

    # Calculate time intervals between log messages for each log level
    log_level_intervals = defaultdict(list)
    for level, timestamps in log_level_timestamps.items():
        for i in range(1, len(timestamps)):
            interval = (timestamps[i] - timestamps[i - 1]).total_seconds()
            log_level_intervals[level].append(interval)

    return log_level_counts, log_level_intervals

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
                log_level_counts, log_level_intervals = analyze_logs(logs)
                print("\nLog Level Counts:")
                for level, count in log_level_counts.items():
                    print("{}: {}".format(level, count))  # Replaced f-string with .format
                print("\nAverage Time Intervals Between Log Messages (in seconds):")
                for level, intervals in log_level_intervals.items():
                    avg_interval = sum(intervals) / len(intervals) if intervals else 0
                    print("{}: {:.2f}".format(level, avg_interval))  # Replaced f-string with .format

    except KeyboardInterrupt:
        print("Consumption interrupted by user.")
    finally:
        consumer.close()

# Main function
if __name__ == "__main__":
    consume_logs()

