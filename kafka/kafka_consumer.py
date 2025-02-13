from kafka import KafkaConsumer

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'server-logs',  # Topic name (ensure it matches producer topic)
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # Read messages from the beginning
    enable_auto_commit=True,  # Automatically commit offsets
    group_id='log-consumer-group',  # Consumer group to track offsets
)

print("Consuming log messages from Kafka topic 'server-logs':")

# Read messages
for message in consumer:
    print(message.value.decode('utf-8'))
