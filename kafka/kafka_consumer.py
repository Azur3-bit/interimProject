from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'server-logs', 
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest', 
    # enable_auto_commit=True,  # Automatically commit offsets  (can be ignored default) 
    # group_id='log-consumer-group',  # Consumer group to track offsets -- (remove this and check optional command) 
)

print("Consuming log messages from Kafka topic 'server-logs':")

for message in consumer:
    print(message.value.decode('utf-8'))
