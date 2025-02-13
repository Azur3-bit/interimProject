from kafka import KafkaConsumer

# Initialize Kafka Consumer
consumer = KafkaConsumer('server_logs', bootstrap_servers='localhost:9092', auto_offset_reset='earliest')

print("Consuming log messages from Kafka topic 'server_logs':")
for message in consumer:
    print(message.value.decode('utf-8'))
