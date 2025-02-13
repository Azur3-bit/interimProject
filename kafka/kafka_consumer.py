from kafka import KafkaConsumer

# Initialize Kafka Consumer
# consumer = KafkaConsumer('log_topic', bootstrap_servers='localhost:9092', auto_offset_reset='earliest')

consumer = KafkaConsumer('server-logs', bootstrap_servers='localhost:9092', auto_offset_reset='earliest')




print("Consuming log messages from Kafka topic 'log_topic':")
for message in consumer:
    print(message.value.decode('utf-8'))
