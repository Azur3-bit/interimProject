from kafka import KafkaConsumer

# Initialize Kafka Consumer
<<<<<<< HEAD
consumer = KafkaConsumer('server_logs', bootstrap_servers='localhost:9092', auto_offset_reset='earliest')
=======
# consumer = KafkaConsumer('log_topic', bootstrap_servers='localhost:9092', auto_offset_reset='earliest')

consumer = KafkaConsumer('server-logs', bootstrap_servers='localhost:9092', auto_offset_reset='earliest')



>>>>>>> d04468222711fa2010172d53333c86abf68a52c5

print("Consuming log messages from Kafka topic 'server_logs':")
for message in consumer:
    print(message.value.decode('utf-8'))
