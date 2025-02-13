from kafka import KafkaProducer
import random
import time
from datetime import datetime

# Initialize Kafka Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Sample log levels
log_levels = ['INFO', 'WARNING', 'ERROR']

while True:
    timestamp = datetfrom kafka import KafkaProducer
import random
import time
from datetime import datetime

# Initialize Kafka Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Sample log levels
log_levels = ['INFO', 'WARNING', 'ERROR']

while True:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_level = random.choice(log_levels)
    message = f"{timestamp}, {log_level}: This is a sample log message."
    producer.send('log_topic', value=message.encode('utf-8'))
    time.sleep(1)

print("Log messages are being produced and sent to Kafka topic 'log_topic'.")
