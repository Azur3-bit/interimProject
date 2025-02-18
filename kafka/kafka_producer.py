from kafka import KafkaProducer
import random
import time
from datetime import datetime

producer = KafkaProducer(bootstrap_servers='localhost:9092')

log_levels = ['INFO', 'WARNING', 'ERROR']

while True:
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_level = random.choice(log_levels)
    message = f'{timestamp} - {log_level} - Sample log message'
    
    producer.send('server-logs', message.encode('utf-8'))
    print(f'Produced: {message}')
    
    time.sleep(1)
