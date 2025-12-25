from kafka import KafkaProducer
import time, random

producer = KafkaProducer(bootstrap_servers='localhost:9092')

levels = ['INFO', 'WARN', 'ERROR']
messages = ['User logged in', 'DB connection failed', 'Memory usage high', 'Timeout occurred']

while True:
    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    level = random.choice(levels)
    msg = random.choice(messages)
    log = f"{ts},{level},{msg}"
    producer.send('app-logs', value=log.encode('utf-8'))
    time.sleep(0.5)
