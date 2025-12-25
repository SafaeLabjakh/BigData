from kafka import KafkaConsumer

# Connexion au topic
consumer = KafkaConsumer(
    'app-logs',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # Commence depuis le début
    group_id='log-consumers'
)

# Lecture des messages
for message in consumer:
    print(f"Log reçu: {message.value.decode('utf-8')}")
