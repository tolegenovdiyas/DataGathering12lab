from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Sending messages...")

count = 0
try:
    while True:
        message = {'id': count, 'data': f'Message {count}'}
        producer.send('quickstart-events', value=message)
        print(f"Sent: {message}")
        count += 1
        time.sleep(2)
except KeyboardInterrupt:
    producer.close()
