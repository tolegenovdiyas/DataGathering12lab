"""
    Task 1 — Write a Kafka Producer

Create a producer that:
        •	Connects to localhost:9092
        •	Sends JSON messages every 1 second
        •	Each message must contain:
 {"id": <int>, "timestamp": <unix_ms>, "value": "<random_string>"}

 Sends messages to topic: lab-events

 Requirements:
        •	Use KafkaProducer
        •	Use a JSON serializer
        •	Print each message before sending
        •	Stop gracefully on KeyboardInterrupt

"""


import random
import string


def random_string(length=6):
    return "".join(random.choice(string.ascii_lowercase) for _ in range(length))


# TODO: Create a KafkaProducer connected to localhost:9092 (Use a JSON serializer for the value)


# TODO: Inform that the producer has started and which topic we're sending to
print()
msg_id = 0

try:
    while True:
        # TODO: Create a message dict with keys: id, timestamp (unix ms), value (random string)
        

        # TODO: Send the message to the 'lab-events' topic using producer.send(...)
        

        # TODO: Print (or log) each message before or after sending
        print("Sent:", )

        msg_id += 1

        # TODO: Wait 1 second between messages
        

except KeyboardInterrupt:
    # TODO: Close the producer and stop gracefully
    print("\nStopping producer...")
    
