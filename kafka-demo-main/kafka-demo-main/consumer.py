"""
Task 2 — Write a Kafka Consumer

Create a consumer that:
    •	Connects to localhost:9092
    •	Reads messages from lab-events
    •	Uses a JSON deserializer
    •	Prints messages in format:
 
 Received -> id=5 | time=1732716251094 | value="abc123"
 
 Requirements:
    •	Use KafkaConsumer
    •	Set group ID to: lab-consumer-group
    •	Enable automatic offset commits
    •	Start reading from the latest messages
    •	Run until KeyboardInterrupt
"""

# TODO: Import KafkaConsumer and json


def run_consumer():
    """
    create a KafkaConsumer against localhost:9092.
    """
    
    # TODO: Create a KafkaConsumer connected to localhost:9092
    # TODO: Subscribe to topic 'lab-events'
    
    # TODO: Inform that the consumer has started and which topic we're listening to
    print()

    try:
        # TODO: Read messages in a loop until KeyboardInterrupt
        for msg in :
            # TODO: Extract the JSON value into a variable named 'data'

            # TODO: Print messages in the required format:
            print()

    except KeyboardInterrupt:
        # TODO: Close the consumer and stop gracefully
        print("\nStopping consumer...")
    finally:
        


if __name__ == "__main__":
    run_consumer()