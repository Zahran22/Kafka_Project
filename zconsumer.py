from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
import sys
import requests
import random
import json

# Group ID and Topics
me = 'zahro'
groupid = me + '-group5'
error_topic = 'zahro-errors'
completed_topic = 'zahro-completed'

# Configuration for consumer and producer
conf = {
    'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094',
    'enable.auto.commit': True,
    'group.id': groupid,
    'auto.offset.reset': 'earliest'
}

def detect_object(id):
    return random.choice(['car', 'house', 'person'])

# Create Consumer and Producer
consumer = Consumer(conf)
producer = Producer(conf)
topics = [me, error_topic, completed_topic]
consumer.subscribe(topics)

try:
    while True:
        # Poll for new messages
        msg = consumer.poll(timeout=1.0)  # Timeout in seconds

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Reached end of partition {msg.partition()}")
            else:
                raise KafkaException(msg.error())
        else:
            message_value = msg.value().decode('utf-8')
            if msg.topic() == me:
                # Send the detection result to the server
                requests.put(f'http://127.0.0.1:5000/object/{message_value}', json={"object": detect_object(message_value)})
                print(f"Consumer group {groupid}: Received message: {message_value}")
            elif msg.topic() == error_topic:
                # Handle error messages
                error_data = json.loads(message_value)
                print(f"Error processing image {error_data['image_id']}: {error_data['error']}")
            elif msg.topic() == completed_topic:
                # Handle completed messages
                completed_data = json.loads(message_value)
                print(f"Completed processing image {completed_data['image_id']}")
                # Notify the web server about the completed task
                requests.post(f'http://127.0.0.1:5000/completed/{completed_data["image_id"]}')

except KeyboardInterrupt:
    print("Consumer interrupted")

finally:
    consumer.close()
    producer.flush()