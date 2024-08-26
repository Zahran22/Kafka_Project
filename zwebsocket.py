from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
import requests
import os
import random
import cv2
import json

# Group ID and Topics
topic = 'zahro'
error_topic = 'zahro-errors'
completed_topic = 'zahro-completed'

# Configuration for the consumer and producer
conf = {
    'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094',
    'enable.auto.commit': True,
    'group.id': 'group1',
    'auto.offset.reset': 'earliest'
}

# Create Consumer and Producer
consumer = Consumer(conf)
consumer.subscribe([topic])
producer = Producer(conf)

IMAGES_DIR = "images"  # Directory where images are stored

def process_image(image_id):
    image_path = os.path.join(IMAGES_DIR, f"{image_id}.jpg")  # Assuming image extension is jpg; adjust if needed
    if os.path.exists(image_path):
        # Read the image using OpenCV
        img = cv2.imread(image_path)
        if img is not None:
            # Convert the image to black and white
            bw_image = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            # Save the black and white image
            cv2.imwrite(image_path, bw_image)
            print(f"Processed image {image_id} and saved as black and white.")
            # Produce to the completed topic
            producer.produce(completed_topic, key=None, value=json.dumps({'image_id': image_id}))
        else:
            print(f"Failed to read image {image_id}.")
            # Produce to the error topic
            producer.produce(error_topic, key=None, value=json.dumps({'image_id': image_id, 'error': 'Failed to read image'}))
    else:
        print(f"Image with ID {image_id} not found.")
        # Produce to the error topic
        producer.produce(error_topic, key=None, value=json.dumps({'image_id': image_id, 'error': 'Image not found'}))

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
            image_id = msg.value().decode('utf-8')
            process_image(image_id)

except KeyboardInterrupt:
    print("Consumer interrupted")

finally:
    consumer.close()
    producer.flush()