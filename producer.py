#!/usr/bin/env python

import json
import os
import time
import uuid
from random import choice, randint, uniform
from datetime import datetime, timezone
from confluent_kafka import Producer
from faker import Faker

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')

fake = Faker()

# Kafka delivery callback
def delivery_callback(err, msg):
    if err:
        print(f'ERROR: Message failed delivery: {err}')
    else:
        print(f"Produced to topic {msg.topic()}: key={msg.key().decode('utf-8')} value={msg.value().decode('utf-8')}")

# Kafka producer config
producer = Producer({
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'compression.type': 'none',
    'batch.size': 15000,
    'retries': 5,
    'max.in.flight.requests.per.connection': 5,
    'enable.idempotence': True,
    'acks': 'all',
})

EVENT_TYPES = ['view', 'add_to_cart', 'purchase']
PRODUCTS = ['book', 'alarm clock', 't-shirts', 'gift card', 'batteries', 'headphones', 'mug']

def generate_event():
    event_type = choice(EVENT_TYPES)
    user_id = f"user_{randint(1,200)}"
    product_id = choice(PRODUCTS)
    quantity = randint(1,5)
    price = round(uniform(5, 200), 2)
    ts = datetime.now(timezone.utc).isoformat()

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "user_id": user_id,
        "product_id": product_id,
        "price": price,
        "currency": "EUR",
        "quantity": quantity,
        "session_id": str(uuid.uuid4()),
        "user_agent": fake.user_agent(),
        "ts": ts
    }
    return event

def produce_events(n=50, delay=0.05):
    for _ in range(n):
        event = generate_event()

        producer.produce(
            topic=KAFKA_TOPIC,
            key=event['user_id'].encode('utf-8'),
            value=json.dumps(event).encode('utf-8'),
            callback=delivery_callback
        )

        producer.poll(0.1)
        time.sleep(delay)
    producer.flush()
    print(f"{n} events produced successfully to topic '{KAFKA_TOPIC}'")

if __name__ == '__main__':
    produce_events(n=100, delay=0.02)
