#!/usr/bin/env python

import os
import json
from confluent_kafka import Consumer, KafkaError
from snowflake.connector import connect

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_GROUP = os.getenv('KAFKA_GROUP')

SNOWFLAKE_USER = os.getenv('SF_USER')
SNOWFLAKE_PASSWORD = os.getenv('SF_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SF_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SF_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SF_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SF_SCHEMA')

# Kafka consumer config
consumer = Consumer({
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'group.id': KAFKA_GROUP,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'session.timeout.ms': 10000,
    'enable.partition.eof': True,
})
consumer.subscribe([KAFKA_TOPIC])

# Snowflake connection
conn = connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA
)
cs = conn.cursor()

def run_consumer(batch_size=100):
    buffer = []
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            event = json.loads(msg.value().decode('utf-8'))
            buffer.append(event)

            if len(buffer) >= batch_size:
                for e in buffer:
                    cs.execute(
                        "INSERT INTO RAW.EVENTS_JSON(EVENT_ID, RAW_DATA) VALUES (%s, TO_VARIANT(%s))",
                        (e['event_id'], json.dumps(e))
                    )
                conn.commit()
                print(f"{len(buffer)} events inserted")
                buffer.clear()
    finally:
        cs.close()
        conn.close()
        consumer.close()

if __name__ == "__main__":
    run_consumer()