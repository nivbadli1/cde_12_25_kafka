"""
Example 1 — Basic Consumer
===========================
RUN ORDER: step 1 of 2  ->  run this FIRST, then run 2_producer.py in a second terminal.

Why first? This consumer has no group_id, so it starts reading from the END of the topic
(auto_offset_reset='latest' is the default). Messages sent before it starts are not shown.

What it does:
    Connects to the topic 'kafka-tst-07' and prints every message with its offset and time.
    The script never ends on its own — stop it with Ctrl+C.
"""
from kafka import KafkaConsumer
from datetime import datetime

# ----- Configuration -----
topic = 'kafka-tst-07'             # must match the topic used in 2_producer.py
brokers = 'course-kafka:9092'      # host:port of the Kafka broker (container name in the course env)

# KafkaConsumer behaves like a Python generator:
# iterating over it yields one message at a time, and it BLOCKS while waiting for new ones.
consumer = KafkaConsumer(topic, bootstrap_servers=brokers)

for message in consumer:
    # message.timestamp is a Unix timestamp in MILLISECONDS -> divide by 1000 for seconds
    dt_object = datetime.fromtimestamp(message.timestamp / 1000)

    # message.value is raw bytes (b'...'); .decode('utf-8') turns it into a normal string
    print("the value is: ", message.value.decode('utf-8'))
    print("the key is:   ", message.key)        # None when the producer sent no key
    print("the offset is:", message.offset)     # position of the message inside its partition
    print("the time is:  ", dt_object)
    print('-' * 40)
