"""
Example 1 — Basic Producer
===========================
RUN ORDER: step 2 of 2  ->  make sure 1_consumer.py is already running in another terminal.

What it does:
    Sends two hard-coded messages to the topic 'kafka-tst-07' and exits.
    Watch them appear in the consumer terminal.
"""
from kafka import KafkaProducer

# ----- Configuration -----
topic = 'kafka-tst-07'
brokers = 'course-kafka:9092'

producer = KafkaProducer(bootstrap_servers=brokers)

# Kafka only transports BYTES -> note the b'' prefix.
# If the topic does not exist yet, the broker creates it automatically on the first send
# (auto.create.topics.enable=true in the course environment).
producer.send(topic, value=b'Hello, World!!!!')

# send() is ASYNCHRONOUS: it only puts the message in an internal buffer.
# flush() BLOCKS until every buffered message has really been delivered to the broker.
producer.flush()

# A message can also carry a KEY.
# Kafka hashes the key to choose the partition -> same key = same partition = guaranteed order.
producer.send(topic, key=b'event#2', value=b'This is a Kafka-Python basic tutorial')
producer.flush()

print('2 messages sent to', topic)
