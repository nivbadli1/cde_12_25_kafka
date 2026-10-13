"""
Example 2 — Consumer (Kafka -> target file)
============================================
Pipeline:  1_log_generator  ->  srcFile.log  ->  3_producer  ->  Kafka  ->  2_consumer  ->  tarFile.log

RUN ORDER: step 2 of 4  ->  Terminal 2. Start it BEFORE the producer and leave it running.
    A new consumer group starts reading from the end of the topic, so starting it first
    guarantees we don't miss the producer's first messages.

What it does:
    Reads every message from 'kafka-tst-02' and appends it to tarFile.log.
"""
from kafka import KafkaConsumer
import json

# ----- Configuration -----
topic2 = 'kafka-tst-02'                 # must match the topic in 3_producer.py
brokers = ["course-kafka:9092"]
target_file = '/home/developer/kafka/sinkFiles/tarFile.log'

consumer = KafkaConsumer(
    topic2,
    # group_id: consumers with the same group_id share the work (each message goes to ONE of them),
    # and Kafka remembers the group's position (offset), so after a restart we continue where we stopped.
    group_id='File2File',
    bootstrap_servers=brokers,
    # Save ("commit") the current offset back to Kafka every 1 second.
    auto_commit_interval_ms=1000)

with open(target_file, 'w') as f:
    for message in consumer:
        # The producer sent a JSON list of lines -> bytes -> str -> Python list
        lines = json.loads(message.value.decode('utf-8'))
        print(f'received {len(lines)} line(s) from offset {message.offset}')

        # Each line already ends with '\n', so writelines() keeps one event per line in the file
        f.writelines(lines)

        # Force the write to disk now (otherwise it may wait in the buffer and 4_check_file.py sees nothing)
        f.flush()
