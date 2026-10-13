"""
Example 2 — Producer (source file -> Kafka)
============================================
Pipeline:  1_log_generator  ->  srcFile.log  ->  3_producer  ->  Kafka  ->  2_consumer  ->  tarFile.log

RUN ORDER: step 3 of 4  ->  Terminal 3, after 1_log_generator.py and 2_consumer.py are running.

What it does:
    "Tails" srcFile.log (like `tail -f`): every time new lines appear, it sends them
    to the topic 'kafka-tst-02' as ONE message containing a JSON list of lines.
"""
from kafka import KafkaProducer
from time import sleep
import json

# ----- Configuration -----
topic2 = 'kafka-tst-02'
brokers = ["course-kafka:9092"]
source_file = '/home/developer/kafka/srcFiles/srcFile.log'

producer = KafkaProducer(
    bootstrap_servers=brokers,
    client_id='producer',           # a name for this client — shows up in broker logs/metrics
    # acks: how many brokers must confirm each message before send() is considered successful
    #   0  = don't wait at all (fastest, may lose data)
    #   1  = wait for the partition LEADER only (balanced — used here)
    #  'all' = wait for all in-sync replicas (safest, slowest)
    acks=1,
    compression_type=None,          # could be 'gzip' / 'snappy' / 'lz4' to save network bandwidth
    retries=3,                      # retry a failed send up to 3 times before raising an error
    reconnect_backoff_ms=50,        # wait 50ms before the first reconnect attempt...
    reconnect_backoff_max_ms=1000)  # ...growing up to 1s between attempts if the broker is down

with open(source_file, 'r') as f:
    while True:
        # readlines() returns ALL lines written since our last read (an empty list if nothing new).
        # The file object remembers its position, so we never send the same line twice.
        lines = f.readlines()

        if not lines:
            # Nothing new yet -> wait a bit and poll again
            sleep(1)
            # Seeking to the current position clears the "end of file" state,
            # so the next readlines() can pick up lines appended by the generator.
            f.seek(f.tell())
        else:
            print(lines)
            # Kafka only accepts bytes: Python list -> JSON string -> UTF-8 bytes
            producer.send(topic=topic2, value=json.dumps(lines).encode('utf-8'))
            producer.flush()        # block until the broker confirms the message
            sleep(3)                # batch lines for 3 seconds before the next read
